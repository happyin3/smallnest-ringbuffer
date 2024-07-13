// Copyright 2019 smallnest. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package ringbuffer

// [Golang如何实现一个环形缓冲器（ringbuffer）](https://juejin.cn/post/7138675261649715236)
// [高性能环形队列及其实现](https://hedzr.com/algorithm/golang/ringbuf-index/)

import (
	"context"
	"errors"
	"io"
	"sync"
	"unsafe"
)

// “哨兵”错误处理策略，错误可导出
var (
	ErrTooMuchDataToWrite = errors.New("too much data to write")
	ErrIsFull             = errors.New("ringbuffer is full")
	ErrIsEmpty            = errors.New("ringbuffer is empty")
	ErrIsNotEmpty         = errors.New("ringbuffer is not empty")
	ErrAcquireLock        = errors.New("unable to acquire lock")
	ErrWriteOnClosed      = errors.New("write on closed ringbuffer")
)

// 支持并发读写（线程安全）
// RingBuffer is a circular buffer that implement io.ReaderWriter interface.
// It operates like a buffered pipe, where data written to a RingBuffer
// and can be read back from another goroutine.
// It is safe to concurrently read and write RingBuffer.
type RingBuffer struct {
	// 字节切片，1byte=1bit
	buf    []byte
	size   int
	r      int // next position to read
	w      int // next position to write
	isFull bool
	err    error
	block  bool
	// 支持并发读写（线程安全）
	mu sync.Mutex
	// Reset的时候用
	wg sync.WaitGroup
	// sync.Cond基于互斥锁/读写锁
	// 互斥锁sync.Mutex通常用来保护临界区和共享资源
	// 条件变量sync.Cond用来协调想要访问共享资源的goroutine
	// sync.Cond方法
	// NewCond：创建实例
	// Broadcast: 广播唤醒所有
	// Signal: 唤醒一个协程
	// Wait: 等待
	readCond  *sync.Cond // Signalled when data has been read.
	writeCond *sync.Cond // Signalled when data has been written.
}

// New returns a new RingBuffer whose buffer has the given size.
func New(size int) *RingBuffer {
	return &RingBuffer{
		// make: 用于创建并初始化特定类型的内建数据结构：切片、映射、通道
		// make([]T, size, cap)：创建一个切片，元素个数为size，容量为cap，并返回切片的引用
		// new：用于分配内存，但不进行初始化，返回的是指向分配内存的指针
		buf:  make([]byte, size),
		size: size,
	}
}

// NewBuffer returns a new RingBuffer whose buffer is provided.
func NewBuffer(b []byte) *RingBuffer {
	return &RingBuffer{
		buf:  b,
		size: len(b),
	}
}

// SetBlocking sets the blocking mode of the ring buffer.
// If block is true, Read and Write will block when there is no data to read or no space to write.
// If block is false, Read and Write will return ErrIsEmpty or ErrIsFull immediately.
// By default, the ring buffer is not blocking.
// This setting should be called before any Read or Write operation or after a Reset.
func (r *RingBuffer) SetBlocking(block bool) *RingBuffer {
	r.block = block
	if block {
		r.readCond = sync.NewCond(&r.mu)
		r.writeCond = sync.NewCond(&r.mu)
	}
	return r
}

// WithCancel sets a context to cancel the ring buffer.
// When the context is canceled, the ring buffer will be closed with the context error.
// A goroutine will be started and run until the provided context is canceled.
func (r *RingBuffer) WithCancel(ctx context.Context) *RingBuffer {
	go func() {
		select {
		case <-ctx.Done():
			r.CloseWithError(ctx.Err())
		}
	}()
	return r
}

func (r *RingBuffer) setErr(err error, locked bool) error {
	if !locked {
		r.mu.Lock()
		defer r.mu.Unlock()
	}
	if r.err != nil && r.err != io.EOF {
		return r.err
	}

	switch err {
	// Internal errors are transient
	case nil, ErrIsEmpty, ErrIsFull, ErrAcquireLock, ErrTooMuchDataToWrite, ErrIsNotEmpty:
		return err
	default:
		r.err = err
		if r.block {
			// 已读，有空间，通知可写
			r.readCond.Broadcast()
			// 已写，有数据，通知可读
			r.writeCond.Broadcast()
		}
	}
	return err
}

func (r *RingBuffer) readErr(locked bool) error {
	// 线程安全，读写时都需要加锁，很讨厌
	// 不管是性能上，还是代码层面，无锁可能是一个优化点
	// 是否已经上锁
	if !locked {
		r.mu.Lock()
		defer r.mu.Unlock()
	}
	// io.EOF: End-Of-File
	// 是Go中重要的错误变量，用户表示输入流的结尾，因为每个文件都有一个结尾，
	// 所以io.EOF很多时候并不能算是一个错误，更重要是表示输入流结束了
	// [io.EOF设计的缺陷和改进](https://mp.weixin.qq.com/s/DPtujfVNMw_Jgel_zGTFvw)
	if r.err != nil {
		if r.err == io.EOF {
			// 表示数组已空
			if r.w == r.r && !r.isFull {
				return io.EOF
			}
			return nil
		}
		return r.err
	}
	return nil
}

// 实现了io.Reader接口
// 利用io.Reader可以实现流式数据传输，Reader方法内部是被循环调用的，
// 每次迭代，会从数据源读取一块数据放入缓冲区p中，直到返回io.EOF错误时停止
// Read reads up to len(p) bytes into p. It returns the number of bytes read (0 <= n <= len(p)) and any error encountered.
// Even if Read returns n < len(p), it may use all of p as scratch space during the call.
// If some data is available but not len(p) bytes, Read conventionally returns what is available instead of waiting for more.
// When Read encounters an error or end-of-file condition after successfully reading n > 0 bytes, it returns the number of bytes read.
// It may return the (non-nil) error from the same call or return the error (and n == 0) from a subsequent call.
// Callers should always process the n > 0 bytes returned before considering the error err.
// Doing so correctly handles I/O errors that happen after reading some bytes and also both of the allowed EOF behaviors.
func (r *RingBuffer) Read(p []byte) (n int, err error) {
	// 理论上是缓冲区p空，为什么要readErr
	if len(p) == 0 {
		return 0, r.readErr(false)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	// 提前判断是否有error
	if err := r.readErr(true); err != nil {
		return 0, err
	}

	// sync.WaitGroup
	// 调用Add后，必要调用done
	// 可以不用调用Wait
	r.wg.Add(1)
	defer r.wg.Done()
	n, err = r.read(p)
	// 空的，需要等待已写通知，写完即可读
	for err == ErrIsEmpty && r.block {
		r.writeCond.Wait()
		if err = r.readErr(true); err != nil {
			break
		}
		n, err = r.read(p)
	}
	// 已经读取了部分，通知可写了
	if r.block && n > 0 {
		r.readCond.Broadcast()
	}
	return n, err
}

// TryRead read up to len(p) bytes into p like Read but it is not blocking.
// If it has not succeeded to acquire the lock, it return 0 as n and ErrAcquireLock.
func (r *RingBuffer) TryRead(p []byte) (n int, err error) {
	// 非阻塞模式去锁操作
	// Go 1.18新特性，保持语言简单性，引入反复讨论
	// [Go1.18 新特性：三顾茅庐，被折腾 N 次的 TryLock](https://juejin.cn/post/7064789056873300004)
	ok := r.mu.TryLock()
	if !ok {
		return 0, ErrAcquireLock
	}
	// 锁申请成功后，不要忘了释放
	defer r.mu.Unlock()
	// ?为什么要判断readErr
	// 判空
	if err := r.readErr(true); err != nil {
		return 0, err
	}
	if len(p) == 0 {
		return 0, r.readErr(true)
	}

	n, err = r.read(p)
	if r.block && n > 0 {
		r.readCond.Broadcast()
	}
	return n, err
}

func (r *RingBuffer) read(p []byte) (n int, err error) {
	// 队列为空或队列满了，所以需要同时判断是否空
	if r.w == r.r && !r.isFull {
		return 0, ErrIsEmpty
	}

	// 没有形成环，不需要考虑环的问题
	if r.w > r.r {
		n = r.w - r.r
		// 确认读取长度
		if n > len(p) {
			n = len(p)
		}
		// 切片深拷贝
		copy(p, r.buf[r.r:r.r+n])
		// 取余可以处理环的情况
		// 没有出现环：7 % 10 = 7
		// 出现环了：13 % 10 = 3
		r.r = (r.r + n) % r.size
		return
	}

	// 形成环了
	// 先计算正向的部分，再计算环的部分
	n = r.size - r.r + r.w
	if n > len(p) {
		n = len(p)
	}

	if r.r+n <= r.size {
		// 需要读取的大小小于正向长度
		copy(p, r.buf[r.r:r.r+n])
	} else {
		c1 := r.size - r.r
		copy(p, r.buf[r.r:r.size])
		c2 := n - c1
		copy(p[c1:], r.buf[0:c2])
	}
	// 取余可以处理环的情况
	// 没有出现环：7 % 10 = 7
	// 出现环了：13 % 10 = 3
	r.r = (r.r + n) % r.size

	r.isFull = false

	return n, r.readErr(true)
}

// ReadByte reads and returns the next byte from the input or ErrIsEmpty.
func (r *RingBuffer) ReadByte() (b byte, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err = r.readErr(true); err != nil {
		return 0, err
	}
	for r.w == r.r && !r.isFull {
		if r.block {
			r.writeCond.Wait()
			err = r.readErr(true)
			if err != nil {
				return 0, err
			}
			continue
		}
		return 0, ErrIsEmpty
	}
	b = r.buf[r.r]
	r.r++
	if r.r == r.size {
		r.r = 0
	}

	r.isFull = false
	return b, r.readErr(true)
}

// 实现了io.Writer接口：表示一个编写器，从缓存区读取数据，并将数据源写入目标资源
// Write writes len(p) bytes from p to the underlying buf.
// It returns the number of bytes written from p (0 <= n <= len(p))
// and any error encountered that caused the write to stop early.
// If blocking n < len(p) will be returned only if an error occurred.
// Write returns a non-nil error if it returns n < len(p).
// Write will not modify the slice data, even temporarily.
func (r *RingBuffer) Write(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, r.setErr(nil, false)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	// 获取锁后，就要判断一次当前状态
	if err := r.err; err != nil {
		if err == io.EOF {
			err = ErrWriteOnClosed
		}
		return 0, err
	}
	wrote := 0
	for len(p) > 0 {
		n, err = r.write(p)
		wrote += n
		if !r.block || err == nil {
			break
		}
		err = r.setErr(err, true)
		if r.block && (err == ErrIsFull || err == ErrTooMuchDataToWrite) {
			// 已写，有数据，通知可读
			r.writeCond.Broadcast()
			// 等待已读通知，可继续写
			r.readCond.Wait()
			p = p[n:]
			err = nil
			continue
		}
		break
	}
	if r.block && wrote > 0 {
		r.writeCond.Broadcast()
	}

	return wrote, r.setErr(err, true)
}

// TryWrite writes len(p) bytes from p to the underlying buf like Write, but it is not blocking.
// If it has not succeeded to accquire the lock, it return 0 as n and ErrAcquireLock.
func (r *RingBuffer) TryWrite(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, r.setErr(nil, false)
	}
	ok := r.mu.TryLock()
	if !ok {
		return 0, ErrAcquireLock
	}
	defer r.mu.Unlock()
	if err := r.err; err != nil {
		if err == io.EOF {
			err = ErrWriteOnClosed
		}
		return 0, err
	}

	n, err = r.write(p)
	if r.block && n > 0 {
		r.writeCond.Broadcast()
	}
	return n, r.setErr(err, true)
}

// 返回error: ErrIsFull, ErrTooMuchDataToWrite
func (r *RingBuffer) write(p []byte) (n int, err error) {
	if r.isFull {
		return 0, ErrIsFull
	}

	var avail int
	if r.w >= r.r {
		avail = r.size - r.w + r.r
	} else {
		avail = r.r - r.w
	}

	if len(p) > avail {
		err = ErrTooMuchDataToWrite
		p = p[:avail]
	}
	n = len(p)

	if r.w >= r.r {
		c1 := r.size - r.w
		if c1 >= n {
			copy(r.buf[r.w:], p)
			r.w += n
		} else {
			copy(r.buf[r.w:], p[:c1])
			c2 := n - c1
			copy(r.buf[0:], p[c1:])
			r.w = c2
		}
	} else {
		copy(r.buf[r.w:], p)
		r.w += n
	}

	if r.w == r.size {
		r.w = 0
	}
	if r.w == r.r {
		r.isFull = true
	}

	return n, err
}

// WriteByte writes one byte into buffer, and returns ErrIsFull if buffer is full.
func (r *RingBuffer) WriteByte(c byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.err; err != nil {
		if err == io.EOF {
			err = ErrWriteOnClosed
		}
		return err
	}
	err := r.writeByte(c)
	for err == ErrIsFull && r.block {
		r.readCond.Wait()
		err = r.setErr(r.writeByte(c), true)
	}
	if r.block && err == nil {
		r.writeCond.Broadcast()
	}
	return err
}

// TryWriteByte writes one byte into buffer without blocking.
// If it has not succeeded to acquire the lock, it return ErrAcquireLock.
func (r *RingBuffer) TryWriteByte(c byte) error {
	ok := r.mu.TryLock()
	if !ok {
		return ErrAcquireLock
	}
	defer r.mu.Unlock()
	if err := r.err; err != nil {
		if err == io.EOF {
			err = ErrWriteOnClosed
		}
		return err
	}

	err := r.writeByte(c)
	if err == nil && r.block {
		r.writeCond.Broadcast()
	}
	return err
}

func (r *RingBuffer) writeByte(c byte) error {
	if r.w == r.r && r.isFull {
		return ErrIsFull
	}
	r.buf[r.w] = c
	r.w++

	if r.w == r.size {
		r.w = 0
	}
	if r.w == r.r {
		r.isFull = true
	}

	return nil
}

// Length return the length of available read bytes.
func (r *RingBuffer) Length() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.w == r.r {
		if r.isFull {
			return r.size
		}
		return 0
	}

	if r.w > r.r {
		return r.w - r.r
	}

	return r.size - r.r + r.w
}

// Capacity returns the size of the underlying buffer.
func (r *RingBuffer) Capacity() int {
	return r.size
}

// Free returns the length of available bytes to write.
func (r *RingBuffer) Free() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.w == r.r {
		if r.isFull {
			return 0
		}
		return r.size
	}

	if r.w < r.r {
		return r.r - r.w
	}

	return r.size - r.w + r.r
}

// WriteString writes the contents of the string s to buffer, which accepts a slice of bytes.
func (r *RingBuffer) WriteString(s string) (n int, err error) {
	x := (*[2]uintptr)(unsafe.Pointer(&s))
	h := [3]uintptr{x[0], x[1], x[1]}
	buf := *(*[]byte)(unsafe.Pointer(&h))
	return r.Write(buf)
}

// Bytes returns all available read bytes.
// It does not move the read pointer and only copy the available data.
// If the dst is big enough it will be used as destination,
// otherwise a new buffer will be allocated.
func (r *RingBuffer) Bytes(dst []byte) []byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	getDst := func(n int) []byte {
		if cap(dst) < n {
			return make([]byte, n)
		}
		return dst[:n]
	}

	if r.w == r.r {
		if r.isFull {
			buf := getDst(r.size)
			copy(buf, r.buf[r.r:])
			copy(buf[r.size-r.r:], r.buf[:r.w])
			return buf
		}
		return nil
	}

	if r.w > r.r {
		buf := getDst(r.w - r.r)
		copy(buf, r.buf[r.r:r.w])
		return buf
	}

	n := r.size - r.r + r.w
	buf := getDst(n)

	if r.r+n < r.size {
		copy(buf, r.buf[r.r:r.r+n])
	} else {
		c1 := r.size - r.r
		copy(buf, r.buf[r.r:r.size])
		c2 := n - c1
		copy(buf[c1:], r.buf[0:c2])
	}

	return buf
}

// IsFull returns this ringbuffer is full.
func (r *RingBuffer) IsFull() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.isFull
}

// IsEmpty returns this ringbuffer is empty.
func (r *RingBuffer) IsEmpty() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	return !r.isFull && r.w == r.r
}

// CloseWithError closes the writer; reads will return
// no bytes and the error err, or EOF if err is nil.
//
// CloseWithError never overwrites the previous error if it exists
// and always returns nil.
func (r *RingBuffer) CloseWithError(err error) {
	if err == nil {
		err = io.EOF
	}
	r.setErr(err, false)
}

// CloseWriter closes the writer.
// Reads will return any remaining bytes and io.EOF.
func (r *RingBuffer) CloseWriter() {
	r.setErr(io.EOF, false)
}

// Flush waits for the buffer to be empty and fully read.
// If not blocking ErrIsNotEmpty will be returned if the buffer still contains data.
func (r *RingBuffer) Flush() error {
	// 数组非空
	for !r.IsEmpty() {
		// 非阻塞
		if !r.block {
			return r.setErr(ErrIsNotEmpty, false)
		}
		// 阻塞
		r.mu.Lock()
		r.readCond.Wait()
		err := r.readErr(true)
		r.mu.Unlock()
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return err
		}
	}

	err := r.readErr(false)
	if err == io.EOF {
		return nil
	}
	return err
}

// Reset the read pointer and writer pointer to zero.
func (r *RingBuffer) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Set error so any readers/writers will return immediately.
	r.setErr(errors.New("reset called"), true)
	if r.block {
		r.readCond.Broadcast()
		r.writeCond.Broadcast()
	}

	// Unlock the mutex so readers/writers can finish.
	r.mu.Unlock()
	r.wg.Wait()
	r.mu.Lock()
	r.r = 0
	r.w = 0
	r.err = nil
	r.isFull = false
}

// WriteCloser returns a WriteCloser that writes to the ring buffer.
// When the returned WriteCloser is closed, it will wait for all data to be read before returning.
func (r *RingBuffer) WriteCloser() io.WriteCloser {
	return &writeCloser{RingBuffer: r}
}

type writeCloser struct {
	*RingBuffer
}

// Close provides a close method for the WriteCloser.
func (wc *writeCloser) Close() error {
	wc.CloseWriter()
	return wc.Flush()
}
