package socks5

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"testing/iotest"
	"time"
)

func TestProxyStream_SmallData(t *testing.T) {
	src := bytes.NewReader([]byte("hello world"))
	var dst bytes.Buffer
	if err := ProxyStream(src, &dst); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dst.String() != "hello world" {
		t.Fatalf("got %q, want %q", dst.String(), "hello world")
	}
}

func TestProxyStream_LargeData(t *testing.T) {
	data := make([]byte, 1<<20) // 1 MiB, much larger than the 64 KiB ring buffer
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	src := bytes.NewReader(data)
	var dst bytes.Buffer
	if err := ProxyStream(src, &dst); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Equal(dst.Bytes(), data) {
		t.Fatalf("data mismatch: got %d bytes, want %d", dst.Len(), len(data))
	}
}

func TestProxyStream_Empty(t *testing.T) {
	src := bytes.NewReader(nil)
	var dst bytes.Buffer
	if err := ProxyStream(src, &dst); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dst.Len() != 0 {
		t.Fatalf("expected empty, got %d bytes", dst.Len())
	}
}

func TestProxyStream_ExactBufferSize(t *testing.T) {
	// Exactly 64 KiB — boundary for the ring buffer.
	data := make([]byte, 64<<10)
	for i := range data {
		data[i] = byte(i)
	}
	src := bytes.NewReader(data)
	var dst bytes.Buffer
	if err := ProxyStream(src, &dst); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Equal(dst.Bytes(), data) {
		t.Fatalf("data mismatch")
	}
}

// TestProxyStream_OneByteReader forces the reader to deliver data one byte
// at a time, which aggressively exercises the ring buffer's wrap-around:
// head advances by 1 on every iteration, so the reader repeatedly crosses
// the bufSize boundary.
func TestProxyStream_OneByteReader(t *testing.T) {
	data := make([]byte, 200<<10) // enough to wrap several times
	for i := range data {
		data[i] = byte(i)
	}
	src := iotest.OneByteReader(bytes.NewReader(data))
	var dst bytes.Buffer
	if err := ProxyStream(src, &dst); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Equal(dst.Bytes(), data) {
		t.Fatalf("data mismatch: got %d bytes, want %d", dst.Len(), len(data))
	}
}

// TestProxyStream_HalfReader exercises wrap-around with a different stride —
// each Read returns only half of the requested slice, so head advances in
// irregular chunks relative to the buffer boundary.
func TestProxyStream_HalfReader(t *testing.T) {
	data := make([]byte, 200<<10)
	for i := range data {
		data[i] = byte(i * 3)
	}
	src := iotest.HalfReader(bytes.NewReader(data))
	var dst bytes.Buffer
	if err := ProxyStream(src, &dst); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Equal(dst.Bytes(), data) {
		t.Fatalf("data mismatch: got %d bytes, want %d", dst.Len(), len(data))
	}
}

// TestProxyStream_DataErrReader verifies we handle the io.Reader contract
// variant where Read returns the final bytes together with a non-nil error
// on the same call, rather than on a subsequent call.
func TestProxyStream_DataErrReader(t *testing.T) {
	data := []byte("hello, data+err reader")
	src := iotest.DataErrReader(bytes.NewReader(data))
	var dst bytes.Buffer
	if err := ProxyStream(src, &dst); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Equal(dst.Bytes(), data) {
		t.Fatalf("got %q, want %q", dst.String(), data)
	}
}

// slowWriter accepts only one byte per call, stressing the writer side of
// the ring buffer (repeated partial drains, re-computing the contiguous
// readable segment on every iteration).
type slowWriter struct {
	buf bytes.Buffer
}

func (s *slowWriter) Write(p []byte) (int, error) {
	if len(p) > 1 {
		return s.buf.Write(p[:1])
	}
	return s.buf.Write(p)
}

func TestProxyStream_SlowWriter(t *testing.T) {
	data := make([]byte, 256<<10)
	for i := range data {
		data[i] = byte(i)
	}
	src := bytes.NewReader(data)
	dst := &slowWriter{}
	if err := ProxyStream(src, dst); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Equal(dst.buf.Bytes(), data) {
		t.Fatalf("data mismatch: got %d bytes, want %d", dst.buf.Len(), len(data))
	}
}

// errWriter fails after n bytes have been written. Used to test the write
// error propagation path. iotest.TruncateWriter silently drops data without
// returning an error, so it doesn't fit here.
type errWriter struct {
	n   int
	buf bytes.Buffer
}

func (e *errWriter) Write(p []byte) (int, error) {
	if e.buf.Len()+len(p) > e.n {
		remaining := e.n - e.buf.Len()
		if remaining > 0 {
			e.buf.Write(p[:remaining])
			return remaining, errors.New("write limit reached")
		}
		return 0, errors.New("write limit reached")
	}
	return e.buf.Write(p)
}

func TestProxyStream_WriteError(t *testing.T) {
	data := make([]byte, 128<<10)
	src := bytes.NewReader(data)
	dst := &errWriter{n: 1000}
	err := ProxyStream(src, dst)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "write error") {
		t.Fatalf("expected write error, got: %v", err)
	}
}

func TestProxyStream_ReadError(t *testing.T) {
	boom := errors.New("boom")
	src := iotest.ErrReader(boom)
	var dst bytes.Buffer
	err := ProxyStream(src, &dst)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "read error") || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("expected wrapped boom, got: %v", err)
	}
}

func TestProxyStream_TCPPair(t *testing.T) {
	// End-to-end over real TCP: exercises the closeReader/closeWriter paths.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	data := make([]byte, 256<<10)
	for i := range data {
		data[i] = byte(i)
	}

	srvErr := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			srvErr <- err
			return
		}
		defer conn.Close()
		if _, err := conn.Write(data); err != nil {
			srvErr <- err
			return
		}
		srvErr <- conn.(*net.TCPConn).CloseWrite()
	}()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	var dst bytes.Buffer
	if err := ProxyStream(conn, &dst); err != nil {
		t.Fatalf("ProxyStream: %v", err)
	}
	if !bytes.Equal(dst.Bytes(), data) {
		t.Fatalf("data mismatch: got %d bytes, want %d", dst.Len(), len(data))
	}
	if err := <-srvErr; err != nil {
		t.Fatalf("server error: %v", err)
	}
}

// plainReader/plainWriter hide the WriteTo/ReadFrom fast paths so io.Copy
// actually goes through its internal buffer — an apples-to-apples baseline.
type plainReader struct{ io.Reader }
type plainWriter struct{ io.Writer }

// delayWriter and delayReader model a slow peer. This is the scenario
// ProxyStream is designed for: the reader keeps filling the ring buffer
// in parallel with a blocked writer (and vice versa).
type delayWriter struct {
	w     io.Writer
	delay time.Duration
}

func (d *delayWriter) Write(p []byte) (int, error) {
	time.Sleep(d.delay)
	return d.w.Write(p)
}

type delayReader struct {
	r     io.Reader
	delay time.Duration
}

func (d *delayReader) Read(p []byte) (int, error) {
	time.Sleep(d.delay)
	return d.r.Read(p)
}

func BenchmarkProxyStream_Fast(b *testing.B) {
	data := make([]byte, 1<<20)
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		src := plainReader{bytes.NewReader(data)}
		ProxyStream(src, plainWriter{io.Discard})
	}
}

func BenchmarkIOCopy_Fast(b *testing.B) {
	data := make([]byte, 1<<20)
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		src := plainReader{bytes.NewReader(data)}
		io.Copy(plainWriter{io.Discard}, src)
	}
}

func BenchmarkProxyStream_SlowWrite(b *testing.B) {
	data := make([]byte, 1<<20)
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		src := &delayReader{r: bytes.NewReader(data), delay: 50 * time.Microsecond}
		dst := &delayWriter{w: io.Discard, delay: 50 * time.Microsecond}
		ProxyStream(src, dst)
	}
}

func BenchmarkIOCopy_SlowWrite(b *testing.B) {
	data := make([]byte, 1<<20)
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		src := &delayReader{r: bytes.NewReader(data), delay: 50 * time.Microsecond}
		dst := &delayWriter{w: io.Discard, delay: 50 * time.Microsecond}
		io.Copy(dst, src)
	}
}
