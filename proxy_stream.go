package socks5

import (
	"errors"
	"fmt"
	"io"
	"sync"
)

type closeWriter interface {
	CloseWrite() error
}

type closeReader interface {
	CloseRead() error
}

// Ring buffer size. Must be a power of two so that `x & mask` is equivalent
// to `x % proxyBufSize` (see ProxyStream for the indexing scheme).
const (
	proxyBufSize uint64 = 64 << 10
	proxyBufMask        = proxyBufSize - 1
)

// proxyBufPool recycles the per-connection 64 KiB ring buffers. The buffer
// dominates ProxyStream's allocation footprint, and under sustained load
// each connection would otherwise force a fresh 64 KiB heap allocation.
// Storing a pointer to a fixed-size array (rather than a slice) avoids the
// extra slice-header allocation that sync.Pool would otherwise introduce.
var proxyBufPool = sync.Pool{
	New: func() any {
		return new([proxyBufSize]byte)
	},
}

// ProxyStream forwards data from src to dst, similar to io.Copy, but with improved performance.
// Unlike io.Copy’s sequential read/write model, it allows reads to continue while writes are in progress,
// using a single 64 KiB ring buffer shared between the reader and writer.
// ProxyStream closes both the read and write sides when the transfer completes.
func ProxyStream(src io.Reader, dst io.Writer) error {
	// head and tail are monotonically increasing byte counters, not positions
	// inside buf. Indexing into buf is done via `& proxyBufMask`, which is
	// equivalent to `% proxyBufSize` but cheaper — this requires proxyBufSize
	// to be a power of two. With this scheme, `head - tail` directly yields
	// the number of occupied bytes regardless of wrap, and we never need to
	// reset the counters.
	bufArr := proxyBufPool.Get().(*[proxyBufSize]byte)
	defer proxyBufPool.Put(bufArr)
	buf := bufArr[:]
	var (
		head     uint64 // total bytes written into buf by reader
		tail     uint64 // total bytes consumed from buf by writer
		mu       sync.Mutex
		readDone bool
		writeErr error
	)
	cond := sync.NewCond(&mu)

	writerDone := make(chan struct{})

	// Writer goroutine: drains [tail, head) from the ring buffer into dst.
	go func() {
		defer close(writerDone)
		for {
			mu.Lock()
			for head == tail && !readDone {
				cond.Wait()
			}
			if head == tail {
				// Reader finished and buffer drained.
				mu.Unlock()
				return
			}

			// Contiguous readable region starting at rIdx. When the occupied
			// range wraps past proxyBufSize, only the first segment
			// [rIdx, proxyBufSize) is taken here; the remainder [0, hIdx) is
			// handled on the next iteration once tail has advanced past the
			// wrap point.
			rIdx := tail & proxyBufMask
			hIdx := head & proxyBufMask
			var data []byte
			if hIdx > rIdx {
				data = buf[rIdx:hIdx]
			} else {
				data = buf[rIdx:proxyBufSize]
			}
			mu.Unlock()

			n, err := dst.Write(data)

			mu.Lock()
			tail += uint64(n)
			cond.Signal()
			if err != nil {
				writeErr = err
				mu.Unlock()
				// Unblock reader if it’s stuck on src.Read.
				if cr, ok := src.(closeReader); ok {
					_ = cr.CloseRead()
				}
				return
			}
			mu.Unlock()
		}
	}()

	// Reader loop: fills [head, tail+proxyBufSize) in the ring buffer from src.
	var readErr error
	for {
		mu.Lock()
		for head-tail == proxyBufSize && writeErr == nil {
			cond.Wait()
		}
		if writeErr != nil {
			mu.Unlock()
			break
		}

		// Contiguous writable region starting at wIdx. Mirrors the writer
		// side: if the free range wraps, only [wIdx, proxyBufSize) is taken
		// here and [0, tIdx) is picked up on the next iteration after head
		// wraps.
		wIdx := head & proxyBufMask
		tIdx := tail & proxyBufMask
		var space []byte
		if tIdx > wIdx {
			space = buf[wIdx:tIdx]
		} else {
			space = buf[wIdx:proxyBufSize]
		}
		mu.Unlock()

		n, err := src.Read(space)

		mu.Lock()
		head += uint64(n)
		if err != nil {
			readDone = true
			if !errors.Is(err, io.EOF) {
				readErr = err
			}
			cond.Signal()
			mu.Unlock()
			break
		}
		if n > 0 {
			cond.Signal()
		}
		mu.Unlock()
	}

	<-writerDone

	// Close both sides.
	if cr, ok := src.(closeReader); ok {
		_ = cr.CloseRead()
	}
	if cw, ok := dst.(closeWriter); ok {
		_ = cw.CloseWrite()
	}

	if readErr != nil {
		readErr = fmt.Errorf("read error: %v", readErr)
	}
	if writeErr != nil {
		writeErr = fmt.Errorf("write error: %v", writeErr)
	}

	return errors.Join(writeErr, readErr)
}
