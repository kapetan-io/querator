package daemon

import (
	"io"
	"net"
	"sync/atomic"
)

// InMemoryListener is intended to be used with testing/synctest
type InMemoryListener struct {
	closed   chan struct{}
	connCh   chan net.Conn
	isClosed atomic.Bool
}

func NewInMemoryListener() *InMemoryListener {
	return &InMemoryListener{
		connCh: make(chan net.Conn),
		closed: make(chan struct{}),
	}
}

func (l *InMemoryListener) ServeConn(conn net.Conn) error {
	if l.isClosed.Load() {
		return net.ErrClosed
	}

	l.connCh <- conn
	return nil
}

func (l *InMemoryListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.connCh:
		return c, nil
	case <-l.closed:
		return nil, io.EOF
	}
}

func (l *InMemoryListener) Close() error {
	if !l.isClosed.CompareAndSwap(false, true) {
		return nil
	}
	close(l.closed)
	return nil
}

func (l *InMemoryListener) Addr() net.Addr {
	return memAddr("memory-listener")
}

type memAddr string

func (a memAddr) Network() string { return string(a) }
func (a memAddr) String() string  { return string(a) }
