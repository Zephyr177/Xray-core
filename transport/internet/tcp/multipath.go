package tcp

import (
	"context"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xtls/xray-core/common/errors"
	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/transport/internet"
)

// MultiPathConn 实现多路径连接聚合
type MultiPathConn struct {
	conns       []net.Conn
	readIndex   uint32
	writeIndex  uint32
	closed      atomic.Bool
	closeMutex  sync.Mutex
	readBuffer  chan []byte
	readError   chan error
	ctx         context.Context
	cancel      context.CancelFunc
}

// NewMultiPathConn 创建一个新的多路径连接
func NewMultiPathConn(ctx context.Context, dest xnet.Destination, streamSettings *internet.MemoryStreamConfig, count uint32) (*MultiPathConn, error) {
	if count < 1 {
		count = 1
	}
	if count > 16 {
		count = 16 // 限制最大连接数
	}

	conns := make([]net.Conn, 0, count)
	ctx, cancel := context.WithCancel(ctx)

	// 创建多个连接
	for i := uint32(0); i < count; i++ {
		conn, err := internet.DialSystem(ctx, dest, streamSettings.SocketSettings)
		if err != nil {
			// 如果创建连接失败，关闭已创建的连接
			for _, c := range conns {
				c.Close()
			}
			cancel()
			return nil, errors.New("failed to create connection ", i).Base(err)
		}
		conns = append(conns, conn)
	}

	mpc := &MultiPathConn{
		conns:      conns,
		readBuffer: make(chan []byte, count*2),
		readError:  make(chan error, count),
		ctx:        ctx,
		cancel:     cancel,
	}

	// 启动读取协程
	for i, conn := range conns {
		go mpc.readLoop(conn, i)
	}

	return mpc, nil
}

// readLoop 从单个连接读取数据
func (m *MultiPathConn) readLoop(conn net.Conn, index int) {
	buffer := make([]byte, 32*1024) // 32KB 缓冲区
	for {
		select {
		case <-m.ctx.Done():
			return
		default:
		}

		n, err := conn.Read(buffer)
		if err != nil {
			if !m.closed.Load() {
				select {
				case m.readError <- err:
				case <-m.ctx.Done():
				}
			}
			return
		}

		if n > 0 {
			data := make([]byte, n)
			copy(data, buffer[:n])
			select {
			case m.readBuffer <- data:
			case <-m.ctx.Done():
				return
			}
		}
	}
}

// Read 从多路径连接读取数据
func (m *MultiPathConn) Read(b []byte) (int, error) {
	if m.closed.Load() {
		return 0, io.EOF
	}

	select {
	case data := <-m.readBuffer:
		n := copy(b, data)
		return n, nil
	case err := <-m.readError:
		return 0, err
	case <-m.ctx.Done():
		return 0, io.EOF
	}
}

// Write 向多路径连接写入数据（轮询方式）
func (m *MultiPathConn) Write(b []byte) (int, error) {
	if m.closed.Load() {
		return 0, io.ErrClosedPipe
	}

	// 使用轮询方式选择连接
	index := atomic.AddUint32(&m.writeIndex, 1) % uint32(len(m.conns))
	conn := m.conns[index]

	return conn.Write(b)
}

// Close 关闭所有连接
func (m *MultiPathConn) Close() error {
	if m.closed.Swap(true) {
		return nil
	}

	m.closeMutex.Lock()
	defer m.closeMutex.Unlock()

	m.cancel()

	var errs []error
	for _, conn := range m.conns {
		if err := conn.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return errors.New("failed to close some connections: ", errs[0])
	}

	return nil
}

// LocalAddr 返回本地地址
func (m *MultiPathConn) LocalAddr() net.Addr {
	if len(m.conns) > 0 {
		return m.conns[0].LocalAddr()
	}
	return nil
}

// RemoteAddr 返回远程地址
func (m *MultiPathConn) RemoteAddr() net.Addr {
	if len(m.conns) > 0 {
		return m.conns[0].RemoteAddr()
	}
	return nil
}

// SetDeadline 设置读写超时
func (m *MultiPathConn) SetDeadline(t time.Time) error {
	var errs []error
	for _, conn := range m.conns {
		if err := conn.SetDeadline(t); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

// SetReadDeadline 设置读超时
func (m *MultiPathConn) SetReadDeadline(t time.Time) error {
	var errs []error
	for _, conn := range m.conns {
		if err := conn.SetReadDeadline(t); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

// SetWriteDeadline 设置写超时
func (m *MultiPathConn) SetWriteDeadline(t time.Time) error {
	var errs []error
	for _, conn := range m.conns {
		if err := conn.SetWriteDeadline(t); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}
