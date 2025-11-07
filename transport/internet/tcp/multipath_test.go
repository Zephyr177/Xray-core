package tcp

import (
	"context"
	"io"
	"net"
	"testing"
	"time"
)

// mockConn 模拟网络连接用于测试
type mockConn struct {
	readData  []byte
	readPos   int
	writeData []byte
	closed    bool
}

func (m *mockConn) Read(b []byte) (n int, err error) {
	if m.closed {
		return 0, io.EOF
	}
	if m.readPos >= len(m.readData) {
		time.Sleep(10 * time.Millisecond)
		return 0, io.EOF
	}
	n = copy(b, m.readData[m.readPos:])
	m.readPos += n
	return n, nil
}

func (m *mockConn) Write(b []byte) (n int, err error) {
	if m.closed {
		return 0, io.ErrClosedPipe
	}
	m.writeData = append(m.writeData, b...)
	return len(b), nil
}

func (m *mockConn) Close() error {
	m.closed = true
	return nil
}

func (m *mockConn) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1234}
}

func (m *mockConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 5678}
}

func (m *mockConn) SetDeadline(t time.Time) error      { return nil }
func (m *mockConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *mockConn) SetWriteDeadline(t time.Time) error { return nil }

func TestMultiPathConfig(t *testing.T) {
	config := &MultiPathConfig{
		Enabled:         true,
		ConnectionCount: 4,
	}

	if !config.GetEnabled() {
		t.Error("Expected enabled to be true")
	}

	if config.GetConnectionCount() != 4 {
		t.Errorf("Expected connection count to be 4, got %d", config.GetConnectionCount())
	}
}

func TestMultiPathConnBasic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 创建模拟连接
	conns := []net.Conn{
		&mockConn{readData: []byte("test data 1")},
		&mockConn{readData: []byte("test data 2")},
	}

	mpc := &MultiPathConn{
		conns:      conns,
		readBuffer: make(chan []byte, 4),
		readError:  make(chan error, 2),
		ctx:        ctx,
		cancel:     cancel,
	}

	// 测试写入
	data := []byte("hello world")
	n, err := mpc.Write(data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != len(data) {
		t.Errorf("Expected to write %d bytes, wrote %d", len(data), n)
	}

	// 测试地址
	if mpc.LocalAddr() == nil {
		t.Error("LocalAddr should not be nil")
	}
	if mpc.RemoteAddr() == nil {
		t.Error("RemoteAddr should not be nil")
	}

	// 测试关闭
	err = mpc.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}

	// 验证连接已关闭
	if !mpc.closed.Load() {
		t.Error("Connection should be marked as closed")
	}
}

func TestMultiPathConnWrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 创建多个模拟连接
	conns := []net.Conn{
		&mockConn{},
		&mockConn{},
		&mockConn{},
	}

	mpc := &MultiPathConn{
		conns:      conns,
		readBuffer: make(chan []byte, 4),
		readError:  make(chan error, 3),
		ctx:        ctx,
		cancel:     cancel,
	}

	// 写入多次数据，测试轮询
	for i := 0; i < 10; i++ {
		data := []byte("test")
		_, err := mpc.Write(data)
		if err != nil {
			t.Fatalf("Write %d failed: %v", i, err)
		}
	}

	// 验证数据分布到不同连接
	totalWritten := 0
	for _, conn := range conns {
		mc := conn.(*mockConn)
		if len(mc.writeData) > 0 {
			totalWritten += len(mc.writeData)
		}
	}

	if totalWritten != 40 { // 10 * 4 bytes
		t.Errorf("Expected total written 40 bytes, got %d", totalWritten)
	}

	mpc.Close()
}

func TestMultiPathConnClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conns := []net.Conn{
		&mockConn{},
		&mockConn{},
	}

	mpc := &MultiPathConn{
		conns:      conns,
		readBuffer: make(chan []byte, 4),
		readError:  make(chan error, 2),
		ctx:        ctx,
		cancel:     cancel,
	}

	// 第一次关闭
	err := mpc.Close()
	if err != nil {
		t.Errorf("First close failed: %v", err)
	}

	// 第二次关闭应该不报错
	err = mpc.Close()
	if err != nil {
		t.Errorf("Second close should not error: %v", err)
	}

	// 验证所有连接都已关闭
	for i, conn := range conns {
		mc := conn.(*mockConn)
		if !mc.closed {
			t.Errorf("Connection %d should be closed", i)
		}
	}
}

func TestConfigWithMultiPath(t *testing.T) {
	config := &Config{
		MultiPath: &MultiPathConfig{
			Enabled:         true,
			ConnectionCount: 8,
		},
	}

	mp := config.GetMultiPath()
	if mp == nil {
		t.Fatal("MultiPath config should not be nil")
	}

	if !mp.GetEnabled() {
		t.Error("MultiPath should be enabled")
	}

	if mp.GetConnectionCount() != 8 {
		t.Errorf("Expected 8 connections, got %d", mp.GetConnectionCount())
	}
}

func TestMultiPathConnDeadlines(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conns := []net.Conn{
		&mockConn{},
		&mockConn{},
	}

	mpc := &MultiPathConn{
		conns:      conns,
		readBuffer: make(chan []byte, 4),
		readError:  make(chan error, 2),
		ctx:        ctx,
		cancel:     cancel,
	}
	defer mpc.Close()

	deadline := time.Now().Add(1 * time.Second)

	// 测试 SetDeadline
	err := mpc.SetDeadline(deadline)
	if err != nil {
		t.Errorf("SetDeadline failed: %v", err)
	}

	// 测试 SetReadDeadline
	err = mpc.SetReadDeadline(deadline)
	if err != nil {
		t.Errorf("SetReadDeadline failed: %v", err)
	}

	// 测试 SetWriteDeadline
	err = mpc.SetWriteDeadline(deadline)
	if err != nil {
		t.Errorf("SetWriteDeadline failed: %v", err)
	}
}
