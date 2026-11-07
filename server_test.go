package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

type pipeListener struct {
	ch        chan net.Conn
	done      chan struct{}
	closeOnce sync.Once
}

func newPipeListener() *pipeListener {
	return &pipeListener{
		ch:   make(chan net.Conn),
		done: make(chan struct{}),
	}
}

func (l *pipeListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.ch:
		return c, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}

func (l *pipeListener) Close() error {
	l.closeOnce.Do(func() {
		close(l.done)
	})
	return nil
}

func (l *pipeListener) Addr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 8080}
}

func (l *pipeListener) Dial() (net.Conn, error) {
	c1, c2 := net.Pipe()
	select {
	case l.ch <- c1:
		return c2, nil
	case <-l.done:
		_ = c1.Close()
		_ = c2.Close()
		return nil, net.ErrClosed
	}
}

func readUntil(t *testing.T, r *bufio.Reader, substr string) string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		line, err := r.ReadString('\n')
		if err != nil {
			t.Fatalf("failed reading expected %q: %v", substr, err)
		}
		if strings.Contains(line, substr) {
			return strings.TrimSpace(line)
		}
	}
	t.Fatalf("timed out waiting for %q", substr)
	return ""
}

func TestChatServer(t *testing.T) {
	ln := newPipeListener()
	srv := NewServer(":0")
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Serve(ln)
	}()

	conn1, err := ln.Dial()
	if err != nil {
		t.Fatalf("dial 1 failed: %v", err)
	}
	defer conn1.Close()
	r1 := bufio.NewReader(conn1)

	conn2, err := ln.Dial()
	if err != nil {
		t.Fatalf("dial 2 failed: %v", err)
	}
	defer conn2.Close()
	r2 := bufio.NewReader(conn2)

	readUntil(t, r1, "joined room #general")
	readUntil(t, r2, "joined room #general")

	// Set nicknames
	fmt.Fprintln(conn1, "/nick alice")
	readUntil(t, r1, "nickname changed to alice")

	fmt.Fprintln(conn2, "/nick bob")
	readUntil(t, r2, "nickname changed to bob")

	// Room messaging in #general
	fmt.Fprintln(conn1, "hello bob")
	readUntil(t, r2, "[#general] alice: hello bob")

	// Join separate room and verify isolation + global broadcast
	fmt.Fprintln(conn1, "/join golang")
	readUntil(t, r1, "joined room #golang")
	readUntil(t, r2, "[room:general] alice left the room")

	fmt.Fprintln(conn1, "/all announcement to everyone")
	readUntil(t, r2, "[broadcast] alice: announcement to everyone")

	// Graceful shutdown notifies connected clients
	go srv.Shutdown()
	readUntil(t, r1, "server is shutting down")
	readUntil(t, r2, "server is shutting down")

	if err := <-errCh; err != nil {
		t.Fatalf("server returned error: %v", err)
	}
}
