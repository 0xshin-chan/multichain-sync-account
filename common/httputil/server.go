package httputil

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync/atomic"
)

// HTTPServer wraps a http.Server, while providing conveniences
// like exposing the running state and address.
// 封装了标准库的 http.Server， 增加了 监听， 保存运行状态的功能
type HTTPServer struct {
	listener net.Listener
	srv      *http.Server
	closed   atomic.Bool
}

// HTTPOption applies a change to an HTTP server
type HTTPOption func(srv *HTTPServer) error

func StartHTTPServer(addr string, handler http.Handler, opts ...HTTPOption) (*HTTPServer, error) {
	// 1. 打开一个 TCP 监听器
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to bind to address %q: %w", addr, err)
	}

	// 2. 用 context 控制 server 生命周期
	srvCtx, srvCancel := context.WithCancel(context.Background())
	// 3. 配置 http.Server
	srv := &http.Server{
		Handler:           handler,
		ReadTimeout:       DefaultTimeouts.ReadTimeout,
		ReadHeaderTimeout: DefaultTimeouts.ReadHeaderTimeout,
		WriteTimeout:      DefaultTimeouts.WriteTimeout,
		IdleTimeout:       DefaultTimeouts.IdleTimeout,
		BaseContext: func(listener net.Listener) context.Context {
			return srvCtx
		},
	}
	// 4. 封装成自定义的 HTTPServer 结构体
	out := &HTTPServer{listener: listener, srv: srv}
	for _, opt := range opts {
		if err := opt(out); err != nil {
			srvCancel()
			return nil, errors.Join(fmt.Errorf("failed to apply HTTP option: %w", err), listener.Close())
		}
	}
	// 6. 后台 goroutine 启动 server
	go func() {
		err := out.srv.Serve(listener) // Serve 会阻塞，直到 server 关闭， Serve 一直在监听端口，不停接收请求
		srvCancel()                    // server 退出时，取消 context
		// no error, unless ErrServerClosed (or unused base context closes, or unused http2 config error)
		if errors.Is(err, http.ErrServerClosed) {
			out.closed.Store(true)
		} else {
			panic(fmt.Errorf("unexpected serve error: %w", err))
		}
	}()
	return out, nil
}

func (s *HTTPServer) Closed() bool {
	return s.closed.Load()
}

// Stop is a convenience method to gracefully shut down the server, but force-close if the ctx is cancelled.
// The ctx error is not returned when the force-close is successful.
// - 先调用 Shutdown()（优雅关闭）
// - 如果失败则调用 Close()（强制关闭）
func (s *HTTPServer) Stop(ctx context.Context) error {
	if err := s.Shutdown(ctx); err != nil {
		if errors.Is(err, ctx.Err()) { // force-close connections if we cancelled the stopping
			return s.Close()
		}
		return err
	}
	return nil
}

// Shutdown shuts down the HTTP server and its listener,
// but allows active connections to close gracefully.
// If the function exits due to a ctx cancellation the listener is closed but active connections may remain,
// a call to Close() can force-close any remaining active connections.
func (s *HTTPServer) Shutdown(ctx context.Context) error {
	// closes the underlying listener too.
	return s.srv.Shutdown(ctx)
}

// Close force-closes the HTTPServer, its listener, and all its active connections.
func (s *HTTPServer) Close() error {
	// closes the underlying listener too
	return s.srv.Close()
}

func (s *HTTPServer) Addr() net.Addr {
	return s.listener.Addr()
}

func WithMaxHeaderBytes(max int) HTTPOption {
	return func(srv *HTTPServer) error {
		srv.srv.MaxHeaderBytes = max
		return nil
	}
}
