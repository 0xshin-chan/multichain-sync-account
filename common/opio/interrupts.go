package opio

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

// DefaultInterruptSignals 定义需要捕捉的系统信号
var DefaultInterruptSignals = []os.Signal{
	os.Interrupt,    // ctrl + c
	os.Kill,         // kill -9 实际上 Go 并不能捕捉 sigkill 定义了但是不会生效
	syscall.SIGTERM, //	kill
	syscall.SIGQUIT, // ctrl + \ 退出
}

// BlockOnInterrupts 会阻塞当前 goroutine 知道收到其中一个信号
func BlockOnInterrupts(signals ...os.Signal) {
	if len(signals) == 0 {
		signals = DefaultInterruptSignals
	}
	interruptChannel := make(chan os.Signal, 1) // 信号通道
	signal.Notify(interruptChannel, signals...) // 监听信号
	<-interruptChannel                          // 阻塞，直到收到信号
}

// BlockOnInterruptsContext 和上面类似，增加了 Context 控制，可以提前退出阻塞
func BlockOnInterruptsContext(ctx context.Context, signals ...os.Signal) {
	if len(signals) == 0 {
		signals = DefaultInterruptSignals
	}
	interruptChannel := make(chan os.Signal, 1)
	signal.Notify(interruptChannel, signals...)
	select {
	case <-interruptChannel: // 收到信号时退出
	case <-ctx.Done():
		signal.Stop(interruptChannel) // context 被取消时退出
	}
}

// 自定义的 key 类型，用于在 context 里存储 BlockFn
type interruptContextKeyType struct{}

var blockerContextKey = interruptContextKeyType{}

// 信号捕捉器
type interruptCatcher struct {
	incoming chan os.Signal
}

func (c *interruptCatcher) Block(ctx context.Context) {
	select {
	case <-c.incoming:
	case <-ctx.Done():
	}
}

// WithInterruptBlocker 给 context 增加一个信号阻塞器
func WithInterruptBlocker(ctx context.Context) context.Context {
	if ctx.Value(blockerContextKey) != nil { // already has an interrupt handler
		return ctx
	}
	catcher := &interruptCatcher{
		incoming: make(chan os.Signal, 10),
	}
	// Notify 第二个参数是可变参，用 ... 将切片 DefaultInterruptSignals 里面所有的信号都展开全量传入
	signal.Notify(catcher.incoming, DefaultInterruptSignals...)

	// 存一个 BlockFn 函数到 context 里
	return context.WithValue(ctx, blockerContextKey, BlockFn(catcher.Block))
}

func WithBlocker(ctx context.Context, fn BlockFn) context.Context {
	return context.WithValue(ctx, blockerContextKey, fn)
}

// BlockFn 是一个函数类型，表示阻塞函数
type BlockFn func(ctx context.Context)

// BlockerFromContext 取出 BlockFn 函数
func BlockerFromContext(ctx context.Context) BlockFn {
	v := ctx.Value(blockerContextKey)
	if v == nil {
		return nil
	}
	return v.(BlockFn) // 类型断言
}

// CancelOnInterrupt 返回一个新的 context
// 当收到中断信号时，会调用 cancel() 自动取消
func CancelOnInterrupt(ctx context.Context) context.Context {
	inner, cancel := context.WithCancel(ctx)

	// 从 context 中取出阻塞函数
	blockOnInterrupt := BlockerFromContext(ctx)
	if blockOnInterrupt == nil {
		// 如果没有，用默认阻塞方式
		blockOnInterrupt = func(ctx context.Context) {
			BlockOnInterruptsContext(ctx) // default signals
		}
	}

	// 启动一个 goroutine 监听信号
	go func() {
		blockOnInterrupt(ctx) // 阻塞直到收到信号
		cancel()              // 收到信号后取消 context
	}()

	return inner
}
