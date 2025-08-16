package clock

import (
	"context"
	"sync"
	"time"
)

type LoopFn struct {
	ctx    context.Context
	cancel context.CancelFunc

	ticker  Ticker
	fn      func(ctx context.Context)
	onClose func() error

	wg sync.WaitGroup
}

func (lf *LoopFn) Close() error {
	lf.cancel()  // 发出“停止信号”，让 work() 里的 select <-ctx.Done() 返回
	lf.wg.Wait() // 等待 goroutine 正常退出
	if lf.onClose != nil {
		return lf.onClose() // optional: user can specify function to close resources with
	}
	return nil
}

func (lf *LoopFn) work() {
	defer lf.wg.Done()     // 退出时减少计数，保证 wg.Wait() 不会卡住
	defer lf.ticker.Stop() // 停止定时器，避免资源泄漏
	for {
		select {
		case <-lf.ctx.Done():
			return
		case <-lf.ticker.Ch():
			ctx, cancel := context.WithCancel(lf.ctx)
			func() {
				defer cancel() // 保证 fn 结束后 ctx 被释放
				lf.fn(ctx)     // 到定时周期就执行一次
			}()
		}
	}
}

func NewLoopFn(clock Clock, fn func(ctx context.Context), onClose func() error, interval time.Duration) *LoopFn {
	ctx, cancel := context.WithCancel(context.Background())
	lf := &LoopFn{
		ctx:     ctx,
		cancel:  cancel,
		fn:      fn,
		ticker:  clock.NewTicker(interval),
		onClose: onClose,
	}
	lf.wg.Add(1) // 表示将要启动 1 个 goroutine
	go lf.work() // 开启后台循环
	return lf
}
