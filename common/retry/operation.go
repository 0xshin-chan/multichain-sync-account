package retry

import (
	"context"
	"fmt"
	"time"
)

// ErrFailedPermanently 永久性的错误，表示在操作达到最大重试次数后仍然失败
type ErrFailedPermanently struct {
	attempts int   // 重试次数
	LastErr  error // 最后一次的错误
}

// 实现 Go 标准库的 Error 接口
func (e *ErrFailedPermanently) Error() string {
	return fmt.Sprintf("operation failed permanently after %d attempts: %v", e.attempts, e.LastErr)
}

func (e *ErrFailedPermanently) Unwrap() error {
	return e.LastErr
}

// 泛型 pair， 用于返回两个值 (T, U)
type pair[T, U any] struct {
	a T
	b U
}

// Do2 对返回两个值 + 错误的函数的重试封装
func Do2[T, U any](ctx context.Context, maxAttempts int, strategy Strategy, op func() (T, U, error)) (T, U, error) {
	// 包装 op， 变成返回 pair + error 的函数
	f := func() (pair[T, U], error) {
		a, b, err := op()
		return pair[T, U]{a, b}, err
	}
	// 调用单值版的 Do 来处理
	res, err := Do(ctx, maxAttempts, strategy, f)
	return res.a, res.b, err
}

// Do 对返回单个值 + 错误的函数的重试封装
func Do[T any](ctx context.Context, maxAttempts int, strategy Strategy, op func() (T, error)) (T, error) {
	var empty, ret T
	var err error
	// 如果重试次数小于 1， 报错
	if maxAttempts < 1 {
		return empty, fmt.Errorf("need at least 1 attempt to run op, but have %d max attempts", maxAttempts)
	}

	// 循环执行，直到成功或到最大重试次数
	for i := 0; i < maxAttempts; i++ {
		// 如果 context 已经取消/超时，直接返回
		if ctx.Err() != nil {
			return empty, ctx.Err()
		}
		ret, err = op()
		if err == nil {
			return ret, nil
		}
		// 如果重试，根据策略 sleep
		if i != maxAttempts-1 {
			time.Sleep(strategy.Duration(i))
		}
	}
	// 最终失败，返回 ErrFailedPermanently
	return empty, &ErrFailedPermanently{
		attempts: maxAttempts,
		LastErr:  err,
	}
}
