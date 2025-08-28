package retry

import (
	"math"
	"math/rand"
	"time"
)

type Strategy interface {
	Duration(attempt int) time.Duration
}

type ExponentialStrategy struct {
	Min       time.Duration
	Max       time.Duration
	MaxJitter time.Duration // 在基础时间上增加的随机抖动，防止多个任务同时重试造成“雪崩”
}

func (e *ExponentialStrategy) Duration(attempt int) time.Duration {
	var jitter time.Duration // non-negative jitter
	if e.MaxJitter > 0 {
		// 随机生成 0～MaxJitter 之间的时间作为抖动
		jitter = time.Duration(rand.Int63n(e.MaxJitter.Nanoseconds()))
	}
	if attempt < 0 {
		return e.Min + jitter
	}
	// 基础等待时间 = Min + 2^attempt 秒
	durFloat := float64(e.Min)
	durFloat += math.Pow(2, float64(attempt)) * float64(time.Second)
	dur := time.Duration(durFloat)
	// 如果超过了最大值，就限制在 Max
	if durFloat > float64(e.Max) {
		dur = e.Max
	}
	// 最终加上随机抖动
	dur += jitter

	return dur
}

func Exponential() Strategy {
	return &ExponentialStrategy{
		Min:       0,
		Max:       10 * time.Second,
		MaxJitter: 250 * time.Millisecond,
	}
}

type FixedStrategy struct {
	Dur time.Duration
}

func (f *FixedStrategy) Duration(attempt int) time.Duration {
	return f.Dur
}

func Fixed(dur time.Duration) Strategy {
	return &FixedStrategy{
		Dur: dur,
	}
}
