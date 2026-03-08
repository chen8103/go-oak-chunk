package task

import (
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/juju/ratelimit"

	"github.com/SisyphusSQ/go-oak-chunk/v3/conf"
)

type RateLimiter struct {
	sleep         atomic.Int64
	maxLag        atomic.Int64
	correct       atomic.Int64
	currentLag    atomic.Int64
	noConsiderLag atomic.Bool
	bucket        *ratelimit.Bucket
}

func NewRateLimiter(sleep, maxLag, correct int64, noConsiderLag bool) *RateLimiter {
	rl := &RateLimiter{
		bucket: ratelimit.NewBucketWithQuantum(1*time.Millisecond, 1, 1),
	}
	rl.sleep.Store(clampNonNegative(sleep))
	rl.maxLag.Store(clampNonNegative(maxLag))
	rl.correct.Store(clampNonNegative(correct))
	rl.noConsiderLag.Store(noConsiderLag)
	return rl
}

func NewRateLimiterFromConfig(c *conf.Config) *RateLimiter {
	return NewRateLimiter(c.Sleep, c.MaxLag, c.Correct, c.NoConsiderLag)
}

func (r *RateLimiter) Bucket() *ratelimit.Bucket {
	return r.bucket
}

func (r *RateLimiter) SetSleep(ms int64) {
	r.sleep.Store(clampNonNegative(ms))
}

func (r *RateLimiter) SetMaxLag(lag int64) {
	r.maxLag.Store(clampNonNegative(lag))
}

func (r *RateLimiter) GetSleep() int64 {
	return r.sleep.Load()
}

func (r *RateLimiter) GetMaxLag() int64 {
	return r.maxLag.Load()
}

func (r *RateLimiter) SetNoConsiderLag(enabled bool) {
	r.noConsiderLag.Store(enabled)
}

func (r *RateLimiter) GetNoConsiderLag() bool {
	return r.noConsiderLag.Load()
}

func (r *RateLimiter) GetCorrect() int64 {
	return r.correct.Load()
}

func (r *RateLimiter) AddCorrect(delta int64) int64 {
	return r.correct.Add(delta)
}

func (r *RateLimiter) DecayCorrect(maxCorrect int64) {
	for {
		old := r.correct.Load()
		if old <= maxCorrect {
			return
		}
		if r.correct.CompareAndSwap(old, old-1) {
			return
		}
	}
}

func (r *RateLimiter) SetCurrentLag(lag int64) {
	r.currentLag.Store(lag)
}

func (r *RateLimiter) GetCurrentLag() int64 {
	return r.currentLag.Load()
}

func (r *RateLimiter) ShouldThrottle(lag int64) bool {
	maxLag := r.maxLag.Load()
	return maxLag > 0 && lag >= maxLag
}

func (r *RateLimiter) bucketHandle(lag int64) int64 {
	sleep := r.sleep.Load()
	x := sleep / 1000
	if lag == 0 && sleep > 0 {
		if sleep <= 1000 {
			return rand.Int63n(sleep)
		}
		return rand.Int63n(1000) + (sleep - 1000)
	}

	if lag == 0 && sleep == 0 {
		return 0
	}

	if r.noConsiderLag.Load() {
		if lag <= x {
			return lag * 1000
		}
		return sleep
	}

	if lag <= x || lag+60 <= x {
		return lag * 1000
	}

	plus := (lag - x) / 60
	return (x + plus) * 1000
}

func (r *RateLimiter) bucketErrHandle() int64 {
	sleep := r.sleep.Load()
	if sleep <= 0 {
		return 0
	}
	if sleep <= 1000 {
		return rand.Int63n(sleep)
	}

	return rand.Int63n(1000) + (sleep - 1000)
}

func clampNonNegative(v int64) int64 {
	if v < 0 {
		return 0
	}
	return v
}
