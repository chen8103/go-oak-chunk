package retry

import (
	"context"
	"math/rand"
	"time"
)

type Policy struct {
	MaxAttempts int           // total attempts including the first
	Base        time.Duration // first backoff
	Cap         time.Duration // max backoff
	Jitter      bool          // apply +/-20% random jitter
}

// DefaultPolicy returns the policy tuned for the chunk loop.
func DefaultPolicy() Policy {
	return Policy{
		MaxAttempts: 4,
		Base:        200 * time.Millisecond,
		Cap:         2 * time.Second,
		Jitter:      true,
	}
}

// WithRetry executes fn up to p.MaxAttempts times, backing off only on
// transient errors. It returns immediately on fatal or canceled errors. The
// 1-based attempt index is passed to fn for logging.
func WithRetry(ctx context.Context, p Policy, fn func(attempt int) error) error {
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = 1
	}

	var lastErr error
	for attempt := 1; attempt <= p.MaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		err := fn(attempt)
		if err == nil {
			return nil
		}
		lastErr = err

		if Classify(err) != ClassTransient {
			return err
		}
		if attempt == p.MaxAttempts {
			return err
		}

		wait := backoff(p, attempt)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
	return lastErr
}

func backoff(p Policy, attempt int) time.Duration {
	d := p.Base << (attempt - 1)
	if d <= 0 || d > p.Cap {
		d = p.Cap
	}

	if p.Jitter && d > 0 {
		j := time.Duration(rand.Int63n(int64(d)/5 + 1))
		if rand.Intn(2) == 0 {
			d -= j
		} else {
			d += j
		}
	}
	return d
}
