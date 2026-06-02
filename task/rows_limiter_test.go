package task

import (
	"context"
	"testing"
	"time"
)

func TestRowsLimiterWaitMath(t *testing.T) {
	var got time.Duration
	sleep := func(_ context.Context, d time.Duration) error {
		got = d
		return nil
	}
	l := NewRowsLimiter(1000, sleep)
	if err := l.Wait(context.Background(), 1000); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if got != time.Second {
		t.Fatalf("expected 1s sleep, got %s", got)
	}

	got = 0
	if err := l.Wait(context.Background(), 500); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if got != 500*time.Millisecond {
		t.Fatalf("expected 500ms sleep, got %s", got)
	}
}

func TestRowsLimiterNoOp(t *testing.T) {
	called := false
	sleep := func(_ context.Context, _ time.Duration) error {
		called = true
		return nil
	}

	cases := []struct {
		name       string
		rowsPerSec int64
		rows       int64
	}{
		{"zero rowsPerSec", 0, 100},
		{"negative rowsPerSec", -1, 100},
		{"zero rows", 1000, 0},
		{"negative rows", 1000, -5},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			called = false
			l := NewRowsLimiter(tc.rowsPerSec, sleep)
			if err := l.Wait(context.Background(), tc.rows); err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if called {
				t.Fatalf("expected no sleep for %s", tc.name)
			}
		})
	}
}

func TestRowsLimiterNilReceiver(t *testing.T) {
	var l *RowsLimiter
	if err := l.Wait(context.Background(), 100); err != nil {
		t.Fatalf("nil receiver should be a no-op, got %v", err)
	}
}

func TestRowsLimiterContextCancel(t *testing.T) {
	// Use the real sleepCtx with a pre-cancelled context to assert it returns
	// ctx.Err() promptly instead of sleeping out the full duration.
	l := NewRowsLimiter(1, nil) // 1 row/sec => 10s sleep for 10 rows
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	err := l.Wait(ctx, 10)
	if err == nil {
		t.Fatal("expected context error, got nil")
	}
	if time.Since(start) > time.Second {
		t.Fatalf("expected prompt return on cancel, took %s", time.Since(start))
	}
}

func TestNewRowsLimiterFromConfigNonNil(t *testing.T) {
	if l := NewRowsLimiterFromConfig(nil); l == nil {
		t.Fatal("expected non-nil limiter for nil config")
	}
}
