package retry

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

func TestWithRetrySuccess(t *testing.T) {
	attempts := 0
	err := WithRetry(context.Background(), DefaultPolicy(), func(attempt int) error {
		attempts++
		return nil
	})
	if err != nil {
		t.Errorf("WithRetry() = %v, want nil", err)
	}
	if attempts != 1 {
		t.Errorf("attempts = %d, want 1", attempts)
	}
}

func TestWithRetryFatalError(t *testing.T) {
	attempts := 0
	fatalErr := &mysql.MySQLError{Number: 1146, Message: "table not found"}
	err := WithRetry(context.Background(), DefaultPolicy(), func(attempt int) error {
		attempts++
		return fatalErr
	})
	if !errors.Is(err, fatalErr) {
		t.Errorf("WithRetry() = %v, want %v", err, fatalErr)
	}
	if attempts != 1 {
		t.Errorf("attempts = %d, want 1 (fatal should fail immediately)", attempts)
	}
}

func TestWithRetryTransientThenSuccess(t *testing.T) {
	attempts := 0
	transientErr := &mysql.MySQLError{Number: 1213, Message: "deadlock"}

	err := WithRetry(context.Background(), Policy{
		MaxAttempts: 3,
		Base:        10 * time.Millisecond,
		Cap:         50 * time.Millisecond,
		Jitter:      false,
	}, func(attempt int) error {
		attempts++
		if attempt < 2 {
			return transientErr
		}
		return nil
	})

	if err != nil {
		t.Errorf("WithRetry() = %v, want nil", err)
	}
	if attempts != 2 {
		t.Errorf("attempts = %d, want 2", attempts)
	}
}

func TestWithRetryContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := WithRetry(ctx, DefaultPolicy(), func(attempt int) error {
		return nil
	})

	if !errors.Is(err, context.Canceled) {
		t.Errorf("WithRetry() = %v, want context.Canceled", err)
	}
}

func TestWithRetryTransientExhausted(t *testing.T) {
	attempts := 0
	transientErr := &mysql.MySQLError{Number: 1205, Message: "lock wait timeout"}

	err := WithRetry(context.Background(), Policy{
		MaxAttempts: 2,
		Base:        1 * time.Millisecond,
		Cap:         10 * time.Millisecond,
		Jitter:      false,
	}, func(attempt int) error {
		attempts++
		return transientErr
	})

	if !errors.Is(err, transientErr) {
		t.Errorf("WithRetry() = %v, want transientErr", err)
	}
	if attempts != 2 {
		t.Errorf("attempts = %d, want 2", attempts)
	}
}
