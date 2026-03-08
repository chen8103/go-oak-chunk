package task

import (
	"context"
	"os"
	"os/exec"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SisyphusSQ/go-oak-chunk/v3/mysql"
)

func TestRunProgressCallback_ShutdownBehavior(t *testing.T) {
	tests := []struct {
		name                      string
		makeCallback              func(release <-chan struct{}) ProgressCallback
		expectDoneBeforeUnblock   bool
		needExplicitUnblockSignal bool
	}{
		{
			name: "non_blocking_callback_can_exit_normally",
			makeCallback: func(_ <-chan struct{}) ProgressCallback {
				return func(_ *ProgressSnapshot) {}
			},
			expectDoneBeforeUnblock: true,
		},
		{
			name: "blocking_callback_does_not_block_shutdown",
			makeCallback: func(release <-chan struct{}) ProgressCallback {
				return func(_ *ProgressSnapshot) {
					<-release
				}
			},
			expectDoneBeforeUnblock:   true,
			needExplicitUnblockSignal: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{}, 1)
			release := make(chan struct{})

			go runProgressCallback(
				ctx,
				NewRateLimiter(0, 0, 0, false),
				&mysql.Writer{},
				5*time.Millisecond,
				tt.makeCallback(release),
				done,
			)

			cancel()

			gotDoneBeforeUnblock := false
			select {
			case <-done:
				gotDoneBeforeUnblock = true
			case <-time.After(80 * time.Millisecond):
			}

			if gotDoneBeforeUnblock != tt.expectDoneBeforeUnblock {
				t.Fatalf("done-before-unblock = %v, want %v", gotDoneBeforeUnblock, tt.expectDoneBeforeUnblock)
			}

			if tt.needExplicitUnblockSignal {
				close(release)
			}
		})
	}
}

func TestRunProgressCallback_PanicInCallbackDoesNotCrashProcess(t *testing.T) {
	const helperEnv = "GO_OAK_CHUNK_PROGRESS_PANIC_HELPER"

	if os.Getenv(helperEnv) == "1" {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{}, 1)
		go runProgressCallback(
			ctx,
			NewRateLimiter(0, 0, 0, false),
			&mysql.Writer{},
			5*time.Millisecond,
			func(_ *ProgressSnapshot) {
				panic("progress callback panic")
			},
			done,
		)
		time.Sleep(30 * time.Millisecond)
		cancel()

		select {
		case <-done:
		case <-time.After(300 * time.Millisecond):
			os.Exit(2)
		}
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run", "^TestRunProgressCallback_PanicInCallbackDoesNotCrashProcess$")
	cmd.Env = append(os.Environ(), helperEnv+"=1")
	err := cmd.Run()
	if err != nil {
		t.Fatalf("expected subprocess success, got err: %v", err)
	}
}

func TestRunProgressCallback_BlockingCallbackSkipsTicksWithoutBacklog(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{}, 1)
	release := make(chan struct{})
	firstInvoke := make(chan struct{}, 1)
	var invokeCount atomic.Int64

	go runProgressCallback(
		ctx,
		NewRateLimiter(0, 0, 0, false),
		&mysql.Writer{},
		5*time.Millisecond,
		func(_ *ProgressSnapshot) {
			if invokeCount.Add(1) == 1 {
				select {
				case firstInvoke <- struct{}{}:
				default:
				}
			}
			<-release
		},
		done,
	)

	select {
	case <-firstInvoke:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("timeout waiting first callback invoke")
	}

	time.Sleep(80 * time.Millisecond)
	if got := invokeCount.Load(); got != 1 {
		t.Fatalf("blocking callback should skip ticks without backlog, invoke count = %d, want 1", got)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("progress callback should exit even when callback is blocked")
	}
	close(release)
}
