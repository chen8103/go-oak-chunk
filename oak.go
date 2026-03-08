package oak

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SisyphusSQ/go-oak-chunk/v3/conf"
	"github.com/SisyphusSQ/go-oak-chunk/v3/log"
	"github.com/SisyphusSQ/go-oak-chunk/v3/mysql"
	"github.com/SisyphusSQ/go-oak-chunk/v3/task"
)

type Executor struct {
	config      *conf.Config
	rateLimiter *task.RateLimiter
	writer      *mysql.Writer

	progressCallback ProgressCallback
	progressInterval time.Duration

	cancelMu sync.Mutex
	cancel   context.CancelFunc

	startedAtNanos atomic.Int64
	hasRun         atomic.Bool
}

type Option func(*Executor)

type ExecutorStatus struct {
	RowAffects   int64
	ElapsedTime  time.Duration
	CurrentSleep int64
	MaxSlaveLag  int64
	IsFinished   bool
}

type ProgressCallback func(status *ExecutorStatus)

var ErrExecutorAlreadyRun = errors.New("executor can only run once")

func WithRateLimiter(rateLimiter *task.RateLimiter) Option {
	return func(e *Executor) {
		e.rateLimiter = rateLimiter
	}
}

func WithProgressCallback(cb ProgressCallback, interval time.Duration) Option {
	return func(e *Executor) {
		e.progressCallback = cb
		e.progressInterval = interval
	}
}

func NewExecutor(config *conf.Config, opts ...Option) (*Executor, error) {
	if config == nil {
		return nil, fmt.Errorf("config is nil")
	}
	if !log.IsConfigured() {
		if err := log.New(config.Debug, log.OutputStderr); err != nil {
			return nil, fmt.Errorf("init logger failed: %w", err)
		}
	}
	if err := config.PreCheck(); err != nil {
		return nil, err
	}

	writer, err := mysql.NewWriter(config)
	if err != nil {
		return nil, err
	}

	executor := &Executor{
		config:           config,
		rateLimiter:      task.NewRateLimiterFromConfig(config),
		writer:           writer,
		progressInterval: 3 * time.Second,
	}

	for _, opt := range opts {
		opt(executor)
	}

	if executor.rateLimiter == nil {
		executor.rateLimiter = task.NewRateLimiterFromConfig(config)
	}
	if executor.progressInterval <= 0 {
		executor.progressInterval = 3 * time.Second
	}

	return executor, nil
}

func (e *Executor) Run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if e.writer == nil {
		return fmt.Errorf("executor writer is nil")
	}
	if !e.hasRun.CompareAndSwap(false, true) {
		return ErrExecutorAlreadyRun
	}

	e.startedAtNanos.Store(time.Now().UnixNano())

	runCtx, cancel := context.WithCancel(ctx)
	e.cancelMu.Lock()
	e.cancel = cancel
	e.cancelMu.Unlock()
	defer func() {
		e.cancelMu.Lock()
		e.cancel = nil
		e.cancelMu.Unlock()
		cancel()
	}()

	var callback task.ProgressCallback
	if e.progressCallback != nil {
		callback = func(snapshot *task.ProgressSnapshot) {
			if snapshot == nil {
				return
			}
			e.progressCallback(&ExecutorStatus{
				RowAffects:   snapshot.RowAffects,
				ElapsedTime:  snapshot.ElapsedTime,
				CurrentSleep: snapshot.CurrentSleep,
				MaxSlaveLag:  snapshot.MaxSlaveLag,
				IsFinished:   snapshot.IsFinished,
			})
		}
	}

	return task.Execute(runCtx, e.config, e.writer, task.RunOptions{
		RateLimiter:      e.rateLimiter,
		ProgressInterval: e.progressInterval,
		ProgressCallback: callback,
	})
}

func (e *Executor) Stop() {
	e.cancelMu.Lock()
	cancel := e.cancel
	e.cancelMu.Unlock()

	if cancel != nil {
		cancel()
	}
}

func (e *Executor) UpdateSleep(ms int64) {
	e.rateLimiter.SetSleep(ms)
}

func (e *Executor) UpdateMaxLag(lag int64) {
	e.rateLimiter.SetMaxLag(lag)
}

func (e *Executor) GetStatus() *ExecutorStatus {
	startedAt := e.startedAtNanos.Load()
	elapsed := time.Duration(0)
	if startedAt > 0 {
		elapsed = time.Since(time.Unix(0, startedAt))
	}

	return &ExecutorStatus{
		RowAffects:   e.writer.GetRowAffects(),
		ElapsedTime:  elapsed,
		CurrentSleep: e.rateLimiter.GetSleep(),
		MaxSlaveLag:  e.rateLimiter.GetCurrentLag(),
		IsFinished:   e.writer.IsFinished(),
	}
}
