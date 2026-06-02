package task

import (
	"context"
	"time"

	"github.com/SisyphusSQ/go-oak-chunk/v3/conf"
)

// SleepFunc 是可注入的、感知 context 的睡眠函数(便于测试)。
type SleepFunc func(context.Context, time.Duration) error

// RowsLimiter 是行级限流器: 每处理 rows 行就等待 rows/rowsPerSec 秒,
// 把整体速率压到 rowsPerSec 行/秒。rowsPerSec<=0 表示不限流。
type RowsLimiter struct {
	rowsPerSec int64
	sleep      SleepFunc
}

// NewRowsLimiter 构造一个行级限流器; sleep 为 nil 时退化为内置的 ctx 感知定时器。
func NewRowsLimiter(rowsPerSec int64, sleep SleepFunc) *RowsLimiter {
	if sleep == nil {
		sleep = sleepCtx
	}
	return &RowsLimiter{rowsPerSec: rowsPerSec, sleep: sleep}
}

// NewRowsLimiterFromConfig 从配置构造一个非 nil 的限流器。
// 始终返回非 nil, 避免把 typed-nil 装进接口造成的调用陷阱。
func NewRowsLimiterFromConfig(c *conf.Config) *RowsLimiter {
	var rowsPerSec int64
	if c != nil {
		rowsPerSec = c.RowsPerSec
	}
	return NewRowsLimiter(rowsPerSec, nil)
}

// Wait 等待 rows/rowsPerSec 秒。当 l==nil、rowsPerSec<=0 或 rows<=0 时为空操作。
// 等待期间 context 取消则返回 ctx.Err()。
func (l *RowsLimiter) Wait(ctx context.Context, rows int64) error {
	if l == nil || l.rowsPerSec <= 0 || rows <= 0 {
		return nil
	}
	d := time.Duration(float64(rows) / float64(l.rowsPerSec) * float64(time.Second))
	if d <= 0 {
		return nil
	}
	return l.sleep(ctx, d)
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
