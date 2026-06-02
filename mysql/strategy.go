package mysql

import (
	"context"
)

// ChunkStrategy 定义了分块执行的抽象策略。
//
// P1 目标: 把现有 Procedure.BuildSQL(生产者) + Writer.Write(消费者) 的逻辑
// 收进一个具体实现里, 对外暴露统一的 Run 入口, 行为零变化, 仅做代码组织重构。
// 为后续 P2 的多策略(covering/partition)和 P3 的并发预留扩展点。
type ChunkStrategy interface {
	// Name 返回策略名称(日志/进度展示用)。示例: "RangeStrategy"。
	Name() string

	// Run 驱动一次完整的分块执行流程。
	//
	// 对标现有逻辑:
	//   - 内部启动 Procedure.BuildSQL 生产 chunk(写入 Writer.ProducerQueue)
	//   - 同时运行 Writer.Write 消费 chunk(含事务/重试/统计/限流)
	//   - 任一阶段出错即返回该错误; 全部完成返回 nil。
	//
	// 限流信号通过 bucketNum 通道传入(由 task.getStopTime 生成),
	// 进度/统计仍由内部 Writer 维护(GetRowAffects/IsFinished 等)。
	Run(ctx context.Context, params RunParams) error
}

// RunParams 是策略执行所需的运行期依赖。
// 这些依赖在 task 层编排时注入, 与具体策略无关。
type RunParams struct {
	// Bucket 是令牌桶限流器(juju/ratelimit.Bucket),
	// 用 any 以避免 mysql 包反向依赖 task 包的限流类型。
	Bucket Bucket

	// BucketNum 是限流信号通道, 由 task.getStopTime 写入。
	BucketNum <-chan int64

	// RowsLimiter 是行级限流器(rows/sec), 在每次事务/批次提交后等待。
	// 用接口而非具体类型, 让 mysql 包不直接依赖 task 包的限流实现。
	// nil 表示不限流(调用点需判空)。
	RowsLimiter RowsLimiter
}

// Bucket 抽象令牌桶的最小接口(对标 juju/ratelimit.Bucket.Wait)。
// 用接口而非具体类型, 让 mysql 包不直接依赖限流实现。
type Bucket interface {
	Wait(count int64)
}

// RowsLimiter 抽象行级限流器的最小接口(对标 task.RowsLimiter.Wait)。
// 用接口而非具体类型, 让 mysql 包不直接依赖 task 包的限流实现。
type RowsLimiter interface {
	Wait(ctx context.Context, rows int64) error
}
