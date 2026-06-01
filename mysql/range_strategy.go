package mysql

import (
	"context"
)

// RangeStrategy 是现有 pt-archiver 范围式分块的策略实现。
//
// P1 阶段为纯结构重构, 行为零变化: 它把现有的两段逻辑原样编排起来——
//   - 生产者: Procedure.BuildSQL, 按唯一键范围逐 chunk 生成 WHERE 条件,
//     写入 Writer.ProducerQueue。
//   - 消费者: Writer.Write, 从队列取 chunk, 在事务里执行 DML(含重试/统计),
//     并按 bucketNum 限流。
//
// 这两段逻辑本身未被改动, 仍由现有 procedure_test/writer_test 守护。
// RangeStrategy 仅作为统一入口, 为 P2 多策略预留扩展点。
type RangeStrategy struct {
	writer *Writer
}

// NewRangeStrategy 从 Writer 创建范围分块策略。
func NewRangeStrategy(w *Writer) *RangeStrategy {
	return &RangeStrategy{writer: w}
}

// Name 实现 ChunkStrategy.Name。
func (rs *RangeStrategy) Name() string {
	return "RangeStrategy"
}

// Run 实现 ChunkStrategy.Run。
//
// 对标原 task.Execute 中 "Procedure 生产 + Writer 消费" 的两协程编排:
// 启动生产者 goroutine 跑 BuildSQL, 当前 goroutine 跑 Writer.Write 消费,
// 任一出错即返回(并取消另一边)。
func (rs *RangeStrategy) Run(ctx context.Context, params RunParams) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// 生产者: Procedure.BuildSQL 写入 ProducerQueue。
	p := NewProcedure(runCtx, rs.writer)
	produceErr := make(chan error, 1)
	go func() {
		produceErr <- p.BuildSQL(rs.writer.ProducerQueue)
	}()

	// 消费者: Writer.Write 从 ProducerQueue 消费。
	writeErr := make(chan error, 1)
	go func() {
		writeErr <- rs.writer.Write(runCtx, params.Bucket, params.BucketNum)
	}()

	// 与原 task.Execute 两协程 + select 的语义一致: 任一侧先出错即返回该错误,
	// 并取消另一侧、回收其结果避免 goroutine 泄漏。
	for produceErr != nil || writeErr != nil {
		select {
		case err := <-produceErr:
			produceErr = nil
			if err != nil {
				cancel()
				if writeErr != nil {
					<-writeErr
				}
				return err
			}
		case err := <-writeErr:
			writeErr = nil
			if err != nil {
				cancel()
				if produceErr != nil {
					<-produceErr
				}
				return err
			}
		}
	}
	return nil
}
