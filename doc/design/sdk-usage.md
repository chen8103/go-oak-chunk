# go-oak-chunk SDK 使用手册（v3）

[English](sdk-usage-en.md) | [简体中文](sdk-usage.md)

## 1. 文档目标

这份文档说明如何把 `go-oak-chunk` 作为 **Go SDK** 嵌入你的服务，而不是只通过命令行运行。

覆盖内容：

- 模块引入与最小可运行示例
- `Executor` API 与生命周期
- 配置模型（`conf.Config`）完整说明
- 日志接入与回调机制
- 运行时动态调参（sleep/maxLag）
- 错误语义、停止语义与并发约束
- 生产集成建议

---

## 2. 适用场景

SDK 方式适合：

- 你已经有常驻服务，需要“嵌入式”执行 chunk DML
- 你要统一接入已有日志、监控、配置中心、信号处理
- 你希望运行中动态调整节奏（例如根据系统负载自动调 sleep）
- 你需要用代码方式管理任务生命周期（启动/停止/超时/取消）
- 你要针对特定数据源做加速 DELETE：OceanBase 覆盖索引快路径 / 分区并行（`WithOBCovering` / `WithPartitionConcurrency`）、TiDB 按 `_tidb_rowid` 清理无主键表（`WithTiDBRowID`）

---

## 3. 依赖与导入

### 3.1 Go Module

```bash
go get github.com/SisyphusSQ/go-oak-chunk/v3
```

### 3.2 导入建议

```go
import (
    "context"
    "errors"
    "time"

    oak "github.com/SisyphusSQ/go-oak-chunk/v3"
    "github.com/SisyphusSQ/go-oak-chunk/v3/conf"
    oaklog "github.com/SisyphusSQ/go-oak-chunk/v3/log"
    "github.com/SisyphusSQ/go-oak-chunk/v3/task"
)
```

---

## 4. 核心 API 总览

| API | 作用 | 关键语义 |
|---|---|---|
| `oak.NewExecutor(config, opts...)` | 创建执行器 | 会校验配置并初始化底层 writer |
| `(*Executor).Run(ctx)` | 启动任务 | **one-shot**：同一个实例只能 `Run` 一次 |
| `(*Executor).Stop()` | 主动停止 | 幂等，可重复调用 |
| `(*Executor).UpdateSleep(ms)` | 动态修改 sleep | 负值会被钳制为 `0` |
| `(*Executor).UpdateMaxLag(lag)` | 动态修改 lag 阈值 | 负值会被钳制为 `0` |
| `(*Executor).GetStatus()` | 读取快照状态 | 返回当前已影响行数、耗时、sleep、lag 等 |
| `oak.WithProgressCallback(cb, interval)` | 注入进度回调 | 回调慢时会跳 tick，避免积压 |
| `oak.WithRateLimiter(rl)` | 注入自定义限流器 | 可完全接管 sleep/maxLag/noConsiderLag |
| `oak.WithRowsPerSec(n)` | 设置全局每秒行数上限 | `0`=不限；与 sleep/maxLag 令牌桶叠加 |
| `oak.WithMaxRows(n)` | 处理满 `n` 行后停止 | `0`=不限；对三种策略均生效 |
| `oak.WithMaxDuration(ms)` | 运行满 `ms` 毫秒后停止 | `0`=不限；对三种策略均生效 |
| `oak.WithPartitionConcurrency(n)` | OceanBase 分区并行 DELETE | `0/1`=关闭；需覆盖快路径 + 分区表 |
| `oak.WithTiDBRowID(true)` | TiDB 按 `_tidb_rowid` 分块 DELETE | 仅 DELETE；NONCLUSTERED 表、无需 PK/UK；与覆盖快路径/分区互斥 |

常见错误：

- `oak.ErrExecutorAlreadyRun`：重复调用 `Run`
- `task.ErrExecutionStopped`：被取消/停止后的统一停止错误（业务上通常视为正常停止）

---

## 5. 最小可运行示例

```go
package main

import (
    "context"
    "errors"
    "fmt"
    "time"

    oak "github.com/SisyphusSQ/go-oak-chunk/v3"
    "github.com/SisyphusSQ/go-oak-chunk/v3/conf"
    "github.com/SisyphusSQ/go-oak-chunk/v3/task"
)

func main() {
    cfg := &conf.Config{
        Host:         "127.0.0.1",
        Port:         3306,
        User:         "root",
        Password:     "xxx",
        Database:     "test_db",
        ExecuteQuery: "DELETE FROM orders WHERE created_at < '2025-01-01 00:00:00'",
        ChunkSize:    1000,
        TxnSize:      2000,
        Sleep:        200, // 毫秒
        MaxLag:       3,   // 秒
        Correct:      50,  // 建议保持 50
    }

    executor, err := oak.NewExecutor(cfg)
    if err != nil {
        panic(err)
    }

    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
    defer cancel()

    err = executor.Run(ctx)
    if err != nil {
        if errors.Is(err, task.ErrExecutionStopped) {
            fmt.Println("任务被取消/停止，按正常停止处理")
            return
        }
        panic(err)
    }
}
```

---

## 6. `conf.Config` 完整说明

> SDK 侧推荐显式构造 `conf.Config`，不要依赖隐式默认值。

### 6.1 关键字段一览

> 说明：SDK 直接构造 `conf.Config`，未显式赋值的字段取 Go **零值**（数值=`0`、bool=`false`、string=空），下表“默认/建议”列即按此给出。

| 字段 | 类型 | 默认/建议 | 说明 |
|---|---|---|---|
| `ExecuteQuery` | `string` | 必填 | 单条 `UPDATE/DELETE` SQL |
| `Database` | `string` | SQL 未使用 `schema.table` 时必填 | 与 SQL schema 同时提供时必须一致 |
| `Host` | `string` | 必填 | 主库地址 |
| `Port` | `int` | 必填 | 主库端口 |
| `User` | `string` | 必填 | 用户名 |
| `Password` | `string` | 建议必填 | 密码 |
| `ChunkSize` | `int64` | `1000` 起步 | 每 chunk 行数；`0` 表示一次性 |
| `TxnSize` | `int64` | `>= ChunkSize` | 每事务最大处理行数 |
| `Sleep` | `int64` | `0`（0~800） | 毫秒级节奏控制 |
| `MaxLag` | `int64` | `0`（0~5） | 从库延迟阈值（秒）；`0`=不限 |
| `NoConsiderLag` | `bool` | `false` | `true` 时不按 lag 放大 sleep |
| `IncludeSlaves` | `string` | 空 | 仅监控这些从库 |
| `ExcludeSlaves` | `string` | 空 | 排除这些从库 |
| `NoSlaves` | `bool` | `false` | 跳过从库检查 |
| `ForceChunkingColumn` | `string` | 空 | 强制使用指定唯一键列集；OB 下会额外基于 `SHOW INDEX` 校验，逗号两侧空格会自动忽略 |
| `PrintProgress` | `bool` | `false` | CLI 终端输出模式开关 |
| `Debug` | `bool` | `false` | debug 日志 |
| `Correct` | `int64` | **建议 50** | 限流修正值（CLI 默认 `50`；SDK 零值为 `0`，建议显式设 `50`） |
| `RowsPerSec` | `int64` | `0` | 全局每秒行数上限；`0`=不限，与 sleep/maxLag 叠加 |
| `SelectOrderBy` | `string` | 空 | 覆盖索引快路径排序列（逗号分隔，仅 DELETE）；`PartitionConcurrency` 的前置条件 |
| `SelectIndex` | `string` | 空 | 候选 SELECT 的 `FORCE INDEX` 名；需配合 `SelectOrderBy` |
| `SelectCursor` | `bool` | `false` | 游标推进候选 SELECT，避免每轮重扫；需配合 `SelectOrderBy`，大表建议开 |
| `MaxRows` | `int64` | `0` | 处理满行数后停止；`0`=不限，对三种策略均生效 |
| `MaxDuration` | `int64` | `0` | 运行满毫秒后停止；`0`=不限，对三种策略均生效 |
| `PartitionConcurrency` | `int` | `0` | OceanBase 分区并行 DELETE worker 数；`0/1`=关闭 |
| `TiDBRowID` | `bool` | `false` | TiDB 按 `_tidb_rowid` 分块 DELETE（NONCLUSTERED 表、无需 PK/UK）；与覆盖快路径/分区/`ForceChunkingColumn` 互斥 |
| `DryRun` | `bool` | `false` | 只打印样例 SQL，不实际执行 |
| `PreflightThreshold` | `int64` | `0` | EXPLAIN 大表确认阈值；`0`=默认 `100000` |
| `AutoConfirm` | `bool` | `false` | 跳过大表确认；SDK/非交互建议 `true` |

### 6.2 `PreCheck` 会校验什么

`NewExecutor` 内部会调用 `config.PreCheck()`，主要校验：

- `ChunkSize >= 0`
- `ExecuteQuery` 非空
- `IncludeSlaves` 与 `ExcludeSlaves` 互斥

> 注意：`Database` 非空检查在 writer 初始化阶段做，不在 `PreCheck` 里。

### 6.3 与 CLI 的一个细微差异

CLI 的“纯命令行模式”会自动把 `Correct` 设为 `50`；  
SDK 模式不会自动注入这个值，建议你在代码里显式设置：

```go
cfg.Correct = 50
```

---

## 7. 生命周期与并发语义（非常重要）

### 7.1 `Executor` 是 one-shot

同一个 `Executor` 实例只能运行一次：

- 第一次 `Run(ctx)`：正常执行
- 第二次 `Run(ctx)`：直接返回 `oak.ErrExecutorAlreadyRun`

即使第一次是因为取消停止，第二次依然会返回这个错误。

### 7.2 `Stop()` 语义

- `Stop()` 内部调用 cancel
- 可重复调用（幂等）
- 没有运行中的任务时调用也安全

### 7.3 推荐生命周期模式

```mermaid
sequenceDiagram
    participant App as 业务服务
    participant Ex as Executor
    participant Core as task.Execute

    App->>Ex: NewExecutor(cfg)
    App->>Ex: Run(ctx)
    Ex->>Core: Execute(runCtx, cfg, writer, opts)
    App->>Ex: UpdateSleep/UpdateMaxLag (可选)
    App->>Ex: Stop() 或 cancel()
    Core-->>Ex: task.ErrExecutionStopped / nil / 其他 error
    Ex-->>App: 返回结果
```

---

## 8. 错误处理建议

推荐统一使用 `errors.Is` 做分类：

```go
err := executor.Run(ctx)
switch {
case err == nil:
    // success
case errors.Is(err, task.ErrExecutionStopped):
    // 用户取消、信号停止、超时等统一停止语义
case errors.Is(err, oak.ErrExecutorAlreadyRun):
    // 复用实例导致
default:
    // 真正失败：建连、SQL 解析、执行等
}
```

常见失败来源：

- SQL 非单条 / 非 `UPDATE|DELETE`
- 目标表不存在
- 找不到主键或唯一键
- `ForceChunkingColumn` 不匹配
- OceanBase 下 `SHOW INDEX` 查询失败（显式设置 `ForceChunkingColumn` 时会直接报错）
- 数据库连接失败或执行失败

---

## 9. 进度回调：`WithProgressCallback`

### 9.1 使用方式

```go
executor, err := oak.NewExecutor(
    cfg,
    oak.WithProgressCallback(func(s *oak.ExecutorStatus) {
        if s == nil {
            return
        }
        // 上报 metrics 或写日志
        // s.RowAffects / s.ElapsedTime / s.CurrentSleep / s.MaxSlaveLag / s.IsFinished
    }, 2*time.Second),
)
```

### 9.2 回调行为细节

- 回调按 `interval` 周期触发
- 回调执行是异步的
- 如果上一次回调还没结束，当前 tick 会跳过（防止队列堆积）
- 回调 panic 会被 recover，不会拖垮主流程
- 任务结束时会尝试推送一次最终快照（若上一次回调仍未结束，可能被跳过）

### 9.3 实践建议

- 回调内不要做重 I/O 阻塞操作
- 回调中如需网络上报，建议再异步投递
- 给回调加超时保护，避免链路抖动反向影响采集

---

## 10. 运行时动态调参

### 10.1 动态修改 sleep / maxLag

```go
executor.UpdateSleep(500) // 500ms
executor.UpdateMaxLag(5)  // 5s
```

负值会自动钳制到 `0`。

### 10.2 查询状态快照

```go
st := executor.GetStatus()
fmt.Printf(
    "rows=%d elapsed=%s sleep=%d lag=%d finished=%v\n",
    st.RowAffects, st.ElapsedTime, st.CurrentSleep, st.MaxSlaveLag, st.IsFinished,
)
```

### 10.3 动态控制样例（简化版）

```go
ticker := time.NewTicker(5 * time.Second)
defer ticker.Stop()

for {
    select {
    case <-ctx.Done():
        return
    case <-ticker.C:
        st := executor.GetStatus()
        // 示例策略：落后严重就放慢
        if st.MaxSlaveLag > 5 {
            executor.UpdateSleep(800)
        } else {
            executor.UpdateSleep(200)
        }
    }
}
```

---

## 11. 自定义限流器：`WithRateLimiter`

如果你希望自行管理限流器：

```go
rl := task.NewRateLimiter(
    300,  // sleep(ms)
    3,    // maxLag(s)
    50,   // correct
    false, // noConsiderLag
)

executor, err := oak.NewExecutor(cfg, oak.WithRateLimiter(rl))
```

用途：

- 多任务统一限流策略
- 接入你自己的参数控制器
- 对不同业务类型使用不同节奏模板

---

## 11bis. 行速率上限与守护边界

> **限流原理（两套独立机制）**：每个 chunk 提交后生效，最终节奏取更严格者。
> - **机制 A（令牌桶）**：管「时间节奏 + 从库保护」，受 `Sleep`/`MaxLag`/`NoConsiderLag`/`Correct` 控制。1 token=1ms；`getStopTime` 协程探测从库延迟并算出应等待的毫秒数喂给消费者，`lag>=MaxLag` 时令消费者暂停 ~1s 等从库追平。
> - **机制 B（rows-per-sec）**：管「行吞吐上限」，独立 limiter，每批等待 `affected/RowsPerSec` 秒。
> - 分区并发下两者均**全局共享**，限制全表合计速率。
> - 完整推导见 CLI 手册 [`cli-usage.md` §9](cli-usage.md)。

### 11bis.1 `WithRowsPerSec`（全局行速率）

```go
oak.NewExecutor(cfg, oak.WithRowsPerSec(50000)) // 也可直接 cfg.RowsPerSec = 50000
```

- 在令牌桶（`Sleep`）与从库延迟（`MaxLag`）之外，再叠加一个**按行**的全局速率上限
- 每次执行 DML 前按本批行数申请配额，配额不足则等待；三种限流**叠加**生效，最终节奏取决于最严格的那个
- `0` 表示不限速
- 该限速器全局共享：分区并行模式下所有 worker 共用，限制的是**全表合计**速率

> 跑满速：`Sleep=0` 且 `RowsPerSec=0`（且无 `MaxLag` 触发）时无任何隐式等待。v3.2.0 修复了覆盖索引/分区 DELETE 每个 chunk 后按行数误等待（~1ms/行）的 bug——此前即使关闭限速也会被压到 ~1000 行/秒。

### 11bis.2 `WithMaxRows` / `WithMaxDuration`（跑够即停）

```go
oak.NewExecutor(cfg,
    oak.WithMaxRows(2000000),      // 累计 200 万行后停止
    oak.WithMaxDuration(600000),   // 或运行满 10 分钟后停止
)
```

- 二者任一触达即停止；`0` 表示不限
- 早期版本只允许在覆盖快路径上设置，**该限制已移除**：range、covering、partition 三种策略均会强制执行
- 停止为**粗粒度**：range 在事务 / chunk 边界停，covering 在 chunk 边界停，partition 把本批裁剪到剩余额度后停（`MaxRows` 在多 worker 间精确生效，合计不超过、不超删）

---

## 11ter. OceanBase 分区并行 DELETE（`WithPartitionConcurrency`）

```go
oak.NewExecutor(cfg,
    oak.WithOBCovering("", "id", false), // 覆盖快路径，提供 SelectOrderBy
    oak.WithPartitionConcurrency(4),     // 4 个并行 worker
    oak.WithRowsPerSec(50000),
    oak.WithMaxRows(2000000),
)
```

生效条件（由 `NewExecutor` 的 `PreCheck` 校验依赖、运行时探测分区）：

- 数据源为 **OceanBase**
- 走覆盖快路径，即设置了 `SelectOrderBy`（仅 DELETE）
- 目标表为分区表（运行时通过 `information_schema.PARTITIONS` 探测）
- `n >= 2` 才启用；`0/1` 退回单 worker 的 covering / range 路径

并发行为：

- worker 数钳制到 `min(配置值, 实际分区数, 内部硬上限 64)`
- 每个 worker 抢占一个分区、独占游标，DELETE 作用域限定到该分区
- 连接池上限随并发自动放大（约 `并发数 + 2`）
- 限速器（令牌桶 + `RowsPerSec`）**全局共享**，限制全表合计速率
- 从库延迟暂停（`MaxLag` 命中）**全局**生效，所有 worker 一起暂停
- `MaxRows` 在多 worker 间**精确**生效（合计不超过、零超删），`MaxRows=0` 仍为完全不限
- 任一 worker 报错取消整个并发组，第一个错误向上返回

---

## 11quater. TiDB `_tidb_rowid` 清理（`WithTiDBRowID`）

```go
oak.NewExecutor(cfg,
    oak.WithTiDBRowID(true), // 也可直接 cfg.TiDBRowID = true
)
```

- 用 TiDB 隐藏行句柄 `_tidb_rowid` 做分块键，让**无主键/唯一键**的 TiDB **NONCLUSTERED（非聚簇）** 表也能分批 DELETE
- **仅 DELETE**；显式开关，不改 TiDB 默认行为（默认仍走范围分块）
- 与 `SelectOrderBy`/`SelectCursor`/`SelectIndex`/`PartitionConcurrency>1`/`ForceChunkingColumn` **互斥**（`PreCheck` 校验）；需要 `ChunkSize > 0`
- 运行时通过 `information_schema.tables.TIDB_PK_TYPE` 校验适用性，CLUSTERED 表清晰报错（老版本回退为 `_tidb_rowid` 探测）
- 采用 seek 游标（`_tidb_rowid > cursor`）+ `_tidb_rowid IN (...)` 删除，对 `SHARD_ROW_ID_BITS`/`AUTO_RANDOM` 稀疏 rowid 健壮；游标只在 DELETE 提交后前移，始终复用冻结后的 WHERE
- `MaxRows`/`MaxDuration` 护栏、`RowsPerSec`/sleep/lag 限流、失败重试（含 TiDB 写冲突码）均复用既有机制

---

## 12. 日志接入策略

SDK 常见三种方式：

### 12.1 方式 A：你不做任何初始化

`NewExecutor` 会自动初始化默认 logger（输出到 stderr）。

### 12.2 方式 B：使用 SDK 内置初始化

```go
_ = oaklog.New(true, oaklog.OutputStderr) // debug + stderr
```

### 12.3 方式 C：复用你已有的 zap logger（推荐）

```go
// sugar := yourZapLogger.Sugar()
oaklog.NewFromSugaredLogger(sugar)
```

适合把 SDK 日志并入你现有日志平台与 trace pipeline。

---

## 13. 生产集成范式（推荐）

1. 服务启动时装载配置（DB、节奏参数、SQL 模板）
2. 调用 `oak.NewExecutor` 创建任务对象
3. 用 `context.WithTimeout` 包裹 `Run`
4. 用回调上报进度指标（QPS、row_affected、lag）
5. 暴露运维接口动态调 `sleep/maxLag`
6. 进程退出时先 `Stop()`，再等待 `Run` 退出

---

## 14. 常见问题与排障

| 问题 | 原因 | 解决 |
|---|---|---|
| `executor can only run once` | 同实例重复 `Run` | 每次任务创建新 `Executor` |
| 返回 `task.ErrExecutionStopped` | 主动 `Stop`/`cancel`/超时 | 作为正常停止处理 |
| `no database specified` | `Database` 空 | 显式设置 `cfg.Database` |
| `please confirm sql type is update or delete` | SQL 类型不支持 | 仅使用单条 Update/Delete |
| `can't find any index which is primary or unique key` | 表无唯一键 | 增加主键/唯一键 |
| `forced_chunking_column doesn't conform ...` | 指定列不匹配唯一键 | 使用真实唯一键列集合 |
| `show index ... failed` | OceanBase 索引元数据查询失败 | 检查权限、连接状态与表名；若设置了 `ForceChunkingColumn` 会直接失败 |
| 回调触发频率不稳定 | 回调执行太慢，tick 被跳过 | 缩短回调逻辑，异步化重操作 |

---

## 15. 与 CLI 的映射（迁移参考）

| CLI 参数 | SDK 配置/API |
|---|---|
| `--execute` | `conf.Config.ExecuteQuery` |
| `--database` | `conf.Config.Database` |
| `--chunk-size` | `conf.Config.ChunkSize` |
| `--txn-size` | `conf.Config.TxnSize` |
| `--sleep` | `conf.Config.Sleep` 或 `executor.UpdateSleep()` |
| `--max-lag` | `conf.Config.MaxLag` 或 `executor.UpdateMaxLag()` |
| `--noConsiderLag` | `conf.Config.NoConsiderLag` |
| `--include-slaves` | `conf.Config.IncludeSlaves` |
| `--exclude-slaves` | `conf.Config.ExcludeSlaves` |
| `--no-slaves` | `conf.Config.NoSlaves` |
| `--debug` | `conf.Config.Debug` |
| `--rows-per-sec` | `conf.Config.RowsPerSec` 或 `oak.WithRowsPerSec()` |
| `--select-order-by` | `conf.Config.SelectOrderBy` 或 `oak.WithOBCovering()` |
| `--max-rows` | `conf.Config.MaxRows` 或 `oak.WithMaxRows()` |
| `--max-duration-ms` | `conf.Config.MaxDuration` 或 `oak.WithMaxDuration()` |
| `--partition-concurrency` | `conf.Config.PartitionConcurrency` 或 `oak.WithPartitionConcurrency()` |
| `--print-progress` | CLI 专属终端输出；SDK 用 `WithProgressCallback` 替代 |

---

## 16. 最佳实践清单

- 每次执行用新的 `Executor`，不要复用
- SQL 必须带清晰 `WHERE`，且先做小流量验证
- 显式设置 `Correct=50` 以贴近 CLI 行为
- 给 `Run` 设置超时上下文，避免悬挂
- 把 `task.ErrExecutionStopped` 当“可预期停止”
- 回调只做快逻辑，慢逻辑异步处理
- 通过 `UpdateSleep/UpdateMaxLag` 做在线调速

---

## 17. 一个更完整的服务化示例（含停止与调速）

```go
func RunChunkJob(ctx context.Context, cfg *conf.Config) error {
    cfg.Correct = 50

    executor, err := oak.NewExecutor(
        cfg,
        oak.WithProgressCallback(func(s *oak.ExecutorStatus) {
            // metrics.Observe(...)
            _ = s
        }, 3*time.Second),
    )
    if err != nil {
        return err
    }

    // 动态调速协程
    ctrlCtx, ctrlCancel := context.WithCancel(ctx)
    defer ctrlCancel()
    go func() {
        ticker := time.NewTicker(5 * time.Second)
        defer ticker.Stop()
        for {
            select {
            case <-ctrlCtx.Done():
                return
            case <-ticker.C:
                st := executor.GetStatus()
                if st.MaxSlaveLag > 8 {
                    executor.UpdateSleep(1000)
                } else if st.MaxSlaveLag > 3 {
                    executor.UpdateSleep(500)
                } else {
                    executor.UpdateSleep(150)
                }
            }
        }
    }()

    runErr := executor.Run(ctx)
    if runErr != nil && errors.Is(runErr, task.ErrExecutionStopped) {
        return nil
    }
    return runErr
}
```

这个模式基本覆盖了生产中的“可观测 + 可调速 + 可中断”的核心诉求。
