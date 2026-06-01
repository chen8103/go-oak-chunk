# go-oak-chunk × obpurge 合并设计文档

> 目标:把 `obpurge` 的 OceanBase 深度清理能力内聚进 `go-oak-chunk`,统一为**一份代码 / 一个 CLI / 一套 SDK**。
> 决策基线(已与需求方对齐):
> 1. 能力内聚进 go-oak-chunk,并**全部 SDK 化**(obpurge 当前仅 CLI)。
> 2. Parser **统一升级到** `github.com/pingcap/tidb/parser`,清理老 `pingcap/parser` + `XiaoMi/soar`。
> 3. OB fast-path **需要并发**,对齐 obpurge 的多 worker / 分区并发模型。
> 4. 本文为详细设计,评审通过后再动手。

---

## 1. 背景与边界

### 1.1 两个工具的定位

| 维度 | go-oak-chunk (v3) | obpurge |
|---|---|---|
| 操作 | UPDATE + DELETE | 仅 DELETE(purge) |
| 分块 | pt-archiver 范围式(先 SELECT 首尾键 → 范围 DML) | PK 半开区间 `[lo,hi)` + 多种 OB fast-path |
| 限流 | 从库延迟驱动令牌桶 + Correct 修正 | rows-per-sec 令牌桶 + sleep |
| 并发 | 单 writer(三协程流水线) | 多 worker(`--threads` / 分区并发) |
| Parser | `pingcap/parser`(2021)+ soar | `pingcap/tidb/parser`(2023) |
| 形态 | CLI + SDK | 仅 CLI |
| OB 支持 | version 探测 / `_show_ddl_in_compat_mode` / `SHOW INDEX` 回退 | 覆盖索引两阶段 / 分区并发 / EXPLAIN 预检 / OB 错误分类 |

### 1.2 合并后的目标能力矩阵

| 能力 | 来源 | UPDATE | DELETE | 备注 |
|---|---|---|---|---|
| 范围分块(默认) | go-oak-chunk | ✅ | ✅ | 保留为默认策略 |
| 从库延迟限流 | go-oak-chunk | ✅ | ✅ | 非 OB / 有从库时启用 |
| NOW() 冻结 | obpurge | ✅ | ✅ | **跨策略公共能力** |
| OB 错误分类 + 退避重试 | obpurge | ✅ | ✅ | **跨策略公共能力** |
| 覆盖索引两阶段 fast-path | obpurge | ❌ | ✅ | DELETE 专属策略 |
| 分区并发 fast-path | obpurge | ❌ | ✅ | DELETE 专属策略 |
| EXPLAIN 预检 + 大表确认 | obpurge | ✅ | ✅ | 公共预检 |
| rows-per-sec 限流 | obpurge | ✅ | ✅ | 与延迟限流并存,可叠加 |
| 多 worker 并发 | obpurge | ⚠️ 后续 | ✅ | DELETE fast-path 先落地 |

> UPDATE 因为是「改值」而非「删行」,obpurge 的 `DELETE ... WHERE pk IN (...)` 两阶段模型不能直接套用(UPDATE 仍需回表改值)。所以 fast-path 落地范围先锁定 DELETE,UPDATE 继续走范围策略。

---

## 2. 总体架构

### 2.1 分层

```
┌────────────────────────────────────────────────────────────┐
│ 入口层   CLI(cmd/) │ SDK(oak.Executor)                       │
├────────────────────────────────────────────────────────────┤
│ 编排层   task.Execute  ── 选择 ChunkStrategy + 公共能力装配   │
│          ├─ 限流器(RateLimiter:lag / rows-per-sec)          │
│          ├─ 进度上报                                          │
│          └─ 公共能力:NOW冻结 / 错误分类 / preflight           │
├────────────────────────────────────────────────────────────┤
│ 策略层   ChunkStrategy 接口                                   │
│          ├─ RangeStrategy        (默认,UPDATE/DELETE)        │
│          ├─ OBCoveringStrategy   (DELETE,覆盖索引两阶段)      │
│          └─ OBPartitionStrategy  (DELETE,分区并发)           │
├────────────────────────────────────────────────────────────┤
│ 执行层   Writer(事务执行 + 重试) │ 多 worker 协调器           │
├────────────────────────────────────────────────────────────┤
│ 数据访问 mysql.Client / ob.Meta(PK/分区/版本探测)            │
└────────────────────────────────────────────────────────────┘
```

### 2.2 策略选择决策树(在 `Execute` 启动时一次性确定)

```
1. 解析 SQL → 得到 SqlType(UPDATE/DELETE)、table、where
2. 探测数据源(已有 detectDataSourceFromVersion)
3. 若用户显式指定 fast-path flag(--select-order-by / --partition-mode):
     ├─ SqlType != DELETE → 报错(fast-path 仅支持 DELETE)
     ├─ --partition-mode/--partitions → OBPartitionStrategy
     └─ --select-order-by               → OBCoveringStrategy
4. 否则 → RangeStrategy(现有逻辑,行为不变)
```

显式 flag 优先,不做"自动猜测进 fast-path"——fast-path 依赖用户对 `EXPLAIN`/`is_index_back` 的判断,自动切换风险高。

---

## 3. 核心抽象:ChunkStrategy 接口

把"如何切出下一批要操作的行 + 如何执行"抽象出来。现有 `Procedure.BuildSQL`(生产者)+ `Writer.Write`(消费者)收敛为 `RangeStrategy`,行为完全不变,纯重构。

```go
// 拟新增 mysql/strategy.go(或独立 chunk/ 包)
type ChunkPlan struct {
    SelectSQL string   // 探测下一批键/边界的 SQL(可空,范围策略用首尾键)
    ExecSQL   string   // 实际 DML
    Args      []any
    Partition string   // 分区策略用;空表示全表
}

type ChunkStrategy interface {
    // Name 用于日志/进度展示
    Name() string
    // Plan 产出下一批 chunk;done=true 表示该 worker/分区扫描完成
    Plan(ctx context.Context, w Worker) (plan *ChunkPlan, done bool, err error)
    // Execute 执行一个 chunk(短事务 + 重试),返回影响行数
    Execute(ctx context.Context, plan *ChunkPlan) (rowsAffected int64, err error)
    // WorkUnits 拆分并发工作单元(PK 区间 / 分区列表),供 worker 池调度,见 §5
    WorkUnits(ctx context.Context, w Worker, concurrency int) ([]WorkUnit, error)
}
```

> 接口签名是示意,落地时按现有 `Producer`/`Writer` 的实际字段微调。**关键原则**:`RangeStrategy` 是把现有代码原样搬进来,绝不在重构阶段改行为;新策略才引入新 SQL 形态。

### 3.1 RangeStrategy(现有逻辑)
- 来源:`mysql/procedure.go` + `mysql/writer.go` 的 Write 主循环。
- 保持 chunk-size = 0/1/>1 三种语义、首尾键范围、NULL 处理。

### 3.2 OBCoveringStrategy(移植自 obpurge 覆盖索引两阶段)
- 来源:obpurge `internal/chunker` + `internal/runner` 的 select fast-path。
- 每个 chunk 两步:
  ```sql
  SELECT <pk-cols> FROM <t> FORCE INDEX(<select-index>)
   WHERE <frozen-where> [AND (<order-cols>,<pk>) > (?,...)]   -- --select-cursor 时追加游标
   ORDER BY <order-cols>,<pk> LIMIT <chunk-size>;
  DELETE FROM <t> WHERE (<pk-cols>) IN ((?,?),(?,?),...);
  ```
- 游标只在 DELETE 成功提交后推进(失败回滚不前移)。
- 对应 SDK option:`WithOBCovering(index, orderBy string, cursor bool)`。

### 3.3 OBPartitionStrategy(移植自 obpurge 分区并发)
- 来源:obpurge `internal/ob/pk.go` 的 `DiscoverPartitions` + runner 分区调度。
- `--partition-mode each` 从 `information_schema.PARTITIONS` 发现分区;`--partitions p0,p1` 手工指定。
- 每分区独立游标,`PARTITION(pN)` 限定,worker 间不共享分区。
- 对应 SDK option:`WithOBPartition(mode string, partitions []string, concurrency int)`。

---

## 4. 公共能力(跨策略,优先落地)

这三项与现有架构**无冲突**,是 P0 首选,且对 UPDATE 也生效。

### 4.1 NOW() 冻结 —— 正确性缺口修复
- 来源:obpurge `internal/sqlparse/freeze.go`。
- 现状缺口:go-oak-chunk 长任务里 `where t <= now()` 的边界会随时间漂移,导致后期 chunk 范围和首次预估不一致。
- 方案:任务启动时 walk WHERE AST,把 `now()/sysdate/current_timestamp/localtime/localtimestamp` 替换为启动时刻字面量,所有 chunk 复用冻结后的 where。
- 落地点:`mysql/writer.go` 的 `getInfoFromTable` 解析出 where 之后,统一 freeze 一次存入 `OriginWhereClause`。
- 依赖:需要 AST(见 §6 parser 升级);P0 若先不升级 parser,可临时用 soar AST 实现等价 freeze。

### 4.2 OB 错误分类 + 退避重试
- 来源:obpurge `internal/obrr/classify.go`。
- 现状缺口:go-oak-chunk `writer.go:206` 的重试逻辑较粗(`rowAffects>0` 就放弃),且不识别 OB `4012` 事务冲突。
- 方案:引入 `Classify(err) → {Transient/Fatal/Canceled}`,allow-list 错误码 `1213/1205/4012/2006/2013`,Transient 指数退避重试整个 chunk 事务。
- 落地点:替换 `Writer.Write` 内联重试段。

### 4.3 EXPLAIN 预检 + 大表二次确认
- 来源:obpurge `internal/preflight/preflight.go`(含 OB query plan 文本解析、`is_index_back` 提取)。
- 方案:`Execute` 启动前用 `EXPLAIN SELECT 1 FROM t WHERE <where>` 估算行数,超阈值且非 `--yes` 时二次确认;OB 计划文本里抓 `is_index_back` 给出 fast-path 建议提示。
- SDK:`WithPreflight(threshold int64, autoConfirm bool)`。

---

## 5. 并发模型对齐

### 5.1 现状差异
- go-oak-chunk:单 writer,三协程是「生产/消费/调速」分工,不是数据并行。
- obpurge:中心协调器预分配最多 `threads` 个**不重叠** PK chunk 给多 worker;分区模式每 worker 一个分区。

### 5.2 合并方案
引入 **worker 池**,由策略决定工作单元拆分:
- **RangeStrategy**:维持单 writer(保持现有行为,先不并行化,降低风险)。
- **OBCoveringStrategy**:中心协调器按 PK 范围切 N 个不重叠区间,N = `--threads`,每 worker 独立游标。
- **OBPartitionStrategy**:worker 数 = `--partition-concurrency`,每 worker 钉一个分区。

```
Coordinator
  ├─ 拆分工作单元(PK 区间 / 分区列表)
  ├─ 启动 N 个 worker
  ├─ 每 worker:Plan → Execute → 汇报 rowsAffected
  └─ 汇总:progress / max-rows / max-duration / 限流 在 chunk 边界统一结算
```

> 注意 obpurge 的语义坑要原样继承并文档化:并发模式下 `--max-rows` 只能在 chunk 边界近似停止,最多超出 `concurrency * chunk-size` 行。

### 5.3 限流统一
- 保留现有 `RateLimiter`(延迟驱动 + Correct)。
- 新增 **rows-per-sec** 模式(obpurge `internal/limiter`):按每个成功 chunk 的 affected rows 在下一 chunk 前等待。
- OB 策略默认走 rows-per-sec 并**跳过从库检测**(`getStopTime` 对 OB 直接降级);非 OB 维持延迟驱动。两者可叠加:取更严格的等待。

---

## 6. Parser 升级(统一到 tidb/parser)

### 6.1 影响面
go-oak-chunk 当前依赖 `pingcap/parser` + `XiaoMi/soar` 的点:
- `mysql/writer.go:getInfoFromTable`:`soar.TiParse` 解析 UPDATE/DELETE 类型、提取 where、解析 `SHOW CREATE TABLE` 得唯一键。
- `mysql/meta.go`:AST visitor 提取 where、唯一键。

### 6.2 迁移步骤
1. go.mod 移除 `pingcap/parser`、`XiaoMi/soar`,引入 `github.com/pingcap/tidb/parser`(注意需要 `_ "parser/test_driver"` 注册 value 驱动)。
2. 把 `soar.TiParse` 调用换成 `parser.New().ParseSQL`,AST 类型从老包 `ast.*` 切到 `tidb/parser/ast.*`(类型名兼容度高,但 import 路径全变)。
3. 复用 obpurge `sqlparse/parse.go` 的校验:单表、禁 multi-table/JOIN/LIMIT/ORDER BY。**注意**:obpurge 只校验 DELETE,要扩展支持 UPDATE 的 set 子句提取。
4. UPDATE 的 `set ... where` 提取:现状用正则(`writer.go:295`),升级后改成从 AST 的 `UpdateStmt.List` + `Where` restore,去掉正则脆弱点。
5. Go 版本:go-oak-chunk 1.23 → 与 tidb/parser 要求对齐(obpurge 用 1.26,确认 parser 最低 Go 版本后统一)。

### 6.3 风险
- 老 parser 与 tidb/parser 对某些 OB 特有语法(hint、分区子句)的容忍度不同,需用线上真实 SQL 回归。
- soar 还可能被其它地方间接使用,升级前 grep 确认。

---

## 7. SDK 化设计(关键增量)

obpurge 现在零 SDK。合并后所有 OB 能力都挂在 `oak.Executor` 的 option 上,与现有 `WithProgressCallback`/`WithRateLimiter` 并列:

```go
executor, err := oak.NewExecutor(cfg,
    // 现有
    oak.WithProgressCallback(cb, 3*time.Second),
    // 新增:OB 覆盖索引两阶段 fast-path(DELETE)
    oak.WithOBCovering(oak.OBCoveringOption{
        SelectIndex:   "idx_create_time",
        SelectOrderBy: "create_time",
        Cursor:        true,
        Threads:       4,
    }),
    // 或:OB 分区并发
    oak.WithOBPartition(oak.OBPartitionOption{
        Mode:        "each",        // 或 Partitions: []string{"p0","p1"}
        Concurrency: 4,
    }),
    // 新增:rows-per-sec 限流 + 预检
    oak.WithRowsPerSec(20000),
    oak.WithPreflight(100000, false),
)
```

- option 之间互斥校验放在 `NewExecutor`(如 covering 与 partition 互斥、fast-path 要求 SqlType=DELETE)。
- `ExecutorStatus` 扩展:增加 `Strategy string`、`Partition string`、`Threads int` 字段,进度回调可观测。
- CLI 侧只是把 flag 映射到这些 option,CLI 与 SDK 同源。

---

## 8. 配置与 flag 映射

新增 flag(全部同时落到 `conf.Config` + SDK option):

| CLI flag | conf.Config 字段 | SDK option | 策略 |
|---|---|---|---|
| `--select-index` | `SelectIndex` | `WithOBCovering` | covering |
| `--select-order-by` | `SelectOrderBy` | `WithOBCovering` | covering |
| `--select-cursor` | `SelectCursor` | `WithOBCovering` | covering |
| `--partition-mode` | `PartitionMode` | `WithOBPartition` | partition |
| `--partitions` | `Partitions` | `WithOBPartition` | partition |
| `--partition-concurrency` | `PartitionConcurrency` | `WithOBPartition` | partition |
| `--threads` | `Threads` | `WithOBCovering` | covering |
| `--rows-per-sec` | `RowsPerSec` | `WithRowsPerSec` | 公共 |
| `--max-rows` | `MaxRows` | `WithLimits` | 公共 |
| `--max-duration` | `MaxDuration` | `WithLimits` | 公共 |
| `--where-extra` | `WhereExtra` | `WithWhereExtra` | 公共 |
| `--dry-run` | `DryRun` | `WithDryRun` | 公共 |
| `--why-stop` | `WhyStop` | (status 返回停止原因) | 公共 |
| `--yes` | `AutoConfirm` | `WithPreflight` | 公共 |

`PreCheck` 新增互斥/依赖校验:
- covering 与 partition 互斥;两者都要求 SqlType=DELETE。
- `--select-cursor` 依赖 `--select-order-by`。
- fast-path + `--threads>1` 时各 worker 游标独立性校验。

---

## 9. 分阶段路线图

| 阶段 | 范围 | 风险 | DoD |
|---|---|---|---|
| **P0** | NOW() 冻结 + OB 错误分类(4012)重试 | 低 | 长任务边界稳定;OB 事务冲突自动重试;UPDATE/DELETE 均生效;不改现有行为 |
| **P1** | Parser 统一升级到 tidb/parser + 抽 ChunkStrategy 接口(现逻辑收进 RangeStrategy) | 中 | 纯重构,全量回归现有 UPDATE/DELETE 用例行为不变 |
| **P2** | OBCoveringStrategy(单 worker)+ EXPLAIN 预检 + SDK option | 中 | DELETE 覆盖索引两阶段可用,SDK + CLI 同源;`--dry-run` 校验 SQL |
| **P3** | 多 worker 并发(covering)+ OBPartitionStrategy + rows-per-sec 限流 | 高 | 对齐 obpurge 并发与分区能力;并发语义坑文档化 |

> P0 可独立交付收益,不阻塞 parser 决策。P1 是后续一切的地基,务必先把回归测试补齐再重构。

---

## 10. 风险与未决项

1. **Parser 升级回归**:UPDATE 的 set/where 提取从正则改 AST,需用线上真实 SQL 集回归(尤其含函数、子查询、OB hint 的)。
2. **并发正确性**:多 worker PK 区间必须严格不重叠(半开区间 `[lo,hi)`),否则重复/漏删;需专门的并发测试。
3. **UPDATE 与 fast-path 的边界**:明确告知用户 fast-path 仅 DELETE,UPDATE 走范围策略,避免误用。
4. **限流叠加语义**:延迟驱动 + rows-per-sec 同时启用时取更严格者,需在文档讲清,避免双重限流过慢。
5. **go.mod 大改**:parser 升级 + Go 版本提升可能牵动其它间接依赖(soar 关联的一堆 pingcap/tidb 旧包),建议升级前先 `go mod graph` 评估爆炸半径。
6. **obpurge 既有语义坑继承**:`--max-rows` 并发下近似停止、无主键表直接报错等,原样继承并在 README 标注。

---

## 11. 测试策略

- **P0**:freeze 单测(各种 NOW 变体)、错误分类单测(对齐 obpurge `classify_test.go`)。
- **P1**:现有 `oak_test.go` / `mysql/*_test.go` / `task/*_test.go` 全绿即重构成功判据;新增 strategy 接口契约测试。
- **P2/P3**:OB 真实集群 smoke(参考 obpurge `scripts/m7_smoke_helper.sh` 与 `docs/test/m7-cluster-smoke.md`);并发不重叠区间的属性测试。
