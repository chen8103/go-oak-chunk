### v3.1.0(20260415)
#### bugFix:
1. 修复 OceanBase 场景下仅靠兼容 DDL 无法识别全局唯一键的问题；现在会额外通过 `SHOW INDEX` 发现主键/唯一键候选
2. 修复 `forced_chunking_column` 在 OceanBase 下无法命中兼容 DDL 中缺失的真实唯一键的问题，如 `uk_order_code(order_code)`

#### optimization:
1. `forced_chunking_column` 输入会自动忽略逗号两侧空格，复合键配置更稳健

### v3.0.1(20250309)
#### feature:
1. 在 getInfoFromTable 中通过 `select version()` 识别数据源类型（MySQL/TiDB/OceanBase）
2. OceanBase 场景下在 SHOW CREATE TABLE 前执行 `SET SESSION _show_ddl_in_compat_mode = true`，获取 MySQL 兼容 DDL

#### optimization:
1. 获取表结构阶段改为固定连接流程（Conn），确保会话变量与 SHOW CREATE TABLE 在同一连接执行
2. writer_get_info_test 合并至 writer_test

---

### v3.0.0(20250308)
#### feature:
1. 新增使用文档入口：CLI 使用手册[`doc/design/cli-usage.md`](doc/design/cli-usage.md)、SDK 使用手册[`doc/design/sdk-usage.md`](doc/design/sdk-usage.md)
2. 模块路径统一为 `github.com/SisyphusSQ/go-oak-chunk/v3`，便于通过 go get 拉取与发布
3. 新增 SDK 模式：根包 `oak` 暴露 `Executor`，支持 `NewExecutor`、`Run`、`Stop`、`UpdateSleep`、`UpdateMaxLag`、`GetStatus`，便于以库形式嵌入业务
4. 新增 `RateLimiter` 组件，封装令牌桶及 Sleep/MaxLag/Correct/NoConsiderLag，支持运行时动态调整
5. 新增 `log.NewFromSugaredLogger`，支持 SDK 侧注入已有 `*zap.SugaredLogger`
6. 新增 `NewSlaveCheckerWithContext`、`CheckVersionContext`，启动链路支持 context 取消
7. 项目结构调整：CLI 入口移至 `cmd/go-oak-chunk/main.go`，根包名改为 `oak`

#### optimization:
1. 全局 import 与 go.mod 模块路径更新为 `github.com/SisyphusSQ/go-oak-chunk/v3`，SDK 使用手册同步更新导入说明
2. 全面去除 `os.Exit`/`log.Fatal`，改为返回 `error`，支持 SDK 优雅失败处理
3. 日志从 tinylog 迁移至 zap + lumberjack，合并 `StreamLogger`/`GlobalLogger` 为单一 `Logger`
4. DB 调用统一改为 `ExecContext`/`QueryContext`，取消信号可及时中断阻塞 I/O
5. `Procedure`/`SlaveChecker`/`Writer` 中查询路径补齐 `rows.Close`、`rows.Err` 收尾
6. Stop/Cancel 统一映射为 `task.ErrExecutionStopped`，CLI 层按正常停止处理
7. `Executor` 采用 one-shot 语义，二次 `Run` 返回 `ErrExecutorAlreadyRun`
8. `RateLimiter` 对 sleep/maxLag/correct 负值做 clamp 保护
9. CLI 日志初始化收敛为配置解析后一次性初始化
10. 完善 `run` 命令错误与流程日志：配置加载/预检/Executor 创建/任务执行/pprof 失败时补充上下文参数，新增任务启动与完成 Info 日志，logger 未初始化时错误输出到 stderr

#### bugFix:
1. 修复 `Writer` 重试分支事务未回滚导致连接泄漏，失败时显式 `Rollback` 后再重试
2. 修复 `ProgressCallback` panic/阻塞导致 `Execute` 退出卡死，增加 recover 与非阻塞 done 超时
3. 修复 `Writer` 的 IsFinished/RowAffects/CostTime 数据竞态，改为 atomic 类型
4. 修复 `getStopTime` 不响应 context 取消，补充 `ctx` 参数并检测 `ctx.Done()`
5. 修复 cleanup 中 channel 重复关闭 panic，使用 `sync.Once` 保护
6. 修复 `conf.NewConfig` 中 `os.Open` 失败时仍执行 `defer file.Close` 的潜在问题
7. 修复 `mysql/writer` 的 `getInfoFromTable` 未检查 `rows.Err()`
8. 修复 `procedure`、`slave_checker` 多处 `rows` 未关闭或未检查 `rows.Err()`

### v2.1.0(20251207)
#### feature:
1. 支持TiDB和OceanBase的分chunk DML功能，使用时添加参数`--no-slaves`
