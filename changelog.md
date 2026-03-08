### v3.0.0(20250308)
#### feature:
1. 新增使用文档入口：CLI 使用手册[`doc/design/cli-usage.md`](doc/design/cli-usage.md)、SDK 使用手册[`doc/design/sdk-usage.md`](doc/design/sdk-usage.md)
2. 新增 SDK 模式：根包 `oak` 暴露 `Executor`，支持 `NewExecutor`、`Run`、`Stop`、`UpdateSleep`、`UpdateMaxLag`、`GetStatus`，便于以库形式嵌入业务
3. 新增 `RateLimiter` 组件，封装令牌桶及 Sleep/MaxLag/Correct/NoConsiderLag，支持运行时动态调整
4. 新增 `log.NewFromSugaredLogger`，支持 SDK 侧注入已有 `*zap.SugaredLogger`
5. 新增 `NewSlaveCheckerWithContext`、`CheckVersionContext`，启动链路支持 context 取消
6. 项目结构调整：CLI 入口移至 `cmd/go-oak-chunk/main.go`，根包名改为 `oak`

#### optimization:
1. 全面去除 `os.Exit`/`log.Fatal`，改为返回 `error`，支持 SDK 优雅失败处理
2. 日志从 tinylog 迁移至 zap + lumberjack，合并 `StreamLogger`/`GlobalLogger` 为单一 `Logger`
3. DB 调用统一改为 `ExecContext`/`QueryContext`，取消信号可及时中断阻塞 I/O
4. `Procedure`/`SlaveChecker`/`Writer` 中查询路径补齐 `rows.Close`、`rows.Err` 收尾
5. Stop/Cancel 统一映射为 `task.ErrExecutionStopped`，CLI 层按正常停止处理
6. `Executor` 采用 one-shot 语义，二次 `Run` 返回 `ErrExecutorAlreadyRun`
7. `RateLimiter` 对 sleep/maxLag/correct 负值做 clamp 保护
8. CLI 日志初始化收敛为配置解析后一次性初始化
9. 完善 `run` 命令错误与流程日志：配置加载/预检/Executor 创建/任务执行/pprof 失败时补充上下文参数，新增任务启动与完成 Info 日志，logger 未初始化时错误输出到 stderr

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
