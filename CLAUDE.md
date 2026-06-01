# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`go-oak-chunk` (binary `goc`) chunks large `UPDATE`/`DELETE` DML on MySQL-like databases (MySQL, TiDB, OceanBase) without full-table scans. It parses the user's SQL with the TiDB parser, derives a primary/unique key to chunk on, and walks the table key-range by key-range (logic adapted from `pt-archiver`'s chunking). It can be driven from the CLI or embedded as an SDK (the root `oak` package).

## Commands

```bash
make build          # build ./goc (injects version vars via -ldflags)
make test           # go test ./...
make race           # go test -race ./...
make vet            # go vet ./...
make linux/darwin   # cross-compile goc.linux.amd64 / goc.darwin.arm64
make release        # clean + linux + darwin + test

go test ./mysql/ -run TestName -v          # single test
go test ./task/... -run TestRateLimiter    # one package + pattern
```

Module is `github.com/SisyphusSQ/go-oak-chunk/v3` — the `/v3` suffix is part of every internal import path. Requires Go 1.23.

## Architecture

The run pipeline is three goroutines coordinated by `task.Execute` (`task/task.go`), the central orchestrator. Two SDK entry points feed into it: CLI (`cmd/run.go` → `oak.NewExecutor` → `Executor.Run`) and direct SDK (`oak.go`). `task.RunTask` is a simpler context-less entry that also funnels into `Execute`.

The three goroutines, communicating over channels:

1. **Producer** (`mysql/procedure.go`, `Procedure.BuildSQL`) — runs `SELECT` queries on the chunking key to compute each chunk's key-range, then pushes `*Producer` items (a WHERE-clause fragment + key boundary values) onto `Writer.ProducerQueue` (buffered, 1000). A `Producer{IsFinished:true}` sentinel signals end-of-data. Chunk SQL shape differs by `chunk-size`: `==1` selects one key at a time (bulk exec WHERE), `>1` fetches first+last key of each range (BETWEEN-style WHERE). `chunk-size==0` means one statement for the whole set.
2. **Writer** (`mysql/writer.go`, `Writer.Write`) — pulls from `ProducerQueue`, assembles `ExecuteSQL + WhereClause` with bound values, and runs them inside transactions bounded by `txn-size`. Throttling: before each transaction it drains `bucketNum` for the latest token count and calls `bucket.Wait(...)` on a `juju/ratelimit` token bucket. Retry-on-failure only happens when no rows have been applied in the current tx yet (can't safely replay consumed chunks mid-transaction).
3. **getStopTime** (`task/task.go`) — the throttle controller. Polls slave lag via the lag checker, computes a token count through `RateLimiter`, and feeds it to the Writer over `bucketNum`. The sentinel `vars.LagThreshold` (`-1`) tells the Writer to pause ~1s to let slaves catch up.

`Execute` selects over per-goroutine error channels plus a `tasksDoneChan`; any error or `ctx.Done()` triggers `cancel()` + `cleanup()` (closes slave checker and MySQL client) exactly once via `sync.Once`. `context.Canceled` is normalized to `task.ErrExecutionStopped` so the CLI can treat SIGINT/SIGTERM as a clean stop.

### Rate limiting / throttle (`task/rate_limiter.go`)

This is the subtle part — see README's "实际中断时间" section for the design rationale. A `juju/ratelimit` bucket produces 1 token/ms. `getStopTime` decides how many tokens the Writer must wait for each cycle, derived from `sleep`, current slave `lag`, `noConsiderLag`, and a `Correct` fudge factor (default 50ms) that's added under throttle and decayed back down over time. `bucketHandle` overshoots `sleep` when lag is high (unless `noConsiderLag`); `bucketErrHandle` returns a randomized `(sleep-1, sleep]` value when the lag checker is unavailable. All fields are atomics — the SDK can mutate `sleep`/`maxLag` live via `Executor.UpdateSleep`/`UpdateMaxLag`.

### Slave lag detection (`task/lag_checker/slave_checker.go`)

On startup, queries the master for slave hosts (`SHOW SLAVE HOSTS` / `SHOW REPLICA HOSTS` depending on version), connects to each (honoring `--include-slaves`/`--exclude-slaves`), and polls `Seconds_Behind_Master`. Version ≥ 8.0.22 uses the `REPLICA` spelling. A slave that errors is marked `canSkip` and dropped. On managed/cloud clusters where slave discovery fails, `Execute` logs the error, sets the checker to `nil`, and continues without lag detection (throttle falls back to `bucketErrHandle`).

### Metadata & key selection (`mysql/writer.go` `getInfoFromTable`)

`NewWriter` → `preCheck` parses the SQL (via XiaoMi/soar's `TiParse` + pingcap parser AST visitor in `mysql/meta.go`), confirms the table exists, then resolves the chunking key. Data source is detected from `SELECT version()` (`detectDataSourceFromVersion`). Keys come from `SHOW CREATE TABLE` for MySQL/TiDB; **OceanBase** additionally enables `_show_ddl_in_compat_mode` and prefers `SHOW INDEX` results (its `SHOW CREATE TABLE` may omit keys). `--force-chunking-column` must match an existing PK/UK exactly (column-set comparison, case-insensitive); otherwise PK is preferred, else the first unique key.

## Conventions

- Logging is the package-level `log.Logger` (zap, in the `log/` package). Init with `log.New(...)`; `NewExecutor`/`Execute` guard with `log.IsConfigured()`. Use `Debugf` for the verbose chunk/SQL trace, gated by `--debug`.
- SQL strings live as `fmt.Sprintf` templates in `vars/const.go`; constraint-type constants (`ConstraintPrimaryKey`, etc.) mirror the pingcap parser's enum.
- `Writer` exposes its mutable counters (`rowAffects`, `costTimeNanos`, `isFinished`) as atomics read by the progress goroutines — don't access them directly across goroutines.
- Progress reporting has two modes: CLI `--print-progress` (`task.PrintProgress`, draws to terminal via tcell) and SDK `WithProgressCallback` (async, single-flight guarded). Don't block in a progress callback.
- User-facing docs: `doc/design/cli-usage.md` and `doc/design/sdk-usage.md`.
