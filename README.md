# go-oak-chunk

[English](README.md) | [简体中文](README_CN.md)

## Documentation

- CLI user guide: [`doc/design/cli-usage-en.md`](doc/design/cli-usage-en.md)
- SDK user guide: [`doc/design/sdk-usage-en.md`](doc/design/sdk-usage-en.md)

## Background

When `oak-chunk-update` (DML tools) performs a DML change, its design requires a full-table scan. This makes DML operations on large tables very slow.

`pt-archiver` avoids a full-table scan, but it only supports `DELETE` and cannot perform `UPDATE` changes.

In addition, neither tool accepts ordinary SQL directly: one requires special fields, while the other only accepts a `WHERE` clause. Both create extra mental overhead for users.

For these reasons, `go-oak-chunk` was developed. It uses TiDB's `parser` module to parse DML statements, so users can write normal SQL without modifying it or providing only a partial statement. Its DML chunking logic is derived from `pt-archiver`, which avoids the possibility of a full-table scan.

## Notes

### 1. Replica-lag detection

The tool queries the primary database for replicas, attempts to connect to them, and checks their replication lag.

For a cloud-hosted cluster, it reports an error and then disables replica-lag detection before continuing with chunked DML. **Replica-lag detection has not been fully tested with TiDB topologies.** TiDB chunked DML and the `--tidb-rowid` cleanup capability are supported; for cloud-hosted environments or topologies without a standard primary/replica relationship, `--no-slaves` is recommended.

### 2. Using `sleep` and `noConsiderLag`

When executing chunked DML, the tool does not directly interrupt the writer goroutine with the `sleep` parameter. Instead, it uses a [token bucket](https://en.wikipedia.org/wiki/Token_bucket) based on that parameter to limit the writer goroutine.

At runtime, `go-oak-chunk` generates one token per millisecond. It then consumes tokens according to `actual interruption time * 1000` to implement rate limiting.

Because machine performance prevents the token bucket from keeping perfectly precise time, there may be a 1–5 ms error. A 50 ms correction is added both to absorb this error and to make the tool run slightly more conservatively.

The benchmark data below shows that the tool still performs very well with this 50 ms correction.

#### Actual interruption time

There are two cases:

A. When the replica-detection goroutine is operating normally

I. When `NotConsiderLag` is disabled

1. If `slaveLag <= c.Sleep`, bucket tokens should be associated with `slaveLag`; the lag may be eliminated without always reaching `c.Sleep`.
2. If `slaveLag > c.Sleep && slaveLag-c.Sleep > 60*n`, the actual wait time becomes `slaveLag+n`.

II. When `NotConsiderLag` is enabled

1. The actual wait time is capped at `c.Sleep`. To keep the logic predictable, after a user specifies `sleep`, the tool sleeps for a value in `(c.sleep-1, c.sleep]`.

B. When the replica-detection goroutine is not operating normally

A random value in `(c.sleep-1, c.sleep]` is used.

### 3. Row-rate limiting: `--rows-per-sec`

In addition to the token bucket (`sleep`/`max-lag`), the tool provides an optional cap on the number of rows processed per second. Both mechanisms apply together.

After each chunk is committed, the tool calculates the required wait from the number of rows affected by that chunk. `0` means unlimited.

### 4. Execution limits: `--max-rows` / `--max-duration-ms`

These limits apply uniformly to all strategies (default range chunking, covering-index fast path, and partition concurrency). Once the row or time limit is reached, the task **stops cleanly** rather than reporting an error.

The stopping granularity differs by strategy: range chunking stops at a transaction/chunk boundary; the covering-index path stops at a chunk boundary; partition concurrency trims the final batch to the remaining allowance. `--max-rows` is exact and never deletes more than the configured limit. `0` means unlimited.

### 5. OceanBase covering-index fast path and partition concurrency

- `--select-order-by` (optionally combined with `--select-index` / `--select-cursor`) enables a two-phase covering-index DELETE: first select candidate primary keys through the covering index, then delete by primary-key `IN`, avoiding a table lookup. **DELETE only.**
- `--partition-concurrency` builds on the fast path and deletes table partitions concurrently (**OceanBase only; the table must be partitioned**). Each worker owns one partition and its own cursor, and limits its statements with `PARTITION(...)`; values `<=1` fall back to a single worker.
  The rate limiter (token bucket plus `--rows-per-sec`) is shared by all workers, so the limit is **global**. A replica-lag pause also pauses all workers together.

### 6. TiDB `_tidb_rowid` cleanup (`--tidb-rowid`)

- TiDB-specific: use the hidden row handle `_tidb_rowid` as the chunking key, allowing TiDB **NONCLUSTERED** tables without a primary or unique key to run batched DELETE. The default RangeStrategy reports an error when it cannot find a PK/UK.
- The strategy uses a seek cursor plus `_tidb_rowid IN (...)` DELETE and handles sparse rowids caused by `SHARD_ROW_ID_BITS`/`AUTO_RANDOM`. It always reuses the frozen `WHERE` clause. **DELETE only.** This is an explicit opt-in and does not change TiDB's default behavior. It is mutually exclusive with the covering fast path, partition concurrency, and `--force-chunking-column`.
- Runtime applicability is validated, and CLUSTERED tables produce a clear error.

For more details, see the [CLI user guide](doc/design/cli-usage-en.md) and [SDK user guide](doc/design/sdk-usage-en.md).

## Usage

```bash
$ ./goc run --help
Start chunk dml

Usage:
  goc run [flags]

Examples:
goc run -c --config <config file>


Flags:
      --chunk-size int                 Number of rows to act on in chunks.
                                       Zero(0) means all rows updated in one operation.
                                       One(1) means update/delete one row everytime.
                                       The lower the number, the shorter any locks are held, but the more operations required and the more total running time. (default 1000)
  -c, --config string                  config file path
      --cpuprofile file                write cpu profile to file
  -d, --database string                Database name. Optional when --execute uses schema.table; if both are set, they must match
      --debug                          If debug_mode is true, print debug logs
      --dry-run                        Print sample SQL without executing
      --exclude-slaves string          which slaves should be include, include_slaves and exclude_slaves are mutually exclusive.
                                       ex: ip or ip1,ip2,... without port
  -e, --execute string                 Query to execute, which must contain where clause
      --force-chunking-column string   Columns to chunk by. Format: for single column keys, or column1_name,column2_name,...
  -h, --help                           help for run
  -H, --host string                    MySQL host (default "localhost")
      --include-slaves string          which slaves should be include, include_slaves and exclude_slaves are mutually exclusive.
                                       ex: ip or ip1,ip2,... without port
      --max-duration-ms int            Stop after this many milliseconds (0=unlimited)
      --max-lag int                    Pause chunk dml if the slave reach Threshold.
      --max-rows int                   Stop after acting on this many rows (0=unlimited)
      --memprofile file                write memory profile to file
      --no-slaves                      If true: don't calculate lags on slaves
      --noConsiderLag                  If true: sleep value will not be overshoot
                                       false: if slave lag is very high, sleep will be overshoot
      --partition-concurrency int      OceanBase only: run the covering-index DELETE across table partitions with this many parallel workers (0/1=off)
  -p, --password string                MySQL password
  -P, --port int                       TCP/IP port (default 3306)
      --preflight-threshold int        EXPLAIN large-table confirmation threshold (0=default 100000)
      --print-progress                 Show number of affected rows during utility runtime
      --rows-per-sec int               Cap rows acted on per second (0=unlimited)
      --select-cursor                  Use a cursor to advance the candidate SELECT, avoiding a re-scan from the start
      --select-index string            FORCE INDEX name for the two-phase candidate SELECT (covering index strategy)
      --select-order-by string         Order columns for the two-phase candidate SELECT (comma-separated). Enables covering-index fast-path (DELETE only)
      --sleep int                      Number of milliseconds to sleep between chunks.
      --tidb-rowid                     TiDB only: chunk DELETE by the hidden _tidb_rowid handle (NONCLUSTERED tables, no PK/UK required)
      --txn-size int                   Number of rows per transaction. (default 1000)
  -u, --user string                    MySQL user (default "root")
      --yes                            Skip the large-table confirmation prompt
```

> `--database` may be omitted when `--execute` uses a fully qualified `schema.table`. If both are provided, they must match or `goc` stops before opening the database connection.

Sample:
```bash
# Print verbose logs and debug information
$ ./goc run --chunk-size 1000 --txn-size 2000 -d test \
--execute "update mybenchx1 set k = 1 where created_at <= '2023-12-28 11:30:06'" \
--host 127.0.0.1 --port 3306 \
--user root --password 'xxx' \
--print-progress --debug

# Add sleep and noConsiderLag
$ ./goc run --chunk-size 1000 --txn-size 2000 -d test \
--execute "delete from mybenchx0 where created_at <= '2024-02-21 00:03:13'" \
--host 127.0.0.1 --port 3306 \
--user root --password 'xxx' \
--sleep 1 --noConsiderLag

# Cap rows per second (combined with sleep / max-lag) and set a row limit
$ ./goc run --chunk-size 1000 -d test \
--execute "delete from mybenchx0 where created_at <= '2024-02-21 00:03:13'" \
--host 127.0.0.1 --port 3306 --user root --password 'xxx' \
--rows-per-sec 50000 --max-rows 2000000

# OceanBase covering-index fast path plus partition-concurrent DELETE
$ ./goc run --chunk-size 1000 -d test \
--execute "delete from mybenchx0 where created_at <= '2024-02-21 00:03:13'" \
--host 127.0.0.1 --port 3306 --user root --password 'xxx' \
--select-order-by created_at --select-cursor \
--partition-concurrency 4

# TiDB: chunk DELETE on a table without a primary key using _tidb_rowid
$ ./goc run --tidb-rowid --chunk-size 1000 -d test \
--execute "delete from rule_set_exe_history where create_time <= date_sub(now(), interval 15 day)" \
--host 127.0.0.1 --port 4000 --user root --password 'xxx' \
--print-progress
```

## Benchmark Results

Benchmark commands:

```bash
# goc
$ ./goc run --chunk-size 1000 --txn-size 2000 -d test \
--execute "update mybenchx0 set k = 1 where created_at <= '2024-02-20 11:03:13'" \
--host 127.0.0.1 --port 3306 \
--user root --password 'xxx' \
--print-progress

# oak-chunk-update.py
$ python oak-chunk-update.py -H 127.0.0.1 -P 3306 -u root -p 'xxx' \
-d test --chunk-size=1000 --slave-lag 999 -v \
-e "update mybenchx0 set k = 1 where created_at <= '2024-02-20 11:03:13' and OAK_CHUNK(mybenchx0)"

# pt-archiver
$ pt-archiver --source h=127.0.0.1,u=root,p='xxx',P=3306,D=test,t=mybenchx0 \
--no-check-charset --where "created_at <= '2024-02-20 11:03:13'" \
--limit 1000 --txn-size 2000 --purge --progress=1000 --bulk-delete --statistics
```

Benchmark table schema

Test tool: https://github.com/SisyphusSQ/mybenchx
```sql
CREATE TABLE `mybenchx0` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `k` bigint(20) NOT NULL DEFAULT '0',
  `c` varchar(120) COLLATE utf8_bin NOT NULL DEFAULT '',
  `pad` varchar(60) COLLATE utf8_bin NOT NULL DEFAULT '',
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `unix_stamp` bigint(20) NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  KEY `idx_created_at` (`created_at`),
  KEY `idx_unix_stamp` (`unix_stamp`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COLLATE=utf8_bin
```

### Update performance tests

#### RDS benchmark: 3,713,388 rows (5,448,241 total)

| #                | Parameters                       | Time       | Notes                                                        |
|------------------|----------------------------------|------------|--------------------------------------------------------------|
| goc              | chunk-size=1 txn-size=1000      | incomplete | Single-row deletion, not a range deletion                    |
|                  |                                  |            | 56m21s    660000                                             |
| goc              | chunk-size=1000 txn-size=1000  | 328s       | Total Processed Rows: 3713388, speed: 11250.09 rows/s       |
| goc              | chunk-size=1000 txn-size=2000  | 211s       | Total Processed Rows: 3713388, speed: 17429.37 rows/s       |
| goc              | chunk-size=2000 txn-size=4000  | 156s       | Total Processed Rows: 3713388, speed: 23349.05 rows/s       |
| goc              | chunk-size=3000 txn-size=6000  | 147s       | Total Processed Rows: 3713388, speed: 24748.83 rows/s       |
| oak-chunk-update | chunk-size=1000                | 288.2s     | Default parameters                                           |
|                  |                                  |            | 3713388 accumulating; seconds: 288.2 elapsed; 119.39 executed |
| oak-chunk-update | chunk-size=2000                | 207.4s     | 3713388 accumulating; seconds: 207.4 elapsed; 107.46 executed |
| oak-chunk-update | chunk-size=3000                | 185.3s     | 3713388 accumulating; seconds: 185.3 elapsed; 104.32 executed |
| goc              | chunk-size=1000 txn-size=2000  | 1116s      | sleep is a random value in (0,1]                         |
|                  | sleep=1(s)                      |            | Total Processed Rows: 3713388, speed: 3317.72 rows/s         |
| oak-chunk-update | chunk-size=1000 sleep=1000(ms) | 5724.3s    | sleep is fixed at 1s                                      |
|                  |                                  |            | 3713388 accumulating; seconds: 5724.3 elapsed; 113.64 executed |

![Public-cloud UPDATE benchmark](doc/cloud_update.png)

#### Private-cloud benchmark: 2,044,230 rows (5,405,000 total)

| Tool               | Parameters                              | Time  | Notes                                                         |
|--------------------|-----------------------------------------|-------|---------------------------------------------------------------|
| goc              | chunk-size=1 txn-size=1000              | /     |                                                               |
| goc              | chunk-size=1000 txn-size=1000           | 114s  | Total Processed Rows: 2044230, speed: 17466.49 rows/s         |
| goc              | chunk-size=1000 txn-size=2000           | 63s   | Total Processed Rows: 2044230, speed: 30963.93 rows/s         |
| goc              | chunk-size=2000 txn-size=4000           | 36s   | Total Processed Rows: 2044230, speed: 52393.56 rows/s         |
| goc              | chunk-size=3000 txn-size=6000           | 27s   | Total Processed Rows: 2044230, speed: 68106.68 rows/s         |
| oak-chunk-update | chunk-size=1000                         | 696.0s| Default parameters                                           |
|                  |                                         |       | 2044230 accumulating; seconds: 696.0 elapsed; 27.03 executed |
| oak-chunk-update | chunk-size=2000                         | 353.8s| 2044230 accumulating; seconds: 353.8 elapsed; 18.23 executed |
| oak-chunk-update | chunk-size=3000                         | 239.2s| 2044230 accumulating; seconds: 239.2 elapsed; 15.33 executed |
| goc              | chunk-size=1000 txn-size=2000 sleep=1(s)| /     | sleep is a random value in (0,1]                              |
| oak-chunk-update | chunk-size=1000 sleep=1000(ms)          | incomplete | sleep is fixed at 1s                                       |
|                  |                                         |          | 1302922 accumulating; seconds: 3839.7 elapsed; 17.06 executed |

![Private-cloud UPDATE benchmark](doc/private_update.png)

### Delete performance tests

#### RDS benchmark: 3,713,388 rows (5,448,241 total)

| Tool               | Parameters                              | Time  | Notes                                                         |
|--------------------|-----------------------------------------|-------|---------------------------------------------------------------|
| goc              | chunk-size=1 txn-size=1000              | /     | Single-row deletion, not a range deletion                    |
| goc              | chunk-size=1000 txn-size=1000           | 334s  | Total Processed Rows: 3713388, speed: 11049.12 rows/s         |
| goc              | chunk-size=1000 txn-size=2000           | 282s  | Total Processed Rows: 3713388, speed: 13026.09 rows/s         |
| goc              | chunk-size=2000 txn-size=4000           | 252s  | Total Processed Rows: 3713388, speed: 14558.70 rows/s         |
| goc              | chunk-size=3000 txn-size=6000           | 246s  | Total Processed Rows: 3713388, speed: 14908.98 rows/s         |
| oak-chunk-update | chunk-size=1000                         | 377.1s| Default parameters                                           |
|                  |                                         |       | 3713388 accumulating; seconds: 377.1 elapsed; 153.91 executed |
| oak-chunk-update | chunk-size=2000                         | 312.5s| 3713388 accumulating; seconds: 312.5 elapsed; 152.46 executed |
| oak-chunk-update | chunk-size=3000                         | 284.5s| 3713388 accumulating; seconds: 284.5 elapsed; 159.96 executed |
| pt-archiver      | limit=1000 txn-size=1000                | 457s  | Started at 2024-02-20T17:01:47, ended at 2024-02-20T17:09:24 |
|                  |                                         |       | Source: D=test,P=3306,h=127.0.0.1,p=...,t=mybenchx0,u=root    |
|                  |                                         |       | SELECT 3713387                                               |
|                  |                                         |       | INSERT 0                                                     |
|                  |                                         |       | DELETE 3713387                                               |
|                  |                                         |       | Action             Count       Time        Pct                |
|                  |                                         |       | bulk_deleting       3714   175.9435      38.43               |
|                  |                                         |       | select              3715    59.2423      12.94               |
|                  |                                         |       | commit               3714    24.2375      5.29                |
|                  |                                         |       | other                  0    198.3879      43.33               |
| pt-archiver      | limit=1000 txn-size=2000                | 439s  | Started at 2024-02-20T17:15:00, ended at 2024-02-20T17:22:20 |
|                  |                                         |       | Source: D=test,P=3306,h=127.0.0.1,t=mybenchx0,u=root   |
|                  |                                         |       | SELECT 3713387                                               |
|                  |                                         |       | INSERT 0                                                     |
|                  |                                         |       | DELETE 3713387                                               |
|                  |                                          |       | Action             Count       Time        Pct                |
|                  |                                          |       | bulk_deleting       3714   146.6919      33.40               |
|                  |                                          |       | select              3715    65.5630      14.93               |
|                  |                                          |       | commit               1857    13.6087       3.10               |
|                  |                                          |       | other                  0    213.3417      48.57               |
| pt-archiver      | limit=2000 txn-size=4000                | 422s  | Started at 2024-02-20T17:28:00, ended at 2024-02-20T17:35:02 |
|                  |                                         |       | Source: D=test,P=3306,h=127.0.0.1,t=mybenchx0,u=root   |
|                  |                                         |       | SELECT 3713387                                               |
|                  |                                         |       | INSERT 0                                                     |
|                  |                                         |       | DELETE 3713387                                               |
|                  |                                         |       | Action             Count       Time        Pct               |
|                  |                                         |       | bulk_deleting       1857   162.3538      38.43               |
|                  |                                         |       | select               1858    48.0480       11.37               |
|                  |                                         |       | commit               929      7.9062       1.87               |
|                  |                                         |       | other                  0    204.1619      48.33               |
| pt-archiver      | limit=3000 txn-size=6000                | 410s  | Started at 2024-02-20T17:54:20, ended at 2024-02-20T18:01:11 |
|                  |                                         |       | Source: D=test,P=3306,h=127.0.0.1,t=mybenchx0,u=root   |
|                  |                                         |       | SELECT 3713387                                               |
|                  |                                         |       | INSERT 0                                                     |
|                  |                                         |       | DELETE 3713387                                               |
|                  |                                         |       | Action             | Count       | Time        | Pct                |
|                  |                                         |       | bulk_deleting       | 1238   161.6140      | 39.33                |
|                  |                                         |       | select              | 1239    35.6418      | 8.67                 |
|                  |                                         |       | commit              | 619     6.1584       | 1.50                 |
|                  |                                         |       | other               | 0      207.4860      | 50.50                |
| goc              | chunk-size=1000 txn-size=2000 sleep=1(s)| /     | sleep is a random value in (0,1]                              |
| oak-chunk-update | chunk-size=1000 sleep=1000(ms)          | /     | sleep is fixed at 1s                                        |

![Public-cloud DELETE benchmark](doc/cloud_delete.png)

#### Private-cloud benchmark: 2,044,230 rows (5,405,000 total)

| Tool               | Parameters                              | Time  | Notes                                                         |
|--------------------|-----------------------------------------|-------|---------------------------------------------------------------|
| goc              | chunk-size=1 txn-size=1000              | /     | Single-row deletion, not a range deletion                    |
| goc              | chunk-size=1000 txn-size=1000           | 123s  | Total Processed Rows: 2044230, speed: 16219.34 rows/s         |
| goc              | chunk-size=1000 txn-size=2000           | 72s   | Total Processed Rows: 2044230, speed: 27248.78 rows/s         |
| goc              | chunk-size=2000 txn-size=4000           | 42s   | Total Processed Rows: 2044230, speed: 45407.27 rows/s         |
| goc              | chunk-size=3000 txn-size=6000           | 31s   | Total Processed Rows: 2044230, speed: 61923.60 rows/s         |
| oak-chunk-update | chunk-size=1000                         | 702.4s| Default parameters                                           |
|                  |                                         |       | 2043463 accumulating; seconds: 702.4 elapsed; 36.43 executed |
| oak-chunk-update | chunk-size=2000                         | 360.9s| 2044230 accumulating; seconds: 360.9 elapsed; 26.77 executed |
| oak-chunk-update | chunk-size=3000                         | 248.9s| 2044230 accumulating; seconds: 248.9 elapsed; 23.61 executed |
| pt-archiver      | limit=1000 txn-size=1000                | 93s   | Started at 2024-02-20T18:06:04, ended at 2024-02-20T18:07:37 |
|                  |                                         |       | Source: D=test,P=3306,h=127.0.0.1,p=...,t=mybenchx0,u=root   |
|                  |                                         |       | SELECT 2044230                                               |
|                  |                                         |       | INSERT 0                                                     |
|                  |                                         |       | DELETE 2044230                                               |
|                  |                                         |       | Action             Count       Time        Pct               |
|                  |                                         |       | bulk_deleting       | 2045    19.9001      21.25               |
|                  |                                         |       | select              | 2046     7.4088       7.91                |
|                  |                                         |       | commit              | 2045     4.4744       4.78                |
|                  |                                         |       | other               | 0      61.8726       66.06                |
| pt-archiver      | limit=1000 txn-size=2000                | 88s   | Started at 2024-02-20T18:09:32, ended at 2024-02-20T18:11:00 |
|                  |                                         |       | Source: D=test,P=3306,h=127.0.0.1,t=mybenchx0,u=root   |
|                  |                                         |       | SELECT 2044230                                               |
|                  |                                         |       | INSERT 0                                                     |
|                  |                                         |       | DELETE 2044230                                               |
|                  |                                         |       | Action             Count       Time       Pct                |
|                  |                                         |       | bulk_deleting       | 2045    18.1700      20.60                |
|                  |                                         |       | select              | 2046     6.8679       7.79                 |
|                  |                                         |       | commit               | 1023     2.9333       3.33                 |
|                  |                                         |       | other               | 0      60.2291       68.29                |
| pt-archiver      | limit=2000 txn-size=4000                | 82s   | Started at 2024-02-20T18:13:26, ended at 2024-02-20T18:14:48 |
|                  |                                         |       | Source: D=test,P=3306,h=127.0.0.1,t=mybenchx0,u=root   |
|                  |                                         |       | SELECT 2044230                                               |
|                  |                                         |       | INSERT 0                                                     |
|                  |                                         |       | DELETE 2044230                                               |
|                  |                                         |       | Action             Count       | Time       | Pct                |
|                  |                                         |       | bulk_deleting       | 1023   16.0654       | 19.45               |
|                  |                                         |       | select               | 1024    5.5288        | 6.69                |
|                  |                                         |       | commit               | 512     2.1804        | 2.64                |
|                  |                                         |       | other               | 0       58.8304        | 71.22               |
| pt-archiver      | limit=3000 txn-size=6000                | 79s   | Started at 2024-02-20T18:16:42, ended at 2024-02-20T18:18:01 |
|                  |                                         |       | Source: D=test,P=3306,h=127.0.0.1,t=mybenchx0,u=root   |
|                  |                                         |       | SELECT 2044230                                               |
|                  |                                         |       | INSERT 0                                                     |
|                  |                                         |       | DELETE 2044230                                               |
|                  |                                         |       | Action             | Count       | Time       | Pct                |
|                  |                                         |       | bulk_deleting       | 682   15.2692      | 19.14                |
|                  |                                         |       | select               | 683    4.8427       | 6.07                 |
|                  |                                         |       | commit               | 341    1.8205       | 2.28                 |
|                  |                                         |       | other               | 0      57.8531       | 72.51                |
| goc              | chunk-size=1000 txn-size=2000 sleep=1(s)| /     | sleep is a random value in (0,1]                              |
| oak-chunk-update | chunk-size=1000 sleep=1000(ms)          | /     | sleep is fixed at 1s                                        |

![Private-cloud DELETE benchmark](doc/private_delete.png)

## Conclusion

| # | Update | Delete |
|---|---|---|
| goc | Private-cloud UPDATE is more efficient than public-cloud UPDATE. | DELETE is less efficient than UPDATE. |
| | Increasing chunk-size and txn-size clearly improves performance, but a bottleneck becomes noticeable at chunk-size=3000. | |
| | sleep uses a random value in (0,x], so it is always more efficient than a fixed value of x. Whether this causes replica lag in public-cloud environments is unknown. | |
| | In private-cloud environments, once replica lag occurs, x is the lower bound of sleep, so this situation does not occur. | |
| oak | UPDATE performance is similar in private- and public-cloud environments. | DELETE is clearly worse than goc. |
| | When the rows to update are a small fraction of the whole table, oak becomes particularly slow because it scans the entire table. | Full-table scan. |
| | If replica-lag detection is disabled and sleep has a fixed x value, oak always sleeps for x, resulting in very low throughput. | |
| pt-archiver | / | Avoids a full-table scan. |
| | | pt's DELETE performance is very good in private-cloud environments, but not as good in public-cloud environments. |

Summary:

- goc can be considered faster than oak and pt-archiver, while its pt-archiver-based implementation avoids the possibility of a full-table scan.
- Under heavier load, goc performs better than pt-archiver; under lighter load, pt-archiver can be better.
- In some cases, goc saves up to six times the execution time compared with oak.
- goc exposes fewer parameters than the other two tools.
