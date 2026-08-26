package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	_ "github.com/pingcap/tidb/parser/test_driver"

	"github.com/SisyphusSQ/go-oak-chunk/v3/conf"
	"github.com/SisyphusSQ/go-oak-chunk/v3/internal/retry"
	"github.com/SisyphusSQ/go-oak-chunk/v3/log"
	"github.com/SisyphusSQ/go-oak-chunk/v3/vars"
)

type Writer struct {
	MysqlClient       *sql.DB
	ExecuteSQL        string
	OriginWhereClause string
	ChunkSize         int64
	TxnSize           int64
	SqlType           string
	Database          string
	Table             string
	noLogBing         bool
	unqKeys           *UnqKeys
	dataSource        dataSourceType
	ProducerQueue     chan *Producer

	// MaxRows 在累计影响行数达到该值后停止(0=不限)。停止发生在事务边界,
	// 实际影响行数可能溢出 MaxRows 至多一个 txn-size, 与 OBCovering 的粗粒度语义一致。
	MaxRows int64
	// MaxDuration 在墙钟时长达到该值后停止(0=不限)。
	MaxDuration time.Duration

	isFinished    atomic.Bool
	rowAffects    atomic.Int64
	costTimeNanos atomic.Int64
}

type UnqKeys struct {
	UniqueKeyColumns []string
	CountColumns     int
	UniqueKeyTypes   []byte
	IsNull           []bool
	Tp               int
}

type Producer struct {
	WhereClause      string
	IsFinished       bool
	CurrentKeyValues []*KeyValue
}

type Proceed struct {
	WhereClause string
	RangeStarts []int64
	RangeEnds   []int64
	IsFinished  bool
}

type dataSourceType string

const (
	dataSourceMySQL     dataSourceType = "mysql"
	dataSourceTiDB      dataSourceType = "tidb"
	dataSourceOceanBase dataSourceType = "oceanbase"

	queryVersionSQL   = "select version()"
	dbNowLiteralSQL   = "SELECT DATE_FORMAT(NOW(), '%Y-%m-%d %H:%i:%s')"
	obCompatModeOnSQL = "SET SESSION _show_ddl_in_compat_mode = true"
	obShowIndexSQL    = "show index from %s"
)

type columnMeta struct {
	columnType byte
	isNull     bool
}

type showIndexRow struct {
	keyName    string
	seqInIndex int64
	columnName string
	nonUnique  int64
}

func NewWriter(c *conf.Config) (*Writer, error) {
	w := &Writer{
		noLogBing:     c.NoLogBin,
		ChunkSize:     c.ChunkSize,
		TxnSize:       c.TxnSize,
		ExecuteSQL:    strings.ReplaceAll(c.ExecuteQuery, ";", ""),
		ProducerQueue: make(chan *Producer, 1000),
		MaxRows:       c.MaxRows,
		MaxDuration:   time.Duration(c.MaxDuration) * time.Millisecond,
	}
	w.SetCostTime(1 * time.Second)

	if err := w.preCheck(c); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *Writer) preCheck(c *conf.Config) error {
	var err error

	if err = w.resolveTarget(c); err != nil {
		return fmt.Errorf("failed to resolve target table: %w", err)
	}

	// init mysql connect
	w.MysqlClient, err = NewMysqlClient(c)
	if err != nil {
		return fmt.Errorf("open mysql connection failed: %w", err)
	}

	exists, err := w.tableExists()
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("table %s.%s does not exist", w.Database, w.Table)
	}

	err = w.getInfoFromTable(c)
	if err != nil {
		return fmt.Errorf("sql parser failed, please check sql: %w", err)
	}

	return nil
}

func (w *Writer) resolveTarget(c *conf.Config) error {
	ref, err := TargetTableMeta(w.ExecuteSQL)
	if err != nil {
		return fmt.Errorf("failed to parse table info: %w", err)
	}

	database, err := resolveTargetDatabase(c.Database, ref.Schema)
	if err != nil {
		return err
	}

	w.Database = database
	w.Table = ref.Table
	// Config is shared with the task and slave-lag paths. Normalize it once so
	// every connection and status message uses the same resolved database.
	c.Database = database
	return nil
}

func (w *Writer) Write(ctx context.Context, bucket Bucket, bucketNum <-chan int64, rowsLimiter RowsLimiter) error {
	runStart := time.Now()
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		// get last bucket number
		var bucketCount int64
		for i := 0; i < len(bucketNum); i++ {
			select {
			case bucketCount = <-bucketNum:
			default:
			}
		}
		if bucketCount == vars.LagThreshold {
			log.Logger.Debug("Sleep 1s to let slave eliminate lag")
			bucket.Wait(1000)
			continue
		}

		log.Logger.Debugf("bucketCount: %d", bucketCount)
		bucket.Wait(bucketCount)

		var rowAffects int64
		beginTime := time.Now()
		tx, err := w.MysqlClient.BeginTx(ctx, nil)
		if err != nil {
			return err
		}

		shouldFinish := false
	consume:
		for {
			if w.TxnSize > 0 && rowAffects >= w.TxnSize {
				break
			}

			var pr *Producer
			select {
			case <-ctx.Done():
				_ = tx.Rollback()
				return nil
			case pr = <-w.ProducerQueue:
			}
			if pr == nil {
				continue
			}

			if pr.IsFinished {
				log.Logger.Debug("Get whereClause is finished")
				w.SetFinished()
				shouldFinish = true
				break consume
			}

			// 在这里组装完sql和参数后，传到writer中去
			execSql := w.ExecuteSQL + pr.WhereClause
			values := getColumnValue(pr.CurrentKeyValues, w.ChunkSize)

			log.Logger.Debugf("execSql: %s", execSql)
			log.Logger.Debugf("parma values: %v", values)

			res, errEx := tx.ExecContext(ctx, execSql, values...)
			if errEx != nil {
				_ = tx.Rollback()
				// 在事务里执行过多条语句后再失败，无法安全重放已消费的 chunk，直接失败返回。
				if rowAffects > 0 {
					return fmt.Errorf("execute sql failed after %d affected rows in current tx: %w", rowAffects, errEx)
				}

				// 单 chunk 失败时，按错误分类做指数退避重试，每次重试都是独立事务。
				var finalRes sql.Result
				retryErr := retry.WithRetry(ctx, retry.DefaultPolicy(), func(attempt int) error {
					if attempt > 1 {
						log.Logger.Debugf("retry chunk execution (attempt %d)", attempt)
					}

					retryTx, txErr := w.MysqlClient.BeginTx(ctx, nil)
					if txErr != nil {
						return txErr
					}

					r, execErr := retryTx.ExecContext(ctx, execSql, values...)
					if execErr != nil {
						_ = retryTx.Rollback()
						return execErr
					}

					if commitErr := retryTx.Commit(); commitErr != nil {
						_ = retryTx.Rollback()
						return commitErr
					}

					finalRes = r
					return nil
				})

				if retryErr != nil {
					return fmt.Errorf("execute chunk sql failed after retry: %w", retryErr)
				}

				// 重试已在独立事务里提交，外层事务已回滚，重开一个供后续 chunk 使用。
				tx, err = w.MysqlClient.BeginTx(ctx, nil)
				if err != nil {
					return err
				}

				res = finalRes
			}

			// 算一下chunk-size和txn-size之间的关系
			affects, _ := res.RowsAffected()
			rowAffects += affects
		}

		// 速度的控制应该在txnSize
		// pt-archiver是在事务结束(commit)之后，才进行sleep
		err = tx.Commit()
		if err != nil {
			return err
		}
		w.AddRowAffects(rowAffects)
		w.SetCostTime(time.Since(beginTime))

		// 行级限流: 按本事务实际影响行数等待 rows/rowsPerSec 秒。
		// ctx 取消时按既有的 clean-stop 语义返回 nil。
		if rowsLimiter != nil {
			if err = rowsLimiter.Wait(ctx, rowAffects); err != nil {
				return nil
			}
		}

		// 护栏: max-rows / max-duration 达到后干净停止(置 finish, 不报错),
		// 与 OBCoveringStrategy.stopReached 语义一致。检查在事务提交后进行,
		// 因此 max-rows 可能溢出至多一个 txn-size。
		if w.MaxRows > 0 && w.GetRowAffects() >= w.MaxRows {
			log.Logger.Infof("max-rows reached (%d), stopping", w.MaxRows)
			w.SetFinished()
			return nil
		}
		if w.MaxDuration > 0 && time.Since(runStart) >= w.MaxDuration {
			log.Logger.Infof("max-duration reached (%s), stopping", w.MaxDuration)
			w.SetFinished()
			return nil
		}

		// finish flag
		if shouldFinish || w.IsFinished() {
			log.Logger.Debug("Execute SQL is finished successfully")
			return nil
		}
	}
}

func (w *Writer) tableExists() (bool, error) {
	var count int
	err := w.MysqlClient.QueryRow(vars.TableExistsSQL, w.Database, w.Table).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("tableExists scan failed: %w", err)
	}
	return count == 1, nil
}

// getInfoFromTable use tidb parser to build necessary info
// fetch sql type
// fetch index
func (w *Writer) getInfoFromTable(c *conf.Config) error {
	// First get sql type, update or delete?
	sqlStmt, _, err := parser.New().ParseSQL(w.ExecuteSQL)
	if err != nil {
		return err
	}

	if len(sqlStmt) == 0 || len(sqlStmt) > 1 {
		return fmt.Errorf("sql is empty or sql number is over 1, please confirm only one sql is provided")
	}

	node := sqlStmt[0]
	v := &visitor{}
	var whereNode ast.ExprNode
	switch n := node.(type) {
	case *ast.DeleteStmt:
		w.SqlType = "Delete"
		node.Accept(v)
		whereNode = n.Where

		w.ExecuteSQL = fmt.Sprintf("DELETE FROM %s WHERE ", QualifiedTableName(w.Database, w.Table))
	case *ast.UpdateStmt:
		w.SqlType = "Update"
		node.Accept(v)
		whereNode = n.Where

		re := regexp.MustCompile(`set.*where|SET.*WHERE|set.*WHERE|SET.*where`)
		sub := re.FindString(c.ExecuteQuery)
		w.ExecuteSQL = fmt.Sprintf("UPDATE %s %s ", QualifiedTableName(w.Database, w.Table), sub)
	default:
		return fmt.Errorf("please confirm sql type is update or delete")
	}

	// Metadata and DB-session-derived literals are read through one connection so
	// session state (timezone, OB compatibility mode) stays coherent.
	ctx := context.Background()
	conn, dataSource, err := w.openMetadataConn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	w.dataSource = dataSource

	if v.whereClause != "" {
		whereClause := v.whereClause
		// freeze NOW()-like functions so every chunk uses the same timestamp
		if HasFreezeNowFunc(whereNode) {
			dbNowLiteral, nowErr := w.fetchDBNowLiteral(ctx, conn)
			if nowErr != nil {
				return nowErr
			}
			frozenWhere, freezeErr := FreezeNowLiteral(whereNode, dbNowLiteral)
			if freezeErr != nil {
				return fmt.Errorf("freeze NOW() failed: %w", freezeErr)
			}
			whereClause = frozenWhere
		}

		// avoid where clause "or", make program confused
		w.OriginWhereClause = fmt.Sprintf("(%s)", whereClause)
		log.Logger.Debugf("originWhereClause: [%s]", whereClause)

		w.ExecuteSQL += w.OriginWhereClause
	}

	forceColumns, err := normalizeForceChunkingColumns(c.ForceChunkingColumn)
	if err != nil {
		return err
	}

	// Second find primary/unique index which can be used
	// check for column in Table meta
	ns := QualifiedTableName(w.Database, w.Table)

	if c.TiDBRowID {
		// _tidb_rowid 模式不依赖 PK/UK, 跳过唯一键解析;
		// 适用性(NONCLUSTERED) 由 TiDBRowIDStrategy.Run 在运行时校验。
		return nil
	}

	if dataSource == dataSourceOceanBase {
		if err = w.enableOceanBaseDDLCompatMode(ctx, conn); err != nil {
			return err
		}
	}

	tableMeta, err := w.fetchTableMeta(ctx, conn, ns)
	if err != nil {
		return err
	}

	tableStmt, _, err := parser.New().ParseSQL(tableMeta)
	if err != nil {
		return err
	}

	uks := make([]*UnqKeys, 0)
	keySource := "show_create_table"
	switch tableNode := tableStmt[0].(type) {
	case *ast.CreateTableStmt:
		columnMetas := buildColumnMetas(tableNode)
		ddlUniqueKeys := GetPossibleUniqueKeys(tableNode)
		uks = ddlUniqueKeys

		if dataSource == dataSourceOceanBase {
			indexUniqueKeys, indexErr := w.fetchUniqueKeysFromShowIndex(
				ctx, conn, ns, columnMetas,
			)
			switch {
			case indexErr != nil:
				if len(forceColumns) > 0 {
					return indexErr
				}
				log.Logger.Warnf(
					"show index %s failed, fallback to show create table unique keys: %v",
					ns, indexErr,
				)
			case len(indexUniqueKeys) == 0:
				if len(forceColumns) > 0 {
					return fmt.Errorf(
						"show index %s returned no primary or unique key for forced_chunking_column: %s",
						ns, strings.Join(forceColumns, ","),
					)
				}
				log.Logger.Warnf(
					"show index %s returned no primary or unique key, fallback to show create table unique keys",
					ns,
				)
			default:
				uks = indexUniqueKeys
				keySource = "show_index"
			}
		}

		if len(uks) == 0 {
			return fmt.Errorf("can't find any index which is primary or unique key")
		}
	default:
		return fmt.Errorf("table meta is not CreateTableStmt, tableMeta: %s", tableMeta)
	}

	w.unqKeys, err = selectChunkingKey(uks, forceColumns, c.ForceChunkingColumn)
	if err == nil && w.unqKeys != nil {
		log.Logger.Debugf(
			"selected chunking key [data_source=%s, metadata_source=%s, tp=%d, columns=%s, forced=%s]",
			dataSource,
			keySource,
			w.unqKeys.Tp,
			strings.Join(w.unqKeys.UniqueKeyColumns, ","),
			c.ForceChunkingColumn,
		)
	}
	return err
}

func detectDataSourceFromVersion(version string) dataSourceType {
	lowerVersion := strings.ToLower(version)
	switch {
	case strings.Contains(lowerVersion, string(dataSourceOceanBase)):
		return dataSourceOceanBase
	case strings.Contains(lowerVersion, string(dataSourceTiDB)):
		return dataSourceTiDB
	default:
		return dataSourceMySQL
	}
}

func (w *Writer) openMetadataConn(
	ctx context.Context,
) (*sql.Conn, dataSourceType, error) {
	conn, err := w.MysqlClient.Conn(ctx)
	if err != nil {
		return nil, "", fmt.Errorf("get db connection failed: %w", err)
	}

	var version string
	if err = conn.QueryRowContext(ctx, queryVersionSQL).Scan(&version); err != nil {
		conn.Close()
		return nil, "", fmt.Errorf("query version failed: %w", err)
	}

	dataSource := detectDataSourceFromVersion(version)
	log.Logger.Debugf("detected data source: [%s], version: [%s]", dataSource, version)
	return conn, dataSource, nil
}

func (w *Writer) fetchDBNowLiteral(ctx context.Context, conn *sql.Conn) (string, error) {
	var literal string
	if err := conn.QueryRowContext(ctx, dbNowLiteralSQL).Scan(&literal); err != nil {
		return "", fmt.Errorf("query database current time failed: %w", err)
	}
	literal = strings.TrimSpace(literal)
	if literal == "" {
		return "", fmt.Errorf("query database current time returned empty literal")
	}
	return literal, nil
}

func (w *Writer) enableOceanBaseDDLCompatMode(
	ctx context.Context, conn *sql.Conn,
) error {
	if _, err := conn.ExecContext(ctx, obCompatModeOnSQL); err != nil {
		return fmt.Errorf("set oceanbase ddl compat mode failed: %w", err)
	}
	return nil
}

func (w *Writer) fetchTableMeta(
	ctx context.Context, conn *sql.Conn, ns string,
) (string, error) {
	rows, err := conn.QueryContext(ctx, fmt.Sprintf(vars.TableInfoSQL, ns))
	if err != nil {
		return "", fmt.Errorf("show create table %s failed: %w", ns, err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return "", fmt.Errorf("show create table %s columns failed: %w", ns, err)
	}

	var tableMeta string
	for rows.Next() {
		scanArgs := make([]interface{}, len(cols))
		for i := range scanArgs {
			scanArgs[i] = &sql.RawBytes{}
		}

		if err = rows.Scan(scanArgs...); err != nil {
			return "", fmt.Errorf("show create table %s scan failed: %w", ns, err)
		}
		tableMeta = ColumnValue(scanArgs, cols, "Create Table")
	}
	if err = rows.Err(); err != nil {
		return "", fmt.Errorf("show create table %s rows iteration failed: %w", ns, err)
	}

	return tableMeta, nil
}

func normalizeForceChunkingColumns(raw string) ([]string, error) {
	if raw == "" {
		return nil, nil
	}

	parts := strings.Split(raw, ",")
	cols := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		cols = append(cols, part)
	}
	if len(cols) == 0 {
		return nil, fmt.Errorf(
			"forced_chunking_column is empty after normalization, forceChunkingColumn: %s",
			raw,
		)
	}
	return cols, nil
}

func comparableColumns(columns []string) []string {
	normalized := make([]string, 0, len(columns))
	for _, column := range columns {
		column = strings.TrimSpace(column)
		if column == "" {
			continue
		}
		normalized = append(normalized, strings.ToLower(column))
	}
	sort.Strings(normalized)
	return normalized
}

func selectChunkingKey(
	uks []*UnqKeys, forceColumns []string, rawForce string,
) (*UnqKeys, error) {
	if len(forceColumns) > 0 {
		targetColumns := comparableColumns(forceColumns)
		for _, uk := range uks {
			if len(targetColumns) != len(uk.UniqueKeyColumns) {
				continue
			}
			if strings.Join(targetColumns, ",") ==
				strings.Join(comparableColumns(uk.UniqueKeyColumns), ",") {
				return uk, nil
			}
		}

		return nil, fmt.Errorf(
			"forced_chunking_column doesn't conform to primary or unique key, forceChunkingColumn: %s",
			rawForce,
		)
	}

	for _, uk := range uks {
		if uk.Tp == vars.ConstraintPrimaryKey {
			return uk, nil
		}
	}

	return uks[0], nil
}

func buildColumnMetas(tableNode *ast.CreateTableStmt) map[string]columnMeta {
	columnMetas := make(map[string]columnMeta, len(tableNode.Cols))
	for _, col := range tableNode.Cols {
		isNull := true
		for _, option := range col.Options {
			if option.Tp == ast.ColumnOptionNotNull {
				isNull = false
				break
			}
		}
		columnMetas[strings.ToLower(col.Name.Name.String())] = columnMeta{
			columnType: col.Tp.GetType(),
			isNull:     isNull,
		}
	}
	return columnMetas
}

func buildUniqueKeysFromShowIndexRows(
	indexRows []*showIndexRow, columnMetas map[string]columnMeta,
) []*UnqKeys {
	type groupedIndex struct {
		tp   int
		rows []*showIndexRow
	}

	grouped := make(map[string]*groupedIndex)
	order := make([]string, 0)
	for _, row := range indexRows {
		if row == nil || row.nonUnique != 0 {
			continue
		}

		group, ok := grouped[row.keyName]
		if !ok {
			group = &groupedIndex{tp: vars.ConstraintUniqKey}
			if strings.EqualFold(row.keyName, "PRIMARY") {
				group.tp = vars.ConstraintPrimaryKey
			}
			grouped[row.keyName] = group
			order = append(order, row.keyName)
		}
		group.rows = append(group.rows, row)
	}

	uks := make([]*UnqKeys, 0, len(order))
	for _, keyName := range order {
		group := grouped[keyName]
		sort.Slice(group.rows, func(i, j int) bool {
			return group.rows[i].seqInIndex < group.rows[j].seqInIndex
		})

		uk := &UnqKeys{
			UniqueKeyColumns: make([]string, 0, len(group.rows)),
			UniqueKeyTypes:   make([]byte, 0, len(group.rows)),
			IsNull:           make([]bool, 0, len(group.rows)),
			Tp:               group.tp,
		}

		valid := true
		for _, row := range group.rows {
			meta, ok := columnMetas[strings.ToLower(row.columnName)]
			if !ok {
				valid = false
				break
			}
			uk.UniqueKeyColumns = append(uk.UniqueKeyColumns, row.columnName)
			uk.UniqueKeyTypes = append(uk.UniqueKeyTypes, meta.columnType)
			uk.IsNull = append(uk.IsNull, meta.isNull)
		}
		if !valid || len(uk.UniqueKeyColumns) == 0 {
			continue
		}

		uk.CountColumns = len(uk.UniqueKeyColumns)
		uks = append(uks, uk)
	}

	return uks
}

func (w *Writer) fetchUniqueKeysFromShowIndex(
	ctx context.Context,
	conn *sql.Conn,
	ns string,
	columnMetas map[string]columnMeta,
) ([]*UnqKeys, error) {
	rows, err := conn.QueryContext(ctx, fmt.Sprintf(obShowIndexSQL, ns))
	if err != nil {
		return nil, fmt.Errorf("show index %s failed: %w", ns, err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("show index %s columns failed: %w", ns, err)
	}

	indexRows := make([]*showIndexRow, 0)
	for rows.Next() {
		scanArgs := make([]interface{}, len(cols))
		for i := range scanArgs {
			scanArgs[i] = &sql.RawBytes{}
		}

		if err = rows.Scan(scanArgs...); err != nil {
			return nil, fmt.Errorf("show index %s scan failed: %w", ns, err)
		}

		nonUnique, err := ColumnValueInt64(scanArgs, cols, "Non_unique")
		if err != nil {
			return nil, fmt.Errorf(
				"show index %s parse Non_unique failed: %w", ns, err,
			)
		}
		seqInIndex, err := ColumnValueInt64(scanArgs, cols, "Seq_in_index")
		if err != nil {
			return nil, fmt.Errorf(
				"show index %s parse Seq_in_index failed: %w", ns, err,
			)
		}

		keyName := strings.TrimSpace(ColumnValue(scanArgs, cols, "Key_name"))
		columnName := strings.TrimSpace(ColumnValue(scanArgs, cols, "Column_name"))
		if keyName == "" || columnName == "" {
			return nil, fmt.Errorf(
				"show index %s returned incomplete key metadata, key_name=%q column_name=%q",
				ns, keyName, columnName,
			)
		}

		indexRows = append(indexRows, &showIndexRow{
			keyName:    keyName,
			seqInIndex: seqInIndex,
			columnName: columnName,
			nonUnique:  nonUnique,
		})
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("show index %s rows iteration failed: %w", ns, err)
	}

	return buildUniqueKeysFromShowIndexRows(indexRows, columnMetas), nil
}

func (w *Writer) lockTableRead() error {
	_, err := w.MysqlClient.Exec(fmt.Sprintf(vars.LockTableSQL, w.Database, w.Table))
	if err != nil {
		return fmt.Errorf("lockTableRead failed: %w", err)
	}
	return nil
}

func (w *Writer) unlockTable() error {
	_, err := w.MysqlClient.Exec(vars.UnlockTableSQL)
	if err != nil {
		return fmt.Errorf("unlockTable failed: %w", err)
	}
	return nil
}

// DataSource 返回探测到的数据源类型("oceanbase"|"tidb"|"mysql")。
// task 包用它判断是否走 OceanBase 分区并发策略, 返回 string 避免反向依赖
// mysql 包的非导出类型 dataSourceType。元信息探测前为空串。
func (w *Writer) DataSource() string {
	return string(w.dataSource)
}

func (w *Writer) SetFinished() {
	w.isFinished.Store(true)
}

func (w *Writer) IsFinished() bool {
	return w.isFinished.Load()
}

func (w *Writer) AddRowAffects(delta int64) {
	w.rowAffects.Add(delta)
}

func (w *Writer) GetRowAffects() int64 {
	return w.rowAffects.Load()
}

func (w *Writer) SetCostTime(cost time.Duration) {
	w.costTimeNanos.Store(cost.Nanoseconds())
}

func (w *Writer) GetCostTime() time.Duration {
	return time.Duration(w.costTimeNanos.Load())
}
