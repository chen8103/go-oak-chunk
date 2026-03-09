package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	soar "github.com/XiaoMi/soar/ast"
	"github.com/juju/ratelimit"
	"github.com/pingcap/parser/ast"

	"github.com/SisyphusSQ/go-oak-chunk/v3/conf"
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
	ProducerQueue     chan *Producer

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

	queryVersionSQL     = "select version()"
	obCompatModeOnSQL   = "SET SESSION _show_ddl_in_compat_mode = true"
)

func NewWriter(c *conf.Config) (*Writer, error) {
	w := &Writer{
		noLogBing:     c.NoLogBin,
		ChunkSize:     c.ChunkSize,
		TxnSize:       c.TxnSize,
		ExecuteSQL:    strings.ReplaceAll(c.ExecuteQuery, ";", ""),
		ProducerQueue: make(chan *Producer, 1000),
	}
	w.SetCostTime(1 * time.Second)

	if err := w.preCheck(c); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *Writer) preCheck(c *conf.Config) error {
	var err error

	// 获取database和table
	//w.Table = c.Table
	w.Database = c.Database
	if w.Database == "" {
		return fmt.Errorf("no database specified. specify Database with -d or --database")
	}

	w.Table, err = TableMetaInfo(w.ExecuteSQL)
	if err != nil {
		return fmt.Errorf("failed to parse table info: %w", err)
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

func (w *Writer) Write(ctx context.Context, bucket *ratelimit.Bucket, bucketNum <-chan int64) error {
	maxRetry := 3

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

				var retryErr error
				for i := 0; i < maxRetry; i++ {
					tx, retryErr = w.MysqlClient.BeginTx(ctx, nil)
					if retryErr != nil {
						return retryErr
					}

					execCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
					res, retryErr = tx.ExecContext(execCtx, execSql, values...)
					cancel()
					if retryErr != nil {
						_ = tx.Rollback()
						if errors.Is(retryErr, context.Canceled) || errors.Is(retryErr, context.DeadlineExceeded) {
							return retryErr
						}
						continue
					}
					break
				}

				if retryErr != nil {
					return fmt.Errorf("execute sql failed after %d retries: %w", maxRetry, retryErr)
				}
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
	sqlStmt, err := soar.TiParse(w.ExecuteSQL, "", "")
	if err != nil {
		return err
	}

	if len(sqlStmt) == 0 || len(sqlStmt) > 1 {
		return fmt.Errorf("sql is empty or sql number is over 1, please confirm only one sql is provided")
	}

	node := sqlStmt[0]
	v := &visitor{}
	switch node.(type) {
	case *ast.DeleteStmt:
		w.SqlType = "Delete"
		node.Accept(v)

		w.ExecuteSQL = fmt.Sprintf("DELETE FROM `%s` WHERE ", w.Table)
	case *ast.UpdateStmt:
		w.SqlType = "Update"
		node.Accept(v)

		re := regexp.MustCompile(`set.*where|SET.*WHERE|set.*WHERE|SET.*where`)
		sub := re.FindString(c.ExecuteQuery)
		w.ExecuteSQL = fmt.Sprintf("UPDATE `%s` %s ", w.Table, sub)
	default:
		return fmt.Errorf("please confirm sql type is update or delete")
	}

	if v.whereClause != "" {
		// avoid where clause "or", make program confused
		w.OriginWhereClause = fmt.Sprintf("(%s)", v.whereClause)
		log.Logger.Debugf("originWhereClause: [%s]", v.whereClause)

		w.ExecuteSQL += w.OriginWhereClause
	}

	// Second find primary/unique index which can be used
	// check for column in Table meta
	ns := fmt.Sprintf("`%s`.`%s`", w.Database, w.Table)
	tableMeta, err := w.fetchTableMeta(ns)
	if err != nil {
		return err
	}

	tableStmt, err := soar.TiParse(tableMeta, "", "")
	if err != nil {
		return err
	}

	uks := make([]*UnqKeys, 0)
	switch tableNode := tableStmt[0].(type) {
	case *ast.CreateTableStmt:
		uks = GetPossibleUniqueKeys(tableNode)
		if len(uks) == 0 {
			return fmt.Errorf("can't find any index which is primary or unique key")
		}
	default:
		return fmt.Errorf("table meta is not CreateTableStmt, tableMeta: %s", tableMeta)
	}

	if c.ForceChunkingColumn != "" {
		uniqueColumns := strings.Split(c.ForceChunkingColumn, ",")
		sort.Strings(uniqueColumns)
		for _, uk := range uks {
			sortKeys := make([]string, len(uk.UniqueKeyColumns))
			copy(sortKeys, uk.UniqueKeyColumns)
			sort.Strings(sortKeys)
			if reflect.DeepEqual(uniqueColumns, sortKeys) {
				w.unqKeys = uk
				return nil
			}
		}

		// 如果for结束没有数据，说明使用者瞎写的ForceChunkingColumn
		return fmt.Errorf("forced_chunking_column doesn't conform to primary or unique key, forceChunkingColumn: %s", c.ForceChunkingColumn)
	}

	for _, uk := range uks {
		if uk.Tp == vars.ConstraintPrimaryKey {
			w.unqKeys = uk
			return nil
		}
	}

	w.unqKeys = uks[0]
	return nil
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

func (w *Writer) fetchTableMeta(ns string) (string, error) {
	ctx := context.Background()
	conn, err := w.MysqlClient.Conn(ctx)
	if err != nil {
		return "", fmt.Errorf("get db connection failed: %w", err)
	}
	defer conn.Close()

	var version string
	if err = conn.QueryRowContext(ctx, queryVersionSQL).Scan(&version); err != nil {
		return "", fmt.Errorf("query version failed: %w", err)
	}

	dataSource := detectDataSourceFromVersion(version)
	log.Logger.Debugf("detected data source: [%s], version: [%s]", dataSource, version)
	if dataSource == dataSourceOceanBase {
		if _, err = conn.ExecContext(ctx, obCompatModeOnSQL); err != nil {
			return "", fmt.Errorf("set oceanbase ddl compat mode failed: %w", err)
		}
	}

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
