package mysql

import (
	"context"
	"database/sql"
	"strconv"

	_ "github.com/go-sql-driver/mysql"

	"github.com/SisyphusSQ/go-oak-chunk/v3/conf"
)

func NewMysqlClient(t *conf.Config) (*sql.DB, error) {
	dsn := t.User + ":" + t.Password + "@(" + t.Host + ":" + strconv.Itoa(t.Port) + ")/" + t.Database
	dsn += "?checkConnLiveness=true"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	// 分区并发策略每个 worker 至少占用一条连接(SELECT + DELETE 事务),
	// 再加上元信息/从库延迟探测借用的连接, 所以连接上限随并发抬高,
	// 并预留 2 条余量。非分区路径(PartitionConcurrency<=0)保持原来的 10。
	maxConns := 10
	if t.PartitionConcurrency > maxConns {
		maxConns = t.PartitionConcurrency + 2
	}
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns)
	return db, nil
}

func NewMysqlClientForSlave(t *conf.Config, host string) (*sql.DB, error) {
	dsn := t.User + ":" + t.Password + "@(" + host + ":" + strconv.Itoa(t.Port) + ")/" + t.Database
	dsn += "?checkConnLiveness=true"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(5)
	return db, nil
}

func CheckVersion(client *sql.DB) (version string, err error) {
	return CheckVersionContext(context.Background(), client)
}

func CheckVersionContext(ctx context.Context, client *sql.DB) (version string, err error) {
	err = client.QueryRowContext(ctx, "select @@version").Scan(&version)
	if err != nil {
		return "", err
	}
	return version, nil
}
