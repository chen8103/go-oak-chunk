// Package retry holds OB/MySQL error classification and exponential backoff retry logic.
package retry

import (
	"context"
	"database/sql/driver"
	"errors"
	"net"
	"strings"

	"github.com/go-sql-driver/mysql"
)

type Class int

const (
	ClassUnknown Class = iota
	// ClassTransient covers deadlock, lock-wait timeout, OB txn conflict and
	// connection disruption errors that are safe to retry.
	ClassTransient
	// ClassFatal covers missing table, syntax error, permission denied and
	// other errors that must not be retried.
	ClassFatal
	// ClassCanceled covers context cancellation and deadline expiry.
	ClassCanceled
)

// transientMySQLErrno is a closed allow-list of MySQL/OB error codes that are
// considered transient. It must not be expanded without an explicit decision.
var transientMySQLErrno = map[uint16]struct{}{
	1213: {}, // deadlock found when trying to get lock
	1205: {}, // lock wait timeout exceeded
	4012: {}, // OB: transaction conflict (rollback and retry)
	2006: {}, // MySQL server has gone away
	2013: {}, // lost connection to MySQL server during query
	9007: {}, // TiDB: write conflict (safe to retry)
	8022: {}, // TiDB: transaction commit failed, safe to retry
	8028: {}, // TiDB: information schema changed during transaction
}

// Classify examines err and returns its retry class.
func Classify(err error) Class {
	if err == nil {
		return ClassUnknown
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return ClassCanceled
	}

	var me *mysql.MySQLError
	if errors.As(err, &me) {
		if _, hit := transientMySQLErrno[me.Number]; hit {
			return ClassTransient
		}
		return ClassFatal
	}

	if errors.Is(err, driver.ErrBadConn) || isNetTransient(err) {
		return ClassTransient
	}

	return ClassFatal
}

func isNetTransient(err error) bool {
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return true
	}

	msg := strings.ToLower(err.Error())
	for _, s := range []string{
		"connection reset by peer",
		"broken pipe",
		"connection refused",
		"i/o timeout",
		"eof",
	} {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}
