package retry

import (
	"context"
	"database/sql/driver"
	"errors"
	"testing"

	"github.com/go-sql-driver/mysql"
)

func TestClassify(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want Class
	}{
		{"nil", nil, ClassUnknown},
		{"ctx canceled", context.Canceled, ClassCanceled},
		{"ctx deadline", context.DeadlineExceeded, ClassCanceled},
		{"deadlock 1213", &mysql.MySQLError{Number: 1213}, ClassTransient},
		{"lock wait 1205", &mysql.MySQLError{Number: 1205}, ClassTransient},
		{"ob conflict 4012", &mysql.MySQLError{Number: 4012}, ClassTransient},
		{"gone 2006", &mysql.MySQLError{Number: 2006}, ClassTransient},
		{"lost 2013", &mysql.MySQLError{Number: 2013}, ClassTransient},
		{"missing table 1146", &mysql.MySQLError{Number: 1146}, ClassFatal},
		{"syntax 1064", &mysql.MySQLError{Number: 1064}, ClassFatal},
		{"unknown column 1054", &mysql.MySQLError{Number: 1054}, ClassFatal},
		{"bad conn", driver.ErrBadConn, ClassTransient},
		{"reset", errors.New("read tcp 1.2.3.4:3306: connection reset by peer"), ClassTransient},
		{"opaque", errors.New("something weird"), ClassFatal},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := Classify(c.err); got != c.want {
				t.Errorf("Classify(%v) = %d, want %d", c.err, got, c.want)
			}
		})
	}
}
