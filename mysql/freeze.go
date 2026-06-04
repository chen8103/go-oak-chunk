package mysql

import (
	"strings"
	"time"

	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/format"
)

const freezeLayout = "2006-01-02 15:04:05"

var freezeFns = map[string]struct{}{
	"now":               {},
	"current_timestamp": {},
	"localtime":         {},
	"localtimestamp":    {},
	"sysdate":           {},
}

// FreezeNow walks the WHERE AST and replaces NOW()-like functions with a frozen
// datetime literal, then restores the clause back to SQL text. When whereClause
// is nil it returns an empty string.
func FreezeNow(whereClause ast.ExprNode, now time.Time) (string, error) {
	return FreezeNowLiteral(whereClause, now.Format(freezeLayout))
}

// FreezeNowLiteral is like FreezeNow, but uses a DB-session-derived literal.
func FreezeNowLiteral(whereClause ast.ExprNode, literal string) (string, error) {
	if whereClause == nil {
		return "", nil
	}

	f := &freezer{lit: literal}
	newNode, _ := whereClause.Accept(f)

	expr, ok := newNode.(ast.ExprNode)
	if !ok {
		expr = whereClause
	}

	buf := new(strings.Builder)
	if err := expr.Restore(&format.RestoreCtx{
		Flags: format.DefaultRestoreFlags,
		In:    buf,
	}); err != nil {
		return "", err
	}

	s := strings.ReplaceAll(buf.String(), "_UTF8MB4", "")
	s = strings.ReplaceAll(s, "_UTF8", "")
	return s, nil
}

// HasFreezeNowFunc reports whether a WHERE AST contains NOW()-like functions.
func HasFreezeNowFunc(whereClause ast.ExprNode) bool {
	if whereClause == nil {
		return false
	}
	d := &freezeDetector{}
	whereClause.Accept(d)
	return d.hit
}

type freezer struct {
	lit string
}

func (f *freezer) Enter(n ast.Node) (ast.Node, bool) {
	return n, false
}

func (f *freezer) Leave(n ast.Node) (ast.Node, bool) {
	if fn, ok := n.(*ast.FuncCallExpr); ok {
		if _, hit := freezeFns[strings.ToLower(fn.FnName.L)]; hit {
			return ast.NewValueExpr(f.lit, "", ""), true
		}
	}
	return n, true
}

type freezeDetector struct {
	hit bool
}

func (d *freezeDetector) Enter(n ast.Node) (ast.Node, bool) {
	return n, false
}

func (d *freezeDetector) Leave(n ast.Node) (ast.Node, bool) {
	if fn, ok := n.(*ast.FuncCallExpr); ok {
		if _, hit := freezeFns[strings.ToLower(fn.FnName.L)]; hit {
			d.hit = true
		}
	}
	return n, true
}
