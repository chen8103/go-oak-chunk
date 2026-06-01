// Package preflight implements EXPLAIN-based row estimation and large-table
// confirmation used before a chunked DELETE/UPDATE is launched.
//
// P2 scope: single worker, DELETE-only fast-path support. The estimation is a
// best-effort hint; failures never block execution (the caller logs and
// proceeds). The large-table confirmation gate is shared by CLI (interactive)
// and SDK (auto-confirm) paths.
package preflight

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
)

// DefaultLargeTableThreshold is the estimated-rows boundary above which a
// confirmation is required when the caller does not supply an explicit one.
const DefaultLargeTableThreshold int64 = 100000

// Result carries the outcome of an EXPLAIN estimation.
type Result struct {
	EstimatedRows int64
	Threshold     int64
}

// Querier is the minimal database surface preflight needs. *sql.DB satisfies it.
type Querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// obRowsHeaderRE matches the OceanBase plan header cell naming the estimated
// rows column ("EST.ROWS" or "ROWS").
var obRowsHeaderRE = regexp.MustCompile(`(?i)^\s*(?:est\.?\s*rows|rows)\s*$`)

// EstimateRows runs EXPLAIN on a DELETE bound by whereClause and returns the
// largest estimated row count it can parse from the plan. It supports both the
// classic MySQL/TiDB tabular EXPLAIN (a "rows" column) and the OceanBase plan
// text format. It returns 0 with a nil error when no estimate can be parsed.
func EstimateRows(ctx context.Context, db Querier, table, whereClause string) (int64, error) {
	if db == nil {
		return 0, fmt.Errorf("preflight: nil querier")
	}

	where := strings.TrimSpace(whereClause)
	explainSQL := fmt.Sprintf("EXPLAIN DELETE FROM %s", table)
	if where != "" {
		explainSQL += " WHERE " + where
	}

	rows, err := db.QueryContext(ctx, explainSQL)
	if err != nil {
		return 0, fmt.Errorf("preflight explain failed: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return 0, fmt.Errorf("preflight explain columns failed: %w", err)
	}

	rowsColIdx := -1
	for i, c := range cols {
		if strings.EqualFold(strings.TrimSpace(c), "rows") {
			rowsColIdx = i
			break
		}
	}

	var maxRows int64
	for rows.Next() {
		scan := make([]any, len(cols))
		raw := make([]sql.RawBytes, len(cols))
		for i := range scan {
			scan[i] = &raw[i]
		}
		if err := rows.Scan(scan...); err != nil {
			return 0, fmt.Errorf("preflight explain scan failed: %w", err)
		}

		// Tabular EXPLAIN (MySQL / TiDB): read the dedicated rows column.
		if rowsColIdx >= 0 {
			if n, ok := parseInt64(string(raw[rowsColIdx])); ok && n > maxRows {
				maxRows = n
			}
			continue
		}

		// OceanBase plan-text EXPLAIN: a single column holds the plan text.
		for i := range raw {
			if n, ok := parseOBPlanRows(string(raw[i])); ok && n > maxRows {
				maxRows = n
			}
		}
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("preflight explain iteration failed: %w", err)
	}

	return maxRows, nil
}

// parseInt64 parses a trimmed numeric cell, tolerating empty/NULL cells.
func parseInt64(s string) (int64, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, false
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// parseOBPlanRows parses an OceanBase plan-text EXPLAIN. The plan is a
// pipe-delimited table; it locates the EST.ROWS column from the header row and
// then returns the largest value in that column across the data rows.
func parseOBPlanRows(plan string) (int64, bool) {
	scanner := bufio.NewScanner(strings.NewReader(plan))
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	rowsColIdx := -1
	var maxRows int64
	found := false

	for scanner.Scan() {
		line := scanner.Text()
		if !strings.Contains(line, "|") {
			continue // border / separator lines
		}
		cells := splitPlanRow(line)

		if rowsColIdx < 0 {
			for i, cell := range cells {
				if obRowsHeaderRE.MatchString(cell) {
					rowsColIdx = i
					break
				}
			}
			continue // header row consumed; do not read it as data
		}

		if rowsColIdx < len(cells) {
			if n, err := strconv.ParseInt(strings.TrimSpace(cells[rowsColIdx]), 10, 64); err == nil {
				found = true
				if n > maxRows {
					maxRows = n
				}
			}
		}
	}
	return maxRows, found
}

// splitPlanRow splits a "|a|b|c|" plan row into trimmed cells, dropping the
// empty leading/trailing fields produced by the outer bars.
func splitPlanRow(line string) []string {
	parts := strings.Split(line, "|")
	cells := make([]string, 0, len(parts))
	for _, p := range parts {
		cells = append(cells, strings.TrimSpace(p))
	}
	// Trim empty bookends from leading/trailing bars.
	for len(cells) > 0 && cells[0] == "" {
		cells = cells[1:]
	}
	for len(cells) > 0 && cells[len(cells)-1] == "" {
		cells = cells[:len(cells)-1]
	}
	return cells
}

// NeedsConfirmation reports whether the estimated rows reach the threshold and
// confirmation has not been auto-granted. Threshold <= 0 disables the gate.
func NeedsConfirmation(r Result, autoConfirm bool) bool {
	if autoConfirm {
		return false
	}
	if r.Threshold <= 0 {
		return false
	}
	return r.EstimatedRows >= r.Threshold
}

// ConfirmLargeDelete prompts on out and reads a yes/no answer from in. It
// returns true only when the user types "yes" (case-insensitive). EOF or any
// other answer returns false without error.
func ConfirmLargeDelete(in io.Reader, out io.Writer, r Result) (bool, error) {
	fmt.Fprintf(out,
		"preflight: estimated %d rows >= threshold %d. Type 'yes' to continue: ",
		r.EstimatedRows, r.Threshold)

	reader := bufio.NewReader(in)
	line, err := reader.ReadString('\n')
	if err != nil && err != io.EOF {
		return false, fmt.Errorf("read confirmation failed: %w", err)
	}
	return strings.EqualFold(strings.TrimSpace(line), "yes"), nil
}
