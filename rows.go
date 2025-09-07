package sqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
)

type Rows struct {
	stmt    *Stmt
	columns []string
	ctx     context.Context
	done    bool
}

func (r *Rows) Columns() []string {
	return r.columns
}

func (r *Rows) Close() error {
	if !r.done {
		sqlite3_reset(r.stmt.stmt)
		r.done = true
	}
	return nil
}

func (r *Rows) Next(dest []driver.Value) error {
	select {
	case <-r.ctx.Done():
		return r.ctx.Err()
	default:
	}

	if r.done {
		return io.EOF
	}

	rc := sqlite3_step(r.stmt.stmt)

	if rc == SQLITE_DONE {
		r.done = true
		return io.EOF
	}

	if rc != SQLITE_ROW {
		return fmt.Errorf("step failed: %s", getErrorMessage(r.stmt.conn.db))
	}

	if len(dest) != len(r.columns) {
		return fmt.Errorf("expected %d destination values, got %d", len(r.columns), len(dest))
	}

	for i := range dest {
		colType := sqlite3_column_type(r.stmt.stmt, i)
		dest[i] = r.scanColumn(i, colType)
	}

	return nil
}

func (r *Rows) scanColumn(i int, colType int) driver.Value {
	switch colType {
	case SQLITE_NULL:
		return nil
	case SQLITE_INTEGER:
		return sqlite3_column_int64(r.stmt.stmt, i)
	case SQLITE_REAL:
		return sqlite3_column_double(r.stmt.stmt, i)
	case SQLITE_TEXT:
		textPtr := sqlite3_column_text(r.stmt.stmt, i)
		length := sqlite3_column_bytes(r.stmt.stmt, i)
		return goStringN(textPtr, length)
	case SQLITE_BLOB:
		blobPtr := sqlite3_column_blob(r.stmt.stmt, i)
		length := sqlite3_column_bytes(r.stmt.stmt, i)
		if length == 0 {
			return []byte{}
		}
		return goBytesN(blobPtr, length)
	default:
		return nil
	}
}

func (r *Rows) ColumnTypeDatabaseTypeName(index int) string {
	if index < 0 || index >= len(r.columns) {
		return ""
	}

	declTypePtr := sqlite3_column_decltype(r.stmt.stmt, index)
	if declTypePtr != 0 {
		return goString(declTypePtr)
	}

	colType := sqlite3_column_type(r.stmt.stmt, index)
	switch colType {
	case SQLITE_INTEGER:
		return "INTEGER"
	case SQLITE_REAL:
		return "REAL"
	case SQLITE_TEXT:
		return "TEXT"
	case SQLITE_BLOB:
		return "BLOB"
	case SQLITE_NULL:
		return "NULL"
	default:
		return ""
	}
}

func (r *Rows) ColumnTypeLength(index int) (int64, bool) {
	colType := sqlite3_column_type(r.stmt.stmt, index)
	switch colType {
	case SQLITE_TEXT, SQLITE_BLOB:
		length := sqlite3_column_bytes(r.stmt.stmt, index)
		return int64(length), true
	default:
		return 0, false
	}
}

func (r *Rows) ColumnTypeNullable(index int) (bool, bool) {
	return true, true
}

func (r *Rows) ColumnTypePrecisionScale(index int) (int64, int64, bool) {
	return 0, 0, false
}

func (r *Rows) ColumnTypeScanType(index int) reflect.Type {
	declTypePtr := sqlite3_column_decltype(r.stmt.stmt, index)
	declType := strings.ToUpper(goString(declTypePtr))
	if strings.Contains(declType, "INT") {
		return reflect.TypeOf(sql.NullInt64{})
	}

	if strings.Contains(declType, "CHAR") || strings.Contains(declType, "CLOB") || strings.Contains(declType, "TEXT") {
		return reflect.TypeOf(sql.NullString{})
	}

	if strings.Contains(declType, "BLOB") {
		return reflect.TypeOf(sql.RawBytes{})
	}

	if strings.Contains(declType, "REAL") || strings.Contains(declType, "FLOA") || strings.Contains(declType, "DOUB") {
		return reflect.TypeOf(sql.NullFloat64{})
	}

	if strings.Contains(declType, "BOOL") {
		return reflect.TypeOf(sql.NullBool{})
	}

	if strings.Contains(declType, "DATE") || strings.Contains(declType, "TIME") {
		return reflect.TypeOf(sql.NullTime{})
	}

	if strings.Contains(declType, "NUMERIC") || strings.Contains(declType, "DECIMAL") || strings.Contains(declType, "NUMBER") {
		return reflect.TypeOf(sql.NullFloat64{})
	}

	return reflect.TypeOf(new(any)).Elem()
}

func (r *Rows) HasNextResultSet() bool {
	return false
}

func (r *Rows) NextResultSet() error {
	return errors.New("multiple result sets not supported")
}

var (
	_ driver.Rows                           = (*Rows)(nil)
	_ driver.RowsColumnTypeDatabaseTypeName = (*Rows)(nil)
	_ driver.RowsColumnTypeLength           = (*Rows)(nil)
	_ driver.RowsColumnTypeNullable         = (*Rows)(nil)
	_ driver.RowsColumnTypePrecisionScale   = (*Rows)(nil)
	_ driver.RowsColumnTypeScanType         = (*Rows)(nil)
	_ driver.RowsNextResultSet              = (*Rows)(nil)
)
