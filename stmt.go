package sqlite

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"time"
)

type Stmt struct {
	conn   *Conn
	stmt   uintptr
	query  string
	closed bool
}

func (s *Stmt) Close() error {
	if s.closed {
		return nil
	}

	s.conn.stmts.Delete(s.stmt)

	rc := sqlite3_finalize(s.stmt)
	if rc != SQLITE_OK {
		return fmt.Errorf("finalize failed: %s", errorString(rc))
	}

	s.closed = true
	return nil
}

func (s *Stmt) NumInput() int {
	return sqlite3_bind_parameter_count(s.stmt)
}

func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	namedArgs := make([]driver.NamedValue, len(args))
	for i, arg := range args {
		namedArgs[i] = driver.NamedValue{
			Ordinal: i + 1,
			Value:   arg,
		}
	}
	return s.ExecContext(context.Background(), namedArgs)
}

func (s *Stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if s.closed {
		return nil, errors.New("statement closed")
	}

	if err := s.bind(args); err != nil {
		return nil, err
	}

	rc := sqlite3_step(s.stmt)
	defer sqlite3_reset(s.stmt)

	if rc != SQLITE_DONE && rc != SQLITE_ROW {
		return nil, fmt.Errorf("exec failed: %s", getErrorMessage(s.conn.db))
	}

	return &Result{
		lastInsertID: sqlite3_last_insert_rowid(s.conn.db),
		rowsAffected: int64(sqlite3_changes(s.conn.db)),
	}, nil
}

func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	namedArgs := make([]driver.NamedValue, len(args))
	for i, arg := range args {
		namedArgs[i] = driver.NamedValue{
			Ordinal: i + 1,
			Value:   arg,
		}
	}
	return s.QueryContext(context.Background(), namedArgs)
}

func (s *Stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if s.closed {
		return nil, errors.New("statement closed")
	}

	if err := s.bind(args); err != nil {
		return nil, err
	}

	columnCount := sqlite3_column_count(s.stmt)
	columns := make([]string, columnCount)
	for i := 0; i < columnCount; i++ {
		namePtr := sqlite3_column_name(s.stmt, i)
		columns[i] = goString(namePtr)
	}

	return &Rows{
		stmt:    s,
		columns: columns,
		ctx:     ctx,
	}, nil
}

func (s *Stmt) bind(args []driver.NamedValue) error {
	expectedArgs := s.NumInput()
	if len(args) != expectedArgs {
		return fmt.Errorf("expected %d arguments, got %d", expectedArgs, len(args))
	}

	for _, arg := range args {
		idx := arg.Ordinal
		if idx <= 0 {
			continue
		}

		value := arg.Value

		if valuer, ok := value.(driver.Valuer); ok {
			var err error
			value, err = valuer.Value()
			if err != nil {
				return fmt.Errorf("valuer error at position %d: %w", idx, err)
			}
		}

		if err := s.bindValue(idx, value); err != nil {
			return err
		}
	}

	return nil
}

func (s *Stmt) bindValue(idx int, value any) error {
	var rc int

	if value == nil {
		rc = sqlite3_bind_null(s.stmt, idx)
		if rc != SQLITE_OK {
			return fmt.Errorf("bind null failed at position %d", idx)
		}
		return nil
	}

	switch v := value.(type) {
	case int64:
		rc = sqlite3_bind_int64(s.stmt, idx, v)
	case int:
		rc = sqlite3_bind_int64(s.stmt, idx, int64(v))
	case int32:
		rc = sqlite3_bind_int64(s.stmt, idx, int64(v))
	case int16:
		rc = sqlite3_bind_int64(s.stmt, idx, int64(v))
	case int8:
		rc = sqlite3_bind_int64(s.stmt, idx, int64(v))
	case uint64:
		rc = sqlite3_bind_int64(s.stmt, idx, int64(v))
	case uint32:
		rc = sqlite3_bind_int64(s.stmt, idx, int64(v))
	case uint16:
		rc = sqlite3_bind_int64(s.stmt, idx, int64(v))
	case uint8:
		rc = sqlite3_bind_int64(s.stmt, idx, int64(v))
	case uint:
		rc = sqlite3_bind_int64(s.stmt, idx, int64(v))
	case bool:
		if v {
			rc = sqlite3_bind_int64(s.stmt, idx, 1)
		} else {
			rc = sqlite3_bind_int64(s.stmt, idx, 0)
		}
	case float64:
		rc = sqlite3_bind_double(s.stmt, idx, v)
	case float32:
		rc = sqlite3_bind_double(s.stmt, idx, float64(v))
	case string:
		strPtr, pinner := cString(v)
		defer unpin(pinner)
		rc = sqlite3_bind_text(s.stmt, idx, strPtr, len(v), SQLITE_TRANSIENT)
	case []byte:
		if len(v) == 0 {
			rc = sqlite3_bind_blob(s.stmt, idx, 0, 0, SQLITE_STATIC)
		} else {
			blobPtr, pinner := allocateBytes(v)
			defer unpin(pinner)
			rc = sqlite3_bind_blob(s.stmt, idx, blobPtr, len(v), SQLITE_TRANSIENT)
		}
	case time.Time:
		strPtr, pinner := cString(v.Format(time.RFC3339Nano))
		defer unpin(pinner)
		rc = sqlite3_bind_text(s.stmt, idx, strPtr, -1, SQLITE_TRANSIENT)
	default:
		return fmt.Errorf("unsupported type %T at position %d", value, idx)
	}

	if rc != SQLITE_OK {
		return fmt.Errorf("bind failed at position %d: %s", idx, getErrorMessage(s.conn.db))
	}

	return nil
}

func (s *Stmt) CheckNamedValue(nv *driver.NamedValue) error {
	return checkNamedValue(nv)
}

func checkNamedValue(nv *driver.NamedValue) error {
	if nv.Value == nil {
		return nil
	}

	if _, ok := nv.Value.(driver.Valuer); ok {
		return nil
	}

	v := reflect.ValueOf(nv.Value)
	switch v.Kind() {
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64, reflect.String:
		return nil
	case reflect.Slice:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return nil
		}
	}

	if _, ok := nv.Value.(time.Time); ok {
		return nil
	}

	return driver.ErrSkip
}

var (
	_ driver.Stmt              = (*Stmt)(nil)
	_ driver.StmtExecContext   = (*Stmt)(nil)
	_ driver.StmtQueryContext  = (*Stmt)(nil)
	_ driver.NamedValueChecker = (*Stmt)(nil)
)
