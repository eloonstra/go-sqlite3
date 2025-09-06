package sqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

type Conn struct {
	db     uintptr
	tx     *Tx
	stmts  *ThreadSafeMap[uintptr, *Stmt]
	mu     *sync.Mutex // Only for SQLite API calls and tx management
	closed atomic.Bool // Atomic for lock-free reads
}

func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *Conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if c.closed.Load() {
		return nil, driver.ErrBadConn
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	queryPtr, pinner := cString(query)
	defer unpin(pinner)

	var stmtPtr uintptr
	rc := sqlite3_prepare_v2(c.db, queryPtr, -1, &stmtPtr, 0)
	if rc != SQLITE_OK {
		return nil, fmt.Errorf("prepare failed: %s", getErrorMessage(c.db))
	}

	if stmtPtr == 0 {
		return nil, errors.New("empty statement")
	}

	stmt := &Stmt{
		conn:  c,
		stmt:  stmtPtr,
		query: query,
	}

	c.stmts.Store(stmtPtr, stmt)
	return stmt, nil
}

func (c *Conn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed.Load() {
		return nil
	}

	for _, stmt := range c.stmts.Iter() {
		sqlite3_finalize(stmt.stmt)
	}
	c.stmts.Clear()

	rc := sqlite3_close(c.db)
	if rc != SQLITE_OK {
		return fmt.Errorf("close failed: %s", errorString(rc))
	}

	c.closed.Store(true)
	return nil
}

func (c *Conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed.Load() {
		return nil, driver.ErrBadConn
	}

	if c.tx != nil && c.tx.finished {
		c.tx = nil
	}

	if c.tx != nil {
		return nil, errors.New("transaction already in progress")
	}

	sqliteMode := "DEFERRED"
	if !opts.ReadOnly {
		switch opts.Isolation {
		case driver.IsolationLevel(sql.LevelRepeatableRead),
			driver.IsolationLevel(sql.LevelSerializable),
			driver.IsolationLevel(sql.LevelLinearizable):
			sqliteMode = "IMMEDIATE"
		case driver.IsolationLevel(sql.LevelWriteCommitted),
			driver.IsolationLevel(sql.LevelSnapshot):
			sqliteMode = "EXCLUSIVE"
		}
	}

	query := fmt.Sprintf("BEGIN %s", sqliteMode)
	queryPtr, pinner := cString(query)
	defer unpin(pinner)

	rc := sqlite3_exec(c.db, queryPtr, 0, 0, 0)
	if rc != SQLITE_OK {
		return nil, fmt.Errorf("begin transaction failed: %s", getErrorMessage(c.db))
	}

	tx := &Tx{
		conn: c,
		opts: opts,
	}
	c.tx = tx

	return tx, nil
}

func (c *Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if len(args) == 0 {
		return c.execDirect(query)
	}

	stmt, err := c.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	return stmt.(*Stmt).ExecContext(ctx, args)
}

func (c *Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	stmt, err := c.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}

	rows, err := stmt.(*Stmt).QueryContext(ctx, args)
	if err != nil {
		stmt.Close()
		return nil, err
	}

	return rows, nil
}

func (c *Conn) Ping(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if c.closed.Load() {
		return driver.ErrBadConn
	}

	return nil
}

func (c *Conn) ResetSession(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed.Load() {
		return driver.ErrBadConn
	}

	if c.tx != nil {
		return errors.New("cannot reset session with active transaction")
	}

	for _, stmt := range c.stmts.Iter() {
		sqlite3_reset(stmt.stmt)
	}

	return nil
}

func (c *Conn) CheckNamedValue(nv *driver.NamedValue) error {
	return checkNamedValue(nv)
}

func (c *Conn) execDirect(query string) (driver.Result, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed.Load() {
		return nil, driver.ErrBadConn
	}

	queryPtr, pinner := cString(query)
	defer unpin(pinner)

	rc := sqlite3_exec(c.db, queryPtr, 0, 0, 0)
	if rc != SQLITE_OK {
		return nil, fmt.Errorf("exec failed: %s", getErrorMessage(c.db))
	}

	return &Result{
		lastInsertID: sqlite3_last_insert_rowid(c.db),
		rowsAffected: int64(sqlite3_changes(c.db)),
	}, nil
}

var (
	_ driver.Conn               = (*Conn)(nil)
	_ driver.ConnPrepareContext = (*Conn)(nil)
	_ driver.ConnBeginTx        = (*Conn)(nil)
	_ driver.ExecerContext      = (*Conn)(nil)
	_ driver.QueryerContext     = (*Conn)(nil)
	_ driver.Pinger             = (*Conn)(nil)
	_ driver.SessionResetter    = (*Conn)(nil)
	_ driver.NamedValueChecker  = (*Conn)(nil)
)
