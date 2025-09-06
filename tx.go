package sqlite

import (
	"database/sql/driver"
	"fmt"
)

type Tx struct {
	conn     *Conn
	opts     driver.TxOptions
	finished bool
}

func (t *Tx) Commit() error {
	if t.finished {
		return fmt.Errorf("transaction already finished")
	}

	t.conn.mu.Lock()
	defer t.conn.mu.Unlock()

	queryPtr, pinner := cString("COMMIT")
	defer unpin(pinner)

	rc := sqlite3_exec(t.conn.db, queryPtr, 0, 0, 0)
	if rc != SQLITE_OK {
		return fmt.Errorf("commit failed: %s", getErrorMessage(t.conn.db))
	}

	t.finished = true
	t.conn.tx = nil
	return nil
}

func (t *Tx) Rollback() error {
	if t.finished {
		return nil
	}

	t.conn.mu.Lock()
	defer t.conn.mu.Unlock()

	queryPtr, pinner := cString("ROLLBACK")
	defer unpin(pinner)

	rc := sqlite3_exec(t.conn.db, queryPtr, 0, 0, 0)
	if rc != SQLITE_OK {
		return fmt.Errorf("rollback failed: %s", getErrorMessage(t.conn.db))
	}

	t.finished = true
	t.conn.tx = nil
	return nil
}

var _ driver.Tx = (*Tx)(nil)
