package sqlite

import "database/sql/driver"

type Result struct {
	lastInsertID int64
	rowsAffected int64
}

func (r *Result) LastInsertId() (int64, error) {
	return r.lastInsertID, nil
}

func (r *Result) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

var _ driver.Result = (*Result)(nil)
