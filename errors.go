package sqlite

import (
	"fmt"
)

func errorString(code int) string {
	switch code {
	case SQLITE_OK:
		return "not an error"
	case SQLITE_ERROR:
		return "SQL logic error"
	case SQLITE_INTERNAL:
		return "internal error"
	case SQLITE_PERM:
		return "permission denied"
	case SQLITE_ABORT:
		return "query aborted"
	case SQLITE_BUSY:
		return "database is locked"
	case SQLITE_LOCKED:
		return "database table is locked"
	case SQLITE_NOMEM:
		return "out of memory"
	case SQLITE_READONLY:
		return "attempt to write a readonly database"
	case SQLITE_INTERRUPT:
		return "interrupted"
	case SQLITE_IOERR:
		return "disk I/O error"
	case SQLITE_CORRUPT:
		return "database disk image is malformed"
	case SQLITE_NOTFOUND:
		return "unknown operation"
	case SQLITE_FULL:
		return "database or disk is full"
	case SQLITE_CANTOPEN:
		return "unable to open database file"
	case SQLITE_PROTOCOL:
		return "locking protocol error"
	case SQLITE_EMPTY:
		return "table is empty"
	case SQLITE_SCHEMA:
		return "database schema has changed"
	case SQLITE_TOOBIG:
		return "string or blob too big"
	case SQLITE_CONSTRAINT:
		return "constraint failed"
	case SQLITE_MISMATCH:
		return "datatype mismatch"
	case SQLITE_MISUSE:
		return "bad parameter or other API misuse"
	case SQLITE_NOLFS:
		return "large file support is disabled"
	case SQLITE_AUTH:
		return "authorization denied"
	case SQLITE_FORMAT:
		return "auxiliary database format error"
	case SQLITE_RANGE:
		return "column index out of range"
	case SQLITE_NOTADB:
		return "file is encrypted or is not a database"
	default:
		return fmt.Sprintf("unknown error code: %d", code)
	}
}

func getErrorMessage(db uintptr) string {
	msgPtr := sqlite3_errmsg(db)
	if msgPtr == 0 {
		code := sqlite3_errcode(db)
		return errorString(code)
	}
	return goString(msgPtr)
}
