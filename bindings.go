package sqlite

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"

	"github.com/ebitengine/purego"
)

const (
	SQLITE_OK         = 0
	SQLITE_ERROR      = 1
	SQLITE_INTERNAL   = 2
	SQLITE_PERM       = 3
	SQLITE_ABORT      = 4
	SQLITE_BUSY       = 5
	SQLITE_LOCKED     = 6
	SQLITE_NOMEM      = 7
	SQLITE_READONLY   = 8
	SQLITE_INTERRUPT  = 9
	SQLITE_IOERR      = 10
	SQLITE_CORRUPT    = 11
	SQLITE_NOTFOUND   = 12
	SQLITE_FULL       = 13
	SQLITE_CANTOPEN   = 14
	SQLITE_PROTOCOL   = 15
	SQLITE_EMPTY      = 16
	SQLITE_SCHEMA     = 17
	SQLITE_TOOBIG     = 18
	SQLITE_CONSTRAINT = 19
	SQLITE_MISMATCH   = 20
	SQLITE_MISUSE     = 21
	SQLITE_NOLFS      = 22
	SQLITE_AUTH       = 23
	SQLITE_FORMAT     = 24
	SQLITE_RANGE      = 25
	SQLITE_NOTADB     = 26
	SQLITE_NOTICE     = 27
	SQLITE_WARNING    = 28
	SQLITE_ROW        = 100
	SQLITE_DONE       = 101

	SQLITE_OPEN_READONLY  = 0x00000001
	SQLITE_OPEN_READWRITE = 0x00000002
	SQLITE_OPEN_CREATE    = 0x00000004
	SQLITE_OPEN_URI       = 0x00000040

	SQLITE_STATIC            = uintptr(0)
	SQLITE_TRANSIENT         = ^uintptr(0)
	SQLITE_OPEN_MEMORY       = 0x00000080
	SQLITE_OPEN_NOMUTEX      = 0x00008000
	SQLITE_OPEN_FULLMUTEX    = 0x00010000
	SQLITE_OPEN_SHAREDCACHE  = 0x00020000
	SQLITE_OPEN_PRIVATECACHE = 0x00040000

	SQLITE_INTEGER = 1
	SQLITE_FLOAT   = 2
	SQLITE_TEXT    = 3
	SQLITE_BLOB    = 4
	SQLITE_NULL    = 5
)

var (
	libsqlite3 uintptr
	initOnce   sync.Once
	initErr    error

	sqlite3_open_v2              func(filename uintptr, ppDb *uintptr, flags int, zVfs uintptr) int
	sqlite3_close                func(db uintptr) int
	sqlite3_prepare_v2           func(db uintptr, zSql uintptr, nByte int, ppStmt *uintptr, pzTail uintptr) int
	sqlite3_step                 func(stmt uintptr) int
	sqlite3_finalize             func(stmt uintptr) int
	sqlite3_reset                func(stmt uintptr) int
	sqlite3_column_count         func(stmt uintptr) int
	sqlite3_column_name          func(stmt uintptr, n int) uintptr
	sqlite3_column_type          func(stmt uintptr, iCol int) int
	sqlite3_column_int64         func(stmt uintptr, iCol int) int64
	sqlite3_column_double        func(stmt uintptr, iCol int) float64
	sqlite3_column_text          func(stmt uintptr, iCol int) uintptr
	sqlite3_column_blob          func(stmt uintptr, iCol int) uintptr
	sqlite3_column_bytes         func(stmt uintptr, iCol int) int
	sqlite3_bind_parameter_count func(stmt uintptr) int
	sqlite3_bind_null            func(stmt uintptr, idx int) int
	sqlite3_bind_int64           func(stmt uintptr, idx int, val int64) int
	sqlite3_bind_double          func(stmt uintptr, idx int, val float64) int
	sqlite3_bind_text            func(stmt uintptr, idx int, val uintptr, n int, destructor uintptr) int
	sqlite3_bind_blob            func(stmt uintptr, idx int, val uintptr, n int, destructor uintptr) int
	sqlite3_last_insert_rowid    func(db uintptr) int64
	sqlite3_changes              func(db uintptr) int
	sqlite3_errmsg               func(db uintptr) uintptr
	sqlite3_errcode              func(db uintptr) int
	sqlite3_exec                 func(db uintptr, sql uintptr, callback uintptr, arg uintptr, errmsg uintptr) int
	sqlite3_interrupt            func(db uintptr)
	sqlite3_busy_handler         func(db uintptr, callback uintptr, arg uintptr) int
	sqlite3_busy_timeout         func(db uintptr, ms int) int
	sqlite3_limit                func(db uintptr, id int, newVal int) int
	sqlite3_extended_errcode     func(db uintptr) int
)

func loadSQLite3() error {
	initOnce.Do(func() {
		initErr = loadLibrary()
	})
	return initErr
}

func loadLibrary() error {
	var libraryNames []string

	if path := os.Getenv("SQLITE_PATH"); path != "" {
		libraryNames = append(libraryNames, path)
	}

	switch runtime.GOOS {
	case "darwin":
		libraryNames = append(libraryNames,
			"libsqlite3.dylib",
			"libsqlite3.0.dylib",
			"/usr/lib/libsqlite3.dylib",
			"/opt/homebrew/lib/libsqlite3.dylib",
			"/usr/local/lib/libsqlite3.dylib",
		)
	case "linux":
		libraryNames = append(libraryNames,
			"libsqlite3.so",
			"libsqlite3.so.0",
			"/usr/lib/x86_64-linux-gnu/libsqlite3.so.0",
			"/usr/lib/libsqlite3.so.0",
			"/usr/lib64/libsqlite3.so.0",
		)
	case "windows":
		libraryNames = append(libraryNames,
			"sqlite3.dll",
			"libsqlite3.dll",
			"libsqlite3-0.dll",
			"C:\\Windows\\System32\\sqlite3.dll",
		)
	default:
		return fmt.Errorf("unsupported platform: %s", runtime.GOOS)
	}

	var loadErrors []string
	for _, name := range libraryNames {
		lib, err := purego.Dlopen(name, purego.RTLD_NOW|purego.RTLD_GLOBAL)
		if err == nil {
			libsqlite3 = lib
			if err := registerFunctions(); err != nil {
				return fmt.Errorf("failed to register functions from %s: %w", name, err)
			}
			return nil
		}
		loadErrors = append(loadErrors, fmt.Sprintf("%s: %v", name, err))
	}

	return fmt.Errorf("failed to load sqlite3 library from any of the following locations:\n  %s",
		strings.Join(loadErrors, "\n  "))
}

func registerFunctions() error {
	if libsqlite3 == 0 {
		return errors.New("library not loaded")
	}

	purego.RegisterLibFunc(&sqlite3_open_v2, libsqlite3, "sqlite3_open_v2")
	purego.RegisterLibFunc(&sqlite3_close, libsqlite3, "sqlite3_close")
	purego.RegisterLibFunc(&sqlite3_prepare_v2, libsqlite3, "sqlite3_prepare_v2")
	purego.RegisterLibFunc(&sqlite3_step, libsqlite3, "sqlite3_step")
	purego.RegisterLibFunc(&sqlite3_finalize, libsqlite3, "sqlite3_finalize")
	purego.RegisterLibFunc(&sqlite3_reset, libsqlite3, "sqlite3_reset")
	purego.RegisterLibFunc(&sqlite3_column_count, libsqlite3, "sqlite3_column_count")
	purego.RegisterLibFunc(&sqlite3_column_name, libsqlite3, "sqlite3_column_name")
	purego.RegisterLibFunc(&sqlite3_column_type, libsqlite3, "sqlite3_column_type")
	purego.RegisterLibFunc(&sqlite3_column_int64, libsqlite3, "sqlite3_column_int64")
	purego.RegisterLibFunc(&sqlite3_column_double, libsqlite3, "sqlite3_column_double")
	purego.RegisterLibFunc(&sqlite3_column_text, libsqlite3, "sqlite3_column_text")
	purego.RegisterLibFunc(&sqlite3_column_blob, libsqlite3, "sqlite3_column_blob")
	purego.RegisterLibFunc(&sqlite3_column_bytes, libsqlite3, "sqlite3_column_bytes")
	purego.RegisterLibFunc(&sqlite3_bind_parameter_count, libsqlite3, "sqlite3_bind_parameter_count")
	purego.RegisterLibFunc(&sqlite3_bind_null, libsqlite3, "sqlite3_bind_null")
	purego.RegisterLibFunc(&sqlite3_bind_int64, libsqlite3, "sqlite3_bind_int64")
	purego.RegisterLibFunc(&sqlite3_bind_double, libsqlite3, "sqlite3_bind_double")
	purego.RegisterLibFunc(&sqlite3_bind_text, libsqlite3, "sqlite3_bind_text")
	purego.RegisterLibFunc(&sqlite3_bind_blob, libsqlite3, "sqlite3_bind_blob")
	purego.RegisterLibFunc(&sqlite3_last_insert_rowid, libsqlite3, "sqlite3_last_insert_rowid")
	purego.RegisterLibFunc(&sqlite3_changes, libsqlite3, "sqlite3_changes")
	purego.RegisterLibFunc(&sqlite3_errmsg, libsqlite3, "sqlite3_errmsg")
	purego.RegisterLibFunc(&sqlite3_errcode, libsqlite3, "sqlite3_errcode")
	purego.RegisterLibFunc(&sqlite3_exec, libsqlite3, "sqlite3_exec")
	purego.RegisterLibFunc(&sqlite3_interrupt, libsqlite3, "sqlite3_interrupt")
	purego.RegisterLibFunc(&sqlite3_busy_handler, libsqlite3, "sqlite3_busy_handler")
	purego.RegisterLibFunc(&sqlite3_busy_timeout, libsqlite3, "sqlite3_busy_timeout")
	purego.RegisterLibFunc(&sqlite3_limit, libsqlite3, "sqlite3_limit")
	purego.RegisterLibFunc(&sqlite3_extended_errcode, libsqlite3, "sqlite3_extended_errcode")
	return nil
}
