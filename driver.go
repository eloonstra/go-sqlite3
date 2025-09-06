package sqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
)

func init() {
	sql.Register("sqlite3", &Driver{})
}

type Driver struct{}

func (d *Driver) Open(dsn string) (driver.Conn, error) {
	if err := loadSQLite3(); err != nil {
		return nil, err
	}

	cfg, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}

	conn, err := openDB(cfg)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func (d *Driver) OpenConnector(dsn string) (driver.Connector, error) {
	cfg, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}

	return &connector{
		driver: d,
		dsn:    dsn,
		cfg:    cfg,
	}, nil
}

type connector struct {
	driver *Driver
	dsn    string
	cfg    *config
}

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	return c.driver.Open(c.dsn)
}

func (c *connector) Driver() driver.Driver {
	return c.driver
}

var (
	_ driver.Driver        = (*Driver)(nil)
	_ driver.DriverContext = (*Driver)(nil)
	_ driver.Connector     = (*connector)(nil)
)

type config struct {
	path        string
	flags       int
	busyTimeout int
	cache       bool
	mutex       string
}

func parseDSN(dsn string) (*config, error) {
	cfg := &config{
		path:  dsn,
		flags: SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE,
	}

	if dsn == "" {
		return nil, errors.New("empty DSN")
	}

	if strings.HasPrefix(dsn, "file:") {
		u, err := url.Parse(dsn)
		if err != nil {
			return nil, fmt.Errorf("invalid DSN: %w", err)
		}

		cfg.path = u.Path

		q := u.Query()

		if mode := q.Get("mode"); mode != "" {
			switch mode {
			case "ro":
				cfg.flags = SQLITE_OPEN_READONLY
			case "rw":
				cfg.flags = SQLITE_OPEN_READWRITE
			case "rwc":
				cfg.flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE
			case "memory":
				cfg.flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_MEMORY
			default:
				return nil, fmt.Errorf("invalid mode: %s", mode)
			}
		}

		if cache := q.Get("cache"); cache != "" {
			switch cache {
			case "shared":
				cfg.flags |= SQLITE_OPEN_SHAREDCACHE
				cfg.cache = true
			case "private":
				cfg.flags |= SQLITE_OPEN_PRIVATECACHE
			}
		}

		if mutex := q.Get("_mutex"); mutex != "" {
			cfg.mutex = mutex
			switch mutex {
			case "no":
				cfg.flags |= SQLITE_OPEN_NOMUTEX
			case "full":
				cfg.flags |= SQLITE_OPEN_FULLMUTEX
			}
		}

		cfg.busyTimeout = 5000
		if bt := q.Get("_busy_timeout"); bt != "" {
			if timeout, err := strconv.Atoi(bt); err == nil && timeout > 0 {
				cfg.busyTimeout = timeout
			}
		}
	}

	if dsn == ":memory:" {
		cfg.path = ":memory:"
		cfg.flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_MEMORY
	}

	cfg.flags |= SQLITE_OPEN_URI

	return cfg, nil
}

func openDB(cfg *config) (*Conn, error) {
	var db uintptr

	pathPtr, pinner := cString(cfg.path)
	defer unpin(pinner)

	rc := sqlite3_open_v2(pathPtr, &db, cfg.flags, 0)
	if rc != SQLITE_OK {
		if db != 0 {
			errMsg := getErrorMessage(db)
			sqlite3_close(db)
			return nil, fmt.Errorf("failed to open database: %s", errMsg)
		}
		return nil, fmt.Errorf("failed to open database: %s", errorString(rc))
	}

	conn := &Conn{
		db:    db,
		stmts: NewThreadSafeMap[uintptr, *Stmt](),
		mu:    &sync.Mutex{},
	}

	if cfg.busyTimeout > 0 {
		sqlite3_busy_timeout(db, cfg.busyTimeout)
	}

	return conn, nil
}
