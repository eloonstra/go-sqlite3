# go-sqlite3

[![Go Reference](https://pkg.go.dev/badge/github.com/eloonstra/go-sqlite3.svg)](https://pkg.go.dev/github.com/eloonstra/go-sqlite3)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Pure Go SQLite3 driver using dynamic linking.

> [!WARNING]  
> This driver is very early in development and should not be used in production environments. Critical features may be
> missing or unstable.

## Features

- No CGO required, uses dynamic linking via [ebitengine/purego](https://github.com/ebitengine/purego)
- Fully compatible with `database/sql`
- Works on Linux, macOS, and Windows (untested)
- Full support for `:memory:` databases

## Planned Features

- **Hrana protocol support** - Connect to hosted SQLite/libSQL instances
- **Custom SQL functions** - Register Go functions as SQLite UDFs
- **Virtual tables** - Create custom virtual table modules in Go

## DSN (Data Source Name)

The driver supports various DSN formats:

### Basic Format

```go
// File path
db, err := sql.Open("sqlite3", "mydb.db")

// In-memory database
db, err := sql.Open("sqlite3", ":memory:")
```

### DSN Parameters

| Parameter       | Values                      | Description                                                                |
|-----------------|-----------------------------|----------------------------------------------------------------------------|
| `mode`          | `ro`, `rw`, `rwc`, `memory` | Database access mode (read-only, read-write, read-write-create, in-memory) |
| `cache`         | `shared`, `private`         | Cache mode for database connections                                        |
| `_mutex`        | `no`, `full`                | Threading mode (no mutex, full mutex)                                      |
| `_busy_timeout` | milliseconds                | Timeout for busy handler (default: 5000ms)                                 |

### Examples

```go
// Read-only mode
db, err := sql.Open("sqlite3", "file:mydb.db?mode=ro")

// Read-write with shared cache
db, err := sql.Open("sqlite3", "file:mydb.db?mode=rw&cache=shared")

// Custom busy timeout (10 seconds)
db, err := sql.Open("sqlite3", "file:mydb.db?_busy_timeout=10000")
```

## Requirements

- Go 1.23 or higher
- SQLite3 library installed on the system
    - **Linux**: Usually pre-installed or available via package manager
    - **macOS**: Pre-installed with the OS
    - **Windows**: May require SQLite3 DLL in PATH

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.