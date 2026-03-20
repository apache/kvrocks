# AGENTS.md

This file provides guidance to AI coding agents (e.g., Claude Code, Cursor, ChatGPT Codex, Gemini) when working with code in this repository.

While working on Apache Kvrocks, please remember:

- Always use English in code and comments.
- Only add meaningful comments when the code's behavior is difficult to understand.
- Add or update tests to cover externally observable behavior and regressions when you change or add functionality.
- Always run the formatter before submitting changes.

## Build and Development Commands

### Building

```bash
# Build kvrocks and utilities
./x.py build                    # Build to ./build directory
./x.py build -j N               # Build with N parallel jobs
./x.py build --unittest         # Build with unit tests
./x.py build -DENABLE_OPENSSL=ON  # Build with TLS support
./x.py build --ninja            # Use Ninja build system
./x.py build --skip-build       # Only run CMake configure
./x.py build -DCMAKE_BUILD_TYPE=Debug  # Debug build

# Run a local server
./build/kvrocks -c kvrocks.conf

# Fetch dependencies
./x.py fetch-deps               # Fetch dependency archives
```

### Testing

```bash
# Build and run C++ unit tests
./x.py build --unittest
./x.py test cpp

# Run Go integration tests
./x.py test go

# Run specific Go test by path
./x.py test go tests/gocase/unit/...
```

### Lint

You must run the formatter and linters before submitting code changes. This ensures code quality and consistency across the project. It requires installing `clang-format`, `clang-tidy`, and `golangci-lint` locally. Please refer to the CONTRIBUTING.md for setup instructions.

```bash
# Format code (must pass before submitting)
./x.py format

# Check code format (fails if not formatted)
./x.py check format

# Run clang-tidy
./x.py check tidy

# Run golangci-lint for Go tests
./x.py check golangci-lint
```

## Architecture Overview

Apache Kvrocks is a distributed key-value NoSQL database compatible with the Redis protocol, using RocksDB as its storage engine.

### Core Structure

- **`src/server/`**: Main server orchestration, connection handling, and worker threads. The `Server` class manages the event loop, worker threads, and coordinates all components.
- **`src/storage/`**: RocksDB integration layer. Key classes:
  - `Storage`: Manages RocksDB instance, column families, and write batches
  - `Context`: Passes snapshot and batch between APIs for transactional consistency
- **`src/commands/`**: Redis protocol command implementations. Each command type has a corresponding `Commander` subclass.
- **`src/types/`**: Redis data structure implementations (String, Hash, List, Set, ZSet, Stream, etc.)
- **`src/cluster/`**: Cluster management, slot migration, and replication.
- **`src/search/`**: Full-text search and vector search (HNSW) implementation.
- **`src/config/`**: Server configuration parsing and management.
- **`src/cli/`**: Command-line interface utilities.
- **`src/common/`**: Shared utilities and helper functions.
- **`src/stats/`**: Statistics and metrics collection.

### Key Patterns

- **Column Families**: 8 column families are used - PrimarySubkey, Metadata, SecondarySubkey, PubSub, Propagate, Stream, Search, Index.
- **Command Registration**: Commands are registered via the `REDIS_REGISTER_COMMANDS` macro with flags like `kCmdWrite`, `kCmdReadOnly`, `kCmdBlocking`, etc.
- **Write Batch with Index**: Used for transactional mode to group writes before commit.
- **Worker Thread Model**: Libevent-based async I/O with dedicated worker threads.
- **Namespace Isolation**: Token-based multi-tenancy using the `__namespace` column family.

### Data Encoding

- `METADATA_ENCODING_VERSION=1` (default): Encodes 64-bit size and expire time in milliseconds.
- `METADATA_ENCODING_VERSION=0`: Legacy encoding.

Refer to https://kvrocks.apache.org/community/data-structure-on-rocksdb for more details.

## Coding Style and Naming Conventions

- C++ formatting follows `.clang-format` (Google-based, 2-space indent, 120-column limit, sorted includes).
- Use `.cc`/`.h` file extensions with `snake_case` filenames.
- Types use `PascalCase`; match existing patterns in nearby code.
- Use existing utilities and helper functions when possible; avoid reinventing the wheel.
- Go code should stay `gofmt`-clean and comply with `tests/gocase/.golangci.yml`.

## Testing Guidelines

You could provide Go tests for integration-level verification of command behaviors and C++ unit tests for internal logic. Focus on testing new features or bug fixes, and avoid adding tests that don't verify meaningful behavior changes.

- **Go Integration Tests** (`tests/gocase/`): Use `*_test.go` files, organized by feature (unit/, integration/, tls/).
- **C++ Unit Tests** (`tests/cppunit/`): Use `*_test.cc` files with GoogleTest framework.
- Add or update tests alongside behavior changes.
- Prefer focused unit tests; add integration coverage when commands or replication/storage behaviors change.
- Use `./x.py test ...` entry points for consistent setup.

## Commit Message Format

Use conventional commits with a scope indicating the affected component:

```
feat(rdb): add DUMP support for SortedInt type
fix(replication): prevent WAL exhaustion from slow consumers
fix(string): add empty string value check for INCR to match Redis behavior
perf(hash): use MultiGet to reduce RocksDB calls in HMSET
chore(deps): Bump rocksdb to v10.10.1
chore(ci): bump crate-ci/typos action to v1.43.1
chore(tests): replace to slices.Reverse() in go test
```

Common scopes: `server`, `storage`, `commands`, `cluster`, `search`, `types`, `replication`, `rdb`, `stream`, `hash`, `string`, `list`, `set`, `zset`, `deps`, `ci`, `tests`, `conf`.

## Common Tasks

### Adding a New Command

1. Create or update the command handler in `src/commands/`.
2. Implement the `Commander` subclass with `Parse()` and `Execute()` methods.
3. Register the command using `REDIS_REGISTER_COMMANDS` macro with appropriate flags.
4. Add the underlying data operation in `src/types/` if needed.
5. Add C++ unit tests in `tests/cppunit/`.
6. Add Go integration tests in `tests/gocase/`.

### Adding a New Data Type

1. Implement the type in `src/types/` following existing patterns.
2. Define the metadata encoding in `src/storage/`.
3. Add command handlers in `src/commands/`.
4. Register commands with the `REDIS_REGISTER_COMMANDS` macro.
5. Add tests for both the type operations and command handlers.

### Debugging

1. Check server logs (configurable log level in kvrocks config).
2. Use the `DEBUG` command for runtime inspection.
3. Use sanitizers via build flags for memory and thread issues.
4. Refer to `tests/lsan-suppressions` and `tests/tsan-suppressions` for known suppression rules.

## Important Notes

- Kvrocks aims for Redis protocol compatibility; always verify behavior against Redis when implementing or fixing commands.
- All changes must pass `./x.py check format` and `./x.py check tidy`.
- Don't change public command behavior unless requested.
- RocksDB is the core storage dependency; be cautious with storage-layer changes.
- Adding a new column family breaks forward compatibility; avoid this if possible and prefer using existing column families.
