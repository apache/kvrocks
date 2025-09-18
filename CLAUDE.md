# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build System

Apache Kvrocks uses a Python-based build script (`x.py`) for all development tasks:

### Core Commands
- `./x.py build` - Build kvrocks and kvrocks2redis binaries (default: build/ directory)
- `./x.py build --unittest` - Build with unit tests included
- `./x.py build -j N` - Build with N parallel jobs
- `./x.py build --ninja` - Use Ninja build system instead of Make
- `./x.py build --compiler clang` - Use specific compiler (auto, gcc, clang)
- `./x.py build -DENABLE_OPENSSL=ON` - Build with TLS support
- `./x.py build -DCMAKE_BUILD_TYPE=Debug` - Build in debug mode

### Testing
- `./x.py test cpp` - Run C++ unit tests
- `./x.py test go` - Run Go integration tests (requires redis-cli)
- `./x.py build --unittest && ./x.py test cpp` - Build and run C++ tests

### Code Quality
- `./x.py format` - Format source code with clang-format
- `./x.py check format` - Check code formatting
- `./x.py check tidy` - Run clang-tidy static analysis
- `./x.py check golangci-lint` - Run Go linter on test code

### Running Kvrocks
- `./build/kvrocks -c kvrocks.conf` - Run server with configuration
- Default port: 6666 (Redis-compatible protocol)
- Connect with: `redis-cli -p 6666`

## Architecture Overview

### Core Modules
- **src/cli/** - Main entry point and daemon utilities
- **src/server/** - Redis protocol server, connection handling, worker threads
- **src/storage/** - RocksDB storage engine integration, batch operations, compaction
- **src/types/** - Redis data type implementations (string, hash, list, set, zset, bitmap, etc.)
- **src/commands/** - Redis command implementations organized by data type
- **src/cluster/** - Cluster mode support, slot management, replication
- **src/config/** - Configuration management and validation
- **src/common/** - Shared utilities (encoding, parsing, threading, etc.)
- **src/search/** - Full-text search capabilities with HNSW vector indexing
- **src/stats/** - Performance monitoring and disk usage statistics

### Data Types
Kvrocks implements Redis-compatible data types plus extensions:
- Standard: String, Hash, List, Set, Sorted Set, Bitmap, HyperLogLog
- Extensions: Bloom Filter, JSON, Stream, Time Series, TDigest, Search indexes
- All types are stored efficiently on RocksDB with custom encoding

### Storage Engine
- Built on RocksDB for persistent storage
- Implements Redis-style key expiration and TTL
- Supports multiple namespaces with token-based access control
- Custom compaction filters for expired key cleanup

### Cluster Architecture
- Redis Cluster protocol compatible
- Centralized management (no gossip protocol)
- Slot-based data distribution (16384 slots)
- Async replication using binlog

## Development Guidelines

### Prerequisites
- CMake 3.16+, GCC 8+/Clang 9+, autoconf, python3
- For TLS: OpenSSL development libraries
- For testing: redis-cli, Go compiler

### Testing Structure
- **tests/cppunit/** - C++ unit tests using Google Test
- **tests/gocase/** - Go integration tests covering Redis protocol compatibility
- Use `./x.py test cpp --` to pass additional arguments to unit tests
- Use `./x.py test go --` to pass additional arguments to Go tests

### Code Organization
- Command implementations in `src/commands/cmd_*.cc`
- Data type logic in `src/types/redis_*.cc`
- Storage abstractions in `src/storage/`
- Network protocol handling in `src/server/`

### Configuration
- Main config file: `kvrocks.conf`
- Runtime configuration via CONFIG command
- Namespace management for multi-tenancy
- Support for both standalone and cluster modes