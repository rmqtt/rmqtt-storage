# rmqtt-storage

<a href="https://github.com/rmqtt/rmqtt-storage/releases"><img alt="GitHub Release" src="https://img.shields.io/github/release/rmqtt/rmqtt-storage?color=brightgreen" /></a>
<a href="https://crates.io/crates/rmqtt-storage"><img alt="crates.io" src="https://img.shields.io/crates/v/rmqtt-storage" /></a>
<a href="https://docs.rs/rmqtt-storage"><img alt="Documentation" src="https://docs.rs/rmqtt-storage/badge.svg" /></a>


Is a simple wrapper around some key-value storages.


## Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
rmqtt-storage = "0.10"
```

## Features

- Supports basic operations of key-value libraries.
- Supports Map data type and related operations.
- Supports List data type and related operations.
- Supports key expiration.
- Provides an implementation for 'sled'.
- Provides an implementation for 'redis'.
- Provides an implementation for 'redis cluster'. Note: the 'len' feature is not supported yet.
- Uses [`postcard`](https://crates.io/crates/postcard) for binary serialization.
- Asynchronous API with command-based architecture for thread-safe operations.
- **Circuit Breaker** (optional): Protects Redis/Redis Cluster backends from cascading failures using [`tower-resilience-circuitbreaker`](https://crates.io/crates/tower-resilience-circuitbreaker). Configure failure thresholds, sliding windows, and automatic recovery.

## Feature Flags

| Feature | Description | Default |
|---------|-------------|---------|
| `sled` | Sled embedded database backend | no |
| `redis` | Single-node Redis backend | no |
| `redis-cluster` | Redis Cluster distributed backend | no |
| `ttl` | Key expiration / TTL support | no |
| `len` | Storage item count (`len()`) | no |
| `map_len` | Map item count (`map.len()`) | no |
| `circuit-breaker` | Circuit breaker protection (requires a storage backend) | no |

### Circuit Breaker Usage

Enable the feature and wrap your storage with `CircuitBrokenDB`:

```toml
[dependencies]
rmqtt-storage = { version = "0.10", features = ["sled", "circuit-breaker"] }
```

```rust
use rmqtt_storage::circuit_breaker::{CircuitBrokenDB, CircuitBreakerConfig};
use rmqtt_storage::init_db;

let inner = init_db(&cfg).await?;
let db = CircuitBrokenDB::new(inner, CircuitBreakerConfig::default());

// All existing APIs work transparently through the circuit breaker
let val: Option<String> = db.get("my-key").await?;
let map = db.map("my-map", None).await?;
map.insert("k", &"v").await?;
```

`CircuitBrokenDB` implements the [`StorageDB`] trait, and `CircuitBrokenMap`/`CircuitBrokenList` implement the [`Map`]/[`List`] traits respectively. This means they can be used polymorphically alongside native storage backends in generic contexts.

All Map / List / DB operations funnel through a **single shared circuit breaker** — a failing backend trips the breaker globally, and failure metrics are unified across all operation types.

When the failure rate exceeds the configured threshold, the circuit opens and subsequent calls fail fast instead of waiting for timeouts. After a configurable recovery period, the circuit transitions to half-open to test if the backend has recovered.

#### Operation Timeout

Set `operation_timeout` to abort calls that take longer than the specified duration. Timed-out calls are automatically counted as failures, triggering the circuit breaker:

```rust
use std::time::Duration;
use rmqtt_storage::circuit_breaker::CircuitBreakerConfig;

let config = CircuitBreakerConfig {
    operation_timeout: Some(Duration::from_secs(5)),
    ..CircuitBreakerConfig::default()
};
```

## Changelog

### 0.9.0

- **Serialization**: Migrated from `bincode` to `postcard` — faster, smaller encoded output
- **Refactor**: Removed sled transaction dependency, replaced with direct tree operations for better single-threaded throughput
- **Testing**: Added `serial_test` to prevent sled I/O contention in parallel test runs

### 0.10.0

- **Circuit Breaker**: New `circuit-breaker` feature — wraps `DefaultStorageDB` / `StorageMap` / `StorageList` with per-instance fault tolerance via `tower-resilience-circuitbreaker` 0.10
  - Sliding window failure rate detection (default: 50% over 20 calls)
  - Half-Open automatic recovery (default: 30 s wait)
  - Slow call detection (default: 2 s threshold)
  - **Operation timeout** (`operation_timeout`): abort hung calls and count as failures
  - State transition logging
  - Independent breakers per DB / Map / List instance
  - **Trait implementation**: `CircuitBrokenDB` now implements `StorageDB` trait, `CircuitBrokenMap` implements `Map` trait, `CircuitBrokenList` implements `List` trait — enabling polymorphic dispatch alongside native storage backends
  - Iterator methods (`map_iter`, `list_iter`, `scan`, `iter`, `key_iter`, `prefix_iter`) bypass the circuit breaker for safe pass-through
  - Zero impact when feature is disabled

### 0.10.1

- **Internal `_raw` methods**: Added `_insert_raw`, `_batch_insert_raw`, `remove_and_fetch_raw` to Redis and Redis Cluster backends — bypass `postcard` ser/de for `circuit-breaker` internal use
  - `insert_raw` / `batch_insert_raw` refactored to delegate to their `_raw` counterparts
  - `remove_and_fetch_raw` — new raw-bytes variant of `remove_and_fetch`
- **CB optimization**: `CBMapRequest::RemoveAndFetch` now uses a single `remove_and_fetch_raw` call instead of `get_raw` + `remove` (fewer round trips, atomic)
- **Bug fix**: Redis `_batch_insert` now guards against empty input to avoid `MSET` with zero arguments
- **Tests**: Added 7 new test cases — `push_limit`, `map_remove`/`list_remove` (DB level), empty list pop/get_index, empty map `remove_and_fetch`, large value (100KB), empty `batch_insert`/`batch_remove`
- **Cleanup**: Removed hanging CB integration tests (redundant with lib.rs `test_init_db` coverage); fixed `init_db` return type and feature-gated imports in `lib.rs`

### 0.10.3

- **Unified breaker**: Map / List / DB now share a **single** `CircuitBreaker` instance instead of having independent breakers — one failure metric for all operation types, simpler concurrency model
  - Removed outer `Mutex` — switched to clone-per-call `Arc<CircuitBreaker>` for true concurrent access
  - `CbTimeoutWrapper<S>` — Tower service layer that injects timeouts inside the breaker chain so timeouts are counted as failures by `DefaultClassifier`
  - Removed `RecordResult` hack and three-step `raw_call` — all three types now use a single `svc.call(req)`
  - Removed `CBMapRequest` / `CBListRequest` — Map/List operations unified into `CBStorageRequest` with inline `StorageMap` / `StorageList` handles
  - Removed `build_map_cb` / `build_list_cb` — Map/List share the DB-level breaker directly
