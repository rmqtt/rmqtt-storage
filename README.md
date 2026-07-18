# rmqtt-storage

<a href="https://github.com/rmqtt/rmqtt-storage/releases"><img alt="GitHub Release" src="https://img.shields.io/github/release/rmqtt/rmqtt-storage?color=brightgreen" /></a>
<a href="https://crates.io/crates/rmqtt-storage"><img alt="crates.io" src="https://img.shields.io/crates/v/rmqtt-storage" /></a>
<a href="https://docs.rs/rmqtt-storage"><img alt="Documentation" src="https://docs.rs/rmqtt-storage/badge.svg" /></a>


Is a simple wrapper around some key-value storages.


## Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
rmqtt-storage = "0.11"
```

## Features

- **KV operations**: Insert, get, remove, contains_key, batch insert/remove
- **Map data type**: Named key-value maps with iter, prefix_iter, key_iter, clear, remove_with_prefix, remove_and_fetch, batch ops, and TTL support
- **List data type**: Ordered lists with push, pop, pushs, push_limit, get_index, all, iter, clear, and TTL support
- **Counters**: Atomic increment/decrement/set/get for isize values
- **Key expiration** (`ttl` feature): Per-key and per-collection TTL with background cleanup
- **Storage backends**:
  - **sled**: Embedded database (BTreeMap-like). Supports all optional features (`ttl`, `len`, `map_len`).
  - **redis**: Single-node Redis backend. Supports all optional features.
  - **redis cluster**: Distributed Redis cluster. Note: the `len` feature is not supported yet.
  - **redb**: Embedded ACID transactional B-Tree. Supports all optional features (`ttl`, `len`, `map_len`).
- Binary serialization via [`postcard`](https://crates.io/crates/postcard) — fast, compact
- Asynchronous API with command-channel architecture for thread-safe operations
- **Circuit Breaker** (optional): Protects all storage backends (sled, Redis, Redis Cluster, Redb) from cascading failures using [`tower-resilience-circuitbreaker`](https://crates.io/crates/tower-resilience-circuitbreaker). Configure failure thresholds, sliding windows, half-open recovery, and per-call operation timeouts.

## Feature Flags

| Feature | Description | Default |
|---------|-------------|---------|
| `sled` | Sled embedded database backend | no |
| `redb` | Redb embedded ACID transactional backend | no |
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
rmqtt-storage = { version = "0.11", features = ["sled", "circuit-breaker"] }
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

## Examples

The [`examples/session_storage_demo.rs`](examples/session_storage_demo.rs) simulates MQTT Broker session management, demonstrating all three core data structures:

- **DB (KV)**: Client session info + counters
- **Map**: Topic subscription management
- **List**: Offline message queue

Run with the sled backend (default):

```bash
cargo run --example session_storage_demo --features "sled,ttl,map_len,len"
```

Run with the redb backend:

```bash
cargo run --example session_storage_demo --features "redb,ttl,map_len,len" -- redb
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

### 0.10.2

- **Unified breaker**: Map / List / DB now share a **single** `CircuitBreaker` instance instead of having independent breakers — one failure metric for all operation types, simpler concurrency model
  - Removed outer `Mutex` — switched to clone-per-call `Arc<CircuitBreaker>` for true concurrent access
  - `CbTimeoutWrapper<S>` — Tower service layer that injects timeouts inside the breaker chain so timeouts are counted as failures by `DefaultClassifier`
  - Removed `RecordResult` hack and three-step `raw_call` — all three types now use a single `svc.call(req)`
  - Removed `CBMapRequest` / `CBListRequest` — Map/List operations unified into `CBStorageRequest` with inline `StorageMap` / `StorageList` handles
  - Removed `build_map_cb` / `build_list_cb` — Map/List share the DB-level breaker directly

### 0.11.0 (current)

- **Redb embedded backend** (new feature `redb`): Full ACID transactional storage backend built on [`redb`](https://github.com/cberner/redb) 4.1.0 — embedded B-Tree with MVCC, WAL, and fsync-guaranteed durability
  - Same command-channel + `spawn_blocking` async architecture as sled
  - 5 typed `TableDefinition<&[u8], &[u8]>` tables (`KV_TABLE`, `MAP_TABLE`, `LIST_TABLE`, `EXPIRE_KEYS_TABLE`, `KEY_EXPIRE_TABLE`)
  - Every write operation wrapped in `begin_write()` + `commit()` for ACID guarantees
  - Read operations use `begin_read()` for snapshot isolation
  - Snapshot-collection iterators (eager `collect::<Vec>()` in read transaction)
  - Full feature support: `ttl`, `len`, `map_len`, `circuit-breaker`
  - Configurable cache size (default: 1GB) via `RedbConfig`
  - Single-file storage — deployment-friendly, no external processes
- **Test coverage (P0/P1/P2)**: 6 new tests covering previously untested `StorageDB::info()`, empty container iterators, `remove_with_prefix` edge cases, complex type ser/de through all 3 storage types, TTL on created Map/List, and scan with no match
- **6-dimension test expansion**: 23 additional tests from 6 angles — cross-feature interaction (TTL+batch, TTL+counter, map_iter+modify, list state chain), concurrency (counter atomicity, concurrent read/write, map_iter consistency), data integrity (overwrite, clear+insert, counter sequence including isize::MAX/MIN, batch duplicate keys, remove_and_fetch idempotence), edge cases (empty/Unicode/binary keys, 1MB values, special characters in names, limit=0 push_limit, map/list name conflict), error handling (get/remove non-existent, double remove), dispatch correctness (name roundtrip for Map/List)
- **Total test count**: 37 → **65** (redb) / 65+ (sled with circuit-breaker)
- **Cross-backend compatibility**: Fixed `test_list_push_limit_zero` divergence between sled and redb; fixed `test_map_iter_while_modifying` cleanup approach; fixed `test_name_with_special_chars` name conflict
- **Bug fix**: `exec_map_is_expired` and `exec_list_is_expired` in `storage_redb.rs` — fixed compilation error when using `_db` parameter name with `ttl` feature (compilation failed with `error[E0425]: cannot find value db`)
