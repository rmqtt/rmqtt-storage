//! Circuit breaker wrappers for storage backends using `tower_resilience_circuitbreaker`.
//!
//! Provides [`CircuitBrokenDB`], [`CircuitBrokenMap`], and [`CircuitBrokenList`] which funnel
//! all operations through a **single shared** DB-level circuit breaker.  Map / List / DB
//! operations share one failure metric so that a failing backend trips the breaker globally.
//!
//! The Tower chain is:
//!
//! ```text
//! CircuitBreaker<CbTimeoutWrapper<CBStorageService>, DefaultClassifier>
//! ```
//!
//! Inside `CbTimeoutWrapper` applies the optional per-operation timeout —
//! timeouts are counted as failures by the `DefaultClassifier` and correctly
//! contribute to the sliding window.
//!
//! # Concurrency
//!
//! The `CircuitBreaker` is held behind an `Arc` and cloned per-call (cheap —
//! only Arcs are copied).  The inner `Circuit` state is shared via its own
//! `Arc<tokio::sync::Mutex<Circuit>>`, providing fine-grained locking.  No
//! outer `Mutex` is needed — `is_open()` is an atomic read, and `call()`
//! acquires the inner lock only during `try_acquire()` / result recording
//! (microseconds).
//!
//! # Feature gate
//!
//! This module is only compiled when the `circuit-breaker` feature is enabled **and** at least
//! one storage backend (sled / redis / redis-cluster) is also enabled.
//!
//! # Example
//!
//! ```ignore
//! use rmqtt_storage::circuit_breaker::{CircuitBrokenDB, CircuitBreakerConfig};
//!
//! let inner = init_db(&cfg).await.unwrap();
//! let db = CircuitBrokenDB::new(inner, CircuitBreakerConfig::default());
//! let val: Option<String> = db.get("my-key").await.unwrap();
//! ```

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::Serialize;
#[allow(unused_imports)]
use tower::{Layer, Service, ServiceExt};
use tower_resilience_circuitbreaker::{
    CircuitBreakerError, CircuitBreakerLayer, DefaultClassifier, SlidingWindowType,
};

use crate::storage::{
    AsyncIterator, DefaultStorageDB, IterItem, Key, List, Map, StorageDB, StorageList, StorageMap,
};
use crate::{Result, TimestampMillis};

// StorageMap doesn't derive Debug; provide one so CBStorageRequest can.
impl fmt::Debug for StorageMap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            #[cfg(feature = "sled")]
            StorageMap::Sled(_) => write!(f, "SledMap"),
            #[cfg(feature = "redis")]
            StorageMap::Redis(_) => write!(f, "RedisMap"),
            #[cfg(feature = "redis-cluster")]
            StorageMap::RedisCluster(_) => write!(f, "RedisClusterMap"),
            #[cfg(feature = "redb")]
            StorageMap::Redb(_) => write!(f, "RedbMap"),
        }
    }
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Circuit breaker configuration.
///
/// Defaults match the Redis-network workload typical of this crate.
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// Failure rate threshold (0.0 – 1.0). Default: 0.25
    pub failure_rate_threshold: f64,
    /// Sliding window configuration (enum — see [`WindowConfig`]).
    /// Default: `WindowConfig::CountBased` with [`CountBasedWindowConfig`] defaults.
    pub window: WindowConfig,
    /// Minimum number of calls before the breaker can trip. Default: 10
    pub minimum_number_of_calls: usize,
    /// Duration the breaker stays Open before transitioning to HalfOpen. Default: 30 s
    pub wait_duration_in_open: Duration,
    /// Slow call duration threshold. Default: 2 s
    pub slow_call_duration_threshold: Duration,
    /// Slow call rate threshold. Default: 1.0 (disabled)
    pub slow_call_rate_threshold: f64,
    /// Per-operation timeout. If `Some(dur)`, every call that takes longer than
    /// `dur` is aborted and counted as a failure by the circuit breaker.
    /// Default: `None` (no timeout).
    pub operation_timeout: Option<Duration>,
    /// Name label for observability. Default: "storage"
    pub name: String,
}

/// CountBased 滑动窗口专有配置。
///
/// 仅在 `WindowConfig::CountBased` 时生效。
#[derive(Debug, Clone)]
pub struct CountBasedWindowConfig {
    /// 滑动窗口大小（调用次数）。默认: 20
    pub sliding_window_size: usize,
}

impl Default for CountBasedWindowConfig {
    fn default() -> Self {
        Self {
            sliding_window_size: 20,
        }
    }
}

impl From<CountBasedWindowConfig> for WindowConfig {
    fn from(cfg: CountBasedWindowConfig) -> Self {
        Self::CountBased(cfg)
    }
}

/// TimeBased 滑动窗口专有配置。
///
/// 仅在 `WindowConfig::TimeBased` 时生效。
#[derive(Debug, Clone)]
pub struct TimeBasedWindowConfig {
    /// 滑动窗口时间跨度。默认: 45s
    pub sliding_window_duration: Duration,
    /// 最大跟踪调用数（影响 `minimum_number_of_calls` 默认值）。默认: 20
    pub sliding_window_size: usize,
}

impl Default for TimeBasedWindowConfig {
    fn default() -> Self {
        Self {
            sliding_window_duration: Duration::from_secs(45),
            sliding_window_size: 20,
        }
    }
}

impl From<TimeBasedWindowConfig> for WindowConfig {
    fn from(cfg: TimeBasedWindowConfig) -> Self {
        Self::TimeBased(cfg)
    }
}

/// 滑动窗口配置枚举。
///
/// - `CountBased` — 基于调用次数的滑动窗口
/// - `TimeBased` — 基于时间跨度的滑动窗口
#[derive(Debug, Clone)]
pub enum WindowConfig {
    /// 基于调用次数的滑动窗口（携带 `CountBasedWindowConfig`）
    CountBased(CountBasedWindowConfig),
    /// 基于时间跨度的滑动窗口（携带 `TimeBasedWindowConfig`）
    TimeBased(TimeBasedWindowConfig),
}

impl Default for WindowConfig {
    fn default() -> Self {
        Self::TimeBased(TimeBasedWindowConfig::default())
    }
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_rate_threshold: 0.25,
            window: WindowConfig::default(),
            minimum_number_of_calls: 10,
            wait_duration_in_open: Duration::from_secs(30),
            slow_call_duration_threshold: Duration::from_secs(2),
            slow_call_rate_threshold: 1.0,
            operation_timeout: None,
            name: "storage".into(),
        }
    }
}

// ---------------------------------------------------------------------------
// DB-level Request / Response
// ---------------------------------------------------------------------------

/// Unified request enum for all `DefaultStorageDB` operations.
#[derive(Debug, Clone)]
pub enum CBStorageRequest {
    Insert {
        key: Vec<u8>,
        val: Vec<u8>,
    },
    Get {
        key: Vec<u8>,
    },
    Remove {
        key: Vec<u8>,
    },
    ContainsKey {
        key: Vec<u8>,
    },
    #[allow(dead_code)]
    DbMap {
        name: Vec<u8>,
        expire: Option<TimestampMillis>,
    },
    DbMapRemove {
        name: Vec<u8>,
    },
    DbMapContainsKey {
        key: Vec<u8>,
    },
    #[allow(dead_code)]
    DbList {
        name: Vec<u8>,
        expire: Option<TimestampMillis>,
    },
    DbListRemove {
        name: Vec<u8>,
    },
    DbListContainsKey {
        key: Vec<u8>,
    },
    CounterIncr {
        key: Vec<u8>,
        increment: isize,
    },
    CounterDecr {
        key: Vec<u8>,
        decrement: isize,
    },
    CounterGet {
        key: Vec<u8>,
    },
    CounterSet {
        key: Vec<u8>,
        val: isize,
    },
    BatchInsert {
        key_vals: Vec<(Vec<u8>, Vec<u8>)>,
    },
    BatchRemove {
        keys: Vec<Vec<u8>>,
    },
    DbSize,
    Info,
    #[cfg(feature = "len")]
    Len,
    #[cfg(feature = "ttl")]
    ExpireAt {
        key: Vec<u8>,
        at: TimestampMillis,
    },
    #[cfg(feature = "ttl")]
    Expire {
        key: Vec<u8>,
        dur: TimestampMillis,
    },
    #[cfg(feature = "ttl")]
    Ttl {
        key: Vec<u8>,
    },
    // ── Map operations ──
    MapInsert {
        map: StorageMap,
        key: Vec<u8>,
        val: Vec<u8>,
    },
    MapGet {
        map: StorageMap,
        key: Vec<u8>,
    },
    MapRemove {
        map: StorageMap,
        key: Vec<u8>,
    },
    MapContainsKey {
        map: StorageMap,
        key: Vec<u8>,
    },
    MapIsEmpty {
        map: StorageMap,
    },
    MapClear {
        map: StorageMap,
    },
    MapRemoveAndFetch {
        map: StorageMap,
        key: Vec<u8>,
    },
    MapRemoveWithPrefix {
        map: StorageMap,
        prefix: Vec<u8>,
    },
    MapBatchInsert {
        map: StorageMap,
        key_vals: Vec<(Vec<u8>, Vec<u8>)>,
    },
    MapBatchRemove {
        map: StorageMap,
        keys: Vec<Vec<u8>>,
    },
    #[cfg(feature = "map_len")]
    MapLen {
        map: StorageMap,
    },
    #[cfg(feature = "ttl")]
    MapExpireAt {
        map: StorageMap,
        at: TimestampMillis,
    },
    #[cfg(feature = "ttl")]
    MapExpire {
        map: StorageMap,
        dur: TimestampMillis,
    },
    #[cfg(feature = "ttl")]
    MapTtl {
        map: StorageMap,
    },
    // ── List operations ──
    ListPush {
        list: StorageList,
        val: Vec<u8>,
    },
    ListPushs {
        list: StorageList,
        vals: Vec<Vec<u8>>,
    },
    ListPushLimit {
        list: StorageList,
        val: Vec<u8>,
        limit: usize,
        pop_front_if_limited: bool,
    },
    ListPop {
        list: StorageList,
    },
    ListGetAll {
        list: StorageList,
    },
    ListGetIndex {
        list: StorageList,
        idx: usize,
    },
    ListLen {
        list: StorageList,
    },
    ListIsEmpty {
        list: StorageList,
    },
    ListClear {
        list: StorageList,
    },
    #[cfg(feature = "ttl")]
    ListExpireAt {
        list: StorageList,
        at: TimestampMillis,
    },
    #[cfg(feature = "ttl")]
    ListExpire {
        list: StorageList,
        dur: TimestampMillis,
    },
    #[cfg(feature = "ttl")]
    ListTtl {
        list: StorageList,
    },
}

/// Unified response enum for all `DefaultStorageDB` operations.
// Manual Debug impl because StorageMap / StorageList don't implement Debug.
pub enum CBStorageResponse {
    Insert,
    Get(Option<Vec<u8>>),
    Remove,
    ContainsKey(bool),
    #[allow(dead_code)]
    DbMap(StorageMap),
    DbMapRemove,
    DbMapContainsKey(bool),
    #[allow(dead_code)]
    DbList(StorageList),
    DbListRemove,
    DbListContainsKey(bool),
    CounterIncr,
    CounterDecr,
    CounterGet(Option<isize>),
    CounterSet,
    BatchInsert,
    BatchRemove,
    DbSize(usize),
    Info(serde_json::Value),
    #[cfg(feature = "len")]
    Len(usize),
    #[cfg(feature = "ttl")]
    ExpireAt(bool),
    #[cfg(feature = "ttl")]
    Expire(bool),
    #[cfg(feature = "ttl")]
    Ttl(Option<TimestampMillis>),
    // ── Map responses ──
    MapInsert,
    MapGet(Option<Vec<u8>>),
    MapRemove,
    MapContainsKey(bool),
    MapIsEmpty(bool),
    MapClear,
    MapRemoveAndFetch(Option<Vec<u8>>),
    MapRemoveWithPrefix,
    MapBatchInsert,
    MapBatchRemove,
    #[cfg(feature = "map_len")]
    MapLen(usize),
    #[cfg(feature = "ttl")]
    MapExpireAt(bool),
    #[cfg(feature = "ttl")]
    MapExpire(bool),
    #[cfg(feature = "ttl")]
    MapTtl(Option<TimestampMillis>),
    // ── List responses ──
    ListPush,
    ListPushs,
    ListPushLimit(Option<Vec<u8>>),
    ListPop(Option<Vec<u8>>),
    ListGetAll(Vec<Vec<u8>>),
    ListGetIndex(Option<Vec<u8>>),
    ListLen(usize),
    ListIsEmpty(bool),
    ListClear,
    #[cfg(feature = "ttl")]
    ListExpireAt(bool),
    #[cfg(feature = "ttl")]
    ListExpire(bool),
    #[cfg(feature = "ttl")]
    ListTtl(Option<TimestampMillis>),
}

impl fmt::Debug for CBStorageResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CBStorageResponse::Insert => write!(f, "Insert"),
            CBStorageResponse::Get(v) => f.debug_tuple("Get").field(v).finish(),
            CBStorageResponse::Remove => write!(f, "Remove"),
            CBStorageResponse::ContainsKey(b) => f.debug_tuple("ContainsKey").field(b).finish(),
            CBStorageResponse::DbMap(_) => write!(f, "Map(..)"),
            CBStorageResponse::DbMapRemove => write!(f, "MapRemove"),
            CBStorageResponse::DbMapContainsKey(b) => {
                f.debug_tuple("MapContainsKey").field(b).finish()
            }
            CBStorageResponse::DbList(_) => write!(f, "List(..)"),
            CBStorageResponse::DbListRemove => write!(f, "ListRemove"),
            CBStorageResponse::DbListContainsKey(b) => {
                f.debug_tuple("ListContainsKey").field(b).finish()
            }
            CBStorageResponse::CounterIncr => write!(f, "CounterIncr"),
            CBStorageResponse::CounterDecr => write!(f, "CounterDecr"),
            CBStorageResponse::CounterGet(v) => f.debug_tuple("CounterGet").field(v).finish(),
            CBStorageResponse::CounterSet => write!(f, "CounterSet"),
            CBStorageResponse::BatchInsert => write!(f, "BatchInsert"),
            CBStorageResponse::BatchRemove => write!(f, "BatchRemove"),
            CBStorageResponse::DbSize(s) => f.debug_tuple("DbSize").field(s).finish(),
            CBStorageResponse::Info(v) => f.debug_tuple("Info").field(v).finish(),
            #[cfg(feature = "len")]
            CBStorageResponse::Len(n) => f.debug_tuple("Len").field(n).finish(),
            #[cfg(feature = "ttl")]
            CBStorageResponse::ExpireAt(b) => f.debug_tuple("ExpireAt").field(b).finish(),
            #[cfg(feature = "ttl")]
            CBStorageResponse::Expire(b) => f.debug_tuple("Expire").field(b).finish(),
            #[cfg(feature = "ttl")]
            CBStorageResponse::Ttl(v) => f.debug_tuple("Ttl").field(v).finish(),
            // ── Map responses ──
            CBStorageResponse::MapInsert => write!(f, "MapInsert"),
            CBStorageResponse::MapGet(v) => f.debug_tuple("MapGet").field(v).finish(),
            CBStorageResponse::MapRemove => write!(f, "MapRemove"),
            CBStorageResponse::MapContainsKey(b) => {
                f.debug_tuple("MapContainsKey").field(b).finish()
            }
            CBStorageResponse::MapIsEmpty(b) => f.debug_tuple("MapIsEmpty").field(b).finish(),
            CBStorageResponse::MapClear => write!(f, "MapClear"),
            CBStorageResponse::MapRemoveAndFetch(v) => {
                f.debug_tuple("MapRemoveAndFetch").field(v).finish()
            }
            CBStorageResponse::MapRemoveWithPrefix => write!(f, "MapRemoveWithPrefix"),
            CBStorageResponse::MapBatchInsert => write!(f, "MapBatchInsert"),
            CBStorageResponse::MapBatchRemove => write!(f, "MapBatchRemove"),
            #[cfg(feature = "map_len")]
            CBStorageResponse::MapLen(n) => f.debug_tuple("MapLen").field(n).finish(),
            #[cfg(feature = "ttl")]
            CBStorageResponse::MapExpireAt(b) => f.debug_tuple("MapExpireAt").field(b).finish(),
            #[cfg(feature = "ttl")]
            CBStorageResponse::MapExpire(b) => f.debug_tuple("MapExpire").field(b).finish(),
            #[cfg(feature = "ttl")]
            CBStorageResponse::MapTtl(v) => f.debug_tuple("MapTtl").field(v).finish(),
            // ── List responses ──
            CBStorageResponse::ListPush => write!(f, "ListPush"),
            CBStorageResponse::ListPushs => write!(f, "ListPushs"),
            CBStorageResponse::ListPushLimit(v) => f.debug_tuple("ListPushLimit").field(v).finish(),
            CBStorageResponse::ListPop(v) => f.debug_tuple("ListPop").field(v).finish(),
            CBStorageResponse::ListGetAll(v) => {
                f.debug_tuple("ListGetAll").field(&v.len()).finish()
            }
            CBStorageResponse::ListGetIndex(v) => f.debug_tuple("ListGetIndex").field(v).finish(),
            CBStorageResponse::ListLen(n) => f.debug_tuple("ListLen").field(n).finish(),
            CBStorageResponse::ListIsEmpty(b) => f.debug_tuple("ListIsEmpty").field(b).finish(),
            CBStorageResponse::ListClear => write!(f, "ListClear"),
            #[cfg(feature = "ttl")]
            CBStorageResponse::ListExpireAt(b) => f.debug_tuple("ListExpireAt").field(b).finish(),
            #[cfg(feature = "ttl")]
            CBStorageResponse::ListExpire(b) => f.debug_tuple("ListExpire").field(b).finish(),
            #[cfg(feature = "ttl")]
            CBStorageResponse::ListTtl(v) => f.debug_tuple("ListTtl").field(v).finish(),
        }
    }
}

// ---------------------------------------------------------------------------
// DB-level Tower Service
// ---------------------------------------------------------------------------

/// A Tower [`Service`] that dispatches [`CBStorageRequest`] to the inner [`DefaultStorageDB`].
#[derive(Clone)]
pub struct CBStorageService {
    inner: DefaultStorageDB,
}

impl tower::Service<CBStorageRequest> for CBStorageService {
    type Response = CBStorageResponse;
    type Error = anyhow::Error;
    type Future =
        std::pin::Pin<Box<dyn std::future::Future<Output = Result<Self::Response>> + Send>>;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: CBStorageRequest) -> Self::Future {
        let this = self.inner.clone();
        Box::pin(async move {
            match req {
                CBStorageRequest::Insert { key, val } => this
                    .insert_raw(&key, &val)
                    .await
                    .map(|_| CBStorageResponse::Insert),
                CBStorageRequest::Get { key } => {
                    let val = this.get_raw(&key).await?;
                    Ok(CBStorageResponse::Get(val))
                }
                CBStorageRequest::Remove { key } => {
                    this.remove(&key).await.map(|_| CBStorageResponse::Remove)
                }
                CBStorageRequest::ContainsKey { key } => this
                    .contains_key(&key)
                    .await
                    .map(CBStorageResponse::ContainsKey),
                CBStorageRequest::DbMap { name, expire } => {
                    let m = this.map(&name, expire).await;
                    Ok(CBStorageResponse::DbMap(m))
                }
                CBStorageRequest::DbMapRemove { name } => this
                    .map_remove(&name)
                    .await
                    .map(|_| CBStorageResponse::DbMapRemove),
                CBStorageRequest::DbMapContainsKey { key } => this
                    .map_contains_key(&key)
                    .await
                    .map(CBStorageResponse::DbMapContainsKey),
                CBStorageRequest::DbList { name, expire } => {
                    let l = this.list(&name, expire).await;
                    Ok(CBStorageResponse::DbList(l))
                }
                CBStorageRequest::DbListRemove { name } => this
                    .list_remove(&name)
                    .await
                    .map(|_| CBStorageResponse::DbListRemove),
                CBStorageRequest::DbListContainsKey { key } => this
                    .list_contains_key(&key)
                    .await
                    .map(CBStorageResponse::DbListContainsKey),
                CBStorageRequest::CounterIncr { key, increment } => this
                    .counter_incr(&key, increment)
                    .await
                    .map(|_| CBStorageResponse::CounterIncr),
                CBStorageRequest::CounterDecr { key, decrement } => this
                    .counter_decr(&key, decrement)
                    .await
                    .map(|_| CBStorageResponse::CounterDecr),
                CBStorageRequest::CounterGet { key } => this
                    .counter_get(&key)
                    .await
                    .map(CBStorageResponse::CounterGet),
                CBStorageRequest::CounterSet { key, val } => this
                    .counter_set(&key, val)
                    .await
                    .map(|_| CBStorageResponse::CounterSet),
                CBStorageRequest::BatchInsert { key_vals } => this
                    .batch_insert_raw(key_vals)
                    .await
                    .map(|_| CBStorageResponse::BatchInsert),
                CBStorageRequest::BatchRemove { keys } => this
                    .batch_remove(keys)
                    .await
                    .map(|_| CBStorageResponse::BatchRemove),
                CBStorageRequest::DbSize => this.db_size().await.map(CBStorageResponse::DbSize),
                CBStorageRequest::Info => this.info().await.map(CBStorageResponse::Info),
                #[cfg(feature = "len")]
                CBStorageRequest::Len => this.len().await.map(CBStorageResponse::Len),
                #[cfg(feature = "ttl")]
                CBStorageRequest::ExpireAt { key, at } => this
                    .expire_at(&key, at)
                    .await
                    .map(CBStorageResponse::ExpireAt),
                #[cfg(feature = "ttl")]
                CBStorageRequest::Expire { key, dur } => {
                    this.expire(&key, dur).await.map(CBStorageResponse::Expire)
                }
                #[cfg(feature = "ttl")]
                CBStorageRequest::Ttl { key } => this.ttl(&key).await.map(CBStorageResponse::Ttl),
                // ── Map dispatch ──
                CBStorageRequest::MapInsert { map, key, val } => map
                    .insert_raw(&key, &val)
                    .await
                    .map(|_| CBStorageResponse::MapInsert),
                CBStorageRequest::MapGet { map, key } => {
                    map.get_raw(&key).await.map(CBStorageResponse::MapGet)
                }
                CBStorageRequest::MapRemove { map, key } => {
                    map.remove(&key).await.map(|_| CBStorageResponse::MapRemove)
                }
                CBStorageRequest::MapContainsKey { map, key } => map
                    .contains_key(&key)
                    .await
                    .map(CBStorageResponse::MapContainsKey),
                CBStorageRequest::MapIsEmpty { map } => {
                    map.is_empty().await.map(CBStorageResponse::MapIsEmpty)
                }
                CBStorageRequest::MapClear { map } => {
                    map.clear().await.map(|_| CBStorageResponse::MapClear)
                }
                CBStorageRequest::MapRemoveAndFetch { map, key } => map
                    .remove_and_fetch_raw(&key)
                    .await
                    .map(CBStorageResponse::MapRemoveAndFetch),
                CBStorageRequest::MapRemoveWithPrefix { map, prefix } => map
                    .remove_with_prefix(&prefix)
                    .await
                    .map(|_| CBStorageResponse::MapRemoveWithPrefix),
                CBStorageRequest::MapBatchInsert { map, key_vals } => map
                    .batch_insert_raw(key_vals)
                    .await
                    .map(|_| CBStorageResponse::MapBatchInsert),
                CBStorageRequest::MapBatchRemove { map, keys } => map
                    .batch_remove(keys)
                    .await
                    .map(|_| CBStorageResponse::MapBatchRemove),
                #[cfg(feature = "map_len")]
                CBStorageRequest::MapLen { map } => map.len().await.map(CBStorageResponse::MapLen),
                #[cfg(feature = "ttl")]
                CBStorageRequest::MapExpireAt { map, at } => {
                    map.expire_at(at).await.map(CBStorageResponse::MapExpireAt)
                }
                #[cfg(feature = "ttl")]
                CBStorageRequest::MapExpire { map, dur } => {
                    map.expire(dur).await.map(CBStorageResponse::MapExpire)
                }
                #[cfg(feature = "ttl")]
                CBStorageRequest::MapTtl { map } => map.ttl().await.map(CBStorageResponse::MapTtl),
                // ── List dispatch ──
                CBStorageRequest::ListPush { list, val } => list
                    .push_raw(&val)
                    .await
                    .map(|_| CBStorageResponse::ListPush),
                CBStorageRequest::ListPushs { list, vals } => list
                    .pushs_raw(vals)
                    .await
                    .map(|_| CBStorageResponse::ListPushs),
                CBStorageRequest::ListPushLimit {
                    list,
                    val,
                    limit,
                    pop_front_if_limited,
                } => list
                    .push_limit_raw(&val, limit, pop_front_if_limited)
                    .await
                    .map(CBStorageResponse::ListPushLimit),
                CBStorageRequest::ListPop { list } => {
                    list.pop_raw().await.map(CBStorageResponse::ListPop)
                }
                CBStorageRequest::ListGetAll { list } => {
                    list.all_raw().await.map(CBStorageResponse::ListGetAll)
                }
                CBStorageRequest::ListGetIndex { list, idx } => list
                    .get_index_raw(idx)
                    .await
                    .map(CBStorageResponse::ListGetIndex),
                CBStorageRequest::ListLen { list } => {
                    list.len().await.map(CBStorageResponse::ListLen)
                }
                CBStorageRequest::ListIsEmpty { list } => {
                    list.is_empty().await.map(CBStorageResponse::ListIsEmpty)
                }
                CBStorageRequest::ListClear { list } => {
                    list.clear().await.map(|_| CBStorageResponse::ListClear)
                }
                #[cfg(feature = "ttl")]
                CBStorageRequest::ListExpireAt { list, at } => list
                    .expire_at(at)
                    .await
                    .map(CBStorageResponse::ListExpireAt),
                #[cfg(feature = "ttl")]
                CBStorageRequest::ListExpire { list, dur } => {
                    list.expire(dur).await.map(CBStorageResponse::ListExpire)
                }
                #[cfg(feature = "ttl")]
                CBStorageRequest::ListTtl { list } => {
                    list.ttl().await.map(CBStorageResponse::ListTtl)
                }
            }
        })
    }
}

// ---------------------------------------------------------------------------
// CbTimeoutWrapper — inserts per-operation timeout into the Tower chain
// ---------------------------------------------------------------------------

/// Tower [`Service`] wrapper that applies an optional timeout to each call.
///
/// When the timeout fires, the inner service's future is dropped and an
/// `anyhow::Error` is returned. The enclosing `CircuitBreaker` then sees an
/// `Err` and the [`DefaultClassifier`] correctly counts it as a failure,
/// so timeouts are properly tracked in the sliding window.
///
/// When `timeout` is `None`, calls pass through unchanged (zero overhead).
#[derive(Clone)]
pub(crate) struct CbTimeoutWrapper<S> {
    inner: S,
    timeout: Option<Duration>,
}

impl<S, Req> tower::Service<Req> for CbTimeoutWrapper<S>
where
    S: tower::Service<Req> + Clone + Send + 'static,
    S::Response: Send + 'static,
    S::Error: Into<anyhow::Error> + Send + 'static,
    S::Future: Send + 'static,
    Req: Send + 'static,
{
    type Response = S::Response;
    type Error = anyhow::Error;
    type Future = std::pin::Pin<
        Box<
            dyn std::future::Future<Output = std::result::Result<Self::Response, Self::Error>>
                + Send,
        >,
    >;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(|e| e.into())
    }

    fn call(&mut self, req: Req) -> Self::Future {
        let fut = self.inner.call(req);
        let timeout = self.timeout;
        Box::pin(async move {
            match timeout {
                Some(dur) => match tokio::time::timeout(dur, fut).await {
                    Ok(r) => r.map_err(|e| e.into()),
                    Err(_) => Err(anyhow!("operation timed out after {:?}", dur)),
                },
                None => fut.await.map_err(|e| e.into()),
            }
        })
    }
}

// ---------------------------------------------------------------------------
// Helper: build Tower service chains with circuit breaker
// ---------------------------------------------------------------------------

/// Build the circuit breaker for DB-level service.
fn build_db_cb(
    inner: CBStorageService,
    config: &CircuitBreakerConfig,
    name: &str,
) -> tower_resilience_circuitbreaker::CircuitBreaker<
    CbTimeoutWrapper<CBStorageService>,
    DefaultClassifier,
> {
    let timeout_svc = CbTimeoutWrapper {
        inner,
        timeout: config.operation_timeout,
    };
    build_cb_inner(timeout_svc, config, name)
}

fn build_cb_inner<S, Req>(
    inner: S,
    config: &CircuitBreakerConfig,
    name: &str,
) -> tower_resilience_circuitbreaker::CircuitBreaker<S, DefaultClassifier>
where
    S: tower::Service<Req> + Clone + Send + 'static,
    S::Response: Send + 'static,
    S::Error: Into<anyhow::Error> + Send + 'static,
    S::Future: Send + 'static,
    Req: Send + 'static,
{
    let n = name.to_owned();
    let mut builder = CircuitBreakerLayer::builder()
        .name(name)
        .failure_rate_threshold(config.failure_rate_threshold)
        .minimum_number_of_calls(config.minimum_number_of_calls)
        .wait_duration_in_open(config.wait_duration_in_open)
        .slow_call_duration_threshold(config.slow_call_duration_threshold)
        .slow_call_rate_threshold(config.slow_call_rate_threshold);

    match &config.window {
        WindowConfig::CountBased(cfg) => {
            builder = builder
                .sliding_window_type(SlidingWindowType::CountBased)
                .sliding_window_size(cfg.sliding_window_size);
        }
        WindowConfig::TimeBased(cfg) => {
            builder = builder
                .sliding_window_type(SlidingWindowType::TimeBased)
                .sliding_window_duration(cfg.sliding_window_duration)
                .sliding_window_size(cfg.sliding_window_size);
        }
    }

    builder
        .on_state_transition(move |from, to| {
            log::info!(
                "[circuit-breaker:{}] state changed: {:?} -> {:?}",
                n,
                from,
                to
            );
        })
        .build()
        .layer(inner)
}

/// Shared circuit breaker type: an `Arc`-wrapped Tower `CircuitBreaker` wrapping
/// a `CbTimeoutWrapper<CBStorageService>` with the default failure classifier.
/// All three layers (DB / Map / List) use this same type.
type SharedBreaker = Arc<
    tower_resilience_circuitbreaker::CircuitBreaker<
        CbTimeoutWrapper<CBStorageService>,
        DefaultClassifier,
    >,
>;

// ---------------------------------------------------------------------------
// CircuitBrokenDB — public wrapper
// ---------------------------------------------------------------------------

/// A circuit-breaker-protected wrapper around [`DefaultStorageDB`].
///
/// Every operation goes through a Tower service chain that tracks success/failure.
/// When the failure rate exceeds the configured threshold, the circuit opens and
/// subsequent calls fail fast with an error rather than waiting for a timeout.
pub struct CircuitBrokenDB {
    /// The DB-level circuit breaker — cloned per-call for concurrency.
    service: SharedBreaker,
    config: CircuitBreakerConfig,
    /// Raw inner DB for pass-through operations that don't need CB protection
    /// (e.g., map_iter, list_iter, scan). Cloned cheaply from the DB passed to
    /// the Tower service chain during construction.
    inner: DefaultStorageDB,
}

impl CircuitBrokenDB {
    /// Wrap a `DefaultStorageDB` with a circuit breaker.
    pub fn new(db: DefaultStorageDB, config: CircuitBreakerConfig) -> Self {
        let name = config.name.clone();
        let inner = db.clone();
        let chain_svc = CBStorageService { inner: db };
        // CbTimeoutWrapper is inserted inside the CircuitBreaker by build_db_cb,
        // so timeout → Err → DefaultClassifier counts it as a failure.
        let service = Arc::new(build_db_cb(chain_svc, &config, &name));
        Self {
            service,
            config,
            inner,
        }
    }

    /// Return a reference to the circuit breaker configuration.
    pub fn config(&self) -> &CircuitBreakerConfig {
        &self.config
    }

    async fn raw_call(&self, req: CBStorageRequest) -> Result<CBStorageResponse> {
        // Clone once per call — shares circuit state (Arc), no lock contention.
        // The CircuitBreaker handles try_acquire (with Open→HalfOpen transition),
        // dispatch through CbTimeoutWrapper (with optional timeout), and automatic
        // result recording via DefaultClassifier — all in a single Tower call.
        let mut svc = self.service.as_ref().clone();

        match svc.call(req).await {
            Ok(r) => Ok(r),
            Err(CircuitBreakerError::OpenCircuit) => Err(anyhow!(
                "storage unavailable (circuit breaker open for '{}')",
                self.config.name
            )),
            Err(CircuitBreakerError::Inner(e)) => Err(e),
        }
    }

    /// Return a JSON snapshot of the circuit breaker's runtime state & config.
    ///
    /// Analogous to [`StorageDB::info()`] but for the breaker itself. Reads
    /// atomic-loaded fields (`state`, `is_open`) without acquiring the
    /// circuit's internal lock, and acquires the lock only for the richer
    /// [`metrics()`](tower_resilience_circuitbreaker::CircuitBreaker::metrics) snapshot.
    pub async fn cb_info(&self) -> serde_json::Value {
        let metrics = self.service.metrics().await;

        serde_json::json!({
            "name": self.config.name,
            "state": format!("{:?}", metrics.state),
            "state_code": metrics.state as u8,
            "is_open": self.service.is_open(),
            "metrics": {
                "total_calls": metrics.total_calls,
                "failure_count": metrics.failure_count,
                "success_count": metrics.success_count,
                "slow_call_count": metrics.slow_call_count,
                "failure_rate": metrics.failure_rate,
                "slow_call_rate": metrics.slow_call_rate,
                "time_since_state_change_ms": metrics.time_since_state_change.as_millis() as u64,
            },
            "config": {
                "failure_rate_threshold": self.config.failure_rate_threshold,
                "window": match &self.config.window {
                    WindowConfig::CountBased(cfg) => serde_json::json!({
                        "type": "CountBased",
                        "sliding_window_size": cfg.sliding_window_size,
                    }),
                    WindowConfig::TimeBased(cfg) => serde_json::json!({
                        "type": "TimeBased",
                        "sliding_window_duration_ms": cfg.sliding_window_duration.as_millis() as u64,
                        "sliding_window_size": cfg.sliding_window_size,
                    }),
                },
                "minimum_number_of_calls": self.config.minimum_number_of_calls,
                "wait_duration_in_open_ms": self.config.wait_duration_in_open.as_millis() as u64,
                "slow_call_duration_threshold_ms": self.config.slow_call_duration_threshold.as_millis() as u64,
                "slow_call_rate_threshold": self.config.slow_call_rate_threshold,
                "operation_timeout_ms": self.config.operation_timeout.map(|d| d.as_millis() as u64),
            },
        })
    }
}

impl Clone for CircuitBrokenDB {
    fn clone(&self) -> Self {
        Self {
            service: Arc::clone(&self.service),
            config: self.config.clone(),
            inner: self.inner.clone(),
        }
    }
}

// ---------------------------------------------------------------------------
// StorageDB trait implementation for CircuitBrokenDB
// ---------------------------------------------------------------------------

#[async_trait]
impl StorageDB for CircuitBrokenDB {
    type MapType = CircuitBrokenMap;
    type ListType = CircuitBrokenList;

    async fn map<N: AsRef<[u8]> + Sync + Send>(
        &self,
        name: N,
        expire: Option<TimestampMillis>,
    ) -> Self::MapType {
        let raw = self.inner.map(name, expire).await;
        CircuitBrokenMap::new(raw, Arc::clone(&self.service), self.config.name.clone())
    }

    async fn map_remove<K>(&self, name: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        self.raw_call(CBStorageRequest::DbMapRemove {
            name: name.as_ref().to_vec(),
        })
        .await
        .map(|_| ())
    }

    async fn map_contains_key<K: AsRef<[u8]> + Sync + Send>(&self, key: K) -> Result<bool> {
        match self
            .raw_call(CBStorageRequest::DbMapContainsKey {
                key: key.as_ref().to_vec(),
            })
            .await?
        {
            CBStorageResponse::DbMapContainsKey(b) => Ok(b),
            _ => Err(anyhow!("unexpected response type for map_contains_key()")),
        }
    }

    async fn list<V: AsRef<[u8]> + Sync + Send>(
        &self,
        name: V,
        expire: Option<TimestampMillis>,
    ) -> Self::ListType {
        let raw = self.inner.list(name, expire).await;
        CircuitBrokenList::new(raw, Arc::clone(&self.service), self.config.name.clone())
    }

    async fn list_remove<K>(&self, name: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        self.raw_call(CBStorageRequest::DbListRemove {
            name: name.as_ref().to_vec(),
        })
        .await
        .map(|_| ())
    }

    async fn list_contains_key<K: AsRef<[u8]> + Sync + Send>(&self, key: K) -> Result<bool> {
        match self
            .raw_call(CBStorageRequest::DbListContainsKey {
                key: key.as_ref().to_vec(),
            })
            .await?
        {
            CBStorageResponse::DbListContainsKey(b) => Ok(b),
            _ => Err(anyhow!("unexpected response type for list_contains_key()")),
        }
    }

    async fn insert<K, V>(&self, key: K, val: &V) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: Serialize + Sync + Send,
    {
        let bytes = postcard::to_stdvec(val)?;
        self.raw_call(CBStorageRequest::Insert {
            key: key.as_ref().to_vec(),
            val: bytes,
        })
        .await
        .map(|_| ())
    }

    async fn get<K, V>(&self, key: K) -> Result<Option<V>>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: DeserializeOwned + Sync + Send,
    {
        let req = CBStorageRequest::Get {
            key: key.as_ref().to_vec(),
        };
        match self.raw_call(req).await? {
            CBStorageResponse::Get(Some(bytes)) => Ok(Some(postcard::from_bytes(&bytes)?)),
            CBStorageResponse::Get(None) => Ok(None),
            _ => Err(anyhow!("unexpected response type for get()")),
        }
    }

    async fn remove<K>(&self, key: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        self.raw_call(CBStorageRequest::Remove {
            key: key.as_ref().to_vec(),
        })
        .await
        .map(|_| ())
    }

    async fn batch_insert<V>(&self, key_vals: Vec<(Key, V)>) -> Result<()>
    where
        V: Serialize + Sync + Send,
    {
        let mut pairs = Vec::with_capacity(key_vals.len());
        for (k, v) in key_vals {
            pairs.push((k, postcard::to_stdvec(&v)?));
        }
        self.raw_call(CBStorageRequest::BatchInsert { key_vals: pairs })
            .await
            .map(|_| ())
    }

    async fn batch_remove(&self, keys: Vec<Key>) -> Result<()> {
        self.raw_call(CBStorageRequest::BatchRemove { keys })
            .await
            .map(|_| ())
    }

    async fn counter_incr<K>(&self, key: K, increment: isize) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        self.raw_call(CBStorageRequest::CounterIncr {
            key: key.as_ref().to_vec(),
            increment,
        })
        .await
        .map(|_| ())
    }

    async fn counter_decr<K>(&self, key: K, decrement: isize) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        self.raw_call(CBStorageRequest::CounterDecr {
            key: key.as_ref().to_vec(),
            decrement,
        })
        .await
        .map(|_| ())
    }

    async fn counter_get<K>(&self, key: K) -> Result<Option<isize>>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        match self
            .raw_call(CBStorageRequest::CounterGet {
                key: key.as_ref().to_vec(),
            })
            .await?
        {
            CBStorageResponse::CounterGet(v) => Ok(v),
            _ => Err(anyhow!("unexpected response type for counter_get()")),
        }
    }

    async fn counter_set<K>(&self, key: K, val: isize) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        self.raw_call(CBStorageRequest::CounterSet {
            key: key.as_ref().to_vec(),
            val,
        })
        .await
        .map(|_| ())
    }

    async fn contains_key<K: AsRef<[u8]> + Sync + Send>(&self, key: K) -> Result<bool> {
        match self
            .raw_call(CBStorageRequest::ContainsKey {
                key: key.as_ref().to_vec(),
            })
            .await?
        {
            CBStorageResponse::ContainsKey(b) => Ok(b),
            _ => Err(anyhow!("unexpected response type for contains_key()")),
        }
    }

    #[cfg(feature = "len")]
    async fn len(&self) -> Result<usize> {
        match self.raw_call(CBStorageRequest::Len).await? {
            CBStorageResponse::Len(n) => Ok(n),
            _ => Err(anyhow!("unexpected response type for len()")),
        }
    }

    async fn db_size(&self) -> Result<usize> {
        match self.raw_call(CBStorageRequest::DbSize).await? {
            CBStorageResponse::DbSize(s) => Ok(s),
            _ => Err(anyhow!("unexpected response type for db_size()")),
        }
    }

    #[cfg(feature = "ttl")]
    async fn expire_at<K>(&self, key: K, at: TimestampMillis) -> Result<bool>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        match self
            .raw_call(CBStorageRequest::ExpireAt {
                key: key.as_ref().to_vec(),
                at,
            })
            .await?
        {
            CBStorageResponse::ExpireAt(b) => Ok(b),
            _ => Err(anyhow!("unexpected response type for expire_at()")),
        }
    }

    #[cfg(feature = "ttl")]
    async fn expire<K>(&self, key: K, dur: TimestampMillis) -> Result<bool>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        match self
            .raw_call(CBStorageRequest::Expire {
                key: key.as_ref().to_vec(),
                dur,
            })
            .await?
        {
            CBStorageResponse::Expire(b) => Ok(b),
            _ => Err(anyhow!("unexpected response type for expire()")),
        }
    }

    #[cfg(feature = "ttl")]
    async fn ttl<K>(&self, key: K) -> Result<Option<TimestampMillis>>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        match self
            .raw_call(CBStorageRequest::Ttl {
                key: key.as_ref().to_vec(),
            })
            .await?
        {
            CBStorageResponse::Ttl(v) => Ok(v),
            _ => Err(anyhow!("unexpected response type for ttl()")),
        }
    }

    async fn map_iter<'a>(
        &'a mut self,
    ) -> Result<Box<dyn AsyncIterator<Item = Result<StorageMap>> + Send + 'a>> {
        self.inner.map_iter().await
    }

    async fn list_iter<'a>(
        &'a mut self,
    ) -> Result<Box<dyn AsyncIterator<Item = Result<StorageList>> + Send + 'a>> {
        self.inner.list_iter().await
    }

    async fn scan<'a, P>(
        &'a mut self,
        pattern: P,
    ) -> Result<Box<dyn AsyncIterator<Item = Result<Key>> + Send + 'a>>
    where
        P: AsRef<[u8]> + Send + Sync,
    {
        self.inner.scan(pattern).await
    }

    async fn info(&self) -> Result<serde_json::Value> {
        let mut base = match self.raw_call(CBStorageRequest::Info).await {
            Ok(CBStorageResponse::Info(v)) => v,
            Err(e) => serde_json::Value::String(e.to_string()),
            _ => return Err(anyhow!("unexpected response type for info()")),
        };

        if let Some(obj) = base.as_object_mut() {
            obj.insert("circuit_breaker".into(), self.cb_info().await);
        } else {
            let cb_state = self.cb_info().await;
            base = serde_json::json!({
                "storage": base,
                "circuit_breaker": cb_state,
            });
        }

        Ok(base)
    }
}

// ---------------------------------------------------------------------------
// CircuitBrokenMap — public wrapper
// ---------------------------------------------------------------------------

/// A circuit-breaker-protected wrapper around [`StorageMap`].
///
/// All operations funnel through the **shared DB-level** `CircuitBreaker`,
/// so Map/List/DB share a single failure metric.  Clone per-call (cheap).
///
/// Iterator methods (`iter`, `key_iter`, `prefix_iter`) bypass the breaker
/// and operate directly on the inner `StorageMap`.
pub struct CircuitBrokenMap {
    /// Reference to the shared DB-level circuit breaker.
    breaker: SharedBreaker,
    /// Raw inner map for name() and iterator methods that bypass the CB.
    inner: StorageMap,
    /// Breaker name for error messages.
    name: String,
}

impl Clone for CircuitBrokenMap {
    fn clone(&self) -> Self {
        Self {
            breaker: Arc::clone(&self.breaker),
            inner: self.inner.clone(),
            name: self.name.clone(),
        }
    }
}

impl CircuitBrokenMap {
    fn new(m: StorageMap, breaker: SharedBreaker, name: String) -> Self {
        Self {
            breaker,
            inner: m,
            name,
        }
    }
}

// ---------------------------------------------------------------------------

#[async_trait]
impl Map for CircuitBrokenMap {
    fn name(&self) -> &[u8] {
        self.inner.name()
    }

    async fn insert<K, V>(&self, key: K, val: &V) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: Serialize + Sync + Send + ?Sized,
    {
        let bytes = postcard::to_stdvec(val)?;
        let mut svc = self.breaker.as_ref().clone();
        svc.call(CBStorageRequest::MapInsert {
            map: self.inner.clone(),
            key: key.as_ref().to_vec(),
            val: bytes,
        })
        .await
        .map_err(|_| {
            anyhow!(
                "storage unavailable (circuit breaker open for '{}')",
                self.name
            )
        })?;
        Ok(())
    }

    async fn get<K, V>(&self, key: K) -> Result<Option<V>>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: DeserializeOwned + Sync + Send,
    {
        let mut svc = self.breaker.as_ref().clone();
        let resp = svc
            .call(CBStorageRequest::MapGet {
                map: self.inner.clone(),
                key: key.as_ref().to_vec(),
            })
            .await
            .map_err(|_| {
                anyhow!(
                    "storage unavailable (circuit breaker open for '{}')",
                    self.name
                )
            })?;
        match resp {
            CBStorageResponse::MapGet(Some(bytes)) => Ok(Some(postcard::from_bytes(&bytes)?)),
            CBStorageResponse::MapGet(None) => Ok(None),
            _ => Err(anyhow!("unexpected response type for map::get()")),
        }
    }

    async fn remove<K>(&self, key: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let mut svc = self.breaker.as_ref().clone();
        svc.call(CBStorageRequest::MapRemove {
            map: self.inner.clone(),
            key: key.as_ref().to_vec(),
        })
        .await
        .map_err(|_| {
            anyhow!(
                "storage unavailable (circuit breaker open for '{}')",
                self.name
            )
        })?;
        Ok(())
    }

    async fn contains_key<K: AsRef<[u8]> + Sync + Send>(&self, key: K) -> Result<bool> {
        let mut svc = self.breaker.as_ref().clone();
        let resp = svc
            .call(CBStorageRequest::MapContainsKey {
                map: self.inner.clone(),
                key: key.as_ref().to_vec(),
            })
            .await
            .map_err(|_| {
                anyhow!(
                    "storage unavailable (circuit breaker open for '{}')",
                    self.name
                )
            })?;
        match resp {
            CBStorageResponse::MapContainsKey(b) => Ok(b),
            _ => Err(anyhow!("unexpected response type for map::contains_key()")),
        }
    }

    #[cfg(feature = "map_len")]
    async fn len(&self) -> Result<usize> {
        let mut svc = self.breaker.as_ref().clone();
        let resp = svc
            .call(CBStorageRequest::MapLen {
                map: self.inner.clone(),
            })
            .await
            .map_err(|_| {
                anyhow!(
                    "storage unavailable (circuit breaker open for '{}')",
                    self.name
                )
            })?;
        match resp {
            CBStorageResponse::MapLen(n) => Ok(n),
            _ => Err(anyhow!("unexpected response type for map::len()")),
        }
    }

    async fn is_empty(&self) -> Result<bool> {
        let mut svc = self.breaker.as_ref().clone();
        let resp = svc
            .call(CBStorageRequest::MapIsEmpty {
                map: self.inner.clone(),
            })
            .await
            .map_err(|_| {
                anyhow!(
                    "storage unavailable (circuit breaker open for '{}')",
                    self.name
                )
            })?;
        match resp {
            CBStorageResponse::MapIsEmpty(b) => Ok(b),
            _ => Err(anyhow!("unexpected response type for map::is_empty()")),
        }
    }

    async fn clear(&self) -> Result<()> {
        let mut svc = self.breaker.as_ref().clone();
        svc.call(CBStorageRequest::MapClear {
            map: self.inner.clone(),
        })
        .await
        .map_err(|_| {
            anyhow!(
                "storage unavailable (circuit breaker open for '{}')",
                self.name
            )
        })?;
        Ok(())
    }

    async fn remove_and_fetch<K, V>(&self, key: K) -> Result<Option<V>>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: DeserializeOwned + Sync + Send,
    {
        let mut svc = self.breaker.as_ref().clone();
        let resp = svc
            .call(CBStorageRequest::MapRemoveAndFetch {
                map: self.inner.clone(),
                key: key.as_ref().to_vec(),
            })
            .await
            .map_err(|_| {
                anyhow!(
                    "storage unavailable (circuit breaker open for '{}')",
                    self.name
                )
            })?;
        match resp {
            CBStorageResponse::MapRemoveAndFetch(Some(bytes)) => {
                Ok(Some(postcard::from_bytes(&bytes)?))
            }
            CBStorageResponse::MapRemoveAndFetch(None) => Ok(None),
            _ => Err(anyhow!(
                "unexpected response type for map::remove_and_fetch()"
            )),
        }
    }

    async fn remove_with_prefix<K>(&self, prefix: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let mut svc = self.breaker.as_ref().clone();
        svc.call(CBStorageRequest::MapRemoveWithPrefix {
            map: self.inner.clone(),
            prefix: prefix.as_ref().to_vec(),
        })
        .await
        .map_err(|_| {
            anyhow!(
                "storage unavailable (circuit breaker open for '{}')",
                self.name
            )
        })?;
        Ok(())
    }

    async fn batch_insert<V>(&self, key_vals: Vec<(Key, V)>) -> Result<()>
    where
        V: Serialize + Sync + Send,
    {
        let mut pairs = Vec::with_capacity(key_vals.len());
        for (k, v) in key_vals {
            pairs.push((k, postcard::to_stdvec(&v)?));
        }
        let mut svc = self.breaker.as_ref().clone();
        svc.call(CBStorageRequest::MapBatchInsert {
            map: self.inner.clone(),
            key_vals: pairs,
        })
        .await
        .map_err(|_| {
            anyhow!(
                "storage unavailable (circuit breaker open for '{}')",
                self.name
            )
        })?;
        Ok(())
    }

    async fn batch_remove(&self, keys: Vec<Key>) -> Result<()> {
        let mut svc = self.breaker.as_ref().clone();
        svc.call(CBStorageRequest::MapBatchRemove {
            map: self.inner.clone(),
            keys,
        })
        .await
        .map_err(|_| {
            anyhow!(
                "storage unavailable (circuit breaker open for '{}')",
                self.name
            )
        })?;
        Ok(())
    }

    // ---- pass-through: iterator methods bypass CB ----
    async fn iter<'a, V>(
        &'a mut self,
    ) -> Result<Box<dyn AsyncIterator<Item = IterItem<V>> + Send + 'a>>
    where
        V: DeserializeOwned + Sync + Send + 'a + 'static,
    {
        self.inner.iter().await
    }

    async fn key_iter<'a>(
        &'a mut self,
    ) -> Result<Box<dyn AsyncIterator<Item = Result<Key>> + Send + 'a>> {
        self.inner.key_iter().await
    }

    async fn prefix_iter<'a, P, V>(
        &'a mut self,
        prefix: P,
    ) -> Result<Box<dyn AsyncIterator<Item = IterItem<V>> + Send + 'a>>
    where
        P: AsRef<[u8]> + Send + Sync,
        V: DeserializeOwned + Sync + Send + 'a + 'static,
    {
        self.inner.prefix_iter(prefix).await
    }

    #[cfg(feature = "ttl")]
    async fn expire_at(&self, at: TimestampMillis) -> Result<bool> {
        let mut svc = self.breaker.as_ref().clone();
        let resp = svc
            .call(CBStorageRequest::MapExpireAt {
                map: self.inner.clone(),
                at,
            })
            .await
            .map_err(|_| {
                anyhow!(
                    "storage unavailable (circuit breaker open for '{}')",
                    self.name
                )
            })?;
        match resp {
            CBStorageResponse::MapExpireAt(b) => Ok(b),
            _ => Err(anyhow!("unexpected response type for map::expire_at()")),
        }
    }

    #[cfg(feature = "ttl")]
    async fn expire(&self, dur: TimestampMillis) -> Result<bool> {
        let mut svc = self.breaker.as_ref().clone();
        let resp = svc
            .call(CBStorageRequest::MapExpire {
                map: self.inner.clone(),
                dur,
            })
            .await
            .map_err(|_| {
                anyhow!(
                    "storage unavailable (circuit breaker open for '{}')",
                    self.name
                )
            })?;
        match resp {
            CBStorageResponse::MapExpire(b) => Ok(b),
            _ => Err(anyhow!("unexpected response type for map::expire()")),
        }
    }

    #[cfg(feature = "ttl")]
    async fn ttl(&self) -> Result<Option<TimestampMillis>> {
        let mut svc = self.breaker.as_ref().clone();
        let resp = svc
            .call(CBStorageRequest::MapTtl {
                map: self.inner.clone(),
            })
            .await
            .map_err(|_| {
                anyhow!(
                    "storage unavailable (circuit breaker open for '{}')",
                    self.name
                )
            })?;
        match resp {
            CBStorageResponse::MapTtl(v) => Ok(v),
            _ => Err(anyhow!("unexpected response type for map::ttl()")),
        }
    }
}

// ---------------------------------------------------------------------------
// CircuitBrokenList — public wrapper
// ---------------------------------------------------------------------------

/// A circuit-breaker-protected wrapper around [`StorageList`].
///
/// All operations funnel through the **shared DB-level** `CircuitBreaker`,
/// so Map/List/DB share a single failure metric.  Clone per-call (cheap).
///
/// Iterator methods (`iter`) bypass the breaker and operate directly on
/// the inner `StorageList`.
pub struct CircuitBrokenList {
    /// Reference to the shared DB-level circuit breaker.
    breaker: SharedBreaker,
    /// Raw inner list for name() and iter() that bypass the CB.
    inner: StorageList,
    /// Breaker name for error messages & identifying the list in requests.
    name: String,
}

impl Clone for CircuitBrokenList {
    fn clone(&self) -> Self {
        Self {
            breaker: Arc::clone(&self.breaker),
            inner: self.inner.clone(),
            name: self.name.clone(),
        }
    }
}

impl CircuitBrokenList {
    fn new(l: StorageList, breaker: SharedBreaker, name: String) -> Self {
        Self {
            breaker,
            inner: l,
            name,
        }
    }
}

// ---------------------------------------------------------------------------

#[async_trait]
impl List for CircuitBrokenList {
    fn name(&self) -> &[u8] {
        self.inner.name()
    }

    async fn push<V>(&self, val: &V) -> Result<()>
    where
        V: Serialize + Sync + Send,
    {
        let bytes = postcard::to_stdvec(val)?;
        let mut svc = self.breaker.as_ref().clone();
        svc.call(CBStorageRequest::ListPush {
            list: self.inner.clone(),
            val: bytes,
        })
        .await
        .map_err(|_| {
            anyhow!(
                "storage unavailable (circuit breaker open for '{}')",
                self.name
            )
        })?;
        Ok(())
    }

    async fn pushs<V>(&self, vals: Vec<V>) -> Result<()>
    where
        V: Serialize + Sync + Send,
    {
        let mut bytes_vec = Vec::with_capacity(vals.len());
        for v in vals {
            bytes_vec.push(postcard::to_stdvec(&v)?);
        }
        let mut svc = self.breaker.as_ref().clone();
        svc.call(CBStorageRequest::ListPushs {
            list: self.inner.clone(),
            vals: bytes_vec,
        })
        .await
        .map_err(|_| {
            anyhow!(
                "storage unavailable (circuit breaker open for '{}')",
                self.name
            )
        })?;
        Ok(())
    }

    async fn push_limit<V>(
        &self,
        val: &V,
        limit: usize,
        pop_front_if_limited: bool,
    ) -> Result<Option<V>>
    where
        V: Serialize + Sync + Send + DeserializeOwned,
    {
        let bytes = postcard::to_stdvec(val)?;
        let mut svc = self.breaker.as_ref().clone();
        let resp = svc
            .call(CBStorageRequest::ListPushLimit {
                list: self.inner.clone(),
                val: bytes,
                limit,
                pop_front_if_limited,
            })
            .await
            .map_err(|_| {
                anyhow!(
                    "storage unavailable (circuit breaker open for '{}')",
                    self.name
                )
            })?;
        match resp {
            CBStorageResponse::ListPushLimit(Some(popped)) => {
                Ok(Some(postcard::from_bytes(&popped)?))
            }
            CBStorageResponse::ListPushLimit(None) => Ok(None),
            _ => Err(anyhow!("unexpected response type for list::push_limit()")),
        }
    }

    async fn pop<V>(&self) -> Result<Option<V>>
    where
        V: DeserializeOwned + Sync + Send,
    {
        let mut svc = self.breaker.as_ref().clone();
        let resp = svc
            .call(CBStorageRequest::ListPop {
                list: self.inner.clone(),
            })
            .await
            .map_err(|_| {
                anyhow!(
                    "storage unavailable (circuit breaker open for '{}')",
                    self.name
                )
            })?;
        match resp {
            CBStorageResponse::ListPop(Some(bytes)) => Ok(Some(postcard::from_bytes(&bytes)?)),
            CBStorageResponse::ListPop(None) => Ok(None),
            _ => Err(anyhow!("unexpected response type for list::pop()")),
        }
    }

    async fn all<V>(&self) -> Result<Vec<V>>
    where
        V: DeserializeOwned + Sync + Send,
    {
        let mut svc = self.breaker.as_ref().clone();
        let resp = svc
            .call(CBStorageRequest::ListGetAll {
                list: self.inner.clone(),
            })
            .await
            .map_err(|_| {
                anyhow!(
                    "storage unavailable (circuit breaker open for '{}')",
                    self.name
                )
            })?;
        match resp {
            CBStorageResponse::ListGetAll(vals) => vals
                .into_iter()
                .map(|bytes| postcard::from_bytes(&bytes).map_err(Into::into))
                .collect(),
            _ => Err(anyhow!("unexpected response type for list::all()")),
        }
    }

    async fn get_index<V>(&self, idx: usize) -> Result<Option<V>>
    where
        V: DeserializeOwned + Sync + Send,
    {
        let mut svc = self.breaker.as_ref().clone();
        let resp = svc
            .call(CBStorageRequest::ListGetIndex {
                list: self.inner.clone(),
                idx,
            })
            .await
            .map_err(|_| {
                anyhow!(
                    "storage unavailable (circuit breaker open for '{}')",
                    self.name
                )
            })?;
        match resp {
            CBStorageResponse::ListGetIndex(Some(bytes)) => Ok(Some(postcard::from_bytes(&bytes)?)),
            CBStorageResponse::ListGetIndex(None) => Ok(None),
            _ => Err(anyhow!("unexpected response type for list::get_index()")),
        }
    }

    async fn len(&self) -> Result<usize> {
        let mut svc = self.breaker.as_ref().clone();
        let resp = svc
            .call(CBStorageRequest::ListLen {
                list: self.inner.clone(),
            })
            .await
            .map_err(|_| {
                anyhow!(
                    "storage unavailable (circuit breaker open for '{}')",
                    self.name
                )
            })?;
        match resp {
            CBStorageResponse::ListLen(n) => Ok(n),
            _ => Err(anyhow!("unexpected response type for list::len()")),
        }
    }

    async fn is_empty(&self) -> Result<bool> {
        let mut svc = self.breaker.as_ref().clone();
        let resp = svc
            .call(CBStorageRequest::ListIsEmpty {
                list: self.inner.clone(),
            })
            .await
            .map_err(|_| {
                anyhow!(
                    "storage unavailable (circuit breaker open for '{}')",
                    self.name
                )
            })?;
        match resp {
            CBStorageResponse::ListIsEmpty(b) => Ok(b),
            _ => Err(anyhow!("unexpected response type for list::is_empty()")),
        }
    }

    async fn clear(&self) -> Result<()> {
        let mut svc = self.breaker.as_ref().clone();
        svc.call(CBStorageRequest::ListClear {
            list: self.inner.clone(),
        })
        .await
        .map_err(|_| {
            anyhow!(
                "storage unavailable (circuit breaker open for '{}')",
                self.name
            )
        })?;
        Ok(())
    }

    // ---- pass-through: iterator bypasses CB ----
    async fn iter<'a, V>(
        &'a mut self,
    ) -> Result<Box<dyn AsyncIterator<Item = Result<V>> + Send + 'a>>
    where
        V: DeserializeOwned + Sync + Send + 'a + 'static,
    {
        self.inner.iter().await
    }

    #[cfg(feature = "ttl")]
    async fn expire_at(&self, at: TimestampMillis) -> Result<bool> {
        let mut svc = self.breaker.as_ref().clone();
        let resp = svc
            .call(CBStorageRequest::ListExpireAt {
                list: self.inner.clone(),
                at,
            })
            .await
            .map_err(|_| {
                anyhow!(
                    "storage unavailable (circuit breaker open for '{}')",
                    self.name
                )
            })?;
        match resp {
            CBStorageResponse::ListExpireAt(b) => Ok(b),
            _ => Err(anyhow!("unexpected response type for list::expire_at()")),
        }
    }

    #[cfg(feature = "ttl")]
    async fn expire(&self, dur: TimestampMillis) -> Result<bool> {
        let mut svc = self.breaker.as_ref().clone();
        let resp = svc
            .call(CBStorageRequest::ListExpire {
                list: self.inner.clone(),
                dur,
            })
            .await
            .map_err(|_| {
                anyhow!(
                    "storage unavailable (circuit breaker open for '{}')",
                    self.name
                )
            })?;
        match resp {
            CBStorageResponse::ListExpire(b) => Ok(b),
            _ => Err(anyhow!("unexpected response type for list::expire()")),
        }
    }

    #[cfg(feature = "ttl")]
    async fn ttl(&self) -> Result<Option<TimestampMillis>> {
        let mut svc = self.breaker.as_ref().clone();
        let resp = svc
            .call(CBStorageRequest::ListTtl {
                list: self.inner.clone(),
            })
            .await
            .map_err(|_| {
                anyhow!(
                    "storage unavailable (circuit breaker open for '{}')",
                    self.name
                )
            })?;
        match resp {
            CBStorageResponse::ListTtl(v) => Ok(v),
            _ => Err(anyhow!("unexpected response type for list::ttl()")),
        }
    }
}

// =========================================================================
// Tests
// =========================================================================

#[cfg(test)]
mod cb_unit_tests {
    use std::pin::Pin;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::task::{Context, Poll};
    use std::time::Duration;

    use anyhow::anyhow;
    use tower::Service;

    use super::*;
    use crate::Result;

    // -----------------------------------------------------------------------
    // Mock service: fails first `fail_until` calls, then succeeds
    // -----------------------------------------------------------------------

    #[derive(Clone)]
    struct MockSvc {
        /// First `fail_until` calls return Err; calls at or after return Ok.
        fail_until: u64,
        /// Total calls made (shared across clones).
        call_count: Arc<AtomicU64>,
    }

    impl MockSvc {
        fn always_ok() -> Self {
            Self {
                fail_until: 0,
                call_count: Arc::new(AtomicU64::new(0)),
            }
        }
        fn fail_for(n: u64) -> Self {
            Self {
                fail_until: n,
                call_count: Arc::new(AtomicU64::new(0)),
            }
        }
    }

    impl Service<CBStorageRequest> for MockSvc {
        type Response = CBStorageResponse;
        type Error = anyhow::Error;
        type Future = Pin<Box<dyn std::future::Future<Output = Result<Self::Response>> + Send>>;

        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: CBStorageRequest) -> Self::Future {
            let count = self.call_count.fetch_add(1, Ordering::SeqCst);
            let fail_until = self.fail_until;
            Box::pin(async move {
                if count < fail_until {
                    Err(anyhow!("mock failure (call {})", count))
                } else {
                    Ok(CBStorageResponse::Insert)
                }
            })
        }
    }

    // -----------------------------------------------------------------------
    // Map-level mock (uses CBStorageRequest for uniform testing)
    // -----------------------------------------------------------------------

    #[derive(Clone)]
    struct MockMapSvc {
        fail_until: u64,
        call_count: Arc<AtomicU64>,
    }

    impl MockMapSvc {
        fn fail_for(n: u64) -> Self {
            Self {
                fail_until: n,
                call_count: Arc::new(AtomicU64::new(0)),
            }
        }
    }

    impl Service<CBStorageRequest> for MockMapSvc {
        type Response = CBStorageResponse;
        type Error = anyhow::Error;
        type Future = Pin<Box<dyn std::future::Future<Output = Result<Self::Response>> + Send>>;

        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: CBStorageRequest) -> Self::Future {
            let count = self.call_count.fetch_add(1, Ordering::SeqCst);
            let fail_until = self.fail_until;
            Box::pin(async move {
                if count < fail_until {
                    Err(anyhow!("mock map failure"))
                } else {
                    Ok(CBStorageResponse::MapIsEmpty(true))
                }
            })
        }
    }

    // -----------------------------------------------------------------------
    // List-level mock
    // -----------------------------------------------------------------------

    #[derive(Clone)]
    struct MockListSvc {
        fail_until: u64,
        call_count: Arc<AtomicU64>,
    }

    impl MockListSvc {
        fn fail_for(n: u64) -> Self {
            Self {
                fail_until: n,
                call_count: Arc::new(AtomicU64::new(0)),
            }
        }
    }

    impl Service<CBStorageRequest> for MockListSvc {
        type Response = CBStorageResponse;
        type Error = anyhow::Error;
        type Future = Pin<Box<dyn std::future::Future<Output = Result<Self::Response>> + Send>>;

        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: CBStorageRequest) -> Self::Future {
            let count = self.call_count.fetch_add(1, Ordering::SeqCst);
            let fail_until = self.fail_until;
            Box::pin(async move {
                if count < fail_until {
                    Err(anyhow!("mock list failure"))
                } else {
                    Ok(CBStorageResponse::ListLen(0))
                }
            })
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /// Aggressive config: trips after 3 calls with ≥50% failure.
    fn fast_cb_config(name: &str) -> CircuitBreakerConfig {
        CircuitBreakerConfig {
            name: name.into(),
            failure_rate_threshold: 0.5,
            window: CountBasedWindowConfig {
                sliding_window_size: 3,
            }
            .into(),
            minimum_number_of_calls: 3,
            wait_duration_in_open: Duration::from_millis(200),
            slow_call_duration_threshold: Duration::from_secs(1),
            slow_call_rate_threshold: 1.0,
            operation_timeout: None,
        }
    }

    // -----------------------------------------------------------------------
    // Tests — DB level
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_ok_calls_pass_through() {
        let mock = MockSvc::always_ok();
        let cfg = fast_cb_config("pass_through");
        let mut cb = build_cb_inner(mock, &cfg, &cfg.name);

        for i in 0..10 {
            let req = CBStorageRequest::Get {
                key: format!("k{}", i).into_bytes(),
            };
            let resp = cb.ready().await.unwrap().call(req).await;
            assert!(resp.is_ok(), "call {} should succeed", i);
        }
    }

    #[tokio::test]
    async fn test_cb_propagates_inner_errors() {
        let mock = MockSvc::fail_for(5);
        let cfg = fast_cb_config("propagate_errs");
        let mut cb = build_cb_inner(mock, &cfg, &cfg.name);

        let req = CBStorageRequest::Get { key: b"x".to_vec() };
        let resp = cb.ready().await.unwrap().call(req).await;
        assert!(resp.is_err(), "inner error should propagate");
        let err = resp.unwrap_err().to_string();
        assert!(err.contains("mock failure"), "unexpected error: {}", err);
    }

    // -----------------------------------------------------------------------
    // Tests — Map level
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_cb_map_propagates_errors() {
        let mock = MockMapSvc::fail_for(5);
        let cfg = fast_cb_config("map_errs");
        let mut cb = build_cb_inner(mock, &cfg, &cfg.name);

        let resp = cb
            .ready()
            .await
            .unwrap()
            .call(CBStorageRequest::DbSize)
            .await;
        assert!(resp.is_err(), "map error should propagate");
    }

    // -----------------------------------------------------------------------
    // Tests — List level
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_cb_list_propagates_errors() {
        let mock = MockListSvc::fail_for(5);
        let cfg = fast_cb_config("list_errs");
        let mut cb = build_cb_inner(mock, &cfg, &cfg.name);

        let resp = cb
            .ready()
            .await
            .unwrap()
            .call(CBStorageRequest::DbSize)
            .await;
        assert!(resp.is_err(), "list error should propagate");
    }
}
