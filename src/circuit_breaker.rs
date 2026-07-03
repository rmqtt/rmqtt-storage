//! Circuit breaker wrappers for storage backends using `tower_resilience_circuitbreaker`.
//!
//! Provides [`CircuitBrokenDB`], [`CircuitBrokenMap`], and [`CircuitBrokenList`] which wrap the
//! corresponding enums with a per-instance circuit breaker. All operations funnel through
//! a Tower service chain so that `tower_resilience_circuitbreaker` can track success/failure
//! and automatically open the circuit when the failure rate exceeds the configured threshold.
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
use tokio::sync::Mutex;
use tower::{Layer, Service, ServiceExt};
use tower_resilience_circuitbreaker::{
    CircuitBreakerError, CircuitBreakerLayer, DefaultClassifier, SlidingWindowType,
};

use crate::storage::{
    AsyncIterator, DefaultStorageDB, IterItem, Key, List, Map, StorageDB, StorageList, StorageMap,
};
use crate::{Result, TimestampMillis};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Circuit breaker configuration.
///
/// Defaults match the Redis-network workload typical of this crate.
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// Failure rate threshold (0.0 – 1.0). Default: 0.5
    pub failure_rate_threshold: f64,
    /// Sliding window type. Default: CountBased
    pub sliding_window_type: SlidingWindowType,
    /// Sliding window size. Default: 20
    pub sliding_window_size: usize,
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

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_rate_threshold: 0.5,
            sliding_window_type: SlidingWindowType::CountBased,
            sliding_window_size: 20,
            minimum_number_of_calls: 10,
            wait_duration_in_open: Duration::from_secs(30),
            slow_call_duration_threshold: Duration::from_secs(2),
            slow_call_rate_threshold: 1.0,
            operation_timeout: None,
            name: "storage".into(),
        }
    }
}

/// Convert a `CircuitBreakerError<anyhow::Error>` into an `anyhow::Error`, extracting
/// the inner error for `Inner` and producing a descriptive message for `OpenCircuit`.
fn map_cb_err(err: CircuitBreakerError<anyhow::Error>, name: &str) -> anyhow::Error {
    match err {
        CircuitBreakerError::OpenCircuit => {
            anyhow!("storage unavailable (circuit breaker open for '{}')", name)
        }
        CircuitBreakerError::Inner(inner) => inner,
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
    Map {
        name: Vec<u8>,
        expire: Option<TimestampMillis>,
    },
    MapRemove {
        name: Vec<u8>,
    },
    MapContainsKey {
        key: Vec<u8>,
    },
    List {
        name: Vec<u8>,
        expire: Option<TimestampMillis>,
    },
    ListRemove {
        name: Vec<u8>,
    },
    ListContainsKey {
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
}

/// Unified response enum for all `DefaultStorageDB` operations.
// Manual Debug impl because StorageMap / StorageList don't implement Debug.
pub enum CBStorageResponse {
    Insert,
    Get(Option<Vec<u8>>),
    Remove,
    ContainsKey(bool),
    Map(StorageMap),
    MapRemove,
    MapContainsKey(bool),
    List(StorageList),
    ListRemove,
    ListContainsKey(bool),
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
}

impl fmt::Debug for CBStorageResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CBStorageResponse::Insert => write!(f, "Insert"),
            CBStorageResponse::Get(v) => f.debug_tuple("Get").field(v).finish(),
            CBStorageResponse::Remove => write!(f, "Remove"),
            CBStorageResponse::ContainsKey(b) => f.debug_tuple("ContainsKey").field(b).finish(),
            CBStorageResponse::Map(_) => write!(f, "Map(..)"),
            CBStorageResponse::MapRemove => write!(f, "MapRemove"),
            CBStorageResponse::MapContainsKey(b) => {
                f.debug_tuple("MapContainsKey").field(b).finish()
            }
            CBStorageResponse::List(_) => write!(f, "List(..)"),
            CBStorageResponse::ListRemove => write!(f, "ListRemove"),
            CBStorageResponse::ListContainsKey(b) => {
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
                CBStorageRequest::Map { name, expire } => {
                    let m = this.map(&name, expire).await?;
                    Ok(CBStorageResponse::Map(m))
                }
                CBStorageRequest::MapRemove { name } => this
                    .map_remove(&name)
                    .await
                    .map(|_| CBStorageResponse::MapRemove),
                CBStorageRequest::MapContainsKey { key } => this
                    .map_contains_key(&key)
                    .await
                    .map(CBStorageResponse::MapContainsKey),
                CBStorageRequest::List { name, expire } => {
                    let l = this.list(&name, expire).await?;
                    Ok(CBStorageResponse::List(l))
                }
                CBStorageRequest::ListRemove { name } => this
                    .list_remove(&name)
                    .await
                    .map(|_| CBStorageResponse::ListRemove),
                CBStorageRequest::ListContainsKey { key } => this
                    .list_contains_key(&key)
                    .await
                    .map(CBStorageResponse::ListContainsKey),
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
            }
        })
    }
}

// ---------------------------------------------------------------------------
// Map-level Request / Response & Service
// ---------------------------------------------------------------------------

/// Unified request enum for all [`Map`](crate::storage::Map) operations.
#[derive(Debug, Clone)]
pub enum CBMapRequest {
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
    IsEmpty,
    Clear,
    RemoveAndFetch {
        key: Vec<u8>,
    },
    RemoveWithPrefix {
        prefix: Vec<u8>,
    },
    BatchInsert {
        key_vals: Vec<(Vec<u8>, Vec<u8>)>,
    },
    BatchRemove {
        keys: Vec<Vec<u8>>,
    },
    #[cfg(feature = "map_len")]
    Len,
    #[cfg(feature = "ttl")]
    ExpireAt(TimestampMillis),
    #[cfg(feature = "ttl")]
    Expire(TimestampMillis),
    #[cfg(feature = "ttl")]
    Ttl,
}

/// Unified response enum for all [`Map`](crate::storage::Map) operations.
pub enum CBMapResponse {
    Insert,
    Get(Option<Vec<u8>>),
    Remove,
    ContainsKey(bool),
    IsEmpty(bool),
    Clear,
    RemoveAndFetch(Option<Vec<u8>>),
    RemoveWithPrefix,
    BatchInsert,
    BatchRemove,
    #[cfg(feature = "map_len")]
    Len(usize),
    #[cfg(feature = "ttl")]
    ExpireAt(bool),
    #[cfg(feature = "ttl")]
    Expire(bool),
    #[cfg(feature = "ttl")]
    Ttl(Option<TimestampMillis>),
}

impl fmt::Debug for CBMapResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CBMapResponse::Insert => write!(f, "Insert"),
            CBMapResponse::Get(v) => f.debug_tuple("Get").field(v).finish(),
            CBMapResponse::Remove => write!(f, "Remove"),
            CBMapResponse::ContainsKey(b) => f.debug_tuple("ContainsKey").field(b).finish(),
            CBMapResponse::IsEmpty(b) => f.debug_tuple("IsEmpty").field(b).finish(),
            CBMapResponse::Clear => write!(f, "Clear"),
            CBMapResponse::RemoveAndFetch(v) => f.debug_tuple("RemoveAndFetch").field(v).finish(),
            CBMapResponse::RemoveWithPrefix => write!(f, "RemoveWithPrefix"),
            CBMapResponse::BatchInsert => write!(f, "BatchInsert"),
            CBMapResponse::BatchRemove => write!(f, "BatchRemove"),
            #[cfg(feature = "map_len")]
            CBMapResponse::Len(n) => f.debug_tuple("Len").field(n).finish(),
            #[cfg(feature = "ttl")]
            CBMapResponse::ExpireAt(b) => f.debug_tuple("ExpireAt").field(b).finish(),
            #[cfg(feature = "ttl")]
            CBMapResponse::Expire(b) => f.debug_tuple("Expire").field(b).finish(),
            #[cfg(feature = "ttl")]
            CBMapResponse::Ttl(v) => f.debug_tuple("Ttl").field(v).finish(),
        }
    }
}

/// A Tower [`Service`] that dispatches [`CBMapRequest`] to the inner [`StorageMap`].
#[derive(Clone)]
pub struct CBMapService {
    inner: StorageMap,
}

impl tower::Service<CBMapRequest> for CBMapService {
    type Response = CBMapResponse;
    type Error = anyhow::Error;
    type Future =
        std::pin::Pin<Box<dyn std::future::Future<Output = Result<Self::Response>> + Send>>;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: CBMapRequest) -> Self::Future {
        let this = self.inner.clone();
        Box::pin(async move {
            match req {
                CBMapRequest::Insert { key, val } => this
                    .insert_raw(&key, &val)
                    .await
                    .map(|_| CBMapResponse::Insert),
                CBMapRequest::Get { key } => {
                    let val = this.get_raw(&key).await?;
                    Ok(CBMapResponse::Get(val))
                }
                CBMapRequest::Remove { key } => {
                    this.remove(&key).await.map(|_| CBMapResponse::Remove)
                }
                CBMapRequest::ContainsKey { key } => this
                    .contains_key(&key)
                    .await
                    .map(CBMapResponse::ContainsKey),
                CBMapRequest::IsEmpty => this.is_empty().await.map(CBMapResponse::IsEmpty),
                CBMapRequest::Clear => this.clear().await.map(|_| CBMapResponse::Clear),
                CBMapRequest::RemoveAndFetch { key } => {
                    let val = this.remove_and_fetch_raw(&key).await?;
                    Ok(CBMapResponse::RemoveAndFetch(val))
                }
                CBMapRequest::RemoveWithPrefix { prefix } => this
                    .remove_with_prefix(&prefix)
                    .await
                    .map(|_| CBMapResponse::RemoveWithPrefix),
                CBMapRequest::BatchInsert { key_vals } => this
                    .batch_insert_raw(key_vals)
                    .await
                    .map(|_| CBMapResponse::BatchInsert),
                CBMapRequest::BatchRemove { keys } => this
                    .batch_remove(keys)
                    .await
                    .map(|_| CBMapResponse::BatchRemove),
                #[cfg(feature = "map_len")]
                CBMapRequest::Len => this.len().await.map(CBMapResponse::Len),
                #[cfg(feature = "ttl")]
                CBMapRequest::ExpireAt(at) => this.expire_at(at).await.map(CBMapResponse::ExpireAt),
                #[cfg(feature = "ttl")]
                CBMapRequest::Expire(dur) => this.expire(dur).await.map(CBMapResponse::Expire),
                #[cfg(feature = "ttl")]
                CBMapRequest::Ttl => this.ttl().await.map(CBMapResponse::Ttl),
            }
        })
    }
}

// ---------------------------------------------------------------------------
// List-level Request / Response & Service
// ---------------------------------------------------------------------------

/// Unified request enum for all [`List`](crate::storage::List) operations.
#[derive(Debug, Clone)]
pub enum CBListRequest {
    Push(Vec<u8>),
    Pushs(Vec<Vec<u8>>),
    PushLimit {
        val: Vec<u8>,
        limit: usize,
        pop_front_if_limited: bool,
    },
    Pop,
    GetAll,
    GetIndex(usize),
    Len,
    IsEmpty,
    Clear,
    #[cfg(feature = "ttl")]
    ExpireAt(TimestampMillis),
    #[cfg(feature = "ttl")]
    Expire(TimestampMillis),
    #[cfg(feature = "ttl")]
    Ttl,
}

/// Unified response enum for all [`List`](crate::storage::List) operations.
pub enum CBListResponse {
    Push,
    Pushs,
    PushLimit(Option<Vec<u8>>),
    Pop(Option<Vec<u8>>),
    GetAll(Vec<Vec<u8>>),
    GetIndex(Option<Vec<u8>>),
    Len(usize),
    IsEmpty(bool),
    Clear,
    #[cfg(feature = "ttl")]
    ExpireAt(bool),
    #[cfg(feature = "ttl")]
    Expire(bool),
    #[cfg(feature = "ttl")]
    Ttl(Option<TimestampMillis>),
}

impl fmt::Debug for CBListResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CBListResponse::Push => write!(f, "Push"),
            CBListResponse::Pushs => write!(f, "Pushs"),
            CBListResponse::PushLimit(v) => f.debug_tuple("PushLimit").field(v).finish(),
            CBListResponse::Pop(v) => f.debug_tuple("Pop").field(v).finish(),
            CBListResponse::GetAll(v) => f.debug_tuple("GetAll").field(&v.len()).finish(),
            CBListResponse::GetIndex(v) => f.debug_tuple("GetIndex").field(v).finish(),
            CBListResponse::Len(n) => f.debug_tuple("Len").field(n).finish(),
            CBListResponse::IsEmpty(b) => f.debug_tuple("IsEmpty").field(b).finish(),
            CBListResponse::Clear => write!(f, "Clear"),
            #[cfg(feature = "ttl")]
            CBListResponse::ExpireAt(b) => f.debug_tuple("ExpireAt").field(b).finish(),
            #[cfg(feature = "ttl")]
            CBListResponse::Expire(b) => f.debug_tuple("Expire").field(b).finish(),
            #[cfg(feature = "ttl")]
            CBListResponse::Ttl(v) => f.debug_tuple("Ttl").field(v).finish(),
        }
    }
}

/// A Tower [`Service`] that dispatches [`CBListRequest`] to the inner [`StorageList`].
#[derive(Clone)]
pub struct CBListService {
    inner: StorageList,
}

impl tower::Service<CBListRequest> for CBListService {
    type Response = CBListResponse;
    type Error = anyhow::Error;
    type Future =
        std::pin::Pin<Box<dyn std::future::Future<Output = Result<Self::Response>> + Send>>;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: CBListRequest) -> Self::Future {
        let this = self.inner.clone();
        Box::pin(async move {
            match req {
                CBListRequest::Push(val) => this.push_raw(&val).await.map(|_| CBListResponse::Push),
                CBListRequest::Pushs(vals) => {
                    this.pushs_raw(vals).await.map(|_| CBListResponse::Pushs)
                }
                CBListRequest::PushLimit {
                    val,
                    limit,
                    pop_front_if_limited,
                } => this
                    .push_limit_raw(&val, limit, pop_front_if_limited)
                    .await
                    .map(CBListResponse::PushLimit),
                CBListRequest::Pop => Ok(CBListResponse::Pop(this.pop_raw().await?)),
                CBListRequest::GetAll => Ok(CBListResponse::GetAll(this.all_raw().await?)),
                CBListRequest::GetIndex(idx) => {
                    Ok(CBListResponse::GetIndex(this.get_index_raw(idx).await?))
                }
                CBListRequest::Len => this.len().await.map(CBListResponse::Len),
                CBListRequest::IsEmpty => this.is_empty().await.map(CBListResponse::IsEmpty),
                CBListRequest::Clear => this.clear().await.map(|_| CBListResponse::Clear),
                #[cfg(feature = "ttl")]
                CBListRequest::ExpireAt(at) => {
                    this.expire_at(at).await.map(CBListResponse::ExpireAt)
                }
                #[cfg(feature = "ttl")]
                CBListRequest::Expire(dur) => this.expire(dur).await.map(CBListResponse::Expire),
                #[cfg(feature = "ttl")]
                CBListRequest::Ttl => this.ttl().await.map(CBListResponse::Ttl),
            }
        })
    }
}

// ---------------------------------------------------------------------------
// Helper: build Tower service chains with circuit breaker
// ---------------------------------------------------------------------------

/// A Tower [`Service`] wrapper that applies an optional per-call timeout.
///
/// When `timeout` is `Some(dur)`, every [`call`](Service::call) is raced against
/// `tokio::time::timeout`. If the inner service does not complete within `dur`,
/// the operation is aborted and an `anyhow::Error` is returned — which the outer
/// circuit breaker will wrap in `CircuitBreakerError::Inner` and count as a failure.
#[derive(Clone)]
struct CbTimeoutWrapper<S> {
    inner: S,
    timeout: Option<Duration>,
}

impl<S, Req> tower::Service<Req> for CbTimeoutWrapper<S>
where
    S: tower::Service<Req>,
    S::Error: Into<anyhow::Error>,
    S::Future: Send + 'static,
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
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, req: Req) -> Self::Future {
        let dur = self.timeout;
        let fut = self.inner.call(req);
        Box::pin(async move {
            match dur {
                Some(d) => match tokio::time::timeout(d, fut).await {
                    Ok(Ok(r)) => Ok(r),
                    Ok(Err(e)) => Err(e.into()),
                    Err(_) => Err(anyhow!("operation timed out after {:?}", d)),
                },
                None => fut.await.map_err(Into::into),
            }
        })
    }
}

/// Type alias: the circuit-breaker-wrapped concrete service for DB ops.
type CbStorageServiceCb = tower_resilience_circuitbreaker::CircuitBreaker<
    CbTimeoutWrapper<CBStorageService>,
    DefaultClassifier,
>;
/// Type alias: the circuit-breaker-wrapped concrete service for Map ops.
type CbMapServiceCb = tower_resilience_circuitbreaker::CircuitBreaker<
    CbTimeoutWrapper<CBMapService>,
    DefaultClassifier,
>;
/// Type alias: the circuit-breaker-wrapped concrete service for List ops.
type CbListServiceCb = tower_resilience_circuitbreaker::CircuitBreaker<
    CbTimeoutWrapper<CBListService>,
    DefaultClassifier,
>;

/// Build circuit breaker wrapping an arbitrary inner service.
fn build_cb_inner<S, Req>(
    inner: S,
    config: &CircuitBreakerConfig,
    name: &str,
) -> tower_resilience_circuitbreaker::CircuitBreaker<CbTimeoutWrapper<S>, DefaultClassifier>
where
    S: tower::Service<Req> + Clone + Send + 'static,
    S::Response: Send + 'static,
    S::Error: Into<anyhow::Error> + Send + 'static,
    S::Future: Send + 'static,
    Req: Send + 'static,
{
    let timeout_svc = CbTimeoutWrapper {
        inner,
        timeout: config.operation_timeout,
    };
    let n = name.to_owned();
    CircuitBreakerLayer::builder()
        .name(name)
        .failure_rate_threshold(config.failure_rate_threshold)
        .sliding_window_type(config.sliding_window_type)
        .sliding_window_size(config.sliding_window_size)
        .minimum_number_of_calls(config.minimum_number_of_calls)
        .wait_duration_in_open(config.wait_duration_in_open)
        .slow_call_duration_threshold(config.slow_call_duration_threshold)
        .slow_call_rate_threshold(config.slow_call_rate_threshold)
        .on_state_transition(move |from, to| {
            log::info!(
                "[circuit-breaker:{}] state changed: {:?} -> {:?}",
                n,
                from,
                to
            );
        })
        .build()
        .layer(timeout_svc)
}

/// Build the circuit breaker for DB-level service.
fn build_db_cb(
    inner: CBStorageService,
    config: &CircuitBreakerConfig,
    name: &str,
) -> CbStorageServiceCb {
    build_cb_inner(inner, config, name)
}

/// Build the circuit breaker for Map-level service.
fn build_map_cb(inner: CBMapService, config: &CircuitBreakerConfig, name: &str) -> CbMapServiceCb {
    build_cb_inner(inner, config, name)
}

/// Build the circuit breaker for List-level service.
fn build_list_cb(
    inner: CBListService,
    config: &CircuitBreakerConfig,
    name: &str,
) -> CbListServiceCb {
    build_cb_inner(inner, config, name)
}

// ---------------------------------------------------------------------------
// CircuitBrokenDB — public wrapper
// ---------------------------------------------------------------------------

/// A circuit-breaker-protected wrapper around [`DefaultStorageDB`].
///
/// Every operation goes through a Tower service chain that tracks success/failure.
/// When the failure rate exceeds the configured threshold, the circuit opens and
/// subsequent calls fail fast with an error rather than waiting for a timeout.
pub struct CircuitBrokenDB {
    service: Arc<Mutex<CbStorageServiceCb>>,
    config: CircuitBreakerConfig,
    /// Raw inner DB for pass-through operations that don't need CB protection
    /// (e.g., map_iter, list_iter, scan). Cloned cheaply from the DB passed to
    /// the Tower service chain during construction.
    inner: DefaultStorageDB,
}

impl CircuitBrokenDB {
    /// Wrap a `DefaultStorageDB` with a circuit breaker.
    pub fn new(db: DefaultStorageDB, config: CircuitBreakerConfig) -> Self {
        let inner = db.clone();
        let name = config.name.clone();
        let inner_svc = CBStorageService { inner: db };
        let service = build_db_cb(inner_svc, &config, &name);
        Self {
            service: Arc::new(Mutex::new(service)),
            config,
            inner,
        }
    }

    /// Return a reference to the circuit breaker configuration.
    pub fn config(&self) -> &CircuitBreakerConfig {
        &self.config
    }

    async fn raw_call(&self, req: CBStorageRequest) -> Result<CBStorageResponse> {
        let name = &self.config.name;
        let mut guard = self.service.lock().await;
        let svc = guard.ready().await.map_err(|e| map_cb_err(e, name))?;
        svc.call(req).await.map_err(|e| map_cb_err(e, name))
    }

    /// Return a JSON snapshot of the circuit breaker's runtime state & config.
    ///
    /// Analogous to [`StorageDB::info()`] but for the breaker itself. Reads
    /// atomic-loaded fields (`state`, `is_open`, `health_status`) without
    /// acquiring the circuit's internal lock, and acquires the lock only for the
    /// richer [`metrics()`](tower_resilience_circuitbreaker::CircuitBreaker::metrics) snapshot.
    pub async fn cb_info(&self) -> serde_json::Value {
        let guard = self.service.lock().await;
        let metrics = guard.metrics().await;

        serde_json::json!({
            "name": self.config.name,
            "state": format!("{:?}", metrics.state),
            "state_code": metrics.state as u8,
            "is_open": guard.is_open(),
            "health": guard.health_status(),
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
                "sliding_window_type": format!("{:?}", self.config.sliding_window_type),
                "sliding_window_size": self.config.sliding_window_size,
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
        // Clone the Arc (same inner storage, but if we wanted independent breaker
        // state each clone would need its own service). Here we keep the same breaker.
        // Use config to rebuild if independent state is desired — for now, sharing the
        // Arc<Mutex<...>> means all clones share the same breaker state.
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
    ) -> Result<Self::MapType> {
        let req = CBStorageRequest::Map {
            name: name.as_ref().to_vec(),
            expire,
        };
        let resp = self.raw_call(req).await?;
        match resp {
            CBStorageResponse::Map(m) => {
                let cfg = CircuitBreakerConfig {
                    name: format!("{}-map", self.config.name),
                    ..self.config.clone()
                };
                Ok(CircuitBrokenMap::new(m, cfg))
            }
            _ => Err(anyhow!("unexpected response type for map()")),
        }
    }

    async fn map_remove<K>(&self, name: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        self.raw_call(CBStorageRequest::MapRemove {
            name: name.as_ref().to_vec(),
        })
        .await
        .map(|_| ())
    }

    async fn map_contains_key<K: AsRef<[u8]> + Sync + Send>(&self, key: K) -> Result<bool> {
        match self
            .raw_call(CBStorageRequest::MapContainsKey {
                key: key.as_ref().to_vec(),
            })
            .await?
        {
            CBStorageResponse::MapContainsKey(b) => Ok(b),
            _ => Err(anyhow!("unexpected response type for map_contains_key()")),
        }
    }

    async fn list<V: AsRef<[u8]> + Sync + Send>(
        &self,
        name: V,
        expire: Option<TimestampMillis>,
    ) -> Result<Self::ListType> {
        let req = CBStorageRequest::List {
            name: name.as_ref().to_vec(),
            expire,
        };
        let resp = self.raw_call(req).await?;
        match resp {
            CBStorageResponse::List(l) => {
                let cfg = CircuitBreakerConfig {
                    name: format!("{}-list", self.config.name),
                    ..self.config.clone()
                };
                Ok(CircuitBrokenList::new(l, cfg))
            }
            _ => Err(anyhow!("unexpected response type for list()")),
        }
    }

    async fn list_remove<K>(&self, name: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        self.raw_call(CBStorageRequest::ListRemove {
            name: name.as_ref().to_vec(),
        })
        .await
        .map(|_| ())
    }

    async fn list_contains_key<K: AsRef<[u8]> + Sync + Send>(&self, key: K) -> Result<bool> {
        match self
            .raw_call(CBStorageRequest::ListContainsKey {
                key: key.as_ref().to_vec(),
            })
            .await?
        {
            CBStorageResponse::ListContainsKey(b) => Ok(b),
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
        let mut base = match self.raw_call(CBStorageRequest::Info).await? {
            CBStorageResponse::Info(v) => v,
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
pub struct CircuitBrokenMap {
    service: Arc<Mutex<CbMapServiceCb>>,
    /// Retained for name() and iterator methods that bypass the CB.
    inner: StorageMap,
}

impl Clone for CircuitBrokenMap {
    fn clone(&self) -> Self {
        Self {
            service: Arc::clone(&self.service),
            inner: self.inner.clone(),
        }
    }
}

impl CircuitBrokenMap {
    fn new(m: StorageMap, config: CircuitBreakerConfig) -> Self {
        let inner = m.clone();
        let name = config.name.clone();
        let inner_svc = CBMapService { inner: m };
        let service = build_map_cb(inner_svc, &config, &name);
        Self {
            service: Arc::new(Mutex::new(service)),
            inner,
        }
    }

    async fn raw_call(&self, req: CBMapRequest) -> Result<CBMapResponse> {
        let mut guard = self.service.lock().await;
        let svc = guard.ready().await.map_err(|e| map_cb_err(e, "map"))?;
        svc.call(req).await.map_err(|e| map_cb_err(e, "map"))
    }
}

// ---------------------------------------------------------------------------
// Map trait implementation for CircuitBrokenMap
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
        self.raw_call(CBMapRequest::Insert {
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
        match self
            .raw_call(CBMapRequest::Get {
                key: key.as_ref().to_vec(),
            })
            .await?
        {
            CBMapResponse::Get(Some(bytes)) => Ok(Some(postcard::from_bytes(&bytes)?)),
            CBMapResponse::Get(None) => Ok(None),
            _ => Err(anyhow!("unexpected response type for map::get()")),
        }
    }

    async fn remove<K>(&self, key: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        self.raw_call(CBMapRequest::Remove {
            key: key.as_ref().to_vec(),
        })
        .await
        .map(|_| ())
    }

    async fn contains_key<K: AsRef<[u8]> + Sync + Send>(&self, key: K) -> Result<bool> {
        match self
            .raw_call(CBMapRequest::ContainsKey {
                key: key.as_ref().to_vec(),
            })
            .await?
        {
            CBMapResponse::ContainsKey(b) => Ok(b),
            _ => Err(anyhow!("unexpected response type for map::contains_key()")),
        }
    }

    async fn is_empty(&self) -> Result<bool> {
        match self.raw_call(CBMapRequest::IsEmpty).await? {
            CBMapResponse::IsEmpty(b) => Ok(b),
            _ => Err(anyhow!("unexpected response type for map::is_empty()")),
        }
    }

    async fn clear(&self) -> Result<()> {
        self.raw_call(CBMapRequest::Clear).await.map(|_| ())
    }

    async fn remove_and_fetch<K, V>(&self, key: K) -> Result<Option<V>>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: DeserializeOwned + Sync + Send,
    {
        match self
            .raw_call(CBMapRequest::RemoveAndFetch {
                key: key.as_ref().to_vec(),
            })
            .await?
        {
            CBMapResponse::RemoveAndFetch(Some(bytes)) => Ok(Some(postcard::from_bytes(&bytes)?)),
            CBMapResponse::RemoveAndFetch(None) => Ok(None),
            _ => Err(anyhow!(
                "unexpected response type for map::remove_and_fetch()"
            )),
        }
    }

    async fn remove_with_prefix<K>(&self, prefix: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        self.raw_call(CBMapRequest::RemoveWithPrefix {
            prefix: prefix.as_ref().to_vec(),
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
        self.raw_call(CBMapRequest::BatchInsert { key_vals: pairs })
            .await
            .map(|_| ())
    }

    async fn batch_remove(&self, keys: Vec<Key>) -> Result<()> {
        self.raw_call(CBMapRequest::BatchRemove { keys })
            .await
            .map(|_| ())
    }

    #[cfg(feature = "map_len")]
    async fn len(&self) -> Result<usize> {
        match self.raw_call(CBMapRequest::Len).await? {
            CBMapResponse::Len(n) => Ok(n),
            _ => Err(anyhow!("unexpected response type for map::len()")),
        }
    }

    #[cfg(feature = "ttl")]
    async fn expire_at(&self, at: TimestampMillis) -> Result<bool> {
        match self.raw_call(CBMapRequest::ExpireAt(at)).await? {
            CBMapResponse::ExpireAt(b) => Ok(b),
            _ => Err(anyhow!("unexpected response type for map::expire_at()")),
        }
    }

    #[cfg(feature = "ttl")]
    async fn expire(&self, dur: TimestampMillis) -> Result<bool> {
        match self.raw_call(CBMapRequest::Expire(dur)).await? {
            CBMapResponse::Expire(b) => Ok(b),
            _ => Err(anyhow!("unexpected response type for map::expire()")),
        }
    }

    #[cfg(feature = "ttl")]
    async fn ttl(&self) -> Result<Option<TimestampMillis>> {
        match self.raw_call(CBMapRequest::Ttl).await? {
            CBMapResponse::Ttl(v) => Ok(v),
            _ => Err(anyhow!("unexpected response type for map::ttl()")),
        }
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
}

// ---------------------------------------------------------------------------
// CircuitBrokenList — public wrapper
// ---------------------------------------------------------------------------

/// A circuit-breaker-protected wrapper around [`StorageList`].
pub struct CircuitBrokenList {
    service: Arc<Mutex<CbListServiceCb>>,
    /// Retained for name() and iter() that bypass the CB.
    inner: StorageList,
}

impl Clone for CircuitBrokenList {
    fn clone(&self) -> Self {
        Self {
            service: Arc::clone(&self.service),
            inner: self.inner.clone(),
        }
    }
}

impl CircuitBrokenList {
    fn new(l: StorageList, config: CircuitBreakerConfig) -> Self {
        let inner = l.clone();
        let name = config.name.clone();
        let inner_svc = CBListService { inner: l };
        let service = build_list_cb(inner_svc, &config, &name);
        Self {
            service: Arc::new(Mutex::new(service)),
            inner,
        }
    }

    async fn raw_call(&self, req: CBListRequest) -> Result<CBListResponse> {
        let mut guard = self.service.lock().await;
        let svc = guard.ready().await.map_err(|e| map_cb_err(e, "list"))?;
        svc.call(req).await.map_err(|e| map_cb_err(e, "list"))
    }
}

// ---------------------------------------------------------------------------
// List trait implementation for CircuitBrokenList
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
        self.raw_call(CBListRequest::Push(postcard::to_stdvec(val)?))
            .await
            .map(|_| ())
    }

    async fn pushs<V>(&self, vals: Vec<V>) -> Result<()>
    where
        V: Serialize + Sync + Send,
    {
        let mut bytes_vec = Vec::with_capacity(vals.len());
        for v in vals {
            bytes_vec.push(postcard::to_stdvec(&v)?);
        }
        self.raw_call(CBListRequest::Pushs(bytes_vec))
            .await
            .map(|_| ())
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
        match self
            .raw_call(CBListRequest::PushLimit {
                val: bytes,
                limit,
                pop_front_if_limited,
            })
            .await?
        {
            CBListResponse::PushLimit(Some(popped)) => Ok(Some(postcard::from_bytes(&popped)?)),
            CBListResponse::PushLimit(None) => Ok(None),
            _ => Err(anyhow!("unexpected response type for list::push_limit()")),
        }
    }

    async fn pop<V>(&self) -> Result<Option<V>>
    where
        V: DeserializeOwned + Sync + Send,
    {
        match self.raw_call(CBListRequest::Pop).await? {
            CBListResponse::Pop(Some(bytes)) => Ok(Some(postcard::from_bytes(&bytes)?)),
            CBListResponse::Pop(None) => Ok(None),
            _ => Err(anyhow!("unexpected response type for list::pop()")),
        }
    }

    async fn all<V>(&self) -> Result<Vec<V>>
    where
        V: DeserializeOwned + Sync + Send,
    {
        match self.raw_call(CBListRequest::GetAll).await? {
            CBListResponse::GetAll(vals) => vals
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
        match self.raw_call(CBListRequest::GetIndex(idx)).await? {
            CBListResponse::GetIndex(Some(bytes)) => Ok(Some(postcard::from_bytes(&bytes)?)),
            CBListResponse::GetIndex(None) => Ok(None),
            _ => Err(anyhow!("unexpected response type for list::get_index()")),
        }
    }

    async fn len(&self) -> Result<usize> {
        match self.raw_call(CBListRequest::Len).await? {
            CBListResponse::Len(n) => Ok(n),
            _ => Err(anyhow!("unexpected response type for list::len()")),
        }
    }

    async fn is_empty(&self) -> Result<bool> {
        match self.raw_call(CBListRequest::IsEmpty).await? {
            CBListResponse::IsEmpty(b) => Ok(b),
            _ => Err(anyhow!("unexpected response type for list::is_empty()")),
        }
    }

    async fn clear(&self) -> Result<()> {
        self.raw_call(CBListRequest::Clear).await.map(|_| ())
    }

    #[cfg(feature = "ttl")]
    async fn expire_at(&self, at: TimestampMillis) -> Result<bool> {
        match self.raw_call(CBListRequest::ExpireAt(at)).await? {
            CBListResponse::ExpireAt(b) => Ok(b),
            _ => Err(anyhow!("unexpected response type for list::expire_at()")),
        }
    }

    #[cfg(feature = "ttl")]
    async fn expire(&self, dur: TimestampMillis) -> Result<bool> {
        match self.raw_call(CBListRequest::Expire(dur)).await? {
            CBListResponse::Expire(b) => Ok(b),
            _ => Err(anyhow!("unexpected response type for list::expire()")),
        }
    }

    #[cfg(feature = "ttl")]
    async fn ttl(&self) -> Result<Option<TimestampMillis>> {
        match self.raw_call(CBListRequest::Ttl).await? {
            CBListResponse::Ttl(v) => Ok(v),
            _ => Err(anyhow!("unexpected response type for list::ttl()")),
        }
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
    // Map-level mock
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

    impl Service<CBMapRequest> for MockMapSvc {
        type Response = CBMapResponse;
        type Error = anyhow::Error;
        type Future = Pin<Box<dyn std::future::Future<Output = Result<Self::Response>> + Send>>;

        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: CBMapRequest) -> Self::Future {
            let count = self.call_count.fetch_add(1, Ordering::SeqCst);
            let fail_until = self.fail_until;
            Box::pin(async move {
                if count < fail_until {
                    Err(anyhow!("mock map failure"))
                } else {
                    Ok(CBMapResponse::IsEmpty(true))
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

    impl Service<CBListRequest> for MockListSvc {
        type Response = CBListResponse;
        type Error = anyhow::Error;
        type Future = Pin<Box<dyn std::future::Future<Output = Result<Self::Response>> + Send>>;

        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: CBListRequest) -> Self::Future {
            let count = self.call_count.fetch_add(1, Ordering::SeqCst);
            let fail_until = self.fail_until;
            Box::pin(async move {
                if count < fail_until {
                    Err(anyhow!("mock list failure"))
                } else {
                    Ok(CBListResponse::Len(0))
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
            sliding_window_size: 3,
            minimum_number_of_calls: 3,
            wait_duration_in_open: Duration::from_millis(200),
            slow_call_duration_threshold: Duration::from_secs(1),
            slow_call_rate_threshold: 1.0,
            operation_timeout: None,
            sliding_window_type: SlidingWindowType::CountBased,
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

        let resp = cb.ready().await.unwrap().call(CBMapRequest::IsEmpty).await;
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

        let resp = cb.ready().await.unwrap().call(CBListRequest::Len).await;
        assert!(resp.is_err(), "list error should propagate");
    }

    // -----------------------------------------------------------------------
    // Tests — Timeout
    // -----------------------------------------------------------------------

    /// A mock service that sleeps for 100ms before responding.
    #[derive(Clone)]
    struct SlowMockSvc {
        sleep: Duration,
    }

    impl Service<CBStorageRequest> for SlowMockSvc {
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
            let sleep = self.sleep;
            Box::pin(async move {
                tokio::time::sleep(sleep).await;
                Ok(CBStorageResponse::Insert)
            })
        }
    }

    #[tokio::test]
    async fn test_timeout_returns_error() {
        // Service takes 100ms, but timeout is 10ms → should time out.
        let mock = SlowMockSvc {
            sleep: Duration::from_millis(100),
        };
        let mut cfg = fast_cb_config("timeout_err");
        cfg.operation_timeout = Some(Duration::from_millis(10));
        let mut cb = build_cb_inner(mock, &cfg, &cfg.name);

        let req = CBStorageRequest::Get { key: b"x".to_vec() };
        let resp = cb.ready().await.unwrap().call(req).await;
        assert!(resp.is_err(), "expected timeout error");
        let err = resp.unwrap_err().to_string();
        assert!(
            err.contains("timed out"),
            "expected 'timed out' in error, got: {}",
            err
        );
    }

    #[tokio::test]
    async fn test_timeout_within_limit_succeeds() {
        // Service takes 10ms, timeout is 200ms → should succeed.
        let mock = SlowMockSvc {
            sleep: Duration::from_millis(10),
        };
        let mut cfg = fast_cb_config("timeout_ok");
        cfg.operation_timeout = Some(Duration::from_millis(200));
        let mut cb = build_cb_inner(mock, &cfg, &cfg.name);

        let req = CBStorageRequest::Get { key: b"y".to_vec() };
        let resp = cb.ready().await.unwrap().call(req).await;
        assert!(resp.is_ok(), "expected success, got: {:?}", resp);
    }

    #[tokio::test]
    async fn test_timeout_counts_as_failure_and_trips_circuit() {
        // Make enough calls that all time out → circuit should open.
        let mock = SlowMockSvc {
            sleep: Duration::from_millis(100),
        };
        let mut cfg = fast_cb_config("timeout_trip");
        cfg.operation_timeout = Some(Duration::from_millis(10));
        // minimum_number_of_calls=3, failure_rate_threshold=0.5 →
        // after 3 failures recorded, the 4th poll_ready should return OpenCircuit.
        let mut cb = build_cb_inner(mock, &cfg, &cfg.name);

        let req = CBStorageRequest::Get { key: b"t".to_vec() };

        // First three calls: each should get a timeout (Inner error).
        for i in 0..3 {
            let resp = cb.ready().await.unwrap().call(req.clone()).await;
            assert!(resp.is_err(), "call {} should have timed out", i);
            let err = resp.unwrap_err().to_string();
            assert!(
                err.contains("timed out"),
                "call {} expected 'timed out', got: {}",
                i,
                err
            );
        }

        // Fourth call: circuit should now be open → OpenCircuit error.
        // Note: CircuitBreaker checks state in call() (via try_acquire), not in
        // poll_ready(), when backpressure mode is disabled (the default).
        let result = cb.ready().await.unwrap().call(req.clone()).await;
        match result {
            Err(err) => {
                let msg = err.to_string();
                assert!(
                    msg.contains("circuit is open"),
                    "expected OpenCircuit error, got: {}",
                    msg
                );
            }
            Ok(_) => panic!("expected OpenCircuit error on fourth call"),
        }
    }
}
