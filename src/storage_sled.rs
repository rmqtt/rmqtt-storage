//! Sled-based persistent storage implementation
//!
//! This module provides a persistent storage solution backed by Sled (an embedded database).
//! It implements key-value storage, maps (dictionaries), and lists (queues) with support for:
//! - Atomic operations and transactions
//! - Asynchronous API
//! - TTL/expiration (optional feature)
//! - Counters
//! - Batch operations
//! - Iterators
//!
//! The implementation uses multiple sled trees for different data types and provides
//! a command-based interface with background processing for concurrent operations.

use core::fmt;
use std::borrow::Cow;
use std::fmt::Debug;
use std::io;
use std::io::{ErrorKind, Read};
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, AtomicIsize, Ordering};
use std::sync::Arc;

use anyhow::{anyhow, Error};
use async_trait::async_trait;
use convert::Bytesize;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

#[allow(unused_imports)]
use sled::{Batch, IVec, Tree};
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::spawn_blocking;

use crate::storage::{AsyncIterator, IterItem, Key, List, Map, StorageDB};
#[allow(unused_imports)]
use crate::{timestamp_millis, TimestampMillis};
use crate::{Result, StorageList, StorageMap};

/// Byte separator used in composite keys
const SEPARATOR: &[u8] = b"@";
/// Tree name for key-value storage
const KV_TREE: &[u8] = b"__kv_tree@";
/// Tree name for map metadata
const MAP_TREE: &[u8] = b"__map_tree@";
/// Tree name for list metadata
const LIST_TREE: &[u8] = b"__list_tree@";
/// Tree for tracking expiration times (expire_at => key)
const EXPIRE_KEYS_TREE: &[u8] = b"__expire_key_tree@";
/// Tree for tracking key expiration (key => expire_at)
const KEY_EXPIRE_TREE: &[u8] = b"__key_expire_tree@";
/// Prefix for map keys
const MAP_NAME_PREFIX: &[u8] = b"__map@";
/// Separator between map name and item key
const MAP_KEY_SEPARATOR: &[u8] = b"@__item@";
#[allow(dead_code)]
/// Suffix for map count keys
const MAP_KEY_COUNT_SUFFIX: &[u8] = b"@__count@";

/// Prefix for list keys
const LIST_NAME_PREFIX: &[u8] = b"__list@";
/// Suffix for list count keys
const LIST_KEY_COUNT_SUFFIX: &[u8] = b"@__count@";
/// Suffix for list content keys
const LIST_KEY_CONTENT_SUFFIX: &[u8] = b"@__content@";

/// Enum representing different key types in storage
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
enum KeyType {
    /// Key-value pair
    KV,
    /// Map structure
    Map,
    /// List structure
    List,
}

impl KeyType {
    /// Encodes key type to a single byte
    #[inline]
    #[allow(dead_code)]
    fn encode(&self) -> &[u8] {
        match self {
            KeyType::KV => &[1],
            KeyType::Map => &[2],
            KeyType::List => &[3],
        }
    }

    /// Decodes key type from byte representation
    #[inline]
    #[allow(dead_code)]
    fn decode(v: &[u8]) -> Result<Self> {
        if v.is_empty() {
            Err(anyhow!("invalid data"))
        } else {
            match v[0] {
                1 => Ok(KeyType::KV),
                2 => Ok(KeyType::Map),
                3 => Ok(KeyType::List),
                _ => Err(anyhow!("invalid data")),
            }
        }
    }
}

/// Enum representing all possible storage operations
enum Command {
    // Database operations
    DBInsert(SledStorageDB, Key, Vec<u8>, oneshot::Sender<Result<()>>),
    DBGet(SledStorageDB, IVec, oneshot::Sender<Result<Option<IVec>>>),
    DBRemove(SledStorageDB, IVec, oneshot::Sender<Result<()>>),
    DBMapNew(
        SledStorageDB,
        IVec,
        Option<TimestampMillis>,
        oneshot::Sender<Result<SledStorageMap>>,
    ),
    DBMapRemove(SledStorageDB, IVec, oneshot::Sender<Result<()>>),
    DBMapContainsKey(SledStorageDB, IVec, oneshot::Sender<Result<bool>>),
    DBListNew(
        SledStorageDB,
        IVec,
        Option<TimestampMillis>,
        oneshot::Sender<Result<SledStorageList>>,
    ),
    DBListRemove(SledStorageDB, IVec, oneshot::Sender<Result<()>>),
    DBListContainsKey(SledStorageDB, IVec, oneshot::Sender<Result<bool>>),
    DBBatchInsert(SledStorageDB, Vec<(Key, IVec)>, oneshot::Sender<Result<()>>),
    DBBatchRemove(SledStorageDB, Vec<Key>, oneshot::Sender<Result<()>>),
    DBCounterIncr(SledStorageDB, IVec, isize, oneshot::Sender<Result<()>>),
    DBCounterDecr(SledStorageDB, IVec, isize, oneshot::Sender<Result<()>>),
    DBCounterGet(SledStorageDB, IVec, oneshot::Sender<Result<Option<isize>>>),
    DBCounterSet(SledStorageDB, IVec, isize, oneshot::Sender<Result<()>>),
    DBContainsKey(SledStorageDB, IVec, oneshot::Sender<Result<bool>>),
    #[cfg(feature = "ttl")]
    DBExpireAt(
        SledStorageDB,
        IVec,
        TimestampMillis,
        oneshot::Sender<Result<bool>>,
    ),
    #[cfg(feature = "ttl")]
    DBTtl(
        SledStorageDB,
        IVec,
        oneshot::Sender<Result<Option<TimestampMillis>>>,
    ),
    DBMapPrefixIter(SledStorageDB, oneshot::Sender<sled::Iter>),
    DBListPrefixIter(SledStorageDB, oneshot::Sender<sled::Iter>),
    DBScanIter(SledStorageDB, Vec<u8>, oneshot::Sender<sled::Iter>),
    #[allow(dead_code)]
    DBLen(SledStorageDB, oneshot::Sender<usize>),
    DBSize(SledStorageDB, oneshot::Sender<usize>),

    // Map operations
    MapInsert(SledStorageMap, IVec, IVec, oneshot::Sender<Result<()>>),
    MapGet(SledStorageMap, IVec, oneshot::Sender<Result<Option<IVec>>>),
    MapRemove(SledStorageMap, IVec, oneshot::Sender<Result<()>>),
    MapContainsKey(SledStorageMap, IVec, oneshot::Sender<Result<bool>>),
    #[cfg(feature = "map_len")]
    MapLen(SledStorageMap, oneshot::Sender<Result<usize>>),
    MapIsEmpty(SledStorageMap, oneshot::Sender<Result<bool>>),
    MapClear(SledStorageMap, oneshot::Sender<Result<()>>),
    MapRemoveAndFetch(SledStorageMap, IVec, oneshot::Sender<Result<Option<IVec>>>),
    MapRemoveWithPrefix(SledStorageMap, IVec, oneshot::Sender<Result<()>>),
    MapBatchInsert(
        SledStorageMap,
        Vec<(IVec, IVec)>,
        oneshot::Sender<Result<()>>,
    ),
    MapBatchRemove(SledStorageMap, Vec<IVec>, oneshot::Sender<Result<()>>),
    #[cfg(feature = "ttl")]
    MapExpireAt(
        SledStorageMap,
        TimestampMillis,
        oneshot::Sender<Result<bool>>,
    ),
    #[cfg(feature = "ttl")]
    MapTTL(
        SledStorageMap,
        oneshot::Sender<Result<Option<TimestampMillis>>>,
    ),
    MapIsExpired(SledStorageMap, oneshot::Sender<Result<bool>>),
    MapPrefixIter(SledStorageMap, Option<IVec>, oneshot::Sender<sled::Iter>),

    // List operations
    ListPush(SledStorageList, IVec, oneshot::Sender<Result<()>>),
    ListPushs(SledStorageList, Vec<IVec>, oneshot::Sender<Result<()>>),
    ListPushLimit(
        SledStorageList,
        IVec,
        usize,
        bool,
        oneshot::Sender<Result<Option<IVec>>>,
    ),
    ListPop(SledStorageList, oneshot::Sender<Result<Option<IVec>>>),
    ListAll(SledStorageList, oneshot::Sender<Result<Vec<IVec>>>),
    ListGetIndex(
        SledStorageList,
        usize,
        oneshot::Sender<Result<Option<IVec>>>,
    ),
    ListLen(SledStorageList, oneshot::Sender<Result<usize>>),
    ListIsEmpty(SledStorageList, oneshot::Sender<Result<bool>>),
    ListClear(SledStorageList, oneshot::Sender<Result<()>>),
    #[cfg(feature = "ttl")]
    ListExpireAt(
        SledStorageList,
        TimestampMillis,
        oneshot::Sender<Result<bool>>,
    ),
    #[cfg(feature = "ttl")]
    ListTTL(
        SledStorageList,
        oneshot::Sender<Result<Option<TimestampMillis>>>,
    ),
    ListIsExpired(SledStorageList, oneshot::Sender<Result<bool>>),
    ListPrefixIter(SledStorageList, oneshot::Sender<sled::Iter>),

    // Iterator operation
    #[allow(clippy::type_complexity)]
    IterNext(
        sled::Iter,
        oneshot::Sender<(sled::Iter, Option<sled::Result<(IVec, IVec)>>)>,
    ),
}

/// Type alias for cleanup function signature
pub type CleanupFun = fn(&SledStorageDB);

/// Default cleanup function that runs in background thread
fn def_cleanup(_db: &SledStorageDB) {
    #[cfg(feature = "ttl")]
    {
        let db = _db.clone();
        std::thread::spawn(move || {
            let limit = 500;
            loop {
                std::thread::sleep(std::time::Duration::from_secs(10));
                let mut total_cleanups = 0;
                let now = std::time::Instant::now();
                loop {
                    let now = std::time::Instant::now();
                    let count = db.cleanup(limit);
                    total_cleanups += count;
                    if count > 0 {
                        log::debug!(
                            "def_cleanup: {}, total cleanups: {}, active_count(): {}, cost time: {:?}",
                            count,
                            total_cleanups,
                            db.active_count(),
                            now.elapsed()
                        );
                    }
                    if count < limit {
                        break;
                    }
                    if db.active_count() > 50 {
                        std::thread::sleep(std::time::Duration::from_millis(500));
                    } else {
                        std::thread::sleep(std::time::Duration::from_millis(0));
                    }
                }
                if now.elapsed().as_secs() > 3 {
                    log::info!(
                        "total cleanups: {}, cost time: {:?}",
                        total_cleanups,
                        now.elapsed()
                    );
                }
            }
        });
    }
}

/// Configuration for Sled storage backend
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SledConfig {
    /// Path to database directory
    pub path: String,
    /// Cache capacity in bytes
    pub cache_capacity: Bytesize,
    /// Cleanup function for expired keys
    #[serde(skip, default = "SledConfig::cleanup_f_default")]
    pub cleanup_f: CleanupFun,
}

impl Default for SledConfig {
    fn default() -> Self {
        SledConfig {
            path: String::default(),
            cache_capacity: Bytesize::from(1024 * 1024 * 1024),
            cleanup_f: def_cleanup,
        }
    }
}

impl SledConfig {
    /// Converts to Sled's native configuration
    #[inline]
    pub fn to_sled_config(&self) -> Result<sled::Config> {
        if self.path.trim().is_empty() {
            return Err(Error::msg("storage dir is empty"));
        }
        let sled_cfg = sled::Config::default()
            .path(self.path.trim())
            .cache_capacity(self.cache_capacity.as_u64())
            .mode(sled::Mode::HighThroughput);
        Ok(sled_cfg)
    }

    /// Returns default cleanup function
    #[inline]
    fn cleanup_f_default() -> CleanupFun {
        def_cleanup
    }
}

/// Increments a counter value stored in bytes
fn _increment(old: Option<&[u8]>) -> Option<Vec<u8>> {
    let number = match old {
        Some(bytes) => {
            if let Ok(array) = bytes.try_into() {
                let number = isize::from_be_bytes(array);
                number + 1
            } else {
                1
            }
        }
        None => 1,
    };

    Some(number.to_be_bytes().to_vec())
}

/// Decrements a counter value stored in bytes
fn _decrement(old: Option<&[u8]>) -> Option<Vec<u8>> {
    let number = match old {
        Some(bytes) => {
            if let Ok(array) = bytes.try_into() {
                let number = isize::from_be_bytes(array);
                number - 1
            } else {
                -1
            }
        }
        None => -1,
    };

    Some(number.to_be_bytes().to_vec())
}

/// Pattern for matching keys with wildcards
#[derive(Clone)]
pub struct Pattern(Arc<Vec<PatternChar>>);

impl Deref for Pattern {
    type Target = Vec<PatternChar>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<&str> for Pattern {
    fn from(pattern: &str) -> Self {
        Pattern::parse(pattern.as_bytes())
    }
}

impl From<&[u8]> for Pattern {
    fn from(pattern: &[u8]) -> Self {
        Pattern::parse(pattern)
    }
}

/// Represents a single character in a pattern
#[derive(Clone)]
pub enum PatternChar {
    /// Literal byte
    Literal(u8),
    /// Wildcard matching zero or more characters
    Wildcard,
    /// Matches any single character
    AnyChar,
}

impl Pattern {
    /// Parses a byte pattern into PatternChar sequence
    pub fn parse(pattern: &[u8]) -> Self {
        let mut parsed_pattern = Vec::new();
        let mut chars = pattern.bytes().peekable();

        while let Some(Ok(c)) = chars.next() {
            if c == b'\\' {
                if let Some(Ok(next_char)) = chars.next() {
                    match next_char {
                        b'?' => parsed_pattern.push(PatternChar::Literal(b'?')),
                        b'*' => parsed_pattern.push(PatternChar::Literal(b'*')),
                        _ => {
                            parsed_pattern.push(PatternChar::Literal(b'\\'));
                            parsed_pattern.push(PatternChar::Literal(next_char));
                        }
                    }
                }
            } else {
                match c {
                    b'?' => parsed_pattern.push(PatternChar::AnyChar),
                    b'*' => parsed_pattern.push(PatternChar::Wildcard),
                    _ => parsed_pattern.push(PatternChar::Literal(c)),
                }
            }
        }

        Pattern(Arc::new(parsed_pattern))
    }
}

/// Checks if text matches the given pattern
fn is_match<P: Into<Pattern>>(pattern: P, text: &[u8]) -> bool {
    let pattern = pattern.into();
    let text_chars = text;
    let pattern_len = pattern.len();
    let text_len = text_chars.len();

    let mut dp = vec![vec![false; text_len + 1]; pattern_len + 1];
    dp[0][0] = true;

    for i in 1..=pattern_len {
        if let PatternChar::Wildcard = pattern[i - 1] {
            dp[i][0] = dp[i - 1][0];
        }
        for j in 1..=text_len {
            match pattern[i - 1] {
                PatternChar::Wildcard => {
                    dp[i][j] = dp[i - 1][j] || dp[i][j - 1];
                }
                PatternChar::AnyChar | PatternChar::Literal(_) => {
                    if let PatternChar::Literal(c) = pattern[i - 1] {
                        dp[i][j] = (c == b'?' || c == text_chars[j - 1]) && dp[i - 1][j - 1];
                    } else {
                        dp[i][j] = dp[i - 1][j - 1];
                    }
                }
            }
        }
    }

    dp[pattern_len][text_len]
}

/// Trait for byte replacement
pub trait BytesReplace {
    /// Replaces all occurrences of `from` with `to` in the byte slice
    fn replace(self, from: &[u8], to: &[u8]) -> Vec<u8>;
}

impl BytesReplace for &[u8] {
    fn replace(self, from: &[u8], to: &[u8]) -> Vec<u8> {
        let input = self;
        let mut result = Vec::new();
        let mut i = 0;
        while i < input.len() {
            if input[i..].starts_with(from) {
                result.extend_from_slice(to);
                i += from.len();
            } else {
                result.push(input[i]);
                i += 1;
            }
        }
        result
    }
}

/// Main database handle for Sled storage
#[derive(Clone)]
pub struct SledStorageDB {
    /// Underlying sled database
    pub(crate) db: Arc<sled::Db>,
    /// Tree for key-value storage
    pub(crate) kv_tree: sled::Tree,
    /// Tree for map metadata
    pub(crate) map_tree: sled::Tree,
    /// Tree for list metadata
    pub(crate) list_tree: sled::Tree,
    /// Tree for tracking expiration times
    #[allow(dead_code)]
    pub(crate) expire_key_tree: sled::Tree,
    /// Tree for tracking key expiration
    #[allow(dead_code)]
    pub(crate) key_expire_tree: sled::Tree,
    /// Channel sender for commands
    cmd_tx: mpsc::Sender<Command>,
    /// Count of active commands
    active_count: Arc<AtomicIsize>,
}

impl SledStorageDB {
    /// Creates a new SledStorageDB instance
    #[inline]
    pub(crate) async fn new(cfg: SledConfig) -> Result<Self> {
        let sled_cfg = cfg.to_sled_config()?;
        let (db, kv_tree, map_tree, list_tree, expire_key_tree, key_expire_tree) =
            sled_cfg.open().map(|db| {
                let kv_tree = db.open_tree(KV_TREE);
                let map_tree = db.open_tree(MAP_TREE);
                let list_tree = db.open_tree(LIST_TREE);
                let expire_key_tree = db.open_tree(EXPIRE_KEYS_TREE);
                let key_expire_tree = db.open_tree(KEY_EXPIRE_TREE);
                (
                    Arc::new(db),
                    kv_tree,
                    map_tree,
                    list_tree,
                    expire_key_tree,
                    key_expire_tree,
                )
            })?;
        let kv_tree = kv_tree?;
        let map_tree = map_tree?;
        let list_tree = list_tree?;
        let expire_key_tree = expire_key_tree?;
        let key_expire_tree = key_expire_tree?;
        let active_count = Arc::new(AtomicIsize::new(0));
        let active_count1 = active_count.clone();

        let (cmd_tx, mut cmd_rx) = tokio::sync::mpsc::channel::<Command>(300_000);
        spawn_blocking(move || {
            Handle::current().block_on(async move {
                while let Some(cmd) = cmd_rx.recv().await {
                    let err = anyhow::Error::msg("send result fail");
                    let snd_res = match cmd {
                        Command::DBInsert(db, key, val, res_tx) => res_tx
                            .send(db._insert(key.as_slice(), val.as_slice()))
                            .map_err(|_| err),
                        Command::DBGet(db, key, res_tx) => {
                            res_tx.send(db._get(key.as_ref())).map_err(|_| err)
                        }
                        Command::DBRemove(db, key, res_tx) => {
                            res_tx.send(db._kv_remove(key.as_ref())).map_err(|_| err)
                        }
                        Command::DBMapNew(db, name, expire_ms, res_tx) => {
                            let map =
                                SledStorageMap::_new_expire(name.as_ref().to_vec(), expire_ms, db);
                            res_tx.send(Ok(map)).map_err(|_| err)
                        }
                        Command::DBMapRemove(db, name, res_tx) => {
                            res_tx.send(db._map_remove(name.as_ref())).map_err(|_| err)
                        }
                        Command::DBMapContainsKey(db, key, res_tx) => res_tx
                            .send(db._self_map_contains_key(key.as_ref()))
                            .map_err(|_| err),
                        Command::DBListNew(db, name, expire_ms, res_tx) => {
                            let list =
                                SledStorageList::_new_expire(name.as_ref().to_vec(), expire_ms, db);
                            res_tx.send(Ok(list)).map_err(|_| err)
                        }
                        Command::DBListRemove(db, name, res_tx) => {
                            res_tx.send(db._list_remove(name.as_ref())).map_err(|_| err)
                        }
                        Command::DBListContainsKey(db, key, res_tx) => res_tx
                            .send(db._self_list_contains_key(key.as_ref()))
                            .map_err(|_| err),
                        Command::DBBatchInsert(db, key_vals, res_tx) => {
                            res_tx.send(db._batch_insert(key_vals)).map_err(|_| err)
                        }
                        Command::DBBatchRemove(db, keys, res_tx) => {
                            res_tx.send(db._batch_remove(keys)).map_err(|_| err)
                        }
                        Command::DBCounterIncr(db, key, increment, res_tx) => res_tx
                            .send(db._counter_incr(key.as_ref(), increment))
                            .map_err(|_| err),
                        Command::DBCounterDecr(db, key, increment, res_tx) => res_tx
                            .send(db._counter_decr(key.as_ref(), increment))
                            .map_err(|_| err),
                        Command::DBCounterGet(db, key, res_tx) => {
                            res_tx.send(db._counter_get(key.as_ref())).map_err(|_| err)
                        }
                        Command::DBCounterSet(db, key, val, res_tx) => res_tx
                            .send(db._counter_set(key.as_ref(), val))
                            .map_err(|_| err),
                        Command::DBContainsKey(db, key, res_tx) => res_tx
                            .send(db._self_contains_key(key.as_ref()))
                            .map_err(|_| err),
                        #[cfg(feature = "ttl")]
                        Command::DBExpireAt(db, key, at, res_tx) => res_tx
                            .send(db._expire_at(key.as_ref(), at, KeyType::KV))
                            .map_err(|_| err),
                        #[cfg(feature = "ttl")]
                        Command::DBTtl(db, key, res_tx) => {
                            res_tx.send(db._self_ttl(key.as_ref())).map_err(|_| err)
                        }
                        Command::DBMapPrefixIter(db, res_tx) => {
                            res_tx.send(db._map_scan_prefix()).map_err(|_| err)
                        }
                        Command::DBListPrefixIter(db, res_tx) => {
                            res_tx.send(db._list_scan_prefix()).map_err(|_| err)
                        }
                        Command::DBScanIter(db, pattern, res_tx) => {
                            res_tx.send(db._db_scan_prefix(pattern)).map_err(|_| err)
                        }
                        Command::DBLen(db, res_tx) => res_tx.send(db._kv_len()).map_err(|_| err),
                        Command::DBSize(db, res_tx) => res_tx.send(db._db_size()).map_err(|_| err),

                        Command::MapInsert(map, key, val, res_tx) => {
                            res_tx.send(map._insert(key, val)).map_err(|_| err)
                        }
                        Command::MapGet(map, key, res_tx) => {
                            res_tx.send(map._get(key)).map_err(|_| err)
                        }
                        Command::MapRemove(map, key, res_tx) => {
                            res_tx.send(map._remove(key)).map_err(|_| err)
                        }
                        Command::MapContainsKey(map, key, res_tx) => {
                            res_tx.send(map._contains_key(key)).map_err(|_| err)
                        }
                        #[cfg(feature = "map_len")]
                        Command::MapLen(map, res_tx) => res_tx.send(map._len()).map_err(|_| err),
                        Command::MapIsEmpty(map, res_tx) => {
                            res_tx.send(map._is_empty()).map_err(|_| err)
                        }
                        Command::MapClear(map, res_tx) => {
                            res_tx.send(map._clear()).map_err(|_| err)
                        }
                        Command::MapRemoveAndFetch(map, key, res_tx) => {
                            res_tx.send(map._remove_and_fetch(key)).map_err(|_| err)
                        }
                        Command::MapRemoveWithPrefix(map, key, res_tx) => {
                            res_tx.send(map._remove_with_prefix(key)).map_err(|_| err)
                        }
                        Command::MapBatchInsert(map, key_vals, res_tx) => {
                            res_tx.send(map._batch_insert(key_vals)).map_err(|_| err)
                        }
                        Command::MapBatchRemove(map, keys, res_tx) => {
                            res_tx.send(map._batch_remove(keys)).map_err(|_| err)
                        }
                        #[cfg(feature = "ttl")]
                        Command::MapExpireAt(map, at, res_tx) => {
                            res_tx.send(map._expire_at(at)).map_err(|_| err)
                        }
                        #[cfg(feature = "ttl")]
                        Command::MapTTL(map, res_tx) => res_tx.send(map._ttl()).map_err(|_| err),
                        Command::MapIsExpired(map, res_tx) => {
                            res_tx.send(map._is_expired()).map_err(|_| err)
                        }
                        Command::MapPrefixIter(map, prefix, res_tx) => {
                            res_tx.send(map._prefix_iter(prefix)).map_err(|_| err)
                        }

                        Command::ListPush(list, val, res_tx) => {
                            res_tx.send(list._push(val)).map_err(|_| err)
                        }
                        Command::ListPushs(list, vals, res_tx) => {
                            res_tx.send(list._pushs(vals)).map_err(|_| err)
                        }
                        Command::ListPushLimit(list, data, limit, pop_front_if_limited, res_tx) => {
                            res_tx
                                .send(list._push_limit(data, limit, pop_front_if_limited))
                                .map_err(|_| err)
                        }
                        Command::ListPop(list, res_tx) => res_tx.send(list._pop()).map_err(|_| err),
                        Command::ListAll(list, res_tx) => res_tx.send(list._all()).map_err(|_| err),
                        Command::ListGetIndex(list, idx, res_tx) => {
                            res_tx.send(list._get_index(idx)).map_err(|_| err)
                        }
                        Command::ListLen(list, res_tx) => res_tx.send(list._len()).map_err(|_| err),
                        Command::ListIsEmpty(list, res_tx) => {
                            res_tx.send(list._is_empty()).map_err(|_| err)
                        }
                        Command::ListClear(list, res_tx) => {
                            res_tx.send(list._clear()).map_err(|_| err)
                        }
                        #[cfg(feature = "ttl")]
                        Command::ListExpireAt(list, at, res_tx) => {
                            res_tx.send(list._expire_at(at)).map_err(|_| err)
                        }
                        #[cfg(feature = "ttl")]
                        Command::ListTTL(list, res_tx) => res_tx.send(list._ttl()).map_err(|_| err),
                        Command::ListIsExpired(list, res_tx) => {
                            res_tx.send(list._is_expired()).map_err(|_| err)
                        }
                        Command::ListPrefixIter(list, res_tx) => {
                            res_tx.send(list._prefix_iter()).map_err(|_| err)
                        }

                        Command::IterNext(mut iter, res_tx) => {
                            let item = iter.next();
                            res_tx.send((iter, item)).map_err(|_| err)
                        }
                    };

                    if let Err(e) = snd_res {
                        log::error!("{:?}", e);
                    }

                    active_count1.fetch_sub(1, Ordering::Relaxed);
                }
                log::error!("cmd_rx.recv() is None");
            })
        });

        let db = Self {
            db,
            kv_tree,
            map_tree,
            list_tree,
            expire_key_tree,
            key_expire_tree,
            cmd_tx,
            active_count,
        };

        (cfg.cleanup_f)(&db);

        Ok(db)
    }

    /// Cleans up expired keys (TTL feature)
    #[cfg(feature = "ttl")]
    #[inline]
    pub fn cleanup(&self, limit: usize) -> usize {
        let rmeove = |typ: &KeyType, key: &[u8]| -> Result<()> {
            match typ {
                KeyType::Map => {
                    self._map(key.as_ref())._clear()?;
                }
                KeyType::List => {
                    self._list(key.as_ref())._clear()?;
                }
                KeyType::KV => {
                    self.kv_tree.remove(key.as_ref())?;
                }
            }
            Ok(())
        };
        let mut count = 0;
        let mut expire_at_key_types = Vec::new();
        for item in self.expire_key_tree.iter() {
            if count > limit {
                break;
            }
            let (expire_at_key, key_type) = match item {
                Ok(item) => item,
                Err(e) => {
                    log::error!("{:?}", e);
                    break;
                }
            };

            let (expire_at_bytes, _) = expire_at_key.as_ref().split_at(8);

            let expire_at = match expire_at_bytes.try_into() {
                Ok(at) => i64::from_be_bytes(at),
                Err(e) => {
                    log::error!("{:?}", e);
                    break;
                }
            };

            if expire_at > timestamp_millis() {
                break;
            }

            let key_type = match KeyType::decode(key_type.as_ref()) {
                Ok(key_type) => key_type,
                Err(e) => {
                    log::error!("{:?}", e);
                    break;
                }
            };

            expire_at_key_types.push((expire_at_key, key_type));
            count += 1;
        }

        let mut key_expire_batch = sled::Batch::default();
        let mut expire_key_batch = sled::Batch::default();
        let keys: Vec<(&[u8], &KeyType)> = expire_at_key_types
            .iter()
            .map(|(expire_at_key, key_type)| {
                let (_, key) = expire_at_key.as_ref().split_at(8);
                key_expire_batch.remove(key);
                expire_key_batch.remove(expire_at_key);
                (key, key_type)
            })
            .collect();

        for (key, key_type) in keys {
            if let Err(e) = rmeove(key_type, key) {
                log::error!("{:?}", e);
            }
        }

        // if let Err(e) = self.key_expire_tree.apply_batch(key_expire_batch) {
        //     log::error!("{:?}", e);
        // }
        // if let Err(e) = self.expire_key_tree.apply_batch(expire_key_batch) {
        //     log::error!("{:?}", e);
        // }

        if let Err(e) = self.key_expire_tree.apply_batch(key_expire_batch) {
            log::error!("{:?}", e);
        }
        if let Err(e) = self.expire_key_tree.apply_batch(expire_key_batch) {
            log::error!("{:?}", e);
        }
        count
    }

    /// Cleans up expired key-value pairs (TTL feature)
    #[cfg(feature = "ttl")]
    #[inline]
    pub fn cleanup_kvs(&self, limit: usize) -> usize {
        let mut count = 0;
        let mut expire_at_key_types = Vec::new();
        for item in self.expire_key_tree.iter() {
            if count > limit {
                break;
            }
            let (expire_at_key, key_type) = match item {
                Ok(item) => item,
                Err(e) => {
                    log::error!("{:?}", e);
                    break;
                }
            };

            let (expire_at_bytes, _) = expire_at_key.as_ref().split_at(8);

            let expire_at = match expire_at_bytes.try_into() {
                Ok(at) => i64::from_be_bytes(at),
                Err(e) => {
                    log::error!("{:?}", e);
                    break;
                }
            };

            if expire_at > timestamp_millis() {
                break;
            }

            let key_type = match KeyType::decode(key_type.as_ref()) {
                Ok(key_type) => key_type,
                Err(e) => {
                    log::error!("{:?}", e);
                    break;
                }
            };

            if matches!(key_type, KeyType::KV) {
                expire_at_key_types.push(expire_at_key);
                count += 1;
            }
        }

        let mut key_expire_batch = sled::Batch::default();
        let mut expire_key_batch = sled::Batch::default();
        let mut keys = Batch::default();
        for expire_at_key in expire_at_key_types {
            let (_, key) = expire_at_key.as_ref().split_at(8);
            key_expire_batch.remove(key);
            expire_key_batch.remove(expire_at_key.as_ref());
            keys.remove(key);
        }

        if let Err(e) = self.kv_tree.apply_batch(keys) {
            log::error!("{:?}", e);
        }
        if let Err(e) = self.key_expire_tree.apply_batch(key_expire_batch) {
            log::error!("{:?}", e);
        }
        if let Err(e) = self.expire_key_tree.apply_batch(expire_key_batch) {
            log::error!("{:?}", e);
        }
        count
    }

    /// Returns the count of active commands
    #[inline]
    pub fn active_count(&self) -> isize {
        self.active_count.load(Ordering::Relaxed)
    }

    // #[inline]
    // pub fn map_size(&self) -> usize {
    //     self.map_tree.len()
    // }
    //
    // #[inline]
    // pub fn list_size(&self) -> usize {
    //     self.list_tree.len()
    // }

    /// Creates a map prefix name
    #[inline]
    fn make_map_prefix_name<K>(name: K) -> Key
    where
        K: AsRef<[u8]>,
    {
        [MAP_NAME_PREFIX, name.as_ref(), SEPARATOR].concat()
    }

    /// Creates a map item prefix name
    #[inline]
    fn make_map_item_prefix_name<K>(name: K) -> Key
    where
        K: AsRef<[u8]>,
    {
        [MAP_NAME_PREFIX, name.as_ref(), MAP_KEY_SEPARATOR].concat()
    }

    /// Creates a map count key name
    #[inline]
    fn make_map_count_key_name<K>(name: K) -> Key
    where
        K: AsRef<[u8]>,
    {
        [MAP_NAME_PREFIX, name.as_ref(), MAP_KEY_COUNT_SUFFIX].concat()
    }

    /// Extracts map name from count key
    #[inline]
    fn map_count_key_to_name(key: &[u8]) -> &[u8] {
        key[MAP_NAME_PREFIX.len()..key.as_ref().len() - MAP_KEY_COUNT_SUFFIX.len()].as_ref()
    }

    /// Checks if a key is a map count key
    #[inline]
    fn is_map_count_key(key: &[u8]) -> bool {
        key.starts_with(MAP_NAME_PREFIX) && key.ends_with(MAP_KEY_COUNT_SUFFIX)
    }

    /// Extracts map name from item key
    #[allow(dead_code)]
    #[inline]
    fn map_item_key_to_name(key: &[u8]) -> Option<&[u8]> {
        use super::storage::SplitSubslice;
        if let Some((prefix, _)) = key.split_subslice(MAP_KEY_SEPARATOR) {
            if prefix.starts_with(MAP_NAME_PREFIX) {
                return Some(
                    prefix[MAP_NAME_PREFIX.len()..(prefix.len() - MAP_KEY_SEPARATOR.len())]
                        .as_ref(),
                );
            }
        }
        None
    }

    /// Creates a list prefix
    #[inline]
    fn make_list_prefix<K>(name: K) -> Key
    where
        K: AsRef<[u8]>,
    {
        [LIST_NAME_PREFIX, name.as_ref()].concat()
    }

    /// Creates a list count key
    #[inline]
    fn make_list_count_key(name: &[u8]) -> Vec<u8> {
        [LIST_NAME_PREFIX, name, LIST_KEY_COUNT_SUFFIX].concat()
    }

    /// Extracts list name from count key
    #[inline]
    fn list_count_key_to_name(key: &[u8]) -> &[u8] {
        key[LIST_NAME_PREFIX.len()..key.as_ref().len() - LIST_KEY_COUNT_SUFFIX.len()].as_ref()
    }

    /// Checks if a key is a list count key
    #[inline]
    fn is_list_count_key(key: &[u8]) -> bool {
        key.starts_with(LIST_NAME_PREFIX) && key.ends_with(LIST_KEY_COUNT_SUFFIX)
    }

    /// Checks if a key exists for a specific key type
    #[inline]
    fn _contains_key<K: AsRef<[u8]> + Sync + Send>(
        &self,
        key: K,
        key_type: KeyType,
    ) -> Result<bool> {
        match key_type {
            KeyType::KV => Self::_kv_contains_key(&self.kv_tree, key),
            KeyType::Map => Self::_map_contains_key(&self.map_tree, key),
            KeyType::List => Self::_list_contains_key(&self.list_tree, key),
        }
    }

    /// Checks if a key exists in key-value store
    #[inline]
    fn _kv_contains_key<K: AsRef<[u8]> + Sync + Send>(kv: &Tree, key: K) -> Result<bool> {
        Ok(kv.contains_key(key.as_ref())?)
    }

    /// Checks if a map exists
    #[inline]
    fn _map_contains_key<K: AsRef<[u8]> + Sync + Send>(tree: &Tree, key: K) -> Result<bool> {
        let count_key = SledStorageDB::make_map_count_key_name(key.as_ref());
        Ok(tree.contains_key(count_key)?)
    }

    /// Checks if a list exists
    #[inline]
    fn _list_contains_key<K: AsRef<[u8]> + Sync + Send>(tree: &Tree, name: K) -> Result<bool> {
        let count_key = SledStorageDB::make_list_count_key(name.as_ref());
        Ok(tree.contains_key(count_key)?)
    }

    /// Removes a map
    #[inline]
    fn _map_remove<K>(&self, key: K) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        #[cfg(not(feature = "ttl"))]
        self._map(key.as_ref())._clear()?;
        #[cfg(feature = "ttl")]
        {
            let map = self._map(key.as_ref());
            let map_clear_batch = map._make_clear_batch();
            map.tree().apply_batch(map_clear_batch)?;
            self._remove_expire_key(key.as_ref())?;
        }
        Ok(())
    }

    /// Removes a list
    #[inline]
    fn _list_remove<K>(&self, key: K) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        #[cfg(not(feature = "ttl"))]
        self._list(key.as_ref())._clear()?;
        #[cfg(feature = "ttl")]
        {
            let list = self._list(key.as_ref());
            let list_clear_batch = list._make_clear_batch();
            list.tree().apply_batch(list_clear_batch)?;
            self._remove_expire_key(key.as_ref())?;
        }
        Ok(())
    }

    /// Removes a key-value pair
    #[inline]
    fn _kv_remove<K>(&self, key: K) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        #[cfg(not(feature = "ttl"))]
        self.kv_tree.remove(key.as_ref())?;
        #[cfg(feature = "ttl")]
        {
            self.kv_tree.remove(key.as_ref())?;
            self._remove_expire_key(key.as_ref())?;
        }
        Ok(())
    }

    /// Removes expiration key (TTL feature)
    #[cfg(feature = "ttl")]
    #[inline]
    fn _remove_expire_key(&self, key: &[u8]) -> Result<()> {
        if let Some(expire_at_bytes) = self.key_expire_tree.get(key)? {
            self.key_expire_tree.remove(key)?;
            let expire_key = [expire_at_bytes.as_ref(), key].concat();
            self.expire_key_tree.remove(expire_key.as_slice())?;
        }
        Ok(())
    }

    /// Checks if a key is expired
    #[inline]
    fn _is_expired<K, F>(&self, _key: K, _contains_key_f: F) -> Result<bool>
    where
        K: AsRef<[u8]> + Sync + Send,
        F: Fn(&[u8]) -> Result<bool>,
    {
        #[cfg(feature = "ttl")]
        {
            if let Some((expire_at, _)) = self._ttl_at(_key, _contains_key_f)? {
                Ok(timestamp_millis() >= expire_at)
            } else {
                Ok(true)
            }
        }
        #[cfg(not(feature = "ttl"))]
        Ok(false)
    }

    /// Gets time-to-live for a key
    #[inline]
    fn _ttl<K, F>(
        &self,
        key: K,
        contains_key_f: F,
    ) -> Result<Option<(TimestampMillis, Option<IVec>)>>
    where
        K: AsRef<[u8]> + Sync + Send,
        F: Fn(&[u8]) -> Result<bool>,
    {
        Ok(self
            ._ttl_at(key, contains_key_f)?
            .map(|(expire_at, at_bytes)| (expire_at - timestamp_millis(), at_bytes)))
    }

    /// Gets expiration time for a key
    #[inline]
    fn _ttl_at<K, F>(
        &self,
        c_key: K,
        contains_key_f: F,
    ) -> Result<Option<(TimestampMillis, Option<IVec>)>>
    where
        K: AsRef<[u8]> + Sync + Send,
        F: Fn(&[u8]) -> Result<bool>,
    {
        let ttl_res = match self.key_expire_tree.get(c_key.as_ref()) {
            Ok(Some(at_bytes)) => {
                if contains_key_f(c_key.as_ref())? {
                    Ok(Some((
                        TimestampMillis::from_be_bytes(at_bytes.as_ref().try_into()?),
                        Some(at_bytes),
                    )))
                } else {
                    Ok(None)
                }
            }
            Ok(None) => {
                if contains_key_f(c_key.as_ref())? {
                    Ok(Some((TimestampMillis::MAX, None)))
                } else {
                    Ok(None)
                }
            }
            Err(e) => Err(anyhow!(e)),
        }?;
        Ok(ttl_res)
    }

    /// Inserts a key-value pair (简化版: 跳过事务, 单独清理 expire)
    #[inline]
    fn _insert(&self, key: &[u8], val: &[u8]) -> Result<()> {
        self.kv_tree.insert(key, val)?;
        if let Some(expire_at_bytes) = self.key_expire_tree.get(key)? {
            let expire_key = [expire_at_bytes.as_ref(), key].concat();
            self.expire_key_tree.remove(expire_key.as_slice())?;
            self.key_expire_tree.remove(key)?;
        }
        Ok(())
    }

    /// Gets a value by key
    #[inline]
    fn _get(&self, key: &[u8]) -> Result<Option<IVec>> {
        let res = if self._is_expired(key.as_ref(), |k| Self::_kv_contains_key(&self.kv_tree, k))? {
            None
        } else {
            self.kv_tree.get(key)?
        };
        Ok(res)
    }

    /// Checks if a map key exists
    #[inline]
    fn _self_map_contains_key(&self, key: &[u8]) -> Result<bool> {
        #[cfg(feature = "ttl")]
        {
            if self._is_expired(key, |k| Self::_map_contains_key(&self.map_tree, k))? {
                Ok(false)
            } else {
                //Self::_map_contains_key(&self.map_tree, key)
                Ok(true)
            }
        }

        #[cfg(not(feature = "ttl"))]
        Self::_map_contains_key(&self.map_tree, key)
    }

    /// Checks if a list key exists
    #[inline]
    fn _self_list_contains_key(&self, key: &[u8]) -> Result<bool> {
        #[cfg(feature = "ttl")]
        {
            let this = self;
            if this._is_expired(key, |k| Self::_list_contains_key(&self.list_tree, k))? {
                Ok(false)
            } else {
                // Self::_list_contains_key(&this.list_tree, key)
                Ok(true)
            }
        }

        #[cfg(not(feature = "ttl"))]
        Self::_list_contains_key(&self.list_tree, key)
    }

    /// Batch insert key-value pairs
    #[inline]
    fn _batch_insert(&self, key_vals: Vec<(Key, IVec)>) -> Result<()> {
        if key_vals.is_empty() {
            return Ok(());
        }

        let mut batch = Batch::default();
        for (k, v) in key_vals.iter() {
            batch.insert(k.as_slice(), v.as_ref());
        }

        let this = self;
        #[cfg(not(feature = "ttl"))]
        this.kv_tree.apply_batch(batch)?;

        #[cfg(feature = "ttl")]
        {
            let mut remove_key_expire_batch = Batch::default();
            let mut remove_expire_key_batch = Batch::default();
            for (k, _) in key_vals.iter() {
                if let Some((expire_at, Some(expire_at_bytes))) =
                    this._ttl(k.as_slice(), |k| Self::_kv_contains_key(&self.kv_tree, k))?
                {
                    if expire_at <= 0 {
                        remove_key_expire_batch.remove(k.as_slice());
                        let expire_key = [expire_at_bytes.as_ref(), k.as_slice()].concat();
                        remove_expire_key_batch.remove(expire_key.as_slice())
                    }
                }
            }

            self.key_expire_tree.apply_batch(remove_key_expire_batch)?;
            self.expire_key_tree.apply_batch(remove_expire_key_batch)?;
            self.kv_tree.apply_batch(batch)?;
        }
        Ok(())
    }

    /// Batch remove keys
    #[inline]
    fn _batch_remove(&self, keys: Vec<Key>) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }

        let mut batch = Batch::default();
        for k in keys.iter() {
            batch.remove(k.as_slice());
        }
        #[cfg(not(feature = "ttl"))]
        self.kv_tree.apply_batch(batch)?;

        #[cfg(feature = "ttl")]
        {
            let mut remove_key_expire_batch = Batch::default();
            let mut remove_expire_key_batch = Batch::default();
            for k in keys.iter() {
                if let Some(expire_at_bytes) = self.key_expire_tree.get(k)? {
                    remove_key_expire_batch.remove(k.as_slice());
                    let expire_key = [expire_at_bytes.as_ref(), k.as_slice()].concat();
                    remove_expire_key_batch.remove(expire_key.as_slice())
                }
            }
            self.key_expire_tree.apply_batch(remove_key_expire_batch)?;
            self.expire_key_tree.apply_batch(remove_expire_key_batch)?;
            self.kv_tree.apply_batch(batch)?;
        }

        Ok(())
    }

    /// Increments a counter
    #[inline]
    fn _counter_incr(&self, key: &[u8], increment: isize) -> Result<()> {
        self.kv_tree.fetch_and_update(key, |old: Option<&[u8]>| {
            let number = match old {
                Some(bytes) => {
                    if let Ok(array) = bytes.try_into() {
                        let number = isize::from_be_bytes(array);
                        number + increment
                    } else {
                        increment
                    }
                }
                None => increment,
            };
            Some(number.to_be_bytes().to_vec())
        })?;
        Ok(())
    }

    /// Decrements a counter
    #[inline]
    fn _counter_decr(&self, key: &[u8], decrement: isize) -> Result<()> {
        self.kv_tree.fetch_and_update(key, |old: Option<&[u8]>| {
            let number = match old {
                Some(bytes) => {
                    if let Ok(array) = bytes.try_into() {
                        let number = isize::from_be_bytes(array);
                        number - decrement
                    } else {
                        -decrement
                    }
                }
                None => -decrement,
            };
            Some(number.to_be_bytes().to_vec())
        })?;
        Ok(())
    }

    /// Gets counter value
    #[inline]
    fn _counter_get(&self, key: &[u8]) -> Result<Option<isize>> {
        let this = self;
        if this._is_expired(key, |k| Self::_kv_contains_key(&self.kv_tree, k))? {
            Ok(None)
        } else if let Some(v) = this.kv_tree.get(key)? {
            Ok(Some(isize::from_be_bytes(v.as_ref().try_into()?)))
        } else {
            Ok(None)
        }
    }

    /// Sets counter value
    #[inline]
    fn _counter_set(&self, key: &[u8], val: isize) -> Result<()> {
        let val = val.to_be_bytes().to_vec();

        #[cfg(not(feature = "ttl"))]
        self.kv_tree.insert(key, val.as_slice())?;
        #[cfg(feature = "ttl")]
        {
            self._remove_expire_key(key)?;
            self.kv_tree.insert(key, val.as_slice())?;
        }
        Ok(())
    }

    /// Checks if a key exists in key-value store
    #[inline]
    fn _self_contains_key(&self, key: &[u8]) -> Result<bool> {
        #[cfg(feature = "ttl")]
        {
            let this = self;
            if this._is_expired(key, |k| Self::_kv_contains_key(&self.kv_tree, k))? {
                Ok(false)
            } else {
                // this._contains_key(key, KeyType::KV)
                Ok(true)
            }
        }
        #[cfg(not(feature = "ttl"))]
        Self::_kv_contains_key(&self.kv_tree, key)
    }

    /// Sets expiration time for a key (TTL feature)
    #[inline]
    #[cfg(feature = "ttl")]
    fn _expire_at(&self, key: &[u8], at: TimestampMillis, key_type: KeyType) -> Result<bool> {
        if self._contains_key(key, key_type)? {
            let res = self._non_tx_expire_at(key, at, key_type)?;
            Ok(res)
        } else {
            Ok(false)
        }
    }

    /// Non-transactional: sets expiration time using direct tree ops
    #[cfg(feature = "ttl")]
    #[inline]
    fn _non_tx_expire_at(
        &self,
        key: &[u8],
        at: TimestampMillis,
        key_type: KeyType,
    ) -> Result<bool> {
        let at_bytes = at.to_be_bytes();
        self.key_expire_tree.insert(key, at_bytes.as_slice())?;
        let res = self
            .expire_key_tree
            .insert([at_bytes.as_ref(), key].concat(), key_type.encode())?;
        Ok(res.is_none())
    }

    /// Gets time-to-live for a key (TTL feature)
    #[inline]
    #[cfg(feature = "ttl")]
    fn _self_ttl(&self, key: &[u8]) -> Result<Option<TimestampMillis>> {
        Ok(self
            ._ttl(key, |k| Self::_kv_contains_key(&self.kv_tree, k))?
            .and_then(|(ttl, _)| if ttl > 0 { Some(ttl) } else { None }))
    }

    /// Creates an iterator for map prefixes
    #[inline]
    fn _map_scan_prefix(&self) -> sled::Iter {
        self.map_tree.scan_prefix(MAP_NAME_PREFIX)
    }

    /// Creates an iterator for list prefixes
    #[inline]
    fn _list_scan_prefix(&self) -> sled::Iter {
        self.list_tree.scan_prefix(LIST_NAME_PREFIX)
    }

    /// Creates an iterator for database scan with pattern
    #[inline]
    fn _db_scan_prefix(&self, pattern: Vec<u8>) -> sled::Iter {
        let mut last_esc_char = false;
        let mut has_esc_char = false;
        let start_pattern = pattern
            .splitn(2, |x| {
                if !last_esc_char && (*x == b'*' || *x == b'?') {
                    true
                } else {
                    last_esc_char = *x == b'\\';
                    if last_esc_char && !has_esc_char {
                        has_esc_char = true;
                    }
                    false
                }
            })
            .next();
        let start_pattern = if has_esc_char {
            start_pattern.map(|start_pattern| {
                Cow::Owned(
                    start_pattern
                        .replace(b"\\*", b"*")
                        .as_slice()
                        .replace(b"\\?", b"?"),
                )
            })
        } else {
            start_pattern.map(Cow::Borrowed)
        };
        let iter = if let Some(start_pattern) = start_pattern {
            self.kv_tree.scan_prefix(start_pattern.as_ref())
        } else {
            self.kv_tree.iter()
        };
        iter
    }

    /// Gets number of key-value pairs
    #[inline]
    fn _kv_len(&self) -> usize {
        #[cfg(feature = "ttl")]
        {
            // let limit = 500;
            // loop {
            //     if self.cleanup_kvs(limit) < limit {
            //         break;
            //     }
            // }
        }
        self.kv_tree.len()
    }

    /// Gets total database size
    #[inline]
    fn _db_size(&self) -> usize {
        self.db.len() + self.kv_tree.len() + self.map_tree.len() + self.list_tree.len()
    }

    /// Sends a command to the background processor
    #[inline]
    async fn cmd_send(&self, cmd: Command) -> Result<()> {
        self.active_count.fetch_add(1, Ordering::Relaxed);
        if let Err(e) = self.cmd_tx.send(cmd).await {
            self.active_count.fetch_sub(1, Ordering::Relaxed);
            Err(anyhow!(e))
        } else {
            Ok(())
        }
    }

    /// Gets a map handle
    #[inline]
    fn _map<N: AsRef<[u8]>>(&self, name: N) -> SledStorageMap {
        SledStorageMap::_new(name.as_ref().to_vec(), self.clone())
    }

    /// Gets a list handle
    #[inline]
    fn _list<V: AsRef<[u8]>>(&self, name: V) -> SledStorageList {
        SledStorageList::_new(name.as_ref().to_vec(), self.clone())
    }
}

#[async_trait]
impl StorageDB for SledStorageDB {
    type MapType = SledStorageMap;
    type ListType = SledStorageList;

    /// Creates or gets a map with optional expiration
    #[inline]
    async fn map<N: AsRef<[u8]> + Sync + Send>(
        &self,
        name: N,
        expire: Option<TimestampMillis>,
    ) -> Self::MapType {
        SledStorageMap::new_expire(name.as_ref().to_vec(), expire, self.clone()).await
    }

    /// Removes a map
    #[inline]
    async fn map_remove<K>(&self, name: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let (tx, rx) = oneshot::channel();
        self.cmd_send(Command::DBMapRemove(self.clone(), name.as_ref().into(), tx))
            .await?;
        rx.await??;
        Ok(())
    }

    /// Checks if a map exists
    #[inline]
    async fn map_contains_key<K: AsRef<[u8]> + Sync + Send>(&self, key: K) -> Result<bool> {
        let (tx, rx) = oneshot::channel();
        self.cmd_send(Command::DBMapContainsKey(
            self.clone(),
            key.as_ref().into(),
            tx,
        ))
        .await?;
        Ok(rx.await??)
    }

    /// Creates or gets a list with optional expiration
    #[inline]
    async fn list<V: AsRef<[u8]> + Sync + Send>(
        &self,
        name: V,
        expire: Option<TimestampMillis>,
    ) -> Self::ListType {
        SledStorageList::new_expire(name.as_ref().to_vec(), expire, self.clone()).await
    }

    /// Removes a list
    #[inline]
    async fn list_remove<K>(&self, name: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let (tx, rx) = oneshot::channel();
        self.cmd_send(Command::DBListRemove(
            self.clone(),
            name.as_ref().into(),
            tx,
        ))
        .await?;
        rx.await??;
        Ok(())
    }

    /// Checks if a list exists
    #[inline]
    async fn list_contains_key<K: AsRef<[u8]> + Sync + Send>(&self, key: K) -> Result<bool> {
        let (tx, rx) = oneshot::channel();
        self.cmd_send(Command::DBListContainsKey(
            self.clone(),
            key.as_ref().into(),
            tx,
        ))
        .await?;
        Ok(rx.await??)
    }

    /// Inserts a key-value pair
    #[inline]
    async fn insert<K, V>(&self, key: K, val: &V) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: serde::ser::Serialize + Sync + Send,
    {
        let val = postcard::to_stdvec(&val)?;
        let (tx, rx) = oneshot::channel();
        self.cmd_send(Command::DBInsert(
            self.clone(),
            key.as_ref().to_vec(),
            val,
            tx,
        ))
        .await?;
        rx.await??;
        Ok(())
    }

    /// Gets a value by key
    #[inline]
    async fn get<K, V>(&self, key: K) -> Result<Option<V>>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: DeserializeOwned + Sync + Send,
    {
        let (tx, rx) = oneshot::channel();
        self.cmd_send(Command::DBGet(self.clone(), key.as_ref().into(), tx))
            .await?;
        match rx.await?? {
            Some(v) => Ok(Some(postcard::from_bytes::<V>(v.as_ref())?)),
            None => Ok(None),
        }
    }

    /// Removes a key-value pair
    #[inline]
    async fn remove<K>(&self, key: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let (tx, rx) = oneshot::channel();
        self.cmd_send(Command::DBRemove(self.clone(), key.as_ref().into(), tx))
            .await?;
        rx.await??;
        Ok(())
    }

    /// Batch inserts key-value pairs
    #[inline]
    async fn batch_insert<V>(&self, key_vals: Vec<(Key, V)>) -> Result<()>
    where
        V: Serialize + Sync + Send,
    {
        if key_vals.is_empty() {
            return Ok(());
        }

        let key_vals = key_vals
            .into_iter()
            .map(|(k, v)| {
                postcard::to_stdvec(&v)
                    .map(|v| (k, v.into()))
                    .map_err(|e| anyhow!(e))
            })
            .collect::<Result<Vec<_>>>()?;

        let (tx, rx) = oneshot::channel();
        self.cmd_send(Command::DBBatchInsert(self.clone(), key_vals, tx))
            .await?;
        Ok(rx.await??)
    }

    /// Batch removes keys
    #[inline]
    async fn batch_remove(&self, keys: Vec<Key>) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }

        let (tx, rx) = oneshot::channel();
        self.cmd_send(Command::DBBatchRemove(self.clone(), keys, tx))
            .await?;
        Ok(rx.await??)
    }

    /// Increments a counter
    #[inline]
    async fn counter_incr<K>(&self, key: K, increment: isize) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let (tx, rx) = oneshot::channel();
        self.cmd_send(Command::DBCounterIncr(
            self.clone(),
            key.as_ref().into(),
            increment,
            tx,
        ))
        .await?;
        Ok(rx.await??)
    }

    /// Decrements a counter
    #[inline]
    async fn counter_decr<K>(&self, key: K, decrement: isize) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let (tx, rx) = oneshot::channel();
        self.cmd_send(Command::DBCounterDecr(
            self.clone(),
            key.as_ref().into(),
            decrement,
            tx,
        ))
        .await?;
        Ok(rx.await??)
    }

    /// Gets counter value
    #[inline]
    async fn counter_get<K>(&self, key: K) -> Result<Option<isize>>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let (tx, rx) = oneshot::channel();
        self.cmd_send(Command::DBCounterGet(self.clone(), key.as_ref().into(), tx))
            .await?;
        Ok(rx.await??)
    }

    /// Sets counter value
    #[inline]
    async fn counter_set<K>(&self, key: K, val: isize) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let (tx, rx) = oneshot::channel();
        self.cmd_send(Command::DBCounterSet(
            self.clone(),
            key.as_ref().into(),
            val,
            tx,
        ))
        .await?;
        Ok(rx.await??)
    }

    /// Checks if a key exists
    #[inline]
    async fn contains_key<K: AsRef<[u8]> + Sync + Send>(&self, key: K) -> Result<bool> {
        let (tx, rx) = oneshot::channel();
        self.cmd_send(Command::DBContainsKey(
            self.clone(),
            key.as_ref().into(),
            tx,
        ))
        .await?;
        Ok(rx.await??)
    }

    /// Gets number of key-value pairs (if enabled)
    #[inline]
    #[cfg(feature = "len")]
    async fn len(&self) -> Result<usize> {
        let (tx, rx) = oneshot::channel();
        self.cmd_send(Command::DBLen(self.clone(), tx)).await?;
        Ok(rx.await?)
    }

    /// Gets total database size
    #[inline]
    async fn db_size(&self) -> Result<usize> {
        let (tx, rx) = oneshot::channel();
        self.cmd_send(Command::DBSize(self.clone(), tx)).await?;
        Ok(rx.await?)
    }

    /// Sets expiration time for a key (TTL feature)
    #[inline]
    #[cfg(feature = "ttl")]
    async fn expire_at<K>(&self, key: K, at: TimestampMillis) -> Result<bool>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let (tx, rx) = oneshot::channel();
        self.cmd_send(Command::DBExpireAt(
            self.clone(),
            key.as_ref().into(),
            at,
            tx,
        ))
        .await?;
        Ok(rx.await??)
    }

    /// Sets time-to-live for a key (TTL feature)
    #[inline]
    #[cfg(feature = "ttl")]
    async fn expire<K>(&self, key: K, dur: TimestampMillis) -> Result<bool>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let at = timestamp_millis() + dur;
        self.expire_at(key, at).await
    }

    /// Gets time-to-live for a key (TTL feature)
    #[inline]
    #[cfg(feature = "ttl")]
    async fn ttl<K>(&self, key: K) -> Result<Option<TimestampMillis>>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let (tx, rx) = oneshot::channel();
        self.cmd_send(Command::DBTtl(self.clone(), key.as_ref().into(), tx))
            .await?;
        Ok(rx.await??)
    }

    /// Iterates over all maps
    #[inline]
    async fn map_iter<'a>(
        &'a mut self,
    ) -> Result<Box<dyn AsyncIterator<Item = Result<StorageMap>> + Send + 'a>> {
        let (tx, rx) = oneshot::channel();
        self.cmd_send(Command::DBMapPrefixIter(self.clone(), tx))
            .await?;
        let iter = rx.await?;
        let iter = Box::new(AsyncMapIter::new(self, iter));
        Ok(iter)
    }

    /// Iterates over all lists
    #[inline]
    async fn list_iter<'a>(
        &'a mut self,
    ) -> Result<Box<dyn AsyncIterator<Item = Result<StorageList>> + Send + 'a>> {
        let (tx, rx) = oneshot::channel();
        self.cmd_send(Command::DBListPrefixIter(self.clone(), tx))
            .await?;
        let iter = rx.await?;
        let iter = Box::new(AsyncListIter {
            db: self,
            iter: Some(iter),
        });
        Ok(iter)
    }

    /// Scans keys matching pattern
    async fn scan<'a, P>(
        &'a mut self,
        pattern: P,
    ) -> Result<Box<dyn AsyncIterator<Item = Result<Key>> + Send + 'a>>
    where
        P: AsRef<[u8]> + Send + Sync,
    {
        let pattern = pattern.as_ref();
        let (tx, rx) = oneshot::channel();
        self.cmd_send(Command::DBScanIter(self.clone(), pattern.to_vec(), tx))
            .await?;
        let iter = rx.await?;
        let pattern = Pattern::from(pattern);
        let iter = Box::new(AsyncDbKeyIter {
            db: self,
            pattern,
            iter: Some(iter),
        });
        Ok(iter)
    }

    /// Gets database information
    #[inline]
    async fn info(&self) -> Result<Value> {
        let active_count = self.active_count.load(Ordering::Relaxed);
        // let this = self.clone();
        Ok(spawn_blocking(move || {
            // let size_on_disk = this.db.size_on_disk().unwrap_or_default();
            // let db_size = this.db_size();
            // let map_size = this.map_size();
            // let list_size = this.list_size();

            // let limit = 20;

            // let mut db_keys = Vec::new();
            // for (i, key) in this.db.iter().keys().enumerate() {
            //     let key = key
            //         .map(|k| String::from_utf8_lossy(k.as_ref()).to_string())
            //         .unwrap_or_else(|e| e.to_string());
            //     db_keys.push(key);
            //     if i > limit {
            //         break;
            //     }
            // }

            // let mut map_names = Vec::new();
            // for (i, key) in this.map_tree.iter().keys().enumerate() {
            //     let key = key
            //         .map(|k| String::from_utf8_lossy(k.as_ref()).to_string())
            //         .unwrap_or_else(|e| e.to_string());
            //     map_names.push(key);
            //     if i > limit {
            //         break;
            //     }
            // }

            // let mut list_names = Vec::new();
            // for (i, key) in this.list_tree.iter().keys().enumerate() {
            //     let key = key
            //         .map(|k| String::from_utf8_lossy(k.as_ref()).to_string())
            //         .unwrap_or_else(|e| e.to_string());
            //     list_names.push(key);
            //     if i > limit {
            //         break;
            //     }
            // }

            serde_json::json!({
                "storage_engine": "Sled",
                "active_count": active_count,
                // "db_size": db_size,
                // "map_size": map_size,
                // "list_size": list_size,
                // "size_on_disk": size_on_disk,
                // "db_keys": db_keys,
                // "map_names": map_names,
                // "list_names": list_names,
            })
        })
        .await?)
    }
}

// ---------------------------------------------------------------------------
// Inherent raw methods on SledStorageDB — NOT part of the StorageDB trait.
// Used internally (e.g., by circuit-breaker) to bypass serde overhead.
// ---------------------------------------------------------------------------
#[cfg(feature = "circuit-breaker")]
impl SledStorageDB {
    /// Inserts raw bytes directly, skipping postcard serialization.
    #[inline]
    pub(crate) async fn insert_raw(&self, key: &[u8], val: &[u8]) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.cmd_send(Command::DBInsert(
            self.clone(),
            key.to_vec(),
            val.to_vec(),
            tx,
        ))
        .await?;
        rx.await??;
        Ok(())
    }

    /// Retrieves raw bytes directly, skipping postcard deserialization.
    #[inline]
    pub(crate) async fn get_raw(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let (tx, rx) = oneshot::channel();
        self.cmd_send(Command::DBGet(self.clone(), key.into(), tx))
            .await?;
        match rx.await?? {
            Some(v) => Ok(Some(v.to_vec())),
            None => Ok(None),
        }
    }

    /// Batch insert of raw key-value pairs, skipping serialization.
    #[inline]
    pub(crate) async fn batch_insert_raw(&self, key_vals: Vec<(Vec<u8>, Vec<u8>)>) -> Result<()> {
        if key_vals.is_empty() {
            return Ok(());
        }
        let key_vals = key_vals
            .into_iter()
            .map(|(k, v)| (k, v.into()))
            .collect::<Vec<(Key, IVec)>>();
        let (tx, rx) = oneshot::channel();
        self.cmd_send(Command::DBBatchInsert(self.clone(), key_vals, tx))
            .await?;
        rx.await??;
        Ok(())
    }
}

/// Map structure for key-value storage within a namespace
#[derive(Clone)]
pub struct SledStorageMap {
    /// Map name
    name: Key,
    /// Prefix for map keys
    map_prefix_name: Key,
    /// Prefix for map items
    map_item_prefix_name: Key,
    /// Key for map count
    map_count_key_name: Key,
    /// Flag indicating if map is empty
    empty: Arc<AtomicBool>,
    /// Database handle
    pub(crate) db: SledStorageDB,
}

impl SledStorageMap {
    /// Creates a new map with optional expiration.
    ///
    /// Errors during construction (e.g. command channel full, sled I/O) are
    /// caught, logged, and a bare map handle is returned. Real errors are
    /// deferred to operations on the returned map.
    #[inline]
    async fn new_expire(name: Key, expire_ms: Option<TimestampMillis>, db: SledStorageDB) -> Self {
        let (tx, rx) = oneshot::channel();
        if db
            .cmd_send(Command::DBMapNew(
                db.clone(),
                name.clone().into(),
                expire_ms,
                tx,
            ))
            .await
            .is_err()
        {
            log::warn!(
                "sled command channel full, creating map '{}' without health check",
                String::from_utf8_lossy(&name)
            );
            return Self::_new(name, db);
        }
        match rx.await {
            Ok(Ok(m)) => m,
            Ok(Err(e)) => {
                log::warn!(
                    "sled command returned error for map '{}': {e}",
                    String::from_utf8_lossy(&name)
                );
                Self::_new(name, db)
            }
            Err(_) => {
                log::warn!(
                    "sled command dropped, creating map '{}' without health check",
                    String::from_utf8_lossy(&name)
                );
                Self::_new(name, db)
            }
        }
    }

    /// Internal method to create map with expiration.
    /// Errors from sled I/O are caught and logged — the map handle is always
    /// returned.
    #[inline]
    fn _new_expire(name: Key, _expire_ms: Option<TimestampMillis>, db: SledStorageDB) -> Self {
        let m = Self::_new(name, db);
        match m._is_empty() {
            Ok(empty) => m.empty.store(empty, Ordering::SeqCst),
            Err(e) => {
                log::warn!(
                    "sled map '{}' is_empty check failed: {e}",
                    String::from_utf8_lossy(m.name())
                );
            }
        }
        #[cfg(feature = "ttl")]
        if let Some(expire_ms) = _expire_ms.as_ref() {
            if let Err(e) = m._expire_at(timestamp_millis() + *expire_ms) {
                log::warn!(
                    "sled map '{}' expire_at failed: {e}",
                    String::from_utf8_lossy(m.name())
                );
            }
        }
        m
    }

    /// Internal method to create map
    #[inline]
    fn _new(name: Key, db: SledStorageDB) -> Self {
        let map_prefix_name = SledStorageDB::make_map_prefix_name(name.as_slice());
        let map_item_prefix_name = SledStorageDB::make_map_item_prefix_name(name.as_slice());
        let map_count_key_name = SledStorageDB::make_map_count_key_name(name.as_slice());
        SledStorageMap {
            name,
            map_prefix_name,
            map_item_prefix_name,
            map_count_key_name,
            empty: Arc::new(AtomicBool::new(true)),
            db,
        }
    }

    /// Gets the underlying tree
    #[inline]
    fn tree(&self) -> &sled::Tree {
        &self.db.map_tree
    }

    /// Creates a full item key
    #[inline]
    fn make_map_item_key<K: AsRef<[u8]>>(&self, key: K) -> Key {
        [self.map_item_prefix_name.as_ref(), key.as_ref()].concat()
    }

    /// Gets map length (if enabled)
    #[cfg(feature = "map_len")]
    #[inline]
    fn _len_get(&self) -> Result<isize> {
        self._counter_get(self.map_count_key_name.as_slice())
    }

    /// Non-transactional: increments a counter on a direct Tree
    #[inline]
    fn _non_tx_counter_inc(tree: &Tree, key: &[u8]) -> Result<isize> {
        let val = match tree.get(key)? {
            Some(data) => {
                if let Ok(array) = data.as_ref().try_into() {
                    isize::from_be_bytes(array) + 1
                } else {
                    1
                }
            }
            None => 1,
        };
        tree.insert(key, &val.to_be_bytes())?;
        Ok(val)
    }

    /// Non-transactional: decrements a counter on a direct Tree
    /// (removes the key when counter reaches 0)
    #[inline]
    fn _non_tx_counter_dec(tree: &Tree, key: &[u8]) -> Result<isize> {
        let val = match tree.get(key)? {
            Some(data) => {
                if let Ok(array) = data.as_ref().try_into() {
                    let n = isize::from_be_bytes(array) - 1;
                    if n > 0 {
                        n
                    } else {
                        tree.remove(key)?;
                        return Ok(0);
                    }
                } else {
                    0
                }
            }
            None => 0,
        };
        tree.insert(key, &val.to_be_bytes())?;
        Ok(val)
    }

    /// Gets counter value
    #[inline]
    fn _counter_get<K: AsRef<[u8]>>(&self, key: K) -> Result<isize> {
        if let Some(v) = self.tree().get(key)? {
            Ok(isize::from_be_bytes(v.as_ref().try_into()?))
        } else {
            Ok(0)
        }
    }

    /// Initializes counter if not present
    #[inline]
    fn _counter_init(&self) -> Result<()> {
        let tree = self.tree();
        if !tree.contains_key(self.map_count_key_name.as_slice())? {
            tree.insert(
                self.map_count_key_name.as_slice(),
                0isize.to_be_bytes().as_slice(),
            )?;
        }
        Ok(())
    }

    /// Clears the map
    #[inline]
    fn _clear(&self) -> Result<()> {
        let batch = self._make_clear_batch();
        self.tree().apply_batch(batch)?;
        self.empty.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// Creates batch for clearing map
    #[inline]
    fn _make_clear_batch(&self) -> Batch {
        let mut batch = Batch::default();
        //clear key-value
        for item in self.tree().scan_prefix(self.map_prefix_name.as_slice()) {
            match item {
                Ok((key, _)) => {
                    batch.remove(key);
                }
                Err(e) => {
                    log::warn!("{:?}", e);
                }
            }
        }
        batch
    }

    /// Inserts a key-value pair into the map
    #[inline]
    fn _insert(&self, key: IVec, val: IVec) -> Result<()> {
        let item_key = self.make_map_item_key(key.as_ref());
        #[cfg(feature = "map_len")]
        {
            let count_key = self.map_count_key_name.as_slice();
            if self
                .tree()
                .insert(item_key.as_slice(), val.as_ref())?
                .is_none()
            {
                Self::_non_tx_counter_inc(self.tree(), count_key)?;
            }
        }
        #[cfg(not(feature = "map_len"))]
        {
            if self.empty.load(Ordering::SeqCst) {
                self._counter_init()?;
                self.empty.store(false, Ordering::SeqCst)
            }
            self.tree().insert(item_key.as_slice(), val.as_ref())?;
        }

        #[cfg(feature = "ttl")]
        {
            if self.db._is_expired(self.name.as_slice(), |k| {
                SledStorageDB::_map_contains_key(self.tree(), k)
            })? {
                self.db._remove_expire_key(self.name.as_slice())?;
            }
        }

        Ok(())
    }

    /// Gets a value from the map
    #[inline]
    fn _get(&self, key: IVec) -> Result<Option<IVec>> {
        let this = self;
        let item_key = self.make_map_item_key(key.as_ref());
        let res = if !this.db._is_expired(this.name.as_slice(), |k| {
            SledStorageDB::_map_contains_key(this.tree(), k)
        })? {
            this.tree().get(item_key).map_err(|e| anyhow!(e))?
        } else {
            None
        };
        Ok(res)
    }

    /// Removes a key from the map
    #[inline]
    fn _remove(&self, key: IVec) -> Result<()> {
        let tree = self.tree();
        let key = self.make_map_item_key(key.as_ref());

        #[cfg(feature = "map_len")]
        {
            let count_key = self.map_count_key_name.as_slice();
            if tree.remove(key.as_slice())?.is_some() {
                Self::_non_tx_counter_dec(tree, count_key)?;
            }
        }

        #[cfg(not(feature = "map_len"))]
        {
            tree.remove(key.as_slice())?;
        }

        Ok(())
    }

    /// Checks if key exists in map
    #[inline]
    fn _contains_key(&self, key: IVec) -> Result<bool> {
        let key = self.make_map_item_key(key.as_ref());
        Ok(self.tree().contains_key(key)?)
    }

    /// Gets map length (if enabled)
    #[cfg(feature = "map_len")]
    #[inline]
    fn _len(&self) -> Result<usize> {
        let this = self;
        let len = {
            if this.db._is_expired(this.name.as_slice(), |k| {
                SledStorageDB::_map_contains_key(this.tree(), k)
            })? {
                Ok(0)
            } else {
                this._len_get()
            }
        }?;
        Ok(len as usize)
    }

    /// Checks if map is empty
    #[inline]
    fn _is_empty(&self) -> Result<bool> {
        let this = self;
        let res = {
            if this.db._is_expired(this.name.as_slice(), |k| {
                SledStorageDB::_map_contains_key(this.tree(), k)
            })? {
                true
            } else {
                self.tree()
                    .scan_prefix(self.map_item_prefix_name.as_slice())
                    .next()
                    .is_none()
            }
        };
        Ok(res)
    }

    /// Removes and returns a value
    #[inline]
    fn _remove_and_fetch(&self, key: IVec) -> Result<Option<IVec>> {
        let key = self.make_map_item_key(key.as_ref());
        let this = self;
        let removed = {
            if this.db._is_expired(this.name.as_slice(), |k| {
                SledStorageDB::_map_contains_key(this.tree(), k)
            })? {
                Ok(None)
            } else {
                #[cfg(feature = "map_len")]
                {
                    let removed = this.tree().remove(key.as_slice())?;
                    if removed.is_some() {
                        Self::_non_tx_counter_dec(this.tree(), this.map_count_key_name.as_slice())?;
                    }
                    Ok::<_, anyhow::Error>(removed)
                }
                #[cfg(not(feature = "map_len"))]
                {
                    let removed = this.tree().remove(key.as_slice())?;
                    Ok::<_, anyhow::Error>(removed)
                }
            }
        }?;

        Ok(removed)
    }

    /// Removes keys with prefix
    #[inline]
    fn _remove_with_prefix(&self, prefix: IVec) -> Result<()> {
        let tree = self.tree();
        let prefix = [self.map_item_prefix_name.as_slice(), prefix.as_ref()]
            .concat()
            .to_vec();

        #[cfg(feature = "map_len")]
        let map_count_key_name = self.map_count_key_name.to_vec();
        {
            let mut removeds = Batch::default();
            #[cfg(feature = "map_len")]
            let mut c = 0;
            for item in tree.scan_prefix(prefix) {
                match item {
                    Ok((k, _v)) => {
                        removeds.remove(k.as_ref());
                        #[cfg(feature = "map_len")]
                        {
                            c += 1;
                        }
                    }
                    Err(e) => {
                        log::warn!("{:?}", e);
                    }
                }
            }

            #[cfg(feature = "map_len")]
            {
                let old = tree
                    .get(map_count_key_name.as_slice())?
                    .map(|v| isize::from_be_bytes(v.as_ref().try_into().unwrap_or([0u8; 8])))
                    .unwrap_or(0);
                let len = old - c;
                if len > 0 {
                    tree.insert(map_count_key_name.as_slice(), &len.to_be_bytes())?;
                } else {
                    tree.remove(map_count_key_name.as_slice())?;
                }
                tree.apply_batch(removeds)?;
            }
            #[cfg(not(feature = "map_len"))]
            {
                tree.apply_batch(removeds)?;
            }
        }
        Ok(())
    }

    /// Batch inserts key-value pairs
    #[inline]
    fn _batch_insert(&self, key_vals: Vec<(IVec, IVec)>) -> Result<()> {
        for (k, v) in key_vals {
            self._insert(k, v)?;
        }
        Ok(())
    }

    /// Batch removes keys
    #[inline]
    fn _batch_remove(&self, keys: Vec<IVec>) -> Result<()> {
        for k in keys {
            self._remove(k)?;
        }
        Ok(())
    }

    /// Sets expiration time (TTL feature)
    #[cfg(feature = "ttl")]
    #[inline]
    fn _expire_at(&self, at: TimestampMillis) -> Result<bool> {
        self.db._expire_at(self.name.as_slice(), at, KeyType::Map)
    }

    /// Gets time-to-live (TTL feature)
    #[cfg(feature = "ttl")]
    #[inline]
    fn _ttl(&self) -> Result<Option<TimestampMillis>> {
        let res = self
            .db
            ._ttl(self.name(), |k| {
                SledStorageDB::_map_contains_key(self.tree(), k)
            })?
            .and_then(|(at, _)| if at > 0 { Some(at) } else { None });
        Ok(res)
    }

    /// Checks if map is expired
    #[inline]
    fn _is_expired(&self) -> Result<bool> {
        self.db._is_expired(self.name.as_slice(), |k| {
            SledStorageDB::_map_contains_key(self.tree(), k)
        })
    }

    /// Checks if map is expired (async)
    #[inline]
    async fn call_is_expired(&self) -> Result<bool> {
        let (tx, rx) = oneshot::channel();
        self.db
            .cmd_send(Command::MapIsExpired(self.clone(), tx))
            .await?;
        rx.await?
    }

    /// Creates prefix iterator
    #[inline]
    fn _prefix_iter(&self, prefix: Option<IVec>) -> sled::Iter {
        if let Some(prefix) = prefix {
            self.tree()
                .scan_prefix([self.map_item_prefix_name.as_slice(), prefix.as_ref()].concat())
        } else {
            self.tree()
                .scan_prefix(self.map_item_prefix_name.as_slice())
        }
    }

    /// Creates prefix iterator (async)
    #[inline]
    async fn call_prefix_iter(&self, prefix: Option<IVec>) -> Result<sled::Iter> {
        let (tx, rx) = oneshot::channel();
        self.db
            .cmd_send(Command::MapPrefixIter(self.clone(), prefix, tx))
            .await?;
        Ok(rx.await?)
    }
}

// ---------------------------------------------------------------------------
// Inherent raw methods on SledStorageMap — NOT part of the Map trait.
// ---------------------------------------------------------------------------
#[cfg(feature = "circuit-breaker")]
impl SledStorageMap {
    /// Inserts raw bytes directly, skipping postcard serialization.
    #[inline]
    pub(crate) async fn insert_raw(&self, key: &[u8], val: &[u8]) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.db
            .cmd_send(Command::MapInsert(self.clone(), key.into(), val.into(), tx))
            .await?;
        rx.await??;
        Ok(())
    }

    /// Retrieves raw bytes directly, skipping postcard deserialization.
    #[inline]
    pub(crate) async fn get_raw(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let (tx, rx) = oneshot::channel();
        self.db
            .cmd_send(Command::MapGet(self.clone(), key.into(), tx))
            .await?;
        match rx.await?? {
            Some(v) => Ok(Some(v.to_vec())),
            None => Ok(None),
        }
    }

    /// Removes and returns a raw value, skipping postcard deserialization.
    #[inline]
    pub(crate) async fn remove_and_fetch_raw(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self._remove_and_fetch(key.into())?.map(|v| v.to_vec()))
    }

    /// Batch insert of raw key-value pairs, skipping serialization.
    #[inline]
    pub(crate) async fn batch_insert_raw(&self, key_vals: Vec<(Vec<u8>, Vec<u8>)>) -> Result<()> {
        if key_vals.is_empty() {
            return Ok(());
        }
        let key_vals = key_vals
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect::<Vec<(IVec, IVec)>>();
        let (tx, rx) = oneshot::channel();
        self.db
            .cmd_send(Command::MapBatchInsert(self.clone(), key_vals, tx))
            .await?;
        rx.await??;
        Ok(())
    }
}

#[async_trait]
impl Map for SledStorageMap {
    /// Gets map name
    #[inline]
    fn name(&self) -> &[u8] {
        self.name.as_slice()
    }

    /// Inserts a key-value pair
    #[inline]
    async fn insert<K, V>(&self, key: K, val: &V) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: Serialize + Sync + Send + ?Sized,
    {
        let val = postcard::to_stdvec(&val)?;
        let (tx, rx) = oneshot::channel();
        self.db
            .cmd_send(Command::MapInsert(
                self.clone(),
                key.as_ref().into(),
                val.into(),
                tx,
            ))
            .await?;
        rx.await??;
        Ok(())
    }

    /// Gets a value by key
    #[inline]
    async fn get<K, V>(&self, key: K) -> Result<Option<V>>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: DeserializeOwned + Sync + Send,
    {
        let (tx, rx) = oneshot::channel();
        self.db
            .cmd_send(Command::MapGet(self.clone(), key.as_ref().into(), tx))
            .await?;

        match rx.await?? {
            Some(v) => Ok(Some(postcard::from_bytes::<V>(v.as_ref())?)),
            None => Ok(None),
        }
    }

    /// Removes a key
    #[inline]
    async fn remove<K>(&self, key: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let (tx, rx) = oneshot::channel();
        self.db
            .cmd_send(Command::MapRemove(self.clone(), key.as_ref().into(), tx))
            .await?;
        rx.await??;
        Ok(())
    }

    /// Checks if key exists
    #[inline]
    async fn contains_key<K: AsRef<[u8]> + Sync + Send>(&self, key: K) -> Result<bool> {
        let (tx, rx) = oneshot::channel();
        self.db
            .cmd_send(Command::MapContainsKey(
                self.clone(),
                key.as_ref().into(),
                tx,
            ))
            .await?;
        Ok(rx.await??)
    }

    /// Gets map length (if enabled)
    #[cfg(feature = "map_len")]
    #[inline]
    async fn len(&self) -> Result<usize> {
        let (tx, rx) = oneshot::channel();
        self.db.cmd_send(Command::MapLen(self.clone(), tx)).await?;
        Ok(rx.await??)
    }

    /// Checks if map is empty
    #[inline]
    async fn is_empty(&self) -> Result<bool> {
        let (tx, rx) = oneshot::channel();
        self.db
            .cmd_send(Command::MapIsEmpty(self.clone(), tx))
            .await?;
        Ok(rx.await??)
    }

    /// Clears the map
    #[inline]
    async fn clear(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.db
            .cmd_send(Command::MapClear(self.clone(), tx))
            .await?;
        rx.await??;
        Ok(())
    }

    /// Removes and returns a value
    #[inline]
    async fn remove_and_fetch<K, V>(&self, key: K) -> Result<Option<V>>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: DeserializeOwned + Sync + Send,
    {
        let (tx, rx) = oneshot::channel();
        self.db
            .cmd_send(Command::MapRemoveAndFetch(
                self.clone(),
                key.as_ref().into(),
                tx,
            ))
            .await?;

        match rx.await?? {
            Some(v) => Ok(Some(postcard::from_bytes::<V>(v.as_ref())?)),
            None => Ok(None),
        }
    }

    /// Removes keys with prefix
    #[inline]
    async fn remove_with_prefix<K>(&self, prefix: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let (tx, rx) = oneshot::channel();
        self.db
            .cmd_send(Command::MapRemoveWithPrefix(
                self.clone(),
                prefix.as_ref().into(),
                tx,
            ))
            .await?;
        rx.await??;
        Ok(())
    }

    /// Batch inserts key-value pairs
    #[inline]
    async fn batch_insert<V>(&self, key_vals: Vec<(Key, V)>) -> Result<()>
    where
        V: serde::ser::Serialize + Sync + Send,
    {
        let key_vals = key_vals
            .into_iter()
            .map(|(k, v)| {
                postcard::to_stdvec(&v)
                    .map(|v| (k.into(), v.into()))
                    .map_err(|e| anyhow!(e))
            })
            .collect::<Result<Vec<(IVec, IVec)>>>()?;

        let (tx, rx) = oneshot::channel();
        self.db
            .cmd_send(Command::MapBatchInsert(self.clone(), key_vals, tx))
            .await?;
        rx.await??;
        Ok(())
    }

    /// Batch removes keys
    #[inline]
    async fn batch_remove(&self, keys: Vec<Key>) -> Result<()> {
        let keys = keys.into_iter().map(|k| k.into()).collect::<Vec<IVec>>();

        let (tx, rx) = oneshot::channel();
        self.db
            .cmd_send(Command::MapBatchRemove(self.clone(), keys, tx))
            .await?;
        rx.await??;
        Ok(())
    }

    /// Iterates over map items
    #[inline]
    async fn iter<'a, V>(
        &'a mut self,
    ) -> Result<Box<dyn AsyncIterator<Item = IterItem<V>> + Send + 'a>>
    where
        V: DeserializeOwned + Sync + Send + 'a + 'static,
    {
        let this = self;
        let res = {
            if this.call_is_expired().await? {
                let iter: Box<dyn AsyncIterator<Item = IterItem<V>> + Send> =
                    Box::new(AsyncEmptyIter {
                        _m: std::marker::PhantomData,
                    });
                Ok::<_, anyhow::Error>(iter)
            } else {
                let tem_prefix_name = this.map_item_prefix_name.len();
                let iter = this.call_prefix_iter(None).await?;
                let iter: Box<dyn AsyncIterator<Item = IterItem<V>> + Send> = Box::new(AsyncIter {
                    db: &this.db,
                    prefix_len: tem_prefix_name,
                    iter: Some(iter),
                    _m: std::marker::PhantomData,
                });
                Ok::<_, anyhow::Error>(iter)
            }
        }?;
        Ok(res)
    }

    /// Iterates over map keys
    #[inline]
    async fn key_iter<'a>(
        &'a mut self,
    ) -> Result<Box<dyn AsyncIterator<Item = Result<Key>> + Send + 'a>> {
        let this = self;
        let res = {
            if this.call_is_expired().await? {
                let iter: Box<dyn AsyncIterator<Item = Result<Key>> + Send> =
                    Box::new(AsyncEmptyIter {
                        _m: std::marker::PhantomData,
                    });
                Ok::<_, anyhow::Error>(iter)
            } else {
                let iter = this.call_prefix_iter(None).await?;
                let iter: Box<dyn AsyncIterator<Item = Result<Key>> + Send> =
                    Box::new(AsyncKeyIter {
                        db: &this.db,
                        prefix_len: this.map_item_prefix_name.len(),
                        iter: Some(iter),
                    });
                Ok::<_, anyhow::Error>(iter)
            }
        }?;
        Ok(res)
    }

    /// Iterates over items with prefix
    #[inline]
    async fn prefix_iter<'a, P, V>(
        &'a mut self,
        prefix: P,
    ) -> Result<Box<dyn AsyncIterator<Item = IterItem<V>> + Send + 'a>>
    where
        P: AsRef<[u8]> + Send + Sync,
        V: DeserializeOwned + Sync + Send + 'a + 'static,
    {
        let this = self;
        let res = {
            if this.call_is_expired().await? {
                let iter: Box<dyn AsyncIterator<Item = IterItem<V>> + Send> =
                    Box::new(AsyncEmptyIter {
                        _m: std::marker::PhantomData,
                    });
                Ok::<_, anyhow::Error>(iter)
            } else {
                let iter = this
                    .call_prefix_iter(Some(IVec::from(prefix.as_ref())))
                    .await?;
                let iter: Box<dyn AsyncIterator<Item = IterItem<V>> + Send> = Box::new(AsyncIter {
                    db: &this.db,
                    prefix_len: this.map_item_prefix_name.len(),
                    iter: Some(iter),
                    _m: std::marker::PhantomData,
                });
                Ok::<_, anyhow::Error>(iter)
            }
        }?;
        Ok(res)
    }

    /// Sets expiration time (TTL feature)
    #[cfg(feature = "ttl")]
    async fn expire_at(&self, at: TimestampMillis) -> Result<bool> {
        let (tx, rx) = oneshot::channel();
        self.db
            .cmd_send(Command::MapExpireAt(self.clone(), at, tx))
            .await?;
        Ok(rx.await??)
    }

    /// Sets time-to-live (TTL feature)
    #[cfg(feature = "ttl")]
    async fn expire(&self, dur: TimestampMillis) -> Result<bool> {
        let at = timestamp_millis() + dur;
        self.expire_at(at).await
    }

    /// Gets time-to-live (TTL feature)
    #[cfg(feature = "ttl")]
    async fn ttl(&self) -> Result<Option<TimestampMillis>> {
        let (tx, rx) = oneshot::channel();
        self.db.cmd_send(Command::MapTTL(self.clone(), tx)).await?;
        Ok(rx.await??)
    }
}

/// List structure for queue-like storage within a namespace
#[derive(Clone)]
pub struct SledStorageList {
    /// List name
    name: Key,
    /// Prefix for list keys
    prefix_name: Key,
    /// Database handle
    pub(crate) db: SledStorageDB,
}

impl SledStorageList {
    /// Creates a new list with optional expiration.
    ///
    /// Errors during construction (e.g. command channel full, sled I/O) are
    /// caught, logged, and a bare list handle is returned. Real errors are
    /// deferred to operations on the returned list.
    #[inline]
    async fn new_expire(name: Key, expire_ms: Option<TimestampMillis>, db: SledStorageDB) -> Self {
        let (tx, rx) = oneshot::channel();
        if db
            .cmd_send(Command::DBListNew(
                db.clone(),
                name.clone().into(),
                expire_ms,
                tx,
            ))
            .await
            .is_err()
        {
            log::warn!(
                "sled command channel full, creating list '{}' without health check",
                String::from_utf8_lossy(&name)
            );
            return Self::_new(name, db);
        }
        match rx.await {
            Ok(Ok(l)) => l,
            Ok(Err(e)) => {
                log::warn!(
                    "sled command returned error for list '{}': {e}",
                    String::from_utf8_lossy(&name)
                );
                Self::_new(name, db)
            }
            Err(_) => {
                log::warn!(
                    "sled command dropped, creating list '{}' without health check",
                    String::from_utf8_lossy(&name)
                );
                Self::_new(name, db)
            }
        }
    }

    /// Internal method to create list with expiration.
    /// Errors from sled I/O are caught and logged — the list handle is always
    /// returned.
    #[inline]
    fn _new_expire(name: Key, _expire_ms: Option<TimestampMillis>, db: SledStorageDB) -> Self {
        let l = Self::_new(name, db);
        #[cfg(feature = "ttl")]
        if let Some(expire_ms) = _expire_ms {
            if let Err(e) = l._expire_at(timestamp_millis() + expire_ms) {
                log::warn!(
                    "sled list '{}' expire_at failed: {e}",
                    String::from_utf8_lossy(l.name())
                );
            }
        }
        l
    }

    /// Internal method to create list
    #[inline]
    fn _new(name: Key, db: SledStorageDB) -> Self {
        let prefix_name = SledStorageDB::make_list_prefix(name.as_slice());
        SledStorageList {
            name,
            prefix_name,
            db,
        }
    }

    /// Gets list name
    #[inline]
    pub(crate) fn name(&self) -> &[u8] {
        self.name.as_slice()
    }

    /// Gets the underlying tree
    #[inline]
    pub(crate) fn tree(&self) -> &sled::Tree {
        &self.db.list_tree
    }

    /// Creates list count key
    #[inline]
    fn make_list_count_key(&self) -> Vec<u8> {
        let list_count_key = [self.prefix_name.as_ref(), LIST_KEY_COUNT_SUFFIX].concat();
        list_count_key
    }

    /// Creates list content prefix
    #[inline]
    fn make_list_content_prefix(prefix_name: &[u8], idx: Option<&[u8]>) -> Vec<u8> {
        if let Some(idx) = idx {
            [prefix_name, LIST_KEY_CONTENT_SUFFIX, idx].concat()
        } else {
            [prefix_name, LIST_KEY_CONTENT_SUFFIX].concat()
        }
    }

    /// Creates list content key
    #[inline]
    fn make_list_content_key(&self, idx: usize) -> Vec<u8> {
        Self::make_list_content_prefix(
            self.prefix_name.as_ref(),
            Some(idx.to_be_bytes().as_slice()),
        )
    }

    /// Creates batch of list content keys
    #[inline]
    fn make_list_content_keys(&self, start: usize, end: usize) -> Vec<Vec<u8>> {
        (start..end)
            .map(|idx| self.make_list_content_key(idx))
            .collect()
    }

    /// Non-transactional: gets list count using direct Tree ops
    #[inline]
    fn non_tx_list_count_get(tree: &Tree, key: &[u8]) -> Result<(usize, usize)> {
        if let Some(v) = tree.get(key)? {
            let (start, end) = postcard::from_bytes::<(usize, usize)>(v.as_ref())?;
            Ok((start, end))
        } else {
            Ok((0, 0))
        }
    }

    /// Clears the list
    #[inline]
    fn _clear(&self) -> Result<()> {
        let mut batch = Batch::default();
        let list_count_key = self.make_list_count_key();
        batch.remove(list_count_key);
        let list_content_prefix = Self::make_list_content_prefix(self.prefix_name.as_slice(), None);
        for item in self.tree().scan_prefix(list_content_prefix).keys() {
            match item {
                Ok(k) => {
                    batch.remove(k);
                }
                Err(e) => {
                    log::warn!("{:?}", e);
                }
            }
        }
        self.tree().apply_batch(batch)?;
        Ok(())
    }

    /// Creates batch for clearing list
    #[inline]
    fn _make_clear_batch(&self) -> Batch {
        let mut batch = Batch::default();
        let list_count_key = self.make_list_count_key();
        batch.remove(list_count_key);
        let list_content_prefix = Self::make_list_content_prefix(self.prefix_name.as_slice(), None);
        for item in self.tree().scan_prefix(list_content_prefix).keys() {
            match item {
                Ok(k) => {
                    batch.remove(k);
                }
                Err(e) => {
                    log::warn!("{:?}", e);
                }
            }
        }
        batch
    }

    /// Pushes value to list
    #[inline]
    fn _push(&self, data: IVec) -> Result<()> {
        let tree = self.tree();
        let list_count_key = self.make_list_count_key();
        let (start, mut end) = Self::non_tx_list_count_get(tree, list_count_key.as_slice())?;
        end += 1;
        let count_bytes = postcard::to_allocvec(&(start, end))?;
        tree.insert(list_count_key.as_slice(), count_bytes.as_slice())?;

        let list_content_key = self.make_list_content_key(end);
        tree.insert(list_content_key.as_slice(), data.as_ref())?;

        #[cfg(feature = "ttl")]
        {
            if self.db._is_expired(self.name.as_slice(), |k| {
                SledStorageDB::_list_contains_key(tree, k)
            })? {
                self.db._remove_expire_key(self.name.as_slice())?;
            }
        }

        Ok(())
    }

    /// Pushes multiple values to list
    #[inline]
    fn _pushs(&self, vals: Vec<IVec>) -> Result<()> {
        if vals.is_empty() {
            return Ok(());
        }
        let tree = self.tree();
        let list_count_key = self.make_list_count_key();
        let (start, mut end) = Self::non_tx_list_count_get(tree, list_count_key.as_slice())?;

        let mut list_content_keys = self.make_list_content_keys(end + 1, end + vals.len() + 1);
        end += vals.len();
        let count_bytes = postcard::to_allocvec(&(start, end))?;
        tree.insert(list_count_key.as_slice(), count_bytes.as_slice())?;

        let mut batch = Batch::default();
        for val in vals.iter() {
            let k = list_content_keys.remove(0);
            batch.insert(k, val.as_ref());
        }
        tree.apply_batch(batch)?;

        #[cfg(feature = "ttl")]
        {
            if self.db._is_expired(self.name.as_slice(), |k| {
                SledStorageDB::_list_contains_key(tree, k)
            })? {
                self.db._remove_expire_key(self.name.as_slice())?;
            }
        }
        Ok(())
    }

    /// Pushes value with limit
    #[inline]
    fn _push_limit(
        &self,
        data: IVec,
        limit: usize,
        pop_front_if_limited: bool,
    ) -> Result<Option<IVec>> {
        let tree = self.tree();
        let list_count_key = self.make_list_count_key();
        let (mut start, mut end) = Self::non_tx_list_count_get(tree, list_count_key.as_slice())?;
        let count = end - start;

        let removed = if count < limit {
            end += 1;
            let count_bytes = postcard::to_allocvec(&(start, end))?;
            tree.insert(list_count_key.as_slice(), count_bytes.as_slice())?;
            let list_content_key = self.make_list_content_key(end);
            tree.insert(list_content_key.as_slice(), data.as_ref())?;
            None
        } else if pop_front_if_limited {
            let mut removed = None;
            let removed_content_key = self.make_list_content_key(start + 1);
            if let Some(v) = tree.remove(removed_content_key)? {
                removed = Some(v);
                start += 1;
            }
            end += 1;
            let count_bytes = postcard::to_allocvec(&(start, end))?;
            tree.insert(list_count_key.as_slice(), count_bytes.as_slice())?;
            let list_content_key = self.make_list_content_key(end);
            tree.insert(list_content_key.as_slice(), data.as_ref())?;
            removed
        } else {
            return Err(anyhow::anyhow!("Is full"));
        };

        #[cfg(feature = "ttl")]
        {
            if self.db._is_expired(self.name.as_slice(), |k| {
                SledStorageDB::_list_contains_key(tree, k)
            })? {
                self.db._remove_expire_key(self.name.as_slice())?;
            }
        }

        Ok(removed)
    }

    /// Pops value from list
    #[inline]
    fn _pop(&self) -> Result<Option<IVec>> {
        if self.db._is_expired(self.name.as_slice(), |k| {
            SledStorageDB::_list_contains_key(self.tree(), k)
        })? {
            return Ok(None);
        }

        let tree = self.tree();
        let list_count_key = self.make_list_count_key();
        let (start, end) = Self::non_tx_list_count_get(tree, list_count_key.as_slice())?;

        let removed = if (end - start) > 0 {
            let removed_content_key = self.make_list_content_key(start + 1);
            if let Some(v) = tree.remove(removed_content_key)? {
                let count_bytes = postcard::to_allocvec(&(start + 1, end))?;
                tree.insert(list_count_key.as_slice(), count_bytes.as_slice())?;
                Some(v)
            } else {
                None
            }
        } else {
            None
        };

        Ok(removed)
    }

    /// Gets all values in list
    #[inline]
    fn _all(&self) -> Result<Vec<IVec>> {
        let this = self;
        let res = {
            if this.db._is_expired(this.name.as_slice(), |k| {
                SledStorageDB::_list_contains_key(this.tree(), k)
            })? {
                Ok(vec![])
            } else {
                let key_content_prefix =
                    Self::make_list_content_prefix(this.prefix_name.as_slice(), None);
                this.tree()
                    .scan_prefix(key_content_prefix)
                    .values()
                    .map(|item| item.map_err(anyhow::Error::new))
                    .collect::<Result<Vec<_>>>()
            }
        }?;
        Ok(res)
    }

    /// Gets value by index
    #[inline]
    fn _get_index(&self, idx: usize) -> Result<Option<IVec>> {
        if self.db._is_expired(self.name.as_slice(), |k| {
            SledStorageDB::_list_contains_key(self.tree(), k)
        })? {
            return Ok(None);
        }

        let tree = self.tree();
        let list_count_key = self.make_list_count_key();
        let (start, end) = Self::non_tx_list_count_get(tree, list_count_key.as_slice())?;

        if idx < (end - start) {
            let list_content_key = self.make_list_content_key(start + idx + 1);
            Ok(tree.get(list_content_key)?)
        } else {
            Ok(None)
        }
    }

    /// Gets list length
    #[inline]
    fn _len(&self) -> Result<usize> {
        let this = self;
        let res = {
            if this.db._is_expired(this.name.as_slice(), |k| {
                SledStorageDB::_list_contains_key(this.tree(), k)
            })? {
                Ok::<usize, anyhow::Error>(0)
            } else {
                let list_count_key = this.make_list_count_key();
                if let Some(v) = this.tree().get(list_count_key.as_slice())? {
                    let (start, end) = postcard::from_bytes::<(usize, usize)>(v.as_ref())?;
                    Ok(end - start)
                } else {
                    Ok(0)
                }
            }
        }?;
        Ok(res)
    }

    /// Checks if list is empty
    #[inline]
    fn _is_empty(&self) -> Result<bool> {
        let this = self;
        let res = {
            if this.db._is_expired(this.name.as_slice(), |k| {
                SledStorageDB::_list_contains_key(this.tree(), k)
            })? {
                Ok::<bool, anyhow::Error>(true)
            } else {
                let list_content_prefix =
                    Self::make_list_content_prefix(this.prefix_name.as_slice(), None);
                Ok(this
                    .tree()
                    .scan_prefix(list_content_prefix)
                    .keys()
                    .next()
                    .is_none())
            }
        }?;
        Ok(res)
    }

    /// Sets expiration time (TTL feature)
    #[cfg(feature = "ttl")]
    #[inline]
    fn _expire_at(&self, at: TimestampMillis) -> Result<bool> {
        self.db._expire_at(self.name.as_slice(), at, KeyType::List)
    }

    /// Gets time-to-live (TTL feature)
    #[cfg(feature = "ttl")]
    #[inline]
    fn _ttl(&self) -> Result<Option<TimestampMillis>> {
        Ok(self
            .db
            ._ttl(self.name(), |k| {
                SledStorageDB::_list_contains_key(self.tree(), k)
            })?
            .and_then(|(at, _)| if at > 0 { Some(at) } else { None }))
    }

    /// Checks if list is expired
    #[inline]
    fn _is_expired(&self) -> Result<bool> {
        self.db._is_expired(self.name.as_slice(), |k| {
            SledStorageDB::_list_contains_key(self.tree(), k)
        })
    }

    /// Checks if list is expired (async)
    #[inline]
    async fn call_is_expired(&self) -> Result<bool> {
        let (tx, rx) = oneshot::channel();
        self.db
            .cmd_send(Command::ListIsExpired(self.clone(), tx))
            .await?;
        rx.await?
    }

    /// Creates prefix iterator
    #[inline]
    fn _prefix_iter(&self) -> sled::Iter {
        let list_content_prefix = Self::make_list_content_prefix(self.prefix_name.as_slice(), None);
        self.tree().scan_prefix(list_content_prefix)
    }

    /// Creates prefix iterator (async)
    #[inline]
    async fn call_prefix_iter(&self) -> Result<sled::Iter> {
        let (tx, rx) = oneshot::channel();
        self.db
            .cmd_send(Command::ListPrefixIter(self.clone(), tx))
            .await?;
        Ok(rx.await?)
    }
}

#[async_trait]
impl List for SledStorageList {
    /// Gets list name
    #[inline]
    fn name(&self) -> &[u8] {
        self.name.as_slice()
    }

    /// Pushes value to list
    #[inline]
    async fn push<V>(&self, val: &V) -> Result<()>
    where
        V: serde::ser::Serialize + Sync + Send,
    {
        let val = postcard::to_stdvec(&val)?;
        let (tx, rx) = oneshot::channel();
        self.db
            .cmd_send(Command::ListPush(self.clone(), val.into(), tx))
            .await?;
        rx.await??;
        Ok(())
    }

    /// Pushes multiple values to list
    #[inline]
    async fn pushs<V>(&self, vals: Vec<V>) -> Result<()>
    where
        V: Serialize + Sync + Send,
    {
        if vals.is_empty() {
            return Ok(());
        }

        let vals = vals
            .into_iter()
            .map(|v| {
                postcard::to_stdvec(&v)
                    .map(|v| v.into())
                    .map_err(|e| anyhow!(e))
            })
            .collect::<Result<Vec<_>>>()?;

        let (tx, rx) = oneshot::channel();
        self.db
            .cmd_send(Command::ListPushs(self.clone(), vals, tx))
            .await?;
        rx.await??;
        Ok(())
    }

    /// Pushes value with limit
    #[inline]
    async fn push_limit<V>(
        &self,
        val: &V,
        limit: usize,
        pop_front_if_limited: bool,
    ) -> Result<Option<V>>
    where
        V: serde::ser::Serialize + Sync + Send,
        V: DeserializeOwned,
    {
        let data = postcard::to_stdvec(&val)?;

        let (tx, rx) = oneshot::channel();
        self.db
            .cmd_send(Command::ListPushLimit(
                self.clone(),
                data.into(),
                limit,
                pop_front_if_limited,
                tx,
            ))
            .await?;

        let removed = if let Some(removed) = rx.await?? {
            Some(
                postcard::from_bytes::<V>(removed.as_ref())
                    .map_err(|e| sled::Error::Io(io::Error::new(ErrorKind::InvalidData, e)))?,
            )
        } else {
            None
        };
        Ok(removed)
    }

    /// Pops value from list
    #[inline]
    async fn pop<V>(&self) -> Result<Option<V>>
    where
        V: DeserializeOwned + Sync + Send,
    {
        let (tx, rx) = oneshot::channel();
        self.db.cmd_send(Command::ListPop(self.clone(), tx)).await?;

        let removed = if let Some(removed) = rx.await?? {
            Some(
                postcard::from_bytes::<V>(removed.as_ref())
                    .map_err(|e| sled::Error::Io(io::Error::new(ErrorKind::InvalidData, e)))?,
            )
        } else {
            None
        };
        Ok(removed)
    }

    /// Gets all values in list
    #[inline]
    async fn all<V>(&self) -> Result<Vec<V>>
    where
        V: DeserializeOwned + Sync + Send,
    {
        let (tx, rx) = oneshot::channel();
        self.db.cmd_send(Command::ListAll(self.clone(), tx)).await?;

        rx.await??
            .iter()
            .map(|v| postcard::from_bytes::<V>(v.as_ref()).map_err(|e| anyhow!(e)))
            .collect::<Result<Vec<_>>>()
    }

    /// Gets value by index
    #[inline]
    async fn get_index<V>(&self, idx: usize) -> Result<Option<V>>
    where
        V: DeserializeOwned + Sync + Send,
    {
        let (tx, rx) = oneshot::channel();
        self.db
            .cmd_send(Command::ListGetIndex(self.clone(), idx, tx))
            .await?;

        Ok(if let Some(res) = rx.await?? {
            Some(postcard::from_bytes::<V>(res.as_ref()).map_err(|e| anyhow!(e))?)
        } else {
            None
        })
    }

    /// Gets list length
    #[inline]
    async fn len(&self) -> Result<usize> {
        let (tx, rx) = oneshot::channel();
        self.db.cmd_send(Command::ListLen(self.clone(), tx)).await?;
        Ok(rx.await??)
    }

    /// Checks if list is empty
    #[inline]
    async fn is_empty(&self) -> Result<bool> {
        let (tx, rx) = oneshot::channel();
        self.db
            .cmd_send(Command::ListIsEmpty(self.clone(), tx))
            .await?;
        Ok(rx.await??)
    }

    /// Clears the list
    #[inline]
    async fn clear(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.db
            .cmd_send(Command::ListClear(self.clone(), tx))
            .await?;
        Ok(rx.await??)
    }

    /// Iterates over list values
    #[inline]
    async fn iter<'a, V>(
        &'a mut self,
    ) -> Result<Box<dyn AsyncIterator<Item = Result<V>> + Send + 'a>>
    where
        V: DeserializeOwned + Sync + Send + 'a + 'static,
    {
        let this = self;
        let res = {
            if this.call_is_expired().await? {
                let iter: Box<dyn AsyncIterator<Item = Result<V>> + Send> =
                    Box::new(AsyncEmptyIter {
                        _m: std::marker::PhantomData,
                    });
                Ok::<_, anyhow::Error>(iter)
            } else {
                let iter = this.call_prefix_iter().await?;
                let iter: Box<dyn AsyncIterator<Item = Result<V>> + Send> =
                    Box::new(AsyncListValIter {
                        db: &this.db,
                        iter: Some(iter),
                        _m: std::marker::PhantomData,
                    });
                Ok::<_, anyhow::Error>(iter)
            }
        }?;
        Ok(res)
    }

    /// Sets expiration time (TTL feature)
    #[cfg(feature = "ttl")]
    async fn expire_at(&self, at: TimestampMillis) -> Result<bool> {
        let (tx, rx) = oneshot::channel();
        self.db
            .cmd_send(Command::ListExpireAt(self.clone(), at, tx))
            .await?;
        Ok(rx.await??)
    }

    /// Sets time-to-live (TTL feature)
    #[cfg(feature = "ttl")]
    async fn expire(&self, dur: TimestampMillis) -> Result<bool> {
        let at = timestamp_millis() + dur;
        self.expire_at(at).await
    }

    /// Gets time-to-live (TTL feature)
    #[cfg(feature = "ttl")]
    async fn ttl(&self) -> Result<Option<TimestampMillis>> {
        let (tx, rx) = oneshot::channel();
        self.db.cmd_send(Command::ListTTL(self.clone(), tx)).await?;
        Ok(rx.await??)
    }
}

// ---------------------------------------------------------------------------
// Inherent raw methods on SledStorageList — NOT part of the List trait.
// ---------------------------------------------------------------------------
#[cfg(feature = "circuit-breaker")]
impl SledStorageList {
    #[inline]
    pub(crate) async fn push_raw(&self, val: &[u8]) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.db
            .cmd_send(Command::ListPush(self.clone(), val.into(), tx))
            .await?;
        rx.await??;
        Ok(())
    }

    #[inline]
    pub(crate) async fn pushs_raw(&self, vals: Vec<Vec<u8>>) -> Result<()> {
        if vals.is_empty() {
            return Ok(());
        }
        let vals: Vec<IVec> = vals.into_iter().map(|v| v.into()).collect();
        let (tx, rx) = oneshot::channel();
        self.db
            .cmd_send(Command::ListPushs(self.clone(), vals, tx))
            .await?;
        rx.await??;
        Ok(())
    }

    #[inline]
    pub(crate) async fn push_limit_raw(
        &self,
        val: &[u8],
        limit: usize,
        pop_front_if_limited: bool,
    ) -> Result<Option<Vec<u8>>> {
        let (tx, rx) = oneshot::channel();
        self.db
            .cmd_send(Command::ListPushLimit(
                self.clone(),
                val.into(),
                limit,
                pop_front_if_limited,
                tx,
            ))
            .await?;

        Ok(rx.await??.map(|v| v.to_vec()))
    }

    #[inline]
    pub(crate) async fn pop_raw(&self) -> Result<Option<Vec<u8>>> {
        let (tx, rx) = oneshot::channel();
        self.db.cmd_send(Command::ListPop(self.clone(), tx)).await?;
        Ok(rx.await??.map(|v| v.to_vec()))
    }

    #[inline]
    pub(crate) async fn all_raw(&self) -> Result<Vec<Vec<u8>>> {
        let (tx, rx) = oneshot::channel();
        self.db.cmd_send(Command::ListAll(self.clone(), tx)).await?;
        Ok(rx.await??.into_iter().map(|v| v.to_vec()).collect())
    }

    #[inline]
    pub(crate) async fn get_index_raw(&self, idx: usize) -> Result<Option<Vec<u8>>> {
        let (tx, rx) = oneshot::channel();
        self.db
            .cmd_send(Command::ListGetIndex(self.clone(), idx, tx))
            .await?;
        Ok(rx.await??.map(|v| v.to_vec()))
    }
}

/// Async iterator for map items
pub struct AsyncIter<'a, V> {
    db: &'a SledStorageDB,
    prefix_len: usize,
    iter: Option<sled::Iter>,
    _m: std::marker::PhantomData<V>,
}

impl<V> Debug for AsyncIter<'_, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("AsyncIter .. ").finish()
    }
}

#[async_trait]
impl<V> AsyncIterator for AsyncIter<'_, V>
where
    V: DeserializeOwned + Sync + Send + 'static,
{
    type Item = IterItem<V>;

    async fn next(&mut self) -> Option<Self::Item> {
        let mut iter = self.iter.take()?;
        let (tx, rx) = oneshot::channel();
        if let Err(e) = self.db.cmd_send(Command::IterNext(iter, tx)).await {
            return Some(Err(e));
        }
        let item = match rx.await {
            Err(e) => {
                return Some(Err(anyhow::Error::new(e)));
            }
            Ok((it, item)) => {
                iter = it;
                item
            }
        };

        match item {
            None => None,
            Some(Err(e)) => Some(Err(anyhow::Error::new(e))),
            Some(Ok((k, v))) => {
                let name = k.as_ref()[self.prefix_len..].to_vec();
                match postcard::from_bytes::<V>(v.as_ref()) {
                    Ok(v) => {
                        self.iter = Some(iter);
                        Some(Ok((name, v)))
                    }
                    Err(e) => Some(Err(anyhow::Error::new(e))),
                }
            }
        }
    }
}

/// Async iterator for map keys
pub struct AsyncKeyIter<'a> {
    db: &'a SledStorageDB,
    prefix_len: usize,
    iter: Option<sled::Iter>,
}

impl Debug for AsyncKeyIter<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("AsyncKeyIter .. ").finish()
    }
}

#[async_trait]
impl AsyncIterator for AsyncKeyIter<'_> {
    type Item = Result<Key>;

    async fn next(&mut self) -> Option<Self::Item> {
        let mut iter = self.iter.take()?;
        let (tx, rx) = oneshot::channel();
        if let Err(e) = self.db.cmd_send(Command::IterNext(iter, tx)).await {
            return Some(Err(e));
        }
        let item = match rx.await {
            Err(e) => {
                return Some(Err(anyhow::Error::new(e)));
            }
            Ok((it, item)) => {
                iter = it;
                item
            }
        };

        return match item {
            None => None,
            Some(Err(e)) => Some(Err(anyhow::Error::new(e))),
            Some(Ok((k, _))) => {
                self.iter = Some(iter);
                let name = k.as_ref()[self.prefix_len..].to_vec();
                Some(Ok(name))
            }
        };
    }
}

/// Async iterator for list values
pub struct AsyncListValIter<'a, V> {
    db: &'a SledStorageDB,
    iter: Option<sled::Iter>,
    _m: std::marker::PhantomData<V>,
}

impl<V> Debug for AsyncListValIter<'_, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("AsyncListValIter .. ").finish()
    }
}

#[async_trait]
impl<V> AsyncIterator for AsyncListValIter<'_, V>
where
    V: DeserializeOwned + Sync + Send + 'static,
{
    type Item = Result<V>;

    async fn next(&mut self) -> Option<Self::Item> {
        let mut iter = self.iter.take()?;
        let (tx, rx) = oneshot::channel();
        if let Err(e) = self.db.cmd_send(Command::IterNext(iter, tx)).await {
            return Some(Err(e));
        }
        let item = match rx.await {
            Err(e) => {
                return Some(Err(anyhow::Error::new(e)));
            }
            Ok((it, item)) => {
                iter = it;
                item
            }
        };

        match item {
            None => None,
            Some(Err(e)) => Some(Err(anyhow::Error::new(e))),
            Some(Ok((_k, v))) => {
                self.iter = Some(iter);
                Some(postcard::from_bytes::<V>(v.as_ref()).map_err(|e| anyhow!(e)))
            }
        }
    }
}

/// Empty iterator
pub struct AsyncEmptyIter<T> {
    _m: std::marker::PhantomData<T>,
}

impl<T> Debug for AsyncEmptyIter<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("AsyncEmptyIter .. ").finish()
    }
}

#[async_trait]
impl<T> AsyncIterator for AsyncEmptyIter<T>
where
    T: Send + Sync + 'static,
{
    type Item = T;

    async fn next(&mut self) -> Option<Self::Item> {
        None
    }
}

/// Async iterator for maps
pub struct AsyncMapIter<'a> {
    db: &'a SledStorageDB,
    iter: Option<sled::Iter>,
}

impl<'a> AsyncMapIter<'a> {
    fn new(db: &'a SledStorageDB, iter: sled::Iter) -> Self {
        Self {
            db,
            iter: Some(iter),
        }
    }
}

impl Debug for AsyncMapIter<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("AsyncMapIter .. ").finish()
    }
}

#[async_trait]
impl AsyncIterator for AsyncMapIter<'_> {
    type Item = Result<StorageMap>;

    async fn next(&mut self) -> Option<Self::Item> {
        let mut iter = self.iter.take()?;
        loop {
            let (tx, rx) = oneshot::channel();
            if let Err(e) = self.db.cmd_send(Command::IterNext(iter, tx)).await {
                return Some(Err(e));
            }
            let item = match rx.await {
                Err(e) => {
                    return Some(Err(anyhow::Error::new(e)));
                }
                Ok((it, item)) => {
                    iter = it;
                    item
                }
            };

            match item {
                None => return None,
                Some(Err(e)) => return Some(Err(anyhow::Error::new(e))),
                Some(Ok((k, _))) => {
                    if !SledStorageDB::is_map_count_key(k.as_ref()) {
                        continue;
                    }
                    self.iter = Some(iter);
                    let name = SledStorageDB::map_count_key_to_name(k.as_ref());
                    return Some(Ok(StorageMap::Sled(self.db._map(name))));
                }
            }
        }
    }
}

/// Async iterator for lists
pub struct AsyncListIter<'a> {
    db: &'a SledStorageDB,
    iter: Option<sled::Iter>,
}

impl Debug for AsyncListIter<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("AsyncListIter .. ").finish()
    }
}

#[async_trait]
impl AsyncIterator for AsyncListIter<'_> {
    type Item = Result<StorageList>;

    async fn next(&mut self) -> Option<Self::Item> {
        let mut iter = self.iter.take()?;
        loop {
            let (tx, rx) = oneshot::channel();
            if let Err(e) = self.db.cmd_send(Command::IterNext(iter, tx)).await {
                return Some(Err(e));
            }
            let item = match rx.await {
                Err(e) => {
                    return Some(Err(anyhow::Error::new(e)));
                }
                Ok((it, item)) => {
                    iter = it;
                    item
                }
            };
            return match item {
                None => None,
                Some(Err(e)) => Some(Err(anyhow::Error::new(e))),
                Some(Ok((k, _))) => {
                    if !SledStorageDB::is_list_count_key(k.as_ref()) {
                        continue;
                    }
                    self.iter = Some(iter);
                    let name = SledStorageDB::list_count_key_to_name(k.as_ref());
                    Some(Ok(StorageList::Sled(self.db._list(name))))
                }
            };
        }
    }
}

/// Async iterator for database keys with pattern matching
pub struct AsyncDbKeyIter<'a> {
    db: &'a SledStorageDB,
    pattern: Pattern,
    iter: Option<sled::Iter>,
}

impl Debug for AsyncDbKeyIter<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("AsyncDbKeyIter .. ").finish()
    }
}

#[async_trait]
impl AsyncIterator for AsyncDbKeyIter<'_> {
    type Item = Result<Key>;

    async fn next(&mut self) -> Option<Self::Item> {
        let mut iter = self.iter.take()?;
        loop {
            let (tx, rx) = oneshot::channel();
            if let Err(e) = self.db.cmd_send(Command::IterNext(iter, tx)).await {
                return Some(Err(e));
            }
            let item = match rx.await {
                Err(e) => {
                    return Some(Err(anyhow::Error::new(e)));
                }
                Ok((it, item)) => {
                    iter = it;
                    item
                }
            };

            return match item {
                None => None,
                Some(Err(e)) => Some(Err(anyhow::Error::new(e))),
                Some(Ok((k, _))) => {
                    if !is_match(self.pattern.clone(), k.as_ref()) {
                        continue;
                    }
                    self.iter = Some(iter);
                    Some(Ok(k.to_vec()))
                }
            };
        }
    }
}
