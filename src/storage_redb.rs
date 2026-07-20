//! Redb-based persistent storage implementation
//!
//! This module provides a persistent storage solution backed by Redb (an embedded database).
//! It implements key-value storage, maps (dictionaries), and lists (queues) with support for:
//! - ACID transactions via redb's WriteTransaction/ReadTransaction
//! - Asynchronous API via command channel + spawn_blocking
//! - TTL/expiration (optional feature)
//! - Counters
//! - Batch operations
//! - Iterators (snapshot-based)
//!
//! # Transaction Strategy
//! Every write operation is wrapped in a single `WriteTransaction` (begin_write → op → commit).
//! Every read operation uses a `ReadTransaction` (snapshot isolation).
//! The command channel serializes all operations to a single background thread,
//! which naturally meets redb's single-writer requirement.

use core::fmt;
use std::sync::atomic::{AtomicBool, AtomicIsize, Ordering};
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::spawn_blocking;

use redb::{
    Database, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition,
    WriteTransaction,
};

use crate::storage::{AsyncIterator, IterItem, Key, List, Map, StorageDB};
#[cfg(feature = "ttl")]
use crate::timestamp_millis;
use crate::{StorageList, StorageMap, TimestampMillis};

// ============================================================================
// Constants & Table Definitions
// ============================================================================

/// redb typed table definitions (all use &[u8] for both key and value)
const KV_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("__kv_table");
const MAP_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("__map_table");
const LIST_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("__list_table");
const EXPIRE_KEYS_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("__expire_key_table");
const KEY_EXPIRE_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("__key_expire_table");

/// Separator between parts of composite keys
#[allow(dead_code)]
const SEPARATOR: &[u8] = b"@";
/// Separator between map name and item key
const MAP_KEY_SEPARATOR: &[u8] = b"@__item@";
/// Suffix for map count keys
const MAP_KEY_COUNT_SUFFIX: &[u8] = b"@__count@";
/// Suffix for list count keys
const LIST_KEY_COUNT_SUFFIX: &[u8] = b"@__count@";
/// Suffix for list content keys
const LIST_KEY_CONTENT_SUFFIX: &[u8] = b"@__content@";

/// Prefix constants (re-use naming from storage::*)
const MAP_NAME_PREFIX: &[u8] = b"__rmqtt_map@";
const LIST_NAME_PREFIX: &[u8] = b"__rmqtt_list@";
const KEY_PREFIX: &[u8] = b"__rmqtt@";
#[allow(dead_code)]
const KEY_PREFIX_LEN: &[u8] = b"__rmqtt_len@";
const COUNTER_PREFIX: &[u8] = b"__counter@";

/// Maximum command channel capacity
const CHANNEL_CAPACITY: usize = 10_000;

// ============================================================================
// Type aliases (for clippy::type_complexity)
// ============================================================================

/// Result sender for iterator responses: Vec of (key, value) pairs
type IterResultSender = oneshot::Sender<Result<Vec<(Vec<u8>, Vec<u8>)>>>;

/// Batch insert item: (key, value, response_sender)
type BatchInsertItem = (Vec<u8>, Vec<u8>, oneshot::Sender<Result<()>>);

// ============================================================================
// Key helper functions
// ============================================================================

/// Build a map prefix: __rmqtt_map@{name}@
#[allow(dead_code)]
fn make_map_prefix(name: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(MAP_NAME_PREFIX.len() + name.len() + 1);
    v.extend_from_slice(MAP_NAME_PREFIX);
    v.extend_from_slice(name);
    v.extend_from_slice(SEPARATOR);
    v
}

/// Build a map item prefix: __rmqtt_map@{name}@@__item@
fn make_map_item_prefix(name: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(MAP_NAME_PREFIX.len() + name.len() + MAP_KEY_SEPARATOR.len());
    v.extend_from_slice(MAP_NAME_PREFIX);
    v.extend_from_slice(name);
    v.extend_from_slice(MAP_KEY_SEPARATOR);
    v
}

/// Build a map item key: __rmqtt_map@{name}@@__item@{key}
fn make_map_item_key(name: &[u8], key: &[u8]) -> Vec<u8> {
    let mut v = make_map_item_prefix(name);
    v.extend_from_slice(key);
    v
}

/// Build a map count key: __rmqtt_map@{name}@@__count@
fn make_map_count_key(name: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(MAP_NAME_PREFIX.len() + name.len() + MAP_KEY_COUNT_SUFFIX.len());
    v.extend_from_slice(MAP_NAME_PREFIX);
    v.extend_from_slice(name);
    v.extend_from_slice(MAP_KEY_COUNT_SUFFIX);
    v
}

/// Check if a key is a map count key
fn is_map_count_key(key: &[u8]) -> bool {
    key.starts_with(MAP_NAME_PREFIX) && key.ends_with(MAP_KEY_COUNT_SUFFIX)
}

/// Extract map name from a count key
fn map_count_key_to_name(key: &[u8]) -> &[u8] {
    let start = MAP_NAME_PREFIX.len();
    let end = key.len() - MAP_KEY_COUNT_SUFFIX.len();
    &key[start..end]
}

/// Extract map name from an item key
#[allow(dead_code)]
fn map_item_key_to_name(key: &[u8]) -> Option<&[u8]> {
    if let Some(pos) = key
        .windows(MAP_KEY_SEPARATOR.len())
        .position(|w| w == MAP_KEY_SEPARATOR)
    {
        if key.starts_with(MAP_NAME_PREFIX) {
            return Some(&key[MAP_NAME_PREFIX.len()..pos]);
        }
    }
    None
}

/// Build a list prefix: __rmqtt_list@{name}
#[allow(dead_code)]
fn make_list_prefix(name: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(LIST_NAME_PREFIX.len() + name.len());
    v.extend_from_slice(LIST_NAME_PREFIX);
    v.extend_from_slice(name);
    v
}

/// Build a list count key: __rmqtt_list@{name}@@__count@
fn make_list_count_key(name: &[u8]) -> Vec<u8> {
    let mut v =
        Vec::with_capacity(LIST_NAME_PREFIX.len() + name.len() + LIST_KEY_COUNT_SUFFIX.len());
    v.extend_from_slice(LIST_NAME_PREFIX);
    v.extend_from_slice(name);
    v.extend_from_slice(LIST_KEY_COUNT_SUFFIX);
    v
}

/// Check if a key is a list count key
fn is_list_count_key(key: &[u8]) -> bool {
    key.starts_with(LIST_NAME_PREFIX) && key.ends_with(LIST_KEY_COUNT_SUFFIX)
}

/// Extract list name from a count key
fn list_count_key_to_name(key: &[u8]) -> &[u8] {
    let start = LIST_NAME_PREFIX.len();
    let end = key.len() - LIST_KEY_COUNT_SUFFIX.len();
    &key[start..end]
}

/// Build a list content key: __rmqtt_list@{name}@@__content@{idx_be}
fn make_list_content_key(name: &[u8], idx: usize) -> Vec<u8> {
    let idx_bytes = idx.to_be_bytes();
    let mut v =
        Vec::with_capacity(LIST_NAME_PREFIX.len() + name.len() + LIST_KEY_CONTENT_SUFFIX.len() + 8);
    v.extend_from_slice(LIST_NAME_PREFIX);
    v.extend_from_slice(name);
    v.extend_from_slice(LIST_KEY_CONTENT_SUFFIX);
    v.extend_from_slice(&idx_bytes);
    v
}

/// Build a list content prefix for range scanning
fn make_list_content_prefix(name: &[u8]) -> Vec<u8> {
    let mut v =
        Vec::with_capacity(LIST_NAME_PREFIX.len() + name.len() + LIST_KEY_CONTENT_SUFFIX.len());
    v.extend_from_slice(LIST_NAME_PREFIX);
    v.extend_from_slice(name);
    v.extend_from_slice(LIST_KEY_CONTENT_SUFFIX);
    v
}

/// Build a counter key: __counter@{key}
#[allow(dead_code)]
fn make_counter_key(key: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(COUNTER_PREFIX.len() + key.len());
    v.extend_from_slice(COUNTER_PREFIX);
    v.extend_from_slice(key);
    v
}

// ============================================================================
// Pattern matching (ported from storage_sled.rs)
// ============================================================================

#[derive(Clone)]
struct Pattern(Arc<Vec<PatternChar>>);

impl std::ops::Deref for Pattern {
    type Target = Vec<PatternChar>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Clone, Debug)]
enum PatternChar {
    /// Exact byte match
    Exact(u8),
    /// Wildcard (*) — matches zero or more characters
    Wildcard,
    /// Any single character (? or +)
    AnyChar,
}

impl From<&[u8]> for Pattern {
    fn from(pattern: &[u8]) -> Self {
        Pattern::parse(pattern)
    }
}

impl Pattern {
    fn parse(pattern: &[u8]) -> Self {
        let mut parsed = Vec::new();
        let mut chars = pattern.iter().copied().peekable();
        while let Some(c) = chars.next() {
            match c {
                b'*' => {
                    // Collapse consecutive wildcards
                    if !matches!(parsed.last(), Some(PatternChar::Wildcard)) {
                        parsed.push(PatternChar::Wildcard);
                    }
                }
                b'?' => parsed.push(PatternChar::AnyChar),
                b'\\' => {
                    if let Some(next) = chars.next() {
                        parsed.push(PatternChar::Exact(next));
                    }
                }
                _ => parsed.push(PatternChar::Exact(c)),
            }
        }
        Pattern(Arc::new(parsed))
    }
}

fn is_match<P: Into<Pattern>>(pattern: P, text: &[u8]) -> bool {
    let pattern = pattern.into();
    let text_chars = text;
    let pattern_len = pattern.len();
    let text_len = text_chars.len();

    // DP table for pattern matching
    let mut dp = vec![vec![false; pattern_len + 1]; text_len + 1];
    dp[0][0] = true;

    for j in 1..=pattern_len {
        if matches!(pattern[j - 1], PatternChar::Wildcard) {
            dp[0][j] = dp[0][j - 1];
        }
    }

    for i in 1..=text_len {
        for j in 1..=pattern_len {
            match &pattern[j - 1] {
                PatternChar::Exact(c) => {
                    if text_chars[i - 1] == *c {
                        dp[i][j] = dp[i - 1][j - 1];
                    }
                }
                PatternChar::AnyChar => {
                    dp[i][j] = dp[i - 1][j - 1];
                }
                PatternChar::Wildcard => {
                    dp[i][j] = dp[i][j - 1] || dp[i - 1][j];
                }
            }
        }
    }

    dp[text_len][pattern_len]
}

// ============================================================================
// Cleanup function type
// ============================================================================

/// Type alias for cleanup function signature
pub type RedbCleanupFun = fn(&RedbStorageDB);

/// Default cleanup function that runs in background thread
fn def_cleanup(_db: &RedbStorageDB) {
    #[cfg(feature = "ttl")]
    {
        let db = _db.clone();
        std::thread::spawn(move || {
            let limit = 5000;
            loop {
                std::thread::sleep(std::time::Duration::from_secs(60));
                let mut total_cleanups = 0;
                let now = std::time::Instant::now();
                loop {
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

// ============================================================================
// RedbConfig
// ============================================================================

/// Configuration for the Redb storage backend
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedbConfig {
    /// Path to the database file
    pub path: String,
    /// Cache size in bytes (default: 1GB)
    #[serde(default = "RedbConfig::default_cache_size")]
    pub cache_size: usize,
    /// Cleanup function for expired keys
    #[serde(skip, default = "RedbConfig::cleanup_f_default")]
    pub cleanup_f: RedbCleanupFun,
}

impl RedbConfig {
    fn default_cache_size() -> usize {
        1024 * 1024 * 1024 // 1GB
    }

    /// Returns default cleanup function
    #[inline]
    fn cleanup_f_default() -> RedbCleanupFun {
        def_cleanup
    }
}

impl Default for RedbConfig {
    fn default() -> Self {
        RedbConfig {
            path: String::default(),
            cache_size: Self::default_cache_size(),
            cleanup_f: def_cleanup,
        }
    }
}

// ============================================================================
// RedbStorageDB
// ============================================================================

/// Redb-backed storage database
pub struct RedbStorageDB {
    db: Arc<Database>,
    cmd_tx: mpsc::Sender<Command>,
    active_count: Arc<AtomicIsize>,
}

impl Clone for RedbStorageDB {
    fn clone(&self) -> Self {
        RedbStorageDB {
            db: self.db.clone(),
            cmd_tx: self.cmd_tx.clone(),
            active_count: self.active_count.clone(),
        }
    }
}

impl fmt::Debug for RedbStorageDB {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RedbStorageDB").finish()
    }
}

impl RedbStorageDB {
    /// Opens a Redb database with the given configuration
    pub fn open(cfg: &RedbConfig) -> Result<Self> {
        // Ensure parent directory exists
        if let Some(parent) = std::path::Path::new(&cfg.path).parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }

        let mut builder = redb::Builder::new();
        builder.set_cache_size(cfg.cache_size);

        // Auto-detect: open existing file, create if missing
        let db = if std::path::Path::new(&cfg.path).exists() {
            builder.open(&cfg.path)?
        } else {
            builder.create(&cfg.path)?
        };

        // Initialize all tables by opening them in a write transaction.
        // This ensures they exist before any read transaction attempts to open them.
        {
            let txn = db.begin_write()?;
            let _ = txn.open_table(KV_TABLE);
            let _ = txn.open_table(MAP_TABLE);
            let _ = txn.open_table(LIST_TABLE);
            let _ = txn.open_table(EXPIRE_KEYS_TABLE);
            let _ = txn.open_table(KEY_EXPIRE_TABLE);
            txn.commit()?;
        }

        let db = Arc::new(db);
        let active_count = Arc::new(AtomicIsize::new(0));
        let (cmd_tx, cmd_rx) = mpsc::channel::<Command>(CHANNEL_CAPACITY);

        Self::start_background_thread(db.clone(), cmd_rx, active_count.clone());

        let storage = RedbStorageDB {
            db,
            cmd_tx,
            active_count,
        };

        (cfg.cleanup_f)(&storage);

        Ok(storage)
    }

    /// Cleans up expired keys (TTL feature)
    #[cfg(feature = "ttl")]
    #[inline]
    pub fn cleanup(&self, limit: usize) -> usize {
        Self::exec_cleanup(&self.db, limit).unwrap_or(0)
    }

    /// Returns the count of active commands
    #[inline]
    pub fn active_count(&self) -> isize {
        self.active_count.load(Ordering::Relaxed)
    }

    /// Sends a command and awaits the response (via closure to construct typed command)
    async fn send_cmd<R, F>(&self, make_cmd: F) -> Result<R>
    where
        R: Send + 'static,
        F: FnOnce(oneshot::Sender<Result<R>>) -> Command,
    {
        let (tx, rx) = oneshot::channel();
        let cmd = make_cmd(tx);
        self.cmd_tx
            .send(cmd)
            .await
            .map_err(|_| anyhow!("redb command channel closed"))?;
        rx.await
            .map_err(|_| anyhow!("redb command response channel dropped"))?
    }

    fn start_background_thread(
        db: Arc<Database>,
        mut rx: mpsc::Receiver<Command>,
        active_count: Arc<AtomicIsize>,
    ) {
        spawn_blocking(move || {
            let _handle = Handle::current();
            'outer: while let Some(cmd) = rx.blocking_recv() {
                active_count.fetch_add(1, Ordering::Release);
                match cmd {
                    // ============ DB Operations ============
                    Command::DBInsert { key, val, tx } => {
                        let result =
                            Self::exec_db_insert(&db, &key, &val).context("redb: db_insert");
                        let _ = tx.send(result);
                    }
                    Command::DBGet { key, tx } => {
                        let result = Self::exec_db_get(&db, &key).context("redb: db_get");
                        let _ = tx.send(result);
                    }
                    Command::DBRemove { key, tx } => {
                        let result = Self::exec_db_remove(&db, &key).context("redb: db_remove");
                        let _ = tx.send(result);
                    }
                    Command::DBContainsKey { key, tx } => {
                        let result =
                            Self::exec_db_contains_key(&db, &key).context("redb: db_contains_key");
                        let _ = tx.send(result);
                    }
                    Command::DBBatchInsert { key_vals, tx } => {
                        let result = Self::exec_db_batch_insert(&db, key_vals)
                            .context("redb: db_batch_insert");
                        let _ = tx.send(result);
                    }
                    Command::DBBatchRemove { keys, tx } => {
                        let result =
                            Self::exec_db_batch_remove(&db, keys).context("redb: db_batch_remove");
                        let _ = tx.send(result);
                    }
                    Command::DBCounterIncr { key, increment, tx } => {
                        let result = Self::exec_counter_incr(&db, &key, increment)
                            .context("redb: counter_incr");
                        let _ = tx.send(result);
                    }
                    Command::DBCounterDecr { key, decrement, tx } => {
                        let result = Self::exec_counter_decr(&db, &key, decrement)
                            .context("redb: counter_decr");
                        let _ = tx.send(result);
                    }
                    Command::DBCounterGet { key, tx } => {
                        let result = Self::exec_counter_get(&db, &key).context("redb: counter_get");
                        let _ = tx.send(result);
                    }
                    Command::DBCounterSet { key, val, tx } => {
                        let result =
                            Self::exec_counter_set(&db, &key, val).context("redb: counter_set");
                        let _ = tx.send(result);
                    }
                    Command::DBLen { tx } => {
                        let result = Self::exec_db_len(&db).context("redb: db_len");
                        let _ = tx.send(result);
                    }
                    Command::DBSize { tx } => {
                        let result = Self::exec_db_size(&db).context("redb: db_size");
                        let _ = tx.send(result);
                    }
                    Command::DBInfo { tx } => {
                        let result = Self::exec_db_info(&db).context("redb: db_info");
                        let _ = tx.send(result);
                    }

                    // ============ Map/List Container Management ============
                    Command::DBMapGet { key, tx } => {
                        let result = Self::exec_db_map_contains_key(&db, &key)
                            .context("redb: db_map_contains_key");
                        let _ = tx.send(result);
                    }
                    Command::DBListGet { key, tx } => {
                        let result = Self::exec_db_list_contains_key(&db, &key)
                            .context("redb: db_list_contains_key");
                        let _ = tx.send(result);
                    }

                    // ============ Iterators ============
                    Command::DBMapIter { tx } => {
                        let result = Self::exec_db_map_iter(&db).context("redb: db_map_iter");
                        let _ = tx.send(result);
                    }
                    Command::DBListIter { tx } => {
                        let result = Self::exec_db_list_iter(&db).context("redb: db_list_iter");
                        let _ = tx.send(result);
                    }
                    Command::DBScanIter { pattern, tx } => {
                        let result = Self::exec_db_scan(&db, &pattern).context("redb: db_scan");
                        let _ = tx.send(result);
                    }

                    // ============ TTL ============
                    #[cfg(feature = "ttl")]
                    Command::DBExpireAt { key, at, tx } => {
                        let result =
                            Self::exec_db_expire_at(&db, &key, at).context("redb: db_expire_at");
                        let _ = tx.send(result);
                    }
                    #[cfg(feature = "ttl")]
                    Command::DBExpire { key, dur, tx } => {
                        let result =
                            Self::exec_db_expire(&db, &key, dur).context("redb: db_expire");
                        let _ = tx.send(result);
                    }
                    #[cfg(feature = "ttl")]
                    Command::DBTtl { key, tx } => {
                        let result = Self::exec_db_ttl(&db, &key).context("redb: db_ttl");
                        let _ = tx.send(result);
                    }
                    #[cfg(feature = "ttl")]
                    Command::DBCleanup { tx } => {
                        let result = Self::exec_cleanup(&db, 1000).context("redb: cleanup");
                        let _ = tx.send(result);
                    }

                    // ============ Map Operations ============
                    Command::MapInsert { map, key, val, tx } => {
                        let result = Self::exec_map_insert(&db, &map.name, &key, &val)
                            .context("redb: map_insert");
                        let _ = tx.send(result);
                    }
                    Command::MapGet { map, key, tx } => {
                        let result =
                            Self::exec_map_get(&db, &map.name, &key).context("redb: map_get");
                        let _ = tx.send(result);
                    }
                    Command::MapRemove { map, key, tx } => {
                        let result =
                            Self::exec_map_remove(&db, &map.name, &key).context("redb: map_remove");
                        let _ = tx.send(result);
                    }
                    Command::MapContainsKey { map, key, tx } => {
                        let result = Self::exec_map_contains_key(&db, &map.name, &key)
                            .context("redb: map_contains_key");
                        let _ = tx.send(result);
                    }
                    #[cfg(feature = "map_len")]
                    Command::MapLen { map, tx } => {
                        let result = Self::exec_map_len(&db, &map.name).context("redb: map_len");
                        let _ = tx.send(result);
                    }
                    Command::MapIsEmpty { map, tx } => {
                        let result =
                            Self::exec_map_is_empty(&db, &map.name).context("redb: map_is_empty");
                        let _ = tx.send(result);
                    }
                    Command::MapClear { map, tx } => {
                        let result =
                            Self::exec_map_clear(&db, &map.name).context("redb: map_clear");
                        let _ = tx.send(result);
                    }
                    Command::MapRemoveAndFetch { map, key, tx } => {
                        let result = Self::exec_map_remove_and_fetch(&db, &map.name, &key)
                            .context("redb: map_remove_and_fetch");
                        let _ = tx.send(result);
                    }
                    Command::MapRemoveWithPrefix { map, prefix, tx } => {
                        let result = Self::exec_map_remove_with_prefix(&db, &map.name, &prefix)
                            .context("redb: map_remove_with_prefix");
                        let _ = tx.send(result);
                    }
                    Command::MapBatchInsert { map, key_vals, tx } => {
                        let result = Self::exec_map_batch_insert(&db, &map.name, key_vals)
                            .context("redb: map_batch_insert");
                        let _ = tx.send(result);
                    }
                    Command::MapBatchRemove { map, keys, tx } => {
                        let result = Self::exec_map_batch_remove(&db, &map.name, keys)
                            .context("redb: map_batch_remove");
                        let _ = tx.send(result);
                    }
                    Command::MapIter { map, tx } => {
                        let result = Self::exec_map_iter(&db, &map.name).context("redb: map_iter");
                        let _ = tx.send(result);
                    }
                    Command::MapKeyIter { map, tx } => {
                        let result =
                            Self::exec_map_key_iter(&db, &map.name).context("redb: map_key_iter");
                        let _ = tx.send(result);
                    }
                    Command::MapPrefixIter { map, prefix, tx } => {
                        let result = Self::exec_map_prefix_iter(&db, &map.name, &prefix)
                            .context("redb: map_prefix_iter");
                        let _ = tx.send(result);
                    }
                    #[cfg(feature = "ttl")]
                    Command::MapExpireAt { map, at, tx } => {
                        let result = Self::exec_map_expire_at(&db, &map.name, at)
                            .context("redb: map_expire_at");
                        let _ = tx.send(result);
                    }
                    #[cfg(feature = "ttl")]
                    Command::MapExpire { map, dur, tx } => {
                        let result =
                            Self::exec_map_expire(&db, &map.name, dur).context("redb: map_expire");
                        let _ = tx.send(result);
                    }
                    #[cfg(feature = "ttl")]
                    Command::MapTTL { map, tx } => {
                        let result = Self::exec_map_ttl(&db, &map.name).context("redb: map_ttl");
                        let _ = tx.send(result);
                    }
                    Command::MapIsExpired { map, tx } => {
                        let result = Self::exec_map_is_expired(&db, &map.name)
                            .context("redb: map_is_expired");
                        let _ = tx.send(result);
                    }

                    // ============ List Operations ============
                    Command::ListPush { list, val, tx } => {
                        let result =
                            Self::exec_list_push(&db, &list.name, &val).context("redb: list_push");
                        let _ = tx.send(result);
                    }
                    Command::ListPushs { list, vals, tx } => {
                        let result = Self::exec_list_pushs(&db, &list.name, vals)
                            .context("redb: list_pushs");
                        let _ = tx.send(result);
                    }
                    Command::ListPushLimit {
                        list,
                        val,
                        limit,
                        pop_front_if_limited,
                        tx,
                    } => {
                        let result = Self::exec_list_push_limit(
                            &db,
                            &list.name,
                            &val,
                            limit,
                            pop_front_if_limited,
                        )
                        .context("redb: list_push_limit");
                        let _ = tx.send(result);
                    }
                    Command::ListPop { list, tx } => {
                        let result = Self::exec_list_pop(&db, &list.name).context("redb: list_pop");
                        let _ = tx.send(result);
                    }
                    Command::ListAll { list, tx } => {
                        let result = Self::exec_list_all(&db, &list.name).context("redb: list_all");
                        let _ = tx.send(result);
                    }
                    Command::ListGetIndex { list, idx, tx } => {
                        let result = Self::exec_list_get_index(&db, &list.name, idx)
                            .context("redb: list_get_index");
                        let _ = tx.send(result);
                    }
                    Command::ListLen { list, tx } => {
                        let result = Self::exec_list_len(&db, &list.name).context("redb: list_len");
                        let _ = tx.send(result);
                    }
                    Command::ListIsEmpty { list, tx } => {
                        let result = Self::exec_list_is_empty(&db, &list.name)
                            .context("redb: list_is_empty");
                        let _ = tx.send(result);
                    }
                    Command::ListClear { list, tx } => {
                        let result =
                            Self::exec_list_clear(&db, &list.name).context("redb: list_clear");
                        let _ = tx.send(result);
                    }
                    Command::ListIter { list, tx } => {
                        let result =
                            Self::exec_list_iter(&db, &list.name).context("redb: list_iter");
                        let _ = tx.send(result);
                    }
                    #[cfg(feature = "ttl")]
                    Command::ListExpireAt { list, at, tx } => {
                        let result = Self::exec_list_expire_at(&db, &list.name, at)
                            .context("redb: list_expire_at");
                        let _ = tx.send(result);
                    }
                    #[cfg(feature = "ttl")]
                    Command::ListExpire { list, dur, tx } => {
                        let result = Self::exec_list_expire(&db, &list.name, dur)
                            .context("redb: list_expire");
                        let _ = tx.send(result);
                    }
                    #[cfg(feature = "ttl")]
                    Command::ListTTL { list, tx } => {
                        let result = Self::exec_list_ttl(&db, &list.name).context("redb: list_ttl");
                        let _ = tx.send(result);
                    }
                    Command::ListIsExpired { list, tx } => {
                        let result = Self::exec_list_is_expired(&db, &list.name)
                            .context("redb: list_is_expired");
                        let _ = tx.send(result);
                    }

                    // ============ Raw Operations (circuit-breaker) ============
                    #[cfg(feature = "circuit-breaker")]
                    Command::DBInsertRaw { key, val, tx } => {
                        let result =
                            Self::exec_db_insert(&db, &key, &val).context("redb: db_insert_raw");
                        let _ = tx.send(result);
                    }
                    #[cfg(feature = "circuit-breaker")]
                    Command::DBGetRaw { key, tx } => {
                        let result = Self::exec_db_get(&db, &key).context("redb: db_get_raw");
                        let _ = tx.send(result);
                    }
                    #[cfg(feature = "circuit-breaker")]
                    Command::DBRemoveRaw { key, tx } => {
                        let result = Self::exec_db_remove(&db, &key).context("redb: db_remove_raw");
                        let _ = tx.send(result);
                    }

                    Command::Shutdown => break 'outer,
                }
                active_count.fetch_sub(1, Ordering::Release);
            }
        });
    }

    // ========================================================================
    // Internal executors — each runs in spawn_blocking, uses redb transactions
    // ========================================================================

    fn exec_db_insert(db: &Database, key: &[u8], val: &[u8]) -> Result<()> {
        let txn = db.begin_write()?;
        {
            let mut table = txn.open_table(KV_TABLE)?;
            table.insert(key, val)?;
            // Sled behavior: remove existing TTL entry so key becomes TTL-less
            #[cfg(feature = "ttl")]
            Self::clear_key_ttl_in_txn(&txn, key)?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Batch insert multiple KV pairs in a single transaction.
    /// Takes ownership of the batch items so we can send responses.
    #[allow(dead_code)]
    fn exec_db_insert_batch(db: &Database, batch: &mut Vec<BatchInsertItem>) {
        let txn = match db.begin_write() {
            Ok(t) => t,
            Err(e) => {
                for (_, _, tx) in batch.drain(..) {
                    let _ = tx.send(Err(anyhow::format_err!("{:?}", e)));
                }
                return;
            }
        };
        let mut kv_table = match txn.open_table(KV_TABLE) {
            Ok(t) => t,
            Err(e) => {
                for (_, _, tx) in batch.drain(..) {
                    let _ = tx.send(Err(anyhow::format_err!("{:?}", e)));
                }
                return;
            }
        };
        #[cfg(feature = "ttl")]
        let mut ke_table = match txn.open_table(KEY_EXPIRE_TABLE) {
            Ok(t) => t,
            Err(e) => {
                for (_, _, tx) in batch.drain(..) {
                    let _ = tx.send(Err(anyhow::format_err!("{:?}", e)));
                }
                return;
            }
        };
        for (key, val, tx) in batch.drain(..) {
            if let Err(e) = kv_table.insert(key.as_slice(), val.as_slice()) {
                let _ = tx.send(Err(anyhow::format_err!("{:?}", e)));
                continue;
            }
            #[cfg(feature = "ttl")]
            if let Ok(encoded) = postcard::to_stdvec(&TimestampMillis::MAX) {
                if let Err(e) = ke_table.insert(key.as_slice(), encoded.as_slice()) {
                    let _ = tx.send(Err(anyhow::format_err!("{:?}", e)));
                    continue;
                }
            }
            let _ = tx.send(Ok(()));
        }
        drop(kv_table);
        #[cfg(feature = "ttl")]
        {
            drop(ke_table);
        }
        if let Err(e) = txn.commit() {
            // All responses already sent, but commit failed
            eprintln!("redb batch commit failed: {:?}", e);
        }
    }

    fn exec_db_get(db: &Database, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let txn = db.begin_read()?;
        let table = txn.open_table(KV_TABLE)?;
        #[cfg(feature = "ttl")]
        {
            if let Some(expire_at_bytes) = txn.open_table(KEY_EXPIRE_TABLE)?.get(key)? {
                let expire_at =
                    i64::from_be_bytes(expire_at_bytes.value().try_into().unwrap_or([0; 8]));
                if expire_at <= timestamp_millis() {
                    return Ok(None);
                }
            }
        }
        match table.get(key)? {
            Some(guard) => {
                let val = guard.value().to_vec();
                Ok(Some(val))
            }
            None => Ok(None),
        }
    }

    fn exec_db_remove(db: &Database, key: &[u8]) -> Result<()> {
        let txn = db.begin_write()?;
        {
            let mut table = txn.open_table(KV_TABLE)?;
            table.remove(key)?;
            Self::clear_key_ttl_in_txn(&txn, key)?;
        }
        txn.commit()?;
        Ok(())
    }

    fn exec_db_contains_key(db: &Database, key: &[u8]) -> Result<bool> {
        let txn = db.begin_read()?;
        #[cfg(feature = "ttl")]
        {
            if let Ok(kv) = txn.open_table(KV_TABLE)?.get(key) {
                if kv.is_some() {
                    if let Some(expire_at_bytes) = txn.open_table(KEY_EXPIRE_TABLE)?.get(key)? {
                        let expire_at = i64::from_be_bytes(
                            expire_at_bytes.value().try_into().unwrap_or([0; 8]),
                        );
                        if expire_at <= timestamp_millis() {
                            return Ok(false);
                        }
                    }
                    return Ok(true);
                }
            }
            Ok(false)
        }
        #[cfg(not(feature = "ttl"))]
        {
            let table = txn.open_table(KV_TABLE)?;
            Ok(table.get(key)?.is_some())
        }
    }

    fn exec_db_batch_insert(db: &Database, key_vals: Vec<(Vec<u8>, Vec<u8>)>) -> Result<()> {
        let txn = db.begin_write()?;
        {
            let mut table = txn.open_table(KV_TABLE)?;
            for (key, val) in key_vals {
                table.insert(key.as_slice(), val.as_slice())?;
                // Sled behavior: remove any existing TTL on each key
                #[cfg(feature = "ttl")]
                Self::clear_key_ttl_in_txn(&txn, &key)?;
            }
        }
        txn.commit()?;
        Ok(())
    }

    fn exec_db_batch_remove(db: &Database, keys: Vec<Vec<u8>>) -> Result<()> {
        let txn = db.begin_write()?;
        {
            let mut table = txn.open_table(KV_TABLE)?;
            for key in keys {
                table.remove(key.as_slice())?;
                Self::clear_key_ttl_in_txn(&txn, &key)?;
            }
        }
        txn.commit()?;
        Ok(())
    }

    fn exec_counter_incr(db: &Database, key: &[u8], increment: isize) -> Result<()> {
        let txn = db.begin_write()?;
        {
            let mut table = txn.open_table(KV_TABLE)?;
            let current: isize = table
                .get(key)?
                .and_then(|g| {
                    <[u8; 8]>::try_from(g.value())
                        .ok()
                        .map(isize::from_be_bytes)
                })
                .unwrap_or(0);
            let new = current + increment;
            table.insert(key, &new.to_be_bytes()[..])?;
        }
        txn.commit()?;
        Ok(())
    }

    fn exec_counter_decr(db: &Database, key: &[u8], decrement: isize) -> Result<()> {
        Self::exec_counter_incr(db, key, -decrement)
    }

    fn exec_counter_get(db: &Database, key: &[u8]) -> Result<Option<isize>> {
        let txn = db.begin_read()?;
        let table = txn.open_table(KV_TABLE)?;
        match table.get(key)? {
            Some(g) => {
                let arr: [u8; 8] = <[u8; 8]>::try_from(g.value()).unwrap_or([0; 8]);
                let val = isize::from_be_bytes(arr);
                Ok(Some(val))
            }
            None => Ok(None),
        }
    }

    fn exec_counter_set(db: &Database, key: &[u8], val: isize) -> Result<()> {
        let txn = db.begin_write()?;
        {
            let mut table = txn.open_table(KV_TABLE)?;
            table.insert(key, &val.to_be_bytes()[..])?;
        }
        txn.commit()?;
        Ok(())
    }

    fn exec_db_len(db: &Database) -> Result<usize> {
        #[cfg(feature = "len")]
        {
            let txn = db.begin_read()?;
            let kv_table = txn.open_table(KV_TABLE)?;
            let mut count = 0usize;
            for result in kv_table.iter()? {
                let (key, _) = result?;
                let k = key.value();
                if k.starts_with(COUNTER_PREFIX) || k.starts_with(KEY_PREFIX) {
                    continue;
                }
                count += 1;
            }
            Ok(count)
        }
        #[cfg(not(feature = "len"))]
        {
            let _ = db;
            Ok(0)
        }
    }

    fn exec_db_size(db: &Database) -> Result<usize> {
        let txn = db.begin_read()?;
        let kv_table = txn.open_table(KV_TABLE)?;
        let map_table = txn.open_table(MAP_TABLE)?;
        let list_table = txn.open_table(LIST_TABLE)?;
        let kv_count = kv_table.len()? as usize;
        let map_count = map_table.len()? as usize;
        let list_count = list_table.len()? as usize;
        Ok(kv_count + map_count + list_count)
    }

    fn exec_db_info(_db: &Database) -> Result<Value> {
        Ok(serde_json::json!({
            "storage_engine": "Redb",
        }))
    }

    fn exec_db_map_contains_key(db: &Database, name: &[u8]) -> Result<bool> {
        let txn = db.begin_read()?;
        #[cfg(feature = "ttl")]
        if let Ok(table) = txn.open_table(KEY_EXPIRE_TABLE) {
            if let Some(guard) = table.get(name)? {
                let expire_at: TimestampMillis = postcard::from_bytes(guard.value())?;
                if expire_at <= timestamp_millis() {
                    return Ok(false);
                }
            }
        }
        let table = txn.open_table(MAP_TABLE)?;
        let count_key = make_map_count_key(name);
        Ok(table.get(count_key.as_slice())?.is_some())
    }

    fn exec_db_list_contains_key(db: &Database, name: &[u8]) -> Result<bool> {
        let txn = db.begin_read()?;
        #[cfg(feature = "ttl")]
        if let Ok(table) = txn.open_table(KEY_EXPIRE_TABLE) {
            if let Some(guard) = table.get(name)? {
                let expire_at: TimestampMillis = postcard::from_bytes(guard.value())?;
                if expire_at <= timestamp_millis() {
                    return Ok(false);
                }
            }
        }
        let table = txn.open_table(LIST_TABLE)?;
        let count_key = make_list_count_key(name);
        Ok(table.get(count_key.as_slice())?.is_some())
    }

    fn exec_db_map_iter(db: &Database) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let txn = db.begin_read()?;
        let table = txn.open_table(MAP_TABLE)?;
        let mut results = Vec::new();
        for result in table.iter()? {
            let (key, val) = result?;
            let k = key.value().to_vec();
            if is_map_count_key(&k) {
                results.push((k, val.value().to_vec()));
            }
        }
        Ok(results)
    }

    fn exec_db_list_iter(db: &Database) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let txn = db.begin_read()?;
        let table = txn.open_table(LIST_TABLE)?;
        let mut results = Vec::new();
        for result in table.iter()? {
            let (key, val) = result?;
            let k = key.value().to_vec();
            if is_list_count_key(&k) {
                results.push((k, val.value().to_vec()));
            }
        }
        Ok(results)
    }

    fn exec_db_scan(db: &Database, pattern: &[u8]) -> Result<Vec<Vec<u8>>> {
        let txn = db.begin_read()?;
        let table = txn.open_table(KV_TABLE)?;
        let pattern = Pattern::from(pattern);
        let mut results = Vec::new();
        for result in table.iter()? {
            let (key, _) = result?;
            let k = key.value();
            if k.starts_with(COUNTER_PREFIX) {
                continue;
            }
            if is_match(pattern.clone(), k) {
                results.push(k.to_vec());
            }
        }
        // Also scan map and list tables for container names
        let map_table = txn.open_table(MAP_TABLE)?;
        for result in map_table.iter()? {
            let (key, _) = result?;
            let k = key.value();
            if is_map_count_key(k) && is_match(pattern.clone(), k) {
                results.push(k.to_vec());
            }
        }
        let list_table = txn.open_table(LIST_TABLE)?;
        for result in list_table.iter()? {
            let (key, _) = result?;
            let k = key.value();
            if is_list_count_key(k) && is_match(pattern.clone(), k) {
                results.push(k.to_vec());
            }
        }
        Ok(results)
    }

    // ---- TTL helpers ----

    #[cfg(feature = "ttl")]
    fn clear_key_ttl_in_txn(txn: &WriteTransaction, key: &[u8]) -> Result<()> {
        // Read old expire info, then use mutable handles for writes
        let mut ke_tbl = txn.open_table(KEY_EXPIRE_TABLE)?;
        let old_expire_bytes: Option<Vec<u8>> = ke_tbl.get(key)?.map(|g| g.value().to_vec());
        if let Some(ref expire_bytes) = old_expire_bytes {
            let mut ek = Vec::with_capacity(8 + key.len());
            ek.extend_from_slice(expire_bytes);
            ek.extend_from_slice(key);
            // Write: remove from both tables
            let mut ek_tbl = txn.open_table(EXPIRE_KEYS_TABLE)?;
            ek_tbl.remove(ek.as_slice())?;
            ke_tbl.remove(key)?;
        }
        Ok(())
    }

    #[cfg(not(feature = "ttl"))]
    fn clear_key_ttl_in_txn(_txn: &WriteTransaction, _key: &[u8]) -> Result<()> {
        Ok(())
    }

    #[cfg(feature = "ttl")]
    fn set_key_ttl_in_txn(
        txn: &WriteTransaction,
        key: &[u8],
        expire_at: TimestampMillis,
    ) -> Result<bool> {
        // Prepare data upfront
        let at_bytes = expire_at.to_be_bytes();
        let mut ek = Vec::with_capacity(8 + key.len());
        ek.extend_from_slice(&at_bytes);
        ek.extend_from_slice(key);

        // Use a single mutable handle for KEY_EXPIRE_TABLE
        let mut ke_tbl = txn.open_table(KEY_EXPIRE_TABLE)?;
        let old_expire_data: Option<Vec<u8>> = ke_tbl.get(key)?.map(|g| g.value().to_vec());
        if let Some(ref old_bytes) = old_expire_data {
            let mut old_ek = Vec::with_capacity(8 + key.len());
            old_ek.extend_from_slice(old_bytes);
            old_ek.extend_from_slice(key);
            // Write: remove old TTL from expire_keys
            let mut ek_tbl = txn.open_table(EXPIRE_KEYS_TABLE)?;
            ek_tbl.remove(old_ek.as_slice())?;
        }

        // Write new TTL
        ke_tbl.insert(key, &at_bytes[..])?;
        let mut ek_tbl = txn.open_table(EXPIRE_KEYS_TABLE)?;
        ek_tbl.insert(ek.as_slice(), &b"kv"[..])?;

        Ok(true)
    }

    #[cfg(feature = "ttl")]
    fn exec_db_expire_at(db: &Database, key: &[u8], at: TimestampMillis) -> Result<bool> {
        let txn = db.begin_read()?;
        let exists = {
            let kv = txn.open_table(KV_TABLE)?.get(key)?.is_some()
                || txn
                    .open_table(MAP_TABLE)?
                    .get(make_map_count_key(key).as_slice())?
                    .is_some()
                || txn
                    .open_table(LIST_TABLE)?
                    .get(make_list_count_key(key).as_slice())?
                    .is_some();
            kv
        };
        drop(txn);
        if !exists {
            return Ok(false);
        }
        let txn = db.begin_write()?;
        {
            Self::set_key_ttl_in_txn(&txn, key, at)?;
        }
        txn.commit()?;
        Ok(true)
    }

    #[cfg(feature = "ttl")]
    fn exec_db_expire(db: &Database, key: &[u8], dur: TimestampMillis) -> Result<bool> {
        let at = timestamp_millis() + dur;
        Self::exec_db_expire_at(db, key, at)
    }

    #[cfg(feature = "ttl")]
    fn exec_db_ttl(db: &Database, key: &[u8]) -> Result<Option<TimestampMillis>> {
        let txn = db.begin_read()?;
        let ke_table = txn.open_table(KEY_EXPIRE_TABLE)?;
        match ke_table.get(key)? {
            Some(guard) => {
                let expire_at = i64::from_be_bytes(guard.value().try_into().unwrap_or([0; 8]));
                let now = timestamp_millis();
                if expire_at <= now {
                    Ok(None)
                } else {
                    Ok(Some(expire_at - now))
                }
            }
            None => {
                // No TTL entry: check if key exists in ANY data table
                // (sled behavior: key exists but no TTL = MAX TTL)
                let kv = txn.open_table(KV_TABLE)?;
                if kv.get(key)?.is_some() {
                    return Ok(Some(TimestampMillis::MAX));
                }
                let mk = make_map_count_key(key);
                if txn.open_table(MAP_TABLE)?.get(mk.as_slice())?.is_some() {
                    return Ok(Some(TimestampMillis::MAX));
                }
                let lk = make_list_count_key(key);
                if txn.open_table(LIST_TABLE)?.get(lk.as_slice())?.is_some() {
                    return Ok(Some(TimestampMillis::MAX));
                }
                Ok(None)
            }
        }
    }

    #[cfg(feature = "ttl")]
    fn exec_cleanup(db: &Database, limit: usize) -> Result<usize> {
        let now = timestamp_millis();
        let mut count = 0usize;

        // Phase 1 — Read transaction: scan expired keys only.
        // No fsync, no write lock held.
        let to_remove: Vec<Vec<u8>> = {
            let txn = db.begin_read()?;
            let expire_keys = txn.open_table(EXPIRE_KEYS_TABLE)?;
            expire_keys
                .range::<&[u8]>(..)?
                .filter_map(|r| {
                    r.ok().and_then(|(k, _)| {
                        let key_bytes = k.value();
                        if key_bytes.len() < 8 {
                            return None;
                        }
                        let expire_at = u64::from_be_bytes(
                            <[u8; 8]>::try_from(&key_bytes[..8]).unwrap_or([0; 8]),
                        ) as TimestampMillis;
                        if expire_at <= now {
                            // Copy out of the read transaction before it drops
                            Some(key_bytes.to_vec())
                        } else {
                            None
                        }
                    })
                })
                .take(limit)
                .collect()
            // read transaction drops here → no fsync
        };

        if to_remove.is_empty() {
            return Ok(0);
        }

        // Phase 2 — Write transaction: delete expired keys (single fsync).
        let txn = db.begin_write()?;
        {
            let mut expire_keys = txn.open_table(EXPIRE_KEYS_TABLE)?;
            let mut key_expire = txn.open_table(KEY_EXPIRE_TABLE)?;
            let mut kv_table = txn.open_table(KV_TABLE)?;
            let mut map_table = txn.open_table(MAP_TABLE)?;
            let mut list_table = txn.open_table(LIST_TABLE)?;

            for expired_key in &to_remove {
                let (_, actual_key) = expired_key.split_at(8);
                let _ = kv_table.remove(actual_key)?;
                let _ = map_table.remove(actual_key)?;
                let _ = list_table.remove(actual_key)?;
                let _ = key_expire.remove(actual_key)?;
                let _ = expire_keys.remove(expired_key.as_slice())?;
                count += 1;
            }
        }
        txn.commit()?;
        Ok(count)
    }

    // ---- TTL check helper for containers (maps/lists) ----

    #[cfg(feature = "ttl")]
    fn is_container_expired(db: &Database, name: &[u8]) -> Result<bool> {
        let txn = db.begin_read()?;
        let expired = match txn.open_table(KEY_EXPIRE_TABLE)?.get(name)? {
            Some(guard) => {
                let expire_at = i64::from_be_bytes(guard.value().try_into().unwrap_or([0; 8]));
                expire_at <= timestamp_millis()
            }
            None => {
                // No TTL entry: container is NOT expired (sled behavior:
                // _is_expired returns false when key exists but no TTL entry,
                // which means MAX TTL semantics). The caller will check if
                // the container actually has data.
                false
            }
        };
        Ok(expired)
    }

    // ---- Map executors ----

    fn exec_map_insert(db: &Database, name: &[u8], key: &[u8], val: &[u8]) -> Result<()> {
        let txn = db.begin_write()?;
        {
            let mut table = txn.open_table(MAP_TABLE)?;
            let item_key = make_map_item_key(name, key);
            let is_new = table.insert(item_key.as_slice(), val)?.is_none();
            // Sled behavior: if container is expired, remove its TTL entry
            #[cfg(feature = "ttl")]
            {
                let mut ke = txn.open_table(KEY_EXPIRE_TABLE)?;
                let expired = ke.get(name)?.is_none_or(|g| {
                    i64::from_be_bytes(g.value().try_into().unwrap_or([0; 8])) <= timestamp_millis()
                });
                if expired {
                    // Inline clear_key_ttl_in_txn using the same ke handle
                    let old_bytes = ke.get(name)?.map(|g| g.value().to_vec());
                    if let Some(ref bytes) = old_bytes {
                        let mut ek = Vec::with_capacity(8 + name.len());
                        ek.extend_from_slice(bytes);
                        ek.extend_from_slice(name);
                        let mut ek_tbl = txn.open_table(EXPIRE_KEYS_TABLE)?;
                        ek_tbl.remove(ek.as_slice())?;
                        ke.remove(name)?;
                    }
                }
            }
            #[cfg(feature = "map_len")]
            if is_new {
                let count_key = make_map_count_key(name);
                let old = table
                    .get(count_key.as_slice())?
                    .map(|g| isize::from_be_bytes(g.value().try_into().unwrap_or([0; 8])))
                    .unwrap_or(0);
                let new_count = old + 1;
                table.insert(count_key.as_slice(), &new_count.to_be_bytes()[..])?;
            }
        }
        txn.commit()?;
        Ok(())
    }

    fn exec_map_get(db: &Database, name: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>> {
        let txn = db.begin_read()?;
        #[cfg(feature = "ttl")]
        {
            if let Some(expire_at_bytes) = txn.open_table(KEY_EXPIRE_TABLE)?.get(name)? {
                let expire_at =
                    i64::from_be_bytes(expire_at_bytes.value().try_into().unwrap_or([0; 8]));
                if expire_at <= timestamp_millis() {
                    return Ok(None);
                }
            }
        }
        let table = txn.open_table(MAP_TABLE)?;
        let item_key = make_map_item_key(name, key);
        match table.get(item_key.as_slice())? {
            Some(guard) => Ok(Some(guard.value().to_vec())),
            None => Ok(None),
        }
    }

    fn exec_map_remove(db: &Database, name: &[u8], key: &[u8]) -> Result<()> {
        let txn = db.begin_write()?;
        {
            let mut table = txn.open_table(MAP_TABLE)?;
            let item_key = make_map_item_key(name, key);
            let existed = table.remove(item_key.as_slice())?.is_some();
            #[cfg(feature = "map_len")]
            if existed {
                let count_key = make_map_count_key(name);
                let old = table
                    .get(count_key.as_slice())?
                    .map(|g| isize::from_be_bytes(g.value().try_into().unwrap_or([1; 8])))
                    .unwrap_or(1);
                let new_count = (old - 1).max(0);
                table.insert(count_key.as_slice(), &new_count.to_be_bytes()[..])?;
            }
        }
        txn.commit()?;
        Ok(())
    }

    fn exec_map_contains_key(db: &Database, name: &[u8], key: &[u8]) -> Result<bool> {
        let txn = db.begin_read()?;
        #[cfg(feature = "ttl")]
        if let Ok(table) = txn.open_table(KEY_EXPIRE_TABLE) {
            if let Some(guard) = table.get(name)? {
                let expire_at: TimestampMillis = postcard::from_bytes(guard.value())?;
                if expire_at <= timestamp_millis() {
                    return Ok(false);
                }
            }
        }
        let table = txn.open_table(MAP_TABLE)?;
        let item_key = make_map_item_key(name, key);
        Ok(table.get(item_key.as_slice())?.is_some())
    }

    #[cfg(feature = "map_len")]
    fn exec_map_len(db: &Database, name: &[u8]) -> Result<usize> {
        let txn = db.begin_read()?;
        #[cfg(feature = "ttl")]
        if let Ok(table) = txn.open_table(KEY_EXPIRE_TABLE) {
            if let Some(guard) = table.get(name)? {
                let expire_at = i64::from_be_bytes(guard.value().try_into().unwrap_or([0; 8]));
                if expire_at <= timestamp_millis() {
                    return Ok(0);
                }
            }
        }
        let table = txn.open_table(MAP_TABLE)?;
        let count_key = make_map_count_key(name);
        match table.get(count_key.as_slice())? {
            Some(g) => {
                let c = isize::from_be_bytes(g.value().try_into().unwrap_or([0; 8]));
                Ok(c.max(0) as usize)
            }
            None => Ok(0),
        }
    }

    fn exec_map_is_empty(db: &Database, name: &[u8]) -> Result<bool> {
        let txn = db.begin_read()?;
        #[cfg(feature = "ttl")]
        if let Ok(table) = txn.open_table(KEY_EXPIRE_TABLE) {
            if let Some(guard) = table.get(name)? {
                let expire_at: TimestampMillis = postcard::from_bytes(guard.value())?;
                if expire_at <= timestamp_millis() {
                    return Ok(true);
                }
            }
        }
        let table = txn.open_table(MAP_TABLE)?;
        let item_prefix = make_map_item_prefix(name);
        let mut iter = table.range(item_prefix.as_slice()..)?;
        Ok(iter.next().is_none())
    }

    fn exec_map_clear(db: &Database, name: &[u8]) -> Result<()> {
        let txn = db.begin_write()?;
        {
            let mut table = txn.open_table(MAP_TABLE)?;
            let item_prefix = make_map_item_prefix(name);
            {
                let extract = table.extract_from_if(item_prefix.as_slice().., |_, _| true)?;
                let _drained: Vec<_> = extract.collect::<Result<Vec<_>, _>>()?;
            }
            let count_key = make_map_count_key(name);
            let _ = table.remove(count_key.as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    fn exec_map_remove_and_fetch(
        db: &Database,
        name: &[u8],
        key: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        #[cfg(feature = "ttl")]
        if Self::is_container_expired(db, name)? {
            return Ok(None);
        }
        let txn = db.begin_write()?;
        let (result, existed);
        {
            let mut table = txn.open_table(MAP_TABLE)?;
            let item_key = make_map_item_key(name, key);
            let removed = table.remove(item_key.as_slice())?;
            result = match removed.map(|g| g.value().to_vec()) {
                Some(v) => Ok(Some(v)),
                None => Ok(None),
            };
            existed = result.as_ref().ok().and_then(|r| r.as_ref()).is_some();
        }
        #[cfg(feature = "map_len")]
        if existed {
            let mut table = txn.open_table(MAP_TABLE)?;
            let count_key = make_map_count_key(name);
            let old = table
                .get(count_key.as_slice())?
                .map(|g| isize::from_be_bytes(g.value().try_into().unwrap_or([1; 8])))
                .unwrap_or(1);
            let new_count = (old - 1).max(0);
            table.insert(count_key.as_slice(), &new_count.to_be_bytes()[..])?;
        }
        txn.commit()?;
        result
    }

    fn exec_map_remove_with_prefix(db: &Database, name: &[u8], prefix: &[u8]) -> Result<()> {
        let txn = db.begin_write()?;
        {
            let mut table = txn.open_table(MAP_TABLE)?;
            let item_prefix = make_map_item_key(name, prefix);
            let keys_to_remove: Vec<Vec<u8>> = table
                .range(item_prefix.as_slice()..)?
                .filter_map(|r| {
                    r.ok().and_then(|(k, _)| {
                        let k = k.value().to_vec();
                        if k.starts_with(item_prefix.as_slice()) {
                            Some(k)
                        } else {
                            None
                        }
                    })
                })
                .collect();
            for k in keys_to_remove {
                let _ = table.remove(k.as_slice())?;
            }
            #[cfg(feature = "map_len")]
            {
                let count_key = make_map_count_key(name);
                let item_prefix2 = make_map_item_prefix(name);
                let mut remaining = 0usize;
                for r in table.range(item_prefix2.as_slice()..)? {
                    let (k, _) = r?;
                    if !k.value().starts_with(item_prefix2.as_slice()) {
                        break;
                    }
                    if !k.value().ends_with(b"@__count@") {
                        remaining += 1;
                    }
                }
                let encoded = postcard::to_stdvec(&remaining)?;
                table.insert(count_key.as_slice(), encoded.as_slice())?;
            }
        }
        txn.commit()?;
        Ok(())
    }

    fn exec_map_batch_insert(
        db: &Database,
        name: &[u8],
        key_vals: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<()> {
        let txn = db.begin_write()?;
        {
            let mut table = txn.open_table(MAP_TABLE)?;
            #[cfg(feature = "map_len")]
            let mut new_count: usize = 0;
            for (key, val) in key_vals {
                let item_key = make_map_item_key(name, &key);
                let is_new = table.insert(item_key.as_slice(), val.as_slice())?.is_none();
                #[cfg(feature = "map_len")]
                if is_new {
                    new_count += 1;
                }
            }
            #[cfg(feature = "map_len")]
            if new_count > 0 {
                let count_key = make_map_count_key(name);
                let old = table
                    .get(count_key.as_slice())?
                    .map(|g| isize::from_be_bytes(g.value().try_into().unwrap_or([0; 8])))
                    .unwrap_or(0);
                let total = old + new_count as isize;
                table.insert(count_key.as_slice(), &total.to_be_bytes()[..])?;
            }
        }
        txn.commit()?;
        Ok(())
    }

    fn exec_map_batch_remove(db: &Database, name: &[u8], keys: Vec<Vec<u8>>) -> Result<()> {
        let txn = db.begin_write()?;
        {
            let mut table = txn.open_table(MAP_TABLE)?;
            #[cfg(feature = "map_len")]
            let mut removed_count: usize = 0;
            for key in keys {
                let item_key = make_map_item_key(name, &key);
                let existed = table.remove(item_key.as_slice())?.is_some();
                #[cfg(feature = "map_len")]
                if existed {
                    removed_count += 1;
                }
            }
            #[cfg(feature = "map_len")]
            if removed_count > 0 {
                let count_key = make_map_count_key(name);
                let old = table
                    .get(count_key.as_slice())?
                    .map(|g| isize::from_be_bytes(g.value().try_into().unwrap_or([0; 8])))
                    .unwrap_or(0);
                let total = (old - removed_count as isize).max(0);
                table.insert(count_key.as_slice(), &total.to_be_bytes()[..])?;
            }
        }
        txn.commit()?;
        Ok(())
    }

    fn exec_map_iter(db: &Database, name: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        #[cfg(feature = "ttl")]
        if Self::is_container_expired(db, name)? {
            return Ok(Vec::new());
        }
        let txn = db.begin_read()?;
        let table = txn.open_table(MAP_TABLE)?;
        let item_prefix = make_map_item_prefix(name);
        let mut results = Vec::new();
        for result in table.range(item_prefix.as_slice()..)? {
            let (key, val) = result?;
            let k = key.value();
            // Stop when we leave this map's prefix
            if !k.starts_with(item_prefix.as_slice()) {
                break;
            }
            let inner_key = k[item_prefix.len()..].to_vec();
            results.push((inner_key, val.value().to_vec()));
        }
        Ok(results)
    }

    fn exec_map_key_iter(db: &Database, name: &[u8]) -> Result<Vec<Vec<u8>>> {
        #[cfg(feature = "ttl")]
        if Self::is_container_expired(db, name)? {
            return Ok(Vec::new());
        }
        let txn = db.begin_read()?;
        let table = txn.open_table(MAP_TABLE)?;
        let item_prefix = make_map_item_prefix(name);
        let mut results = Vec::new();
        for result in table.range(item_prefix.as_slice()..)? {
            let (key, _) = result?;
            let k = key.value();
            if !k.starts_with(item_prefix.as_slice()) {
                break;
            }
            results.push(k[item_prefix.len()..].to_vec());
        }
        Ok(results)
    }

    fn exec_map_prefix_iter(
        db: &Database,
        name: &[u8],
        prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        #[cfg(feature = "ttl")]
        if Self::is_container_expired(db, name)? {
            return Ok(Vec::new());
        }
        let txn = db.begin_read()?;
        let table = txn.open_table(MAP_TABLE)?;
        let start_key = make_map_item_key(name, prefix);
        let prefix_key = make_map_item_prefix(name);
        let mut results = Vec::new();
        for result in table.range(start_key.as_slice()..)? {
            let (key, val) = result?;
            let k = key.value();
            if !k.starts_with(prefix_key.as_slice()) {
                break;
            }
            if !k.starts_with(start_key.as_slice()) {
                break;
            }
            let inner_key = k[prefix_key.len()..].to_vec();
            results.push((inner_key, val.value().to_vec()));
        }
        Ok(results)
    }

    #[cfg(feature = "ttl")]
    fn exec_map_expire_at(db: &Database, name: &[u8], at: TimestampMillis) -> Result<bool> {
        let txn = db.begin_write()?;
        {
            let map_table = txn.open_table(MAP_TABLE)?;
            let count_key = make_map_count_key(name);
            if map_table.get(count_key.as_slice())?.is_none() {
                return Ok(false);
            }
            Self::set_key_ttl_in_txn(&txn, name, at)?;
        }
        txn.commit()?;
        Ok(true)
    }

    #[cfg(feature = "ttl")]
    fn exec_map_expire(db: &Database, name: &[u8], dur: TimestampMillis) -> Result<bool> {
        let at = timestamp_millis() + dur;
        Self::exec_map_expire_at(db, name, at)
    }

    #[cfg(feature = "ttl")]
    fn exec_map_ttl(db: &Database, name: &[u8]) -> Result<Option<TimestampMillis>> {
        Self::exec_db_ttl(db, name)
    }

    fn exec_map_is_expired(db: &Database, name: &[u8]) -> Result<bool> {
        #[cfg(feature = "ttl")]
        {
            match Self::exec_db_ttl(db, name)? {
                Some(ttl) if ttl > 0 => Ok(false),
                _ => Ok(true),
            }
        }
        #[cfg(not(feature = "ttl"))]
        {
            let _ = (db, name);
            Ok(false)
        }
    }

    // ---- List executors ----

    fn exec_list_push(db: &Database, name: &[u8], val: &[u8]) -> Result<()> {
        // Sled behavior: if container is expired, remove its TTL entry
        #[cfg(feature = "ttl")]
        {
            let check_txn = db.begin_read()?;
            let expired = match check_txn.open_table(KEY_EXPIRE_TABLE)?.get(name)? {
                Some(guard) => {
                    let expire_at = i64::from_be_bytes(guard.value().try_into().unwrap_or([0; 8]));
                    expire_at <= timestamp_millis()
                }
                None => true,
            };
            drop(check_txn);
            if expired {
                let write_txn = db.begin_write()?;
                Self::clear_key_ttl_in_txn(&write_txn, name)?;
                write_txn.commit()?;
            }
        }
        let txn = db.begin_write()?;
        {
            let mut table = txn.open_table(LIST_TABLE)?;
            let count_key = make_list_count_key(name);
            let (start, end) = table
                .get(count_key.as_slice())?
                .map(|g| postcard::from_bytes::<(usize, usize)>(g.value()).unwrap_or((0, 0)))
                .unwrap_or((0, 0));
            let content_key = make_list_content_key(name, end);
            table.insert(content_key.as_slice(), val)?;
            let encoded = postcard::to_stdvec(&(start, end + 1))?;
            table.insert(count_key.as_slice(), encoded.as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    fn exec_list_pushs(db: &Database, name: &[u8], vals: Vec<Vec<u8>>) -> Result<()> {
        let txn = db.begin_write()?;
        {
            let mut table = txn.open_table(LIST_TABLE)?;
            let count_key = make_list_count_key(name);
            let (start, mut end) = table
                .get(count_key.as_slice())?
                .map(|g| postcard::from_bytes::<(usize, usize)>(g.value()).unwrap_or((0, 0)))
                .unwrap_or((0, 0));
            for val in &vals {
                let content_key = make_list_content_key(name, end);
                table.insert(content_key.as_slice(), val.as_slice())?;
                end += 1;
            }
            let encoded = postcard::to_stdvec(&(start, end))?;
            table.insert(count_key.as_slice(), encoded.as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    fn exec_list_push_limit(
        db: &Database,
        name: &[u8],
        val: &[u8],
        limit: usize,
        pop_front_if_limited: bool,
    ) -> Result<Option<Vec<u8>>> {
        let txn = db.begin_write()?;
        let result = {
            let mut table = txn.open_table(LIST_TABLE)?;
            let count_key = make_list_count_key(name);
            let (mut start, mut end) = table
                .get(count_key.as_slice())?
                .map(|g| postcard::from_bytes::<(usize, usize)>(g.value()).unwrap_or((0, 0)))
                .unwrap_or((0, 0));

            let mut popped: Option<Vec<u8>> = None;
            if end - start >= limit {
                if pop_front_if_limited {
                    let front_key = make_list_content_key(name, start);
                    popped = table
                        .remove(front_key.as_slice())?
                        .map(|g| g.value().to_vec());
                    start += 1;
                } else {
                    return Err(anyhow!(
                        "list '{}' is full, limit reached: {}",
                        String::from_utf8_lossy(name),
                        limit
                    ));
                }
            }

            let content_key = make_list_content_key(name, end);
            table.insert(content_key.as_slice(), val)?;
            end += 1;

            let encoded = postcard::to_stdvec(&(start, end))?;
            table.insert(count_key.as_slice(), encoded.as_slice())?;

            popped
        };
        txn.commit()?;
        Ok(result)
    }

    fn exec_list_pop(db: &Database, name: &[u8]) -> Result<Option<Vec<u8>>> {
        #[cfg(feature = "ttl")]
        if Self::is_container_expired(db, name)? {
            return Ok(None);
        }
        let txn = db.begin_write()?;
        let result = {
            let mut table = txn.open_table(LIST_TABLE)?;
            let count_key = make_list_count_key(name);
            let (start, end) = table
                .get(count_key.as_slice())?
                .map(|g| postcard::from_bytes::<(usize, usize)>(g.value()).unwrap_or((0, 0)))
                .unwrap_or((0, 0));

            if start >= end {
                None
            } else {
                let front_key = make_list_content_key(name, start);
                let val = table
                    .remove(front_key.as_slice())?
                    .map(|g| g.value().to_vec());
                let encoded = postcard::to_stdvec(&(start + 1, end))?;
                table.insert(count_key.as_slice(), encoded.as_slice())?;
                val
            }
        };
        txn.commit()?;
        Ok(result)
    }

    fn exec_list_all(db: &Database, name: &[u8]) -> Result<Vec<Vec<u8>>> {
        let txn = db.begin_read()?;
        #[cfg(feature = "ttl")]
        if let Ok(table) = txn.open_table(KEY_EXPIRE_TABLE) {
            if let Some(guard) = table.get(name)? {
                let expire_at: TimestampMillis = postcard::from_bytes(guard.value())?;
                if expire_at <= timestamp_millis() {
                    return Ok(Vec::new());
                }
            }
        }
        let table = txn.open_table(LIST_TABLE)?;
        let count_key = make_list_count_key(name);
        let (start, end) = table
            .get(count_key.as_slice())?
            .map(|g| postcard::from_bytes::<(usize, usize)>(g.value()).unwrap_or((0, 0)))
            .unwrap_or((0, 0));

        let mut results = Vec::with_capacity(end.saturating_sub(start));
        for i in start..end {
            let ck = make_list_content_key(name, i);
            if let Some(val) = table.get(ck.as_slice())? {
                results.push(val.value().to_vec());
            }
        }
        Ok(results)
    }

    fn exec_list_get_index(db: &Database, name: &[u8], idx: usize) -> Result<Option<Vec<u8>>> {
        #[cfg(feature = "ttl")]
        if Self::is_container_expired(db, name)? {
            return Ok(None);
        }
        let txn = db.begin_read()?;
        let table = txn.open_table(LIST_TABLE)?;
        let count_key = make_list_count_key(name);
        let (start, end) = table
            .get(count_key.as_slice())?
            .map(|g| postcard::from_bytes::<(usize, usize)>(g.value()).unwrap_or((0, 0)))
            .unwrap_or((0, 0));

        let actual_idx = start + idx;
        if actual_idx >= end {
            return Ok(None);
        }
        let ck = make_list_content_key(name, actual_idx);
        match table.get(ck.as_slice())? {
            Some(guard) => Ok(Some(guard.value().to_vec())),
            None => Ok(None),
        }
    }

    fn exec_list_len(db: &Database, name: &[u8]) -> Result<usize> {
        let txn = db.begin_read()?;
        #[cfg(feature = "ttl")]
        if let Ok(table) = txn.open_table(KEY_EXPIRE_TABLE) {
            if let Some(guard) = table.get(name)? {
                let expire_at: TimestampMillis = postcard::from_bytes(guard.value())?;
                if expire_at <= timestamp_millis() {
                    return Ok(0);
                }
            }
        }
        let table = txn.open_table(LIST_TABLE)?;
        let count_key = make_list_count_key(name);
        match table.get(count_key.as_slice())? {
            Some(g) => {
                let (start, end) = postcard::from_bytes::<(usize, usize)>(g.value())?;
                Ok(end - start)
            }
            None => Ok(0),
        }
    }

    fn exec_list_is_empty(db: &Database, name: &[u8]) -> Result<bool> {
        Ok(Self::exec_list_len(db, name)? == 0)
    }

    fn exec_list_clear(db: &Database, name: &[u8]) -> Result<()> {
        let txn = db.begin_write()?;
        {
            let mut table = txn.open_table(LIST_TABLE)?;
            let content_prefix = make_list_content_prefix(name);
            let count_key = make_list_count_key(name);

            // Remove all content entries
            {
                let extract = table.extract_from_if(content_prefix.as_slice().., |_, _| true)?;
                let _drained: Vec<_> = extract.collect::<Result<Vec<_>, _>>()?;
            }
            let _ = table.remove(count_key.as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    fn exec_list_iter(db: &Database, name: &[u8]) -> Result<Vec<Vec<u8>>> {
        #[cfg(feature = "ttl")]
        if Self::is_container_expired(db, name)? {
            return Ok(Vec::new());
        }
        let txn = db.begin_read()?;
        let table = txn.open_table(LIST_TABLE)?;
        let count_key = make_list_count_key(name);
        let (start, end) = table
            .get(count_key.as_slice())?
            .map(|g| postcard::from_bytes::<(usize, usize)>(g.value()).unwrap_or((0, 0)))
            .unwrap_or((0, 0));

        let mut results = Vec::with_capacity(end.saturating_sub(start));
        for i in start..end {
            let ck = make_list_content_key(name, i);
            if let Some(val) = table.get(ck.as_slice())? {
                results.push(val.value().to_vec());
            }
        }
        Ok(results)
    }

    #[cfg(feature = "ttl")]
    fn exec_list_expire_at(db: &Database, name: &[u8], at: TimestampMillis) -> Result<bool> {
        let txn = db.begin_write()?;
        {
            let list_table = txn.open_table(LIST_TABLE)?;
            let count_key = make_list_count_key(name);
            if list_table.get(count_key.as_slice())?.is_none() {
                return Ok(false);
            }
            Self::set_key_ttl_in_txn(&txn, name, at)?;
        }
        txn.commit()?;
        Ok(true)
    }

    #[cfg(feature = "ttl")]
    fn exec_list_expire(db: &Database, name: &[u8], dur: TimestampMillis) -> Result<bool> {
        let at = timestamp_millis() + dur;
        Self::exec_list_expire_at(db, name, at)
    }

    #[cfg(feature = "ttl")]
    fn exec_list_ttl(db: &Database, name: &[u8]) -> Result<Option<TimestampMillis>> {
        Self::exec_db_ttl(db, name)
    }

    fn exec_list_is_expired(db: &Database, name: &[u8]) -> Result<bool> {
        #[cfg(feature = "ttl")]
        {
            match Self::exec_db_ttl(db, name)? {
                Some(ttl) if ttl > 0 => Ok(false),
                _ => Ok(true),
            }
        }
        #[cfg(not(feature = "ttl"))]
        {
            let _ = (db, name);
            Ok(false)
        }
    }
}

// ============================================================================
// Command Enum
// ============================================================================

enum Command {
    // === DB Operations ===
    DBInsert {
        key: Vec<u8>,
        val: Vec<u8>,
        tx: oneshot::Sender<Result<()>>,
    },
    DBGet {
        key: Vec<u8>,
        tx: oneshot::Sender<Result<Option<Vec<u8>>>>,
    },
    DBRemove {
        key: Vec<u8>,
        tx: oneshot::Sender<Result<()>>,
    },
    DBContainsKey {
        key: Vec<u8>,
        tx: oneshot::Sender<Result<bool>>,
    },
    DBBatchInsert {
        key_vals: Vec<(Vec<u8>, Vec<u8>)>,
        tx: oneshot::Sender<Result<()>>,
    },
    DBBatchRemove {
        keys: Vec<Vec<u8>>,
        tx: oneshot::Sender<Result<()>>,
    },
    DBCounterIncr {
        key: Vec<u8>,
        increment: isize,
        tx: oneshot::Sender<Result<()>>,
    },
    DBCounterDecr {
        key: Vec<u8>,
        decrement: isize,
        tx: oneshot::Sender<Result<()>>,
    },
    DBCounterGet {
        key: Vec<u8>,
        tx: oneshot::Sender<Result<Option<isize>>>,
    },
    DBCounterSet {
        key: Vec<u8>,
        val: isize,
        tx: oneshot::Sender<Result<()>>,
    },
    DBLen {
        tx: oneshot::Sender<Result<usize>>,
    },
    DBSize {
        tx: oneshot::Sender<Result<usize>>,
    },
    DBInfo {
        tx: oneshot::Sender<Result<Value>>,
    },
    // === Container Management ===
    DBMapGet {
        key: Vec<u8>,
        tx: oneshot::Sender<Result<bool>>,
    },
    DBListGet {
        key: Vec<u8>,
        tx: oneshot::Sender<Result<bool>>,
    },
    // === Iterators ===
    DBMapIter {
        tx: IterResultSender,
    },
    DBListIter {
        tx: IterResultSender,
    },
    DBScanIter {
        pattern: Vec<u8>,
        tx: oneshot::Sender<Result<Vec<Vec<u8>>>>,
    },
    // === TTL ===
    #[cfg(feature = "ttl")]
    DBExpireAt {
        key: Vec<u8>,
        at: TimestampMillis,
        tx: oneshot::Sender<Result<bool>>,
    },
    #[cfg(feature = "ttl")]
    DBExpire {
        key: Vec<u8>,
        dur: TimestampMillis,
        tx: oneshot::Sender<Result<bool>>,
    },
    #[cfg(feature = "ttl")]
    DBTtl {
        key: Vec<u8>,
        tx: oneshot::Sender<Result<Option<TimestampMillis>>>,
    },
    #[cfg(feature = "ttl")]
    #[allow(dead_code)]
    DBCleanup {
        tx: oneshot::Sender<Result<usize>>,
    },
    // === Map Operations ===
    MapInsert {
        map: RedbStorageMap,
        key: Vec<u8>,
        val: Vec<u8>,
        tx: oneshot::Sender<Result<()>>,
    },
    MapGet {
        map: RedbStorageMap,
        key: Vec<u8>,
        tx: oneshot::Sender<Result<Option<Vec<u8>>>>,
    },
    MapRemove {
        map: RedbStorageMap,
        key: Vec<u8>,
        tx: oneshot::Sender<Result<()>>,
    },
    MapContainsKey {
        map: RedbStorageMap,
        key: Vec<u8>,
        tx: oneshot::Sender<Result<bool>>,
    },
    #[cfg(feature = "map_len")]
    MapLen {
        map: RedbStorageMap,
        tx: oneshot::Sender<Result<usize>>,
    },
    MapIsEmpty {
        map: RedbStorageMap,
        tx: oneshot::Sender<Result<bool>>,
    },
    MapClear {
        map: RedbStorageMap,
        tx: oneshot::Sender<Result<()>>,
    },
    MapRemoveAndFetch {
        map: RedbStorageMap,
        key: Vec<u8>,
        tx: oneshot::Sender<Result<Option<Vec<u8>>>>,
    },
    MapRemoveWithPrefix {
        map: RedbStorageMap,
        prefix: Vec<u8>,
        tx: oneshot::Sender<Result<()>>,
    },
    MapBatchInsert {
        map: RedbStorageMap,
        key_vals: Vec<(Vec<u8>, Vec<u8>)>,
        tx: oneshot::Sender<Result<()>>,
    },
    MapBatchRemove {
        map: RedbStorageMap,
        keys: Vec<Vec<u8>>,
        tx: oneshot::Sender<Result<()>>,
    },
    MapIter {
        map: RedbStorageMap,
        tx: IterResultSender,
    },
    MapKeyIter {
        map: RedbStorageMap,
        tx: oneshot::Sender<Result<Vec<Vec<u8>>>>,
    },
    MapPrefixIter {
        map: RedbStorageMap,
        prefix: Vec<u8>,
        tx: IterResultSender,
    },
    #[cfg(feature = "ttl")]
    MapExpireAt {
        map: RedbStorageMap,
        at: TimestampMillis,
        tx: oneshot::Sender<Result<bool>>,
    },
    #[cfg(feature = "ttl")]
    MapExpire {
        map: RedbStorageMap,
        dur: TimestampMillis,
        tx: oneshot::Sender<Result<bool>>,
    },
    #[cfg(feature = "ttl")]
    MapTTL {
        map: RedbStorageMap,
        tx: oneshot::Sender<Result<Option<TimestampMillis>>>,
    },
    #[allow(dead_code)]
    MapIsExpired {
        map: RedbStorageMap,
        tx: oneshot::Sender<Result<bool>>,
    },
    // === List Operations ===
    ListPush {
        list: RedbStorageList,
        val: Vec<u8>,
        tx: oneshot::Sender<Result<()>>,
    },
    ListPushs {
        list: RedbStorageList,
        vals: Vec<Vec<u8>>,
        tx: oneshot::Sender<Result<()>>,
    },
    ListPushLimit {
        list: RedbStorageList,
        val: Vec<u8>,
        limit: usize,
        pop_front_if_limited: bool,
        tx: oneshot::Sender<Result<Option<Vec<u8>>>>,
    },
    ListPop {
        list: RedbStorageList,
        tx: oneshot::Sender<Result<Option<Vec<u8>>>>,
    },
    ListAll {
        list: RedbStorageList,
        tx: oneshot::Sender<Result<Vec<Vec<u8>>>>,
    },
    ListGetIndex {
        list: RedbStorageList,
        idx: usize,
        tx: oneshot::Sender<Result<Option<Vec<u8>>>>,
    },
    ListLen {
        list: RedbStorageList,
        tx: oneshot::Sender<Result<usize>>,
    },
    ListIsEmpty {
        list: RedbStorageList,
        tx: oneshot::Sender<Result<bool>>,
    },
    ListClear {
        list: RedbStorageList,
        tx: oneshot::Sender<Result<()>>,
    },
    ListIter {
        list: RedbStorageList,
        tx: oneshot::Sender<Result<Vec<Vec<u8>>>>,
    },
    #[cfg(feature = "ttl")]
    ListExpireAt {
        list: RedbStorageList,
        at: TimestampMillis,
        tx: oneshot::Sender<Result<bool>>,
    },
    #[cfg(feature = "ttl")]
    ListExpire {
        list: RedbStorageList,
        dur: TimestampMillis,
        tx: oneshot::Sender<Result<bool>>,
    },
    #[cfg(feature = "ttl")]
    ListTTL {
        list: RedbStorageList,
        tx: oneshot::Sender<Result<Option<TimestampMillis>>>,
    },
    #[allow(dead_code)]
    ListIsExpired {
        list: RedbStorageList,
        tx: oneshot::Sender<Result<bool>>,
    },
    // === Raw (circuit-breaker) ===
    #[cfg(feature = "circuit-breaker")]
    DBInsertRaw {
        key: Vec<u8>,
        val: Vec<u8>,
        tx: oneshot::Sender<Result<()>>,
    },
    #[cfg(feature = "circuit-breaker")]
    DBGetRaw {
        key: Vec<u8>,
        tx: oneshot::Sender<Result<Option<Vec<u8>>>>,
    },
    #[cfg(feature = "circuit-breaker")]
    #[allow(dead_code)]
    DBRemoveRaw {
        key: Vec<u8>,
        tx: oneshot::Sender<Result<()>>,
    },
    // === Shutdown ===
    #[allow(dead_code)]
    Shutdown,
}

// ============================================================================
// RedbStorageMap
// ============================================================================

/// A redb-backed map (dictionary) identified by a name
#[derive(Clone)]
pub struct RedbStorageMap {
    db: RedbStorageDB,
    name: Arc<Key>,
    #[allow(dead_code)]
    expire: Option<TimestampMillis>,
    empty: Arc<AtomicBool>,
}

impl fmt::Debug for RedbStorageMap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RedbStorageMap")
            .field("name", &String::from_utf8_lossy(&self.name))
            .finish()
    }
}

impl RedbStorageMap {
    fn new(db: RedbStorageDB, name: Key, expire: Option<TimestampMillis>) -> Self {
        RedbStorageMap {
            db,
            name: Arc::new(name),
            expire,
            empty: Arc::new(AtomicBool::new(true)),
        }
    }
}

#[async_trait]
impl Map for RedbStorageMap {
    fn name(&self) -> &[u8] {
        &self.name
    }

    async fn insert<K, V>(&self, key: K, val: &V) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: Serialize + Sync + Send + ?Sized,
    {
        let key_bytes = key.as_ref().to_vec();
        let val_bytes = postcard::to_stdvec(val)?;
        self.db
            .send_cmd(|tx| Command::MapInsert {
                map: self.clone(),
                key: key_bytes,
                val: val_bytes,
                tx,
            })
            .await
    }

    async fn get<K, V>(&self, key: K) -> Result<Option<V>>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: DeserializeOwned + Sync + Send,
    {
        let key_bytes = key.as_ref().to_vec();
        let result: Option<Vec<u8>> = self
            .db
            .send_cmd(|tx| Command::MapGet {
                map: self.clone(),
                key: key_bytes,
                tx,
            })
            .await?;
        match result {
            Some(bytes) => Ok(Some(postcard::from_bytes(&bytes)?)),
            None => Ok(None),
        }
    }

    async fn remove<K>(&self, key: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let key_bytes = key.as_ref().to_vec();
        self.db
            .send_cmd(|tx| Command::MapRemove {
                map: self.clone(),
                key: key_bytes,
                tx,
            })
            .await
    }

    async fn contains_key<K: AsRef<[u8]> + Sync + Send>(&self, key: K) -> Result<bool> {
        let key_bytes = key.as_ref().to_vec();
        self.db
            .send_cmd(|tx| Command::MapContainsKey {
                map: self.clone(),
                key: key_bytes,
                tx,
            })
            .await
    }

    #[cfg(feature = "map_len")]
    async fn len(&self) -> Result<usize> {
        self.db
            .send_cmd(|tx| Command::MapLen {
                map: self.clone(),
                tx,
            })
            .await
    }

    async fn is_empty(&self) -> Result<bool> {
        if !self.empty.load(Ordering::Acquire) {
            return Ok(false);
        }
        self.db
            .send_cmd(|tx| Command::MapIsEmpty {
                map: self.clone(),
                tx,
            })
            .await
    }

    async fn clear(&self) -> Result<()> {
        let result: Result<()> = self
            .db
            .send_cmd(|tx| Command::MapClear {
                map: self.clone(),
                tx,
            })
            .await;
        if result.is_ok() {
            self.empty.store(true, Ordering::Release);
        }
        result
    }

    async fn remove_and_fetch<K, V>(&self, key: K) -> Result<Option<V>>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: DeserializeOwned + Sync + Send,
    {
        let key_bytes = key.as_ref().to_vec();
        let result: Option<Vec<u8>> = self
            .db
            .send_cmd(|tx| Command::MapRemoveAndFetch {
                map: self.clone(),
                key: key_bytes,
                tx,
            })
            .await?;
        match result {
            Some(bytes) => Ok(Some(postcard::from_bytes(&bytes)?)),
            None => Ok(None),
        }
    }

    async fn remove_with_prefix<K>(&self, prefix: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let prefix_bytes = prefix.as_ref().to_vec();
        self.db
            .send_cmd(|tx| Command::MapRemoveWithPrefix {
                map: self.clone(),
                prefix: prefix_bytes,
                tx,
            })
            .await
    }

    async fn batch_insert<V>(&self, key_vals: Vec<(Key, V)>) -> Result<()>
    where
        V: Serialize + Sync + Send,
    {
        let mut kvs = Vec::with_capacity(key_vals.len());
        for (k, v) in key_vals {
            let val_bytes = postcard::to_stdvec(&v)?;
            kvs.push((k, val_bytes));
        }
        self.db
            .send_cmd(|tx| Command::MapBatchInsert {
                map: self.clone(),
                key_vals: kvs,
                tx,
            })
            .await
    }

    async fn batch_remove(&self, keys: Vec<Key>) -> Result<()> {
        self.db
            .send_cmd(|tx| Command::MapBatchRemove {
                map: self.clone(),
                keys,
                tx,
            })
            .await
    }

    async fn iter<'a, V>(
        &'a mut self,
    ) -> Result<Box<dyn AsyncIterator<Item = IterItem<V>> + Send + 'a>>
    where
        V: DeserializeOwned + Sync + Send + 'a + 'static,
    {
        let entries: Vec<(Vec<u8>, Vec<u8>)> = self
            .db
            .send_cmd(|tx| Command::MapIter {
                map: self.clone(),
                tx,
            })
            .await?;
        let iter = RedbIter {
            entries,
            pos: 0,
            _phantom: std::marker::PhantomData,
        };
        Ok(Box::new(iter))
    }

    async fn key_iter<'a>(
        &'a mut self,
    ) -> Result<Box<dyn AsyncIterator<Item = Result<Key>> + Send + 'a>> {
        let keys: Vec<Vec<u8>> = self
            .db
            .send_cmd(|tx| Command::MapKeyIter {
                map: self.clone(),
                tx,
            })
            .await?;
        let iter = RedbKeyIter { keys, pos: 0 };
        Ok(Box::new(iter))
    }

    async fn prefix_iter<'a, P, V>(
        &'a mut self,
        prefix: P,
    ) -> Result<Box<dyn AsyncIterator<Item = IterItem<V>> + Send + 'a>>
    where
        P: AsRef<[u8]> + Send + Sync,
        V: DeserializeOwned + Sync + Send + 'a + 'static,
    {
        let prefix_bytes = prefix.as_ref().to_vec();
        let entries: Vec<(Vec<u8>, Vec<u8>)> = self
            .db
            .send_cmd(|tx| Command::MapPrefixIter {
                map: self.clone(),
                prefix: prefix_bytes,
                tx,
            })
            .await?;
        let iter = RedbPrefixIter {
            entries,
            pos: 0,
            _phantom: std::marker::PhantomData,
        };
        Ok(Box::new(iter))
    }

    #[cfg(feature = "ttl")]
    async fn expire_at(&self, at: TimestampMillis) -> Result<bool> {
        self.db
            .send_cmd(|tx| Command::MapExpireAt {
                map: self.clone(),
                at,
                tx,
            })
            .await
    }

    #[cfg(feature = "ttl")]
    async fn expire(&self, dur: TimestampMillis) -> Result<bool> {
        self.db
            .send_cmd(|tx| Command::MapExpire {
                map: self.clone(),
                dur,
                tx,
            })
            .await
    }

    #[cfg(feature = "ttl")]
    async fn ttl(&self) -> Result<Option<TimestampMillis>> {
        self.db
            .send_cmd(|tx| Command::MapTTL {
                map: self.clone(),
                tx,
            })
            .await
    }
}

// ============================================================================
// RedbStorageList
// ============================================================================

/// A redb-backed list (queue) identified by a name
#[derive(Clone)]
pub struct RedbStorageList {
    db: RedbStorageDB,
    name: Arc<Key>,
    #[allow(dead_code)]
    expire: Option<TimestampMillis>,
    empty: Arc<AtomicBool>,
}

impl fmt::Debug for RedbStorageList {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RedbStorageList")
            .field("name", &String::from_utf8_lossy(&self.name))
            .finish()
    }
}

impl RedbStorageList {
    fn new(db: RedbStorageDB, name: Key, expire: Option<TimestampMillis>) -> Self {
        RedbStorageList {
            db,
            name: Arc::new(name),
            expire,
            empty: Arc::new(AtomicBool::new(true)),
        }
    }
}

#[async_trait]
impl List for RedbStorageList {
    fn name(&self) -> &[u8] {
        &self.name
    }

    async fn push<V>(&self, val: &V) -> Result<()>
    where
        V: Serialize + Sync + Send,
    {
        let val_bytes = postcard::to_stdvec(val)?;
        self.db
            .send_cmd(|tx| Command::ListPush {
                list: self.clone(),
                val: val_bytes,
                tx,
            })
            .await
    }

    async fn pushs<V>(&self, vals: Vec<V>) -> Result<()>
    where
        V: Serialize + Sync + Send,
    {
        let mut val_bytes = Vec::with_capacity(vals.len());
        for v in vals {
            val_bytes.push(postcard::to_stdvec(&v)?);
        }
        self.db
            .send_cmd(|tx| Command::ListPushs {
                list: self.clone(),
                vals: val_bytes,
                tx,
            })
            .await
    }

    async fn push_limit<V>(
        &self,
        val: &V,
        limit: usize,
        pop_front_if_limited: bool,
    ) -> Result<Option<V>>
    where
        V: Serialize + Sync + Send,
        V: DeserializeOwned,
    {
        let val_bytes = postcard::to_stdvec(val)?;
        let result: Option<Vec<u8>> = self
            .db
            .send_cmd(|tx| Command::ListPushLimit {
                list: self.clone(),
                val: val_bytes,
                limit,
                pop_front_if_limited,
                tx,
            })
            .await?;
        match result {
            Some(bytes) => Ok(Some(postcard::from_bytes(&bytes)?)),
            None => Ok(None),
        }
    }

    async fn pop<V>(&self) -> Result<Option<V>>
    where
        V: DeserializeOwned + Sync + Send,
    {
        let result: Option<Vec<u8>> = self
            .db
            .send_cmd(|tx| Command::ListPop {
                list: self.clone(),
                tx,
            })
            .await?;
        match result {
            Some(bytes) => Ok(Some(postcard::from_bytes(&bytes)?)),
            None => Ok(None),
        }
    }

    async fn all<V>(&self) -> Result<Vec<V>>
    where
        V: DeserializeOwned + Sync + Send,
    {
        let result: Vec<Vec<u8>> = self
            .db
            .send_cmd(|tx| Command::ListAll {
                list: self.clone(),
                tx,
            })
            .await?;
        let mut vals = Vec::with_capacity(result.len());
        for bytes in result {
            vals.push(postcard::from_bytes(&bytes)?);
        }
        Ok(vals)
    }

    async fn get_index<V>(&self, idx: usize) -> Result<Option<V>>
    where
        V: DeserializeOwned + Sync + Send,
    {
        let result: Option<Vec<u8>> = self
            .db
            .send_cmd(|tx| Command::ListGetIndex {
                list: self.clone(),
                idx,
                tx,
            })
            .await?;
        match result {
            Some(bytes) => Ok(Some(postcard::from_bytes(&bytes)?)),
            None => Ok(None),
        }
    }

    async fn len(&self) -> Result<usize> {
        self.db
            .send_cmd(|tx| Command::ListLen {
                list: self.clone(),
                tx,
            })
            .await
    }

    async fn is_empty(&self) -> Result<bool> {
        if !self.empty.load(Ordering::Acquire) {
            return Ok(false);
        }
        self.db
            .send_cmd(|tx| Command::ListIsEmpty {
                list: self.clone(),
                tx,
            })
            .await
    }

    async fn clear(&self) -> Result<()> {
        let result: Result<()> = self
            .db
            .send_cmd(|tx| Command::ListClear {
                list: self.clone(),
                tx,
            })
            .await;
        if result.is_ok() {
            self.empty.store(true, Ordering::Release);
        }
        result
    }

    async fn iter<'a, V>(
        &'a mut self,
    ) -> Result<Box<dyn AsyncIterator<Item = Result<V>> + Send + 'a>>
    where
        V: DeserializeOwned + Sync + Send + 'a + 'static,
    {
        let entries: Vec<Vec<u8>> = self
            .db
            .send_cmd(|tx| Command::ListIter {
                list: self.clone(),
                tx,
            })
            .await?;
        let iter = RedbListValIter {
            entries,
            pos: 0,
            _phantom: std::marker::PhantomData,
        };
        Ok(Box::new(iter))
    }

    #[cfg(feature = "ttl")]
    async fn expire_at(&self, at: TimestampMillis) -> Result<bool> {
        self.db
            .send_cmd(|tx| Command::ListExpireAt {
                list: self.clone(),
                at,
                tx,
            })
            .await
    }

    #[cfg(feature = "ttl")]
    async fn expire(&self, dur: TimestampMillis) -> Result<bool> {
        self.db
            .send_cmd(|tx| Command::ListExpire {
                list: self.clone(),
                dur,
                tx,
            })
            .await
    }

    #[cfg(feature = "ttl")]
    async fn ttl(&self) -> Result<Option<TimestampMillis>> {
        self.db
            .send_cmd(|tx| Command::ListTTL {
                list: self.clone(),
                tx,
            })
            .await
    }
}

// ============================================================================
// StorageDB trait implementation for RedbStorageDB
// ============================================================================

#[async_trait]
impl StorageDB for RedbStorageDB {
    type MapType = RedbStorageMap;
    type ListType = RedbStorageList;

    async fn map<N: AsRef<[u8]> + Sync + Send>(
        &self,
        name: N,
        expire: Option<TimestampMillis>,
    ) -> Self::MapType {
        let name_bytes = name.as_ref().to_vec();
        let map = RedbStorageMap::new(self.clone(), name_bytes, expire);
        #[cfg(feature = "ttl")]
        if let Some(expire_ms) = expire {
            if let Err(e) = self
                .send_cmd(|tx| Command::DBExpireAt {
                    key: map.name.to_vec(),
                    at: timestamp_millis() + expire_ms,
                    tx,
                })
                .await
            {
                log::warn!(
                    "redb map '{}' expire_at failed: {e}",
                    String::from_utf8_lossy(&map.name)
                );
            }
        }
        map
    }

    async fn map_remove<K>(&self, name: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        // Remove all items from the map
        let name_bytes = name.as_ref().to_vec();
        // First clear the map contents
        let _ = self
            .send_cmd(|tx| Command::MapClear {
                map: RedbStorageMap::new(self.clone(), name_bytes.clone(), None),
                tx,
            })
            .await;
        Ok(())
    }

    async fn map_contains_key<K: AsRef<[u8]> + Sync + Send>(&self, key: K) -> Result<bool> {
        let key_bytes = key.as_ref().to_vec();
        self.send_cmd(|tx| Command::DBMapGet { key: key_bytes, tx })
            .await
    }

    async fn list<N: AsRef<[u8]> + Sync + Send>(
        &self,
        name: N,
        expire: Option<TimestampMillis>,
    ) -> Self::ListType {
        let name_bytes = name.as_ref().to_vec();
        let list = RedbStorageList::new(self.clone(), name_bytes, expire);
        #[cfg(feature = "ttl")]
        if let Some(expire_ms) = expire {
            if let Err(e) = self
                .send_cmd(|tx| Command::DBExpireAt {
                    key: list.name.to_vec(),
                    at: timestamp_millis() + expire_ms,
                    tx,
                })
                .await
            {
                log::warn!(
                    "redb list '{}' expire_at failed: {e}",
                    String::from_utf8_lossy(&list.name)
                );
            }
        }
        list
    }

    async fn list_remove<K>(&self, name: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let name_bytes = name.as_ref().to_vec();
        let _ = self
            .send_cmd(|tx| Command::ListClear {
                list: RedbStorageList::new(self.clone(), name_bytes.clone(), None),
                tx,
            })
            .await;
        Ok(())
    }

    async fn list_contains_key<K: AsRef<[u8]> + Sync + Send>(&self, key: K) -> Result<bool> {
        let key_bytes = key.as_ref().to_vec();
        self.send_cmd(|tx| Command::DBListGet { key: key_bytes, tx })
            .await
    }

    async fn insert<K, V>(&self, key: K, val: &V) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: Serialize + Sync + Send + ?Sized,
    {
        let key_bytes = key.as_ref().to_vec();
        let val_bytes = postcard::to_stdvec(val)?;
        self.send_cmd(|tx| Command::DBInsert {
            key: key_bytes,
            val: val_bytes,
            tx,
        })
        .await
    }

    async fn get<K, V>(&self, key: K) -> Result<Option<V>>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: DeserializeOwned + Sync + Send,
    {
        let key_bytes = key.as_ref().to_vec();
        let result: Option<Vec<u8>> = self
            .send_cmd(|tx| Command::DBGet { key: key_bytes, tx })
            .await?;
        match result {
            Some(bytes) => Ok(Some(postcard::from_bytes(&bytes)?)),
            None => Ok(None),
        }
    }

    async fn remove<K>(&self, key: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let key_bytes = key.as_ref().to_vec();
        self.send_cmd(|tx| Command::DBRemove { key: key_bytes, tx })
            .await
    }

    async fn contains_key<K: AsRef<[u8]> + Sync + Send>(&self, key: K) -> Result<bool> {
        let key_bytes = key.as_ref().to_vec();
        self.send_cmd(|tx| Command::DBContainsKey { key: key_bytes, tx })
            .await
    }

    async fn batch_insert<V>(&self, key_vals: Vec<(Key, V)>) -> Result<()>
    where
        V: Serialize + Sync + Send,
    {
        let mut kvs = Vec::with_capacity(key_vals.len());
        for (k, v) in key_vals {
            let val_bytes = postcard::to_stdvec(&v)?;
            kvs.push((k, val_bytes));
        }
        self.send_cmd(|tx| Command::DBBatchInsert { key_vals: kvs, tx })
            .await
    }

    async fn batch_remove(&self, keys: Vec<Key>) -> Result<()> {
        self.send_cmd(|tx| Command::DBBatchRemove { keys, tx })
            .await
    }

    async fn counter_incr<K>(&self, key: K, increment: isize) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let key_bytes = key.as_ref().to_vec();
        self.send_cmd(|tx| Command::DBCounterIncr {
            key: key_bytes,
            increment,
            tx,
        })
        .await
    }

    async fn counter_decr<K>(&self, key: K, decrement: isize) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let key_bytes = key.as_ref().to_vec();
        self.send_cmd(|tx| Command::DBCounterDecr {
            key: key_bytes,
            decrement,
            tx,
        })
        .await
    }

    async fn counter_get<K>(&self, key: K) -> Result<Option<isize>>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let key_bytes = key.as_ref().to_vec();
        self.send_cmd(|tx| Command::DBCounterGet { key: key_bytes, tx })
            .await
    }

    async fn counter_set<K>(&self, key: K, val: isize) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let key_bytes = key.as_ref().to_vec();
        self.send_cmd(|tx| Command::DBCounterSet {
            key: key_bytes,
            val,
            tx,
        })
        .await
    }

    #[cfg(feature = "len")]
    async fn len(&self) -> Result<usize> {
        self.send_cmd(|tx| Command::DBLen { tx }).await
    }

    async fn db_size(&self) -> Result<usize> {
        self.send_cmd(|tx| Command::DBSize { tx }).await
    }

    #[cfg(feature = "ttl")]
    async fn expire_at<K>(&self, key: K, at: TimestampMillis) -> Result<bool>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let key_bytes = key.as_ref().to_vec();
        self.send_cmd(|tx| Command::DBExpireAt {
            key: key_bytes,
            at,
            tx,
        })
        .await
    }

    #[cfg(feature = "ttl")]
    async fn expire<K>(&self, key: K, dur: TimestampMillis) -> Result<bool>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let key_bytes = key.as_ref().to_vec();
        self.send_cmd(|tx| Command::DBExpire {
            key: key_bytes,
            dur,
            tx,
        })
        .await
    }

    #[cfg(feature = "ttl")]
    async fn ttl<K>(&self, key: K) -> Result<Option<TimestampMillis>>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let key_bytes = key.as_ref().to_vec();
        self.send_cmd(|tx| Command::DBTtl { key: key_bytes, tx })
            .await
    }

    async fn map_iter<'a>(
        &'a mut self,
    ) -> Result<Box<dyn AsyncIterator<Item = Result<StorageMap>> + Send + 'a>> {
        let entries: Vec<(Vec<u8>, Vec<u8>)> =
            self.send_cmd(|tx| Command::DBMapIter { tx }).await?;
        let map = RedbMapIter {
            db: self.clone(),
            entries,
            pos: 0,
        };
        Ok(Box::new(map))
    }

    async fn list_iter<'a>(
        &'a mut self,
    ) -> Result<Box<dyn AsyncIterator<Item = Result<StorageList>> + Send + 'a>> {
        let entries: Vec<(Vec<u8>, Vec<u8>)> =
            self.send_cmd(|tx| Command::DBListIter { tx }).await?;
        let list = RedbListIter {
            db: self.clone(),
            entries,
            pos: 0,
        };
        Ok(Box::new(list))
    }

    async fn scan<'a, P>(
        &'a mut self,
        pattern: P,
    ) -> Result<Box<dyn AsyncIterator<Item = Result<Key>> + Send + 'a>>
    where
        P: AsRef<[u8]> + Sync + Send,
    {
        let pattern_bytes = pattern.as_ref().to_vec();
        let keys: Vec<Vec<u8>> = self
            .send_cmd(|tx| Command::DBScanIter {
                pattern: pattern_bytes.clone(),
                tx,
            })
            .await?;
        let pattern = Pattern::from(pattern_bytes.as_slice());
        let iter = AsyncDbKeyIter {
            keys,
            pos: 0,
            pattern,
        };
        Ok(Box::new(iter))
    }

    async fn info(&self) -> Result<Value> {
        self.send_cmd(|tx| Command::DBInfo { tx }).await
    }
}

// ============================================================================
// Circuit-breaker raw methods
// ============================================================================

#[cfg(feature = "circuit-breaker")]
impl RedbStorageDB {
    pub(crate) async fn insert_raw(&self, key: &[u8], val: &[u8]) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::DBInsertRaw {
                key: key.to_vec(),
                val: val.to_vec(),
                tx,
            })
            .await
            .map_err(|_| anyhow!("redb command channel closed"))?;
        rx.await
            .map_err(|_| anyhow!("redb command response channel dropped"))??;
        Ok(())
    }

    pub(crate) async fn get_raw(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::DBGetRaw {
                key: key.to_vec(),
                tx,
            })
            .await
            .map_err(|_| anyhow!("redb command channel closed"))?;
        rx.await
            .map_err(|_| anyhow!("redb command response channel dropped"))?
    }

    #[allow(dead_code)]
    pub(crate) async fn remove_raw(&self, key: &[u8]) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::DBRemoveRaw {
                key: key.to_vec(),
                tx,
            })
            .await
            .map_err(|_| anyhow!("redb command channel closed"))?;
        rx.await
            .map_err(|_| anyhow!("redb command response channel dropped"))??;
        Ok(())
    }

    pub(crate) async fn batch_insert_raw(&self, key_vals: Vec<(Vec<u8>, Vec<u8>)>) -> Result<()> {
        self.send_cmd(|tx| Command::DBBatchInsert { key_vals, tx })
            .await
    }
}

#[cfg(feature = "circuit-breaker")]
impl RedbStorageMap {
    pub(crate) async fn insert_raw(&self, key: &[u8], val: &[u8]) -> Result<()> {
        self.db
            .send_cmd(|tx| Command::MapInsert {
                map: self.clone(),
                key: key.to_vec(),
                val: val.to_vec(),
                tx,
            })
            .await
    }

    pub(crate) async fn get_raw(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.db
            .send_cmd(|tx| Command::MapGet {
                map: self.clone(),
                key: key.to_vec(),
                tx,
            })
            .await
    }

    pub(crate) async fn remove_and_fetch_raw(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.db
            .send_cmd(|tx| Command::MapRemoveAndFetch {
                map: self.clone(),
                key: key.to_vec(),
                tx,
            })
            .await
    }

    pub(crate) async fn batch_insert_raw(&self, key_vals: Vec<(Vec<u8>, Vec<u8>)>) -> Result<()> {
        self.db
            .send_cmd(|tx| Command::MapBatchInsert {
                map: self.clone(),
                key_vals,
                tx,
            })
            .await
    }
}

#[cfg(feature = "circuit-breaker")]
impl RedbStorageList {
    pub(crate) async fn push_raw(&self, val: &[u8]) -> Result<()> {
        self.db
            .send_cmd(|tx| Command::ListPush {
                list: self.clone(),
                val: val.to_vec(),
                tx,
            })
            .await
    }

    pub(crate) async fn pushs_raw(&self, vals: Vec<Vec<u8>>) -> Result<()> {
        self.db
            .send_cmd(|tx| Command::ListPushs {
                list: self.clone(),
                vals,
                tx,
            })
            .await
    }

    pub(crate) async fn pop_raw(&self) -> Result<Option<Vec<u8>>> {
        self.db
            .send_cmd(|tx| Command::ListPop {
                list: self.clone(),
                tx,
            })
            .await
    }

    pub(crate) async fn all_raw(&self) -> Result<Vec<Vec<u8>>> {
        self.db
            .send_cmd(|tx| Command::ListAll {
                list: self.clone(),
                tx,
            })
            .await
    }

    pub(crate) async fn get_index_raw(&self, idx: usize) -> Result<Option<Vec<u8>>> {
        self.db
            .send_cmd(|tx| Command::ListGetIndex {
                list: self.clone(),
                idx,
                tx,
            })
            .await
    }

    pub(crate) async fn push_limit_raw(
        &self,
        val: &[u8],
        limit: usize,
        pop_front_if_limited: bool,
    ) -> Result<Option<Vec<u8>>> {
        self.db
            .send_cmd(|tx| Command::ListPushLimit {
                list: self.clone(),
                val: val.to_vec(),
                limit,
                pop_front_if_limited,
                tx,
            })
            .await
    }
}

// ============================================================================
// Iterator Types
// ============================================================================

/// Iterator over map key-value pairs
pub struct RedbIter<V> {
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    pos: usize,
    _phantom: std::marker::PhantomData<V>,
}

#[async_trait]
impl<V: DeserializeOwned + Send> AsyncIterator for RedbIter<V> {
    type Item = IterItem<V>;
    async fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.entries.len() {
            return None;
        }
        let (key, val_bytes) = self.entries[self.pos].clone();
        self.pos += 1;
        match postcard::from_bytes(&val_bytes) {
            Ok(val) => Some(Ok((key, val))),
            Err(e) => Some(Err(e.into())),
        }
    }
}

/// Iterator over map keys
pub struct RedbKeyIter {
    keys: Vec<Vec<u8>>,
    pos: usize,
}

#[async_trait]
impl AsyncIterator for RedbKeyIter {
    type Item = Result<Key>;
    async fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.keys.len() {
            return None;
        }
        let key = self.keys[self.pos].clone();
        self.pos += 1;
        Some(Ok(key))
    }
}

/// Iterator over map entries with a prefix filter
pub struct RedbPrefixIter<V> {
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    pos: usize,
    _phantom: std::marker::PhantomData<V>,
}

#[async_trait]
impl<V: DeserializeOwned + Send> AsyncIterator for RedbPrefixIter<V> {
    type Item = IterItem<V>;
    async fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.entries.len() {
            return None;
        }
        let (key, val_bytes) = self.entries[self.pos].clone();
        self.pos += 1;
        match postcard::from_bytes(&val_bytes) {
            Ok(val) => Some(Ok((key, val))),
            Err(e) => Some(Err(e.into())),
        }
    }
}

/// Iterator for iterating over all maps in the database
pub struct RedbMapIter {
    db: RedbStorageDB,
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    pos: usize,
}

#[async_trait]
impl AsyncIterator for RedbMapIter {
    type Item = Result<StorageMap>;
    async fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.entries.len() {
            return None;
        }
        let (key, _) = self.entries[self.pos].clone();
        self.pos += 1;
        let map_name = map_count_key_to_name(&key).to_vec();
        let map = RedbStorageMap::new(self.db.clone(), map_name, None);
        Some(Ok(StorageMap::Redb(map)))
    }
}

/// Iterator for iterating over all lists in the database
pub struct RedbListIter {
    db: RedbStorageDB,
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    pos: usize,
}

#[async_trait]
impl AsyncIterator for RedbListIter {
    type Item = Result<StorageList>;
    async fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.entries.len() {
            return None;
        }
        let (key, _) = self.entries[self.pos].clone();
        self.pos += 1;
        let list_name = list_count_key_to_name(&key).to_vec();
        let list = RedbStorageList::new(self.db.clone(), list_name, None);
        Some(Ok(StorageList::Redb(list)))
    }
}

/// Iterator for scanning keys with pattern matching
pub struct AsyncDbKeyIter {
    keys: Vec<Vec<u8>>,
    pos: usize,
    pattern: Pattern,
}

#[async_trait]
impl AsyncIterator for AsyncDbKeyIter {
    type Item = Result<Key>;
    async fn next(&mut self) -> Option<Self::Item> {
        while self.pos < self.keys.len() {
            let key = self.keys[self.pos].clone();
            self.pos += 1;
            if is_match(self.pattern.clone(), &key) {
                // Skip internal keys
                if key.starts_with(COUNTER_PREFIX) || key.starts_with(KEY_PREFIX) {
                    continue;
                }
                return Some(Ok(key));
            }
        }
        None
    }
}

/// Iterator over list values
pub struct RedbListValIter<V> {
    entries: Vec<Vec<u8>>,
    pos: usize,
    _phantom: std::marker::PhantomData<V>,
}

#[async_trait]
impl<V: DeserializeOwned + Send> AsyncIterator for RedbListValIter<V> {
    type Item = Result<V>;
    async fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.entries.len() {
            return None;
        }
        let bytes = self.entries[self.pos].clone();
        self.pos += 1;
        match postcard::from_bytes(&bytes) {
            Ok(val) => Some(Ok(val)),
            Err(e) => Some(Err(e.into())),
        }
    }
}
