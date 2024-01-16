use core::fmt;
use std::fmt::Debug;
use std::io;
use std::io::ErrorKind;
use std::sync::atomic::{AtomicBool, AtomicIsize, Ordering};
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use convert::Bytesize;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;

#[allow(unused_imports)]
use sled::transaction::TransactionResult;
use sled::transaction::{
    ConflictableTransactionError, ConflictableTransactionResult, TransactionError,
    TransactionalTree,
};
use sled::{Batch, Db, IVec, Tree};
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::spawn_blocking;

use crate::storage::{AsyncIterator, IterItem, Key, List, Map, StorageDB};
#[allow(unused_imports)]
use crate::{timestamp_millis, TimestampMillis};
use crate::{Error, Result, StorageList, StorageMap};

const SEPARATOR: &[u8] = b"@";
const MAP_TREE: &[u8] = b"__map_tree@";
const LIST_TREE: &[u8] = b"__list_tree@";
const MAP_NAME_PREFIX: &[u8] = b"__map@";
const MAP_KEY_SEPARATOR: &[u8] = b"@__item@";
#[allow(dead_code)]
const MAP_KEY_COUNT_SUFFIX: &[u8] = b"@__count@";

const LIST_NAME_PREFIX: &[u8] = b"__list@";
const LIST_KEY_COUNT_SUFFIX: &[u8] = b"@__count@";
const LIST_KEY_CONTENT_SUFFIX: &[u8] = b"@__content@";
#[allow(dead_code)]
const EXPIRE_AT_KEY_PREFIX: &[u8] = b"__expireat@";
const EXPIRE_AT_KEY_SUFFIX_DB: &[u8] = b"@__db@";
const EXPIRE_AT_KEY_SUFFIX_MAP: &[u8] = b"@__map@";
const EXPIRE_AT_KEY_SUFFIX_LIST: &[u8] = b"@__list@";

#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
enum KeyType {
    DB,
    Map,
    List,
}

enum Command {
    DBInsert(Arc<Db>, Key, Vec<u8>, oneshot::Sender<Result<()>>),
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

    #[allow(clippy::type_complexity)]
    IterNext(
        sled::Iter,
        oneshot::Sender<(sled::Iter, Option<sled::Result<(IVec, IVec)>>)>,
    ),
}

pub type CleanupFun = fn(&SledStorageDB);

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SledConfig {
    pub path: String,
    pub cache_capacity: Bytesize,
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

    #[inline]
    fn cleanup_f_default() -> CleanupFun {
        def_cleanup
    }
}

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

#[derive(Clone)]
pub struct SledStorageDB {
    pub(crate) db: Arc<sled::Db>,
    pub(crate) map_tree: sled::Tree,
    pub(crate) list_tree: sled::Tree,
    cmd_tx: mpsc::Sender<Command>,
    active_count: Arc<AtomicIsize>, //Active Command Count
}

impl SledStorageDB {
    #[inline]
    pub(crate) async fn new(cfg: SledConfig) -> Result<Self> {
        let sled_cfg = cfg.to_sled_config()?;
        let (db, map_tree, list_tree) = sled_cfg.open().map(|db| {
            let map_tree = db.open_tree(MAP_TREE);
            let list_tree = db.open_tree(LIST_TREE);
            (Arc::new(db), map_tree, list_tree)
        })?;
        let map_tree = map_tree?;
        let list_tree = list_tree?;
        let active_count = Arc::new(AtomicIsize::new(0));
        let active_count1 = active_count.clone();

        let (cmd_tx, mut cmd_rx) = tokio::sync::mpsc::channel::<Command>(300_000);
        spawn_blocking(move || {
            Handle::current().block_on(async move {
                while let Some(cmd) = cmd_rx.recv().await {
                    let err = anyhow::Error::msg("send result fail");
                    let snd_res = match cmd {
                        Command::DBInsert(db, key, val, res_tx) => res_tx
                            .send(SledStorageDB::_insert(
                                db.as_ref(),
                                key.as_slice(),
                                val.as_slice(),
                            ))
                            .map_err(|_| err),
                        Command::DBGet(db, key, res_tx) => {
                            res_tx.send(db._get(key.as_ref())).map_err(|_| err)
                        }
                        Command::DBRemove(db, key, res_tx) => {
                            res_tx.send(db._db_remove(key.as_ref())).map_err(|_| err)
                        }
                        Command::DBMapNew(db, name, expire_ms, res_tx) => {
                            let map =
                                SledStorageMap::_new_expire(name.as_ref().to_vec(), expire_ms, db);
                            res_tx.send(map).map_err(|_| err)
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
                            res_tx.send(list).map_err(|_| err)
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
                            .send(db._expire_at(key.as_ref(), at))
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
            })
        });

        let db = Self {
            db,
            map_tree,
            list_tree,
            cmd_tx,
            active_count,
        };

        (cfg.cleanup_f)(&db);

        Ok(db)
    }

    #[cfg(feature = "ttl")]
    #[inline]
    pub fn cleanup(&self, limit: usize) -> usize {
        let mut count = 0;
        let mut expire_key_vals = Vec::new();
        for item in self.db.scan_prefix(EXPIRE_AT_KEY_PREFIX) {
            if count > limit {
                break;
            }
            match item {
                Ok((expire_key, v)) => {
                    if let Some((key, key_type)) = Self::_separate_expire_key(expire_key.as_ref()) {
                        expire_key_vals.push((key, key_type, v));
                    }
                }
                Err(e) => {
                    log::warn!("{:?}", e);
                }
            }
        }
        for (key, key_type, v) in expire_key_vals {
            match self._ttl3(key_type, key.as_slice(), v.as_ref()) {
                Ok(None) => {
                    match key_type {
                        KeyType::Map => {
                            if let Err(e) = self._map_remove(key) {
                                log::warn!("{:?}", e);
                            }
                        }
                        KeyType::List => {
                            if let Err(e) = self._list_remove(key) {
                                log::warn!("{:?}", e);
                            }
                        }
                        KeyType::DB => {
                            if let Err(e) = self._db_remove(key) {
                                log::warn!("{:?}", e);
                            }
                        }
                    }
                    count += 1;
                }
                Ok(_) => {}
                Err(e) => {
                    log::warn!("{:?}", e);
                }
            }
        }

        count
    }

    #[cfg(feature = "ttl")]
    #[inline]
    pub fn cleanup_old(&self, limit: usize) -> usize {
        let mut count = 0;
        for item in self.db.scan_prefix(EXPIRE_AT_KEY_PREFIX) {
            if count > limit {
                break;
            }
            match item {
                Ok((expire_key, v)) => {
                    if let Some((key, key_type)) = Self::_separate_expire_key(expire_key.as_ref()) {
                        match self._ttl3(key_type, key.as_slice(), v.as_ref()) {
                            Ok(None) => {
                                match key_type {
                                    KeyType::Map => {
                                        if let Err(e) = self._map_remove(key) {
                                            log::warn!("{:?}", e);
                                        }
                                    }
                                    KeyType::List => {
                                        if let Err(e) = self._list_remove(key) {
                                            log::warn!("{:?}", e);
                                        }
                                    }
                                    KeyType::DB => {
                                        if let Err(e) = self._db_remove(key) {
                                            log::warn!("{:?}", e);
                                        }
                                    }
                                }
                                count += 1;
                            }
                            Ok(_) => {}
                            Err(e) => {
                                log::warn!("{:?}", e);
                            }
                        }
                    }
                }
                Err(e) => {
                    log::warn!("{:?}", e);
                }
            }
        }
        count
    }

    #[inline]
    pub fn active_count(&self) -> isize {
        self.active_count.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn db_size(&self) -> usize {
        self.db.len()
    }

    #[inline]
    pub fn map_size(&self) -> usize {
        self.map_tree.len()
    }

    #[inline]
    pub fn list_size(&self) -> usize {
        self.list_tree.len()
    }

    #[inline]
    fn _make_expire_key<K>(key: K, suffix: &[u8]) -> Key
    where
        K: AsRef<[u8]>,
    {
        [EXPIRE_AT_KEY_PREFIX, key.as_ref(), suffix].concat()
    }

    #[inline]
    fn _separate_expire_key<K>(expire_key: K) -> Option<(Key, KeyType)>
    where
        K: AsRef<[u8]>,
    {
        let expire_key = expire_key.as_ref();

        if expire_key.len() > (EXPIRE_AT_KEY_PREFIX.len() + EXPIRE_AT_KEY_SUFFIX_DB.len()) {
            if expire_key.ends_with(EXPIRE_AT_KEY_SUFFIX_MAP) {
                let key = expire_key[EXPIRE_AT_KEY_PREFIX.len()
                    ..(expire_key.len() - EXPIRE_AT_KEY_SUFFIX_MAP.len())]
                    .to_vec();
                Some((key, KeyType::Map))
            } else if expire_key.ends_with(EXPIRE_AT_KEY_SUFFIX_LIST) {
                let key = expire_key[EXPIRE_AT_KEY_PREFIX.len()
                    ..(expire_key.len() - EXPIRE_AT_KEY_SUFFIX_LIST.len())]
                    .to_vec();
                Some((key, KeyType::List))
            } else if expire_key.ends_with(EXPIRE_AT_KEY_SUFFIX_DB) {
                let key = expire_key[EXPIRE_AT_KEY_PREFIX.len()
                    ..(expire_key.len() - EXPIRE_AT_KEY_SUFFIX_DB.len())]
                    .to_vec();
                Some((key, KeyType::DB))
            } else {
                None
            }
        } else {
            None
        }
    }

    #[inline]
    fn make_map_prefix_name<K>(name: K) -> Key
    where
        K: AsRef<[u8]>,
    {
        [MAP_NAME_PREFIX, name.as_ref(), SEPARATOR].concat()
    }

    #[inline]
    fn make_map_item_prefix_name<K>(name: K) -> Key
    where
        K: AsRef<[u8]>,
    {
        [MAP_NAME_PREFIX, name.as_ref(), MAP_KEY_SEPARATOR].concat()
    }

    #[inline]
    fn make_map_count_key_name<K>(name: K) -> Key
    where
        K: AsRef<[u8]>,
    {
        [MAP_NAME_PREFIX, name.as_ref(), MAP_KEY_COUNT_SUFFIX].concat()
    }

    #[inline]
    fn map_count_key_to_name(key: &[u8]) -> &[u8] {
        key[MAP_NAME_PREFIX.len()..key.as_ref().len() - MAP_KEY_COUNT_SUFFIX.len()].as_ref()
    }

    #[inline]
    fn is_map_count_key(key: &[u8]) -> bool {
        key.starts_with(MAP_NAME_PREFIX) && key.ends_with(MAP_KEY_COUNT_SUFFIX)
    }

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

    #[inline]
    fn make_list_prefix<K>(name: K) -> Key
    where
        K: AsRef<[u8]>,
    {
        [LIST_NAME_PREFIX, name.as_ref()].concat()
    }

    #[inline]
    fn make_list_count_key(name: &[u8]) -> Vec<u8> {
        [LIST_NAME_PREFIX, name, LIST_KEY_COUNT_SUFFIX].concat()
    }

    #[inline]
    fn list_count_key_to_name(key: &[u8]) -> &[u8] {
        key[LIST_NAME_PREFIX.len()..key.as_ref().len() - LIST_KEY_COUNT_SUFFIX.len()].as_ref()
    }

    #[inline]
    fn is_list_count_key(key: &[u8]) -> bool {
        key.starts_with(LIST_NAME_PREFIX) && key.ends_with(LIST_KEY_COUNT_SUFFIX)
    }

    #[inline]
    fn _contains_key<K: AsRef<[u8]> + Sync + Send>(&self, key: K) -> Result<bool> {
        Ok(self.db.contains_key(key.as_ref())?)
    }

    #[inline]
    fn _db_contains_key<K: AsRef<[u8]> + Sync + Send>(db: &Db, key: K) -> Result<bool> {
        Ok(db.contains_key(key.as_ref())?)
    }

    #[inline]
    fn _map_contains_key<K: AsRef<[u8]> + Sync + Send>(tree: &Tree, key: K) -> Result<bool> {
        let count_key = SledStorageDB::make_map_count_key_name(key.as_ref());
        Ok(tree.contains_key(count_key)?)
    }

    #[inline]
    fn _list_contains_key<K: AsRef<[u8]> + Sync + Send>(tree: &Tree, name: K) -> Result<bool> {
        let count_key = SledStorageDB::make_list_count_key(name.as_ref());
        Ok(tree.contains_key(count_key)?)
    }

    #[inline]
    fn _map_remove<K>(&self, key: K) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        self._map(key.as_ref())._clear()?;
        #[cfg(feature = "ttl")]
        Self::_remove_expire_key(self.db.as_ref(), key.as_ref(), EXPIRE_AT_KEY_SUFFIX_MAP)
            .map_err(|e| anyhow!(format!("{:?}", e)))?;
        Ok(())
    }

    #[inline]
    fn _list_remove<K>(&self, key: K) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        self._list(key.as_ref())._clear()?;
        #[cfg(feature = "ttl")]
        Self::_remove_expire_key(self.db.as_ref(), key.as_ref(), EXPIRE_AT_KEY_SUFFIX_LIST)
            .map_err(|e| anyhow!(format!("{:?}", e)))?;
        Ok(())
    }

    #[inline]
    fn _db_remove<K>(&self, key: K) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        self.db.remove(key.as_ref())?;
        #[cfg(feature = "ttl")]
        Self::_remove_expire_key(self.db.as_ref(), key.as_ref(), EXPIRE_AT_KEY_SUFFIX_DB)
            .map_err(|e| anyhow!(format!("{:?}", e)))?;
        Ok(())
    }

    #[cfg(feature = "ttl")]
    #[inline]
    fn _remove_expire_key<K>(db: &sled::Db, key: K, suffix: &[u8]) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        let expire_key = Self::_make_expire_key(key, suffix);
        db.remove(expire_key)?;
        Ok(())
    }

    #[cfg(feature = "ttl")]
    #[inline]
    fn _batch_remove_expire_key<K>(b: &mut Batch, key: K, suffix: &[u8])
    where
        K: AsRef<[u8]>,
    {
        let expire_key = Self::_make_expire_key(key, suffix);
        b.remove(expire_key);
    }

    #[inline]
    fn _is_expired<K, F>(&self, _key: K, _suffix: &[u8], _contains_key_f: F) -> Result<bool>
    where
        K: AsRef<[u8]> + Sync + Send,
        F: Fn(&[u8]) -> Result<bool>,
    {
        #[cfg(feature = "ttl")]
        {
            let expire_key = Self::_make_expire_key(_key.as_ref(), _suffix);
            let res = self
                ._ttl2(_key.as_ref(), expire_key.as_slice(), _contains_key_f)?
                .and_then(|ttl| if ttl > 0 { Some(()) } else { None });
            Ok(res.is_none())
        }
        #[cfg(not(feature = "ttl"))]
        Ok(false)
    }

    #[inline]
    fn _ttl<K, F>(
        &self,
        key: K,
        suffix: &[u8],
        contains_key_f: F,
    ) -> Result<Option<TimestampMillis>>
    where
        K: AsRef<[u8]> + Sync + Send,
        F: Fn(&[u8]) -> Result<bool>,
    {
        let expire_key = Self::_make_expire_key(key.as_ref(), suffix);
        self._ttl2(key.as_ref(), expire_key.as_slice(), contains_key_f)
    }

    #[inline]
    fn _ttl2<K, F>(
        &self,
        c_key: K,
        expire_key: K,
        contains_key_f: F,
    ) -> Result<Option<TimestampMillis>>
    where
        K: AsRef<[u8]> + Sync + Send,
        F: Fn(&[u8]) -> Result<bool>,
    {
        let ttl_res = match self.db.get(expire_key) {
            Ok(Some(v)) => {
                if contains_key_f(c_key.as_ref())? {
                    Ok(Some(TimestampMillis::from_be_bytes(v.as_ref().try_into()?)))
                } else {
                    Ok(None)
                }
            }
            Ok(None) => {
                if contains_key_f(c_key.as_ref())? {
                    Ok(Some(TimestampMillis::MAX))
                } else {
                    Ok(None)
                }
            }
            Err(e) => Err(anyhow!(e)),
        }?;

        let ttl_res = if let Some(at) = ttl_res {
            let now = timestamp_millis();
            if now > at {
                //is expire
                None
            } else {
                Some(at - now)
            }
        } else {
            None
        };
        Ok(ttl_res)
    }

    #[inline]
    fn _ttl3<K>(
        &self,
        key_type: KeyType,
        c_key: K,
        ttl_val: &[u8],
    ) -> Result<Option<TimestampMillis>>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let ttl_at = match key_type {
            KeyType::Map => {
                if Self::_map_contains_key(&self.map_tree, c_key)? {
                    Some(TimestampMillis::from_be_bytes(ttl_val.as_ref().try_into()?))
                } else {
                    None
                }
            }
            KeyType::List => {
                if Self::_list_contains_key(&self.list_tree, c_key)? {
                    Some(TimestampMillis::from_be_bytes(ttl_val.as_ref().try_into()?))
                } else {
                    None
                }
            }
            KeyType::DB => {
                if self._contains_key(c_key)? {
                    Some(TimestampMillis::from_be_bytes(ttl_val.as_ref().try_into()?))
                } else {
                    None
                }
            }
        };

        let ttl_millis = if let Some(at) = ttl_at {
            let now = timestamp_millis() + (1000 * 10);
            if now > at {
                None
            } else {
                Some(at - now)
            }
        } else {
            None
        };
        Ok(ttl_millis)
    }

    #[inline]
    fn _insert(db: &Db, key: &[u8], val: &[u8]) -> Result<()> {
        db.insert(key, val)?;
        #[cfg(feature = "ttl")]
        Self::_remove_expire_key(db, key, EXPIRE_AT_KEY_SUFFIX_DB)?;
        Ok(())
    }

    #[inline]
    fn _get(&self, key: &[u8]) -> Result<Option<IVec>> {
        let this = self;
        let res = if this._is_expired(key.as_ref(), EXPIRE_AT_KEY_SUFFIX_DB, |k| {
            Self::_db_contains_key(this.db.as_ref(), k)
        })? {
            None
        } else {
            this.db.get(key)?
        };
        Ok(res)
    }

    #[inline]
    fn _self_map_contains_key(&self, key: &[u8]) -> Result<bool> {
        let this = self;
        if this._is_expired(key.as_ref(), EXPIRE_AT_KEY_SUFFIX_MAP, |k| {
            Self::_map_contains_key(&this.map_tree, k)
        })? {
            Ok(false)
        } else {
            Self::_map_contains_key(&this.map_tree, key)
        }
    }

    #[inline]
    fn _self_list_contains_key(&self, key: &[u8]) -> Result<bool> {
        let this = self;
        if this._is_expired(key, EXPIRE_AT_KEY_SUFFIX_LIST, |k| {
            Self::_list_contains_key(&this.list_tree, k)
        })? {
            Ok(false)
        } else {
            Self::_list_contains_key(&this.list_tree, key)
        }
    }

    #[inline]
    fn _batch_insert(&self, key_vals: Vec<(Key, IVec)>) -> Result<()> {
        if key_vals.is_empty() {
            return Ok(());
        }

        let mut batch = Batch::default();
        for (k, v) in key_vals.iter() {
            batch.insert(k.as_slice(), v.as_ref());
        }

        #[cfg(feature = "ttl")]
        let keys = key_vals.into_iter().map(|(k, _)| k).collect::<Vec<Key>>();

        let this = self;

        #[cfg(feature = "ttl")]
        {
            let mut remove_expire_batch = Batch::default();
            for k in keys {
                if this._is_expired(k.as_slice(), EXPIRE_AT_KEY_SUFFIX_DB, |k| {
                    Self::_db_contains_key(this.db.as_ref(), k)
                })? {
                    SledStorageDB::_batch_remove_expire_key(
                        &mut remove_expire_batch,
                        k,
                        EXPIRE_AT_KEY_SUFFIX_DB,
                    );
                }
            }
            this.db.apply_batch(remove_expire_batch)?;
        }

        this.db.apply_batch(batch)?;

        Ok(())
    }

    #[inline]
    fn _batch_remove(&self, keys: Vec<Key>) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }

        let this = self;
        let mut batch = Batch::default();
        for k in keys.iter() {
            batch.remove(k.as_slice());
        }
        this.db.apply_batch(batch)?;
        #[cfg(feature = "ttl")]
        {
            let mut remove_expire_batch = Batch::default();
            for k in keys.iter() {
                SledStorageDB::_batch_remove_expire_key(
                    &mut remove_expire_batch,
                    k,
                    EXPIRE_AT_KEY_SUFFIX_DB,
                );
            }
            this.db.apply_batch(remove_expire_batch)?;
        }

        Ok(())
    }

    #[inline]
    fn _counter_incr(&self, key: &[u8], increment: isize) -> Result<()> {
        self.db.fetch_and_update(key, |old: Option<&[u8]>| {
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

    #[inline]
    fn _counter_decr(&self, key: &[u8], decrement: isize) -> Result<()> {
        self.db.fetch_and_update(key, |old: Option<&[u8]>| {
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

    #[inline]
    fn _counter_get(&self, key: &[u8]) -> Result<Option<isize>> {
        let this = self;
        if this._is_expired(key, EXPIRE_AT_KEY_SUFFIX_DB, |k| {
            Self::_db_contains_key(this.db.as_ref(), k)
        })? {
            Ok(None)
        } else if let Some(v) = this.db.get(key)? {
            Ok(Some(isize::from_be_bytes(v.as_ref().try_into()?)))
        } else {
            Ok(None)
        }
    }

    #[inline]
    fn _counter_set(&self, key: &[u8], val: isize) -> Result<()> {
        let db = self.db.as_ref();
        let val = val.to_be_bytes().to_vec();

        db.insert(key, val.as_slice())?;
        #[cfg(feature = "ttl")]
        Self::_remove_expire_key(db, key, EXPIRE_AT_KEY_SUFFIX_DB)?;

        Ok(())
    }

    #[inline]
    fn _self_contains_key(&self, key: &[u8]) -> Result<bool> {
        let this = self;
        if this._is_expired(key, EXPIRE_AT_KEY_SUFFIX_DB, |k| {
            Self::_db_contains_key(this.db.as_ref(), k)
        })? {
            Ok(false)
        } else {
            this._contains_key(key)
        }
    }

    #[inline]
    #[cfg(feature = "ttl")]
    fn _expire_at(&self, key: &[u8], at: TimestampMillis) -> Result<bool> {
        let expire_key = Self::_make_expire_key(key, EXPIRE_AT_KEY_SUFFIX_DB);
        if self._contains_key(key)? {
            let at_bytes = at.to_be_bytes();
            self.db
                .insert(expire_key, at_bytes.as_slice())
                .map_err(|e| anyhow!(e))
                .map(|_| true)
        } else {
            Ok(false)
        }
    }

    #[inline]
    #[cfg(feature = "ttl")]
    fn _self_ttl(&self, key: &[u8]) -> Result<Option<TimestampMillis>> {
        self._ttl(key, EXPIRE_AT_KEY_SUFFIX_DB, |k| {
            Self::_db_contains_key(self.db.as_ref(), k)
        })
    }

    #[inline]
    fn _map_scan_prefix(&self) -> sled::Iter {
        self.map_tree.scan_prefix(MAP_NAME_PREFIX)
    }

    #[inline]
    fn _list_scan_prefix(&self) -> sled::Iter {
        self.list_tree.scan_prefix(LIST_NAME_PREFIX)
    }

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

    #[inline]
    fn _map<N: AsRef<[u8]>>(&self, name: N) -> SledStorageMap {
        SledStorageMap::_new(name.as_ref().to_vec(), self.clone())
    }

    #[inline]
    fn _list<V: AsRef<[u8]>>(&self, name: V) -> SledStorageList {
        SledStorageList::_new(name.as_ref().to_vec(), self.clone())
    }
}

#[async_trait]
impl StorageDB for SledStorageDB {
    type MapType = SledStorageMap;
    type ListType = SledStorageList;

    #[inline]
    async fn map<N: AsRef<[u8]> + Sync + Send>(
        &self,
        name: N,
        expire: Option<TimestampMillis>,
    ) -> Result<Self::MapType> {
        SledStorageMap::new_expire(name.as_ref().to_vec(), expire, self.clone()).await
    }

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

    #[inline]
    async fn list<V: AsRef<[u8]> + Sync + Send>(
        &self,
        name: V,
        expire: Option<TimestampMillis>,
    ) -> Result<Self::ListType> {
        SledStorageList::new_expire(name.as_ref().to_vec(), expire, self.clone()).await
    }

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

    #[inline]
    async fn insert<K, V>(&self, key: K, val: &V) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: serde::ser::Serialize + Sync + Send,
    {
        let val = bincode::serialize(val)?;
        let (tx, rx) = oneshot::channel();
        self.cmd_send(Command::DBInsert(
            self.db.clone(),
            key.as_ref().to_vec(),
            val,
            tx,
        ))
        .await?;
        rx.await??;
        Ok(())
    }

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
            Some(v) => Ok(Some(bincode::deserialize::<V>(v.as_ref())?)),
            None => Ok(None),
        }
    }

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
                bincode::serialize(&v)
                    .map(|v| (k, v.into()))
                    .map_err(|e| anyhow!(e))
            })
            .collect::<Result<Vec<_>>>()?;

        let (tx, rx) = oneshot::channel();
        self.cmd_send(Command::DBBatchInsert(self.clone(), key_vals, tx))
            .await?;
        Ok(rx.await??)
    }

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

    #[inline]
    #[cfg(feature = "ttl")]
    async fn expire<K>(&self, key: K, dur: TimestampMillis) -> Result<bool>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let at = timestamp_millis() + dur;
        self.expire_at(key, at).await
    }

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

    #[inline]
    async fn info(&self) -> Result<Value> {
        let active_count = self.active_count.load(Ordering::Relaxed);
        let this = self.clone();
        Ok(spawn_blocking(move || {
            let size_on_disk = this.db.size_on_disk().unwrap_or_default();
            let db_size = this.db_size();
            let map_size = this.map_size();
            let list_size = this.list_size();

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
                "db_size": db_size,
                "map_size": map_size,
                "list_size": list_size,
                "size_on_disk": size_on_disk,
                // "db_keys": db_keys,
                // "map_names": map_names,
                // "list_names": list_names,
            })
        })
        .await?)
    }
}

#[derive(Clone)]
pub struct SledStorageMap {
    name: Key,
    map_prefix_name: Key,
    map_item_prefix_name: Key,
    map_count_key_name: Key,
    empty: Arc<AtomicBool>,
    pub(crate) db: SledStorageDB,
}

impl SledStorageMap {
    #[inline]
    async fn new_expire(
        name: Key,
        expire_ms: Option<TimestampMillis>,
        db: SledStorageDB,
    ) -> Result<Self> {
        let (tx, rx) = oneshot::channel();
        db.cmd_send(Command::DBMapNew(db.clone(), name.into(), expire_ms, tx))
            .await?;
        rx.await?
    }

    #[inline]
    fn _new_expire(
        name: Key,
        _expire_ms: Option<TimestampMillis>,
        db: SledStorageDB,
    ) -> Result<Self> {
        let m = Self::_new(name, db);
        m.empty.store(m._is_empty()?, Ordering::SeqCst);
        #[cfg(feature = "ttl")]
        if let Some(expire_ms) = _expire_ms.as_ref() {
            m._expire_at(timestamp_millis() + *expire_ms)?;
        }
        Ok(m)
    }

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

    #[inline]
    fn tree(&self) -> &sled::Tree {
        &self.db.map_tree
    }

    #[inline]
    fn make_map_item_key<K: AsRef<[u8]>>(&self, key: K) -> Key {
        [self.map_item_prefix_name.as_ref(), key.as_ref()].concat()
    }

    #[cfg(feature = "map_len")]
    #[inline]
    fn _len_get(&self) -> Result<isize> {
        self._counter_get(self.map_count_key_name.as_slice())
    }

    #[inline]
    fn _tx_counter_inc<K: AsRef<[u8]>>(
        tx: &TransactionalTree,
        key: K,
    ) -> ConflictableTransactionResult<()> {
        let val = match tx.get(key.as_ref())? {
            Some(data) => {
                if let Ok(array) = data.as_ref().try_into() {
                    let number = isize::from_be_bytes(array);
                    number + 1
                } else {
                    1
                }
            }
            None => 1,
        };
        tx.insert(key.as_ref(), val.to_be_bytes().as_slice())?;
        Ok(())
    }

    #[inline]
    fn _tx_counter_dec<K: AsRef<[u8]>>(
        tx: &TransactionalTree,
        key: K,
    ) -> ConflictableTransactionResult<()> {
        let val = match tx.get(key.as_ref())? {
            Some(data) => {
                if let Ok(array) = data.as_ref().try_into() {
                    let number = isize::from_be_bytes(array);
                    number - 1
                } else {
                    -1
                }
            }
            None => -1,
        };
        if val > 0 {
            tx.insert(key.as_ref(), val.to_be_bytes().as_slice())?;
        } else {
            tx.remove(key.as_ref())?;
        }
        Ok(())
    }

    #[inline]
    fn _tx_counter_get<K: AsRef<[u8]>, E>(
        tx: &TransactionalTree,
        key: K,
    ) -> ConflictableTransactionResult<isize, E> {
        if let Some(v) = tx.get(key)? {
            let c = match v.as_ref().try_into() {
                Ok(c) => c,
                Err(e) => {
                    return Err(ConflictableTransactionError::Storage(sled::Error::Io(
                        io::Error::new(ErrorKind::InvalidData, e),
                    )))
                }
            };
            Ok(isize::from_be_bytes(c))
        } else {
            Ok(0)
        }
    }

    #[inline]
    fn _tx_counter_set<K: AsRef<[u8]>, E>(
        tx: &TransactionalTree,
        key: K,
        val: isize,
    ) -> ConflictableTransactionResult<(), E> {
        tx.insert(key.as_ref(), val.to_be_bytes().as_slice())?;
        Ok(())
    }

    #[inline]
    fn _tx_counter_remove<K: AsRef<[u8]>, E>(
        tx: &TransactionalTree,
        key: K,
    ) -> ConflictableTransactionResult<(), E> {
        tx.remove(key.as_ref())?;
        Ok(())
    }

    #[inline]
    fn _counter_get<K: AsRef<[u8]>>(&self, key: K) -> Result<isize> {
        if let Some(v) = self.tree().get(key)? {
            Ok(isize::from_be_bytes(v.as_ref().try_into()?))
        } else {
            Ok(0)
        }
    }

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

    #[inline]
    fn _clear(&self) -> Result<()> {
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
        self.tree().apply_batch(batch)?;
        self.empty.store(true, Ordering::SeqCst);
        Ok(())
    }

    #[inline]
    fn _insert(&self, key: IVec, val: IVec) -> Result<()> {
        let item_key = self.make_map_item_key(key.as_ref());
        let this = self;
        #[cfg(feature = "map_len")]
        {
            let count_key = this.map_count_key_name.as_slice();
            this.tree()
                .transaction(move |tx| {
                    if tx.insert(item_key.as_slice(), val.as_ref())?.is_none() {
                        Self::_tx_counter_inc(tx, count_key)?;
                    }
                    Ok(())
                })
                .map_err(|e| anyhow!(format!("{:?}", e)))?;
        }
        #[cfg(not(feature = "map_len"))]
        {
            if self.empty.load(Ordering::SeqCst) {
                self._counter_init()?;
                self.empty.store(false, Ordering::SeqCst)
            }
            this.tree().insert(item_key.as_slice(), val.as_ref())?;
        }

        #[cfg(feature = "ttl")]
        {
            if this
                .db
                ._is_expired(this.name.as_slice(), EXPIRE_AT_KEY_SUFFIX_MAP, |k| {
                    SledStorageDB::_map_contains_key(this.tree(), k)
                })?
            {
                SledStorageDB::_remove_expire_key(
                    this.db.db.as_ref(),
                    this.name.as_slice(),
                    EXPIRE_AT_KEY_SUFFIX_MAP,
                )?;
            }
        }

        Ok(())
    }

    #[inline]
    fn _get(&self, key: IVec) -> Result<Option<IVec>> {
        let this = self;
        let item_key = self.make_map_item_key(key.as_ref());
        let res = if !this
            .db
            ._is_expired(this.name.as_slice(), EXPIRE_AT_KEY_SUFFIX_MAP, |k| {
                SledStorageDB::_map_contains_key(this.tree(), k)
            })? {
            this.tree().get(item_key).map_err(|e| anyhow!(e))?
        } else {
            None
        };
        Ok(res)
    }

    #[inline]
    fn _remove(&self, key: IVec) -> Result<()> {
        let tree = self.tree();
        let key = self.make_map_item_key(key.as_ref());
        #[cfg(feature = "map_len")]
        let count_key = self.map_count_key_name.to_vec();

        #[cfg(feature = "map_len")]
        {
            tree.transaction(move |tx| {
                if tx.remove(key.as_slice())?.is_some() {
                    Self::_tx_counter_dec(tx, count_key.as_slice())?;
                }
                Ok(())
            })
            .map_err(|e| anyhow!(format!("{:?}", e)))?;
        }

        #[cfg(not(feature = "map_len"))]
        {
            tree.remove(key.as_slice())?;
        }

        Ok(())
    }

    #[inline]
    fn _contains_key(&self, key: IVec) -> Result<bool> {
        let key = self.make_map_item_key(key.as_ref());
        Ok(self.tree().contains_key(key)?)
    }

    #[cfg(feature = "map_len")]
    #[inline]
    fn _len(&self) -> Result<usize> {
        let this = self;
        let len = {
            if this
                .db
                ._is_expired(this.name.as_slice(), EXPIRE_AT_KEY_SUFFIX_MAP, |k| {
                    SledStorageDB::_map_contains_key(this.tree(), k)
                })?
            {
                Ok(0)
            } else {
                this._len_get()
            }
        }?;

        Ok(len as usize)
    }

    #[inline]
    fn _is_empty(&self) -> Result<bool> {
        let this = self;
        let res = {
            if this
                .db
                ._is_expired(this.name.as_slice(), EXPIRE_AT_KEY_SUFFIX_MAP, |k| {
                    SledStorageDB::_map_contains_key(this.tree(), k)
                })?
            {
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

    #[inline]
    fn _remove_and_fetch(&self, key: IVec) -> Result<Option<IVec>> {
        let key = self.make_map_item_key(key.as_ref());
        let this = self;
        let removed = {
            if this
                .db
                ._is_expired(this.name.as_slice(), EXPIRE_AT_KEY_SUFFIX_MAP, |k| {
                    SledStorageDB::_map_contains_key(this.tree(), k)
                })?
            {
                Ok(None)
            } else {
                #[cfg(feature = "map_len")]
                {
                    let count_key = this.map_count_key_name.to_vec();
                    this.tree().transaction(move |tx| {
                        if let Some(removed) = tx.remove(key.as_slice())? {
                            Self::_tx_counter_dec(tx, count_key.as_slice())?;
                            Ok(Some(removed))
                        } else {
                            Ok(None)
                        }
                    })
                }
                #[cfg(not(feature = "map_len"))]
                {
                    let removed = this.tree().remove(key.as_slice())?;
                    Ok::<_, TransactionError<()>>(removed)
                }
            }
        }
        .map_err(|e| anyhow!(format!("{:?}", e)))?;

        Ok(removed)
    }

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
                tree.transaction(move |tx| {
                    let len = Self::_tx_counter_get(tx, map_count_key_name.as_slice())? - c;
                    if len > 0 {
                        Self::_tx_counter_set(tx, map_count_key_name.as_slice(), len)?;
                    } else {
                        Self::_tx_counter_remove(tx, map_count_key_name.as_slice())?;
                    };
                    tx.apply_batch(&removeds)?;
                    Ok::<(), ConflictableTransactionError<sled::Error>>(())
                })
            }
            #[cfg(not(feature = "map_len"))]
            {
                tree.apply_batch(removeds)?;
                Ok::<(), ConflictableTransactionError<sled::Error>>(())
            }
        }?;
        Ok(())
    }

    #[inline]
    fn _batch_insert(&self, key_vals: Vec<(IVec, IVec)>) -> Result<()> {
        for (k, v) in key_vals {
            self._insert(k, v)?;
        }
        Ok(())
    }

    #[inline]
    fn _batch_remove(&self, keys: Vec<IVec>) -> Result<()> {
        for k in keys {
            self._remove(k)?;
        }
        Ok(())
    }

    #[cfg(feature = "ttl")]
    #[inline]
    fn _expire_at(&self, at: TimestampMillis) -> Result<bool> {
        let this = self;
        let expire_key =
            SledStorageDB::_make_expire_key(self.name.as_slice(), EXPIRE_AT_KEY_SUFFIX_MAP);
        let res = {
            let at_bytes = at.to_be_bytes();
            this.db
                .db
                .insert(expire_key, at_bytes.as_slice())
                .map_err(|e| anyhow!(e))
                .map(|_| true)
        }?;
        Ok(res)
    }

    #[cfg(feature = "ttl")]
    #[inline]
    fn _ttl(&self) -> Result<Option<TimestampMillis>> {
        let res = self.db._ttl(self.name(), EXPIRE_AT_KEY_SUFFIX_MAP, |k| {
            SledStorageDB::_map_contains_key(self.tree(), k)
        })?;
        Ok(res)
    }

    #[inline]
    fn _is_expired(&self) -> Result<bool> {
        self.db
            ._is_expired(self.name.as_slice(), EXPIRE_AT_KEY_SUFFIX_MAP, |k| {
                SledStorageDB::_map_contains_key(self.tree(), k)
            })
    }

    #[inline]
    async fn call_is_expired(&self) -> Result<bool> {
        let (tx, rx) = oneshot::channel();
        self.db
            .cmd_send(Command::MapIsExpired(self.clone(), tx))
            .await?;
        rx.await?
    }

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

    #[inline]
    async fn call_prefix_iter(&self, prefix: Option<IVec>) -> Result<sled::Iter> {
        let (tx, rx) = oneshot::channel();
        self.db
            .cmd_send(Command::MapPrefixIter(self.clone(), prefix, tx))
            .await?;
        Ok(rx.await?)
    }
}

#[async_trait]
impl Map for SledStorageMap {
    #[inline]
    fn name(&self) -> &[u8] {
        self.name.as_slice()
    }

    #[inline]
    async fn insert<K, V>(&self, key: K, val: &V) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: Serialize + Sync + Send + ?Sized,
    {
        let val = bincode::serialize(val)?;
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
            Some(v) => Ok(Some(bincode::deserialize::<V>(v.as_ref())?)),
            None => Ok(None),
        }
    }

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

    #[cfg(feature = "map_len")]
    #[inline]
    async fn len(&self) -> Result<usize> {
        let (tx, rx) = oneshot::channel();
        self.db.cmd_send(Command::MapLen(self.clone(), tx)).await?;
        Ok(rx.await??)
    }

    #[inline]
    async fn is_empty(&self) -> Result<bool> {
        let (tx, rx) = oneshot::channel();
        self.db
            .cmd_send(Command::MapIsEmpty(self.clone(), tx))
            .await?;
        Ok(rx.await??)
    }

    #[inline]
    async fn clear(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.db
            .cmd_send(Command::MapClear(self.clone(), tx))
            .await?;
        rx.await??;
        Ok(())
    }

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
            Some(v) => Ok(Some(bincode::deserialize::<V>(v.as_ref())?)),
            None => Ok(None),
        }
    }

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

    #[inline]
    async fn batch_insert<V>(&self, key_vals: Vec<(Key, V)>) -> Result<()>
    where
        V: serde::ser::Serialize + Sync + Send,
    {
        let key_vals = key_vals
            .into_iter()
            .map(|(k, v)| {
                bincode::serialize(&v)
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

    #[cfg(feature = "ttl")]
    async fn expire_at(&self, at: TimestampMillis) -> Result<bool> {
        let (tx, rx) = oneshot::channel();
        self.db
            .cmd_send(Command::MapExpireAt(self.clone(), at, tx))
            .await?;
        Ok(rx.await??)
    }

    #[cfg(feature = "ttl")]
    async fn expire(&self, dur: TimestampMillis) -> Result<bool> {
        let at = timestamp_millis() + dur;
        self.expire_at(at).await
    }

    #[cfg(feature = "ttl")]
    async fn ttl(&self) -> Result<Option<TimestampMillis>> {
        let (tx, rx) = oneshot::channel();
        self.db.cmd_send(Command::MapTTL(self.clone(), tx)).await?;
        Ok(rx.await??)
    }
}

#[derive(Clone)]
pub struct SledStorageList {
    name: Key,
    prefix_name: Key,
    pub(crate) db: SledStorageDB,
}

impl SledStorageList {
    #[inline]
    async fn new_expire(
        name: Key,
        expire_ms: Option<TimestampMillis>,
        db: SledStorageDB,
    ) -> Result<Self> {
        let (tx, rx) = oneshot::channel();
        db.cmd_send(Command::DBListNew(db.clone(), name.into(), expire_ms, tx))
            .await?;
        rx.await?
    }

    #[inline]
    fn _new_expire(
        name: Key,
        _expire_ms: Option<TimestampMillis>,
        db: SledStorageDB,
    ) -> Result<Self> {
        let l = Self::_new(name, db);
        #[cfg(feature = "ttl")]
        if let Some(expire_ms) = _expire_ms {
            l._expire_at(timestamp_millis() + expire_ms)?;
        }
        Ok(l)
    }

    #[inline]
    fn _new(name: Key, db: SledStorageDB) -> Self {
        let prefix_name = SledStorageDB::make_list_prefix(name.as_slice());
        SledStorageList {
            name,
            prefix_name,
            db,
        }
    }

    #[inline]
    pub(crate) fn name(&self) -> &[u8] {
        self.name.as_slice()
    }

    #[inline]
    pub(crate) fn tree(&self) -> &sled::Tree {
        &self.db.list_tree
    }

    #[inline]
    fn make_list_count_key(&self) -> Vec<u8> {
        let list_count_key = [self.prefix_name.as_ref(), LIST_KEY_COUNT_SUFFIX].concat();
        list_count_key
    }

    #[inline]
    fn make_list_content_prefix(prefix_name: &[u8], idx: Option<&[u8]>) -> Vec<u8> {
        if let Some(idx) = idx {
            [prefix_name, LIST_KEY_CONTENT_SUFFIX, idx].concat()
        } else {
            [prefix_name, LIST_KEY_CONTENT_SUFFIX].concat()
        }
    }

    #[inline]
    fn make_list_content_key(&self, idx: usize) -> Vec<u8> {
        Self::make_list_content_prefix(
            self.prefix_name.as_ref(),
            Some(idx.to_be_bytes().as_slice()),
        )
    }

    #[inline]
    fn make_list_content_keys(&self, start: usize, end: usize) -> Vec<Vec<u8>> {
        (start..end)
            .map(|idx| self.make_list_content_key(idx))
            .collect()
    }

    #[inline]
    fn tx_list_count_get<K, E>(
        tx: &TransactionalTree,
        list_count_key: K,
    ) -> ConflictableTransactionResult<(usize, usize), E>
    where
        K: AsRef<[u8]>,
    {
        if let Some(v) = tx.get(list_count_key.as_ref())? {
            let (start, end) = bincode::deserialize::<(usize, usize)>(v.as_ref()).map_err(|e| {
                ConflictableTransactionError::Storage(sled::Error::Io(io::Error::new(
                    ErrorKind::InvalidData,
                    e,
                )))
            })?;
            Ok((start, end))
        } else {
            Ok((0, 0))
        }
    }

    #[inline]
    fn tx_list_count_set<K, E>(
        tx: &TransactionalTree,
        key_count: K,
        start: usize,
        end: usize,
    ) -> ConflictableTransactionResult<(), E>
    where
        K: AsRef<[u8]>,
    {
        let count_bytes = bincode::serialize(&(start, end)).map_err(|e| {
            ConflictableTransactionError::Storage(sled::Error::Io(io::Error::new(
                ErrorKind::InvalidData,
                e,
            )))
        })?;
        tx.insert(key_count.as_ref(), count_bytes.as_slice())?;
        Ok(())
    }

    #[inline]
    fn tx_list_content_set<K, V, E>(
        tx: &TransactionalTree,
        key_content: K,
        data: V,
    ) -> ConflictableTransactionResult<(), E>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        tx.insert(key_content.as_ref(), data.as_ref())?;
        Ok(())
    }

    #[inline]
    fn tx_list_content_batch_set<K, V, E>(
        tx: &TransactionalTree,
        key_contents: Vec<(K, V)>,
    ) -> ConflictableTransactionResult<(), E>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let mut batch = Batch::default();
        for (k, v) in key_contents {
            batch.insert(k.as_ref(), v.as_ref());
        }
        tx.apply_batch(&batch)?;
        Ok(())
    }

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

    #[inline]
    fn _push(&self, data: IVec) -> Result<()> {
        let this = self;

        this.tree().transaction(move |tx| {
            let list_count_key = this.make_list_count_key();
            let (start, mut end) = Self::tx_list_count_get::<
                _,
                ConflictableTransactionError<sled::Error>,
            >(tx, list_count_key.as_slice())?;
            end += 1;
            Self::tx_list_count_set(tx, list_count_key.as_slice(), start, end)?;

            let list_content_key = this.make_list_content_key(end);
            Self::tx_list_content_set(tx, list_content_key.as_slice(), data.as_ref())?;
            Ok(())
        })?;

        #[cfg(feature = "ttl")]
        {
            if this
                .db
                ._is_expired(this.name.as_slice(), EXPIRE_AT_KEY_SUFFIX_LIST, |k| {
                    SledStorageDB::_list_contains_key(this.tree(), k)
                })?
            {
                SledStorageDB::_remove_expire_key(
                    this.db.db.as_ref(),
                    this.name.as_slice(),
                    EXPIRE_AT_KEY_SUFFIX_LIST,
                )?;
            }
        }

        Ok(())
    }

    #[inline]
    fn _pushs(&self, vals: Vec<IVec>) -> Result<()> {
        if vals.is_empty() {
            return Ok(());
        }

        let tree = self.tree();
        let this = self;

        tree.transaction(move |tx| {
            let list_count_key = this.make_list_count_key();
            let (start, mut end) = Self::tx_list_count_get::<
                _,
                ConflictableTransactionError<sled::Error>,
            >(tx, list_count_key.as_slice())?;

            let mut list_content_keys = this.make_list_content_keys(end + 1, end + vals.len() + 1);
            //assert_eq!(vals.len(), list_content_keys.len());
            end += vals.len();
            Self::tx_list_count_set(tx, list_count_key.as_slice(), start, end)?;

            let list_contents = vals
                .iter()
                .map(|val| (list_content_keys.remove(0), val))
                .collect::<Vec<_>>();
            Self::tx_list_content_batch_set(tx, list_contents)?;
            Ok(())
        })?;

        #[cfg(feature = "ttl")]
        {
            if this
                .db
                ._is_expired(this.name.as_slice(), EXPIRE_AT_KEY_SUFFIX_LIST, |k| {
                    SledStorageDB::_list_contains_key(this.tree(), k)
                })?
            {
                SledStorageDB::_remove_expire_key(
                    this.db.db.as_ref(),
                    this.name.as_slice(),
                    EXPIRE_AT_KEY_SUFFIX_LIST,
                )?;
            }
        }
        Ok(())
    }

    #[inline]
    fn _push_limit(
        &self,
        data: IVec,
        limit: usize,
        pop_front_if_limited: bool,
    ) -> Result<Option<IVec>> {
        let tree = self.tree();
        let this = self;

        let removed = {
            let res = tree.transaction(move |tx| {
                let list_count_key = this.make_list_count_key();
                let (mut start, mut end) = Self::tx_list_count_get::<
                    _,
                    ConflictableTransactionError<sled::Error>,
                >(tx, list_count_key.as_slice())?;
                let count = end - start;

                if count < limit {
                    end += 1;
                    Self::tx_list_count_set(tx, list_count_key.as_slice(), start, end)?;
                    let list_content_key = this.make_list_content_key(end);
                    Self::tx_list_content_set(tx, list_content_key.as_slice(), data.as_ref())?;
                    Ok(None)
                } else if pop_front_if_limited {
                    let mut removed = None;
                    let removed_content_key = this.make_list_content_key(start + 1);
                    if let Some(v) = tx.remove(removed_content_key)? {
                        removed = Some(v);
                        start += 1;
                    }
                    end += 1;
                    Self::tx_list_count_set(tx, list_count_key.as_slice(), start, end)?;
                    let list_content_key = this.make_list_content_key(end);
                    Self::tx_list_content_set(tx, list_content_key.as_slice(), data.as_ref())?;
                    Ok(removed)
                } else {
                    Err(ConflictableTransactionError::Storage(sled::Error::Io(
                        io::Error::new(ErrorKind::InvalidData, "Is full"),
                    )))
                }
            });

            #[cfg(feature = "ttl")]
            {
                if this
                    .db
                    ._is_expired(this.name.as_slice(), EXPIRE_AT_KEY_SUFFIX_LIST, |k| {
                        SledStorageDB::_list_contains_key(this.tree(), k)
                    })?
                {
                    SledStorageDB::_remove_expire_key(
                        this.db.db.as_ref(),
                        this.name.as_slice(),
                        EXPIRE_AT_KEY_SUFFIX_LIST,
                    )?;
                }
            }

            Ok::<_, TransactionError<()>>(res)
        }
        .map_err(|e| anyhow!(format!("{:?}", e)))??;

        Ok(removed)
    }

    #[inline]
    fn _pop(&self) -> Result<Option<IVec>> {
        let this = self;
        let removed = {
            if this
                .db
                ._is_expired(this.name.as_slice(), EXPIRE_AT_KEY_SUFFIX_LIST, |k| {
                    SledStorageDB::_list_contains_key(this.tree(), k)
                })?
            {
                Ok(None)
            } else {
                let removed = this.tree().transaction(move |tx| {
                    let list_count_key = this.make_list_count_key();
                    let (start, end) = Self::tx_list_count_get(tx, list_count_key.as_slice())?;

                    let mut removed = None;
                    if (end - start) > 0 {
                        let removed_content_key = this.make_list_content_key(start + 1);
                        if let Some(v) = tx.remove(removed_content_key)? {
                            removed = Some(v);
                            Self::tx_list_count_set(tx, list_count_key.as_slice(), start + 1, end)?;
                        }
                    }
                    Ok::<_, ConflictableTransactionError<sled::Error>>(removed)
                });
                removed
            }
        }?;

        Ok(removed)
    }

    #[inline]
    fn _all(&self) -> Result<Vec<IVec>> {
        let this = self;
        let res = {
            if this
                .db
                ._is_expired(this.name.as_slice(), EXPIRE_AT_KEY_SUFFIX_LIST, |k| {
                    SledStorageDB::_list_contains_key(this.tree(), k)
                })?
            {
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

    #[inline]
    fn _get_index(&self, idx: usize) -> Result<Option<IVec>> {
        let this = self;
        let res = {
            if this
                .db
                ._is_expired(this.name.as_slice(), EXPIRE_AT_KEY_SUFFIX_LIST, |k| {
                    SledStorageDB::_list_contains_key(this.tree(), k)
                })?
            {
                Ok(None)
            } else {
                this.tree().transaction(move |tx| {
                    let list_count_key = this.make_list_count_key();
                    let (start, end) = Self::tx_list_count_get::<
                        _,
                        ConflictableTransactionError<sled::Error>,
                    >(tx, list_count_key.as_slice())?;
                    if idx < (end - start) {
                        let list_content_key = this.make_list_content_key(start + idx + 1);
                        if let Some(v) = tx.get(list_content_key)? {
                            Ok(Some(v))
                        } else {
                            Ok(None)
                        }
                    } else {
                        Ok(None)
                    }
                })
            }
        }?;
        Ok(res)
    }

    #[inline]
    fn _len(&self) -> Result<usize> {
        let this = self;
        let res = {
            if this
                .db
                ._is_expired(this.name.as_slice(), EXPIRE_AT_KEY_SUFFIX_LIST, |k| {
                    SledStorageDB::_list_contains_key(this.tree(), k)
                })?
            {
                Ok::<usize, anyhow::Error>(0)
            } else {
                let list_count_key = this.make_list_count_key();
                if let Some(v) = this.tree().get(list_count_key.as_slice())? {
                    let (start, end) = bincode::deserialize::<(usize, usize)>(v.as_ref())?;
                    Ok(end - start)
                } else {
                    Ok(0)
                }
            }
        }?;
        Ok(res)
    }

    #[inline]
    fn _is_empty(&self) -> Result<bool> {
        let this = self;
        let res = {
            if this
                .db
                ._is_expired(this.name.as_slice(), EXPIRE_AT_KEY_SUFFIX_LIST, |k| {
                    SledStorageDB::_list_contains_key(this.tree(), k)
                })?
            {
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

    #[cfg(feature = "ttl")]
    #[inline]
    fn _expire_at(&self, at: TimestampMillis) -> Result<bool> {
        let this = self;
        let expire_key =
            SledStorageDB::_make_expire_key(self.name.as_slice(), EXPIRE_AT_KEY_SUFFIX_LIST);
        let res = {
            let at_bytes = at.to_be_bytes();
            this.db
                .db
                .insert(expire_key, at_bytes.as_slice())
                .map_err(|e| anyhow!(e))
                .map(|_| true)
        }?;
        Ok(res)
    }

    #[cfg(feature = "ttl")]
    #[inline]
    fn _ttl(&self) -> Result<Option<TimestampMillis>> {
        self.db._ttl(self.name(), EXPIRE_AT_KEY_SUFFIX_LIST, |k| {
            SledStorageDB::_list_contains_key(self.tree(), k)
        })
    }

    #[inline]
    fn _is_expired(&self) -> Result<bool> {
        self.db
            ._is_expired(self.name.as_slice(), EXPIRE_AT_KEY_SUFFIX_LIST, |k| {
                SledStorageDB::_list_contains_key(self.tree(), k)
            })
    }

    #[inline]
    async fn call_is_expired(&self) -> Result<bool> {
        let (tx, rx) = oneshot::channel();
        self.db
            .cmd_send(Command::ListIsExpired(self.clone(), tx))
            .await?;
        rx.await?
    }

    #[inline]
    fn _prefix_iter(&self) -> sled::Iter {
        let list_content_prefix = Self::make_list_content_prefix(self.prefix_name.as_slice(), None);
        self.tree().scan_prefix(list_content_prefix)
    }

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
    #[inline]
    fn name(&self) -> &[u8] {
        self.name.as_slice()
    }

    #[inline]
    async fn push<V>(&self, val: &V) -> Result<()>
    where
        V: serde::ser::Serialize + Sync + Send,
    {
        let val = bincode::serialize(val)?;
        let (tx, rx) = oneshot::channel();
        self.db
            .cmd_send(Command::ListPush(self.clone(), val.into(), tx))
            .await?;
        rx.await??;
        Ok(())
    }

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
                bincode::serialize(&v)
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
        let data = bincode::serialize(val)?;

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
                bincode::deserialize::<V>(removed.as_ref())
                    .map_err(|e| sled::Error::Io(io::Error::new(ErrorKind::InvalidData, e)))?,
            )
        } else {
            None
        };
        Ok(removed)
    }

    #[inline]
    async fn pop<V>(&self) -> Result<Option<V>>
    where
        V: DeserializeOwned + Sync + Send,
    {
        let (tx, rx) = oneshot::channel();
        self.db.cmd_send(Command::ListPop(self.clone(), tx)).await?;

        let removed = if let Some(removed) = rx.await?? {
            Some(
                bincode::deserialize::<V>(removed.as_ref())
                    .map_err(|e| sled::Error::Io(io::Error::new(ErrorKind::InvalidData, e)))?,
            )
        } else {
            None
        };
        Ok(removed)
    }

    #[inline]
    async fn all<V>(&self) -> Result<Vec<V>>
    where
        V: DeserializeOwned + Sync + Send,
    {
        let (tx, rx) = oneshot::channel();
        self.db.cmd_send(Command::ListAll(self.clone(), tx)).await?;

        rx.await??
            .iter()
            .map(|v| bincode::deserialize::<V>(v.as_ref()).map_err(|e| anyhow!(e)))
            .collect::<Result<Vec<_>>>()
    }

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
            Some(bincode::deserialize::<V>(res.as_ref()).map_err(|e| anyhow!(e))?)
        } else {
            None
        })
    }

    #[inline]
    async fn len(&self) -> Result<usize> {
        let (tx, rx) = oneshot::channel();
        self.db.cmd_send(Command::ListLen(self.clone(), tx)).await?;
        Ok(rx.await??)
    }

    #[inline]
    async fn is_empty(&self) -> Result<bool> {
        let (tx, rx) = oneshot::channel();
        self.db
            .cmd_send(Command::ListIsEmpty(self.clone(), tx))
            .await?;
        Ok(rx.await??)
    }

    #[inline]
    async fn clear(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.db
            .cmd_send(Command::ListClear(self.clone(), tx))
            .await?;
        Ok(rx.await??)
    }

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

    #[cfg(feature = "ttl")]
    async fn expire_at(&self, at: TimestampMillis) -> Result<bool> {
        let (tx, rx) = oneshot::channel();
        self.db
            .cmd_send(Command::ListExpireAt(self.clone(), at, tx))
            .await?;
        Ok(rx.await??)
    }

    #[cfg(feature = "ttl")]
    async fn expire(&self, dur: TimestampMillis) -> Result<bool> {
        let at = timestamp_millis() + dur;
        self.expire_at(at).await
    }

    #[cfg(feature = "ttl")]
    async fn ttl(&self) -> Result<Option<TimestampMillis>> {
        let (tx, rx) = oneshot::channel();
        self.db.cmd_send(Command::ListTTL(self.clone(), tx)).await?;
        Ok(rx.await??)
    }
}

pub struct AsyncIter<'a, V> {
    db: &'a SledStorageDB,
    prefix_len: usize,
    iter: Option<sled::Iter>,
    _m: std::marker::PhantomData<V>,
}

impl<'a, V> Debug for AsyncIter<'a, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("AsyncIter .. ").finish()
    }
}

#[async_trait]
impl<'a, V> AsyncIterator for AsyncIter<'a, V>
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
                match bincode::deserialize::<V>(v.as_ref()) {
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

pub struct AsyncKeyIter<'a> {
    db: &'a SledStorageDB,
    prefix_len: usize,
    iter: Option<sled::Iter>,
}

impl<'a> Debug for AsyncKeyIter<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("AsyncKeyIter .. ").finish()
    }
}

#[async_trait]
impl<'a> AsyncIterator for AsyncKeyIter<'a> {
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

pub struct AsyncListValIter<'a, V> {
    db: &'a SledStorageDB,
    iter: Option<sled::Iter>,
    _m: std::marker::PhantomData<V>,
}

impl<'a, V> Debug for AsyncListValIter<'a, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("AsyncListValIter .. ").finish()
    }
}

#[async_trait]
impl<'a, V> AsyncIterator for AsyncListValIter<'a, V>
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
                Some(bincode::deserialize::<V>(v.as_ref()).map_err(|e| anyhow!(e)))
            }
        }
    }
}

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

impl<'a> Debug for AsyncMapIter<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("AsyncMapIter .. ").finish()
    }
}

#[async_trait]
impl<'a> AsyncIterator for AsyncMapIter<'a> {
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

pub struct AsyncListIter<'a> {
    db: &'a SledStorageDB,
    iter: Option<sled::Iter>,
}

impl<'a> Debug for AsyncListIter<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("AsyncListIter .. ").finish()
    }
}

#[async_trait]
impl<'a> AsyncIterator for AsyncListIter<'a> {
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
