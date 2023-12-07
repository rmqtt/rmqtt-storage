use std::future::Future;
use std::io::ErrorKind;
use std::sync::Arc;
use std::time::Duration;
use std::{io, thread};

use anyhow::anyhow;
use async_trait::async_trait;
use chrono::Timelike;
use convert::Bytesize;
use parking_lot::RwLock;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::task::spawn_blocking;

use sled::transaction::{
    ConflictableTransactionError, ConflictableTransactionResult, TransactionError,
    TransactionResult, TransactionalTree,
};
use sled::Batch;

use crate::storage::{AsyncIterator, IterItem, Key, List, Map, StorageDB};
use crate::{timestamp_millis, Error, Result, StorageList, StorageMap, TimestampMillis};

const SEPARATOR: &[u8] = b"@";
const DEF_TREE: &[u8] = b"__default_tree@";
const MAP_NAME_PREFIX: &[u8] = b"__map@";
const MAP_KEY_SEPARATOR: &[u8] = b"@__item@";
const MAP_KEY_COUNT_SUFFIX: &[u8] = b"@__count@";
const LIST_NAME_PREFIX: &[u8] = b"__list@";
const LIST_KEY_COUNT_SUFFIX: &[u8] = b"@__count@";
const LIST_KEY_CONTENT_SUFFIX: &[u8] = b"@__content@";
const EXPIRE_AT_KEY_PREFIX: &[u8] = b"__expireat@";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SledConfig {
    pub path: String,
    pub cache_capacity: Bytesize,
    pub gc_at_hour: u32,
    pub gc_at_minute: u32,
}

impl Default for SledConfig {
    fn default() -> Self {
        SledConfig {
            path: String::default(),
            cache_capacity: Bytesize::from(1024 * 1024 * 1024),
            gc_at_hour: 2,
            gc_at_minute: 30,
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
    db: Arc<sled::Db>,
    def_tree: sled::Tree,
}

impl SledStorageDB {
    #[inline]
    pub(crate) async fn new(cfg: SledConfig) -> Result<Self> {
        let sled_cfg = cfg.to_sled_config()?;
        let (db, def_tree) = spawn_blocking(move || {
            sled_cfg.open().map(|db| {
                let def_tree = db.open_tree(DEF_TREE);
                (Arc::new(db), def_tree)
            })
        })
        .await??;
        let def_tree = def_tree?;
        let db = Self { db, def_tree };
        let sled_db = db.clone();

        std::thread::spawn(move || {
            let db = sled_db.db.clone();
            if let Err(e) = Self::run_scheduler_task(cfg.gc_at_hour, cfg.gc_at_minute, move || {
                let now = std::time::Instant::now();
                log::info!("Start Cleanup Operation ... ");
                for item in db.scan_prefix(EXPIRE_AT_KEY_PREFIX) {
                    match item {
                        Ok((expire_key, v)) => {
                            if let Some(key) = Self::separate_expire_key(expire_key.as_ref()) {
                                match sled_db._ttl3(key.as_slice(), v.as_ref()) {
                                    Ok(None) => {
                                        if let Err(e) = sled_db._remove(key) {
                                            log::warn!("{:?}", e);
                                        }
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
                log::info!("Completed cleanup operation in {:?}", now.elapsed());
            }) {
                log::warn!("{:?}", e);
            }
        });

        Ok(db)
    }

    fn run_scheduler_task<F>(hour: u32, minute: u32, mut task: F) -> Result<()>
    where
        F: FnMut(),
    {
        let one_day = Duration::from_secs(24 * 60 * 60);

        // Get the current time.
        let current_timestamp = chrono::Local::now().timestamp(); //current_time.as_secs();
        let next_execution_time = chrono::Local::now()
            .with_hour(hour)
            .and_then(|t| t.with_minute(minute))
            .map(|t| t.timestamp())
            .unwrap_or(current_timestamp);
        let duration_until_next_execution = if next_execution_time > current_timestamp {
            Duration::from_secs((next_execution_time - current_timestamp) as u64)
        } else {
            Duration::from_secs(
                one_day.as_secs() - (current_timestamp - next_execution_time) as u64,
            )
        };
        log::info!(
            "duration_until_next_execution: {:?}",
            duration_until_next_execution
        );
        thread::sleep(duration_until_next_execution);
        loop {
            task();
            thread::sleep(one_day);
        }
    }

    #[inline]
    fn make_expire_key<K>(key: K) -> Key
    where
        K: AsRef<[u8]>,
    {
        [EXPIRE_AT_KEY_PREFIX, key.as_ref(), SEPARATOR].concat()
    }

    #[inline]
    fn separate_expire_key<K>(expire_key: K) -> Option<Key>
    where
        K: AsRef<[u8]>,
    {
        let expire_key = expire_key.as_ref();
        if expire_key.len() > (EXPIRE_AT_KEY_PREFIX.len() + SEPARATOR.len()) {
            let key = expire_key[EXPIRE_AT_KEY_PREFIX.len()..(expire_key.len() - SEPARATOR.len())]
                .to_vec();
            Some(key)
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
        [
            MAP_NAME_PREFIX,
            name.as_ref(),
            //SEPARATOR,
            MAP_KEY_COUNT_SUFFIX,
        ]
        .concat()
    }

    #[inline]
    fn map_count_key_to_name(key: &[u8]) -> &[u8] {
        key[MAP_NAME_PREFIX.len()..key.as_ref().len() - MAP_KEY_COUNT_SUFFIX.len()].as_ref()
    }

    #[inline]
    fn is_map_count_key(key: &[u8]) -> bool {
        key.starts_with(MAP_NAME_PREFIX) && key.ends_with(MAP_KEY_COUNT_SUFFIX)
    }

    #[inline]
    fn make_list_prefix<K>(name: K) -> Key
    where
        K: AsRef<[u8]>,
    {
        [LIST_NAME_PREFIX, name.as_ref()].concat()
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
        if self.db.contains_key(key.as_ref())? {
            Ok(true)
        } else {
            let m = self.map(key.as_ref());
            if !m._is_empty() {
                Ok(true)
            } else {
                let l = self.list(key.as_ref());
                Ok(!l._is_empty()?)
            }
        }
    }

    #[inline]
    fn _remove<K>(&self, key: K) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        self.db.remove(key.as_ref())?;
        self.map(key.as_ref())._clear()?;
        self.list(key.as_ref())._clear()?;
        Self::_remove_expire_key(self.db.as_ref(), key.as_ref())
            .map_err(|e| anyhow!(format!("{:?}", e)))?;
        Ok(())
    }

    #[inline]
    fn _remove_expire_key<K>(db: &sled::Db, key: K) -> TransactionResult<()>
    where
        K: AsRef<[u8]>,
    {
        let expire_key = Self::make_expire_key(key);
        db.remove(expire_key)?;
        Ok(())
    }

    #[inline]
    fn _tx_remove_expire_key<K>(tx: &TransactionalTree, key: K) -> ConflictableTransactionResult<()>
    where
        K: AsRef<[u8]>,
    {
        let expire_key = Self::make_expire_key(key);
        tx.remove(expire_key)?;
        Ok(())
    }

    #[inline]
    fn _is_expired<K>(&self, key: K) -> Result<bool>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let expire_key = Self::make_expire_key(key.as_ref());
        let res = self
            ._ttl2(key.as_ref(), expire_key.as_slice())?
            .and_then(|ttl| if ttl > 0 { Some(()) } else { None });
        Ok(res.is_none())
    }

    #[inline]
    fn _ttl<K>(&self, key: K) -> Result<Option<TimestampMillis>>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let expire_key = Self::make_expire_key(key.as_ref());
        self._ttl2(key.as_ref(), expire_key.as_slice())
    }

    #[inline]
    fn _ttl2<K>(&self, c_key: K, expire_key: K) -> Result<Option<TimestampMillis>>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let ttl_res = match self.db.get(expire_key) {
            Ok(Some(v)) => {
                if self._contains_key(c_key)? {
                    Ok(Some(TimestampMillis::from_be_bytes(v.as_ref().try_into()?)))
                } else {
                    Ok(None)
                }
            }
            Ok(None) => {
                if self._contains_key(c_key)? {
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
    fn _ttl3<K>(&self, c_key: K, ttl_val: &[u8]) -> Result<Option<TimestampMillis>>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let ttl_at = if self._contains_key(c_key)? {
            Some(TimestampMillis::from_be_bytes(ttl_val.as_ref().try_into()?))
        } else {
            None
        };

        let ttl_millis = if let Some(at) = ttl_at {
            let now = timestamp_millis();
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
}

#[async_trait]
impl StorageDB for SledStorageDB {
    type MapType = SledStorageMap;
    type ListType = SledStorageList;

    #[inline]
    fn map<K: AsRef<[u8]>>(&self, name: K) -> Self::MapType {
        let map_prefix_name = SledStorageDB::make_map_prefix_name(name.as_ref());
        let map_item_prefix_name = SledStorageDB::make_map_item_prefix_name(name.as_ref());
        let map_count_key_name = SledStorageDB::make_map_count_key_name(name.as_ref());
        SledStorageMap {
            name: name.as_ref().to_vec(),
            map_prefix_name,
            map_item_prefix_name,
            map_count_key_name,
            db: self.clone(),
        }
    }

    #[inline]
    fn list<V: AsRef<[u8]>>(&self, name: V) -> Self::ListType {
        SledStorageList {
            name: name.as_ref().to_vec(),
            prefix_name: SledStorageDB::make_list_prefix(name),
            db: self.clone(),
        }
    }

    #[inline]
    async fn insert<K, V>(&self, key: K, val: &V) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: serde::ser::Serialize + Sync + Send,
    {
        let db = self.db.clone();
        let key = key.as_ref().to_vec();
        let val = bincode::serialize(val)?;
        spawn_blocking(move || {
            // db.insert(key.as_slice(), val.as_slice())?;
            // Self::_remove_expire_key(&db, key.as_slice())?;
            // Ok::<(), TransactionError<()>>(())
            db.transaction(move |tx| {
                tx.insert(key.as_slice(), val.as_slice())?;
                Self::_tx_remove_expire_key(tx, key.as_slice())?;
                Ok(())
            })
        })
        .await?
        .map_err(|e| anyhow!(format!("{:?}", e)))?;
        Ok(())
    }

    #[inline]
    async fn get<K, V>(&self, key: K) -> Result<Option<V>>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: DeserializeOwned + Sync + Send,
    {
        let this = self.clone();
        let key = key.as_ref().to_vec();
        match spawn_blocking(move || {
            let res: Result<_> = if this._is_expired(key.as_slice())? {
                Ok(None)
            } else {
                Ok(this.db.get(key)?)
            };
            res
        })
        .await??
        {
            Some(v) => Ok(Some(bincode::deserialize::<V>(v.as_ref())?)),
            None => Ok(None),
        }
    }

    #[inline]
    async fn remove<K>(&self, key: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let this = self.clone();
        let key = key.as_ref().to_vec();
        spawn_blocking(move || this._remove(key)).await??;
        Ok(())
    }

    #[inline]
    async fn counter_incr<K>(&self, key: K, increment: isize) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let this = self.clone();
        let key = key.as_ref().to_vec();
        spawn_blocking(move || {
            this.db.fetch_and_update(key, |old: Option<&[u8]>| {
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
            })
        })
        .await??;
        Ok(())
    }

    #[inline]
    async fn counter_decr<K>(&self, key: K, decrement: isize) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let this = self.clone();
        let key = key.as_ref().to_vec();
        spawn_blocking(move || {
            this.db.fetch_and_update(key, |old: Option<&[u8]>| {
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
            })
        })
        .await??;
        Ok(())
    }

    #[inline]
    async fn counter_get<K>(&self, key: K) -> Result<Option<isize>>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let this = self.clone();
        let key = key.as_ref().to_vec();
        spawn_blocking(move || {
            let res: Result<_> = if this._is_expired(key.as_slice())? {
                Ok(None)
            } else if let Some(v) = this.db.get(key)? {
                Ok(Some(isize::from_be_bytes(v.as_ref().try_into()?)))
            } else {
                Ok(None)
            };
            res
        })
        .await?
    }

    #[inline]
    async fn counter_set<K>(&self, key: K, val: isize) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let db = self.db.clone();
        let key = key.as_ref().to_vec();
        let val = val.to_be_bytes().to_vec();
        spawn_blocking(move || {
            // db.insert(key.as_slice(), val.as_slice())?;
            // Self::_remove_expire_key(&db, key.as_slice())?;
            // Ok::<(), TransactionError<()>>(())
            db.transaction(move |tx| {
                tx.insert(key.as_slice(), val.as_slice())?;
                Self::_tx_remove_expire_key(tx, key.as_slice())?;
                Ok(())
            })
        })
        .await?
        .map_err(|e| anyhow!(format!("{:?}", e)))?;
        Ok(())
    }

    #[inline]
    async fn contains_key<K: AsRef<[u8]> + Sync + Send>(&self, key: K) -> Result<bool> {
        let this = self.clone();
        let key = key.as_ref().to_vec();
        Ok(spawn_blocking(move || {
            if this._is_expired(key.as_slice())? {
                Ok(false)
            } else {
                this._contains_key(key)
            }
        })
        .await??)
    }

    #[inline]
    async fn expire_at<K>(&self, key: K, at: TimestampMillis) -> Result<bool>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let db = self.clone();
        let expire_key = Self::make_expire_key(key.as_ref());
        let key = key.as_ref().to_vec();
        let res = spawn_blocking(move || {
            if db._contains_key(key)? {
                let at_bytes = at.to_be_bytes();
                db.db
                    .insert(expire_key, at_bytes.as_slice())
                    .map_err(|e| anyhow!(e))
                    .map(|_| true)
            } else {
                Ok(false)
            }
        })
        .await??;
        Ok(res)
    }

    #[inline]
    async fn expire<K>(&self, key: K, dur: TimestampMillis) -> Result<bool>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let at = timestamp_millis() + dur;
        self.expire_at(key, at).await
    }

    #[inline]
    async fn ttl<K>(&self, key: K) -> Result<Option<TimestampMillis>>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let key = key.as_ref().to_vec();
        let this = self.clone();
        Ok(spawn_blocking(move || this._ttl(key)).await??)
    }

    #[inline]
    async fn map_iter<'a>(
        &'a mut self,
    ) -> Result<Box<dyn AsyncIterator<Item = Result<StorageMap>> + Send + 'a>> {
        let this = self.clone();
        let iter = spawn_blocking(move || {
            let iter = Arc::new(RwLock::new(this.def_tree.scan_prefix(MAP_NAME_PREFIX)));
            Box::new(AsyncMapIter { db: this, iter })
        })
        .await?;
        Ok(iter)
    }

    #[inline]
    async fn list_iter<'a>(
        &'a mut self,
    ) -> Result<Box<dyn AsyncIterator<Item = Result<StorageList>> + Send + 'a>> {
        let this = self.clone();
        let iter = spawn_blocking(move || {
            let iter = Arc::new(RwLock::new(this.def_tree.scan_prefix(LIST_NAME_PREFIX)));
            Box::new(AsyncListIter { db: this, iter })
        })
        .await?;
        Ok(iter)
    }
}

#[derive(Clone)]
pub struct SledStorageMap {
    name: Key,
    map_prefix_name: Key,
    map_item_prefix_name: Key,
    map_count_key_name: Key,
    pub(crate) db: SledStorageDB,
}

impl SledStorageMap {
    #[inline]
    fn tree(&self) -> &sled::Tree {
        &self.db.def_tree
    }

    #[inline]
    fn make_map_item_key<K: AsRef<[u8]>>(&self, key: K) -> Key {
        [self.map_item_prefix_name.as_ref(), key.as_ref()].concat()
    }

    #[inline]
    fn map_item_key_to_name(map_prefix_len: usize, key: &[u8]) -> &[u8] {
        key[map_prefix_len..].as_ref()
    }

    #[inline]
    fn _len_get(&self) -> Result<isize> {
        self._counter_get(self.map_count_key_name.as_slice())
    }

    #[inline]
    fn _tx_len_add<K: AsRef<[u8]>>(
        tx: &TransactionalTree,
        count_name: K,
        v: isize,
    ) -> ConflictableTransactionResult<()> {
        let c = Self::_tx_counter_get(tx, count_name.as_ref())?;
        Self::_tx_counter_set(tx, count_name.as_ref(), c - v)?;
        Ok(())
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
    async fn _retain_with_key<'a, F, Out>(&'a self, f: F) -> Result<()>
    where
        F: Fn(Result<Key>) -> Out + Send + Sync,
        Out: Future<Output = bool> + Send + 'a,
    {
        let mut batch = Batch::default();
        let mut count = 0;
        for key in self
            .tree()
            .scan_prefix(self.map_item_prefix_name.as_slice())
            .keys()
        {
            match key {
                Ok(k) => {
                    let name = SledStorageMap::map_item_key_to_name(
                        self.map_item_prefix_name.len(),
                        k.as_ref(),
                    );
                    if !f(Ok(name.to_vec())).await {
                        batch.remove(k);
                        count += 1;
                    }
                }
                Err(e) => {
                    log::warn!("{:?}", e);
                }
            }
        }

        if count > 0 {
            let count_key = self.map_count_key_name.as_slice();
            let res: TransactionResult<()> = self.tree().transaction(move |tx| {
                tx.apply_batch(&batch)?;
                Self::_tx_len_add(tx, count_key, count)?;
                Ok(())
            });
            res.map_err(|e| anyhow!(format!("{:?}", e)))?;
        }
        Ok(())
    }

    #[inline]
    async fn _retain<'a, F, Out, V>(&'a self, f: F) -> Result<()>
    where
        F: Fn(Result<(Key, V)>) -> Out + Send + Sync,
        Out: Future<Output = bool> + Send + 'a,
        V: DeserializeOwned + Sync + Send + 'a,
    {
        let mut batch = Batch::default();
        let mut count = 0;
        for item in self
            .tree()
            .scan_prefix(self.map_item_prefix_name.as_slice())
        {
            let (k, v) = item?;
            match bincode::deserialize::<V>(v.as_ref()) {
                Ok(v) => {
                    let name = SledStorageMap::map_item_key_to_name(
                        self.map_item_prefix_name.len(),
                        k.as_ref(),
                    );
                    if !f(Ok((name.to_vec(), v))).await {
                        batch.remove(k.as_ref());
                        count += 1;
                    }
                }
                Err(e) => {
                    if !f(Err(anyhow::Error::new(e))).await {
                        batch.remove(k.as_ref());
                        count += 1;
                    }
                }
            }
        }

        if count > 0 {
            let count_key = self.map_count_key_name.as_slice();
            let res: TransactionResult<()> = self.tree().transaction(move |tx| {
                tx.apply_batch(&batch)?;
                Self::_tx_len_add(tx, count_key, count)?;
                Ok(())
            });
            res.map_err(|e| anyhow!(format!("{:?}", e)))?;
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
        Ok(())
    }

    #[inline]
    fn _is_empty(&self) -> bool {
        self.tree()
            .scan_prefix(self.map_item_prefix_name.as_slice())
            .next()
            .is_none()
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
        let tree = self.tree().clone();
        let val = bincode::serialize(val)?;
        let item_key = self.make_map_item_key(key.as_ref());
        let count_key = self.map_count_key_name.clone();

        let this = self.clone();
        spawn_blocking(move || {
            tree.transaction(move |tx| {
                if tx.insert(item_key.as_slice(), val.as_slice())?.is_none() {
                    Self::_tx_counter_inc(tx, count_key.as_slice())?;
                }
                Ok(())
            })?;
            if this.db._is_expired(this.name.as_slice()).map_err(|e| {
                TransactionError::Storage(sled::Error::Io(io::Error::new(
                    ErrorKind::InvalidData,
                    e,
                )))
            })? {
                SledStorageDB::_remove_expire_key(this.db.db.as_ref(), this.name.as_slice())?;
            }
            Ok::<(), TransactionError<()>>(())
        })
        .await?
        .map_err(|e| anyhow!(format!("{:?}", e)))?;
        Ok(())
    }

    #[inline]
    async fn get<K, V>(&self, key: K) -> Result<Option<V>>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: DeserializeOwned + Sync + Send,
    {
        let this = self.clone();
        let item_key = self.make_map_item_key(key.as_ref());
        match spawn_blocking(move || {
            if !this.db._is_expired(this.name.as_slice())? {
                this.tree().get(item_key).map_err(|e| anyhow!(e))
            } else {
                Ok(None)
            }
        })
        .await??
        {
            Some(v) => Ok(Some(bincode::deserialize::<V>(v.as_ref())?)),
            None => Ok(None),
        }
    }

    #[inline]
    async fn remove<K>(&self, key: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let tree = self.tree().clone();
        let key = self.make_map_item_key(key.as_ref());
        let count_key = self.map_count_key_name.to_vec();
        spawn_blocking(move || {
            tree.transaction(move |tx| {
                if tx.remove(key.as_slice())?.is_some() {
                    Self::_tx_counter_dec(tx, count_key.as_slice())?;
                }
                Ok(())
            })
        })
        .await?
        .map_err(|e| anyhow!(format!("{:?}", e)))?;
        Ok(())
    }

    #[inline]
    async fn contains_key<K: AsRef<[u8]> + Sync + Send>(&self, key: K) -> Result<bool> {
        let tree = self.tree().clone();
        let key = self.make_map_item_key(key.as_ref());
        Ok(spawn_blocking(move || tree.contains_key(key)).await??)
    }

    #[inline]
    async fn len(&self) -> Result<usize> {
        let this = self.clone();
        let len = spawn_blocking(move || {
            if this.db._is_expired(this.name.as_slice())? {
                Ok(0)
            } else {
                this._len_get()
            }
        })
        .await??;
        Ok(len as usize)
    }

    #[inline]
    async fn is_empty(&self) -> Result<bool> {
        let this = self.clone();
        spawn_blocking(move || {
            if this.db._is_expired(this.name.as_slice())? {
                Ok(true)
            } else {
                Ok(this._is_empty())
            }
        })
        .await?
    }

    #[inline]
    async fn clear(&self) -> Result<()> {
        let this = self.clone();
        spawn_blocking(move || this._clear()).await??;
        Ok(())
    }

    #[inline]
    async fn remove_and_fetch<K, V>(&self, key: K) -> Result<Option<V>>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: DeserializeOwned + Sync + Send,
    {
        let key = self.make_map_item_key(key.as_ref());
        let this = self.clone();
        let removed = spawn_blocking(move || {
            if this.db._is_expired(this.name.as_slice()).map_err(|e| {
                TransactionError::Storage(sled::Error::Io(io::Error::new(
                    ErrorKind::InvalidData,
                    e,
                )))
            })? {
                Ok(None)
            } else {
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
        })
        .await?
        .map_err(|e| anyhow!(format!("{:?}", e)))?;

        if let Some(removed) = removed {
            Ok(Some(bincode::deserialize::<V>(removed.as_ref())?))
        } else {
            Ok(None)
        }
    }

    #[inline]
    async fn remove_with_prefix<K>(&self, prefix: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let tree = self.tree().clone();
        let prefix = [self.map_item_prefix_name.as_slice(), prefix.as_ref()]
            .concat()
            .to_vec();

        let map_count_key_name = self.map_count_key_name.to_vec();
        spawn_blocking(move || {
            let mut removeds = Batch::default();
            let mut c = 0;
            for item in tree.scan_prefix(prefix) {
                match item {
                    Ok((k, _v)) => {
                        removeds.remove(k.as_ref());
                        c += 1;
                    }
                    Err(e) => {
                        log::warn!("{:?}", e);
                    }
                }
            }

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
        })
        .await??;
        Ok(())
    }

    #[inline]
    async fn batch_insert<V>(&self, key_vals: Vec<(Key, V)>) -> Result<()>
    where
        V: serde::ser::Serialize + Sync + Send,
    {
        for (k, v) in key_vals {
            self.insert(k, &v).await?;
        }
        Ok(())
    }

    #[inline]
    async fn batch_remove(&self, keys: Vec<Key>) -> Result<()> {
        for k in keys {
            self.remove(k).await?;
        }
        Ok(())
    }

    #[inline]
    async fn iter<'a, V>(
        &'a mut self,
    ) -> Result<Box<dyn AsyncIterator<Item = IterItem<V>> + Send + 'a>>
    where
        V: DeserializeOwned + Sync + Send + 'a + 'static,
    {
        let this = self.clone();
        let res = spawn_blocking(move || {
            if this.db._is_expired(this.name.as_slice())? {
                let iter: Box<dyn AsyncIterator<Item = IterItem<V>> + Send> =
                    Box::new(AsyncEmptyIter {
                        _m: std::marker::PhantomData,
                    });
                Ok::<_, anyhow::Error>(iter)
            } else {
                let tem_prefix_name = this.map_item_prefix_name.len();
                let iter = Arc::new(RwLock::new(
                    this.tree()
                        .scan_prefix(this.map_item_prefix_name.as_slice()),
                ));
                let iter: Box<dyn AsyncIterator<Item = IterItem<V>> + Send> = Box::new(AsyncIter {
                    prefix_len: tem_prefix_name,
                    iter,
                    _m: std::marker::PhantomData,
                });
                Ok::<_, anyhow::Error>(iter)
            }
        })
        .await??;
        Ok(res)
    }

    #[inline]
    async fn key_iter<'a>(
        &'a mut self,
    ) -> Result<Box<dyn AsyncIterator<Item = Result<Key>> + Send + 'a>> {
        let this = self.clone();
        let res = spawn_blocking(move || {
            if this.db._is_expired(this.name.as_slice())? {
                let iter: Box<dyn AsyncIterator<Item = Result<Key>> + Send> =
                    Box::new(AsyncEmptyIter {
                        _m: std::marker::PhantomData,
                    });
                Ok::<_, anyhow::Error>(iter)
            } else {
                let iter = Arc::new(RwLock::new(
                    this.tree()
                        .scan_prefix(this.map_item_prefix_name.as_slice()),
                ));
                let iter: Box<dyn AsyncIterator<Item = Result<Key>> + Send> =
                    Box::new(AsyncKeyIter {
                        prefix_len: this.map_item_prefix_name.len(),
                        iter,
                    });
                Ok::<_, anyhow::Error>(iter)
            }
        })
        .await??;
        Ok(res)
    }

    #[inline]
    async fn prefix_iter<'a, P, V>(
        &'a mut self,
        prefix: P,
    ) -> Result<Box<dyn AsyncIterator<Item = IterItem<V>> + Send + 'a>>
    where
        P: AsRef<[u8]> + Send,
        V: DeserializeOwned + Sync + Send + 'a + 'static,
    {
        let this = self.clone();
        let prefix = prefix.as_ref().to_vec();
        let res = spawn_blocking(move || {
            if this.db._is_expired(this.name.as_slice())? {
                let iter: Box<dyn AsyncIterator<Item = IterItem<V>> + Send> =
                    Box::new(AsyncEmptyIter {
                        _m: std::marker::PhantomData,
                    });
                Ok::<_, anyhow::Error>(iter)
            } else {
                let iter = Arc::new(RwLock::new(this.tree().scan_prefix(
                    [this.map_item_prefix_name.as_slice(), prefix.as_ref()].concat(),
                )));
                let iter: Box<dyn AsyncIterator<Item = IterItem<V>> + Send> = Box::new(AsyncIter {
                    prefix_len: this.map_item_prefix_name.len(),
                    iter,
                    _m: std::marker::PhantomData,
                });
                Ok::<_, anyhow::Error>(iter)
            }
        })
        .await??;
        Ok(res)
    }

    #[inline]
    async fn retain<'a, F, Out, V>(&'a self, f: F) -> Result<()>
    where
        F: Fn(Result<(Key, V)>) -> Out + Send + Sync + 'static,
        Out: Future<Output = bool> + Send + 'a,
        V: DeserializeOwned + Sync + Send + 'a,
    {
        let this = self.clone();
        spawn_blocking(move || {
            tokio::runtime::Handle::current().block_on(async move { this._retain(f).await })
        })
        .await??;
        Ok(())
    }

    #[inline]
    async fn retain_with_key<'a, F, Out>(&'a self, f: F) -> Result<()>
    where
        F: Fn(Result<Key>) -> Out + Send + Sync + 'static,
        Out: Future<Output = bool> + Send + 'a,
    {
        let this = self.clone();
        spawn_blocking(move || {
            tokio::runtime::Handle::current()
                .block_on(async move { this._retain_with_key(f).await })
        })
        .await??;
        Ok(())
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
    pub(crate) fn name(&self) -> &[u8] {
        self.name.as_slice()
    }

    #[inline]
    pub(crate) fn tree(&self) -> &sled::Tree {
        &self.db.def_tree
    }

    #[inline]
    fn make_list_count_key(&self) -> Vec<u8> {
        let list_count_key = [self.prefix_name.as_ref(), LIST_KEY_COUNT_SUFFIX].concat();
        list_count_key
    }

    #[inline]
    fn make_list_content_prefix(&self, idx: Option<&[u8]>) -> Vec<u8> {
        if let Some(idx) = idx {
            [self.prefix_name.as_ref(), LIST_KEY_CONTENT_SUFFIX, idx].concat()
        } else {
            [self.prefix_name.as_ref(), LIST_KEY_CONTENT_SUFFIX].concat()
        }
    }

    #[inline]
    fn make_list_content_key(&self, idx: usize) -> Vec<u8> {
        self.make_list_content_prefix(Some(idx.to_be_bytes().as_slice()))
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
    fn _clear(&self) -> Result<()> {
        let mut batch = Batch::default();
        let list_count_key = self.make_list_count_key();
        batch.remove(list_count_key);
        let list_content_prefix = self.make_list_content_prefix(None);
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
    fn _is_empty(&self) -> Result<bool> {
        let list_content_prefix = self.make_list_content_prefix(None);
        Ok(self
            .tree()
            .scan_prefix(list_content_prefix)
            .keys()
            .next()
            .is_none())
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
        let data = bincode::serialize(val)?;
        let tree = self.tree().clone();
        let this = self.clone();
        spawn_blocking(move || {
            let this1 = this.clone();
            tree.transaction(move |tx| {
                let list_count_key = this.make_list_count_key();
                let (start, mut end) = Self::tx_list_count_get(tx, list_count_key.as_slice())?;
                end += 1;
                Self::tx_list_count_set(tx, list_count_key.as_slice(), start, end)?;

                let list_content_key = this.make_list_content_key(end);
                Self::tx_list_content_set(tx, list_content_key.as_slice(), &data)?;
                Ok(())
            })?;

            if this1.db._is_expired(this1.name.as_slice()).map_err(|e| {
                TransactionError::Storage(sled::Error::Io(io::Error::new(
                    ErrorKind::InvalidData,
                    e,
                )))
            })? {
                SledStorageDB::_remove_expire_key(this1.db.db.as_ref(), this1.name.as_slice())?;
            }
            Ok::<(), TransactionError<()>>(())
        })
        .await?
        .map_err(|e| anyhow!(format!("{:?}", e)))?;

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
        let tree = self.tree().clone();
        let this = self.clone();

        let removed = spawn_blocking(move || {
            let this1 = this.clone();
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
                    Self::tx_list_content_set(tx, list_content_key.as_slice(), &data)?;
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
                    Self::tx_list_content_set(tx, list_content_key.as_slice(), &data)?;
                    Ok(removed)
                } else {
                    Err(ConflictableTransactionError::Storage(sled::Error::Io(
                        io::Error::new(ErrorKind::InvalidData, "Is full"),
                    )))
                }
            });

            if this1.db._is_expired(this1.name.as_slice()).map_err(|e| {
                TransactionError::Storage(sled::Error::Io(io::Error::new(
                    ErrorKind::InvalidData,
                    e,
                )))
            })? {
                SledStorageDB::_remove_expire_key(this1.db.db.as_ref(), this1.name.as_slice())?;
            }

            Ok::<_, TransactionError<()>>(res)
        })
        .await?
        .map_err(|e| anyhow!(format!("{:?}", e)))??;

        let removed = if let Some(removed) = removed {
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
        let this = self.clone();
        let removed = spawn_blocking(move || {
            if this.db._is_expired(this.name.as_slice()).map_err(|e| {
                TransactionError::Storage(sled::Error::Io(io::Error::new(
                    ErrorKind::InvalidData,
                    e,
                )))
            })? {
                Ok(None)
            } else {
                let removed = this.tree().clone().transaction(move |tx| {
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
        })
        .await??;

        let removed = if let Some(v) = removed {
            Some(bincode::deserialize::<V>(v.as_ref()).map_err(|e| anyhow!(e))?)
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
        let this = self.clone();
        let res = spawn_blocking(move || {
            if this.db._is_expired(this.name.as_slice())? {
                Ok(vec![])
            } else {
                let key_content_prefix = this.make_list_content_prefix(None);
                this.tree()
                    .scan_prefix(key_content_prefix)
                    .values()
                    .map(|item| item.map_err(anyhow::Error::new))
                    .collect::<Result<Vec<_>>>()
            }
        })
        .await??;

        res.iter()
            .map(|v| bincode::deserialize::<V>(v.as_ref()).map_err(|e| anyhow!(e)))
            .collect::<Result<Vec<_>>>()
    }

    #[inline]
    async fn get_index<V>(&self, idx: usize) -> Result<Option<V>>
    where
        V: DeserializeOwned + Sync + Send,
    {
        let tree = self.tree().clone();
        let this = self.clone();
        let res = spawn_blocking(move || {
            if this.db._is_expired(this.name.as_slice()).map_err(|e| {
                TransactionError::Storage(sled::Error::Io(io::Error::new(
                    ErrorKind::InvalidData,
                    e,
                )))
            })? {
                Ok(None)
            } else {
                tree.transaction(move |tx| {
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
        })
        .await??;

        Ok(if let Some(res) = res {
            Some(bincode::deserialize::<V>(res.as_ref()).map_err(|e| anyhow!(e))?)
        } else {
            None
        })
    }

    #[inline]
    async fn len(&self) -> Result<usize> {
        let this = self.clone();
        spawn_blocking(move || {
            if this.db._is_expired(this.name.as_slice())? {
                Ok(0)
            } else {
                let list_count_key = this.make_list_count_key();
                if let Some(v) = this.tree().get(list_count_key.as_slice())? {
                    let (start, end) = bincode::deserialize::<(usize, usize)>(v.as_ref())?;
                    Ok(end - start)
                } else {
                    Ok(0)
                }
            }
        })
        .await?
    }

    #[inline]
    async fn is_empty(&self) -> Result<bool> {
        let this = self.clone();
        Ok(spawn_blocking(move || {
            if this.db._is_expired(this.name.as_slice())? {
                Ok(true)
            } else {
                this._is_empty()
            }
        })
        .await??)
    }

    #[inline]
    async fn clear(&self) -> Result<()> {
        let this = self.clone();
        spawn_blocking(move || this._clear()).await??;
        Ok(())
    }

    #[inline]
    async fn iter<'a, V>(
        &'a mut self,
    ) -> Result<Box<dyn AsyncIterator<Item = Result<V>> + Send + 'a>>
    where
        V: DeserializeOwned + Sync + Send + 'a + 'static,
    {
        let this = self.clone();
        let res = spawn_blocking(move || {
            if this.db._is_expired(this.name.as_slice())? {
                let iter: Box<dyn AsyncIterator<Item = Result<V>> + Send> =
                    Box::new(AsyncEmptyIter {
                        _m: std::marker::PhantomData,
                    });
                Ok::<_, anyhow::Error>(iter)
            } else {
                let list_content_prefix = this.make_list_content_prefix(None);
                let iter = Arc::new(RwLock::new(this.tree().scan_prefix(list_content_prefix)));
                let iter: Box<dyn AsyncIterator<Item = Result<V>> + Send> =
                    Box::new(AsyncListValIter {
                        iter,
                        _m: std::marker::PhantomData,
                    });
                Ok::<_, anyhow::Error>(iter)
            }
        })
        .await??;
        Ok(res)
    }
}

pub struct AsyncIter<V> {
    prefix_len: usize,
    iter: Arc<RwLock<sled::Iter>>,
    _m: std::marker::PhantomData<V>,
}

#[async_trait]
impl<V> AsyncIterator for AsyncIter<V>
where
    V: DeserializeOwned + Sync + Send + 'static,
{
    type Item = IterItem<V>;

    async fn next(&mut self) -> Option<Self::Item> {
        let iter = self.iter.clone();
        let prefix_len = self.prefix_len;
        let item = spawn_blocking(move || match iter.write().next() {
            None => None,
            Some(Err(e)) => Some(Err(anyhow::Error::new(e))),
            Some(Ok((k, v))) => {
                let name = k.as_ref()[prefix_len..].to_vec();
                match bincode::deserialize::<V>(v.as_ref()) {
                    Ok(v) => Some(Ok((name, v))),
                    Err(e) => Some(Err(anyhow::Error::new(e))),
                }
            }
        })
        .await;
        match item {
            Err(e) => Some(Err(anyhow!(e))),
            Ok(item) => item,
        }
    }
}

pub struct AsyncKeyIter {
    prefix_len: usize,
    iter: Arc<RwLock<sled::Iter>>,
}

#[async_trait]
impl AsyncIterator for AsyncKeyIter {
    type Item = Result<Key>;

    async fn next(&mut self) -> Option<Self::Item> {
        let iter = self.iter.clone();
        let prefix_len = self.prefix_len;
        let item = spawn_blocking(move || {
            return match iter.write().next() {
                None => None,
                Some(Err(e)) => Some(Err(anyhow::Error::new(e))),
                Some(Ok((k, _))) => {
                    let name = k.as_ref()[prefix_len..].to_vec();
                    Some(Ok(name))
                }
            };
        })
        .await;
        match item {
            Err(e) => Some(Err(anyhow!(e))),
            Ok(item) => item,
        }
    }
}

pub struct AsyncListValIter<V> {
    iter: Arc<RwLock<sled::Iter>>,
    _m: std::marker::PhantomData<V>,
}

#[async_trait]
impl<V> AsyncIterator for AsyncListValIter<V>
where
    V: DeserializeOwned + Sync + Send + 'static,
{
    type Item = Result<V>;

    async fn next(&mut self) -> Option<Self::Item> {
        let iter = self.iter.clone();
        spawn_blocking(move || match iter.write().next() {
            None => None,
            Some(Err(e)) => Some(Err(anyhow::Error::new(e))),
            Some(Ok((_k, v))) => {
                Some(bincode::deserialize::<V>(v.as_ref()).map_err(|e| anyhow!(e)))
            }
        })
        .await
        .unwrap()
    }
}

pub struct AsyncEmptyIter<T> {
    _m: std::marker::PhantomData<T>,
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

pub struct AsyncMapIter {
    db: SledStorageDB,
    iter: Arc<RwLock<sled::Iter>>,
}

#[async_trait]
impl AsyncIterator for AsyncMapIter {
    type Item = Result<StorageMap>;

    async fn next(&mut self) -> Option<Self::Item> {
        let iter = self.iter.clone();
        let db = self.db.clone();
        let res = spawn_blocking(move || loop {
            match iter.write().next() {
                None => return None,
                Some(Err(e)) => return Some(Err(anyhow::Error::new(e))),
                Some(Ok((k, _))) => {
                    if !SledStorageDB::is_map_count_key(k.as_ref()) {
                        continue;
                    }
                    let name = SledStorageDB::map_count_key_to_name(k.as_ref());
                    return Some(Ok(StorageMap::Sled(db.map(name))));
                }
            }
        })
        .await;
        match res {
            Err(e) => Some(Err(anyhow!(e))),
            Ok(item) => item,
        }
    }
}

pub struct AsyncListIter {
    db: SledStorageDB,
    iter: Arc<RwLock<sled::Iter>>,
}

#[async_trait]
impl AsyncIterator for AsyncListIter {
    type Item = Result<StorageList>;

    async fn next(&mut self) -> Option<Self::Item> {
        let iter = self.iter.clone();
        let db = self.db.clone();
        let res = spawn_blocking(move || loop {
            match iter.write().next() {
                None => return None,
                Some(Err(e)) => return Some(Err(anyhow::Error::new(e))),
                Some(Ok((k, _))) => {
                    if !SledStorageDB::is_list_count_key(k.as_ref()) {
                        continue;
                    }
                    let name = SledStorageDB::list_count_key_to_name(k.as_ref());
                    return Some(Ok(StorageList::Sled(db.list(name))));
                }
            }
        })
        .await;
        match res {
            Err(e) => Some(Err(anyhow!(e))),
            Ok(item) => item,
        }
    }
}
