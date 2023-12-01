use anyhow::anyhow;
use std::future::Future;
use std::io::ErrorKind;
use std::sync::Arc;
use std::time::Duration;
use std::{io, thread};

use async_trait::async_trait;
use chrono::Timelike;
use serde::de::DeserializeOwned;
use serde::Serialize;

use sled::transaction::{
    ConflictableTransactionError, ConflictableTransactionResult, TransactionError,
    TransactionResult, TransactionalTree,
};
use sled::Batch;

use tokio::task::{block_in_place, spawn_blocking};

use crate::storage::{IterItem, Key, List, Map, StorageDB};
use crate::{timestamp_millis, Error, Result, TimestampMillis};

const SEPARATOR: &[u8] = b"@";
const DEF_TREE: &[u8] = b"__@default_tree@";
const MAP_NAME_PREFIX: &[u8] = b"__@map@";
const MAP_KEY_SEPARATOR: &[u8] = b"@item@";
const MAP_KEY_COUNT_SUFFIX: &[u8] = b"count@";
const LIST_NAME_PREFIX: &[u8] = b"__@list@";
const LIST_KEY_COUNT_SUFFIX: &[u8] = b"count@";
const LIST_KEY_CONTENT_SUFFIX: &[u8] = b"content@";
const EXPIRE_AT_KEY_PREFIX: &[u8] = b"__@expireat@";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SledConfig {
    pub path: String,
    pub cache_capacity: u64,
    pub gc_at_hour: u32,
    pub gc_at_minute: u32,
}

impl Default for SledConfig {
    fn default() -> Self {
        SledConfig {
            path: String::default(),
            cache_capacity: 1024 * 1024 * 1024,
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
            .cache_capacity(self.cache_capacity)
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
    pub(crate) fn new(cfg: SledConfig) -> Result<Self> {
        let sled_cfg = cfg.to_sled_config()?;
        let (db, def_tree) = block_in_place(move || {
            sled_cfg.open().map(|db| {
                let def_tree = db.open_tree(DEF_TREE);
                (Arc::new(db), def_tree)
            })
        })?;
        let def_tree = def_tree?;
        let db = Self { db, def_tree };
        let mut sled_db = db.clone();

        std::thread::spawn(move || {
            let db = sled_db.db.clone();
            if let Err(e) = Self::run_scheduler_task(cfg.gc_at_hour, cfg.gc_at_minute, move || {
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
            Duration::ZERO
        };

        // Wait until the next specified time.
        thread::sleep(duration_until_next_execution);
        loop {
            // Execute the provided daily task logic.
            task();
            // Set up a loop to run at the specified time every day.
            let one_day = Duration::from_secs(24 * 60 * 60);
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
            SEPARATOR,
            MAP_KEY_COUNT_SUFFIX,
        ]
        .concat()
    }

    #[inline]
    fn make_list_prefix<K>(name: K) -> Key
    where
        K: AsRef<[u8]>,
    {
        [LIST_NAME_PREFIX, name.as_ref(), SEPARATOR].concat()
    }

    #[inline]
    fn _contains_key<K: AsRef<[u8]> + Sync + Send>(&mut self, key: K) -> Result<bool> {
        if self.db.contains_key(key.as_ref())? {
            Ok(true)
        } else {
            let m = self.map(key.as_ref())?;
            if !m._is_empty() {
                Ok(true)
            } else {
                let l = self.list(key.as_ref())?;
                Ok(!l._is_empty()?)
            }
        }
    }

    #[inline]
    fn _remove<K>(&mut self, key: K) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        self.db.remove(key.as_ref())?;
        self.map(key.as_ref())?._clear()?;
        self.list(key.as_ref())?._clear()?;
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
    fn _is_expired<K>(&mut self, key: K) -> Result<bool>
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
    fn _ttl<K>(&mut self, key: K) -> Result<Option<TimestampMillis>>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let expire_key = Self::make_expire_key(key.as_ref());
        self._ttl2(key.as_ref(), expire_key.as_slice())
    }

    #[inline]
    fn _ttl2<K>(&mut self, c_key: K, expire_key: K) -> Result<Option<TimestampMillis>>
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
    fn _ttl3<K>(&mut self, c_key: K, ttl_val: &[u8]) -> Result<Option<TimestampMillis>>
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
    fn map<K: AsRef<[u8]>>(&mut self, name: K) -> Result<Self::MapType> {
        let map_prefix_name = SledStorageDB::make_map_prefix_name(name.as_ref());
        let map_item_prefix_name = SledStorageDB::make_map_item_prefix_name(name.as_ref());
        let map_count_key_name = SledStorageDB::make_map_count_key_name(name.as_ref());
        Ok(SledStorageMap {
            name: name.as_ref().to_vec(),
            map_prefix_name,
            map_item_prefix_name,
            map_count_key_name,
            db: self.clone(),
        })
    }

    #[inline]
    fn list<V: AsRef<[u8]>>(&mut self, name: V) -> Result<Self::ListType> {
        Ok(SledStorageList {
            name: name.as_ref().to_vec(),
            prefix_name: SledStorageDB::make_list_prefix(name),
            db: self.clone(),
        })
    }

    #[inline]
    async fn insert<K, V>(&mut self, key: K, val: &V) -> Result<()>
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
    async fn get<K, V>(&mut self, key: K) -> Result<Option<V>>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: DeserializeOwned + Sync + Send,
    {
        let mut this = self.clone();
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
    async fn remove<K>(&mut self, key: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let mut this = self.clone();
        let key = key.as_ref().to_vec();
        spawn_blocking(move || this._remove(key)).await??;
        Ok(())
    }

    #[inline]
    async fn contains_key<K: AsRef<[u8]> + Sync + Send>(&mut self, key: K) -> Result<bool> {
        let mut this = self.clone();
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
    async fn expire_at<K>(&mut self, key: K, at: TimestampMillis) -> Result<bool>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let mut db = self.clone();
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
    async fn expire<K>(&mut self, key: K, dur: TimestampMillis) -> Result<bool>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let at = timestamp_millis() + dur;
        self.expire_at(key, at).await
    }

    #[inline]
    async fn ttl<K>(&mut self, key: K) -> Result<Option<TimestampMillis>>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let key = key.as_ref().to_vec();
        let mut this = self.clone();
        Ok(spawn_blocking(move || this._ttl(key)).await??)
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
    fn map_count_key_to_name(map_prefix_len: usize, key: &[u8]) -> &[u8] {
        key[map_prefix_len..].as_ref()
    }

    #[inline]
    fn _len_inc(&self) -> Result<()> {
        self._counter_inc(self.map_count_key_name.as_slice())?;
        Ok(())
    }

    #[inline]
    fn _len_dec(&self) -> Result<()> {
        self._counter_dec(self.map_count_key_name.as_slice())?;
        Ok(())
    }

    #[inline]
    fn _len_get(&self) -> Result<isize> {
        self._counter_get(self.map_count_key_name.as_slice())
    }

    #[inline]
    fn _len_clear(&self) -> Result<()> {
        self._counter_set(self.map_count_key_name.as_slice(), 0)?;
        Ok(())
    }

    #[inline]
    fn _tx_len_clear<K: AsRef<[u8]>>(
        tx: &TransactionalTree,
        count_name: K,
    ) -> ConflictableTransactionResult<()> {
        tx.insert(count_name.as_ref(), 0isize.to_be_bytes().as_slice())?;
        Ok(())
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
    fn _counter_inc<K: AsRef<[u8]>>(&self, key: K) -> Result<isize> {
        let res = if let Some(res) = self.tree().update_and_fetch(key.as_ref(), _increment)? {
            isize::from_be_bytes(res.as_ref().try_into()?)
        } else {
            0
        };
        Ok(res)
    }

    #[inline]
    fn _counter_dec<K: AsRef<[u8]>>(&self, key: K) -> Result<isize> {
        let res = if let Some(res) = self.tree().update_and_fetch(key.as_ref(), _decrement)? {
            isize::from_be_bytes(res.as_ref().try_into()?)
        } else {
            0
        };
        Ok(res)
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
    fn _counter_set<K: AsRef<[u8]>>(&self, key: K, val: isize) -> Result<()> {
        self.tree().insert(key, val.to_be_bytes().as_slice())?;
        Ok(())
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
                    let name = SledStorageMap::map_count_key_to_name(
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
    async fn _retain<'a, F, Out, V>(&'a mut self, f: F) -> Result<()>
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
                    let name = SledStorageMap::map_count_key_to_name(
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
    async fn insert<K, V>(&mut self, key: K, val: &V) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: Serialize + Sync + Send + ?Sized,
    {
        let tree = self.tree().clone();
        let val = bincode::serialize(val)?;
        let item_key = self.make_map_item_key(key.as_ref());
        let count_key = self.map_count_key_name.clone();

        let mut this = self.clone();
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
    async fn get<K, V>(&mut self, key: K) -> Result<Option<V>>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: DeserializeOwned + Sync + Send,
    {
        let mut this = self.clone();
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
    async fn remove<K>(&mut self, key: K) -> Result<()>
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
    async fn contains_key<K: AsRef<[u8]> + Sync + Send>(&mut self, key: K) -> Result<bool> {
        let tree = self.tree().clone();
        let key = self.make_map_item_key(key.as_ref());
        Ok(spawn_blocking(move || tree.contains_key(key)).await??)
    }

    #[inline]
    async fn len(&mut self) -> Result<usize> {
        let mut this = self.clone();
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
    async fn is_empty(&mut self) -> Result<bool> {
        let mut this = self.clone();
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
    async fn clear(&mut self) -> Result<()> {
        let this = self.clone();
        spawn_blocking(move || this._clear()).await??;
        Ok(())
    }

    #[inline]
    async fn remove_and_fetch<K, V>(&mut self, key: K) -> Result<Option<V>>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: DeserializeOwned + Sync + Send,
    {
        let key = self.make_map_item_key(key.as_ref());
        let mut this = self.clone();
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
    async fn remove_with_prefix<K>(&mut self, prefix: K) -> Result<()>
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
    async fn batch_insert<V>(&mut self, key_vals: Vec<(Key, V)>) -> Result<()>
    where
        V: serde::ser::Serialize + Sync + Send,
    {
        for (k, v) in key_vals {
            self.insert(k, &v).await?;
        }
        Ok(())
    }

    #[inline]
    async fn batch_remove(&mut self, keys: Vec<Key>) -> Result<()> {
        for k in keys {
            self.remove(k).await?;
        }
        Ok(())
    }

    #[inline]
    async fn iter<'a, V>(&'a mut self) -> Result<Box<dyn Iterator<Item = IterItem<'a, V>> + 'a>>
    where
        V: DeserializeOwned + Sync + Send + 'a,
    {
        block_in_place(move || {
            if self.db._is_expired(self.name.as_slice())? {
                let iter: Box<dyn Iterator<Item = IterItem<V>>> = Box::new(EmptyIter {
                    _m: std::marker::PhantomData,
                });
                Ok(iter)
            } else {
                let tem_prefix_name = self.map_item_prefix_name.len();
                let iter: Box<dyn Iterator<Item = IterItem<V>>> = Box::new(Iter {
                    prefix_len: tem_prefix_name,
                    _tree: self,
                    iter: self
                        .tree()
                        .scan_prefix(self.map_item_prefix_name.as_slice()),
                    _m: std::marker::PhantomData,
                });
                Ok(iter)
            }
        })
    }

    #[inline]
    async fn key_iter<'a>(&'a mut self) -> Result<Box<dyn Iterator<Item = Result<Key>> + 'a>> {
        block_in_place(move || {
            if self.db._is_expired(self.name.as_slice())? {
                let iter: Box<dyn Iterator<Item = Result<Key>>> = Box::new(EmptyIter {
                    _m: std::marker::PhantomData,
                });
                Ok(iter)
            } else {
                let iter: Box<dyn Iterator<Item = Result<Key>>> = Box::new(KeyIter {
                    prefix_len: self.map_item_prefix_name.len(),
                    iter: self
                        .tree()
                        .scan_prefix(self.map_item_prefix_name.as_slice()),
                });
                Ok(iter)
            }
        })
    }

    #[inline]
    async fn prefix_iter<'a, P, V>(
        &'a mut self,
        prefix: P,
    ) -> Result<Box<dyn Iterator<Item = IterItem<'a, V>> + 'a>>
    where
        P: AsRef<[u8]> + Send,
        V: DeserializeOwned + Sync + Send + 'a,
    {
        block_in_place(move || {
            if self.db._is_expired(self.name.as_slice())? {
                let iter: Box<dyn Iterator<Item = IterItem<V>>> = Box::new(EmptyIter {
                    _m: std::marker::PhantomData,
                });
                Ok(iter)
            } else {
                let iter: Box<dyn Iterator<Item = IterItem<V>>> = Box::new(Iter {
                    prefix_len: self.map_item_prefix_name.len(),
                    _tree: self,
                    iter: self.tree().scan_prefix(
                        [self.map_item_prefix_name.as_slice(), prefix.as_ref()].concat(),
                    ),
                    _m: std::marker::PhantomData,
                });
                Ok(iter)
            }
        })
    }

    #[inline]
    async fn retain<'a, F, Out, V>(&'a mut self, f: F) -> Result<()>
    where
        F: Fn(Result<(Key, V)>) -> Out + Send + Sync,
        Out: Future<Output = bool> + Send + 'a,
        V: DeserializeOwned + Sync + Send + 'a,
    {
        block_in_place(move || {
            tokio::runtime::Handle::current().block_on(async move { self._retain(f).await })
        })?;
        Ok(())
    }

    #[inline]
    async fn retain_with_key<'a, F, Out>(&'a mut self, f: F) -> Result<()>
    where
        F: Fn(Result<Key>) -> Out + Send + Sync,
        Out: Future<Output = bool> + Send + 'a,
    {
        block_in_place(move || {
            tokio::runtime::Handle::current()
                .block_on(async move { self._retain_with_key(f).await })
        })?;
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
    async fn push<V>(&self, val: &V) -> Result<()>
    where
        V: serde::ser::Serialize + Sync + Send,
    {
        let data = bincode::serialize(val)?;
        let tree = self.tree().clone();
        let this = self.clone();
        spawn_blocking(move || {
            let mut this1 = this.clone();
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
            let mut this1 = this.clone();
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
        let mut this = self.clone();
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
        let mut this = self.clone();
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
        let mut this = self.clone();
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
        let mut this = self.clone();
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
        let mut this = self.clone();
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
    fn iter<'a, V>(&'a self) -> Result<Box<dyn Iterator<Item = Result<V>> + 'a>>
    where
        V: DeserializeOwned + Sync + Send + 'a + 'static,
    {
        let mut this = self.clone();
        block_in_place(move || {
            if this.db._is_expired(self.name.as_slice())? {
                let iter: Box<dyn Iterator<Item = Result<V>>> = Box::new(EmptyIter {
                    _m: std::marker::PhantomData,
                });
                Ok(iter)
            } else {
                let list_content_prefix = this.make_list_content_prefix(None);
                let iter: Box<dyn Iterator<Item = Result<V>>> = Box::new(ListValIter {
                    iter: this.tree().scan_prefix(list_content_prefix),
                    _m: std::marker::PhantomData,
                });
                Ok(iter)
            }
        })
    }
}

pub struct Iter<'a, V> {
    prefix_len: usize,
    _tree: &'a SledStorageMap,
    iter: sled::Iter,
    _m: std::marker::PhantomData<V>,
}

impl<'a, V> Iterator for Iter<'a, V>
where
    V: DeserializeOwned + Sync + Send + 'a,
{
    type Item = IterItem<'a, V>;

    fn next(&mut self) -> Option<Self::Item> {
        let item = block_in_place(|| match self.iter.next() {
            None => None,
            Some(Err(e)) => Some(Err(anyhow::Error::new(e))),
            Some(Ok((k, v))) => {
                let name = k.as_ref()[self.prefix_len..].to_vec();
                match bincode::deserialize::<V>(v.as_ref()) {
                    Ok(v) => Some(Ok((name, v))),
                    Err(e) => Some(Err(anyhow::Error::new(e))),
                }
            }
        });
        item
    }
}

pub struct KeyIter {
    prefix_len: usize,
    iter: sled::Iter,
}

impl Iterator for KeyIter {
    type Item = Result<Key>;

    fn next(&mut self) -> Option<Self::Item> {
        block_in_place(|| {
            return match self.iter.next() {
                None => None,
                Some(Err(e)) => Some(Err(anyhow::Error::new(e))),
                Some(Ok((k, _))) => {
                    let name = k.as_ref()[self.prefix_len..].to_vec();
                    Some(Ok(name))
                }
            };
        })
    }
}

pub struct ListValIter<V> {
    iter: sled::Iter,
    _m: std::marker::PhantomData<V>,
}

impl<V> Iterator for ListValIter<V>
where
    V: DeserializeOwned + Sync + Send + 'static,
{
    type Item = Result<V>;

    fn next(&mut self) -> Option<Self::Item> {
        block_in_place(|| match self.iter.next() {
            None => None,
            Some(Err(e)) => Some(Err(anyhow::Error::new(e))),
            Some(Ok((_k, v))) => {
                Some(bincode::deserialize::<V>(v.as_ref()).map_err(|e| anyhow!(e)))
            }
        })
    }
}

pub struct EmptyIter<T> {
    _m: std::marker::PhantomData<T>,
}

impl<T> Iterator for EmptyIter<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}
