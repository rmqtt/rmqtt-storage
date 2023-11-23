use anyhow::anyhow;
use std::future::Future;
use std::io;
use std::io::ErrorKind;
use std::mem::size_of;
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashSet;
use serde::de::DeserializeOwned;
use serde::Serialize;

use sled::transaction::{
    ConflictableTransactionError, ConflictableTransactionResult, TransactionalTree,
};
use sled::{Batch, IVec};

use tokio::task::{block_in_place, spawn_blocking};

use crate::storage::{IsList, IterItem, Key, List, Map, StorageDB, StorageList, Value};
use crate::{timestamp_millis, Error, Result, TimestampMillis};

const LIST_KEY_COUNT_PREFIX: &[u8] = b"@count@";
const LIST_KEY_CONTENT_PREFIX: &[u8] = b"@content@";
const EXPIRE_AT_KEY_PREFIX: &[u8] = b"@expireat@";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SledConfig {
    pub path: String,
    pub cache_capacity: u64,
}

impl Default for SledConfig {
    fn default() -> Self {
        SledConfig {
            path: String::default(),
            cache_capacity: 1024 * 1024 * 1024,
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

#[derive(Clone)]
pub struct SledStorageDB {
    db: Arc<sled::Db>,
    tree_names: Arc<DashSet<IVec>>,
}

impl SledStorageDB {
    #[inline]
    pub(crate) fn new(cfg: sled::Config) -> Result<Self> {
        let (db, tree_names) = block_in_place(move || {
            cfg.open().map(|db| {
                let tree_names = db.tree_names();
                (Arc::new(db), tree_names)
            })
        })?;

        let tree_names = tree_names
            .into_iter()
            //.map(|name| name.to_vec())
            .collect::<DashSet<_>>();

        Ok(Self {
            db,
            tree_names: Arc::new(tree_names),
        })
    }

    #[inline]
    fn make_expire_key<K>(key: K) -> Key
    where
        K: AsRef<[u8]>,
    {
        [EXPIRE_AT_KEY_PREFIX, key.as_ref()].concat()
    }

    #[inline]
    fn _contains_key<K: AsRef<[u8]> + Sync + Send>(&self, key: K) -> Result<bool> {
        if self.tree_names.contains(key.as_ref()) {
            let tree = self.db.open_tree(key.as_ref())?;
            Ok(!tree.is_empty())
        } else {
            Ok(self.db.contains_key(key)?)
        }
    }

    #[inline]
    fn _remove<K>(&self, key: K) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        if self.tree_names.contains(key.as_ref()) {
            self.db.drop_tree(key.as_ref())?;
            self.tree_names.remove(key.as_ref());
        }
        self.db.remove(key)?;
        Ok(())
    }
}

#[async_trait]
impl StorageDB for SledStorageDB {
    type MapType = SledStorageMap;

    #[inline]
    fn map<V: AsRef<[u8]>>(&mut self, name: V) -> Result<Self::MapType> {
        let tree = block_in_place(move || {
            let tree = self.db.open_tree(name.as_ref());
            if tree.is_ok() && !self.tree_names.contains(name.as_ref()) {
                self.tree_names.insert(IVec::from(name.as_ref()));
            }
            tree
        })?;
        Ok(SledStorageMap { tree })
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
        let _ = spawn_blocking(move || db.insert(key, val).map_err(|e| anyhow!(e))).await??;
        Ok(())
    }

    #[inline]
    async fn get<K, V>(&self, key: K) -> Result<Option<V>>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: DeserializeOwned + Sync + Send,
    {
        let db = self.db.clone();
        let key = key.as_ref().to_vec();
        match spawn_blocking(move || db.get(key)).await?? {
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
    async fn contains_key<K: AsRef<[u8]> + Sync + Send>(&mut self, key: K) -> Result<bool> {
        if self.tree_names.contains(key.as_ref()) {
            let db = self.db.clone();
            let key = key.as_ref().to_vec();
            let is_empty =
                spawn_blocking(move || db.open_tree(key).map(|tree| tree.is_empty())).await??;
            Ok(!is_empty)
        } else {
            let key = key.as_ref().to_vec();
            let db = self.db.clone();
            Ok(spawn_blocking(move || db.contains_key(key)).await??)
        }
    }

    #[inline]
    async fn expire_at<K>(&mut self, key: K, at: TimestampMillis) -> Result<bool>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let db = self.db.clone();
        let key = Self::make_expire_key(key);
        let _ = spawn_blocking(move || {
            db.insert(key, at.to_be_bytes().as_slice())
                .map_err(|e| anyhow!(e))
        })
        .await??;
        Ok(true)
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
        let c_key = key.as_ref().to_vec();
        let e_key = Self::make_expire_key(key.as_ref());
        let this = self.clone();
        let ttl_res = spawn_blocking(move || match this.db.get(e_key) {
            Ok(Some(v)) => Ok(Some(TimestampMillis::from_be_bytes(v.as_ref().try_into()?))),
            Ok(None) => {
                if this._contains_key(c_key)? {
                    Ok(Some(TimestampMillis::MAX))
                } else {
                    Ok(None)
                }
            }
            Err(e) => Err(anyhow!(e)),
        })
        .await??;

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
}

#[derive(Clone)]
pub struct SledStorageMap {
    //_db: SledStorageDB,
    pub(crate) tree: sled::Tree,
}

impl SledStorageMap {
    #[inline]
    async fn _retain_with_key<'a, F, Out>(&'a self, f: F) -> Result<()>
    where
        F: Fn(Result<(Key, IsList)>) -> Out + Send + Sync,
        Out: Future<Output = bool> + Send + 'a,
    {
        let mut batch = Batch::default();
        for key in self.tree.iter().keys() {
            match key {
                Ok(k) => {
                    if SledStorageList::is_array_key_content(k.as_ref()) {
                        continue;
                    }

                    if SledStorageList::is_array_key_count(k.as_ref()) {
                        let key = SledStorageList::array_key_count_to_key(k.as_ref());
                        //let l = self.list(key)?;
                        if !f(Ok((key.to_vec(), true))).await {
                            self.list(key)?._clear(&mut batch);
                        }
                    } else if !f(Ok((k.to_vec(), false))).await {
                        batch.remove(k);
                    }
                }
                Err(e) => {
                    log::warn!("{:?}", e);
                }
            }
        }
        self.tree.apply_batch(batch)?;
        Ok(())
    }

    #[inline]
    async fn _retain<'a, F, Out, V>(&'a mut self, f: F) -> Result<()>
    where
        F: Fn(Result<(Key, Value<V>)>) -> Out + Send + Sync,
        Out: Future<Output = bool> + Send + 'a,
        V: DeserializeOwned + Sync + Send + 'a,
    {
        let mut batch = Batch::default();

        for item in self.tree.iter() {
            let (k, v) = item?;

            if SledStorageList::is_array_key_content(k.as_ref()) {
                continue;
            }

            if SledStorageList::is_array_key_count(k.as_ref()) {
                let key = SledStorageList::array_key_count_to_key(k.as_ref());
                let l = self.list(key)?;
                if !f(Ok((
                    key.to_vec(),
                    Value::List(StorageList::Sled(l.clone())),
                )))
                .await
                {
                    l._clear(&mut batch);
                }
            } else {
                match bincode::deserialize::<V>(v.as_ref()) {
                    Ok(v) => {
                        if !f(Ok((k.to_vec(), Value::Val(v)))).await {
                            batch.remove(k.as_ref());
                        }
                    }
                    Err(e) => {
                        if !f(Err(anyhow::Error::new(e))).await {
                            batch.remove(k.as_ref());
                        }
                    }
                }
            }
        }
        self.tree.apply_batch(batch)?;
        Ok(())
    }
}

#[async_trait]
impl Map for SledStorageMap {
    type ListType = SledStorageList;

    fn list<V: AsRef<[u8]>>(&self, name: V) -> Result<Self::ListType> {
        Ok(SledStorageList::new(
            name.as_ref().to_vec(),
            self.tree.clone(),
        ))
    }

    #[inline]
    async fn list_iter<'a>(
        &'a mut self,
    ) -> Result<Box<dyn Iterator<Item = Result<(Key, StorageList)>> + 'a>> {
        Ok(block_in_place(move || {
            Box::new(MapListIter {
                tree: self,
                iter: self.tree.iter(),
            })
        }))
    }

    #[inline]
    async fn list_key_iter<'a>(&'a mut self) -> Result<Box<dyn Iterator<Item = Result<Key>> + 'a>> {
        Ok(block_in_place(move || {
            Box::new(MapListKeyIter {
                iter: self.tree.iter(),
            })
        }))
    }

    #[inline]
    async fn insert<K, V>(&self, key: K, val: &V) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: Serialize + Sync + Send + ?Sized,
    {
        let tree = self.tree.clone();
        let key = key.as_ref().to_vec();
        let val = bincode::serialize(val)?;
        let _ = spawn_blocking(move || tree.insert(key, val).map_err(|e| anyhow!(e))).await??;
        Ok(())
    }

    #[inline]
    async fn get<K, V>(&self, key: K) -> Result<Option<V>>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: DeserializeOwned + Sync + Send,
    {
        let tree = self.tree.clone();
        let key = key.as_ref().to_vec();
        match spawn_blocking(move || tree.get(key)).await?? {
            Some(v) => Ok(Some(bincode::deserialize::<V>(v.as_ref())?)),
            None => Ok(None),
        }
    }

    #[inline]
    async fn remove<K>(&self, key: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let tree = self.tree.clone();
        let key = key.as_ref().to_vec();
        spawn_blocking(move || tree.remove(key)).await??;
        Ok(())
    }

    #[inline]
    async fn contains_key<K: AsRef<[u8]> + Sync + Send>(&self, key: K) -> Result<bool> {
        let tree = self.tree.clone();
        let key = key.as_ref().to_vec();
        Ok(spawn_blocking(move || tree.contains_key(key)).await??)
    }

    #[inline]
    async fn len(&self) -> Result<usize> {
        let tree = self.tree.clone();
        Ok(spawn_blocking(move || tree.len()).await?)
    }

    #[inline]
    async fn is_empty(&self) -> Result<bool> {
        let tree = self.tree.clone();
        Ok(spawn_blocking(move || tree.is_empty()).await?)
    }

    #[inline]
    async fn clear(&mut self) -> Result<()> {
        //let db = self.db.clone();
        let tree = self.tree.clone();
        spawn_blocking(move || {
            tree.clear()
            //            db._remove(tree.name().as_ref())
        })
        .await??;
        Ok(())
    }

    #[inline]
    async fn remove_and_fetch<K, V>(&self, key: K) -> Result<Option<V>>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: DeserializeOwned + Sync + Send,
    {
        let tree = self.tree.clone();
        let key = key.as_ref().to_vec();
        match spawn_blocking(move || tree.remove(key)).await?? {
            Some(v) => Ok(Some(bincode::deserialize::<V>(v.as_ref())?)),
            None => Ok(None),
        }
    }

    #[inline]
    async fn remove_with_prefix<K>(&self, prefix: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let tree = self.tree.clone();
        let prefix = prefix.as_ref().to_vec();
        spawn_blocking(move || {
            let mut removeds = Batch::default();
            for item in tree.scan_prefix(prefix) {
                match item {
                    Ok((k, _v)) => {
                        removeds.remove(k.as_ref());
                    }
                    Err(e) => {
                        log::warn!("{:?}", e);
                    }
                }
            }
            tree.apply_batch(removeds)
        })
        .await??;
        Ok(())
    }

    #[inline]
    async fn batch_insert<V>(&self, key_vals: Vec<(Key, V)>) -> Result<()>
    where
        V: serde::ser::Serialize + Sync + Send,
    {
        let mut batch = sled::Batch::default();
        for (key, val) in key_vals {
            match bincode::serialize(&val) {
                Ok(data) => {
                    batch.insert(key, data);
                }
                Err(e) => {
                    log::warn!("{:?}", e);
                }
            }
        }

        let tree = self.tree.clone();
        spawn_blocking(move || tree.apply_batch(batch)).await??;
        Ok(())
    }

    #[inline]
    async fn batch_remove(&self, keys: Vec<Key>) -> Result<()> {
        let mut batch = sled::Batch::default();
        for key in keys {
            batch.remove(key);
        }
        let tree = self.tree.clone();
        spawn_blocking(move || tree.apply_batch(batch)).await??;
        Ok(())
    }

    #[inline]
    async fn iter<'a, V>(&'a mut self) -> Result<Box<dyn Iterator<Item = IterItem<'a, V>> + 'a>>
    where
        V: DeserializeOwned + Sync + Send + 'a,
    {
        Ok(block_in_place(move || {
            Box::new(AsyncIter {
                _tree: self,
                iter: self.tree.iter(),
                _m: std::marker::PhantomData,
            })
        }))
    }

    #[inline]
    async fn key_iter<'a>(&'a mut self) -> Result<Box<dyn Iterator<Item = Result<Key>> + 'a>> {
        Ok(block_in_place(move || {
            Box::new(KeyIter {
                iter: self.tree.iter(),
            })
        }))
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
        Ok(block_in_place(move || {
            Box::new(Iter {
                _tree: self,
                iter: self.tree.scan_prefix(prefix.as_ref()),
                _m: std::marker::PhantomData,
            })
        }))
    }

    #[inline]
    async fn retain<'a, F, Out, V>(&'a mut self, f: F) -> Result<()>
    where
        F: Fn(Result<(Key, Value<V>)>) -> Out + Send + Sync,
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
        F: Fn(Result<(Key, IsList)>) -> Out + Send + Sync,
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
    key: Key,
    tree: sled::Tree,
}

impl SledStorageList {
    pub(crate) fn new(key: Key, tree: sled::Tree) -> SledStorageList {
        Self { key, tree }
    }

    #[inline]
    pub(crate) fn name(&self) -> &[u8] {
        self.key.as_slice()
    }

    #[inline]
    fn array_key_count<K>(key: K) -> Vec<u8>
    where
        K: AsRef<[u8]>,
    {
        //let mut key_count = LIST_KEY_COUNT_PREFIX.to_vec();
        //key_count.extend_from_slice(key.as_ref());
        let mut key_count = key.as_ref().to_vec();
        key_count.extend_from_slice(LIST_KEY_COUNT_PREFIX);
        key_count
    }

    #[inline]
    fn array_key_content<K>(key: K, idx: usize) -> Vec<u8>
    where
        K: AsRef<[u8]>,
    {
        let mut key_content = Self::array_key_content_prefix(key);
        key_content.extend_from_slice(idx.to_be_bytes().as_slice());
        key_content
    }

    #[inline]
    fn array_key_content_prefix<K>(key: K) -> Vec<u8>
    where
        K: AsRef<[u8]>,
    {
        //let mut key_content = LIST_KEY_CONTENT_PREFIX.to_vec();
        //key_content.extend_from_slice(key.as_ref());
        let mut key_content = key.as_ref().to_vec();
        key_content.extend_from_slice(LIST_KEY_CONTENT_PREFIX);
        key_content
    }

    #[inline]
    fn is_array_key_content<K>(key: K) -> bool
    where
        K: AsRef<[u8]>,
    {
        if key.as_ref().len() > LIST_KEY_CONTENT_PREFIX.len() {
            let prefix = key.as_ref()[0..key.as_ref().len() - size_of::<usize>()].as_ref();
            prefix.ends_with(LIST_KEY_CONTENT_PREFIX)
        } else {
            false
        }
    }

    #[inline]
    fn is_array_key_count<K>(key: K) -> bool
    where
        K: AsRef<[u8]>,
    {
        key.as_ref().len() > LIST_KEY_COUNT_PREFIX.len()
            && key.as_ref().ends_with(LIST_KEY_COUNT_PREFIX)
    }

    #[inline]
    fn array_key_count_to_key(key: &[u8]) -> &[u8] {
        key[0..key.len() - LIST_KEY_COUNT_PREFIX.len()].as_ref()
    }

    #[inline]
    fn _array_key_content_to_key(key: &[u8]) -> &[u8] {
        key[0..key.len() - (size_of::<usize>() + LIST_KEY_CONTENT_PREFIX.len())].as_ref()
    }

    #[inline]
    fn _array_key_content_to_idx(key: &[u8]) -> Result<usize> {
        let idx = usize::from_be_bytes(
            key[(key.len() - size_of::<usize>())..]
                .as_ref()
                .try_into()?,
        );
        Ok(idx)
    }

    #[inline]
    fn _array_tx_count_with(count_val: IVec) -> Result<(usize, usize)> {
        Ok(bincode::deserialize::<(usize, usize)>(count_val.as_ref())?)
    }

    #[inline]
    fn _array_tx_count<K>(tree: &sled::Tree, key_count: K) -> Result<(usize, usize)>
    where
        K: AsRef<[u8]>,
    {
        if let Some(v) = tree.get(key_count.as_ref())? {
            let (start, end) = bincode::deserialize::<(usize, usize)>(v.as_ref())?;
            Ok((start, end))
        } else {
            Ok((0, 0))
        }
    }

    #[inline]
    fn array_tx_count<K, E>(
        tx: &TransactionalTree,
        key_count: K,
    ) -> ConflictableTransactionResult<(usize, usize), E>
    where
        K: AsRef<[u8]>,
    {
        if let Some(v) = tx.get(key_count.as_ref())? {
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
    fn array_tx_set_count<K, E>(
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
    fn array_tx_set_content<K, V, E>(
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
    fn _array_get<K, V>(&self, key: K) -> Result<Vec<V>>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: DeserializeOwned + Sync + Send,
    {
        let key_content_prefix = Self::array_key_content_prefix(key.as_ref());

        let res = self
            .tree
            .scan_prefix(key_content_prefix)
            .values()
            .map(|item| {
                item.and_then(|v| {
                    bincode::deserialize::<V>(v.as_ref())
                        .map_err(|e| sled::Error::Io(io::Error::new(ErrorKind::InvalidData, e)))
                })
                .map_err(anyhow::Error::new)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(res)
    }

    #[inline]
    fn _clear(&self, batch: &mut Batch) {
        let key_count = Self::array_key_count(self.key.as_slice());
        let key_content_prefix = Self::array_key_content_prefix(self.key.as_slice());

        for item in self.tree.scan_prefix(key_count).keys() {
            match item {
                Ok(k) => {
                    batch.remove(k);
                }
                Err(e) => {
                    log::warn!("{:?}", e);
                }
            }
        }

        for item in self.tree.scan_prefix(key_content_prefix).keys() {
            match item {
                Ok(k) => {
                    batch.remove(k);
                }
                Err(e) => {
                    log::warn!("{:?}", e);
                }
            }
        }
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
        let key_count = Self::array_key_count(self.key.as_slice());
        let key = self.key.clone();
        let tree = self.tree.clone();
        spawn_blocking(move || {
            tree.transaction(move |tx| {
                let (start, mut end) = Self::array_tx_count(tx, key_count.as_slice())?;
                end += 1;
                Self::array_tx_set_count(tx, key_count.as_slice(), start, end)?;
                let key_content = Self::array_key_content(key.as_slice(), end);
                Self::array_tx_set_content(tx, key_content.as_slice(), &data)?;
                Ok::<(), ConflictableTransactionError<sled::Error>>(())
            })
        })
        .await??;

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
        let key_count = Self::array_key_count(self.key.as_slice());
        let key = self.key.clone();
        let tree = self.tree.clone();
        let removed = spawn_blocking(move || {
            tree.transaction(move |tx| {
                let (mut start, mut end) = Self::array_tx_count::<
                    _,
                    ConflictableTransactionError<sled::Error>,
                >(tx, key_count.as_slice())?;
                let count = end - start;

                let res = if count < limit {
                    end += 1;
                    Self::array_tx_set_count(tx, key_count.as_slice(), start, end)?;
                    let key_content = Self::array_key_content(key.as_slice(), end);
                    Self::array_tx_set_content(tx, key_content.as_slice(), &data)?;
                    Ok(None)
                } else if pop_front_if_limited {
                    let mut removed = None;
                    let removed_key_content = Self::array_key_content(key.as_slice(), start + 1);
                    if let Some(v) = tx.remove(removed_key_content)? {
                        removed = Some(v);
                        start += 1;
                    }
                    end += 1;
                    Self::array_tx_set_count(tx, key_count.as_slice(), start, end)?;
                    let key_content = Self::array_key_content(key.as_slice(), end);
                    Self::array_tx_set_content(tx, key_content.as_slice(), &data)?;
                    Ok(removed)
                } else {
                    Err(ConflictableTransactionError::Storage(sled::Error::Io(
                        io::Error::new(ErrorKind::InvalidData, "Is full"),
                    )))
                };
                res
            })
        })
        .await??;

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
    async fn all<V>(&self) -> Result<Vec<V>>
    where
        V: DeserializeOwned + Sync + Send,
    {
        let key_content_prefix = Self::array_key_content_prefix(self.key.as_slice());

        let tree = self.tree.clone();
        let res = spawn_blocking(move || {
            tree.scan_prefix(key_content_prefix)
                .values()
                .map(|item| item.map_err(anyhow::Error::new))
                .collect::<Result<Vec<_>>>()
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
        let key_count = Self::array_key_count(self.key.as_slice());
        let key = self.key.clone();
        let tree = self.tree.clone();
        let res = spawn_blocking(move || {
            let res = tree.transaction(move |tx| {
                let (start, end) = Self::array_tx_count::<
                    _,
                    ConflictableTransactionError<sled::Error>,
                >(tx, key_count.as_slice())?;
                let res = if idx < (end - start) {
                    let key_content = Self::array_key_content(key.as_slice(), start + idx + 1);
                    if let Some(v) = tx.get(key_content)? {
                        Ok(v)
                    } else {
                        Err(ConflictableTransactionError::Storage(sled::Error::Io(
                            io::Error::new(ErrorKind::InvalidData, "Data Disorder"),
                        )))
                    }
                } else {
                    Err(ConflictableTransactionError::Storage(sled::Error::Io(
                        io::Error::new(ErrorKind::InvalidData, "Index Out of Bounds"),
                    )))
                };
                res
            });
            res
        })
        .await??;

        Ok(Some(
            bincode::deserialize::<V>(res.as_ref()).map_err(|e| anyhow!(e))?,
        ))
    }

    #[inline]
    async fn len(&self) -> Result<usize> {
        let key_count = Self::array_key_count(self.key.as_slice());
        let tree = self.tree.clone();
        spawn_blocking(move || {
            if let Some(v) = tree.get(key_count.as_slice())? {
                let (start, end) = bincode::deserialize::<(usize, usize)>(v.as_ref())?;
                Ok(end - start)
            } else {
                Ok(0)
            }
        })
        .await?
    }

    #[inline]
    async fn is_empty(&self) -> Result<bool> {
        Ok(self.len().await? == 0)
    }

    #[inline]
    async fn clear(&self) -> Result<()> {
        let this = self.clone();
        spawn_blocking(move || {
            let mut batch = Batch::default();
            this._clear(&mut batch);
            this.tree.apply_batch(batch)
        })
        .await??;
        Ok(())
    }

    #[inline]
    async fn pop<V>(&self) -> Result<Option<V>>
    where
        V: DeserializeOwned + Sync + Send,
    {
        let key_count = Self::array_key_count(self.key.as_slice());
        let key = self.key.clone();
        let tree = self.tree.clone();
        let removed = spawn_blocking(move || {
            let removed = tree.transaction(move |tx| {
                let (start, end) = Self::array_tx_count(tx, key_count.as_slice())?;

                let mut removed = None;
                if (end - start) > 0 {
                    let removed_key_content = Self::array_key_content(key.as_slice(), start + 1);
                    if let Some(v) = tx.remove(removed_key_content)? {
                        removed = Some(v);
                        Self::array_tx_set_count(tx, key_count.as_slice(), start + 1, end)?;
                    }
                }
                Ok::<_, ConflictableTransactionError<sled::Error>>(removed)
            });
            removed
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
    fn iter<'a, V>(&'a self) -> Result<Box<dyn Iterator<Item = Result<V>> + 'a>>
    where
        V: DeserializeOwned + Sync + Send + 'a + 'static,
    {
        let key_content_prefix = Self::array_key_content_prefix(self.key.as_slice());
        Ok(block_in_place(move || {
            Box::new(ListValIter {
                iter: self.tree.scan_prefix(key_content_prefix),
                _m: std::marker::PhantomData,
            })
        }))
    }
}

pub struct AsyncIter<'a, V> {
    _tree: &'a SledStorageMap,
    iter: sled::Iter,
    _m: std::marker::PhantomData<V>,
}

//#[async_trait]
impl<'a, V> Iterator for AsyncIter<'a, V>
where
    V: DeserializeOwned + Sync + Send + 'a,
{
    type Item = IterItem<'a, V>;

    fn next(&mut self) -> Option<Self::Item> {
        let item = block_in_place(|| loop {
            match self.iter.next() {
                None => return None,
                Some(Err(e)) => return Some(Err(anyhow::Error::new(e))),
                Some(Ok((k, v))) => {
                    if SledStorageList::is_array_key_content(k.as_ref()) {
                        continue;
                    }
                    if SledStorageList::is_array_key_count(k.as_ref()) {
                        continue;
                    }

                    return match bincode::deserialize::<V>(v.as_ref()) {
                        //Ok(v) => Some(Ok((k.to_vec(), Value::Val(v)))),
                        Ok(v) => Some(Ok((k.to_vec(), v))),
                        Err(e) => Some(Err(anyhow::Error::new(e))),
                    };
                    /*
                    return if SledStorageList::is_array_key_count(k.as_ref()) {
                        let key = SledStorageList::array_key_count_to_key(k.as_ref());
                        match self.tree.list(key) {
                            Ok(l) => Some(Ok((key.to_vec(), Value::List(StorageList::Sled(l))))),
                            Err(e) => Some(Err(e)),
                        }
                    } else {
                        match bincode::deserialize::<V>(v.as_ref()) {
                            Ok(v) => Some(Ok((k.to_vec(), Value::Val(v)))),
                            Err(e) => Some(Err(anyhow::Error::new(e))),
                        }
                    };*/
                }
            }
        });
        item
    }
}

pub struct Iter<'a, V> {
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
        let item = block_in_place(|| loop {
            match self.iter.next() {
                None => return None,
                Some(Err(e)) => return Some(Err(anyhow::Error::new(e))),
                Some(Ok((k, v))) => {
                    if SledStorageList::is_array_key_content(k.as_ref()) {
                        continue;
                    }
                    if SledStorageList::is_array_key_count(k.as_ref()) {
                        continue;
                    }

                    return match bincode::deserialize::<V>(v.as_ref()) {
                        //Ok(v) => Some(Ok((k.to_vec(), Value::Val(v)))),
                        Ok(v) => Some(Ok((k.to_vec(), v))),
                        Err(e) => Some(Err(anyhow::Error::new(e))),
                    };
                    /*
                    return if SledStorageList::is_array_key_count(k.as_ref()) {
                        let key = SledStorageList::array_key_count_to_key(k.as_ref());
                        match self.tree.list(key) {
                            Ok(l) => Some(Ok((key.to_vec(), Value::List(StorageList::Sled(l))))),
                            Err(e) => Some(Err(e)),
                        }
                    } else {
                        match bincode::deserialize::<V>(v.as_ref()) {
                            Ok(v) => Some(Ok((k.to_vec(), Value::Val(v)))),
                            Err(e) => Some(Err(anyhow::Error::new(e))),
                        }
                    }; */
                }
            }
        });
        item
    }
}

pub struct KeyIter {
    iter: sled::Iter,
}

impl Iterator for KeyIter {
    type Item = Result<Key>;

    fn next(&mut self) -> Option<Self::Item> {
        block_in_place(|| loop {
            match self.iter.next() {
                None => return None,
                Some(Err(e)) => return Some(Err(anyhow::Error::new(e))),
                Some(Ok((k, _))) => {
                    if SledStorageList::is_array_key_content(k.as_ref()) {
                        continue;
                    }

                    if SledStorageList::is_array_key_count(k.as_ref()) {
                        continue;
                    }

                    return Some(Ok(k.to_vec()));
                }
            }
        })
    }
}

pub struct MapListIter<'a> {
    tree: &'a SledStorageMap,
    iter: sled::Iter,
}

impl<'a> Iterator for MapListIter<'a> {
    type Item = Result<(Key, StorageList)>;

    fn next(&mut self) -> Option<Self::Item> {
        block_in_place(|| loop {
            match self.iter.next() {
                None => return None,
                Some(Err(e)) => return Some(Err(anyhow::Error::new(e))),
                Some(Ok((k, _))) => {
                    if SledStorageList::is_array_key_content(k.as_ref()) {
                        continue;
                    }
                    if SledStorageList::is_array_key_count(k.as_ref()) {
                        let key = SledStorageList::array_key_count_to_key(k.as_ref());
                        return match self.tree.list(key) {
                            Ok(l) => Some(Ok((key.to_vec(), StorageList::Sled(l)))),
                            Err(e) => Some(Err(e)),
                        };
                    } else {
                        continue;
                    };
                }
            }
        })
    }
}

pub struct MapListKeyIter {
    iter: sled::Iter,
}

impl Iterator for MapListKeyIter {
    type Item = Result<Key>;

    fn next(&mut self) -> Option<Self::Item> {
        block_in_place(|| loop {
            match self.iter.next() {
                None => return None,
                Some(Err(e)) => return Some(Err(anyhow::Error::new(e))),
                Some(Ok((k, _))) => {
                    if SledStorageList::is_array_key_content(k.as_ref()) {
                        continue;
                    }
                    if SledStorageList::is_array_key_count(k.as_ref()) {
                        let key = SledStorageList::array_key_count_to_key(k.as_ref());
                        return Some(Ok(key.to_vec()));
                    } else {
                        continue;
                    };
                }
            }
        })
    }
}

pub struct ArrayKeyIter {
    iter: sled::Iter,
}

impl Iterator for ArrayKeyIter {
    type Item = Result<Key>;

    fn next(&mut self) -> Option<Self::Item> {
        block_in_place(|| match self.iter.next() {
            None => None,
            Some(Err(e)) => Some(Err(anyhow::Error::new(e))),
            Some(Ok((k, _v))) => {
                let key = k[LIST_KEY_COUNT_PREFIX.len()..].to_vec();
                Some(Ok(key))
            }
        })
    }
}

pub struct ListIter<'a, V> {
    tree: &'a SledStorageList,
    iter: sled::Iter,
    _m: std::marker::PhantomData<V>,
}

impl<'a, V> Iterator for ListIter<'a, V>
where
    V: DeserializeOwned + Sync + Send + 'static,
{
    type Item = Result<(Key, Vec<V>)>;

    fn next(&mut self) -> Option<Self::Item> {
        block_in_place(|| match self.iter.next() {
            None => None,
            Some(Err(e)) => Some(Err(anyhow::Error::new(e))),
            Some(Ok((k, _v))) => {
                let key = k[LIST_KEY_COUNT_PREFIX.len()..].as_ref();
                let arrays = self.tree._array_get::<_, V>(key);
                let arrays = arrays.map(|arrays| (key.to_vec(), arrays));
                Some(arrays)
            }
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
