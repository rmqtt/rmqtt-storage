use core::fmt;
use std::future::Future;

use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::storage_redis::{RedisStorageDB, RedisStorageList, RedisStorageMap};
use crate::storage_sled::SledStorageDB;
use crate::storage_sled::{SledStorageList, SledStorageMap};
use crate::{Result, TimestampMillis};

pub type Key = Vec<u8>;

pub type IterItem<V> = Result<(Key, V)>;

#[async_trait]
pub trait AsyncIterator {
    type Item;
    async fn next(&mut self) -> Option<Self::Item>;
}

#[async_trait]
pub trait StorageDB: Send + Sync {
    type MapType: Map;
    type ListType: List;

    fn map<V: AsRef<[u8]>>(&self, name: V) -> Self::MapType;

    fn list<V: AsRef<[u8]>>(&self, name: V) -> Self::ListType;

    async fn insert<K, V>(&self, key: K, val: &V) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: serde::ser::Serialize + Sync + Send;

    async fn get<K, V>(&self, key: K) -> Result<Option<V>>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: DeserializeOwned + Sync + Send;

    async fn remove<K>(&self, key: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send;

    async fn batch_insert<V>(&self, key_vals: Vec<(Key, V)>) -> Result<()>
    where
        V: serde::ser::Serialize + Sync + Send;

    async fn batch_remove(&self, keys: Vec<Key>) -> Result<()>;

    async fn counter_incr<K>(&self, key: K, increment: isize) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send;

    async fn counter_decr<K>(&self, key: K, increment: isize) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send;

    async fn counter_get<K>(&self, key: K) -> Result<Option<isize>>
    where
        K: AsRef<[u8]> + Sync + Send;

    async fn counter_set<K>(&self, key: K, val: isize) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send;

    async fn contains_key<K: AsRef<[u8]> + Sync + Send>(&self, key: K) -> Result<bool>;

    async fn expire_at<K>(&self, key: K, dur: TimestampMillis) -> Result<bool>
    where
        K: AsRef<[u8]> + Sync + Send;

    async fn expire<K>(&self, key: K, dur: TimestampMillis) -> Result<bool>
    where
        K: AsRef<[u8]> + Sync + Send;

    async fn ttl<K>(&self, key: K) -> Result<Option<TimestampMillis>>
    where
        K: AsRef<[u8]> + Sync + Send;

    async fn map_iter<'a>(
        &'a mut self,
    ) -> Result<Box<dyn AsyncIterator<Item = Result<StorageMap>> + Send + 'a>>;

    async fn list_iter<'a>(
        &'a mut self,
    ) -> Result<Box<dyn AsyncIterator<Item = Result<StorageList>> + Send + 'a>>;
}

#[async_trait]
pub trait Map: Sync + Send {
    fn name(&self) -> &[u8];

    async fn insert<K, V>(&self, key: K, val: &V) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: serde::ser::Serialize + Sync + Send + ?Sized;

    async fn get<K, V>(&self, key: K) -> Result<Option<V>>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: DeserializeOwned + Sync + Send;

    async fn remove<K>(&self, key: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send;

    async fn contains_key<K: AsRef<[u8]> + Sync + Send>(&self, key: K) -> Result<bool>;

    async fn len(&self) -> Result<usize>;

    async fn is_empty(&self) -> Result<bool>;

    async fn clear(&self) -> Result<()>;

    async fn remove_and_fetch<K, V>(&self, key: K) -> Result<Option<V>>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: DeserializeOwned + Sync + Send;

    async fn remove_with_prefix<K>(&self, prefix: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send;

    async fn batch_insert<V>(&self, key_vals: Vec<(Key, V)>) -> Result<()>
    where
        V: serde::ser::Serialize + Sync + Send;

    async fn batch_remove(&self, keys: Vec<Key>) -> Result<()>;

    async fn iter<'a, V>(
        &'a mut self,
    ) -> Result<Box<dyn AsyncIterator<Item = IterItem<V>> + Send + 'a>>
    where
        V: DeserializeOwned + Sync + Send + 'a + 'static;

    async fn key_iter<'a>(
        &'a mut self,
    ) -> Result<Box<dyn AsyncIterator<Item = Result<Key>> + Send + 'a>>;

    async fn prefix_iter<'a, P, V>(
        &'a mut self,
        prefix: P,
    ) -> Result<Box<dyn AsyncIterator<Item = IterItem<V>> + Send + 'a>>
    where
        P: AsRef<[u8]> + Send,
        V: DeserializeOwned + Sync + Send + 'a + 'static;

    async fn retain<'a, F, Out, V>(&'a self, f: F) -> Result<()>
    where
        F: Fn(Result<(Key, V)>) -> Out + Send + Sync + 'static,
        Out: Future<Output = bool> + Send + 'a,
        V: DeserializeOwned + Sync + Send + 'a;

    async fn retain_with_key<'a, F, Out>(&'a self, f: F) -> Result<()>
    where
        F: Fn(Result<Key>) -> Out + Send + Sync + 'static,
        Out: Future<Output = bool> + Send + 'a;
}

#[async_trait]
pub trait List: Sync + Send {
    fn name(&self) -> &[u8];

    async fn push<V>(&self, val: &V) -> Result<()>
    where
        V: serde::ser::Serialize + Sync + Send;

    async fn push_limit<V>(
        &self,
        val: &V,
        limit: usize,
        pop_front_if_limited: bool,
    ) -> Result<Option<V>>
    where
        V: serde::ser::Serialize + Sync + Send,
        V: DeserializeOwned;

    async fn pop<V>(&self) -> Result<Option<V>>
    where
        V: DeserializeOwned + Sync + Send;

    async fn all<V>(&self) -> Result<Vec<V>>
    where
        V: DeserializeOwned + Sync + Send;

    async fn get_index<V>(&self, idx: usize) -> Result<Option<V>>
    where
        V: DeserializeOwned + Sync + Send;

    async fn len(&self) -> Result<usize>;

    async fn is_empty(&self) -> Result<bool>;

    async fn clear(&self) -> Result<()>;

    async fn iter<'a, V>(
        &'a mut self,
    ) -> Result<Box<dyn AsyncIterator<Item = Result<V>> + Send + 'a>>
    where
        V: DeserializeOwned + Sync + Send + 'a + 'static;
}

#[derive(Clone)]
pub enum DefaultStorageDB {
    Sled(SledStorageDB),
    Redis(RedisStorageDB),
}

impl DefaultStorageDB {
    #[inline]
    pub fn map<V: AsRef<[u8]>>(&self, name: V) -> StorageMap {
        match self {
            DefaultStorageDB::Sled(db) => StorageMap::Sled(db.map(name)),
            DefaultStorageDB::Redis(db) => StorageMap::Redis(db.map(name)),
        }
    }

    #[inline]
    pub fn list<V: AsRef<[u8]>>(&self, name: V) -> StorageList {
        match self {
            DefaultStorageDB::Sled(db) => StorageList::Sled(db.list(name)),
            DefaultStorageDB::Redis(db) => StorageList::Redis(db.list(name)),
        }
    }

    #[inline]
    pub async fn insert<K, V>(&self, key: K, val: &V) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: Serialize + Sync + Send,
    {
        match self {
            DefaultStorageDB::Sled(db) => db.insert(key, val).await,
            DefaultStorageDB::Redis(db) => db.insert(key, val).await,
        }
    }

    #[inline]
    pub async fn get<K, V>(&self, key: K) -> Result<Option<V>>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: DeserializeOwned + Sync + Send,
    {
        match self {
            DefaultStorageDB::Sled(db) => db.get(key).await,
            DefaultStorageDB::Redis(db) => db.get(key).await,
        }
    }

    #[inline]
    pub async fn remove<K>(&self, key: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        match self {
            DefaultStorageDB::Sled(db) => db.remove(key).await,
            DefaultStorageDB::Redis(db) => db.remove(key).await,
        }
    }

    #[inline]
    pub async fn batch_insert<V>(&self, key_vals: Vec<(Key, V)>) -> Result<()>
    where
        V: serde::ser::Serialize + Sync + Send,
    {
        match self {
            DefaultStorageDB::Sled(db) => db.batch_insert(key_vals).await,
            DefaultStorageDB::Redis(db) => db.batch_insert(key_vals).await,
        }
    }

    #[inline]
    pub async fn batch_remove(&self, keys: Vec<Key>) -> Result<()> {
        match self {
            DefaultStorageDB::Sled(db) => db.batch_remove(keys).await,
            DefaultStorageDB::Redis(db) => db.batch_remove(keys).await,
        }
    }

    #[inline]
    pub async fn counter_incr<K>(&self, key: K, increment: isize) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        match self {
            DefaultStorageDB::Sled(db) => db.counter_incr(key, increment).await,
            DefaultStorageDB::Redis(db) => db.counter_incr(key, increment).await,
        }
    }

    #[inline]
    pub async fn counter_decr<K>(&self, key: K, decrement: isize) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        match self {
            DefaultStorageDB::Sled(db) => db.counter_decr(key, decrement).await,
            DefaultStorageDB::Redis(db) => db.counter_decr(key, decrement).await,
        }
    }

    #[inline]
    pub async fn counter_get<K>(&self, key: K) -> Result<Option<isize>>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        match self {
            DefaultStorageDB::Sled(db) => db.counter_get(key).await,
            DefaultStorageDB::Redis(db) => db.counter_get(key).await,
        }
    }

    #[inline]
    pub async fn counter_set<K>(&self, key: K, val: isize) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        match self {
            DefaultStorageDB::Sled(db) => db.counter_set(key, val).await,
            DefaultStorageDB::Redis(db) => db.counter_set(key, val).await,
        }
    }

    #[inline]
    pub async fn contains_key<K: AsRef<[u8]> + Sync + Send>(&self, key: K) -> Result<bool> {
        match self {
            DefaultStorageDB::Sled(db) => db.contains_key(key).await,
            DefaultStorageDB::Redis(db) => db.contains_key(key).await,
        }
    }

    #[inline]
    pub async fn expire_at<K>(&self, key: K, dur: TimestampMillis) -> Result<bool>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        match self {
            DefaultStorageDB::Sled(db) => db.expire_at(key, dur).await,
            DefaultStorageDB::Redis(db) => db.expire_at(key, dur).await,
        }
    }

    #[inline]
    pub async fn expire<K>(&self, key: K, dur: TimestampMillis) -> Result<bool>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        match self {
            DefaultStorageDB::Sled(db) => db.expire(key, dur).await,
            DefaultStorageDB::Redis(db) => db.expire(key, dur).await,
        }
    }

    #[inline]
    pub async fn ttl<K>(&self, key: K) -> Result<Option<TimestampMillis>>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        match self {
            DefaultStorageDB::Sled(db) => db.ttl(key).await,
            DefaultStorageDB::Redis(db) => db.ttl(key).await,
        }
    }

    #[inline]
    pub async fn map_iter<'a>(
        &'a mut self,
    ) -> Result<Box<dyn AsyncIterator<Item = Result<StorageMap>> + Send + 'a>> {
        match self {
            DefaultStorageDB::Sled(db) => db.map_iter().await,
            DefaultStorageDB::Redis(db) => db.map_iter().await,
        }
    }

    #[inline]
    pub async fn list_iter<'a>(
        &'a mut self,
    ) -> Result<Box<dyn AsyncIterator<Item = Result<StorageList>> + Send + 'a>> {
        match self {
            DefaultStorageDB::Sled(db) => db.list_iter().await,
            DefaultStorageDB::Redis(db) => db.list_iter().await,
        }
    }
}

#[derive(Clone)]
pub enum StorageMap {
    Sled(SledStorageMap),
    Redis(RedisStorageMap),
}

#[async_trait]
impl Map for StorageMap {
    fn name(&self) -> &[u8] {
        match self {
            StorageMap::Sled(m) => m.name(),
            StorageMap::Redis(m) => m.name(),
        }
    }

    async fn insert<K, V>(&self, key: K, val: &V) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: Serialize + Sync + Send + ?Sized,
    {
        match self {
            StorageMap::Sled(m) => m.insert(key, val).await,
            StorageMap::Redis(m) => m.insert(key, val).await,
        }
    }

    async fn get<K, V>(&self, key: K) -> Result<Option<V>>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: DeserializeOwned + Sync + Send,
    {
        match self {
            StorageMap::Sled(m) => m.get(key).await,
            StorageMap::Redis(m) => m.get(key).await,
        }
    }

    async fn remove<K>(&self, key: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        match self {
            StorageMap::Sled(m) => m.remove(key).await,
            StorageMap::Redis(m) => m.remove(key).await,
        }
    }

    async fn contains_key<K: AsRef<[u8]> + Sync + Send>(&self, key: K) -> Result<bool> {
        match self {
            StorageMap::Sled(m) => m.contains_key(key).await,
            StorageMap::Redis(m) => m.contains_key(key).await,
        }
    }

    async fn len(&self) -> Result<usize> {
        match self {
            StorageMap::Sled(m) => m.len().await,
            StorageMap::Redis(m) => m.len().await,
        }
    }

    async fn is_empty(&self) -> Result<bool> {
        match self {
            StorageMap::Sled(m) => m.is_empty().await,
            StorageMap::Redis(m) => m.is_empty().await,
        }
    }

    async fn clear(&self) -> Result<()> {
        match self {
            StorageMap::Sled(m) => m.clear().await,
            StorageMap::Redis(m) => m.clear().await,
        }
    }

    async fn remove_and_fetch<K, V>(&self, key: K) -> Result<Option<V>>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: DeserializeOwned + Sync + Send,
    {
        match self {
            StorageMap::Sled(m) => m.remove_and_fetch(key).await,
            StorageMap::Redis(m) => m.remove_and_fetch(key).await,
        }
    }

    async fn remove_with_prefix<K>(&self, prefix: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        match self {
            StorageMap::Sled(m) => m.remove_with_prefix(prefix).await,
            StorageMap::Redis(m) => m.remove_with_prefix(prefix).await,
        }
    }

    async fn batch_insert<V>(&self, key_vals: Vec<(Key, V)>) -> Result<()>
    where
        V: Serialize + Sync + Send,
    {
        match self {
            StorageMap::Sled(m) => m.batch_insert(key_vals).await,
            StorageMap::Redis(m) => m.batch_insert(key_vals).await,
        }
    }

    async fn batch_remove(&self, keys: Vec<Key>) -> Result<()> {
        match self {
            StorageMap::Sled(m) => m.batch_remove(keys).await,
            StorageMap::Redis(m) => m.batch_remove(keys).await,
        }
    }

    async fn iter<'a, V>(
        &'a mut self,
    ) -> Result<Box<dyn AsyncIterator<Item = IterItem<V>> + Send + 'a>>
    where
        V: DeserializeOwned + Sync + Send + 'a + 'static,
    {
        match self {
            StorageMap::Sled(m) => m.iter().await,
            StorageMap::Redis(m) => m.iter().await,
        }
    }

    async fn key_iter<'a>(
        &'a mut self,
    ) -> Result<Box<dyn AsyncIterator<Item = Result<Key>> + Send + 'a>> {
        match self {
            StorageMap::Sled(m) => m.key_iter().await,
            StorageMap::Redis(m) => m.key_iter().await,
        }
    }

    async fn prefix_iter<'a, P, V>(
        &'a mut self,
        prefix: P,
    ) -> Result<Box<dyn AsyncIterator<Item = IterItem<V>> + Send + 'a>>
    where
        P: AsRef<[u8]> + Send,
        V: DeserializeOwned + Sync + Send + 'a + 'static,
    {
        match self {
            StorageMap::Sled(m) => m.prefix_iter(prefix).await,
            StorageMap::Redis(m) => m.prefix_iter(prefix).await,
        }
    }

    async fn retain<'a, F, Out, V>(&'a self, f: F) -> Result<()>
    where
        F: Fn(Result<(Key, V)>) -> Out + Send + Sync + 'static,
        Out: Future<Output = bool> + Send + 'a,
        V: DeserializeOwned + Sync + Send + 'a,
    {
        match self {
            StorageMap::Sled(m) => m.retain(f).await,
            StorageMap::Redis(m) => m.retain(f).await,
        }
    }

    async fn retain_with_key<'a, F, Out>(&'a self, f: F) -> Result<()>
    where
        F: Fn(Result<Key>) -> Out + Send + Sync + 'static,
        Out: Future<Output = bool> + Send + 'a,
    {
        match self {
            StorageMap::Sled(m) => m.retain_with_key(f).await,
            StorageMap::Redis(m) => m.retain_with_key(f).await,
        }
    }
}

#[derive(Clone)]
pub enum StorageList {
    Sled(SledStorageList),
    Redis(RedisStorageList),
}

impl fmt::Debug for StorageList {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            StorageList::Sled(list) => list.name(),
            StorageList::Redis(list) => list.name(),
        };

        f.debug_tuple(&format!("StorageList({:?})", String::from_utf8_lossy(name)))
            .finish()
    }
}

#[async_trait]
impl List for StorageList {
    fn name(&self) -> &[u8] {
        match self {
            StorageList::Sled(m) => m.name(),
            StorageList::Redis(m) => m.name(),
        }
    }

    async fn push<V>(&self, val: &V) -> Result<()>
    where
        V: Serialize + Sync + Send,
    {
        match self {
            StorageList::Sled(list) => list.push(val).await,
            StorageList::Redis(list) => list.push(val).await,
        }
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
        match self {
            StorageList::Sled(list) => list.push_limit(val, limit, pop_front_if_limited).await,
            StorageList::Redis(list) => list.push_limit(val, limit, pop_front_if_limited).await,
        }
    }

    async fn pop<V>(&self) -> Result<Option<V>>
    where
        V: DeserializeOwned + Sync + Send,
    {
        match self {
            StorageList::Sled(list) => list.pop().await,
            StorageList::Redis(list) => list.pop().await,
        }
    }

    async fn all<V>(&self) -> Result<Vec<V>>
    where
        V: DeserializeOwned + Sync + Send,
    {
        match self {
            StorageList::Sled(list) => list.all().await,
            StorageList::Redis(list) => list.all().await,
        }
    }

    async fn get_index<V>(&self, idx: usize) -> Result<Option<V>>
    where
        V: DeserializeOwned + Sync + Send,
    {
        match self {
            StorageList::Sled(list) => list.get_index(idx).await,
            StorageList::Redis(list) => list.get_index(idx).await,
        }
    }

    async fn len(&self) -> Result<usize> {
        match self {
            StorageList::Sled(list) => list.len().await,
            StorageList::Redis(list) => list.len().await,
        }
    }

    async fn is_empty(&self) -> Result<bool> {
        match self {
            StorageList::Sled(list) => list.is_empty().await,
            StorageList::Redis(list) => list.is_empty().await,
        }
    }

    async fn clear(&self) -> Result<()> {
        match self {
            StorageList::Sled(list) => list.clear().await,
            StorageList::Redis(list) => list.clear().await,
        }
    }

    async fn iter<'a, V>(
        &'a mut self,
    ) -> Result<Box<dyn AsyncIterator<Item = Result<V>> + Send + 'a>>
    where
        V: DeserializeOwned + Sync + Send + 'a + 'static,
    {
        match self {
            StorageList::Sled(list) => list.iter().await,
            StorageList::Redis(list) => list.iter().await,
        }
    }
}
