use core::fmt;
use std::fmt::Debug;
use std::future::Future;

use crate::storage_redis::{RedisStorageDB, RedisStorageList, RedisStorageMap};
use crate::storage_sled::{SledStorageList, SledStorageMap};
use crate::TimestampMillis;
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::Serialize;

use super::storage_sled::SledStorageDB;
use super::Result;

pub type Key = Vec<u8>;
pub type IsList = bool;

#[derive(Debug)]
pub enum Value<V> {
    Val(V),
    List(StorageList),
}

//pub type IterItem<'a, V> = BoxFuture<'a, Result<(Key, Value<V>)>>;
//pub type IterItem<'a, V> = Result<(Key, Value<V>)>;
pub type IterItem<'a, V> = Result<(Key, V)>;

#[async_trait]
pub(crate) trait AsyncIterator {
    type Item;
    async fn next(&mut self) -> Option<Self::Item>;
}

#[async_trait]
pub trait StorageDB: Send + Sync {
    type MapType: Map;

    fn map<V: AsRef<[u8]>>(&mut self, name: V) -> Result<Self::MapType>;

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

    async fn contains_key<K: AsRef<[u8]> + Sync + Send>(&mut self, key: K) -> Result<bool>;

    async fn expire_at<K>(&mut self, key: K, dur: TimestampMillis) -> Result<bool>
    where
        K: AsRef<[u8]> + Sync + Send;

    async fn expire<K>(&mut self, key: K, dur: TimestampMillis) -> Result<bool>
    where
        K: AsRef<[u8]> + Sync + Send;

    async fn ttl<K>(&mut self, key: K) -> Result<Option<TimestampMillis>>
    where
        K: AsRef<[u8]> + Sync + Send;
}

#[async_trait]
pub trait Map: Sync + Send {
    type ListType: List;

    fn list<V: AsRef<[u8]>>(&self, name: V) -> Result<Self::ListType>;

    async fn list_iter<'a>(
        &'a mut self,
    ) -> Result<Box<dyn Iterator<Item = Result<(Key, StorageList)>> + 'a>>;

    async fn list_key_iter<'a>(&'a mut self) -> Result<Box<dyn Iterator<Item = Result<Key>> + 'a>>;

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

    async fn clear(&mut self) -> Result<()>;

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

    async fn iter<'a, V>(&'a mut self) -> Result<Box<dyn Iterator<Item = IterItem<'a, V>> + 'a>>
    where
        V: DeserializeOwned + Sync + Send + 'a;

    async fn key_iter<'a>(&'a mut self) -> Result<Box<dyn Iterator<Item = Result<Key>> + 'a>>;

    async fn prefix_iter<'a, P, V>(
        &'a mut self,
        prefix: P,
    ) -> Result<Box<dyn Iterator<Item = IterItem<'a, V>> + 'a>>
    where
        P: AsRef<[u8]> + Send,
        V: DeserializeOwned + Sync + Send + 'a;

    async fn retain<'a, F, Out, V>(&'a mut self, f: F) -> Result<()>
    where
        F: Fn(Result<(Key, Value<V>)>) -> Out + Send + Sync,
        Out: Future<Output = bool> + Send + 'a,
        V: DeserializeOwned + Sync + Send + 'a;

    async fn retain_with_key<'a, F, Out>(&'a mut self, f: F) -> Result<()>
    where
        F: Fn(Result<(Key, IsList)>) -> Out + Send + Sync,
        Out: Future<Output = bool> + Send + 'a;
}

#[async_trait]
pub trait List: Sync + Send {
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

    fn iter<'a, V>(&'a self) -> Result<Box<dyn Iterator<Item = Result<V>> + 'a>>
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
    pub fn map<V: AsRef<[u8]>>(&mut self, name: V) -> Result<StorageMap> {
        match self {
            DefaultStorageDB::Sled(db) => {
                let s = db.map(name)?;
                Ok(StorageMap::Sled(s))
            }
            DefaultStorageDB::Redis(db) => {
                let s = db.map(name)?;
                Ok(StorageMap::Redis(s))
            }
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
    pub async fn contains_key<K: AsRef<[u8]> + Sync + Send>(&mut self, key: K) -> Result<bool> {
        match self {
            DefaultStorageDB::Sled(db) => db.contains_key(key).await,
            DefaultStorageDB::Redis(db) => db.contains_key(key).await,
        }
    }

    #[inline]
    pub async fn expire_at<K>(&mut self, key: K, dur: TimestampMillis) -> Result<bool>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        match self {
            DefaultStorageDB::Sled(db) => db.expire_at(key, dur).await,
            DefaultStorageDB::Redis(db) => db.expire_at(key, dur).await,
        }
    }

    #[inline]
    pub async fn expire<K>(&mut self, key: K, dur: TimestampMillis) -> Result<bool>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        match self {
            DefaultStorageDB::Sled(db) => db.expire(key, dur).await,
            DefaultStorageDB::Redis(db) => db.expire(key, dur).await,
        }
    }

    #[inline]
    pub async fn ttl<K>(&mut self, key: K) -> Result<Option<TimestampMillis>>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        match self {
            DefaultStorageDB::Sled(db) => db.ttl(key).await,
            DefaultStorageDB::Redis(db) => db.ttl(key).await,
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
    type ListType = StorageList;
    fn list<V: AsRef<[u8]>>(&self, name: V) -> Result<Self::ListType> {
        Ok(match self {
            StorageMap::Sled(map) => StorageList::Sled(map.list(name)?),
            StorageMap::Redis(map) => StorageList::Redis(map.list(name)?),
        })
    }

    async fn list_iter<'a>(
        &'a mut self,
    ) -> Result<Box<dyn Iterator<Item = Result<(Key, StorageList)>> + 'a>> {
        match self {
            StorageMap::Sled(tree) => tree.list_iter().await,
            StorageMap::Redis(tree) => tree.list_iter().await,
        }
    }

    async fn list_key_iter<'a>(&'a mut self) -> Result<Box<dyn Iterator<Item = Result<Key>> + 'a>> {
        match self {
            StorageMap::Sled(tree) => tree.list_key_iter().await,
            StorageMap::Redis(tree) => tree.list_key_iter().await,
        }
    }

    async fn insert<K, V>(&self, key: K, val: &V) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: Serialize + Sync + Send + ?Sized,
    {
        match self {
            StorageMap::Sled(tree) => tree.insert(key, val).await,
            StorageMap::Redis(tree) => tree.insert(key, val).await,
        }
    }

    async fn get<K, V>(&self, key: K) -> Result<Option<V>>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: DeserializeOwned + Sync + Send,
    {
        match self {
            StorageMap::Sled(tree) => tree.get(key).await,
            StorageMap::Redis(tree) => tree.get(key).await,
        }
    }

    async fn remove<K>(&self, key: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        match self {
            StorageMap::Sled(tree) => tree.remove(key).await,
            StorageMap::Redis(tree) => tree.remove(key).await,
        }
    }

    async fn contains_key<K: AsRef<[u8]> + Sync + Send>(&self, key: K) -> Result<bool> {
        match self {
            StorageMap::Sled(tree) => tree.contains_key(key).await,
            StorageMap::Redis(tree) => tree.contains_key(key).await,
        }
    }

    async fn len(&self) -> Result<usize> {
        match self {
            StorageMap::Sled(tree) => tree.len().await,
            StorageMap::Redis(tree) => tree.len().await,
        }
    }

    async fn is_empty(&self) -> Result<bool> {
        match self {
            StorageMap::Sled(tree) => tree.is_empty().await,
            StorageMap::Redis(tree) => tree.is_empty().await,
        }
    }

    async fn clear(&mut self) -> Result<()> {
        match self {
            StorageMap::Sled(tree) => tree.clear().await,
            StorageMap::Redis(tree) => tree.clear().await,
        }
    }

    async fn remove_and_fetch<K, V>(&self, key: K) -> Result<Option<V>>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: DeserializeOwned + Sync + Send,
    {
        match self {
            StorageMap::Sled(tree) => tree.remove_and_fetch(key).await,
            StorageMap::Redis(tree) => tree.remove_and_fetch(key).await,
        }
    }

    async fn remove_with_prefix<K>(&self, prefix: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        match self {
            StorageMap::Sled(tree) => tree.remove_with_prefix(prefix).await,
            StorageMap::Redis(tree) => tree.remove_with_prefix(prefix).await,
        }
    }

    async fn batch_insert<V>(&self, key_vals: Vec<(Key, V)>) -> Result<()>
    where
        V: Serialize + Sync + Send,
    {
        match self {
            StorageMap::Sled(tree) => tree.batch_insert(key_vals).await,
            StorageMap::Redis(tree) => tree.batch_insert(key_vals).await,
        }
    }

    async fn batch_remove(&self, keys: Vec<Key>) -> Result<()> {
        match self {
            StorageMap::Sled(tree) => tree.batch_remove(keys).await,
            StorageMap::Redis(tree) => tree.batch_remove(keys).await,
        }
    }

    async fn iter<'a, V>(&'a mut self) -> Result<Box<dyn Iterator<Item = IterItem<'a, V>> + 'a>>
    where
        V: DeserializeOwned + Sync + Send + 'a,
    {
        match self {
            StorageMap::Sled(tree) => tree.iter().await,
            StorageMap::Redis(tree) => tree.iter().await,
        }
    }

    async fn key_iter<'a>(&'a mut self) -> Result<Box<dyn Iterator<Item = Result<Key>> + 'a>> {
        match self {
            StorageMap::Sled(tree) => tree.key_iter().await,
            StorageMap::Redis(tree) => tree.key_iter().await,
        }
    }

    async fn prefix_iter<'a, P, V>(
        &'a mut self,
        prefix: P,
    ) -> Result<Box<dyn Iterator<Item = IterItem<'a, V>> + 'a>>
    where
        P: AsRef<[u8]> + Send,
        V: DeserializeOwned + Sync + Send + 'a,
    {
        match self {
            StorageMap::Sled(tree) => tree.prefix_iter(prefix).await,
            StorageMap::Redis(tree) => tree.prefix_iter(prefix).await,
        }
    }

    async fn retain<'a, F, Out, V>(&'a mut self, f: F) -> Result<()>
    where
        F: Fn(Result<(Key, Value<V>)>) -> Out + Send + Sync,
        Out: Future<Output = bool> + Send + 'a,
        V: DeserializeOwned + Sync + Send + 'a,
    {
        match self {
            StorageMap::Sled(tree) => tree.retain(f).await,
            StorageMap::Redis(tree) => tree.retain(f).await,
        }
    }

    async fn retain_with_key<'a, F, Out>(&'a mut self, f: F) -> Result<()>
    where
        F: Fn(Result<(Key, IsList)>) -> Out + Send + Sync,
        Out: Future<Output = bool> + Send + 'a,
    {
        match self {
            StorageMap::Sled(tree) => tree.retain_with_key(f).await,
            StorageMap::Redis(tree) => tree.retain_with_key(f).await,
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

    async fn pop<V>(&self) -> Result<Option<V>>
    where
        V: DeserializeOwned + Sync + Send,
    {
        match self {
            StorageList::Sled(list) => list.pop().await,
            StorageList::Redis(list) => list.pop().await,
        }
    }

    fn iter<'a, V>(&'a self) -> Result<Box<dyn Iterator<Item = Result<V>> + 'a>>
    where
        V: DeserializeOwned + Sync + Send + 'a + 'static,
    {
        match self {
            StorageList::Sled(list) => list.iter(),
            StorageList::Redis(list) => list.iter(),
        }
    }
}
