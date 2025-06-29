use core::fmt;

use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::Serialize;

#[cfg(feature = "redis")]
use crate::storage_redis::{RedisStorageDB, RedisStorageList, RedisStorageMap};
#[cfg(feature = "redis-cluster")]
use crate::storage_redis_cluster::{
    RedisStorageDB as RedisClusterStorageDB, RedisStorageList as RedisClusterStorageList,
    RedisStorageMap as RedisClusterStorageMap,
};
#[cfg(feature = "sled")]
use crate::storage_sled::{SledStorageDB, SledStorageList, SledStorageMap};
use crate::Result;

#[allow(unused_imports)]
use crate::TimestampMillis;

#[allow(unused)]
pub(crate) const SEPARATOR: &[u8] = b"@";
#[allow(unused)]
pub(crate) const KEY_PREFIX: &[u8] = b"__rmqtt@";
#[allow(unused)]
pub(crate) const KEY_PREFIX_LEN: &[u8] = b"__rmqtt_len@";
#[allow(unused)]
pub(crate) const MAP_NAME_PREFIX: &[u8] = b"__rmqtt_map@";
#[allow(unused)]
pub(crate) const LIST_NAME_PREFIX: &[u8] = b"__rmqtt_list@";

pub type Key = Vec<u8>;

pub type IterItem<V> = Result<(Key, V)>;

#[async_trait]
pub trait AsyncIterator {
    type Item;
    async fn next(&mut self) -> Option<Self::Item>;
}

#[cfg(feature = "sled")]
pub trait SplitSubslice {
    fn split_subslice(&self, subslice: &[u8]) -> Option<(&[u8], &[u8])>;
}

#[cfg(feature = "sled")]
impl SplitSubslice for [u8] {
    fn split_subslice(&self, subslice: &[u8]) -> Option<(&[u8], &[u8])> {
        self.windows(subslice.len())
            .position(|window| window == subslice)
            .map(|index| self.split_at(index + subslice.len()))
    }
}

#[async_trait]
#[allow(clippy::len_without_is_empty)]
pub trait StorageDB: Send + Sync {
    type MapType: Map;
    type ListType: List;

    async fn map<N: AsRef<[u8]> + Sync + Send>(
        &self,
        name: N,
        expire: Option<TimestampMillis>,
    ) -> Result<Self::MapType>;

    async fn map_remove<K>(&self, name: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send;

    async fn map_contains_key<K: AsRef<[u8]> + Sync + Send>(&self, key: K) -> Result<bool>;

    async fn list<V: AsRef<[u8]> + Sync + Send>(
        &self,
        name: V,
        expire: Option<TimestampMillis>,
    ) -> Result<Self::ListType>;

    async fn list_remove<K>(&self, name: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send;

    async fn list_contains_key<K: AsRef<[u8]> + Sync + Send>(&self, key: K) -> Result<bool>;

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

    #[cfg(feature = "len")]
    async fn len(&self) -> Result<usize>;

    async fn db_size(&self) -> Result<usize>;

    #[cfg(feature = "ttl")]
    async fn expire_at<K>(&self, key: K, at: TimestampMillis) -> Result<bool>
    where
        K: AsRef<[u8]> + Sync + Send;

    #[cfg(feature = "ttl")]
    async fn expire<K>(&self, key: K, dur: TimestampMillis) -> Result<bool>
    where
        K: AsRef<[u8]> + Sync + Send;

    #[cfg(feature = "ttl")]
    async fn ttl<K>(&self, key: K) -> Result<Option<TimestampMillis>>
    where
        K: AsRef<[u8]> + Sync + Send;

    async fn map_iter<'a>(
        &'a mut self,
    ) -> Result<Box<dyn AsyncIterator<Item = Result<StorageMap>> + Send + 'a>>;

    async fn list_iter<'a>(
        &'a mut self,
    ) -> Result<Box<dyn AsyncIterator<Item = Result<StorageList>> + Send + 'a>>;

    //pattern - * or ?
    async fn scan<'a, P>(
        &'a mut self,
        pattern: P,
    ) -> Result<Box<dyn AsyncIterator<Item = Result<Key>> + Send + 'a>>
    where
        P: AsRef<[u8]> + Send + Sync;

    async fn info(&self) -> Result<serde_json::Value>;
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

    #[cfg(feature = "map_len")]
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
        P: AsRef<[u8]> + Send + Sync,
        V: DeserializeOwned + Sync + Send + 'a + 'static;

    #[cfg(feature = "ttl")]
    async fn expire_at(&self, at: TimestampMillis) -> Result<bool>;

    #[cfg(feature = "ttl")]
    async fn expire(&self, dur: TimestampMillis) -> Result<bool>;

    #[cfg(feature = "ttl")]
    async fn ttl(&self) -> Result<Option<TimestampMillis>>;
}

#[async_trait]
pub trait List: Sync + Send {
    fn name(&self) -> &[u8];

    async fn push<V>(&self, val: &V) -> Result<()>
    where
        V: serde::ser::Serialize + Sync + Send;

    async fn pushs<V>(&self, vals: Vec<V>) -> Result<()>
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

    #[cfg(feature = "ttl")]
    async fn expire_at(&self, at: TimestampMillis) -> Result<bool>;

    #[cfg(feature = "ttl")]
    async fn expire(&self, dur: TimestampMillis) -> Result<bool>;

    #[cfg(feature = "ttl")]
    async fn ttl(&self) -> Result<Option<TimestampMillis>>;
}

#[derive(Clone)]
pub enum DefaultStorageDB {
    #[cfg(feature = "sled")]
    Sled(SledStorageDB),
    #[cfg(feature = "redis")]
    Redis(RedisStorageDB),
    #[cfg(feature = "redis-cluster")]
    RedisCluster(RedisClusterStorageDB),
}

impl DefaultStorageDB {
    #[inline]
    pub async fn map<V: AsRef<[u8]> + Sync + Send>(
        &self,
        name: V,
        expire: Option<TimestampMillis>,
    ) -> Result<StorageMap> {
        Ok(match self {
            #[cfg(feature = "sled")]
            DefaultStorageDB::Sled(db) => StorageMap::Sled(db.map(name, expire).await?),
            #[cfg(feature = "redis")]
            DefaultStorageDB::Redis(db) => StorageMap::Redis(db.map(name, expire).await?),
            #[cfg(feature = "redis-cluster")]
            DefaultStorageDB::RedisCluster(db) => {
                StorageMap::RedisCluster(db.map(name, expire).await?)
            }
        })
    }

    #[inline]
    pub async fn map_remove<K>(&self, name: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        match self {
            #[cfg(feature = "sled")]
            DefaultStorageDB::Sled(db) => db.map_remove(name).await,
            #[cfg(feature = "redis")]
            DefaultStorageDB::Redis(db) => db.map_remove(name).await,
            #[cfg(feature = "redis-cluster")]
            DefaultStorageDB::RedisCluster(db) => db.map_remove(name).await,
        }
    }

    #[inline]
    pub async fn map_contains_key<K: AsRef<[u8]> + Sync + Send>(&self, key: K) -> Result<bool> {
        match self {
            #[cfg(feature = "sled")]
            DefaultStorageDB::Sled(db) => db.map_contains_key(key).await,
            #[cfg(feature = "redis")]
            DefaultStorageDB::Redis(db) => db.map_contains_key(key).await,
            #[cfg(feature = "redis-cluster")]
            DefaultStorageDB::RedisCluster(db) => db.map_contains_key(key).await,
        }
    }

    #[inline]
    pub async fn list<V: AsRef<[u8]> + Sync + Send>(
        &self,
        name: V,
        expire: Option<TimestampMillis>,
    ) -> Result<StorageList> {
        Ok(match self {
            #[cfg(feature = "sled")]
            DefaultStorageDB::Sled(db) => StorageList::Sled(db.list(name, expire).await?),
            #[cfg(feature = "redis")]
            DefaultStorageDB::Redis(db) => StorageList::Redis(db.list(name, expire).await?),
            #[cfg(feature = "redis-cluster")]
            DefaultStorageDB::RedisCluster(db) => {
                StorageList::RedisCluster(db.list(name, expire).await?)
            }
        })
    }

    #[inline]
    pub async fn list_remove<K>(&self, name: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        match self {
            #[cfg(feature = "sled")]
            DefaultStorageDB::Sled(db) => db.list_remove(name).await,
            #[cfg(feature = "redis")]
            DefaultStorageDB::Redis(db) => db.list_remove(name).await,
            #[cfg(feature = "redis-cluster")]
            DefaultStorageDB::RedisCluster(db) => db.list_remove(name).await,
        }
    }

    #[inline]
    pub async fn list_contains_key<K: AsRef<[u8]> + Sync + Send>(&self, key: K) -> Result<bool> {
        match self {
            #[cfg(feature = "sled")]
            DefaultStorageDB::Sled(db) => db.list_contains_key(key).await,
            #[cfg(feature = "redis")]
            DefaultStorageDB::Redis(db) => db.list_contains_key(key).await,
            #[cfg(feature = "redis-cluster")]
            DefaultStorageDB::RedisCluster(db) => db.list_contains_key(key).await,
        }
    }

    #[inline]
    pub async fn insert<K, V>(&self, key: K, val: &V) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: Serialize + Sync + Send,
    {
        match self {
            #[cfg(feature = "sled")]
            DefaultStorageDB::Sled(db) => db.insert(key, val).await,
            #[cfg(feature = "redis")]
            DefaultStorageDB::Redis(db) => db.insert(key, val).await,
            #[cfg(feature = "redis-cluster")]
            DefaultStorageDB::RedisCluster(db) => db.insert(key, val).await,
        }
    }

    #[inline]
    pub async fn get<K, V>(&self, key: K) -> Result<Option<V>>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: DeserializeOwned + Sync + Send,
    {
        match self {
            #[cfg(feature = "sled")]
            DefaultStorageDB::Sled(db) => db.get(key).await,
            #[cfg(feature = "redis")]
            DefaultStorageDB::Redis(db) => db.get(key).await,
            #[cfg(feature = "redis-cluster")]
            DefaultStorageDB::RedisCluster(db) => db.get(key).await,
        }
    }

    #[inline]
    pub async fn remove<K>(&self, key: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        match self {
            #[cfg(feature = "sled")]
            DefaultStorageDB::Sled(db) => db.remove(key).await,
            #[cfg(feature = "redis")]
            DefaultStorageDB::Redis(db) => db.remove(key).await,
            #[cfg(feature = "redis-cluster")]
            DefaultStorageDB::RedisCluster(db) => db.remove(key).await,
        }
    }

    #[inline]
    pub async fn batch_insert<V>(&self, key_vals: Vec<(Key, V)>) -> Result<()>
    where
        V: serde::ser::Serialize + Sync + Send,
    {
        match self {
            #[cfg(feature = "sled")]
            DefaultStorageDB::Sled(db) => db.batch_insert(key_vals).await,
            #[cfg(feature = "redis")]
            DefaultStorageDB::Redis(db) => db.batch_insert(key_vals).await,
            #[cfg(feature = "redis-cluster")]
            DefaultStorageDB::RedisCluster(db) => db.batch_insert(key_vals).await,
        }
    }

    #[inline]
    pub async fn batch_remove(&self, keys: Vec<Key>) -> Result<()> {
        match self {
            #[cfg(feature = "sled")]
            DefaultStorageDB::Sled(db) => db.batch_remove(keys).await,
            #[cfg(feature = "redis")]
            DefaultStorageDB::Redis(db) => db.batch_remove(keys).await,
            #[cfg(feature = "redis-cluster")]
            DefaultStorageDB::RedisCluster(db) => db.batch_remove(keys).await,
        }
    }

    #[inline]
    pub async fn counter_incr<K>(&self, key: K, increment: isize) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        match self {
            #[cfg(feature = "sled")]
            DefaultStorageDB::Sled(db) => db.counter_incr(key, increment).await,
            #[cfg(feature = "redis")]
            DefaultStorageDB::Redis(db) => db.counter_incr(key, increment).await,
            #[cfg(feature = "redis-cluster")]
            DefaultStorageDB::RedisCluster(db) => db.counter_incr(key, increment).await,
        }
    }

    #[inline]
    pub async fn counter_decr<K>(&self, key: K, decrement: isize) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        match self {
            #[cfg(feature = "sled")]
            DefaultStorageDB::Sled(db) => db.counter_decr(key, decrement).await,
            #[cfg(feature = "redis")]
            DefaultStorageDB::Redis(db) => db.counter_decr(key, decrement).await,
            #[cfg(feature = "redis-cluster")]
            DefaultStorageDB::RedisCluster(db) => db.counter_decr(key, decrement).await,
        }
    }

    #[inline]
    pub async fn counter_get<K>(&self, key: K) -> Result<Option<isize>>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        match self {
            #[cfg(feature = "sled")]
            DefaultStorageDB::Sled(db) => db.counter_get(key).await,
            #[cfg(feature = "redis")]
            DefaultStorageDB::Redis(db) => db.counter_get(key).await,
            #[cfg(feature = "redis-cluster")]
            DefaultStorageDB::RedisCluster(db) => db.counter_get(key).await,
        }
    }

    #[inline]
    pub async fn counter_set<K>(&self, key: K, val: isize) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        match self {
            #[cfg(feature = "sled")]
            DefaultStorageDB::Sled(db) => db.counter_set(key, val).await,
            #[cfg(feature = "redis")]
            DefaultStorageDB::Redis(db) => db.counter_set(key, val).await,
            #[cfg(feature = "redis-cluster")]
            DefaultStorageDB::RedisCluster(db) => db.counter_set(key, val).await,
        }
    }

    #[inline]
    #[cfg(feature = "len")]
    pub async fn len(&self) -> Result<usize> {
        match self {
            #[cfg(feature = "sled")]
            DefaultStorageDB::Sled(db) => db.len().await,
            #[cfg(feature = "redis")]
            DefaultStorageDB::Redis(db) => db.len().await,
            #[cfg(feature = "redis-cluster")]
            DefaultStorageDB::RedisCluster(db) => db.len().await,
        }
    }

    #[inline]
    pub async fn db_size(&self) -> Result<usize> {
        match self {
            #[cfg(feature = "sled")]
            DefaultStorageDB::Sled(db) => db.db_size().await,
            #[cfg(feature = "redis")]
            DefaultStorageDB::Redis(db) => db.db_size().await,
            #[cfg(feature = "redis-cluster")]
            DefaultStorageDB::RedisCluster(db) => db.db_size().await,
        }
    }

    #[inline]
    pub async fn contains_key<K: AsRef<[u8]> + Sync + Send>(&self, key: K) -> Result<bool> {
        match self {
            #[cfg(feature = "sled")]
            DefaultStorageDB::Sled(db) => db.contains_key(key).await,
            #[cfg(feature = "redis")]
            DefaultStorageDB::Redis(db) => db.contains_key(key).await,
            #[cfg(feature = "redis-cluster")]
            DefaultStorageDB::RedisCluster(db) => db.contains_key(key).await,
        }
    }

    #[inline]
    #[cfg(feature = "ttl")]
    pub async fn expire_at<K>(&self, key: K, at: TimestampMillis) -> Result<bool>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        match self {
            #[cfg(feature = "sled")]
            DefaultStorageDB::Sled(db) => db.expire_at(key, at).await,
            #[cfg(feature = "redis")]
            DefaultStorageDB::Redis(db) => db.expire_at(key, at).await,
            #[cfg(feature = "redis-cluster")]
            DefaultStorageDB::RedisCluster(db) => db.expire_at(key, at).await,
        }
    }

    #[inline]
    #[cfg(feature = "ttl")]
    pub async fn expire<K>(&self, key: K, dur: TimestampMillis) -> Result<bool>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        match self {
            #[cfg(feature = "sled")]
            DefaultStorageDB::Sled(db) => db.expire(key, dur).await,
            #[cfg(feature = "redis")]
            DefaultStorageDB::Redis(db) => db.expire(key, dur).await,
            #[cfg(feature = "redis-cluster")]
            DefaultStorageDB::RedisCluster(db) => db.expire(key, dur).await,
        }
    }

    #[inline]
    #[cfg(feature = "ttl")]
    pub async fn ttl<K>(&self, key: K) -> Result<Option<TimestampMillis>>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        match self {
            #[cfg(feature = "sled")]
            DefaultStorageDB::Sled(db) => db.ttl(key).await,
            #[cfg(feature = "redis")]
            DefaultStorageDB::Redis(db) => db.ttl(key).await,
            #[cfg(feature = "redis-cluster")]
            DefaultStorageDB::RedisCluster(db) => db.ttl(key).await,
        }
    }

    #[inline]
    pub async fn map_iter<'a>(
        &'a mut self,
    ) -> Result<Box<dyn AsyncIterator<Item = Result<StorageMap>> + Send + 'a>> {
        match self {
            #[cfg(feature = "sled")]
            DefaultStorageDB::Sled(db) => db.map_iter().await,
            #[cfg(feature = "redis")]
            DefaultStorageDB::Redis(db) => db.map_iter().await,
            #[cfg(feature = "redis-cluster")]
            DefaultStorageDB::RedisCluster(db) => db.map_iter().await,
        }
    }

    #[inline]
    pub async fn list_iter<'a>(
        &'a mut self,
    ) -> Result<Box<dyn AsyncIterator<Item = Result<StorageList>> + Send + 'a>> {
        match self {
            #[cfg(feature = "sled")]
            DefaultStorageDB::Sled(db) => db.list_iter().await,
            #[cfg(feature = "redis")]
            DefaultStorageDB::Redis(db) => db.list_iter().await,
            #[cfg(feature = "redis-cluster")]
            DefaultStorageDB::RedisCluster(db) => db.list_iter().await,
        }
    }

    #[inline]
    pub async fn scan<'a, P>(
        &'a mut self,
        pattern: P,
    ) -> Result<Box<dyn AsyncIterator<Item = Result<Key>> + Send + 'a>>
    where
        P: AsRef<[u8]> + Send + Sync,
    {
        match self {
            #[cfg(feature = "sled")]
            DefaultStorageDB::Sled(db) => db.scan(pattern).await,
            #[cfg(feature = "redis")]
            DefaultStorageDB::Redis(db) => db.scan(pattern).await,
            #[cfg(feature = "redis-cluster")]
            DefaultStorageDB::RedisCluster(db) => db.scan(pattern).await,
        }
    }

    #[inline]
    pub async fn info(&self) -> Result<serde_json::Value> {
        match self {
            #[cfg(feature = "sled")]
            DefaultStorageDB::Sled(db) => db.info().await,
            #[cfg(feature = "redis")]
            DefaultStorageDB::Redis(db) => db.info().await,
            #[cfg(feature = "redis-cluster")]
            DefaultStorageDB::RedisCluster(db) => db.info().await,
        }
    }
}

#[derive(Clone)]
pub enum StorageMap {
    #[cfg(feature = "sled")]
    Sled(SledStorageMap),
    #[cfg(feature = "redis")]
    Redis(RedisStorageMap),
    #[cfg(feature = "redis-cluster")]
    RedisCluster(RedisClusterStorageMap),
}

#[async_trait]
impl Map for StorageMap {
    fn name(&self) -> &[u8] {
        match self {
            #[cfg(feature = "sled")]
            StorageMap::Sled(m) => m.name(),
            #[cfg(feature = "redis")]
            StorageMap::Redis(m) => m.name(),
            #[cfg(feature = "redis-cluster")]
            StorageMap::RedisCluster(m) => m.name(),
        }
    }

    async fn insert<K, V>(&self, key: K, val: &V) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: Serialize + Sync + Send + ?Sized,
    {
        match self {
            #[cfg(feature = "sled")]
            StorageMap::Sled(m) => m.insert(key, val).await,
            #[cfg(feature = "redis")]
            StorageMap::Redis(m) => m.insert(key, val).await,
            #[cfg(feature = "redis-cluster")]
            StorageMap::RedisCluster(m) => m.insert(key, val).await,
        }
    }

    async fn get<K, V>(&self, key: K) -> Result<Option<V>>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: DeserializeOwned + Sync + Send,
    {
        match self {
            #[cfg(feature = "sled")]
            StorageMap::Sled(m) => m.get(key).await,
            #[cfg(feature = "redis")]
            StorageMap::Redis(m) => m.get(key).await,
            #[cfg(feature = "redis-cluster")]
            StorageMap::RedisCluster(m) => m.get(key).await,
        }
    }

    async fn remove<K>(&self, key: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        match self {
            #[cfg(feature = "sled")]
            StorageMap::Sled(m) => m.remove(key).await,
            #[cfg(feature = "redis")]
            StorageMap::Redis(m) => m.remove(key).await,
            #[cfg(feature = "redis-cluster")]
            StorageMap::RedisCluster(m) => m.remove(key).await,
        }
    }

    async fn contains_key<K: AsRef<[u8]> + Sync + Send>(&self, key: K) -> Result<bool> {
        match self {
            #[cfg(feature = "sled")]
            StorageMap::Sled(m) => m.contains_key(key).await,
            #[cfg(feature = "redis")]
            StorageMap::Redis(m) => m.contains_key(key).await,
            #[cfg(feature = "redis-cluster")]
            StorageMap::RedisCluster(m) => m.contains_key(key).await,
        }
    }

    #[cfg(feature = "map_len")]
    async fn len(&self) -> Result<usize> {
        match self {
            #[cfg(feature = "sled")]
            StorageMap::Sled(m) => m.len().await,
            #[cfg(feature = "redis")]
            StorageMap::Redis(m) => m.len().await,
            #[cfg(feature = "redis-cluster")]
            StorageMap::RedisCluster(m) => m.len().await,
        }
    }

    async fn is_empty(&self) -> Result<bool> {
        match self {
            #[cfg(feature = "sled")]
            StorageMap::Sled(m) => m.is_empty().await,
            #[cfg(feature = "redis")]
            StorageMap::Redis(m) => m.is_empty().await,
            #[cfg(feature = "redis-cluster")]
            StorageMap::RedisCluster(m) => m.is_empty().await,
        }
    }

    async fn clear(&self) -> Result<()> {
        match self {
            #[cfg(feature = "sled")]
            StorageMap::Sled(m) => m.clear().await,
            #[cfg(feature = "redis")]
            StorageMap::Redis(m) => m.clear().await,
            #[cfg(feature = "redis-cluster")]
            StorageMap::RedisCluster(m) => m.clear().await,
        }
    }

    async fn remove_and_fetch<K, V>(&self, key: K) -> Result<Option<V>>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: DeserializeOwned + Sync + Send,
    {
        match self {
            #[cfg(feature = "sled")]
            StorageMap::Sled(m) => m.remove_and_fetch(key).await,
            #[cfg(feature = "redis")]
            StorageMap::Redis(m) => m.remove_and_fetch(key).await,
            #[cfg(feature = "redis-cluster")]
            StorageMap::RedisCluster(m) => m.remove_and_fetch(key).await,
        }
    }

    async fn remove_with_prefix<K>(&self, prefix: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        match self {
            #[cfg(feature = "sled")]
            StorageMap::Sled(m) => m.remove_with_prefix(prefix).await,
            #[cfg(feature = "redis")]
            StorageMap::Redis(m) => m.remove_with_prefix(prefix).await,
            #[cfg(feature = "redis-cluster")]
            StorageMap::RedisCluster(m) => m.remove_with_prefix(prefix).await,
        }
    }

    async fn batch_insert<V>(&self, key_vals: Vec<(Key, V)>) -> Result<()>
    where
        V: Serialize + Sync + Send,
    {
        match self {
            #[cfg(feature = "sled")]
            StorageMap::Sled(m) => m.batch_insert(key_vals).await,
            #[cfg(feature = "redis")]
            StorageMap::Redis(m) => m.batch_insert(key_vals).await,
            #[cfg(feature = "redis-cluster")]
            StorageMap::RedisCluster(m) => m.batch_insert(key_vals).await,
        }
    }

    async fn batch_remove(&self, keys: Vec<Key>) -> Result<()> {
        match self {
            #[cfg(feature = "sled")]
            StorageMap::Sled(m) => m.batch_remove(keys).await,
            #[cfg(feature = "redis")]
            StorageMap::Redis(m) => m.batch_remove(keys).await,
            #[cfg(feature = "redis-cluster")]
            StorageMap::RedisCluster(m) => m.batch_remove(keys).await,
        }
    }

    async fn iter<'a, V>(
        &'a mut self,
    ) -> Result<Box<dyn AsyncIterator<Item = IterItem<V>> + Send + 'a>>
    where
        V: DeserializeOwned + Sync + Send + 'a + 'static,
    {
        match self {
            #[cfg(feature = "sled")]
            StorageMap::Sled(m) => m.iter().await,
            #[cfg(feature = "redis")]
            StorageMap::Redis(m) => m.iter().await,
            #[cfg(feature = "redis-cluster")]
            StorageMap::RedisCluster(m) => m.iter().await,
        }
    }

    async fn key_iter<'a>(
        &'a mut self,
    ) -> Result<Box<dyn AsyncIterator<Item = Result<Key>> + Send + 'a>> {
        match self {
            #[cfg(feature = "sled")]
            StorageMap::Sled(m) => m.key_iter().await,
            #[cfg(feature = "redis")]
            StorageMap::Redis(m) => m.key_iter().await,
            #[cfg(feature = "redis-cluster")]
            StorageMap::RedisCluster(m) => m.key_iter().await,
        }
    }

    async fn prefix_iter<'a, P, V>(
        &'a mut self,
        prefix: P,
    ) -> Result<Box<dyn AsyncIterator<Item = IterItem<V>> + Send + 'a>>
    where
        P: AsRef<[u8]> + Send + Sync,
        V: DeserializeOwned + Sync + Send + 'a + 'static,
    {
        match self {
            #[cfg(feature = "sled")]
            StorageMap::Sled(m) => m.prefix_iter(prefix).await,
            #[cfg(feature = "redis")]
            StorageMap::Redis(m) => m.prefix_iter(prefix).await,
            #[cfg(feature = "redis-cluster")]
            StorageMap::RedisCluster(m) => m.prefix_iter(prefix).await,
        }
    }

    #[cfg(feature = "ttl")]
    async fn expire_at(&self, at: TimestampMillis) -> Result<bool> {
        match self {
            #[cfg(feature = "sled")]
            StorageMap::Sled(m) => m.expire_at(at).await,
            #[cfg(feature = "redis")]
            StorageMap::Redis(m) => m.expire_at(at).await,
            #[cfg(feature = "redis-cluster")]
            StorageMap::RedisCluster(m) => m.expire_at(at).await,
        }
    }

    #[cfg(feature = "ttl")]
    async fn expire(&self, dur: TimestampMillis) -> Result<bool> {
        match self {
            #[cfg(feature = "sled")]
            StorageMap::Sled(m) => m.expire(dur).await,
            #[cfg(feature = "redis")]
            StorageMap::Redis(m) => m.expire(dur).await,
            #[cfg(feature = "redis-cluster")]
            StorageMap::RedisCluster(m) => m.expire(dur).await,
        }
    }

    #[cfg(feature = "ttl")]
    async fn ttl(&self) -> Result<Option<TimestampMillis>> {
        match self {
            #[cfg(feature = "sled")]
            StorageMap::Sled(m) => m.ttl().await,
            #[cfg(feature = "redis")]
            StorageMap::Redis(m) => m.ttl().await,
            #[cfg(feature = "redis-cluster")]
            StorageMap::RedisCluster(m) => m.ttl().await,
        }
    }
}

#[derive(Clone)]
pub enum StorageList {
    #[cfg(feature = "sled")]
    Sled(SledStorageList),
    #[cfg(feature = "redis")]
    Redis(RedisStorageList),
    #[cfg(feature = "redis-cluster")]
    RedisCluster(RedisClusterStorageList),
}

impl fmt::Debug for StorageList {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            #[cfg(feature = "sled")]
            StorageList::Sled(list) => list.name(),
            #[cfg(feature = "redis")]
            StorageList::Redis(list) => list.name(),
            #[cfg(feature = "redis-cluster")]
            StorageList::RedisCluster(list) => list.name(),
        };

        f.debug_tuple(&format!("StorageList({:?})", String::from_utf8_lossy(name)))
            .finish()
    }
}

#[async_trait]
impl List for StorageList {
    fn name(&self) -> &[u8] {
        match self {
            #[cfg(feature = "sled")]
            StorageList::Sled(m) => m.name(),
            #[cfg(feature = "redis")]
            StorageList::Redis(m) => m.name(),
            #[cfg(feature = "redis-cluster")]
            StorageList::RedisCluster(m) => m.name(),
        }
    }

    async fn push<V>(&self, val: &V) -> Result<()>
    where
        V: Serialize + Sync + Send,
    {
        match self {
            #[cfg(feature = "sled")]
            StorageList::Sled(list) => list.push(val).await,
            #[cfg(feature = "redis")]
            StorageList::Redis(list) => list.push(val).await,
            #[cfg(feature = "redis-cluster")]
            StorageList::RedisCluster(list) => list.push(val).await,
        }
    }

    async fn pushs<V>(&self, vals: Vec<V>) -> Result<()>
    where
        V: serde::ser::Serialize + Sync + Send,
    {
        match self {
            #[cfg(feature = "sled")]
            StorageList::Sled(list) => list.pushs(vals).await,
            #[cfg(feature = "redis")]
            StorageList::Redis(list) => list.pushs(vals).await,
            #[cfg(feature = "redis-cluster")]
            StorageList::RedisCluster(list) => list.pushs(vals).await,
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
            #[cfg(feature = "sled")]
            StorageList::Sled(list) => list.push_limit(val, limit, pop_front_if_limited).await,
            #[cfg(feature = "redis")]
            StorageList::Redis(list) => list.push_limit(val, limit, pop_front_if_limited).await,
            #[cfg(feature = "redis-cluster")]
            StorageList::RedisCluster(list) => {
                list.push_limit(val, limit, pop_front_if_limited).await
            }
        }
    }

    async fn pop<V>(&self) -> Result<Option<V>>
    where
        V: DeserializeOwned + Sync + Send,
    {
        match self {
            #[cfg(feature = "sled")]
            StorageList::Sled(list) => list.pop().await,
            #[cfg(feature = "redis")]
            StorageList::Redis(list) => list.pop().await,
            #[cfg(feature = "redis-cluster")]
            StorageList::RedisCluster(list) => list.pop().await,
        }
    }

    async fn all<V>(&self) -> Result<Vec<V>>
    where
        V: DeserializeOwned + Sync + Send,
    {
        match self {
            #[cfg(feature = "sled")]
            StorageList::Sled(list) => list.all().await,
            #[cfg(feature = "redis")]
            StorageList::Redis(list) => list.all().await,
            #[cfg(feature = "redis-cluster")]
            StorageList::RedisCluster(list) => list.all().await,
        }
    }

    async fn get_index<V>(&self, idx: usize) -> Result<Option<V>>
    where
        V: DeserializeOwned + Sync + Send,
    {
        match self {
            #[cfg(feature = "sled")]
            StorageList::Sled(list) => list.get_index(idx).await,
            #[cfg(feature = "redis")]
            StorageList::Redis(list) => list.get_index(idx).await,
            #[cfg(feature = "redis-cluster")]
            StorageList::RedisCluster(list) => list.get_index(idx).await,
        }
    }

    async fn len(&self) -> Result<usize> {
        match self {
            #[cfg(feature = "sled")]
            StorageList::Sled(list) => list.len().await,
            #[cfg(feature = "redis")]
            StorageList::Redis(list) => list.len().await,
            #[cfg(feature = "redis-cluster")]
            StorageList::RedisCluster(list) => list.len().await,
        }
    }

    async fn is_empty(&self) -> Result<bool> {
        match self {
            #[cfg(feature = "sled")]
            StorageList::Sled(list) => list.is_empty().await,
            #[cfg(feature = "redis")]
            StorageList::Redis(list) => list.is_empty().await,
            #[cfg(feature = "redis-cluster")]
            StorageList::RedisCluster(list) => list.is_empty().await,
        }
    }

    async fn clear(&self) -> Result<()> {
        match self {
            #[cfg(feature = "sled")]
            StorageList::Sled(list) => list.clear().await,
            #[cfg(feature = "redis")]
            StorageList::Redis(list) => list.clear().await,
            #[cfg(feature = "redis-cluster")]
            StorageList::RedisCluster(list) => list.clear().await,
        }
    }

    async fn iter<'a, V>(
        &'a mut self,
    ) -> Result<Box<dyn AsyncIterator<Item = Result<V>> + Send + 'a>>
    where
        V: DeserializeOwned + Sync + Send + 'a + 'static,
    {
        match self {
            #[cfg(feature = "sled")]
            StorageList::Sled(list) => list.iter().await,
            #[cfg(feature = "redis")]
            StorageList::Redis(list) => list.iter().await,
            #[cfg(feature = "redis-cluster")]
            StorageList::RedisCluster(list) => list.iter().await,
        }
    }

    #[cfg(feature = "ttl")]
    async fn expire_at(&self, at: TimestampMillis) -> Result<bool> {
        match self {
            #[cfg(feature = "sled")]
            StorageList::Sled(l) => l.expire_at(at).await,
            #[cfg(feature = "redis")]
            StorageList::Redis(l) => l.expire_at(at).await,
            #[cfg(feature = "redis-cluster")]
            StorageList::RedisCluster(l) => l.expire_at(at).await,
        }
    }

    #[cfg(feature = "ttl")]
    async fn expire(&self, dur: TimestampMillis) -> Result<bool> {
        match self {
            #[cfg(feature = "sled")]
            StorageList::Sled(l) => l.expire(dur).await,
            #[cfg(feature = "redis")]
            StorageList::Redis(l) => l.expire(dur).await,
            #[cfg(feature = "redis-cluster")]
            StorageList::RedisCluster(l) => l.expire(dur).await,
        }
    }

    #[cfg(feature = "ttl")]
    async fn ttl(&self) -> Result<Option<TimestampMillis>> {
        match self {
            #[cfg(feature = "sled")]
            StorageList::Sled(l) => l.ttl().await,
            #[cfg(feature = "redis")]
            StorageList::Redis(l) => l.ttl().await,
            #[cfg(feature = "redis-cluster")]
            StorageList::RedisCluster(l) => l.ttl().await,
        }
    }
}
