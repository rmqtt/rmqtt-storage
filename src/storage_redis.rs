//! Redis storage implementation for key-value, map, and list data structures
//!
//! This module provides a Redis-backed storage system with support for:
//! - Key-value storage with expiration
//! - Map (hash) data structures
//! - List data structures
//! - Counters with atomic operations
//! - Iteration and scanning capabilities
//! - Standalone Redis connection support

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use async_trait::async_trait;
use redis::aio::{ConnectionManager, ConnectionManagerConfig};
use redis::{pipe, AsyncCommands};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

use crate::storage::{AsyncIterator, IterItem, Key, List, Map, StorageDB};
use crate::{Result, StorageList, StorageMap};

#[allow(unused_imports)]
use crate::{timestamp_millis, TimestampMillis};

use crate::storage::{KEY_PREFIX, KEY_PREFIX_LEN, LIST_NAME_PREFIX, MAP_NAME_PREFIX, SEPARATOR};

/// Type alias for Redis connection manager
type RedisConnection = ConnectionManager;

/// Configuration for Redis storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedisConfig {
    /// Redis server URL
    pub url: String,
    /// Key prefix for all storage operations
    pub prefix: String,
}

impl Default for RedisConfig {
    fn default() -> Self {
        RedisConfig {
            url: String::default(),
            prefix: "__def".into(),
        }
    }
}

/// Redis storage database implementation
#[derive(Clone)]
pub struct RedisStorageDB {
    /// Prefix for all keys
    prefix: Key,
    /// Asynchronous connection manager
    async_conn: RedisConnection,
}

impl RedisStorageDB {
    /// Creates a new Redis storage instance
    #[inline]
    pub(crate) async fn new(cfg: RedisConfig) -> Result<Self> {
        let prefix = [cfg.prefix.as_bytes(), SEPARATOR].concat();
        let client = match redis::Client::open(cfg.url.as_str()) {
            Ok(c) => c,
            Err(e) => {
                log::error!("open redis error, config is {:?}, {:?}", cfg, e);
                return Err(anyhow!(e));
            }
        };

        // Configure connection manager with retry settings
        let mgr_cfg = ConnectionManagerConfig::default()
            .set_exponent_base(100)
            .set_factor(2)
            .set_number_of_retries(2)
            .set_connection_timeout(Duration::from_secs(15))
            .set_response_timeout(Duration::from_secs(10));

        // Create connection manager
        let async_conn = match client.get_connection_manager_with_config(mgr_cfg).await {
            Ok(conn) => conn,
            Err(e) => {
                log::error!("get redis connection error, config is {:?}, {:?}", cfg, e);
                return Err(anyhow!(e));
            }
        };

        // Create database instance and start cleanup task
        let db = Self { prefix, async_conn }.cleanup();
        Ok(db)
    }

    /// Starts background cleanup task
    fn cleanup(self) -> Self {
        let db = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                let mut async_conn = db.async_conn();
                let db_zkey = db.make_len_sortedset_key();
                if let Err(e) = async_conn
                    .zrembyscore::<'_, _, _, _, ()>(db_zkey.as_slice(), 0, timestamp_millis())
                    .await
                {
                    log::error!("{:?}", e);
                }
            }
        });
        self
    }

    /// Gets a clone of the async connection
    #[inline]
    fn async_conn(&self) -> RedisConnection {
        self.async_conn.clone()
    }

    /// Gets a mutable reference to the async connection
    #[inline]
    fn async_conn_mut(&mut self) -> &mut RedisConnection {
        &mut self.async_conn
    }

    /// Creates key for length tracking sorted set
    #[inline]
    #[allow(dead_code)]
    fn make_len_sortedset_key(&self) -> Key {
        [KEY_PREFIX_LEN, self.prefix.as_slice()].concat()
    }

    /// Creates full key with prefix
    #[inline]
    fn make_full_key<K>(&self, key: K) -> Key
    where
        K: AsRef<[u8]>,
    {
        [KEY_PREFIX, self.prefix.as_slice(), key.as_ref()].concat()
    }

    /// Creates scan pattern with prefix
    #[inline]
    fn make_scan_pattern_match<P: AsRef<[u8]>>(&self, pattern: P) -> Key {
        [KEY_PREFIX, self.prefix.as_slice(), pattern.as_ref()].concat()
    }

    /// Creates full map name with prefix
    #[inline]
    fn make_map_full_name<K>(&self, name: K) -> Key
    where
        K: AsRef<[u8]>,
    {
        [MAP_NAME_PREFIX, self.prefix.as_slice(), name.as_ref()].concat()
    }

    /// Creates full list name with prefix
    #[inline]
    fn make_list_full_name<K>(&self, name: K) -> Key
    where
        K: AsRef<[u8]>,
    {
        [LIST_NAME_PREFIX, self.prefix.as_slice(), name.as_ref()].concat()
    }

    /// Creates map prefix pattern for scanning
    #[inline]
    fn make_map_prefix_match(&self) -> Key {
        [MAP_NAME_PREFIX, self.prefix.as_slice(), b"*"].concat()
    }

    /// Creates list prefix pattern for scanning
    #[inline]
    fn make_list_prefix_match(&self) -> Key {
        [LIST_NAME_PREFIX, self.prefix.as_slice(), b"*"].concat()
    }

    /// Extracts map key from full name
    #[inline]
    fn map_full_name_to_key<'a>(&self, full_name: &'a [u8]) -> &'a [u8] {
        full_name[MAP_NAME_PREFIX.len() + self.prefix.len()..].as_ref()
    }

    /// Extracts list key from full name
    #[inline]
    fn list_full_name_to_key<'a>(&self, full_name: &'a [u8]) -> &'a [u8] {
        full_name[LIST_NAME_PREFIX.len() + self.prefix.len()..].as_ref()
    }

    /// Gets full key name for a given key
    #[inline]
    async fn _get_full_name(&self, key: &[u8]) -> Result<Key> {
        let map_full_name = self.make_map_full_name(key);
        let mut async_conn = self.async_conn();
        let full_name = if async_conn.exists(map_full_name.as_slice()).await? {
            map_full_name
        } else {
            let list_full_name = self.make_list_full_name(key);
            if async_conn.exists(list_full_name.as_slice()).await? {
                list_full_name
            } else {
                self.make_full_key(key)
            }
        };
        Ok(full_name)
    }

    /// Internal method to insert a key-value pair
    #[inline]
    async fn _insert<K, V>(
        &self,
        key: K,
        val: &V,
        expire_interval: Option<TimestampMillis>,
    ) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: serde::ser::Serialize + Sync + Send,
    {
        let full_key = self.make_full_key(key.as_ref());

        #[cfg(not(feature = "len"))]
        {
            if let Some(expire_interval) = expire_interval {
                let mut async_conn = self.async_conn();
                pipe()
                    .atomic()
                    .set(full_key.as_slice(), bincode::serialize(val)?)
                    .pexpire(full_key.as_slice(), expire_interval)
                    .query_async::<()>(&mut async_conn)
                    .await?;
            } else {
                let _: () = self
                    .async_conn()
                    .set(full_key, bincode::serialize(val)?)
                    .await?;
            }
        }
        #[cfg(feature = "len")]
        {
            let db_zkey = self.make_len_sortedset_key();
            let mut async_conn = self.async_conn();
            if let Some(expire_interval) = expire_interval {
                pipe()
                    .atomic()
                    .set(full_key.as_slice(), bincode::serialize(val)?)
                    .pexpire(full_key.as_slice(), expire_interval)
                    .zadd(db_zkey, key.as_ref(), timestamp_millis() + expire_interval)
                    .query_async::<()>(&mut async_conn)
                    .await?;
            } else {
                pipe()
                    .atomic()
                    .set(full_key.as_slice(), bincode::serialize(val)?)
                    .zadd(db_zkey, key.as_ref(), i64::MAX)
                    .query_async::<()>(&mut async_conn)
                    .await?;
            }
        }

        Ok(())
    }

    /// Internal method for batch insertion
    #[inline]
    async fn _batch_insert(
        &self,
        key_val_expires: Vec<(Key, Vec<u8>, Option<TimestampMillis>)>,
    ) -> Result<()> {
        #[cfg(not(feature = "len"))]
        {
            let keys_vals: Vec<(Key, &Vec<u8>)> = key_val_expires
                .iter()
                .map(|(key_ref, value, _)| (self.make_full_key(key_ref), value))
                .collect();

            let mut async_conn = self.async_conn();
            let mut p = pipe();
            let mut rpipe = p.atomic().mset(keys_vals.as_slice());
            for (k, _, at) in key_val_expires {
                if let Some(at) = at {
                    rpipe = rpipe.expire(k, at);
                }
            }
            rpipe.query_async::<()>(&mut async_conn).await?;
        }

        #[cfg(feature = "len")]
        {
            let (full_key_vals, expire_keys): (Vec<_>, Vec<_>) = key_val_expires
                .iter()
                .map(|(key_ref, value, timestamp)| {
                    let full_key_vals = (self.make_full_key(key_ref), value);
                    let expire_keys = (
                        timestamp
                            .map(|t| timestamp_millis() + t)
                            .unwrap_or(i64::MAX),
                        key_ref,
                    );
                    (full_key_vals, expire_keys)
                })
                .unzip();

            let db_zkey = self.make_len_sortedset_key();
            let mut async_conn = self.async_conn();
            let mut p = pipe();
            let mut rpipe = p
                .atomic()
                .mset(full_key_vals.as_slice())
                .zadd_multiple(db_zkey, expire_keys.as_slice());
            for (k, _, at) in key_val_expires {
                if let Some(at) = at {
                    rpipe = rpipe.expire(k, at);
                }
            }
            rpipe.query_async::<((), ())>(&mut async_conn).await?;
        }
        Ok(())
    }

    /// Internal method for batch removal
    #[inline]
    async fn _batch_remove(&self, keys: Vec<Key>) -> Result<()> {
        let full_keys = keys
            .iter()
            .map(|k| self.make_full_key(k))
            .collect::<Vec<_>>();
        #[cfg(not(feature = "len"))]
        {
            let _: () = self.async_conn().del(full_keys).await?;
        }
        #[cfg(feature = "len")]
        {
            let db_zkey = self.make_len_sortedset_key();
            let mut async_conn = self.async_conn();
            pipe()
                .atomic()
                .del(full_keys.as_slice())
                .zrem(db_zkey, keys)
                .query_async::<()>(&mut async_conn)
                .await?;
        }
        Ok(())
    }

    /// Internal method to increment a counter
    #[inline]
    async fn _counter_incr<K>(
        &self,
        key: K,
        increment: isize,
        expire_interval: Option<TimestampMillis>,
    ) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let full_key = self.make_full_key(key.as_ref());
        #[cfg(not(feature = "len"))]
        {
            if let Some(expire_interval) = expire_interval {
                let mut async_conn = self.async_conn();
                pipe()
                    .atomic()
                    .incr(full_key.as_slice(), increment)
                    .pexpire(full_key.as_slice(), expire_interval)
                    .query_async::<()>(&mut async_conn)
                    .await?;
            } else {
                let _: () = self.async_conn().incr(full_key, increment).await?;
            }
        }
        #[cfg(feature = "len")]
        {
            let db_zkey = self.make_len_sortedset_key();
            let mut async_conn = self.async_conn();
            if let Some(expire_interval) = expire_interval {
                pipe()
                    .atomic()
                    .incr(full_key.as_slice(), increment)
                    .pexpire(full_key.as_slice(), expire_interval)
                    .zadd(db_zkey, key.as_ref(), timestamp_millis() + expire_interval)
                    .query_async::<()>(&mut async_conn)
                    .await?;
            } else {
                pipe()
                    .atomic()
                    .incr(full_key.as_slice(), increment)
                    .zadd(db_zkey, key.as_ref(), i64::MAX)
                    .query_async::<()>(&mut async_conn)
                    .await?;
            }
        }
        Ok(())
    }

    /// Internal method to decrement a counter
    #[inline]
    async fn _counter_decr<K>(
        &self,
        key: K,
        decrement: isize,
        expire_interval: Option<TimestampMillis>,
    ) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let full_key = self.make_full_key(key.as_ref());

        #[cfg(not(feature = "len"))]
        {
            if let Some(expire_interval) = expire_interval {
                let mut async_conn = self.async_conn();
                pipe()
                    .atomic()
                    .decr(full_key.as_slice(), decrement)
                    .pexpire(full_key.as_slice(), expire_interval)
                    .query_async::<()>(&mut async_conn)
                    .await?;
            } else {
                let _: () = self.async_conn().decr(full_key, decrement).await?;
            }
        }
        #[cfg(feature = "len")]
        {
            let db_zkey = self.make_len_sortedset_key();
            let mut async_conn = self.async_conn();
            if let Some(expire_interval) = expire_interval {
                pipe()
                    .atomic()
                    .decr(full_key.as_slice(), decrement)
                    .pexpire(full_key.as_slice(), expire_interval)
                    .zadd(db_zkey, key.as_ref(), timestamp_millis() + expire_interval)
                    .query_async::<()>(&mut async_conn)
                    .await?;
            } else {
                pipe()
                    .atomic()
                    .decr(full_key.as_slice(), decrement)
                    .zadd(db_zkey, key.as_ref(), i64::MAX)
                    .query_async::<()>(&mut async_conn)
                    .await?;
            }
        }
        Ok(())
    }

    /// Internal method to set a counter value
    #[inline]
    async fn _counter_set<K>(
        &self,
        key: K,
        val: isize,
        expire_interval: Option<TimestampMillis>,
    ) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let full_key = self.make_full_key(key.as_ref());
        #[cfg(not(feature = "len"))]
        {
            if let Some(expire_interval) = expire_interval {
                let mut async_conn = self.async_conn();
                pipe()
                    .atomic()
                    .set(full_key.as_slice(), val)
                    .pexpire(full_key.as_slice(), expire_interval)
                    .query_async::<()>(&mut async_conn)
                    .await?;
            } else {
                let _: () = self.async_conn().set(full_key, val).await?;
            }
        }
        #[cfg(feature = "len")]
        {
            let db_zkey = self.make_len_sortedset_key();
            let mut async_conn = self.async_conn();
            if let Some(expire_interval) = expire_interval {
                pipe()
                    .atomic()
                    .set(full_key.as_slice(), val)
                    .pexpire(full_key.as_slice(), expire_interval)
                    .zadd(db_zkey, key.as_ref(), timestamp_millis() + expire_interval)
                    .query_async::<()>(&mut async_conn)
                    .await?;
            } else {
                pipe()
                    .atomic()
                    .set(full_key.as_slice(), val)
                    .zadd(db_zkey, key.as_ref(), i64::MAX)
                    .query_async::<()>(&mut async_conn)
                    .await?;
            }
        }

        Ok(())
    }

    /// Internal method to remove a key
    #[inline]
    async fn _remove<K>(&self, key: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let full_key = self.make_full_key(key.as_ref());

        #[cfg(not(feature = "len"))]
        {
            let _: () = self.async_conn().del(full_key).await?;
        }
        #[cfg(feature = "len")]
        {
            let db_zkey = self.make_len_sortedset_key();
            let mut async_conn = self.async_conn();
            pipe()
                .atomic()
                .del(full_key.as_slice())
                .zrem(db_zkey, key.as_ref())
                .query_async::<()>(&mut async_conn)
                .await?;
        }
        Ok(())
    }
}

#[async_trait]
impl StorageDB for RedisStorageDB {
    type MapType = RedisStorageMap;
    type ListType = RedisStorageList;

    /// Creates a new map with optional expiration
    #[inline]
    async fn map<V: AsRef<[u8]> + Sync + Send>(
        &self,
        name: V,
        expire: Option<TimestampMillis>,
    ) -> Result<Self::MapType> {
        let full_name = self.make_map_full_name(name.as_ref());
        Ok(
            RedisStorageMap::new_expire(name.as_ref().to_vec(), full_name, expire, self.clone())
                .await?,
        )
    }

    /// Removes a map
    #[inline]
    async fn map_remove<K>(&self, name: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let map_full_name = self.make_map_full_name(name.as_ref());
        let _: () = self.async_conn().del(map_full_name).await?;
        Ok(())
    }

    /// Checks if a map exists
    #[inline]
    async fn map_contains_key<K: AsRef<[u8]> + Sync + Send>(&self, key: K) -> Result<bool> {
        let map_full_name = self.make_map_full_name(key.as_ref());
        Ok(self.async_conn().exists(map_full_name).await?)
    }

    /// Creates a new list with optional expiration
    #[inline]
    async fn list<V: AsRef<[u8]> + Sync + Send>(
        &self,
        name: V,
        expire: Option<TimestampMillis>,
    ) -> Result<Self::ListType> {
        let full_name = self.make_list_full_name(name.as_ref());
        Ok(
            RedisStorageList::new_expire(name.as_ref().to_vec(), full_name, expire, self.clone())
                .await?,
        )
    }

    /// Removes a list
    #[inline]
    async fn list_remove<K>(&self, name: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let list_full_name = self.make_list_full_name(name.as_ref());
        let _: () = self.async_conn().del(list_full_name).await?;
        Ok(())
    }

    /// Checks if a list exists
    #[inline]
    async fn list_contains_key<K: AsRef<[u8]> + Sync + Send>(&self, key: K) -> Result<bool> {
        let list_full_name = self.make_list_full_name(key.as_ref());
        Ok(self.async_conn().exists(list_full_name).await?)
    }

    /// Inserts a key-value pair
    #[inline]
    async fn insert<K, V>(&self, key: K, val: &V) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: serde::ser::Serialize + Sync + Send,
    {
        self._insert(key, val, None).await
    }

    /// Gets a value by key
    #[inline]
    async fn get<K, V>(&self, key: K) -> Result<Option<V>>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: DeserializeOwned + Sync + Send,
    {
        let full_key = self.make_full_key(key);
        if let Some(v) = self
            .async_conn()
            .get::<_, Option<Vec<u8>>>(full_key)
            .await?
        {
            Ok(Some(bincode::deserialize::<V>(v.as_ref())?))
        } else {
            Ok(None)
        }
    }

    /// Removes a key
    #[inline]
    async fn remove<K>(&self, key: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        self._remove(key).await
    }

    /// Batch insertion of key-value pairs
    #[inline]
    async fn batch_insert<V>(&self, key_vals: Vec<(Key, V)>) -> Result<()>
    where
        V: Serialize + Sync + Send,
    {
        if !key_vals.is_empty() {
            let keys_vals_expires = key_vals
                .into_iter()
                .map(|(k, v)| {
                    bincode::serialize(&v)
                        .map(move |v| (k, v, None))
                        .map_err(|e| anyhow!(e))
                })
                .collect::<Result<Vec<_>>>()?;
            self._batch_insert(keys_vals_expires).await?;
        }
        Ok(())
    }

    /// Batch removal of keys
    #[inline]
    async fn batch_remove(&self, keys: Vec<Key>) -> Result<()> {
        if !keys.is_empty() {
            self._batch_remove(keys).await?;
        }
        Ok(())
    }

    /// Increments a counter
    #[inline]
    async fn counter_incr<K>(&self, key: K, increment: isize) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        self._counter_incr(key, increment, None).await
    }

    /// Decrements a counter
    #[inline]
    async fn counter_decr<K>(&self, key: K, decrement: isize) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        self._counter_decr(key, decrement, None).await
    }

    /// Gets a counter value
    #[inline]
    async fn counter_get<K>(&self, key: K) -> Result<Option<isize>>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let full_key = self.make_full_key(key);
        Ok(self.async_conn().get::<_, Option<isize>>(full_key).await?)
    }

    /// Sets a counter value
    #[inline]
    async fn counter_set<K>(&self, key: K, val: isize) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        self._counter_set(key, val, None).await
    }

    /// Checks if a key exists
    #[inline]
    async fn contains_key<K: AsRef<[u8]> + Sync + Send>(&self, key: K) -> Result<bool> {
        let full_key = self.make_full_key(key.as_ref());
        Ok(self.async_conn().exists(full_key).await?)
    }

    /// Gets the number of keys in the database
    #[inline]
    #[cfg(feature = "len")]
    async fn len(&self) -> Result<usize> {
        let db_zkey = self.make_len_sortedset_key();
        let mut async_conn = self.async_conn();
        let (_, count) = pipe()
            .zrembyscore(db_zkey.as_slice(), 0, timestamp_millis())
            .zcard(db_zkey.as_slice())
            .query_async::<(i64, usize)>(&mut async_conn)
            .await?;
        Ok(count)
    }

    /// Gets the total database size
    #[inline]
    async fn db_size(&self) -> Result<usize> {
        let mut async_conn = self.async_conn();
        //DBSIZE
        let dbsize = redis::pipe()
            .cmd("DBSIZE")
            .query_async::<redis::Value>(&mut async_conn)
            .await?;
        let dbsize = dbsize.as_sequence().and_then(|vs| {
            vs.iter().next().and_then(|v| {
                if let redis::Value::Int(v) = v {
                    Some(*v)
                } else {
                    None
                }
            })
        });
        Ok(dbsize.unwrap_or(0) as usize)
    }

    /// Sets expiration time for a key
    #[inline]
    #[cfg(feature = "ttl")]
    async fn expire_at<K>(&self, key: K, at: TimestampMillis) -> Result<bool>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let full_name = self.make_full_key(key.as_ref());
        #[cfg(not(feature = "len"))]
        {
            let res = self
                .async_conn()
                .pexpire_at::<_, bool>(full_name, at)
                .await?;
            Ok(res)
        }
        #[cfg(feature = "len")]
        {
            let db_zkey = self.make_len_sortedset_key();
            let mut async_conn = self.async_conn();
            let (_, res) = pipe()
                .atomic()
                .zadd(db_zkey, key.as_ref(), at)
                .pexpire_at(full_name.as_slice(), at)
                .query_async::<(i64, bool)>(&mut async_conn)
                .await?;
            Ok(res)
        }
    }

    /// Sets expiration duration for a key
    #[inline]
    #[cfg(feature = "ttl")]
    async fn expire<K>(&self, key: K, dur: TimestampMillis) -> Result<bool>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let full_name = self.make_full_key(key.as_ref());

        #[cfg(not(feature = "len"))]
        {
            let res = self.async_conn().pexpire::<_, bool>(full_name, dur).await?;
            Ok(res)
        }
        #[cfg(feature = "len")]
        {
            let db_zkey = self.make_len_sortedset_key();
            let mut async_conn = self.async_conn();
            let (_, res) = pipe()
                .atomic()
                .zadd(db_zkey, key.as_ref(), timestamp_millis() + dur)
                .pexpire(full_name.as_slice(), dur)
                .query_async::<(i64, bool)>(&mut async_conn)
                .await?;
            Ok(res)
        }
    }

    /// Gets time-to-live for a key
    #[inline]
    #[cfg(feature = "ttl")]
    async fn ttl<K>(&self, key: K) -> Result<Option<TimestampMillis>>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let mut async_conn = self.async_conn();
        let full_key = self.make_full_key(key.as_ref());
        let res = async_conn.pttl::<_, isize>(full_key).await?;
        match res {
            -2 => Ok(None),
            -1 => Ok(Some(TimestampMillis::MAX)),
            _ => Ok(Some(res as TimestampMillis)),
        }
    }

    /// Creates an iterator for all maps
    #[inline]
    async fn map_iter<'a>(
        &'a mut self,
    ) -> Result<Box<dyn AsyncIterator<Item = Result<StorageMap>> + Send + 'a>> {
        let pattern = self.make_map_prefix_match();
        let iter = AsyncMapIter {
            db: self.clone(),
            iter: self.async_conn_mut().scan_match::<_, Key>(pattern).await?,
        };
        Ok(Box::new(iter))
    }

    /// Creates an iterator for all lists
    #[inline]
    async fn list_iter<'a>(
        &'a mut self,
    ) -> Result<Box<dyn AsyncIterator<Item = Result<StorageList>> + Send + 'a>> {
        let pattern = self.make_list_prefix_match();
        let iter = AsyncListIter {
            db: self.clone(),
            iter: self.async_conn_mut().scan_match::<_, Key>(pattern).await?,
        };
        Ok(Box::new(iter))
    }

    /// Creates an iterator for keys matching a pattern
    async fn scan<'a, P>(
        &'a mut self,
        pattern: P,
    ) -> Result<Box<dyn AsyncIterator<Item = Result<Key>> + Send + 'a>>
    where
        P: AsRef<[u8]> + Send + Sync,
    {
        let pattern = self.make_scan_pattern_match(pattern);
        let prefix_len = KEY_PREFIX.len() + self.prefix.len();
        let iter = AsyncDbKeyIter {
            prefix_len,
            iter: self
                .async_conn_mut()
                .scan_match::<_, Key>(pattern.as_slice())
                .await?,
        };
        Ok(Box::new(iter))
    }

    /// Gets database information
    #[inline]
    async fn info(&self) -> Result<Value> {
        let mut conn = self.async_conn();
        let dbsize = redis::pipe()
            .cmd("dbsize")
            .query_async::<redis::Value>(&mut conn)
            .await?;
        let dbsize = dbsize.as_sequence().and_then(|vs| {
            vs.iter().next().and_then(|v| {
                if let redis::Value::Int(v) = v {
                    Some(*v)
                } else {
                    None
                }
            })
        });
        Ok(serde_json::json!({
            "storage_engine": "Redis",
            "dbsize": dbsize,
        }))
    }
}

/// Redis-backed map storage implementation
#[derive(Clone)]
pub struct RedisStorageMap {
    /// Name of the map
    name: Key,
    /// Full key name with prefix
    full_name: Key,
    /// Optional expiration time in milliseconds
    #[allow(dead_code)]
    expire: Option<TimestampMillis>,
    /// Flag indicating if the map is empty
    empty: Arc<AtomicBool>,
    /// Reference to the parent database
    pub(crate) db: RedisStorageDB,
}

impl RedisStorageMap {
    /// Creates a new map without expiration
    #[inline]
    pub(crate) fn new(name: Key, full_name: Key, db: RedisStorageDB) -> Self {
        Self {
            name,
            full_name,
            expire: None,
            empty: Arc::new(AtomicBool::new(true)),
            db,
        }
    }

    /// Creates a new map with expiration
    #[inline]
    pub(crate) async fn new_expire(
        name: Key,
        full_name: Key,
        expire: Option<TimestampMillis>,
        mut db: RedisStorageDB,
    ) -> Result<Self> {
        let empty = if expire.is_some() {
            let empty = Self::_is_empty(&mut db.async_conn, full_name.as_slice()).await?;
            Arc::new(AtomicBool::new(empty))
        } else {
            Arc::new(AtomicBool::new(true))
        };
        Ok(Self {
            name,
            full_name,
            expire,
            empty,
            db,
        })
    }

    /// Gets a clone of the async connection
    #[inline]
    fn async_conn(&self) -> RedisConnection {
        self.db.async_conn()
    }

    /// Gets a mutable reference to the async connection
    #[inline]
    fn async_conn_mut(&mut self) -> &mut RedisConnection {
        self.db.async_conn_mut()
    }

    /// Checks if the map is empty
    #[inline]
    async fn _is_empty(async_conn: &mut RedisConnection, full_name: &[u8]) -> Result<bool> {
        let res = async_conn
            .hscan::<_, Vec<u8>>(full_name)
            .await?
            .next_item()
            .await
            .is_none();
        Ok(res)
    }

    /// Internal method to insert with expiration handling
    #[inline]
    async fn _insert_expire(&self, key: &[u8], val: Vec<u8>) -> Result<()> {
        let mut async_conn = self.async_conn();
        let name = self.full_name.as_slice();

        #[cfg(feature = "ttl")]
        if self.empty.load(Ordering::SeqCst) {
            if let Some(expire) = self.expire.as_ref() {
                let _: () = redis::pipe()
                    .atomic()
                    .hset(name, key, val)
                    .pexpire(name, *expire)
                    .query_async(&mut async_conn)
                    .await?;
                self.empty.store(false, Ordering::SeqCst);
                return Ok(());
            }
        }

        let _: () = async_conn.hset(name, key.as_ref(), val).await?;
        Ok(())
    }

    /// Internal method for batch insertion with expiration
    #[inline]
    async fn _batch_insert_expire(&self, key_vals: Vec<(Key, Vec<u8>)>) -> Result<()> {
        let mut async_conn = self.async_conn();
        let name = self.full_name.as_slice();

        #[cfg(feature = "ttl")]
        if self.empty.load(Ordering::SeqCst) {
            if let Some(expire) = self.expire.as_ref() {
                let _: () = redis::pipe()
                    .atomic()
                    .hset_multiple(name, key_vals.as_slice())
                    .pexpire(name, *expire)
                    .query_async(&mut async_conn)
                    .await?;

                self.empty.store(false, Ordering::SeqCst);
                return Ok(());
            }
        }

        let _: () = async_conn.hset_multiple(name, key_vals.as_slice()).await?;
        Ok(())
    }
}

#[async_trait]
impl Map for RedisStorageMap {
    /// Gets the map name
    #[inline]
    fn name(&self) -> &[u8] {
        self.name.as_slice()
    }

    /// Inserts a key-value pair into the map
    #[inline]
    async fn insert<K, V>(&self, key: K, val: &V) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: Serialize + Sync + Send + ?Sized,
    {
        self._insert_expire(key.as_ref(), bincode::serialize(val)?)
            .await
    }

    /// Gets a value from the map
    #[inline]
    async fn get<K, V>(&self, key: K) -> Result<Option<V>>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: DeserializeOwned + Sync + Send,
    {
        let res: Option<Vec<u8>> = self
            .async_conn()
            .hget(self.full_name.as_slice(), key.as_ref())
            .await?;
        if let Some(res) = res {
            Ok(Some(bincode::deserialize::<V>(res.as_ref())?))
        } else {
            Ok(None)
        }
    }

    /// Removes a key from the map
    #[inline]
    async fn remove<K>(&self, key: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let _: () = self
            .async_conn()
            .hdel(self.full_name.as_slice(), key.as_ref())
            .await?;
        Ok(())
    }

    /// Checks if a key exists in the map
    #[inline]
    async fn contains_key<K: AsRef<[u8]> + Sync + Send>(&self, key: K) -> Result<bool> {
        let res = self
            .async_conn()
            .hexists(self.full_name.as_slice(), key.as_ref())
            .await?;
        Ok(res)
    }

    /// Gets the number of elements in the map
    #[cfg(feature = "map_len")]
    #[inline]
    async fn len(&self) -> Result<usize> {
        Ok(self.async_conn().hlen(self.full_name.as_slice()).await?)
    }

    /// Checks if the map is empty
    #[inline]
    async fn is_empty(&self) -> Result<bool> {
        let res = self
            .async_conn()
            .hscan::<_, Vec<u8>>(self.full_name.as_slice())
            .await?
            .next_item()
            .await
            .is_none();
        Ok(res)
    }

    /// Clears all elements from the map
    #[inline]
    async fn clear(&self) -> Result<()> {
        let _: () = self.async_conn().del(self.full_name.as_slice()).await?;
        self.empty.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// Removes and returns a value from the map
    #[inline]
    async fn remove_and_fetch<K, V>(&self, key: K) -> Result<Option<V>>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: DeserializeOwned + Sync + Send,
    {
        let name = self.full_name.as_slice();
        let mut conn = self.async_conn();
        let (res, _): (Option<Vec<u8>>, isize) = redis::pipe()
            .atomic()
            .hget(name, key.as_ref())
            .hdel(name, key.as_ref())
            .query_async(&mut conn)
            .await?;

        if let Some(res) = res {
            Ok(Some(bincode::deserialize::<V>(res.as_ref())?))
        } else {
            Ok(None)
        }
    }

    /// Removes all keys with a given prefix
    #[inline]
    async fn remove_with_prefix<K>(&self, prefix: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let name = self.full_name.as_slice();
        let mut conn = self.async_conn();
        let mut conn2 = conn.clone();
        let mut prefix = prefix.as_ref().to_vec();
        prefix.push(b'*');
        let mut removeds = Vec::new();
        while let Some(key) = conn
            .hscan_match::<_, _, Vec<u8>>(name, prefix.as_slice())
            .await?
            .next_item()
            .await
        {
            removeds.push(key?);
            if removeds.len() > 20 {
                let _: () = conn2.hdel(name, removeds.as_slice()).await?;
                removeds.clear();
            }
        }
        if !removeds.is_empty() {
            let _: () = conn.hdel(name, removeds).await?;
        }
        Ok(())
    }

    /// Batch insertion of key-value pairs
    #[inline]
    async fn batch_insert<V>(&self, key_vals: Vec<(Key, V)>) -> Result<()>
    where
        V: Serialize + Sync + Send,
    {
        if !key_vals.is_empty() {
            let key_vals = key_vals
                .into_iter()
                .map(|(k, v)| {
                    bincode::serialize(&v)
                        .map(move |v| (k, v))
                        .map_err(|e| anyhow!(e))
                })
                .collect::<Result<Vec<_>>>()?;

            self._batch_insert_expire(key_vals).await?;
        }
        Ok(())
    }

    /// Batch removal of keys
    #[inline]
    async fn batch_remove(&self, keys: Vec<Key>) -> Result<()> {
        if !keys.is_empty() {
            let _: () = self
                .async_conn()
                .hdel(self.full_name.as_slice(), keys)
                .await?;
        }
        Ok(())
    }

    /// Creates an iterator over key-value pairs
    #[inline]
    async fn iter<'a, V>(
        &'a mut self,
    ) -> Result<Box<dyn AsyncIterator<Item = IterItem<V>> + Send + 'a>>
    where
        V: DeserializeOwned + Sync + Send + 'a + 'static,
    {
        let name = self.full_name.clone();
        let iter = AsyncIter {
            iter: self
                .async_conn_mut()
                .hscan::<_, (Key, Vec<u8>)>(name)
                .await?,
            _m: std::marker::PhantomData,
        };
        Ok(Box::new(iter))
    }

    /// Creates an iterator over keys
    #[inline]
    async fn key_iter<'a>(
        &'a mut self,
    ) -> Result<Box<dyn AsyncIterator<Item = Result<Key>> + Send + 'a>> {
        let iter = AsyncKeyIter {
            iter: self
                .db
                .async_conn
                .hscan::<_, (Key, ())>(self.full_name.as_slice())
                .await?,
        };
        Ok(Box::new(iter))
    }

    /// Creates an iterator over key-value pairs with a prefix
    #[inline]
    async fn prefix_iter<'a, P, V>(
        &'a mut self,
        prefix: P,
    ) -> Result<Box<dyn AsyncIterator<Item = IterItem<V>> + Send + 'a>>
    where
        P: AsRef<[u8]> + Send + Sync,
        V: DeserializeOwned + Sync + Send + 'a + 'static,
    {
        let name = self.full_name.clone();
        let mut prefix = prefix.as_ref().to_vec();
        prefix.push(b'*');
        let iter = AsyncIter {
            iter: self
                .async_conn_mut()
                .hscan_match::<_, _, (Key, Vec<u8>)>(name, prefix.as_slice())
                .await?,
            _m: std::marker::PhantomData,
        };
        Ok(Box::new(iter))
    }

    /// Sets expiration time for the map
    #[cfg(feature = "ttl")]
    async fn expire_at(&self, at: TimestampMillis) -> Result<bool> {
        let res = self
            .async_conn()
            .pexpire_at::<_, bool>(self.full_name.as_slice(), at)
            .await?;
        Ok(res)
    }

    /// Sets expiration duration for the map
    #[cfg(feature = "ttl")]
    async fn expire(&self, dur: TimestampMillis) -> Result<bool> {
        let res = self
            .async_conn()
            .pexpire::<_, bool>(self.full_name.as_slice(), dur)
            .await?;
        Ok(res)
    }

    /// Gets time-to-live for the map
    #[cfg(feature = "ttl")]
    async fn ttl(&self) -> Result<Option<TimestampMillis>> {
        let mut async_conn = self.async_conn();
        let res = async_conn
            .pttl::<_, isize>(self.full_name.as_slice())
            .await?;
        match res {
            -2 => Ok(None),
            -1 => Ok(Some(TimestampMillis::MAX)),
            _ => Ok(Some(res as TimestampMillis)),
        }
    }
}

/// Redis-backed list storage implementation
#[derive(Clone)]
pub struct RedisStorageList {
    /// Name of the list
    name: Key,
    /// Full key name with prefix
    full_name: Key,
    /// Optional expiration time in milliseconds
    #[allow(dead_code)]
    expire: Option<TimestampMillis>,
    /// Flag indicating if the list is empty
    empty: Arc<AtomicBool>,
    /// Reference to the parent database
    pub(crate) db: RedisStorageDB,
}

impl RedisStorageList {
    /// Creates a new list without expiration
    #[inline]
    pub(crate) fn new(name: Key, full_name: Key, db: RedisStorageDB) -> Self {
        Self {
            name,
            full_name,
            expire: None,
            empty: Arc::new(AtomicBool::new(true)),
            db,
        }
    }

    /// Creates a new list with expiration
    #[inline]
    pub(crate) async fn new_expire(
        name: Key,
        full_name: Key,
        expire: Option<TimestampMillis>,
        mut db: RedisStorageDB,
    ) -> Result<Self> {
        let empty = if expire.is_some() {
            let empty = Self::_is_empty(&mut db.async_conn, full_name.as_slice()).await?;
            Arc::new(AtomicBool::new(empty))
        } else {
            Arc::new(AtomicBool::new(true))
        };
        Ok(Self {
            name,
            full_name,
            expire,
            empty,
            db,
        })
    }

    /// Gets a clone of the async connection
    #[inline]
    pub(crate) fn async_conn(&self) -> RedisConnection {
        self.db.async_conn()
    }

    /// Checks if the list is empty
    #[inline]
    async fn _is_empty(async_conn: &mut RedisConnection, full_name: &[u8]) -> Result<bool> {
        Ok(async_conn.llen::<_, usize>(full_name).await? == 0)
    }

    /// Internal method to push with expiration handling
    #[inline]
    async fn _push_expire(&self, val: Vec<u8>) -> Result<()> {
        let mut async_conn = self.async_conn();
        let name = self.full_name.as_slice();

        #[cfg(feature = "ttl")]
        if self.empty.load(Ordering::SeqCst) {
            if let Some(expire) = self.expire.as_ref() {
                let _: () = redis::pipe()
                    .atomic()
                    .rpush(name, val)
                    .pexpire(name, *expire)
                    .query_async(&mut async_conn)
                    .await?;
                self.empty.store(false, Ordering::SeqCst);
                return Ok(());
            }
        }

        let _: () = async_conn.rpush(name, val).await?;
        Ok(())
    }

    /// Internal method for batch push with expiration
    #[inline]
    async fn _pushs_expire(&self, vals: Vec<Vec<u8>>) -> Result<()> {
        let mut async_conn = self.async_conn();

        #[cfg(feature = "ttl")]
        if self.empty.load(Ordering::SeqCst) {
            if let Some(expire) = self.expire.as_ref() {
                let name = self.full_name.as_slice();
                let _: () = redis::pipe()
                    .atomic()
                    .rpush(name, vals)
                    .pexpire(name, *expire)
                    .query_async(&mut async_conn)
                    .await?;
                self.empty.store(false, Ordering::SeqCst);
                return Ok(());
            }
        }

        let _: () = async_conn.rpush(self.full_name.as_slice(), vals).await?;
        Ok(())
    }

    /// Internal method for push with limit and expiration
    #[inline]
    async fn _push_limit_expire(
        &self,
        val: Vec<u8>,
        limit: usize,
        pop_front_if_limited: bool,
    ) -> Result<Option<Vec<u8>>> {
        let mut conn = self.async_conn();

        #[cfg(feature = "ttl")]
        if self.empty.load(Ordering::SeqCst) {
            if let Some(expire) = self.expire.as_ref() {
                let name = self.full_name.as_slice();
                let count = conn.llen::<_, usize>(name).await?;
                let res = if count < limit {
                    let _: () = redis::pipe()
                        .atomic()
                        .rpush(name, val)
                        .pexpire(name, *expire)
                        .query_async(&mut conn)
                        .await?;
                    Ok(None)
                } else if pop_front_if_limited {
                    let (poped, _): (Option<Vec<u8>>, Option<()>) = redis::pipe()
                        .atomic()
                        .lpop(name, None)
                        .rpush(name, val)
                        .pexpire(name, *expire)
                        .query_async(&mut conn)
                        .await?;

                    Ok(poped)
                } else {
                    Err(anyhow::Error::msg("Is full"))
                };
                self.empty.store(false, Ordering::SeqCst);
                return res;
            }
        }

        self._push_limit(val, limit, pop_front_if_limited, &mut conn)
            .await
    }

    /// Internal method for push with limit
    #[inline]
    async fn _push_limit(
        &self,
        val: Vec<u8>,
        limit: usize,
        pop_front_if_limited: bool,
        async_conn: &mut RedisConnection,
    ) -> Result<Option<Vec<u8>>> {
        let name = self.full_name.as_slice();

        let count = async_conn.llen::<_, usize>(name).await?;
        if count < limit {
            let _: () = async_conn.rpush(name, val).await?;
            Ok(None)
        } else if pop_front_if_limited {
            let (poped, _): (Option<Vec<u8>>, Option<()>) = redis::pipe()
                .atomic()
                .lpop(name, None)
                .rpush(name, val)
                .query_async(async_conn)
                .await?;
            Ok(poped)
        } else {
            Err(anyhow::Error::msg("Is full"))
        }
    }
}

#[async_trait]
impl List for RedisStorageList {
    /// Gets the list name
    #[inline]
    fn name(&self) -> &[u8] {
        self.name.as_slice()
    }

    /// Pushes a value to the end of the list
    #[inline]
    async fn push<V>(&self, val: &V) -> Result<()>
    where
        V: Serialize + Sync + Send,
    {
        self._push_expire(bincode::serialize(val)?).await
    }

    /// Pushes multiple values to the end of the list
    #[inline]
    async fn pushs<V>(&self, vals: Vec<V>) -> Result<()>
    where
        V: Serialize + Sync + Send,
    {
        let vals = vals
            .into_iter()
            .map(|v| bincode::serialize(&v).map_err(|e| anyhow!(e)))
            .collect::<Result<Vec<_>>>()?;
        self._pushs_expire(vals).await
    }

    /// Pushes a value with size limit handling
    #[inline]
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
        let data = bincode::serialize(val)?;

        if let Some(res) = self
            ._push_limit_expire(data, limit, pop_front_if_limited)
            .await?
        {
            Ok(Some(
                bincode::deserialize::<V>(res.as_ref()).map_err(|e| anyhow!(e))?,
            ))
        } else {
            Ok(None)
        }
    }

    /// Pops a value from the front of the list
    #[inline]
    async fn pop<V>(&self) -> Result<Option<V>>
    where
        V: DeserializeOwned + Sync + Send,
    {
        let removed = self
            .async_conn()
            .lpop::<_, Option<Vec<u8>>>(self.full_name.as_slice(), None)
            .await?;

        let removed = if let Some(v) = removed {
            Some(bincode::deserialize::<V>(v.as_ref()).map_err(|e| anyhow!(e))?)
        } else {
            None
        };

        Ok(removed)
    }

    /// Gets all values in the list
    #[inline]
    async fn all<V>(&self) -> Result<Vec<V>>
    where
        V: DeserializeOwned + Sync + Send,
    {
        let all = self
            .async_conn()
            .lrange::<_, Vec<Vec<u8>>>(self.full_name.as_slice(), 0, -1)
            .await?;
        all.iter()
            .map(|v| bincode::deserialize::<V>(v.as_ref()).map_err(|e| anyhow!(e)))
            .collect::<Result<Vec<_>>>()
    }

    /// Gets a value by index
    #[inline]
    async fn get_index<V>(&self, idx: usize) -> Result<Option<V>>
    where
        V: DeserializeOwned + Sync + Send,
    {
        let val = self
            .async_conn()
            .lindex::<_, Option<Vec<u8>>>(self.full_name.as_slice(), idx as isize)
            .await?;

        Ok(if let Some(v) = val {
            Some(bincode::deserialize::<V>(v.as_ref()).map_err(|e| anyhow!(e))?)
        } else {
            None
        })
    }

    /// Gets the length of the list
    #[inline]
    async fn len(&self) -> Result<usize> {
        Ok(self.async_conn().llen(self.full_name.as_slice()).await?)
    }

    /// Checks if the list is empty
    #[inline]
    async fn is_empty(&self) -> Result<bool> {
        Ok(self.len().await? == 0)
    }

    /// Clears the list
    #[inline]
    async fn clear(&self) -> Result<()> {
        let _: () = self.async_conn().del(self.full_name.as_slice()).await?;
        self.empty.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// Creates an iterator over list values
    #[inline]
    async fn iter<'a, V>(
        &'a mut self,
    ) -> Result<Box<dyn AsyncIterator<Item = Result<V>> + Send + 'a>>
    where
        V: DeserializeOwned + Sync + Send + 'a + 'static,
    {
        Ok(Box::new(AsyncListValIter::new(
            self.full_name.as_slice(),
            self.db.async_conn(),
        )))
    }

    /// Sets expiration time for the list
    #[cfg(feature = "ttl")]
    async fn expire_at(&self, at: TimestampMillis) -> Result<bool> {
        let res = self
            .async_conn()
            .pexpire_at::<_, bool>(self.full_name.as_slice(), at)
            .await?;
        Ok(res)
    }

    /// Sets expiration duration for the list
    #[cfg(feature = "ttl")]
    async fn expire(&self, dur: TimestampMillis) -> Result<bool> {
        let res = self
            .async_conn()
            .pexpire::<_, bool>(self.full_name.as_slice(), dur)
            .await?;
        Ok(res)
    }

    /// Gets time-to-live for the list
    #[cfg(feature = "ttl")]
    async fn ttl(&self) -> Result<Option<TimestampMillis>> {
        let mut async_conn = self.async_conn();
        let res = async_conn
            .pttl::<_, isize>(self.full_name.as_slice())
            .await?;
        match res {
            -2 => Ok(None),
            -1 => Ok(Some(TimestampMillis::MAX)),
            _ => Ok(Some(res as TimestampMillis)),
        }
    }
}

/// Iterator for list values
pub struct AsyncListValIter<'a, V> {
    name: &'a [u8],
    conn: RedisConnection,
    start: isize,
    limit: isize,
    catch_vals: Vec<Vec<u8>>,
    _m: std::marker::PhantomData<V>,
}

impl<'a, V> AsyncListValIter<'a, V> {
    /// Creates a new list value iterator
    fn new(name: &'a [u8], conn: RedisConnection) -> Self {
        let start = 0;
        let limit = 20;
        Self {
            name,
            conn,
            start,
            limit,
            catch_vals: Vec::with_capacity((limit + 1) as usize),
            _m: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<V> AsyncIterator for AsyncListValIter<'_, V>
where
    V: DeserializeOwned + Sync + Send + 'static,
{
    type Item = Result<V>;

    async fn next(&mut self) -> Option<Self::Item> {
        if let Some(val) = self.catch_vals.pop() {
            return Some(bincode::deserialize::<V>(val.as_ref()).map_err(|e| anyhow!(e)));
        }

        let vals = self
            .conn
            .lrange::<_, Vec<Vec<u8>>>(self.name, self.start, self.start + self.limit)
            .await;

        match vals {
            Err(e) => return Some(Err(anyhow!(e))),
            Ok(vals) => {
                if vals.is_empty() {
                    return None;
                }
                self.start += vals.len() as isize;
                self.catch_vals = vals;
                self.catch_vals.reverse();
            }
        }

        self.catch_vals
            .pop()
            .map(|val| bincode::deserialize::<V>(val.as_ref()).map_err(|e| anyhow!(e)))
    }
}

/// Iterator for map entries
pub struct AsyncIter<'a, V> {
    iter: redis::AsyncIter<'a, (Key, Vec<u8>)>,
    _m: std::marker::PhantomData<V>,
}

#[async_trait]
impl<'a, V> AsyncIterator for AsyncIter<'a, V>
where
    V: DeserializeOwned + Sync + Send + 'a,
{
    type Item = IterItem<V>;

    async fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next_item().await {
            None => None,
            Some(Err(e)) => Some(Err(anyhow::Error::new(e))),
            Some(Ok((key, v))) => match bincode::deserialize::<V>(v.as_ref()) {
                Ok(v) => Some(Ok((key, v))),
                Err(e) => Some(Err(anyhow::Error::new(e))),
            },
        }
    }
}

/// Iterator for database keys
pub struct AsyncDbKeyIter<'a> {
    prefix_len: usize,
    iter: redis::AsyncIter<'a, Key>,
}

#[async_trait]
impl AsyncIterator for AsyncDbKeyIter<'_> {
    type Item = Result<Key>;

    async fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next_item().await {
            None => None,
            Some(Err(e)) => Some(Err(anyhow::Error::new(e))),
            Some(Ok(key)) => Some(Ok(key[self.prefix_len..].to_vec())),
        }
    }
}

/// Iterator for map keys
pub struct AsyncKeyIter<'a> {
    iter: redis::AsyncIter<'a, (Key, ())>,
}

#[async_trait]
impl AsyncIterator for AsyncKeyIter<'_> {
    type Item = Result<Key>;

    async fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next_item().await {
            None => None,
            Some(Err(e)) => Some(Err(anyhow::Error::new(e))),
            Some(Ok((key, _))) => Some(Ok(key)),
        }
    }
}

/// Iterator for maps
pub struct AsyncMapIter<'a> {
    db: RedisStorageDB,
    iter: redis::AsyncIter<'a, Key>,
}

#[async_trait]
impl AsyncIterator for AsyncMapIter<'_> {
    type Item = Result<StorageMap>;

    async fn next(&mut self) -> Option<Self::Item> {
        let full_name = match self.iter.next_item().await {
            None => return None,
            Some(Err(e)) => return Some(Err(anyhow::Error::new(e))),
            Some(Ok(key)) => key,
        };

        let name = self.db.map_full_name_to_key(full_name.as_slice()).to_vec();
        let m = RedisStorageMap::new(name, full_name, self.db.clone());
        Some(Ok(StorageMap::Redis(m)))
    }
}

/// Iterator for lists
pub struct AsyncListIter<'a> {
    db: RedisStorageDB,
    iter: redis::AsyncIter<'a, Key>,
}

#[async_trait]
impl AsyncIterator for AsyncListIter<'_> {
    type Item = Result<StorageList>;

    async fn next(&mut self) -> Option<Self::Item> {
        let full_name = match self.iter.next_item().await {
            None => return None,
            Some(Err(e)) => return Some(Err(anyhow::Error::new(e))),
            Some(Ok(key)) => key,
        };

        let name = self.db.list_full_name_to_key(full_name.as_slice()).to_vec();
        let l = RedisStorageList::new(name, full_name, self.db.clone());
        Some(Ok(StorageList::Redis(l)))
    }
}
