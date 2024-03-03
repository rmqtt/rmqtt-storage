use anyhow::anyhow;
use async_trait::async_trait;
use redis::aio::ConnectionManager;
use redis::{pipe, AsyncCommands};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::storage::{AsyncIterator, IterItem, Key, List, Map, StorageDB};
use crate::{Result, StorageList, StorageMap};

#[allow(unused_imports)]
use crate::{timestamp_millis, TimestampMillis};

const SEPARATOR: &[u8] = b"@";
const KEY_PREFIX: &[u8] = b"__rmqtt@";
const KEY_PREFIX_LEN: &[u8] = b"__rmqtt_len@";
const MAP_NAME_PREFIX: &[u8] = b"__rmqtt_map@";
const LIST_NAME_PREFIX: &[u8] = b"__rmqtt_list@";

type RedisConnection = ConnectionManager;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedisConfig {
    pub url: String,
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

#[derive(Clone)]
pub struct RedisStorageDB {
    prefix: Key,
    async_conn: RedisConnection,
}

impl RedisStorageDB {
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
        let async_conn = match client
            .get_connection_manager_with_backoff(
                2, 100,
                2,
                // Duration::from_secs(5),
                // Duration::from_secs(8),
            )
            .await
        {
            Ok(conn) => conn,
            Err(e) => {
                log::error!("get redis connection error, config is {:?}, {:?}", cfg, e);
                return Err(anyhow!(e));
            }
        };
        let db = Self { prefix, async_conn }.cleanup();
        Ok(db)
    }

    fn cleanup(self) -> Self {
        let db = self.clone();
        std::thread::spawn(move || loop {
            std::thread::sleep(std::time::Duration::from_secs(30));
            let mut async_conn = db.async_conn();
            let db_zkey = db.make_len_sortedset_key();
            futures::executor::block_on(async move {
                if let Err(e) = async_conn
                    .zrembyscore::<'_, _, _, _, ()>(db_zkey.as_slice(), 0, timestamp_millis())
                    .await
                {
                    log::error!("{:?}", e);
                }
            });
        });
        self
    }

    #[inline]
    fn async_conn(&self) -> RedisConnection {
        self.async_conn.clone()
    }

    #[inline]
    fn async_conn_mut(&mut self) -> &mut RedisConnection {
        &mut self.async_conn
    }

    #[inline]
    #[allow(dead_code)]
    fn make_len_sortedset_key(&self) -> Key {
        [KEY_PREFIX_LEN, self.prefix.as_slice()].concat()
    }

    #[inline]
    fn make_full_key<K>(&self, key: K) -> Key
    where
        K: AsRef<[u8]>,
    {
        [KEY_PREFIX, self.prefix.as_slice(), key.as_ref()].concat()
    }

    #[inline]
    fn make_scan_pattern_match<P: AsRef<[u8]>>(&self, pattern: P) -> Key {
        [KEY_PREFIX, self.prefix.as_slice(), pattern.as_ref()].concat()
    }

    #[inline]
    fn make_map_full_name<K>(&self, name: K) -> Key
    where
        K: AsRef<[u8]>,
    {
        [MAP_NAME_PREFIX, self.prefix.as_slice(), name.as_ref()].concat()
    }

    #[inline]
    fn make_list_full_name<K>(&self, name: K) -> Key
    where
        K: AsRef<[u8]>,
    {
        [LIST_NAME_PREFIX, self.prefix.as_slice(), name.as_ref()].concat()
    }

    #[inline]
    fn make_map_prefix_match(&self) -> Key {
        [MAP_NAME_PREFIX, self.prefix.as_slice(), b"*"].concat()
    }

    #[inline]
    fn make_list_prefix_match(&self) -> Key {
        [LIST_NAME_PREFIX, self.prefix.as_slice(), b"*"].concat()
    }

    #[inline]
    fn map_full_name_to_key<'a>(&self, full_name: &'a [u8]) -> &'a [u8] {
        full_name[MAP_NAME_PREFIX.len() + self.prefix.len()..].as_ref()
    }

    #[inline]
    fn list_full_name_to_key<'a>(&self, full_name: &'a [u8]) -> &'a [u8] {
        full_name[LIST_NAME_PREFIX.len() + self.prefix.len()..].as_ref()
    }

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
                    .query_async::<_, ()>(&mut async_conn)
                    .await?;
            } else {
                self.async_conn()
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
                    .query_async::<_, ()>(&mut async_conn)
                    .await?;
            } else {
                pipe()
                    .atomic()
                    .set(full_key.as_slice(), bincode::serialize(val)?)
                    .zadd(db_zkey, key.as_ref(), i64::MAX)
                    .query_async::<_, ()>(&mut async_conn)
                    .await?;
            }
        }

        Ok(())
    }

    #[inline]
    async fn _batch_insert(
        &self,
        key_val_expires: Vec<(Key, Vec<u8>, Option<TimestampMillis>)>,
    ) -> Result<()> {
        // let full_key = self.make_full_key(k);
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
            rpipe.query_async::<_, ()>(&mut async_conn).await?;
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
            rpipe.query_async::<_, ((), ())>(&mut async_conn).await?;
        }
        Ok(())
    }

    #[inline]
    async fn _batch_remove(&self, keys: Vec<Key>) -> Result<()> {
        let full_keys = keys
            .iter()
            .map(|k| self.make_full_key(k))
            .collect::<Vec<_>>();
        #[cfg(not(feature = "len"))]
        {
            self.async_conn().del(full_keys).await?;
        }
        #[cfg(feature = "len")]
        {
            let db_zkey = self.make_len_sortedset_key();
            let mut async_conn = self.async_conn();
            pipe()
                .atomic()
                .del(full_keys.as_slice())
                .zrem(db_zkey, keys)
                .query_async::<_, ()>(&mut async_conn)
                .await?;
        }
        Ok(())
    }

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
                    .query_async::<_, ()>(&mut async_conn)
                    .await?;
            } else {
                self.async_conn().incr(full_key, increment).await?;
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
                    .query_async::<_, ()>(&mut async_conn)
                    .await?;
            } else {
                pipe()
                    .atomic()
                    .incr(full_key.as_slice(), increment)
                    .zadd(db_zkey, key.as_ref(), i64::MAX)
                    .query_async::<_, ()>(&mut async_conn)
                    .await?;
            }
        }
        Ok(())
    }

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
                    .query_async::<_, ()>(&mut async_conn)
                    .await?;
            } else {
                self.async_conn().decr(full_key, decrement).await?;
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
                    .query_async::<_, ()>(&mut async_conn)
                    .await?;
            } else {
                pipe()
                    .atomic()
                    .decr(full_key.as_slice(), decrement)
                    .zadd(db_zkey, key.as_ref(), i64::MAX)
                    .query_async::<_, ()>(&mut async_conn)
                    .await?;
            }
        }
        Ok(())
    }

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
                    .query_async::<_, ()>(&mut async_conn)
                    .await?;
            } else {
                self.async_conn().set(full_key, val).await?;
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
                    .query_async::<_, ()>(&mut async_conn)
                    .await?;
            } else {
                pipe()
                    .atomic()
                    .set(full_key.as_slice(), val)
                    .zadd(db_zkey, key.as_ref(), i64::MAX)
                    .query_async::<_, ()>(&mut async_conn)
                    .await?;
            }
        }

        Ok(())
    }

    #[inline]
    async fn _remove<K>(&self, key: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let full_key = self.make_full_key(key.as_ref());

        #[cfg(not(feature = "len"))]
        {
            self.async_conn().del(full_key).await?;
        }
        #[cfg(feature = "len")]
        {
            let db_zkey = self.make_len_sortedset_key();
            let mut async_conn = self.async_conn();
            pipe()
                .atomic()
                .del(full_key.as_slice())
                .zrem(db_zkey, key.as_ref())
                .query_async::<_, ()>(&mut async_conn)
                .await?;
        }
        Ok(())
    }
}

#[async_trait]
impl StorageDB for RedisStorageDB {
    type MapType = RedisStorageMap;
    type ListType = RedisStorageList;

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

    #[inline]
    async fn map_remove<K>(&self, name: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let map_full_name = self.make_map_full_name(name.as_ref());
        self.async_conn().del(map_full_name).await?;
        Ok(())
    }

    #[inline]
    async fn map_contains_key<K: AsRef<[u8]> + Sync + Send>(&self, key: K) -> Result<bool> {
        let map_full_name = self.make_map_full_name(key.as_ref());
        Ok(self.async_conn().exists(map_full_name).await?)
    }

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

    #[inline]
    async fn list_remove<K>(&self, name: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let list_full_name = self.make_list_full_name(name.as_ref());
        self.async_conn().del(list_full_name).await?;
        Ok(())
    }

    #[inline]
    async fn list_contains_key<K: AsRef<[u8]> + Sync + Send>(&self, key: K) -> Result<bool> {
        let list_full_name = self.make_list_full_name(key.as_ref());
        Ok(self.async_conn().exists(list_full_name).await?)
    }

    #[inline]
    async fn insert<K, V>(&self, key: K, val: &V) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: serde::ser::Serialize + Sync + Send,
    {
        self._insert(key, val, None).await
    }

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

    #[inline]
    async fn remove<K>(&self, key: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        self._remove(key).await
    }

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

    #[inline]
    async fn batch_remove(&self, keys: Vec<Key>) -> Result<()> {
        if !keys.is_empty() {
            self._batch_remove(keys).await?;
        }
        Ok(())
    }

    #[inline]
    async fn counter_incr<K>(&self, key: K, increment: isize) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        self._counter_incr(key, increment, None).await
    }

    #[inline]
    async fn counter_decr<K>(&self, key: K, decrement: isize) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        self._counter_decr(key, decrement, None).await
    }

    #[inline]
    async fn counter_get<K>(&self, key: K) -> Result<Option<isize>>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let full_key = self.make_full_key(key);
        Ok(self.async_conn().get::<_, Option<isize>>(full_key).await?)
    }

    #[inline]
    async fn counter_set<K>(&self, key: K, val: isize) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        self._counter_set(key, val, None).await
    }

    #[inline]
    async fn contains_key<K: AsRef<[u8]> + Sync + Send>(&self, key: K) -> Result<bool> {
        //HEXISTS key field
        let full_key = self.make_full_key(key.as_ref());
        Ok(self.async_conn().exists(full_key).await?)
    }

    #[inline]
    #[cfg(feature = "len")]
    async fn len(&self) -> Result<usize> {
        let db_zkey = self.make_len_sortedset_key();
        let mut async_conn = self.async_conn();
        let (_, count) = pipe()
            .zrembyscore(db_zkey.as_slice(), 0, timestamp_millis())
            .zcard(db_zkey.as_slice())
            .query_async::<_, (i64, usize)>(&mut async_conn)
            .await?;
        Ok(count)
    }

    #[inline]
    async fn db_size(&self) -> Result<usize> {
        let mut async_conn = self.async_conn();
        //DBSIZE
        let dbsize = redis::pipe()
            .cmd("DBSIZE")
            .query_async::<_, redis::Value>(&mut async_conn)
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
                .query_async::<_, (i64, bool)>(&mut async_conn)
                .await?;
            Ok(res)
        }
    }

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
                .query_async::<_, (i64, bool)>(&mut async_conn)
                .await?;
            Ok(res)
        }
    }

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

    #[inline]
    async fn info(&self) -> Result<Value> {
        let mut conn = self.async_conn();
        let dbsize = redis::pipe()
            .cmd("dbsize")
            .query_async::<_, redis::Value>(&mut conn)
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

#[derive(Clone)]
pub struct RedisStorageMap {
    name: Key,
    full_name: Key,
    #[allow(dead_code)]
    expire: Option<TimestampMillis>,
    empty: Arc<AtomicBool>,
    pub(crate) db: RedisStorageDB,
}

impl RedisStorageMap {
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

    #[inline]
    fn async_conn(&self) -> RedisConnection {
        self.db.async_conn()
    }

    #[inline]
    fn async_conn_mut(&mut self) -> &mut RedisConnection {
        self.db.async_conn_mut()
    }

    #[inline]
    async fn _is_empty(async_conn: &mut RedisConnection, full_name: &[u8]) -> Result<bool> {
        //HSCAN key cursor [MATCH pattern] [COUNT count]
        let res = async_conn
            .hscan::<_, Vec<u8>>(full_name)
            .await?
            .next_item()
            .await
            .is_none();
        Ok(res)
    }

    #[inline]
    async fn _insert_expire(&self, key: &[u8], val: Vec<u8>) -> Result<()> {
        let mut async_conn = self.async_conn();
        let name = self.full_name.as_slice();

        #[cfg(feature = "ttl")]
        if self.empty.load(Ordering::SeqCst) {
            if let Some(expire) = self.expire.as_ref() {
                //HSET key field value
                //PEXPIRE key ms
                redis::pipe()
                    .atomic()
                    .hset(name, key, val)
                    .pexpire(name, *expire)
                    .query_async(&mut async_conn)
                    .await?;
                self.empty.store(false, Ordering::SeqCst);
                return Ok(());
            }
        }

        //HSET key field value
        async_conn.hset(name, key.as_ref(), val).await?;
        Ok(())
    }

    #[inline]
    async fn _batch_insert_expire(&self, key_vals: Vec<(Key, Vec<u8>)>) -> Result<()> {
        let mut async_conn = self.async_conn();
        let name = self.full_name.as_slice();

        #[cfg(feature = "ttl")]
        if self.empty.load(Ordering::SeqCst) {
            if let Some(expire) = self.expire.as_ref() {
                //HMSET key field value
                //PEXPIRE key ms
                redis::pipe()
                    .atomic()
                    .hset_multiple(name, key_vals.as_slice())
                    .pexpire(name, *expire)
                    .query_async(&mut async_conn)
                    .await?;

                self.empty.store(false, Ordering::SeqCst);
                return Ok(());
            }
        }

        //HSET key field value
        async_conn.hset_multiple(name, key_vals.as_slice()).await?;
        Ok(())
    }
}

#[async_trait]
impl Map for RedisStorageMap {
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
        self._insert_expire(key.as_ref(), bincode::serialize(val)?)
            .await
    }

    #[inline]
    async fn get<K, V>(&self, key: K) -> Result<Option<V>>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: DeserializeOwned + Sync + Send,
    {
        //HSET key field value
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

    #[inline]
    async fn remove<K>(&self, key: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        //HDEL key field [field ...]
        self.async_conn()
            .hdel(self.full_name.as_slice(), key.as_ref())
            .await?;
        Ok(())
    }

    #[inline]
    async fn contains_key<K: AsRef<[u8]> + Sync + Send>(&self, key: K) -> Result<bool> {
        //HEXISTS key field
        let res = self
            .async_conn()
            .hexists(self.full_name.as_slice(), key.as_ref())
            .await?;
        Ok(res)
    }

    #[cfg(feature = "map_len")]
    #[inline]
    async fn len(&self) -> Result<usize> {
        //HLEN key
        Ok(self.async_conn().hlen(self.full_name.as_slice()).await?)
    }

    #[inline]
    async fn is_empty(&self) -> Result<bool> {
        //HSCAN key cursor [MATCH pattern] [COUNT count]
        let res = self
            .async_conn()
            .hscan::<_, Vec<u8>>(self.full_name.as_slice())
            .await?
            .next_item()
            .await
            .is_none();
        Ok(res)
    }

    #[inline]
    async fn clear(&self) -> Result<()> {
        //DEL key [key ...]
        self.async_conn().del(self.full_name.as_slice()).await?;
        self.empty.store(true, Ordering::SeqCst);
        Ok(())
    }

    #[inline]
    async fn remove_and_fetch<K, V>(&self, key: K) -> Result<Option<V>>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: DeserializeOwned + Sync + Send,
    {
        //HSET key field value
        //HDEL key field [field ...]
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
            removeds.push(key);
            if removeds.len() > 20 {
                conn2.hdel(name, removeds.as_slice()).await?;
                removeds.clear();
            }
        }
        if !removeds.is_empty() {
            conn.hdel(name, removeds).await?;
        }
        Ok(())
    }

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

    #[inline]
    async fn batch_remove(&self, keys: Vec<Key>) -> Result<()> {
        if !keys.is_empty() {
            self.async_conn()
                .hdel(self.full_name.as_slice(), keys)
                .await?;
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

    #[cfg(feature = "ttl")]
    async fn expire_at(&self, at: TimestampMillis) -> Result<bool> {
        let res = self
            .async_conn()
            .pexpire_at::<_, bool>(self.full_name.as_slice(), at)
            .await?;
        Ok(res)
    }

    #[cfg(feature = "ttl")]
    async fn expire(&self, dur: TimestampMillis) -> Result<bool> {
        let res = self
            .async_conn()
            .pexpire::<_, bool>(self.full_name.as_slice(), dur)
            .await?;
        Ok(res)
    }

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

#[derive(Clone)]
pub struct RedisStorageList {
    name: Key,
    full_name: Key,
    #[allow(dead_code)]
    expire: Option<TimestampMillis>,
    empty: Arc<AtomicBool>,
    pub(crate) db: RedisStorageDB,
}

impl RedisStorageList {
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

    #[inline]
    pub(crate) fn async_conn(&self) -> RedisConnection {
        self.db.async_conn()
    }

    #[inline]
    async fn _is_empty(async_conn: &mut RedisConnection, full_name: &[u8]) -> Result<bool> {
        Ok(async_conn.llen::<_, usize>(full_name).await? == 0)
    }

    #[inline]
    async fn _push_expire(&self, val: Vec<u8>) -> Result<()> {
        let mut async_conn = self.async_conn();
        let name = self.full_name.as_slice();

        #[cfg(feature = "ttl")]
        if self.empty.load(Ordering::SeqCst) {
            if let Some(expire) = self.expire.as_ref() {
                //RPUSH key value [value ...]
                //PEXPIRE key ms
                redis::pipe()
                    .atomic()
                    .rpush(name, val)
                    .pexpire(name, *expire)
                    .query_async(&mut async_conn)
                    .await?;
                self.empty.store(false, Ordering::SeqCst);
                return Ok(());
            }
        }

        //RPUSH key value [value ...]
        async_conn.rpush(name, val).await?;
        Ok(())
    }

    #[inline]
    async fn _pushs_expire(&self, vals: Vec<Vec<u8>>) -> Result<()> {
        let mut async_conn = self.async_conn();

        #[cfg(feature = "ttl")]
        if self.empty.load(Ordering::SeqCst) {
            if let Some(expire) = self.expire.as_ref() {
                let name = self.full_name.as_slice();
                //RPUSH key value [value ...]
                //PEXPIRE key ms
                redis::pipe()
                    .atomic()
                    .rpush(name, vals)
                    .pexpire(name, *expire)
                    .query_async(&mut async_conn)
                    .await?;
                self.empty.store(false, Ordering::SeqCst);
                return Ok(());
            }
        }

        //RPUSH key value [value ...]
        async_conn.rpush(self.full_name.as_slice(), vals).await?;
        Ok(())
    }

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
                    redis::pipe()
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
            async_conn.rpush(name, val).await?;
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
    #[inline]
    fn name(&self) -> &[u8] {
        self.name.as_slice()
    }

    #[inline]
    async fn push<V>(&self, val: &V) -> Result<()>
    where
        V: Serialize + Sync + Send,
    {
        self._push_expire(bincode::serialize(val)?).await
    }

    #[inline]
    async fn pushs<V>(&self, vals: Vec<V>) -> Result<()>
    where
        V: Serialize + Sync + Send,
    {
        //RPUSH key value [value ...]
        let vals = vals
            .into_iter()
            .map(|v| bincode::serialize(&v).map_err(|e| anyhow!(e)))
            .collect::<Result<Vec<_>>>()?;
        self._pushs_expire(vals).await
    }

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

    #[inline]
    async fn pop<V>(&self) -> Result<Option<V>>
    where
        V: DeserializeOwned + Sync + Send,
    {
        //LPOP key
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

    #[inline]
    async fn all<V>(&self) -> Result<Vec<V>>
    where
        V: DeserializeOwned + Sync + Send,
    {
        //LRANGE key 0 -1
        let all = self
            .async_conn()
            .lrange::<_, Vec<Vec<u8>>>(self.full_name.as_slice(), 0, -1)
            .await?;
        all.iter()
            .map(|v| bincode::deserialize::<V>(v.as_ref()).map_err(|e| anyhow!(e)))
            .collect::<Result<Vec<_>>>()
    }

    #[inline]
    async fn get_index<V>(&self, idx: usize) -> Result<Option<V>>
    where
        V: DeserializeOwned + Sync + Send,
    {
        //LINDEX key index
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

    #[inline]
    async fn len(&self) -> Result<usize> {
        //LLEN key
        Ok(self.async_conn().llen(self.full_name.as_slice()).await?)
    }

    #[inline]
    async fn is_empty(&self) -> Result<bool> {
        Ok(self.len().await? == 0)
    }

    #[inline]
    async fn clear(&self) -> Result<()> {
        self.async_conn().del(self.full_name.as_slice()).await?;
        self.empty.store(true, Ordering::SeqCst);
        Ok(())
    }

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

    #[cfg(feature = "ttl")]
    async fn expire_at(&self, at: TimestampMillis) -> Result<bool> {
        let res = self
            .async_conn()
            .pexpire_at::<_, bool>(self.full_name.as_slice(), at)
            .await?;
        Ok(res)
    }

    #[cfg(feature = "ttl")]
    async fn expire(&self, dur: TimestampMillis) -> Result<bool> {
        let res = self
            .async_conn()
            .pexpire::<_, bool>(self.full_name.as_slice(), dur)
            .await?;
        Ok(res)
    }

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

pub struct AsyncListValIter<'a, V> {
    name: &'a [u8],
    conn: RedisConnection,
    start: isize,
    limit: isize,
    catch_vals: Vec<Vec<u8>>,
    _m: std::marker::PhantomData<V>,
}

impl<'a, V> AsyncListValIter<'a, V> {
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
impl<'a, V> AsyncIterator for AsyncListValIter<'a, V>
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
        let item = self.iter.next_item().await;
        item.map(|(key, v)| match bincode::deserialize::<V>(v.as_ref()) {
            Ok(v) => Ok((key, v)),
            Err(e) => Err(anyhow::Error::new(e)),
        })
    }
}

pub struct AsyncDbKeyIter<'a> {
    prefix_len: usize,
    iter: redis::AsyncIter<'a, Key>,
}

#[async_trait]
impl<'a> AsyncIterator for AsyncDbKeyIter<'a> {
    type Item = Result<Key>;

    async fn next(&mut self) -> Option<Self::Item> {
        self.iter
            .next_item()
            .await
            .map(|key| Ok(key[self.prefix_len..].to_vec()))
    }
}

pub struct AsyncKeyIter<'a> {
    iter: redis::AsyncIter<'a, (Key, ())>,
}

#[async_trait]
impl<'a> AsyncIterator for AsyncKeyIter<'a> {
    type Item = Result<Key>;

    async fn next(&mut self) -> Option<Self::Item> {
        self.iter.next_item().await.map(|(key, _)| Ok(key))
    }
}

pub struct AsyncMapIter<'a> {
    db: RedisStorageDB,
    iter: redis::AsyncIter<'a, Key>,
}

#[async_trait]
impl<'a> AsyncIterator for AsyncMapIter<'a> {
    type Item = Result<StorageMap>;

    async fn next(&mut self) -> Option<Self::Item> {
        let full_name = self.iter.next_item().await;
        if let Some(full_name) = full_name {
            let name = self.db.map_full_name_to_key(full_name.as_slice()).to_vec();
            let m = RedisStorageMap::new(name, full_name, self.db.clone());
            Some(Ok(StorageMap::Redis(m)))
        } else {
            None
        }
    }
}

pub struct AsyncListIter<'a> {
    db: RedisStorageDB,
    iter: redis::AsyncIter<'a, Key>,
}

#[async_trait]
impl<'a> AsyncIterator for AsyncListIter<'a> {
    type Item = Result<StorageList>;

    async fn next(&mut self) -> Option<Self::Item> {
        let full_name = self.iter.next_item().await;
        if let Some(full_name) = full_name {
            let name = self.db.list_full_name_to_key(full_name.as_slice()).to_vec();
            let l = RedisStorageList::new(name, full_name, self.db.clone());
            Some(Ok(StorageList::Redis(l)))
        } else {
            None
        }
    }
}
