use std::future::Future;

use anyhow::anyhow;
use async_trait::async_trait;
use redis::aio::MultiplexedConnection;
use redis::AsyncCommands;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::storage::{AsyncIterator, IterItem, Key, List, Map, StorageDB};
use crate::{Result, StorageList, StorageMap, TimestampMillis};

const SEPARATOR: &[u8] = b"@";
const KEY_PREFIX: &[u8] = b"__rmqtt@";
const MAP_NAME_PREFIX: &[u8] = b"__rmqtt_map@";
const LIST_NAME_PREFIX: &[u8] = b"__rmqtt_list@";

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
    async_conn: MultiplexedConnection,
}

impl RedisStorageDB {
    #[inline]
    pub(crate) async fn new(cfg: RedisConfig) -> Result<Self> {
        let prefix = [cfg.prefix.as_bytes(), SEPARATOR].concat();
        let client = redis::Client::open(cfg.url.as_str())?;
        let async_conn = client.get_multiplexed_tokio_connection().await?;
        Ok(Self { prefix, async_conn })
    }

    #[inline]
    fn async_conn(&self) -> MultiplexedConnection {
        self.async_conn.clone()
    }

    #[inline]
    fn async_conn_mut(&mut self) -> &mut MultiplexedConnection {
        &mut self.async_conn
    }

    #[inline]
    fn make_full_key<K>(&self, key: K) -> Key
    where
        K: AsRef<[u8]>,
    {
        [KEY_PREFIX, self.prefix.as_slice(), key.as_ref()].concat()
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
    async fn get_full_name(&self, key: &[u8]) -> Result<Key> {
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
}

#[async_trait]
impl StorageDB for RedisStorageDB {
    type MapType = RedisStorageMap;
    type ListType = RedisStorageList;

    #[inline]
    fn map<V: AsRef<[u8]>>(&self, name: V) -> Self::MapType {
        let full_name = self.make_map_full_name(name.as_ref());
        RedisStorageMap::new(name.as_ref().to_vec(), full_name, self.clone())
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
    fn list<V: AsRef<[u8]>>(&self, name: V) -> Self::ListType {
        let full_name = self.make_list_full_name(name.as_ref());
        RedisStorageList::new(name.as_ref().to_vec(), full_name, self.clone())
    }

    async fn list_remove<K>(&self, name: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let list_full_name = self.make_list_full_name(name.as_ref());
        self.async_conn().del(list_full_name).await?;
        Ok(())
    }

    #[inline]
    async fn insert<K, V>(&self, key: K, val: &V) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: serde::ser::Serialize + Sync + Send,
    {
        let full_key = self.make_full_key(key);
        self.async_conn()
            .set(full_key, bincode::serialize(val)?)
            .await?;
        Ok(())
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
        let full_key = self.make_full_key(key.as_ref());
        self.async_conn().del(full_key).await?;
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
                    let full_key = self.make_full_key(k);
                    bincode::serialize(&v)
                        .map(move |v| (full_key, v))
                        .map_err(|e| anyhow!(e))
                })
                .collect::<Result<Vec<_>>>()?;
            self.async_conn().mset(key_vals.as_slice()).await?;
        }
        Ok(())
    }

    #[inline]
    async fn batch_remove(&self, keys: Vec<Key>) -> Result<()> {
        if !keys.is_empty() {
            let keys = keys
                .into_iter()
                .map(|k| self.make_full_key(k))
                .collect::<Vec<_>>();
            self.async_conn().del(keys).await?;
        }
        Ok(())
    }

    #[inline]
    async fn counter_incr<K>(&self, key: K, increment: isize) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let full_key = self.make_full_key(key);
        self.async_conn().incr(full_key, increment).await?;
        Ok(())
    }

    #[inline]
    async fn counter_decr<K>(&self, key: K, decrement: isize) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let full_key = self.make_full_key(key);
        self.async_conn().decr(full_key, decrement).await?;
        Ok(())
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
        let full_key = self.make_full_key(key);
        self.async_conn().set(full_key, val).await?;
        Ok(())
    }

    #[inline]
    async fn contains_key<K: AsRef<[u8]> + Sync + Send>(&self, key: K) -> Result<bool> {
        //HEXISTS key field
        let mut async_conn = self.async_conn();
        let map_full_name = self.make_map_full_name(key.as_ref());
        if async_conn.exists(map_full_name).await? {
            Ok(true)
        } else {
            let list_full_name = self.make_list_full_name(key.as_ref());
            if async_conn.exists(list_full_name).await? {
                Ok(true)
            } else {
                let full_key = self.make_full_key(key.as_ref());
                Ok(async_conn.exists(full_key).await?)
            }
        }
    }

    #[inline]
    async fn expire_at<K>(&self, key: K, e_at: TimestampMillis) -> Result<bool>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let full_name = self.get_full_name(key.as_ref()).await?;
        let res = self
            .async_conn()
            .pexpire_at::<_, bool>(full_name, e_at)
            .await?;
        Ok(res)
    }

    #[inline]
    async fn expire<K>(&self, key: K, dur: TimestampMillis) -> Result<bool>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let full_name = self.get_full_name(key.as_ref()).await?;
        let res = self.async_conn().pexpire::<_, bool>(full_name, dur).await?;
        Ok(res)
    }

    #[inline]
    async fn ttl<K>(&self, key: K) -> Result<Option<TimestampMillis>>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let mut async_conn = self.async_conn();
        let map_full_name = self.make_map_full_name(key.as_ref());
        let res = async_conn.pttl::<_, isize>(map_full_name).await?;
        match res {
            -2 => {}
            -1 => return Ok(Some(TimestampMillis::MAX)),
            _ => return Ok(Some(res as TimestampMillis)),
        }

        let list_full_name = self.make_list_full_name(key.as_ref());
        let res = async_conn.pttl::<_, isize>(list_full_name).await?;
        match res {
            -2 => {}
            -1 => return Ok(Some(TimestampMillis::MAX)),
            _ => return Ok(Some(res as TimestampMillis)),
        }

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
}

#[derive(Clone)]
pub struct RedisStorageMap {
    name: Key,
    full_name: Key,
    pub(crate) db: RedisStorageDB,
}

impl RedisStorageMap {
    #[inline]
    pub(crate) fn new(name: Key, full_name: Key, db: RedisStorageDB) -> Self {
        Self {
            name,
            full_name,
            db,
        }
    }

    #[inline]
    fn async_conn(&self) -> MultiplexedConnection {
        self.db.async_conn()
    }

    #[inline]
    fn async_conn_mut(&mut self) -> &mut MultiplexedConnection {
        self.db.async_conn_mut()
    }

    #[inline]
    async fn _retain<'a, F, Out, V>(&'a self, f: F) -> Result<()>
    where
        F: Fn(Result<(Key, V)>) -> Out + Send + Sync,
        Out: Future<Output = bool> + Send + 'a,
        V: DeserializeOwned + Sync + Send + 'a,
    {
        let name = self.full_name.clone();
        let mut async_conn = self.async_conn();
        let batch_size = 20;

        let mut removeds = Vec::new();
        //Remove hash
        let mut iter = async_conn
            .hscan::<_, (Key, Vec<u8>)>(name.as_slice())
            .await?;
        while let Some((key, val)) = iter.next_item().await {
            match bincode::deserialize::<V>(val.as_ref()) {
                Ok(v) => {
                    if !f(Ok((key.clone(), v))).await {
                        removeds.push(key);
                    }
                }
                Err(e) => {
                    if !f(Err(anyhow::Error::new(e))).await {
                        removeds.push(key);
                    }
                }
            }
        }

        drop(iter);
        for batch in removeds.chunks(batch_size) {
            if !batch.is_empty() {
                async_conn.hdel(name.as_slice(), batch).await?;
            }
        }

        Ok(())
    }

    #[inline]
    async fn _retain_with_key<'a, F, Out>(&'a self, f: F) -> Result<()>
    where
        F: Fn(Result<Key>) -> Out + Send + Sync,
        Out: Future<Output = bool> + Send + 'a,
    {
        let name = self.full_name.clone();
        let mut async_conn = self.async_conn();

        let batch_size = 20;

        let mut removeds = Vec::new();
        //Remove hash
        let mut iter = async_conn.hscan::<_, (Key, ())>(name.as_slice()).await?;
        while let Some((key, _)) = iter.next_item().await {
            if !f(Ok(key.clone())).await {
                removeds.push(key);
            }
        }

        drop(iter);
        for batch in removeds.chunks(batch_size) {
            if !batch.is_empty() {
                async_conn.hdel(name.as_slice(), batch).await?;
            }
        }

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
        //HSET key field value
        let name = self.full_name.clone();
        self.async_conn()
            .hset(name, key.as_ref(), bincode::serialize(val)?)
            .await?;
        Ok(())
    }

    #[inline]
    async fn get<K, V>(&self, key: K) -> Result<Option<V>>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: DeserializeOwned + Sync + Send,
    {
        //HSET key field value
        let name = self.full_name.clone();
        let res: Option<Vec<u8>> = self.async_conn().hget(name, key.as_ref()).await?;
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
        let name = self.full_name.clone();
        self.async_conn().hdel(name, key.as_ref()).await?;
        Ok(())
    }

    #[inline]
    async fn contains_key<K: AsRef<[u8]> + Sync + Send>(&self, key: K) -> Result<bool> {
        //HEXISTS key field
        let name = self.full_name.clone();
        let res = self.async_conn().hexists(name, key.as_ref()).await?;
        Ok(res)
    }

    #[inline]
    async fn len(&self) -> Result<usize> {
        //HLEN key
        let name = self.full_name.clone();
        Ok(self.async_conn().hlen(name).await?)
    }

    #[inline]
    async fn is_empty(&self) -> Result<bool> {
        //HSCAN key cursor [MATCH pattern] [COUNT count]
        let name = self.full_name.clone();
        let res = self
            .async_conn()
            .hscan::<_, Vec<u8>>(name)
            .await?
            .next_item()
            .await
            .is_none();
        Ok(res)
    }

    #[inline]
    async fn clear(&self) -> Result<()> {
        //DEL key [key ...]
        let name = self.full_name.clone();
        self.async_conn().del(name).await?;
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
        let name = self.full_name.clone();
        let mut conn = self.async_conn();
        let (res, _): (Option<Vec<u8>>, isize) = redis::pipe()
            .atomic()
            .hget(name.as_slice(), key.as_ref())
            .hdel(name.as_slice(), key.as_ref())
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
        let name = self.full_name.clone();
        let mut conn = self.async_conn();
        let mut conn2 = conn.clone();
        let mut prefix = prefix.as_ref().to_vec();
        prefix.push(b'*');
        let mut removeds = Vec::new();
        while let Some(key) = conn
            .hscan_match::<_, _, Vec<u8>>(name.as_slice(), prefix.as_slice())
            .await?
            .next_item()
            .await
        {
            removeds.push(key);
            if removeds.len() > 20 {
                conn2.hdel(name.as_slice(), removeds.as_slice()).await?;
                removeds.clear();
            }
        }
        if !removeds.is_empty() {
            conn.hdel(name.as_slice(), removeds).await?;
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
            let name = self.full_name.clone();
            self.async_conn()
                .hset_multiple(name, key_vals.as_slice())
                .await?;
        }
        Ok(())
    }

    #[inline]
    async fn batch_remove(&self, keys: Vec<Key>) -> Result<()> {
        if !keys.is_empty() {
            let name = self.full_name.clone();
            self.async_conn().hdel(name, keys).await?;
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
        let name = self.full_name.clone();
        let iter = AsyncKeyIter {
            iter: self.db.async_conn.hscan::<_, (Key, ())>(name).await?,
        };
        Ok(Box::new(iter))
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

    #[inline]
    async fn retain<'a, F, Out, V>(&'a self, f: F) -> Result<()>
    where
        F: Fn(Result<(Key, V)>) -> Out + Send + Sync + 'static,
        Out: Future<Output = bool> + Send + 'a,
        V: DeserializeOwned + Sync + Send + 'a,
    {
        self._retain(f).await
    }

    #[inline]
    async fn retain_with_key<'a, F, Out>(&'a self, f: F) -> Result<()>
    where
        F: Fn(Result<Key>) -> Out + Send + Sync + 'static,
        Out: Future<Output = bool> + Send + 'a,
    {
        self._retain_with_key(f).await
    }
}

#[derive(Clone)]
pub struct RedisStorageList {
    name: Key,
    full_name: Key,
    pub(crate) db: RedisStorageDB,
}

impl RedisStorageList {
    #[inline]
    pub(crate) fn new(name: Key, full_name: Key, db: RedisStorageDB) -> Self {
        Self {
            name,
            full_name,
            db,
        }
    }

    #[inline]
    pub(crate) fn async_conn(&self) -> MultiplexedConnection {
        self.db.async_conn()
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
        //RPUSH key value [value ...]
        self.async_conn()
            .rpush(self.full_name.as_slice(), bincode::serialize(val)?)
            .await?;
        Ok(())
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
        self.async_conn()
            .rpush(self.full_name.as_slice(), vals)
            .await?;
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
        V: Serialize + Sync + Send,
        V: DeserializeOwned,
    {
        let data = bincode::serialize(val)?;

        let key = self.full_name.as_slice();
        let mut conn = self.async_conn();

        let count = conn.llen::<_, usize>(key).await?;
        if count < limit {
            conn.rpush(key, data).await?;
            Ok(None)
        } else if pop_front_if_limited {
            let (poped, _): (Option<Vec<u8>>, Option<()>) = redis::pipe()
                .atomic()
                .lpop(key, None)
                .rpush(key, data)
                .query_async(&mut conn)
                .await?;

            Ok(if let Some(v) = poped {
                Some(bincode::deserialize::<V>(v.as_ref()).map_err(|e| anyhow!(e))?)
            } else {
                None
            })
        } else {
            Err(anyhow::Error::msg("Is full"))
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
    async fn pop_f<'a, F, V>(&'a self, f: F) -> Result<Option<V>>
    where
        F: Fn(&V) -> bool + Send + Sync + 'static,
        V: DeserializeOwned + Sync + Send + 'a + 'static,
    {
        let mut async_conn = self.async_conn();
        let v = async_conn
            .lindex::<_, Option<Vec<u8>>>(self.full_name.as_slice(), 0)
            .await?;
        let removed = if let Some(v) = v {
            let val = bincode::deserialize::<V>(v.as_ref()).map_err(|e| anyhow!(e))?;
            if f(&val) {
                async_conn
                    .lpop::<_, Option<Vec<u8>>>(self.full_name.as_slice(), None)
                    .await?;
                Some(val)
            } else {
                None
            }
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
}

pub struct AsyncListValIter<'a, V> {
    name: &'a [u8],
    conn: MultiplexedConnection,
    start: isize,
    limit: isize,
    catch_vals: Vec<Vec<u8>>,
    _m: std::marker::PhantomData<V>,
}

impl<'a, V> AsyncListValIter<'a, V> {
    fn new(name: &'a [u8], conn: MultiplexedConnection) -> Self {
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
