use anyhow::anyhow;
use async_trait::async_trait;
use std::future::Future;
use std::time::Duration;

use redis::aio::MultiplexedConnection;
use redis::{AsyncCommands, Commands, Connection};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::task::block_in_place;

use crate::storage::{IterItem, Key, Map, StorageDB};
use crate::{List, Result, TimestampMillis};

#[derive(Clone)]
pub struct RedisStorageDB {
    client: redis::Client,
    async_conn: MultiplexedConnection,
}

impl RedisStorageDB {
    #[inline]
    pub(crate) async fn new(url: &str) -> Result<Self> {
        let client = redis::Client::open(url)?;
        let async_conn = client.get_multiplexed_tokio_connection().await?;
        Ok(Self { client, async_conn })
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
    pub(crate) fn conn(&self) -> Result<Connection> {
        let conn = self
            .client
            .get_connection_with_timeout(Duration::from_secs(10))?;
        Ok(conn)
    }
}

#[async_trait]
impl StorageDB for RedisStorageDB {
    type MapType = RedisStorageMap;
    type ListType = RedisStorageList;

    #[inline]
    fn map<V: AsRef<[u8]>>(&mut self, name: V) -> Result<Self::MapType> {
        Ok(RedisStorageMap::new(name.as_ref().to_vec(), self.clone()))
    }

    fn list<V: AsRef<[u8]>>(&mut self, name: V) -> Result<Self::ListType> {
        Ok(RedisStorageList::new(name.as_ref().to_vec(), self.clone()))
    }

    #[inline]
    async fn insert<K, V>(&mut self, key: K, val: &V) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: serde::ser::Serialize + Sync + Send,
    {
        self.async_conn_mut()
            .set(key.as_ref(), bincode::serialize(val)?)
            .await?;
        Ok(())
    }

    #[inline]
    async fn get<K, V>(&mut self, key: K) -> Result<Option<V>>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: DeserializeOwned + Sync + Send,
    {
        if let Some(v) = self
            .async_conn_mut()
            .get::<_, Option<Vec<u8>>>(key.as_ref())
            .await?
        {
            Ok(Some(bincode::deserialize::<V>(v.as_ref())?))
        } else {
            Ok(None)
        }
    }

    #[inline]
    async fn remove<K>(&mut self, key: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        self.async_conn_mut().del(key.as_ref()).await?;
        Ok(())
    }

    #[inline]
    async fn contains_key<K: AsRef<[u8]> + Sync + Send>(&mut self, key: K) -> Result<bool> {
        //HEXISTS key field
        Ok(self.async_conn_mut().exists(key.as_ref()).await?)
    }

    #[inline]
    async fn expire_at<K>(&mut self, key: K, e_at: TimestampMillis) -> Result<bool>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let res = self
            .async_conn_mut()
            .pexpire_at::<_, bool>(key.as_ref(), e_at)
            .await?;
        Ok(res)
    }

    #[inline]
    async fn expire<K>(&mut self, key: K, dur: TimestampMillis) -> Result<bool>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let res = self
            .async_conn_mut()
            .pexpire::<_, bool>(key.as_ref(), dur)
            .await?;
        Ok(res)
    }

    async fn ttl<K>(&mut self, key: K) -> Result<Option<TimestampMillis>>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let res = self.async_conn_mut().pttl::<_, isize>(key.as_ref()).await?;
        match res {
            -2 => Ok(None),
            -1 => Ok(Some(TimestampMillis::MAX)),
            _ => Ok(Some(res as TimestampMillis)),
        }
    }
}

#[derive(Clone)]
pub struct RedisStorageMap {
    name: Key,
    pub(crate) db: RedisStorageDB,
}

impl RedisStorageMap {
    #[inline]
    pub(crate) fn new(name: Key, db: RedisStorageDB) -> Self {
        Self { name, db }
    }

    // #[inline]
    // fn async_conn(&self) -> MultiplexedConnection {
    //     self.db.async_conn()
    // }

    #[inline]
    fn async_conn_mut(&mut self) -> &mut MultiplexedConnection {
        self.db.async_conn_mut()
    }

    #[inline]
    async fn _hscan_key_iter<'a>(
        &'a mut self,
        name: Key,
    ) -> Result<Box<dyn Iterator<Item = Result<Key>> + 'a + Send>> {
        let iter = Box::new(KeyIter {
            iter: self.async_conn_mut().hscan::<_, (Key, ())>(name).await?,
        });
        Ok(iter)
    }

    #[inline]
    async fn _retain<'a, F, Out, V>(&'a mut self, f: F) -> Result<()>
    where
        F: Fn(Result<(Key, V)>) -> Out + Send + Sync,
        Out: Future<Output = bool> + Send + 'a,
        V: DeserializeOwned + Sync + Send + 'a,
    {
        let name = self.name.clone();
        let async_conn_mut = self.async_conn_mut();
        let batch_size = 20;

        let mut removeds = Vec::new();
        //Remove hash
        let mut iter = async_conn_mut
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
            async_conn_mut.hdel(name.as_slice(), batch).await?;
        }

        Ok(())
    }

    #[inline]
    async fn _retain_with_key<'a, F, Out>(&'a mut self, f: F) -> Result<()>
    where
        F: Fn(Result<Key>) -> Out + Send + Sync,
        Out: Future<Output = bool> + Send + 'a,
    {
        let name = self.name.clone();
        let async_conn_mut = self.async_conn_mut();

        let batch_size = 20;

        let mut removeds = Vec::new();
        //Remove hash
        let mut iter = async_conn_mut
            .hscan::<_, (Key, ())>(name.as_slice())
            .await?;
        while let Some((key, _)) = iter.next_item().await {
            if !f(Ok(key.clone())).await {
                removeds.push(key);
            }
        }

        drop(iter);
        for batch in removeds.chunks(batch_size) {
            async_conn_mut.hdel(name.as_slice(), batch).await?;
        }

        Ok(())
    }
}

#[async_trait]
impl Map for RedisStorageMap {
    #[inline]
    async fn insert<K, V>(&mut self, key: K, val: &V) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: Serialize + Sync + Send + ?Sized,
    {
        //HSET key field value
        let name = self.name.clone();
        self.async_conn_mut()
            .hset(name, key.as_ref(), bincode::serialize(val)?)
            .await?;
        Ok(())
    }

    #[inline]
    async fn get<K, V>(&mut self, key: K) -> Result<Option<V>>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: DeserializeOwned + Sync + Send,
    {
        //HSET key field value
        let name = self.name.clone();
        let res: Option<Vec<u8>> = self.async_conn_mut().hget(name, key.as_ref()).await?;
        if let Some(res) = res {
            Ok(Some(bincode::deserialize::<V>(res.as_ref())?))
        } else {
            Ok(None)
        }
    }

    #[inline]
    async fn remove<K>(&mut self, key: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        //HDEL key field [field ...]
        let name = self.name.clone();
        self.async_conn_mut().hdel(name, key.as_ref()).await?;
        Ok(())
    }

    #[inline]
    async fn contains_key<K: AsRef<[u8]> + Sync + Send>(&mut self, key: K) -> Result<bool> {
        //HEXISTS key field
        let name = self.name.clone();
        let res = self.async_conn_mut().hexists(name, key.as_ref()).await?;
        Ok(res)
    }

    #[inline]
    async fn len(&mut self) -> Result<usize> {
        //HLEN key
        let name = self.name.clone();
        Ok(self.async_conn_mut().hlen(name).await?)
    }

    #[inline]
    async fn is_empty(&mut self) -> Result<bool> {
        //HSCAN key cursor [MATCH pattern] [COUNT count]
        let name = self.name.clone();
        let res = self
            .async_conn_mut()
            .hscan::<_, Vec<u8>>(name)
            .await?
            .next_item()
            .await
            .is_none();
        Ok(res)
    }

    #[inline]
    async fn clear(&mut self) -> Result<()> {
        //DEL key [key ...]
        let name = self.name.clone();
        self.async_conn_mut().del(name).await?;
        Ok(())
    }

    #[inline]
    async fn remove_and_fetch<K, V>(&mut self, key: K) -> Result<Option<V>>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: DeserializeOwned + Sync + Send,
    {
        //HSET key field value
        //HDEL key field [field ...]
        let name = self.name.clone();
        let conn = self.async_conn_mut();
        let (res, _): (Option<Vec<u8>>, isize) = redis::pipe()
            .atomic()
            .hget(name.as_slice(), key.as_ref())
            .hdel(name.as_slice(), key.as_ref())
            .query_async(conn)
            .await?;

        if let Some(res) = res {
            Ok(Some(bincode::deserialize::<V>(res.as_ref())?))
        } else {
            Ok(None)
        }
    }

    #[inline]
    async fn remove_with_prefix<K>(&mut self, prefix: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        let name = self.name.clone();
        let conn = self.async_conn_mut();
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
    async fn batch_insert<V>(&mut self, key_vals: Vec<(Key, V)>) -> Result<()>
    where
        V: Serialize + Sync + Send,
    {
        let key_vals = key_vals
            .into_iter()
            .map(|(k, v)| {
                bincode::serialize(&v)
                    .map(move |v| (k, v))
                    .map_err(|e| anyhow!(e))
            })
            .collect::<Result<Vec<_>>>()?;
        let name = self.name.clone();
        self.async_conn_mut()
            .hset_multiple(name, key_vals.as_slice())
            .await?;
        Ok(())
    }

    #[inline]
    async fn batch_remove(&mut self, keys: Vec<Key>) -> Result<()> {
        let name = self.name.clone();
        self.async_conn_mut().hdel(name, keys).await?;
        Ok(())
    }

    #[inline]
    async fn iter<'a, V>(&'a mut self) -> Result<Box<dyn Iterator<Item = IterItem<'a, V>> + 'a>>
    where
        V: DeserializeOwned + Sync + Send + 'a,
    {
        let name = self.name.clone();
        let iter = Iter {
            iter: self
                .async_conn_mut()
                .hscan::<_, (Key, Vec<u8>)>(name)
                .await?,
            _m: std::marker::PhantomData,
        };
        Ok(Box::new(iter))
    }

    #[inline]
    async fn key_iter<'a>(&'a mut self) -> Result<Box<dyn Iterator<Item = Result<Key>> + 'a>> {
        let name = self.name.clone();
        let iter = KeyIter {
            iter: self.db.async_conn.hscan::<_, (Key, ())>(name).await?,
        };
        Ok(Box::new(iter))
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
        let name = self.name.clone();
        let mut prefix = prefix.as_ref().to_vec();
        prefix.push(b'*');
        let iter = Iter {
            iter: self
                .async_conn_mut()
                .hscan_match::<_, _, (Key, Vec<u8>)>(name, prefix.as_slice())
                .await?,
            _m: std::marker::PhantomData,
        };
        Ok(Box::new(iter))
    }

    #[inline]
    async fn retain<'a, F, Out, V>(&'a mut self, f: F) -> Result<()>
    where
        F: Fn(Result<(Key, V)>) -> Out + Send + Sync,
        Out: Future<Output = bool> + Send + 'a,
        V: DeserializeOwned + Sync + Send + 'a,
    {
        self._retain(f).await
    }

    #[inline]
    async fn retain_with_key<'a, F, Out>(&'a mut self, f: F) -> Result<()>
    where
        F: Fn(Result<Key>) -> Out + Send + Sync,
        Out: Future<Output = bool> + Send + 'a,
    {
        self._retain_with_key(f).await
    }
}

#[derive(Clone)]
pub struct RedisStorageList {
    name: Key,
    pub(crate) db: RedisStorageDB,
}

impl RedisStorageList {
    #[inline]
    pub(crate) fn new(name: Key, db: RedisStorageDB) -> Self {
        Self { name, db }
    }

    #[inline]
    pub(crate) fn name(&self) -> &[u8] {
        self.name.as_slice()
    }

    #[inline]
    pub(crate) fn async_conn(&self) -> MultiplexedConnection {
        self.db.async_conn()
    }

    #[inline]
    pub(crate) fn conn(&self) -> Result<Connection> {
        self.db.conn()
    }
}

#[async_trait]
impl List for RedisStorageList {
    async fn push<V>(&self, val: &V) -> Result<()>
    where
        V: Serialize + Sync + Send,
    {
        //RPUSH key value [value ...]
        self.async_conn()
            .rpush(self.name.as_slice(), bincode::serialize(val)?)
            .await?;
        Ok(())
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
        let data = bincode::serialize(val)?;

        let key = self.name.as_slice();
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

    async fn pop<V>(&self) -> Result<Option<V>>
    where
        V: DeserializeOwned + Sync + Send,
    {
        //LPOP key
        let removed = self
            .async_conn()
            .lpop::<_, Option<Vec<u8>>>(self.name.as_slice(), None)
            .await?;

        let removed = if let Some(v) = removed {
            Some(bincode::deserialize::<V>(v.as_ref()).map_err(|e| anyhow!(e))?)
        } else {
            None
        };

        Ok(removed)
    }

    async fn all<V>(&self) -> Result<Vec<V>>
    where
        V: DeserializeOwned + Sync + Send,
    {
        //LRANGE key 0 -1
        let all = self
            .async_conn()
            .lrange::<_, Vec<Vec<u8>>>(self.name.as_slice(), 0, -1)
            .await?;
        all.iter()
            .map(|v| bincode::deserialize::<V>(v.as_ref()).map_err(|e| anyhow!(e)))
            .collect::<Result<Vec<_>>>()
    }

    async fn get_index<V>(&self, idx: usize) -> Result<Option<V>>
    where
        V: DeserializeOwned + Sync + Send,
    {
        //LINDEX key index
        let val = self
            .async_conn()
            .lindex::<_, Option<Vec<u8>>>(self.name.as_slice(), idx as isize)
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
        Ok(self.async_conn().llen(self.name.as_slice()).await?)
    }

    #[inline]
    async fn is_empty(&self) -> Result<bool> {
        Ok(self.len().await? == 0)
    }

    #[inline]
    async fn clear(&self) -> Result<()> {
        self.async_conn().del(self.name.as_slice()).await?;
        Ok(())
    }

    fn iter<'a, V>(&'a self) -> Result<Box<dyn Iterator<Item = Result<V>> + 'a>>
    where
        V: DeserializeOwned + Sync + Send + 'a + 'static,
    {
        Ok(Box::new(ListValIter::new(
            self.name.as_slice(),
            self.conn()?,
        )))
    }
}

pub struct ListValIter<'a, V> {
    name: &'a [u8],
    conn: Connection,
    start: isize,
    limit: isize,
    catch_vals: Vec<Vec<u8>>,
    _m: std::marker::PhantomData<V>,
}

impl<'a, V> ListValIter<'a, V> {
    fn new(name: &'a [u8], conn: Connection) -> Self {
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

impl<'a, V> Iterator for ListValIter<'a, V>
where
    V: DeserializeOwned + Sync + Send + 'static,
{
    type Item = Result<V>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(val) = self.catch_vals.pop() {
            return Some(bincode::deserialize::<V>(val.as_ref()).map_err(|e| anyhow!(e)));
        }

        let vals = block_in_place(|| {
            self.conn
                .lrange::<_, Vec<Vec<u8>>>(self.name, self.start, self.start + self.limit)
        });
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

pub struct Iter<'a, V> {
    iter: redis::AsyncIter<'a, (Key, Vec<u8>)>,
    _m: std::marker::PhantomData<V>,
}

impl<'a, V> Iterator for Iter<'a, V>
where
    V: DeserializeOwned + Sync + Send + 'a,
{
    type Item = IterItem<'a, V>;

    fn next(&mut self) -> Option<Self::Item> {
        let item = block_in_place(move || {
            tokio::runtime::Handle::current().block_on(async move {
                let item = self.iter.next_item().await;
                item.map(|(key, v)| match bincode::deserialize::<V>(v.as_ref()) {
                    Ok(v) => Ok((key, v)),
                    Err(e) => Err(anyhow::Error::new(e)),
                })
            })
        });
        item
    }
}

pub struct KeyIter<'a> {
    iter: redis::AsyncIter<'a, (Key, ())>,
}

impl<'a> Iterator for KeyIter<'a> {
    type Item = Result<Key>;

    fn next(&mut self) -> Option<Self::Item> {
        block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(async move { self.iter.next_item().await.map(|(key, _)| Ok(key)) })
        })
    }
}

pub struct MapListKeyIter<'a> {
    list_prefix: Key,
    iter: redis::AsyncIter<'a, Key>,
}

impl<'a> Iterator for MapListKeyIter<'a> {
    type Item = Result<Key>;

    fn next(&mut self) -> Option<Self::Item> {
        block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                self.iter.next_item().await.map(|key| {
                    let key = key[self.list_prefix.len()..].to_vec();
                    Ok(key)
                })
            })
        })
    }
}
