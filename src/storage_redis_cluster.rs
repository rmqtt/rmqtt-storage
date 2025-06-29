use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use async_trait::async_trait;
use redis::{
    aio::{ConnectionLike, ConnectionManager, ConnectionManagerConfig},
    cluster::ClusterClient,
    cluster_async::ClusterConnection,
    cluster_routing::get_slot,
    pipe, AsyncCommands, Cmd,
};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

use crate::storage::{AsyncIterator, IterItem, Key, List, Map, StorageDB};
use crate::{Result, StorageList, StorageMap};

#[allow(unused_imports)]
use crate::{timestamp_millis, TimestampMillis};

use crate::storage::{KEY_PREFIX, KEY_PREFIX_LEN, LIST_NAME_PREFIX, MAP_NAME_PREFIX, SEPARATOR};

type RedisConnection = ClusterConnection;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedisConfig {
    pub urls: Vec<String>,
    pub prefix: String,
}

impl Default for RedisConfig {
    fn default() -> Self {
        RedisConfig {
            urls: Vec::default(),
            prefix: "__def".into(),
        }
    }
}

#[derive(Clone)]
pub struct RedisStorageDB {
    prefix: Key,
    async_conn: RedisConnection,
    nodes: Vec<ConnectionManager>,
    nodes_update_time: TimestampMillis,
}

impl RedisStorageDB {
    #[inline]
    pub(crate) async fn new(cfg: RedisConfig) -> Result<Self> {
        let prefix = [cfg.prefix.as_bytes(), SEPARATOR].concat();

        let client = ClusterClient::builder(cfg.urls)
            .retry_wait_formula(2, 100)
            .retries(2)
            .connection_timeout(Duration::from_secs(15))
            .response_timeout(Duration::from_secs(10))
            .build()?;

        let async_conn = client.get_async_connection().await?;

        let mut db = Self {
            prefix,
            async_conn,
            nodes: Vec::new(),
            nodes_update_time: timestamp_millis(),
        }
        .cleanup();
        db.refresh_cluster_nodes().await?;
        Ok(db)
    }

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

    #[inline]
    fn async_conn(&self) -> RedisConnection {
        self.async_conn.clone()
    }

    #[inline]
    fn async_conn_mut(&mut self) -> &mut RedisConnection {
        &mut self.async_conn
    }

    #[inline]
    async fn refresh_cluster_nodes(&mut self) -> Result<()> {
        let slots = self
            .async_conn()
            .req_packed_command(Cmd::new().arg("CLUSTER").arg("SLOTS"))
            .await?;

        let mut addrs = Vec::new();
        for slot in slots
            .as_sequence()
            .map(|arrs| {
                arrs.iter()
                    .filter_map(|obj| obj.as_sequence())
                    .collect::<Vec<_>>()
            })
            .iter()
            .flatten()
            .collect::<Vec<_>>()
        {
            if let Some(addr_info) = slot.get(2) {
                if let Some(addr_items) = addr_info.as_sequence() {
                    if addr_items.len() > 1 {
                        if let (redis::Value::BulkString(addr), redis::Value::Int(port)) =
                            (&addr_items[0], &addr_items[1])
                        {
                            let addr =
                                format!("redis://{}:{}", String::from_utf8_lossy(addr), *port);
                            addrs.push(addr);
                        }
                    }
                }
            }
        }

        // let mut nodes = BTreeMap::new();
        let mut nodes = Vec::new();
        for addr in addrs {
            let client = match redis::Client::open(addr.as_str()) {
                Ok(c) => c,
                Err(e) => {
                    log::error!("open redis node error, addr is {}, {:?}", addr, e);
                    return Err(anyhow!(e));
                }
            };
            let mgr_cfg = ConnectionManagerConfig::default()
                .set_exponent_base(100)
                .set_factor(2)
                .set_number_of_retries(2)
                .set_connection_timeout(Duration::from_secs(15))
                .set_response_timeout(Duration::from_secs(10));
            let conn = match client.get_connection_manager_with_config(mgr_cfg).await {
                Ok(conn) => conn,
                Err(e) => {
                    log::error!("get redis connection error, addr {:?}, {:?}", addr, e);
                    return Err(anyhow!(e));
                }
            };
            nodes.push(conn);
        }
        self.nodes = nodes;
        log::info!("nodes.len(): {:?}", self.nodes.len());
        Ok(())
    }

    #[inline]
    async fn nodes_mut(&mut self) -> Result<&mut Vec<ConnectionManager>> {
        //Refresh after a certain interval.
        if (timestamp_millis() - self.nodes_update_time) > 5000 {
            self.refresh_cluster_nodes().await?;
        }
        Ok(&mut self.nodes)
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
        _key: K,
        _val: &V,
        _expire_interval: Option<TimestampMillis>,
    ) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
        V: serde::ser::Serialize + Sync + Send,
    {
        #[cfg(not(feature = "len"))]
        {
            let full_key = self.make_full_key(_key.as_ref());
            if let Some(expire_interval) = _expire_interval {
                let mut async_conn = self.async_conn();
                pipe()
                    .atomic()
                    .set(full_key.as_slice(), bincode::serialize(_val)?)
                    .pexpire(full_key.as_slice(), expire_interval)
                    .query_async::<()>(&mut async_conn)
                    .await?;
            } else {
                let _: () = self
                    .async_conn()
                    .set(full_key, bincode::serialize(_val)?)
                    .await?;
            }
        }
        #[cfg(feature = "len")]
        {
            return Err(anyhow!("unsupported!"));
            //@TODO ...
            #[allow(unreachable_code)]
            {
                let full_key = self.make_full_key(_key.as_ref());
                let db_zkey = self.make_len_sortedset_key();
                let mut async_conn = self.async_conn();
                if get_slot(db_zkey.as_slice()) == get_slot(full_key.as_slice()) {
                    if let Some(expire_interval) = _expire_interval {
                        pipe()
                            .atomic()
                            .set(full_key.as_slice(), bincode::serialize(_val)?)
                            .pexpire(full_key.as_slice(), expire_interval)
                            .zadd(db_zkey, _key.as_ref(), timestamp_millis() + expire_interval)
                            .query_async::<()>(&mut async_conn)
                            .await?;
                    } else {
                        pipe()
                            .atomic()
                            .set(full_key.as_slice(), bincode::serialize(_val)?)
                            .zadd(db_zkey, _key.as_ref(), i64::MAX)
                            .query_async::<()>(&mut async_conn)
                            .await?;
                    }
                } else {
                    //
                    if let Some(expire_interval) = _expire_interval {
                        let _: () = pipe()
                            .atomic()
                            .set(full_key.as_slice(), bincode::serialize(_val)?)
                            .pexpire(full_key.as_slice(), expire_interval)
                            .query_async(&mut async_conn)
                            .await?;
                        let _: () = async_conn
                            .zadd(db_zkey, _key.as_ref(), timestamp_millis() + expire_interval)
                            .await?;
                    } else {
                        let _: () = async_conn
                            .set(full_key.as_slice(), bincode::serialize(_val)?)
                            .await?;
                        let _: () = async_conn.zadd(db_zkey, _key.as_ref(), i64::MAX).await?;
                    }
                }
            }
        }

        Ok(())
    }

    #[inline]
    async fn _batch_insert<V>(
        &self,
        _key_val_expires: Vec<(Key, Key, V, Option<TimestampMillis>)>,
    ) -> Result<()>
    where
        V: serde::ser::Serialize + Sync + Send,
    {
        #[cfg(not(feature = "len"))]
        {
            let keys_vals: Vec<(&Key, Vec<u8>)> = _key_val_expires
                .iter()
                .map(|(_, full_key, v, _)| {
                    bincode::serialize(&v)
                        .map(move |v| (full_key, v))
                        .map_err(|e| anyhow!(e))
                })
                .collect::<Result<Vec<_>>>()?;

            let mut async_conn = self.async_conn();
            let mut p = pipe();
            let mut rpipe = p.atomic().mset(keys_vals.as_slice());
            for (_, full_key, _, at) in _key_val_expires {
                if let Some(at) = at {
                    rpipe = rpipe.expire(full_key, at);
                }
            }
            rpipe.query_async::<()>(&mut async_conn).await?;
            Ok(())
        }

        #[cfg(feature = "len")]
        {
            return Err(anyhow!("unsupported!"));
            //@TODO ...
            #[allow(unreachable_code)]
            {
                for (k, _, v, expire) in _key_val_expires {
                    self._insert(k.as_slice(), &v, expire).await?;
                }
                Ok(())
            }
        }
    }

    #[inline]
    async fn _batch_remove(&self, _keys: Vec<Key>) -> Result<()> {
        #[cfg(not(feature = "len"))]
        {
            let full_keys = _keys
                .iter()
                .map(|k| self.make_full_key(k))
                .collect::<Vec<_>>();
            let _: () = self.async_conn().del(full_keys).await?;
        }
        #[cfg(feature = "len")]
        {
            return Err(anyhow!("unsupported!"));
            #[allow(unreachable_code)]
            //@TODO ...
            {
                for key in _keys {
                    self._remove(key).await?;
                }
            }
        }
        Ok(())
    }

    #[inline]
    async fn _counter_incr<K>(
        &self,
        _key: K,
        _increment: isize,
        _expire_interval: Option<TimestampMillis>,
    ) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        #[cfg(not(feature = "len"))]
        {
            let full_key = self.make_full_key(_key.as_ref());
            if let Some(expire_interval) = _expire_interval {
                let mut async_conn = self.async_conn();
                pipe()
                    .atomic()
                    .incr(full_key.as_slice(), _increment)
                    .pexpire(full_key.as_slice(), expire_interval)
                    .query_async::<()>(&mut async_conn)
                    .await?;
            } else {
                let _: () = self.async_conn().incr(full_key, _increment).await?;
            }
        }
        #[cfg(feature = "len")]
        {
            return Err(anyhow!("unsupported!"));
            //@TODO ...
            #[allow(unreachable_code)]
            {
                let full_key = self.make_full_key(_key.as_ref());
                let db_zkey = self.make_len_sortedset_key();
                if get_slot(&db_zkey) == get_slot(full_key.as_slice()) {
                    let mut async_conn = self.async_conn();
                    if let Some(expire_interval) = _expire_interval {
                        pipe()
                            .atomic()
                            .incr(full_key.as_slice(), _increment)
                            .pexpire(full_key.as_slice(), expire_interval)
                            .zadd(db_zkey, _key.as_ref(), timestamp_millis() + expire_interval)
                            .query_async::<()>(&mut async_conn)
                            .await?;
                    } else {
                        pipe()
                            .atomic()
                            .incr(full_key.as_slice(), _increment)
                            .zadd(db_zkey, _key.as_ref(), i64::MAX)
                            .query_async::<()>(&mut async_conn)
                            .await?;
                    }
                } else {
                    let mut async_conn = self.async_conn();
                    if let Some(expire_interval) = _expire_interval {
                        let _: () = pipe()
                            .atomic()
                            .incr(full_key.as_slice(), _increment)
                            .pexpire(full_key.as_slice(), expire_interval)
                            .query_async::<()>(&mut async_conn)
                            .await?;
                        let _: () = async_conn
                            .zadd(db_zkey, _key.as_ref(), timestamp_millis() + expire_interval)
                            .await?;
                    } else {
                        let _: () = async_conn.incr(full_key.as_slice(), _increment).await?;
                        let _: () = async_conn.zadd(db_zkey, _key.as_ref(), i64::MAX).await?;
                    }
                }
            }
        }
        Ok(())
    }

    #[inline]
    async fn _counter_decr<K>(
        &self,
        _key: K,
        _decrement: isize,
        _expire_interval: Option<TimestampMillis>,
    ) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        #[cfg(not(feature = "len"))]
        {
            let full_key = self.make_full_key(_key.as_ref());
            if let Some(expire_interval) = _expire_interval {
                let mut async_conn = self.async_conn();
                pipe()
                    .atomic()
                    .decr(full_key.as_slice(), _decrement)
                    .pexpire(full_key.as_slice(), expire_interval)
                    .query_async::<()>(&mut async_conn)
                    .await?;
            } else {
                let _: () = self.async_conn().decr(full_key, _decrement).await?;
            }
        }
        #[cfg(feature = "len")]
        {
            return Err(anyhow!("unsupported!"));
            //@TODO ...
            #[allow(unreachable_code)]
            {
                let full_key = self.make_full_key(_key.as_ref());
                let db_zkey = self.make_len_sortedset_key();
                let mut async_conn = self.async_conn();
                if get_slot(&db_zkey) == get_slot(full_key.as_slice()) {
                    if let Some(expire_interval) = _expire_interval {
                        pipe()
                            .atomic()
                            .decr(full_key.as_slice(), _decrement)
                            .pexpire(full_key.as_slice(), expire_interval)
                            .zadd(db_zkey, _key.as_ref(), timestamp_millis() + expire_interval)
                            .query_async::<()>(&mut async_conn)
                            .await?;
                    } else {
                        pipe()
                            .atomic()
                            .decr(full_key.as_slice(), _decrement)
                            .zadd(db_zkey, _key.as_ref(), i64::MAX)
                            .query_async::<()>(&mut async_conn)
                            .await?;
                    }
                } else {
                    //
                    if let Some(expire_interval) = _expire_interval {
                        let _: () = pipe()
                            .atomic()
                            .decr(full_key.as_slice(), _decrement)
                            .pexpire(full_key.as_slice(), expire_interval)
                            .query_async::<()>(&mut async_conn)
                            .await?;
                        let _: () = async_conn
                            .zadd(db_zkey, _key.as_ref(), timestamp_millis() + expire_interval)
                            .await?;
                    } else {
                        let _: () = async_conn.decr(full_key.as_slice(), _decrement).await?;
                        let _: () = async_conn.zadd(db_zkey, _key.as_ref(), i64::MAX).await?;
                    }
                }
            }
        }
        Ok(())
    }

    #[inline]
    async fn _counter_set<K>(
        &self,
        _key: K,
        _val: isize,
        _expire_interval: Option<TimestampMillis>,
    ) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        #[cfg(not(feature = "len"))]
        {
            let full_key = self.make_full_key(_key.as_ref());
            if let Some(expire_interval) = _expire_interval {
                let mut async_conn = self.async_conn();
                pipe()
                    .atomic()
                    .set(full_key.as_slice(), _val)
                    .pexpire(full_key.as_slice(), expire_interval)
                    .query_async::<()>(&mut async_conn)
                    .await?;
            } else {
                let _: () = self.async_conn().set(full_key, _val).await?;
            }
        }
        #[cfg(feature = "len")]
        {
            return Err(anyhow!("unsupported!"));
            //@TODO ...
            #[allow(unreachable_code)]
            {
                let full_key = self.make_full_key(_key.as_ref());
                let db_zkey = self.make_len_sortedset_key();
                let mut async_conn = self.async_conn();
                if get_slot(&db_zkey) == get_slot(full_key.as_slice()) {
                    if let Some(expire_interval) = _expire_interval {
                        pipe()
                            .atomic()
                            .set(full_key.as_slice(), _val)
                            .pexpire(full_key.as_slice(), expire_interval)
                            .zadd(db_zkey, _key.as_ref(), timestamp_millis() + expire_interval)
                            .query_async::<()>(&mut async_conn)
                            .await?;
                    } else {
                        pipe()
                            .atomic()
                            .set(full_key.as_slice(), _val)
                            .zadd(db_zkey, _key.as_ref(), i64::MAX)
                            .query_async::<()>(&mut async_conn)
                            .await?;
                    }
                } else {
                    //
                    if let Some(expire_interval) = _expire_interval {
                        pipe()
                            .atomic()
                            .set(full_key.as_slice(), _val)
                            .pexpire(full_key.as_slice(), expire_interval)
                            .query_async::<()>(&mut async_conn)
                            .await?;
                        let _: () = async_conn
                            .zadd(db_zkey, _key.as_ref(), timestamp_millis() + expire_interval)
                            .await?;
                    } else {
                        let _: () = async_conn.set(full_key.as_slice(), _val).await?;
                        let _: () = async_conn.zadd(db_zkey, _key.as_ref(), i64::MAX).await?;
                    }
                }
            }
        }

        Ok(())
    }

    #[inline]
    async fn _remove<K>(&self, _key: K) -> Result<()>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        #[cfg(not(feature = "len"))]
        {
            let full_key = self.make_full_key(_key.as_ref());
            let _: () = self.async_conn().del(full_key).await?;
            Ok(())
        }
        #[cfg(feature = "len")]
        {
            return Err(anyhow!("unsupported!"));
            //@TODO ...
            #[allow(unreachable_code)]
            {
                let full_key = self.make_full_key(_key.as_ref());
                let db_zkey = self.make_len_sortedset_key();

                let mut async_conn = self.async_conn();
                if get_slot(&db_zkey) == get_slot(_key.as_ref()) {
                    pipe()
                        .atomic()
                        .del(full_key.as_slice())
                        .zrem(db_zkey, _key.as_ref())
                        .query_async::<()>(&mut async_conn)
                        .await?;
                } else {
                    let _: () = async_conn.zrem(db_zkey, _key.as_ref()).await?;
                    let _: () = async_conn.del(full_key).await?;
                }
                Ok(())
            }
        }
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
        let _: () = self.async_conn().del(map_full_name).await?;
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
        let _: () = self.async_conn().del(list_full_name).await?;
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
            let mut slot_keys_vals_expires = key_vals
                .into_iter()
                .map(|(k, v)| {
                    let full_key = self.make_full_key(k.as_slice());
                    (get_slot(&full_key), (k, full_key, v, None))
                })
                .collect::<Vec<_>>();

            if !slot_keys_vals_expires.is_empty() {
                for keys_vals_expires in transform_by_slot(slot_keys_vals_expires) {
                    self._batch_insert(keys_vals_expires).await?;
                }
            } else if let Some((_, keys_vals_expires)) = slot_keys_vals_expires.pop() {
                self._batch_insert(vec![keys_vals_expires]).await?;
            }
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
        return Err(anyhow!("unsupported!"));
        //@TODO ...
        #[allow(unreachable_code)]
        {
            let db_zkey = self.make_len_sortedset_key();
            let mut async_conn = self.async_conn();
            let (_, count) = pipe()
                .zrembyscore(db_zkey.as_slice(), 0, timestamp_millis())
                .zcard(db_zkey.as_slice())
                .query_async::<(i64, usize)>(&mut async_conn)
                .await?;
            Ok(count)
        }
    }

    #[inline]
    async fn db_size(&self) -> Result<usize> {
        let mut dbsize = 0;
        for mut async_conn in self.nodes.clone() {
            //DBSIZE
            let dbsize_val = redis::pipe()
                .cmd("DBSIZE")
                .query_async::<redis::Value>(&mut async_conn)
                .await?;
            dbsize += dbsize_val
                .as_sequence()
                .and_then(|vs| {
                    vs.iter().next().and_then(|v| {
                        if let redis::Value::Int(v) = v {
                            Some(*v)
                        } else {
                            None
                        }
                    })
                })
                .unwrap_or_default();
        }
        Ok(dbsize as usize)
    }

    #[inline]
    #[cfg(feature = "ttl")]
    async fn expire_at<K>(&self, _key: K, _at: TimestampMillis) -> Result<bool>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        #[cfg(not(feature = "len"))]
        {
            let full_name = self.make_full_key(_key.as_ref());
            let res = self
                .async_conn()
                .pexpire_at::<_, bool>(full_name, _at)
                .await?;
            Ok(res)
        }
        #[cfg(feature = "len")]
        {
            return Err(anyhow!("unsupported!"));
            //@TODO ...
            #[allow(unreachable_code)]
            {
                let full_name = self.make_full_key(_key.as_ref());
                let db_zkey = self.make_len_sortedset_key();
                let mut async_conn = self.async_conn();
                if get_slot(&db_zkey) == get_slot(full_name.as_slice()) {
                    let (_, res) = pipe()
                        .atomic()
                        .zadd(db_zkey, _key.as_ref(), _at)
                        .pexpire_at(full_name.as_slice(), _at)
                        .query_async::<(i64, bool)>(&mut async_conn)
                        .await?;
                    Ok(res)
                } else {
                    let res = async_conn.zadd(db_zkey, _key.as_ref(), _at).await?;
                    let _: () = async_conn.pexpire_at(full_name.as_slice(), _at).await?;
                    Ok(res)
                }
            }
        }
    }

    #[inline]
    #[cfg(feature = "ttl")]
    async fn expire<K>(&self, _key: K, _dur: TimestampMillis) -> Result<bool>
    where
        K: AsRef<[u8]> + Sync + Send,
    {
        #[cfg(not(feature = "len"))]
        {
            let full_name = self.make_full_key(_key.as_ref());
            let res = self
                .async_conn()
                .pexpire::<_, bool>(full_name, _dur)
                .await?;
            Ok(res)
        }
        #[cfg(feature = "len")]
        {
            return Err(anyhow!("unsupported!"));
            //@TODO ...
            #[allow(unreachable_code)]
            {
                let full_name = self.make_full_key(_key.as_ref());
                let db_zkey = self.make_len_sortedset_key();
                let mut async_conn = self.async_conn();
                if get_slot(db_zkey.as_slice()) == get_slot(full_name.as_slice()) {
                    let (_, res) = pipe()
                        .atomic()
                        .zadd(db_zkey, _key.as_ref(), timestamp_millis() + _dur)
                        .pexpire(full_name.as_slice(), _dur)
                        .query_async::<(i64, bool)>(&mut async_conn)
                        .await?;
                    Ok(res)
                } else {
                    let _: () = async_conn
                        .zadd(db_zkey, _key.as_ref(), timestamp_millis() + _dur)
                        .await?;
                    let res = async_conn
                        .pexpire::<_, bool>(full_name.as_slice(), _dur)
                        .await?;
                    Ok(res)
                }
            }
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
        let db = self.clone();
        let pattern = self.make_map_prefix_match();

        let mut iters = Vec::new();
        for conn in self.nodes_mut().await?.iter_mut() {
            let iter = conn.scan_match::<_, Key>(pattern.as_slice()).await?;
            iters.push(iter);
        }

        let iter = AsyncMapIter { db, iters };
        Ok(Box::new(iter))
    }

    #[inline]
    async fn list_iter<'a>(
        &'a mut self,
    ) -> Result<Box<dyn AsyncIterator<Item = Result<StorageList>> + Send + 'a>> {
        let db = self.clone();
        let pattern = self.make_list_prefix_match();

        let mut iters = Vec::new();
        for conn in self.nodes_mut().await?.iter_mut() {
            let iter = conn.scan_match::<_, Key>(pattern.as_slice()).await?;
            iters.push(iter);
        }

        let iter = AsyncListIter { db, iters };
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

        let mut iters = Vec::new();
        for conn in self.nodes_mut().await?.iter_mut() {
            let iter = conn.scan_match::<_, Key>(pattern.as_slice()).await?;
            iters.push(iter);
        }
        Ok(Box::new(AsyncDbKeyIter { prefix_len, iters }))
    }

    #[inline]
    async fn info(&self) -> Result<Value> {
        Ok(serde_json::json!({
            "storage_engine": "RedisCluster",
            "dbsize": self.db_size().await?,
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

        //HSET key field value
        let _: () = async_conn.hset(name, key.as_ref(), val).await?;
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

        //HSET key field value
        let _: () = async_conn.hset_multiple(name, key_vals.as_slice()).await?;
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
        let _: () = self
            .async_conn()
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
        let _: () = self.async_conn().del(self.full_name.as_slice()).await?;
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
            let _: () = self
                .async_conn()
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

        //RPUSH key value [value ...]
        let _: () = async_conn.rpush(name, val).await?;
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

        //RPUSH key value [value ...]
        let _: () = async_conn.rpush(self.full_name.as_slice(), vals).await?;
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
        let _: () = self.async_conn().del(self.full_name.as_slice()).await?;
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
impl<V> AsyncIterator for AsyncListValIter<'_, V>
where
    V: DeserializeOwned + Sync + Send,
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
impl<V> AsyncIterator for AsyncIter<'_, V>
where
    V: DeserializeOwned + Sync + Send,
{
    type Item = IterItem<V>;

    async fn next(&mut self) -> Option<Self::Item> {
        let item = match self.iter.next_item().await {
            None => None,
            Some(Err(e)) => return Some(Err(anyhow::Error::new(e))),
            Some(Ok(item)) => Some(item),
        };
        item.map(|(key, v)| match bincode::deserialize::<V>(v.as_ref()) {
            Ok(v) => Ok((key, v)),
            Err(e) => Err(anyhow::Error::new(e)),
        })
    }
}

pub struct AsyncDbKeyIter<'a> {
    prefix_len: usize,
    iters: Vec<redis::AsyncIter<'a, Key>>,
}

#[async_trait]
impl AsyncIterator for AsyncDbKeyIter<'_> {
    type Item = Result<Key>;
    async fn next(&mut self) -> Option<Self::Item> {
        while let Some(iter) = self.iters.last_mut() {
            let item = match iter.next_item().await {
                None => None,
                Some(Err(e)) => Some(Err(anyhow::Error::new(e))),
                Some(Ok(key)) => Some(Ok(key[self.prefix_len..].to_vec())),
            };

            if item.is_some() {
                return item;
            }
            self.iters.pop();
            if self.iters.is_empty() {
                return None;
            }
        }
        None
    }
}

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

pub struct AsyncMapIter<'a> {
    db: RedisStorageDB,
    iters: Vec<redis::AsyncIter<'a, Key>>,
}

#[async_trait]
impl AsyncIterator for AsyncMapIter<'_> {
    type Item = Result<StorageMap>;

    async fn next(&mut self) -> Option<Self::Item> {
        while let Some(iter) = self.iters.last_mut() {
            let full_name = match iter.next_item().await {
                None => None,
                Some(Err(e)) => return Some(Err(anyhow::Error::new(e))),
                Some(Ok(key)) => Some(key),
            };

            if let Some(full_name) = full_name {
                let name = self.db.map_full_name_to_key(full_name.as_slice()).to_vec();
                let m = RedisStorageMap::new(name, full_name, self.db.clone());
                return Some(Ok(StorageMap::RedisCluster(m)));
            }
            self.iters.pop();
            if self.iters.is_empty() {
                return None;
            }
        }
        None
    }
}

pub struct AsyncListIter<'a> {
    db: RedisStorageDB,
    iters: Vec<redis::AsyncIter<'a, Key>>,
}

#[async_trait]
impl AsyncIterator for AsyncListIter<'_> {
    type Item = Result<StorageList>;

    async fn next(&mut self) -> Option<Self::Item> {
        while let Some(iter) = self.iters.last_mut() {
            let full_name = match iter.next_item().await {
                None => None,
                Some(Err(e)) => return Some(Err(anyhow::Error::new(e))),
                Some(Ok(key)) => Some(key),
            };

            if let Some(full_name) = full_name {
                let name = self.db.list_full_name_to_key(full_name.as_slice()).to_vec();
                let l = RedisStorageList::new(name, full_name, self.db.clone());
                return Some(Ok(StorageList::RedisCluster(l)));
            }
            self.iters.pop();
            if self.iters.is_empty() {
                return None;
            }
        }
        None
    }
}

#[inline]
fn transform_by_slot<T>(input: Vec<(u16, T)>) -> Vec<Vec<T>> {
    let mut grouped_data: BTreeMap<u16, Vec<T>> = BTreeMap::new();

    for (group_key, item) in input {
        grouped_data.entry(group_key).or_default().push(item);
    }

    grouped_data.into_values().collect()
}

const _: () = {
    if cfg!(feature = "len") {
        panic!("The `len` feature is not allowed in this file.");
    }
};
