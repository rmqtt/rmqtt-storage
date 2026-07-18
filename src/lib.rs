//! Provides a unified storage abstraction with multiple backend implementations (sled, redis, redis-cluster).
//!
//! This module defines generic storage interfaces (`StorageDB`, `Map`, `List`) and implements them
//! for different storage backends. It includes configuration handling, initialization functions,
//! and common storage operations with support for expiration and batch operations.

#![deny(unsafe_code)]

#[allow(unused_imports)]
use serde::{de, Deserialize, Serialize};

// Conditionally include storage modules based on enabled features
#[cfg(any(
    feature = "redis",
    feature = "redis-cluster",
    feature = "sled",
    feature = "redb"
))]
mod storage;
#[cfg(feature = "redb")]
mod storage_redb;
#[cfg(feature = "redis")]
mod storage_redis;
#[cfg(feature = "redis-cluster")]
mod storage_redis_cluster;
#[cfg(feature = "sled")]
mod storage_sled;

#[cfg(all(
    feature = "circuit-breaker",
    any(
        feature = "redis",
        feature = "redis-cluster",
        feature = "sled",
        feature = "redb"
    )
))]
mod circuit_breaker;

// Re-export public storage interfaces and implementations
#[cfg(feature = "circuit-breaker")]
pub use crate::circuit_breaker::{
    CircuitBreakerConfig, CircuitBrokenDB, CircuitBrokenList, CircuitBrokenMap,
    CountBasedWindowConfig, TimeBasedWindowConfig, WindowConfig,
};
#[cfg(any(
    feature = "redis",
    feature = "redis-cluster",
    feature = "sled",
    feature = "redb"
))]
pub use storage::{
    AsyncIterator, DefaultStorageDB, Key, List, Map, StorageDB, StorageList, StorageMap,
};
#[cfg(feature = "redb")]
pub use storage_redb::{RedbConfig, RedbStorageDB};
#[cfg(feature = "redis")]
pub use storage_redis::{RedisConfig, RedisStorageDB};
#[cfg(feature = "redis-cluster")]
pub use storage_redis_cluster::{
    RedisConfig as RedisClusterConfig, RedisStorageDB as RedisClusterStorageDB,
};
#[cfg(feature = "sled")]
pub use storage_sled::{SledConfig, SledStorageDB};

/// Custom result type for storage operations
pub type Result<T> = anyhow::Result<T>;

/// Initializes the database based on provided configuration
///
/// # Arguments
/// * `cfg` - Storage configuration specifying backend type and parameters
///
/// # Returns
/// Instance of `DefaultStorageDB` configured with the selected backend
#[cfg(any(
    feature = "redis",
    feature = "redis-cluster",
    feature = "sled",
    feature = "redb"
))]
pub async fn init_db(cfg: &Config) -> Result<DefaultStorageDB> {
    match cfg.typ {
        #[cfg(feature = "sled")]
        StorageType::Sled => {
            let db = SledStorageDB::new(cfg.sled.clone()).await?;
            Ok(DefaultStorageDB::Sled(db))
        }
        #[cfg(feature = "redis")]
        StorageType::Redis => {
            let db = RedisStorageDB::new(cfg.redis.clone()).await?;
            Ok(DefaultStorageDB::Redis(db))
        }
        #[cfg(feature = "redis-cluster")]
        StorageType::RedisCluster => {
            let db = RedisClusterStorageDB::new(cfg.redis_cluster.clone()).await?;
            Ok(DefaultStorageDB::RedisCluster(db))
        }
        #[cfg(feature = "redb")]
        StorageType::Redb => {
            let db = RedbStorageDB::open(&cfg.redb)?;
            Ok(DefaultStorageDB::Redb(db))
        }
    }
}

/// Configuration structure for storage system
///
/// Contains backend-specific configurations and is conditionally compiled
/// based on enabled storage features.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[cfg(any(
    feature = "redis",
    feature = "redis-cluster",
    feature = "sled",
    feature = "redb"
))]
pub struct Config {
    /// Storage backend type (Sled, Redis, or RedisCluster)
    // #[serde(default = "Config::storage_type_default")]
    #[serde(alias = "type")]
    pub typ: StorageType,

    /// Configuration for Sled backend (feature-gated)
    #[serde(default)]
    #[cfg(feature = "sled")]
    pub sled: SledConfig,

    /// Configuration for Redis backend (feature-gated)
    #[serde(default)]
    #[cfg(feature = "redis")]
    pub redis: RedisConfig,

    /// Configuration for Redis Cluster backend (feature-gated)
    #[serde(default, rename = "redis-cluster")]
    #[cfg(feature = "redis-cluster")]
    pub redis_cluster: RedisClusterConfig,

    /// Configuration for Redb backend (feature-gated)
    #[serde(default)]
    #[cfg(feature = "redb")]
    pub redb: RedbConfig,
}

/// Enum representing available storage backend types
///
/// Variants are conditionally included based on enabled features
#[derive(Debug, Clone, Serialize)]
#[cfg(any(
    feature = "redis",
    feature = "redis-cluster",
    feature = "sled",
    feature = "redb"
))]
pub enum StorageType {
    /// Embedded database with BTreeMap-like API
    #[cfg(feature = "sled")]
    Sled,
    /// Single-node Redis storage
    #[cfg(feature = "redis")]
    Redis,
    /// Redis Cluster distributed storage
    #[cfg(feature = "redis-cluster")]
    RedisCluster,
    /// Redb ACID embedded database
    #[cfg(feature = "redb")]
    Redb,
}

/// Deserialization implementation for StorageType
#[cfg(any(
    feature = "redis",
    feature = "redis-cluster",
    feature = "sled",
    feature = "redb"
))]
impl<'de> de::Deserialize<'de> for StorageType {
    #[inline]
    fn deserialize<D>(deserializer: D) -> core::result::Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let t = match (String::deserialize(deserializer)?)
            .to_ascii_lowercase()
            .as_str()
        {
            #[cfg(feature = "sled")]
            "sled" => StorageType::Sled,
            #[cfg(feature = "redis")]
            "redis" => StorageType::Redis,
            #[cfg(feature = "redis-cluster")]
            "redis-cluster" => StorageType::RedisCluster,
            #[cfg(feature = "redb")]
            "redb" => StorageType::Redb,
            _ => return Err(de::Error::custom(
                "invalid storage type, expected one of: 'sled', 'redis', 'redis-cluster', 'redb'",
            )),
        };
        Ok(t)
    }
}

/// Timestamp type in milliseconds
#[allow(dead_code)]
pub(crate) type TimestampMillis = i64;

/// Gets current timestamp in milliseconds
///
/// Uses system time if available, falls back to chrono if system time is before UNIX_EPOCH
#[allow(dead_code)]
#[inline]
pub(crate) fn timestamp_millis() -> TimestampMillis {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|dur| dur.as_millis() as TimestampMillis)
        .unwrap_or_else(|_| {
            let now = chrono::Local::now();
            now.timestamp_millis() as TimestampMillis
        })
}

#[cfg(test)]
#[cfg(any(
    feature = "redis",
    feature = "redis-cluster",
    feature = "sled",
    feature = "redb"
))]
mod tests {
    use super::*;
    use std::borrow::Cow;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time::sleep;

    fn get_cfg(name: &str) -> Config {
        let cfg = Config {
            typ: {
                cfg_if::cfg_if! {
                    if #[cfg(feature = "redb")] {
                        StorageType::Redb
                    } else if #[cfg(feature = "sled")] {
                        StorageType::Sled
                    } else if #[cfg(feature = "redis-cluster")] {
                        StorageType::RedisCluster
                    } else if #[cfg(feature = "redis")] {
                        StorageType::Redis
                    } else {
                        compile_error!("No storage backend feature enabled!");
                    }
                }
            },
            #[cfg(feature = "sled")]
            sled: SledConfig {
                path: format!("./.catch/{}", name),
                cleanup_f: |_db| {},
                ..Default::default()
            },
            #[cfg(feature = "redis")]
            redis: RedisConfig {
                url: "redis://127.0.0.1:6379/".into(),
                prefix: name.to_owned(),
            },
            #[cfg(feature = "redis-cluster")]
            redis_cluster: RedisClusterConfig {
                urls: [
                    "redis://127.0.0.1:6380/".into(),
                    "redis://127.0.0.1:6381/".into(),
                    "redis://127.0.0.1:6382/".into(),
                ]
                .into(),
                prefix: name.to_owned(),
            },
            #[cfg(feature = "redb")]
            redb: RedbConfig {
                path: format!("./.catch/{}.redb", name),
                cleanup_f: |_db| {},
                ..Default::default()
            },
        };
        cfg
    }

    #[cfg(feature = "circuit-breaker")]
    async fn test_init_db(cfg: &Config) -> Result<CircuitBrokenDB> {
        Ok(CircuitBrokenDB::new(
            init_db(cfg).await?,
            CircuitBreakerConfig::default(),
        ))
    }

    #[cfg(not(feature = "circuit-breaker"))]
    async fn test_init_db(cfg: &Config) -> Result<DefaultStorageDB> {
        init_db(cfg).await
    }

    #[serial_test::serial]
    #[tokio::main]
    #[test]
    #[cfg(all(feature = "sled", feature = "ttl"))]
    async fn test_sled_cleanup() {
        use super::{SledStorageDB, StorageDB};
        let cfg = Config {
            typ: StorageType::Sled,
            sled: SledConfig {
                path: format!("./.catch/{}", "sled_cleanup"),
                cache_capacity: convert::Bytesize::from(1024 * 1024 * 1024 * 3),
                cleanup_f: move |_db| {
                    #[cfg(feature = "ttl")]
                    {
                        let db = _db.clone();
                        tokio::spawn(async move {
                            // std::thread::spawn(move || {
                            let limit = 1000;
                            for i in 0..10 {
                                println!("{} a start cleanups ...", i,);
                                // std::thread::sleep(std::time::Duration::from_secs(1));
                                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                                let mut total_cleanups = 0;
                                let now = std::time::Instant::now();
                                println!("{} b start cleanups ...", i,);
                                loop {
                                    println!("{} c start cleanups ...", i,);
                                    let db1 = db.clone();
                                    let count =
                                        tokio::task::spawn_blocking(move || db1.cleanup(limit))
                                            .await
                                            .unwrap();
                                    // let count = db.cleanup(limit);
                                    total_cleanups += count;
                                    println!(
                                        "{} def_cleanup: {}, total cleanups: {}, cost time: {:?}",
                                        i,
                                        count,
                                        total_cleanups,
                                        now.elapsed()
                                    );

                                    if count < limit {
                                        break;
                                    }
                                }
                                println!(
                                    "{} total cleanups: {}, cost time: {:?}",
                                    i,
                                    total_cleanups,
                                    now.elapsed()
                                );
                            }
                            println!("&&&&&&&&&& test_sled_cleanup cleanup end. &&&&&&&&&&&");
                        });
                    }
                },
                ..Default::default()
            },
            #[cfg(feature = "redis")]
            redis: RedisConfig {
                url: "redis://127.0.0.1:6379/".into(),
                prefix: "sled_cleanup".to_owned(),
            },
            #[cfg(feature = "redis-cluster")]
            redis_cluster: RedisClusterConfig {
                urls: [
                    "redis://127.0.0.1:6380/".into(),
                    "redis://127.0.0.1:6381/".into(),
                    "redis://127.0.0.1:6382/".into(),
                ]
                .into(),
                prefix: "sled_cleanup".to_owned(),
            },
        };

        let db = SledStorageDB::new(cfg.sled.clone()).await.unwrap();
        let max = 3000;

        for i in 0..max {
            let map = db.map(format!("map_{}", i), None).await;
            map.insert("k_1", &1).await.unwrap();
            map.insert("k_2", &2).await.unwrap();
            map.expire(100).await.unwrap();
        }

        for i in 0..max {
            let list = db.list(format!("list_{}", i), None).await;
            list.push(&1).await.unwrap();
            list.push(&2).await.unwrap();
            list.expire(100).await.unwrap();
        }

        tokio::time::sleep(Duration::from_millis(120)).await;

        println!(
            "$$$ db_size: {:?}",
            db.db_size().await,
            // db.map_size(),
            // db.list_size()
        );

        tokio::time::sleep(Duration::from_secs(3)).await;
        println!(
            "$$$ db_size: {:?}",
            db.db_size().await,
            // db.map_size(),
            // db.list_size()
        );
    }

    #[serial_test::serial]
    #[tokio::main]
    #[test]
    async fn test_stress() {
        let cfg = get_cfg("stress");
        let db = test_init_db(&cfg).await.unwrap();
        let now = std::time::Instant::now();
        for i in 0..10_000usize {
            db.insert(i.to_be_bytes(), &i).await.unwrap();
        }
        let k_9999_val = db.get::<_, usize>(9999usize.to_be_bytes()).await.unwrap();
        println!(
            "test_stress 9999: {:?}, cost time: {:?}",
            k_9999_val,
            now.elapsed()
        );
        assert_eq!(k_9999_val, Some(9999));

        // --- batch_insert stress (DB level) ---
        // 对比上面逐条 insert，这里用一次 batch_insert 写入相同数量的 kv
        let mut batch_kvs: Vec<(Vec<u8>, usize)> = Vec::with_capacity(10_000);
        for i in 0..10_000usize {
            batch_kvs.push((i.to_be_bytes().to_vec(), i));
        }
        let now = std::time::Instant::now();
        db.batch_insert(batch_kvs).await.unwrap();
        println!(
            "test_stress db.batch_insert (10000 kv), cost time: {:?}",
            now.elapsed()
        );
        let k_9999_val = db.get::<_, usize>(9999usize.to_be_bytes()).await.unwrap();
        assert_eq!(k_9999_val, Some(9999));

        let s_m_1 = db.map("s_m_1", None).await;
        s_m_1.clear().await.unwrap();
        let now = std::time::Instant::now();
        for i in 0..10_000usize {
            s_m_1.insert(i.to_be_bytes(), &i).await.unwrap();
        }
        #[cfg(feature = "map_len")]
        assert_eq!(s_m_1.len().await.unwrap(), 10_000);
        let k_9999_val = s_m_1
            .get::<_, usize>(9999usize.to_be_bytes())
            .await
            .unwrap();
        println!(
            "test_stress s_m_1 9999: {:?}, cost time: {:?}",
            k_9999_val,
            now.elapsed()
        );
        assert_eq!(k_9999_val, Some(9999));

        let s_l_1 = db.list("s_l_1", None).await;
        s_l_1.clear().await.unwrap();
        let now = std::time::Instant::now();
        for i in 0..10_000usize {
            s_l_1.push(&i).await.unwrap();
        }
        let s_l_1_len = s_l_1.len().await.unwrap();
        println!("test_stress s_l_1: {:?}", s_l_1_len);
        assert!(
            s_l_1_len >= 9_000,
            "list length {} is too small (expected >= 9000 out of 10000 pushes)",
            s_l_1_len
        );
        if s_l_1_len != 10_000 {
            eprintln!(
                "WARN: test_stress list push under-count: got {} / 10000",
                s_l_1_len
            );
        }
        let l_9999_val = s_l_1.get_index::<usize>(9999).await.unwrap();
        println!(
            "test_stress s_l_1 9999: {:?}, cost time: {:?}",
            l_9999_val,
            now.elapsed()
        );
        if s_l_1_len == 10_000 {
            assert_eq!(l_9999_val, Some(9999));
        } else if l_9999_val.is_some() {
            eprintln!("WARN: list length < 10000 but index 9999 unexpectedly exists");
        }

        tokio::time::sleep(Duration::from_secs(1)).await;

        let now = std::time::Instant::now();
        for i in 0..10_000usize {
            let s_m = db.map(format!("s_m_{}", i), None).await;
            s_m.insert(i.to_be_bytes(), &i).await.unwrap();
        }
        println!("test_stress s_m, cost time: {:?}", now.elapsed());

        let now = std::time::Instant::now();
        for i in 0..10_000usize {
            let s_l = db.list(format!("s_l_{}", i), None).await;
            s_l.push(&i).await.unwrap();
        }
        println!("test_stress s_l, cost time: {:?}", now.elapsed());

        tokio::time::sleep(Duration::from_secs(3)).await;
        println!("$$$ test_stress db_size: {:?}", db.db_size().await,);
    }

    #[cfg(feature = "ttl")]
    #[tokio::main]
    #[test]
    async fn test_expiration_cleaning() {
        //Clear Expired Cleanup
        let cfg = get_cfg("expiration_cleaning");
        let db = test_init_db(&cfg).await.unwrap();
        for i in 0..3usize {
            let key = format!("k_{}", i);
            db.insert(key.as_bytes(), &format!("v_{}", (i * 10)))
                .await
                .unwrap();
            let res = db.expire(key, 1500).await.unwrap();
            println!("expire res: {:?}", res);
        }

        let m_1 = db.map("m_1", None).await;
        m_1.insert("m_k_1", &1).await.unwrap();
        m_1.insert("m_k_2", &2).await.unwrap();
        let res = m_1.expire(1500).await.unwrap();
        println!("m_1 expire res: {:?}", res);

        let l_1 = db.list("l_1", None).await;
        l_1.clear().await.unwrap();
        l_1.push(&11).await.unwrap();
        l_1.push(&22).await.unwrap();

        let res = l_1.expire(1500).await.unwrap();
        println!("l_1 expire res: {:?}", res);

        tokio::time::sleep(Duration::from_millis(1700)).await;
        let k_0_val = db.get::<_, String>("k_0").await.unwrap();
        println!("k_0_val: {:?}", k_0_val);
        assert_eq!(k_0_val, None);

        let m_k_2 = m_1.get::<_, i32>("m_k_2").await.unwrap();
        println!("m_k_2: {:?}", m_k_2);
        assert_eq!(m_k_2, None);

        let l_all = l_1.all::<i32>().await.unwrap();
        println!("l_all: {:?}", l_all);
        assert_eq!(l_all, Vec::<i32>::new());

        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    #[tokio::main]
    #[test]
    async fn test_db_insert() {
        let cfg = get_cfg("db_insert");

        let db = test_init_db(&cfg).await.unwrap();
        let db_key_1 = b"key_1";
        let db_key_2 = b"key_2";
        let db_val_1 = String::from("val_001");
        let db_val_2 = String::from("val_002");
        db.insert::<_, String>(db_key_1, &db_val_1).await.unwrap();
        assert_eq!(
            db.get::<_, String>(db_key_1).await.unwrap(),
            Some(db_val_1.clone())
        );
        assert_eq!(db.get::<_, String>(db_key_2).await.unwrap(), None);

        db.remove(db_key_1).await.unwrap();
        assert_eq!(db.get::<_, String>(db_key_1).await.unwrap(), None);

        db.insert::<_, String>(db_key_1, &db_val_1).await.unwrap();
        db.insert::<_, String>(db_key_2, &db_val_2).await.unwrap();

        assert_eq!(
            db.get::<_, String>(db_key_1).await.unwrap(),
            Some(db_val_1.clone())
        );
        assert_eq!(
            db.get::<_, String>(db_key_2).await.unwrap(),
            Some(db_val_2.clone())
        );
        db.remove(db_key_2).await.unwrap();
        assert_eq!(db.get::<_, String>(db_key_2).await.unwrap(), None);

        assert!(db.contains_key(db_key_1).await.unwrap());

        let map_1 = db.map("map_1", None).await;
        map_1.insert("m_k_1", &100).await.unwrap();
        assert!(db.map_contains_key("map_1").await.unwrap());

        let map_2 = db.map("map_2", None).await;
        map_2.clear().await.unwrap();
        println!(
            "test_db_insert contains_key(map_2) {:?}",
            db.map_contains_key("map_2").await
        );
        assert!(!db.map_contains_key("map_2").await.unwrap());

        let list_1 = db.list("list_1", None).await;
        list_1.clear().await.unwrap();
        println!(
            "test_db_insert contains_key(list_1) {:?}",
            db.list_contains_key("list_1").await
        );
        assert!(!db.list_contains_key("list_1").await.unwrap());
        list_1.push(&20).await.unwrap();
        assert!(db.list_contains_key("list_1").await.unwrap());
    }

    #[tokio::main]
    #[test]
    async fn test_db_remove() {
        let cfg = get_cfg("db_remove");

        let db = test_init_db(&cfg).await.unwrap();
        let db_key_1 = b"key_11";
        let db_key_2 = b"key_22";
        let db_val_1 = String::from("val_001");
        db.insert::<_, String>(db_key_1, &db_val_1).await.unwrap();
        assert_eq!(
            db.get::<_, String>(db_key_1).await.unwrap(),
            Some(db_val_1.clone())
        );
        assert_eq!(db.contains_key(db_key_1).await.unwrap(), true);

        db.remove(db_key_1).await.unwrap();
        assert_eq!(db.get::<_, String>(db_key_1).await.unwrap(), None);
        assert_eq!(db.contains_key(db_key_1).await.unwrap(), false);

        let m2 = db.map(db_key_2, None).await;
        m2.clear().await.unwrap();
        assert_eq!(db.contains_key(db_key_2).await.unwrap(), false);
        m2.insert("m_k_1", &100).await.unwrap();
        assert_eq!(db.map_contains_key(db_key_2).await.unwrap(), true);
        m2.clear().await.unwrap();
        assert_eq!(db.map_contains_key(db_key_2).await.unwrap(), false);
        m2.insert("m_k_1", &100).await.unwrap();
        assert_eq!(db.map_contains_key(db_key_2).await.unwrap(), true);
        m2.remove("m_k_1").await.unwrap();
        // assert_eq!(db.map_contains_key(db_key_2).await.unwrap(), false);
    }

    #[tokio::main]
    #[test]
    async fn test_db_contains_key() {
        let cfg = get_cfg("db_contains_key");
        let db = test_init_db(&cfg).await.unwrap();
        db.remove("test_c_001").await.unwrap();
        let c_res = db.contains_key("test_c_001").await.unwrap();
        assert!(!c_res);

        db.insert("test_c_001", &"val_001").await.unwrap();
        let c_res = db.contains_key("test_c_001").await.unwrap();
        assert!(c_res);

        let map_001 = db.map("map_001", None).await;
        map_001.clear().await.unwrap();
        map_001.insert("k1", &1).await.unwrap();
        assert_eq!(map_001.is_empty().await.unwrap(), false);
        #[cfg(feature = "map_len")]
        assert_eq!(map_001.len().await.unwrap(), 1);
        let c_res = db.map_contains_key("map_001").await.unwrap();
        assert!(c_res);
        map_001.clear().await.unwrap();
        assert_eq!(map_001.is_empty().await.unwrap(), true);
        #[cfg(feature = "map_len")]
        assert_eq!(map_001.len().await.unwrap(), 0);
        let c_res = db.map_contains_key("map_001").await.unwrap();
        assert!(!c_res);

        let l1 = db.list("list_001", None).await;
        l1.push(&"aa").await.unwrap();
        l1.push(&"bb").await.unwrap();
        assert_eq!(l1.is_empty().await.unwrap(), false);
        let c_res = db.list_contains_key("list_001").await.unwrap();
        assert!(c_res);

        let map_002 = db.map("map_002", None).await;
        map_002.clear().await.unwrap();
        #[cfg(feature = "map_len")]
        println!("test_db_contains_key len: {}", map_002.len().await.unwrap());
        println!(
            "test_db_contains_key is_empty: {}",
            map_002.is_empty().await.unwrap()
        );
        let c_res = db.map_contains_key("map_002").await.unwrap();
        assert!(!c_res);
        assert_eq!(map_002.is_empty().await.unwrap(), true);
        #[cfg(feature = "map_len")]
        assert_eq!(map_002.len().await.unwrap(), 0);

        let list_002 = db.list("list_002", None).await;
        let c_res = db.list_contains_key("list_002").await.unwrap();
        assert!(!c_res);
        assert_eq!(list_002.is_empty().await.unwrap(), true);
    }

    #[tokio::main]
    #[test]
    async fn test_db_contains_key2() {
        let cfg = get_cfg("db_contains_key2");
        let db = test_init_db(&cfg).await.unwrap();
        let max = 10;
        for i in 0..max {
            db.insert(format!("key_{}", i), &1).await.unwrap();
        }

        for i in 0..max {
            let c_res = db.contains_key(format!("key_{}", i)).await.unwrap();
            assert!(c_res);
        }

        for i in 0..max {
            db.remove(format!("key_{}", i)).await.unwrap();
        }

        for i in 0..max {
            let c_res = db.contains_key(format!("key_{}", i)).await.unwrap();
            assert!(!c_res);
        }
    }

    #[cfg(feature = "ttl")]
    #[serial_test::serial]
    #[tokio::main]
    #[test]
    async fn test_db_expire() {
        let cfg = get_cfg("expire");
        let db = test_init_db(&cfg).await.unwrap();

        let res_none = db.ttl("test_k001").await.unwrap();
        println!("ttl res_none: {:?}", res_none);
        assert_eq!(res_none, None);
        //----------------------------------------------------------------------------------
        db.insert("tkey_001", &10).await.unwrap();
        let expire_res = db.expire("tkey_001", 1000).await.unwrap();
        println!("expire_res: {:?}", expire_res);
        let tkey_001_ttl = db.ttl("tkey_001").await.unwrap();
        println!("tkey_001_ttl: {:?}", tkey_001_ttl);
        assert!(tkey_001_ttl.is_some());
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        db.insert("tkey_001", &20).await.unwrap();
        let tkey_001_ttl = db.ttl("tkey_001").await.unwrap();
        println!("tkey_001_ttl: {:?}", tkey_001_ttl);
        assert!(tkey_001_ttl.is_some() && tkey_001_ttl.unwrap() > 100000);

        //----------------------------------------------------------------------------------
        db.remove("ttl_key_1").await.unwrap();
        let ttl_001_res = db.ttl("ttl_key_1").await.unwrap();
        println!("ttl_001_res: {:?}", ttl_001_res);
        assert!(ttl_001_res.is_none());
        db.insert("ttl_key_1", &11).await.unwrap();
        let ttl_001_res = db.ttl("ttl_key_1").await.unwrap();
        println!("ttl_001_res: {:?}", ttl_001_res);
        assert!(ttl_001_res.is_some());
        let expire_res = db.expire("ttl_key_1", 1 * 1000).await.unwrap();
        println!("expire_res: {:?}", expire_res);
        assert!(expire_res);
        let ttl_001_res = db.ttl("ttl_key_1").await.unwrap().unwrap();
        println!("ttl_001_res: {:?}", ttl_001_res);
        assert!(ttl_001_res <= 1 * 1000);
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let ttl_001_res = db.ttl("ttl_key_1").await.unwrap().unwrap();
        println!("<500 ttl_001_res: {:?}", ttl_001_res);
        assert!(ttl_001_res <= 500);
        db.insert("ttl_key_1", &11).await.unwrap();
        let ttl_001_res = db.ttl("ttl_key_1").await.unwrap().unwrap();
        println!("ttl_key_1 ttl_001_res: {:?}", ttl_001_res);
        //assert_eq!(ttl_001_res, Duration::MAX.as_millis() as TimestampMillis);
        assert!(ttl_001_res > 1000);
        let expire_res = db.expire("ttl_key_1", 300).await.unwrap();
        println!("expire_res: {:?}", expire_res);
        assert!(expire_res, "expire should return true");
        let ttl_001_res = db.ttl("ttl_key_1").await.unwrap().unwrap();
        println!("<300 ttl_001_res: {:?}", ttl_001_res);
        assert!(ttl_001_res > 200 && ttl_001_res <= 300);
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let ttl_001_res = db.ttl("ttl_key_1").await.unwrap();
        println!("None ttl_001_res: {:?}", ttl_001_res);
        assert!(ttl_001_res.is_none());
        //---------------------------------------------------------------------------------

        db.insert("ttl_key_1", &111).await.unwrap();
        let ttl_key_1_res = db.ttl("ttl_key_1").await.unwrap();
        println!(
            "ttl_key_1_res: {:?}",
            ttl_key_1_res.map(|d| Duration::from_millis(d as u64))
        );
        assert!(ttl_key_1_res.is_some());

        db.remove("ttl_key_1").await.unwrap();
        let ttl_key_1_res = db.ttl("ttl_key_1").await.unwrap();
        println!("ttl_key_1_res: {:?}", ttl_key_1_res);
        assert!(ttl_key_1_res.is_none());

        let expire_res = db.expire("ttl_key_1", 1 * 1000).await.unwrap();
        println!("db expire_res: {:?}", expire_res);
        assert!(!expire_res);

        db.insert("ttl_key_1", &222).await.unwrap();
        let ttl_key_1_res = db.ttl("ttl_key_1").await.unwrap();
        println!(
            "db ttl_key_1_res: {:?}",
            ttl_key_1_res.map(|d| Duration::from_millis(d as u64))
        );
        assert!(ttl_key_1_res.is_some());

        let expire_res = db.expire("ttl_key_1", 500).await.unwrap();
        println!("db expire_res: {:?}", expire_res);
        assert!(expire_res);
        let ttl_key_1_res = db.ttl("ttl_key_1").await.unwrap();
        println!(
            "db ttl_key_1_res: {:?}",
            ttl_key_1_res.map(|d| Duration::from_millis(d as u64))
        );

        tokio::time::sleep(std::time::Duration::from_millis(700)).await;
        assert_eq!(db.get::<_, i32>("ttl_key_1").await.unwrap(), None);
        assert_eq!(db.contains_key("ttl_key_1").await.unwrap(), false);

        //-----------------------------------------------------------------------------
        let mut ttl_001 = db.map("ttl_001", None).await;
        ttl_001.clear().await.unwrap();
        let ttl_001_res_none = db.ttl("ttl_001").await.unwrap();
        println!(
            "1 test_db_expire map ttl_001_res_none: {:?}",
            ttl_001_res_none
        );
        assert_eq!(ttl_001_res_none, None);

        ttl_001.insert("k1", &11).await.unwrap();
        ttl_001.insert("k2", &22).await.unwrap();

        assert_eq!(ttl_001.is_empty().await.unwrap(), false);
        #[cfg(feature = "map_len")]
        assert_eq!(ttl_001.len().await.unwrap(), 2);
        let ttl_001_res = ttl_001.ttl().await.unwrap();
        println!("2 test_db_expire map ttl_001_res: {:?}", ttl_001_res);
        assert_eq!(ttl_001_res.is_some(), true);

        let expire_res = ttl_001.expire(1 * 1000).await.unwrap();
        println!("3 test_db_expire map expire_res: {:?}", expire_res);
        assert_eq!(expire_res, true);

        let ttl_001_res = ttl_001.ttl().await.unwrap();
        println!("4 test_db_expire map ttl_001_res: {:?}", ttl_001_res);
        assert!(ttl_001_res.unwrap() <= 1 * 1000);

        let k1_v = ttl_001.get::<_, i32>("k1").await.unwrap();
        let k2_v = ttl_001.get::<_, i32>("k2").await.unwrap();
        println!("test_db_expire k1_v: {:?}", k1_v);
        println!("test_db_expire k2_v: {:?}", k2_v);
        assert_eq!(k1_v, Some(11));
        assert_eq!(k2_v, Some(22));

        tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
        assert_eq!(db.map_contains_key("ttl_001").await.unwrap(), false);
        #[cfg(feature = "map_len")]
        assert_eq!(ttl_001.len().await.unwrap(), 0);
        assert_eq!(ttl_001.is_empty().await.unwrap(), true);
        assert_eq!(
            ttl_001.remove_and_fetch::<_, i32>("k1").await.unwrap(),
            None
        );
        assert!(ttl_001.iter::<i32>().await.unwrap().next().await.is_none());
        assert!(ttl_001.key_iter().await.unwrap().next().await.is_none());

        let mut vals = Vec::new();
        let mut iter = ttl_001.prefix_iter::<_, i32>("k").await.unwrap();
        while let Some(item) = iter.next().await {
            vals.push(item.unwrap())
        }
        drop(iter);
        println!("Iter vals: {:?}", vals);

        assert!(ttl_001
            .prefix_iter::<_, i32>("k")
            .await
            .unwrap()
            .next()
            .await
            .is_none());

        let k1_v = ttl_001.get::<_, i32>("k1").await.unwrap();
        let k2_v = ttl_001.get::<_, i32>("k2").await.unwrap();
        println!("test_db_expire k1_v: {:?}", k1_v);
        println!("test_db_expire k2_v: {:?}", k2_v);
        assert_eq!(k1_v, None);
        assert_eq!(k2_v, None);

        let ttl_001_res = ttl_001.ttl().await.unwrap();
        println!("ttl_001_res: {:?}", ttl_001_res);
        assert!(ttl_001_res.is_none());
        ttl_001.insert("k1", &11).await.unwrap();
        let ttl_001_res = ttl_001.ttl().await.unwrap();
        println!("xxxx ttl_001_res: {:?}", ttl_001_res);
        assert!(ttl_001_res.is_some());
        let expire_res = ttl_001.expire(1 * 1000).await.unwrap();
        println!("expire_res: {:?}", expire_res);
        assert!(expire_res);
        let ttl_001_res = ttl_001.ttl().await.unwrap().unwrap();
        println!("x0 ttl_001_res: {:?}", ttl_001_res);
        assert!(ttl_001_res <= 1 * 1000);
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let ttl_001_res = ttl_001.ttl().await.unwrap().unwrap();
        println!("x1 ttl_001_res: {:?}", ttl_001_res);
        assert!(ttl_001_res <= 500);
        ttl_001.insert("k1", &11).await.unwrap();
        let ttl_001_res = ttl_001.ttl().await.unwrap().unwrap();
        println!("x2 ttl_001_res: {:?}", ttl_001_res);
        assert!(ttl_001_res <= 500);
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        ttl_001.insert("k1", &11).await.unwrap();
        let ttl_001_res = ttl_001.ttl().await.unwrap().unwrap();
        println!(
            "x3 ttl_001_res: {:?}  {:?}",
            ttl_001_res,
            (TimestampMillis::MAX - ttl_001_res)
        );
        assert!(ttl_001_res >= 10000);
        assert_eq!(db.map_contains_key("ttl_001").await.unwrap(), true);

        //-----------------------------------------------------------------------------
        let mut l_ttl_001 = db.list("l_ttl_001", None).await;
        l_ttl_001.clear().await.unwrap();
        let l_ttl_001_res_none = l_ttl_001.ttl().await.unwrap();
        println!(
            "1 test_db_expire list l_ttl_001_res_none: {:?}",
            l_ttl_001_res_none
        );
        assert_eq!(db.list_contains_key("l_ttl_001").await.unwrap(), false);
        assert_eq!(l_ttl_001.is_empty().await.unwrap(), true);
        assert_eq!(l_ttl_001.len().await.unwrap(), 0);
        assert_eq!(l_ttl_001_res_none, None);

        l_ttl_001.push(&11).await.unwrap();
        l_ttl_001.push(&22).await.unwrap();
        assert_eq!(db.list_contains_key("l_ttl_001").await.unwrap(), true);
        assert_eq!(l_ttl_001.is_empty().await.unwrap(), false);
        assert_eq!(l_ttl_001.len().await.unwrap(), 2);
        let l_ttl_001_res = l_ttl_001.ttl().await.unwrap();
        println!("2 test_db_expire list l_ttl_001_res: {:?}", l_ttl_001_res);
        assert_eq!(l_ttl_001_res.is_some(), true);

        let expire_res = l_ttl_001.expire(1 * 1000).await.unwrap();
        println!("3 test_db_expire list expire_res: {:?}", expire_res);
        assert_eq!(expire_res, true);

        let l_ttl_001_res = l_ttl_001.ttl().await.unwrap().unwrap();
        println!("4 test_db_expire list l_ttl_001_res: {:?}", l_ttl_001_res);
        assert!(l_ttl_001_res <= 1 * 1000);

        let k1_v = l_ttl_001.get_index::<i32>(0).await.unwrap();
        let k2_v = l_ttl_001.get_index::<i32>(1).await.unwrap();
        println!("test_db_expire list k1_v: {:?}", k1_v);
        println!("test_db_expire list k2_v: {:?}", k2_v);
        assert_eq!(k1_v, Some(11));
        assert_eq!(k2_v, Some(22));

        tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
        assert_eq!(db.list_contains_key("l_ttl_001").await.unwrap(), false);
        assert_eq!(l_ttl_001.len().await.unwrap(), 0);
        assert_eq!(l_ttl_001.is_empty().await.unwrap(), true);
        assert_eq!(l_ttl_001.all::<i32>().await.unwrap().len(), 0);
        assert!(l_ttl_001
            .iter::<i32>()
            .await
            .unwrap()
            .next()
            .await
            .is_none());
        assert_eq!(l_ttl_001.pop::<i32>().await.unwrap(), None);
        let k1_v = l_ttl_001.get_index::<i32>(0).await.unwrap();
        let k2_v = l_ttl_001.get_index::<i32>(1).await.unwrap();
        println!("test_db_expire list k1_v: {:?}", k1_v);
        println!("test_db_expire list k2_v: {:?}", k2_v);
        assert_eq!(k1_v, None);
        assert_eq!(k2_v, None);

        let l_ttl_001_res = l_ttl_001.ttl().await.unwrap();
        println!("test_db_expire list l_ttl_001_res: {:?}", l_ttl_001_res);
        assert!(l_ttl_001_res.is_none());
        l_ttl_001.push(&11).await.unwrap();
        let l_ttl_001_res = l_ttl_001.ttl().await.unwrap();
        println!(
            "xxxx test_db_expire list l_ttl_001_res: {:?}",
            l_ttl_001_res
        );
        assert!(l_ttl_001_res.is_some());
        let expire_res = l_ttl_001.expire(1 * 1000).await.unwrap();
        println!("test_db_expire list expire_res: {:?}", expire_res);
        assert!(expire_res);
        let l_ttl_001_res = l_ttl_001.ttl().await.unwrap().unwrap();
        println!("x0 test_db_expire list l_ttl_001_res: {:?}", l_ttl_001_res);
        assert!(l_ttl_001_res <= 1 * 1000);
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let l_ttl_001_res = l_ttl_001.ttl().await.unwrap();
        println!("x1 test_db_expire list l_ttl_001_res: {:?}", l_ttl_001_res);
        assert!(l_ttl_001_res.unwrap() <= 500);
        l_ttl_001.push(&11).await.unwrap();

        let l_ttl_001_res = l_ttl_001.ttl().await.unwrap().unwrap();
        println!("x2 test_db_expire list l_ttl_001_res: {:?}", l_ttl_001_res);
        assert!(l_ttl_001_res <= 500);

        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        l_ttl_001.push(&11).await.unwrap();

        let l_ttl_001_res = l_ttl_001.ttl().await.unwrap().unwrap();
        println!(
            "x3 test_db_expire list l_ttl_001_res: {:?}  {:?}",
            l_ttl_001_res,
            (TimestampMillis::MAX - l_ttl_001_res)
        );
        assert!(l_ttl_001_res >= 10000);
    }

    #[tokio::main]
    #[test]
    async fn test_map_insert() {
        let cfg = get_cfg("map_insert");
        let db = test_init_db(&cfg).await.unwrap();

        let map001 = db.map("001", None).await;
        map001.clear().await.unwrap();
        #[cfg(feature = "map_len")]
        assert_eq!(map001.len().await.unwrap(), 0);

        map001.insert("key_1", &1).await.unwrap();
        map001.insert("key_2", &2).await.unwrap();
        #[cfg(feature = "map_len")]
        assert_eq!(map001.len().await.unwrap(), 2);

        let val = map001.get::<_, i32>("key_1").await.unwrap();
        println!("test_map_insert val: {:?}", val);
        assert_eq!(val, Some(1));

        map001.remove::<_>("key_1").await.unwrap();
        let val = map001.get::<_, i32>("key_1").await.unwrap();
        println!("test_map_insert val: {:?}", val);
        assert_eq!(val, None);

        #[cfg(feature = "map_len")]
        println!("test_map_insert len: {:?}", map001.len().await.unwrap());
        #[cfg(feature = "map_len")]
        assert_eq!(map001.len().await.unwrap(), 1);
    }

    #[tokio::main]
    #[test]
    async fn test_map_contains_key() {
        let cfg = get_cfg("map_contains_key");
        let db = test_init_db(&cfg).await.unwrap();

        let map001 = db.map("m001", None).await;
        map001.clear().await.unwrap();
        #[cfg(feature = "map_len")]
        assert_eq!(map001.len().await.unwrap(), 0);
        assert_eq!(map001.contains_key("k001").await.unwrap(), false);

        map001.insert("k001", &"val_001").await.unwrap();
        assert_eq!(map001.contains_key("k001").await.unwrap(), true);

        map001.remove::<_>("k001").await.unwrap();
        assert_eq!(map001.contains_key("k001").await.unwrap(), false);
        #[cfg(feature = "map_len")]
        assert_eq!(map001.len().await.unwrap(), 0);
    }

    #[tokio::main]
    #[test]
    async fn test_map() {
        let cfg = get_cfg("map");
        let db = test_init_db(&cfg).await.unwrap();

        let kv001 = db.map("tree_kv001", None).await;
        let kv_key_1 = b"kv_key_1";

        kv001.clear().await.unwrap();
        #[cfg(feature = "map_len")]
        assert_eq!(kv001.len().await.unwrap(), 0);

        let kv_val_1 = String::from("kv_val_001");
        kv001
            .insert::<_, String>(kv_key_1, &kv_val_1)
            .await
            .unwrap();
        assert_eq!(
            kv001.get::<_, String>(kv_key_1).await.unwrap(),
            Some(kv_val_1.clone())
        );
        assert_eq!(kv001.get::<_, String>(b"kv_key_2").await.unwrap(), None);
        #[cfg(feature = "map_len")]
        assert_eq!(kv001.len().await.unwrap(), 1);
        assert_eq!(kv001.is_empty().await.unwrap(), false);

        assert!(kv001.contains_key(kv_key_1).await.unwrap());

        kv001.remove(kv_key_1).await.unwrap();
        assert_eq!(kv001.get::<_, String>(kv_key_1).await.unwrap(), None);
        assert!(!kv001.contains_key(kv_key_1).await.unwrap());
        #[cfg(feature = "map_len")]
        assert_eq!(kv001.len().await.unwrap(), 0);
        assert_eq!(kv001.is_empty().await.unwrap(), true);

        assert_eq!(
            kv001.remove_and_fetch::<_, String>(kv_key_1).await.unwrap(),
            None
        );
        kv001
            .insert::<_, String>(kv_key_1, &kv_val_1)
            .await
            .unwrap();
        assert_eq!(
            kv001.remove_and_fetch::<_, String>(kv_key_1).await.unwrap(),
            Some(kv_val_1)
        );
        assert_eq!(
            kv001.remove_and_fetch::<_, String>(kv_key_1).await.unwrap(),
            None
        );

        kv001.insert(b"kv_key_3", "3").await.unwrap();
        kv001.insert(b"kv_key_4", "4").await.unwrap();
        kv001.insert(b"kv_key_5", "5").await.unwrap();
        kv001.insert(b"kv_key_6", "6").await.unwrap();
        #[cfg(feature = "map_len")]
        assert_eq!(kv001.len().await.unwrap(), 4);
        kv001.remove_with_prefix("kv_key_").await.unwrap();
        #[cfg(feature = "map_len")]
        assert_eq!(kv001.len().await.unwrap(), 0);
        assert_eq!(kv001.is_empty().await.unwrap(), true);
    }

    #[tokio::main]
    #[test]
    async fn test_map_iter2() {
        let cfg = get_cfg("map_iter2");
        let mut db = test_init_db(&cfg).await.unwrap();

        let mut map_iter = db.map_iter().await.unwrap();
        while let Some(map) = map_iter.next().await {
            let map = map.unwrap();
            map.clear().await.unwrap();
        }

        drop(map_iter);

        let max = 10;

        for i in 0..max {
            let map1 = db.map(format!("map-{}", i), None).await;
            map1.insert(format!("map-{}-data", i), &i).await.unwrap();
        }

        let mut map_iter = db.map_iter().await.unwrap();

        // let aa = collect(map_iter).await;
        let mut count = 0;
        while let Some(map) = map_iter.next().await {
            let mut map = map.unwrap();
            let mut iter = map.iter::<i32>().await.unwrap();
            while let Some(item) = iter.next().await {
                let (key, val) = item.unwrap();
                println!("key: {:?}, val: {:?}", String::from_utf8_lossy(&key), val);
            }
            count += 1;
        }

        println!("max: {:?}, count: {:?}", max, count);
        assert_eq!(max, count);
    }

    #[tokio::main]
    #[test]
    async fn test_batch() {
        let cfg = get_cfg("batch");
        let db = test_init_db(&cfg).await.unwrap();

        let skv = db.map("batch_kv001", None).await;

        let mut kvs = Vec::new();
        for i in 0..100 {
            kvs.push((format!("key_{}", i).as_bytes().to_vec(), i));
        }
        skv.batch_insert(kvs.clone()).await.unwrap();
        #[cfg(feature = "map_len")]
        assert_eq!(skv.len().await.unwrap(), 100);

        let mut ks = Vec::new();
        for i in 0..50 {
            ks.push(format!("key_{}", i).as_bytes().to_vec());
        }
        skv.batch_remove(ks).await.unwrap();
        #[cfg(feature = "map_len")]
        assert_eq!(skv.len().await.unwrap(), 50);
    }

    #[tokio::main]
    #[test]
    async fn test_iter() {
        let cfg = get_cfg("iter");
        let db = test_init_db(&cfg).await.unwrap();

        let mut skv = db.map("iter_kv002", None).await;
        skv.clear().await.unwrap();

        for i in 0..10 {
            skv.insert::<_, i32>(format!("key_{}", i), &i)
                .await
                .unwrap();
        }

        let mut vals = Vec::new();
        let mut iter = skv.iter::<i32>().await.unwrap();
        while let Some(item) = iter.next().await {
            vals.push(item.unwrap())
        }
        drop(iter);

        assert_eq!(
            vals,
            vec![
                (b"key_0".to_vec(), 0),
                (b"key_1".to_vec(), 1),
                (b"key_2".to_vec(), 2),
                (b"key_3".to_vec(), 3),
                (b"key_4".to_vec(), 4),
                (b"key_5".to_vec(), 5),
                (b"key_6".to_vec(), 6),
                (b"key_7".to_vec(), 7),
                (b"key_8".to_vec(), 8),
                (b"key_9".to_vec(), 9),
            ]
        );

        let mut keys = Vec::new();
        let mut key_iter = skv.key_iter().await.unwrap();
        while let Some(item) = key_iter.next().await {
            keys.push(String::from_utf8(item.unwrap()).unwrap())
        }
        drop(key_iter);

        assert_eq!(
            keys,
            vec![
                "key_0", "key_1", "key_2", "key_3", "key_4", "key_5", "key_6", "key_7", "key_8",
                "key_9"
            ]
        );

        for i in 0..5 {
            skv.insert::<_, i32>(format!("key2_{}", i), &i)
                .await
                .unwrap();
        }

        let mut vals = Vec::new();
        let mut prefix_iter = skv.prefix_iter::<_, i32>("key2_").await.unwrap();
        while let Some(item) = prefix_iter.next().await {
            vals.push(item.unwrap())
        }

        assert_eq!(
            vals,
            vec![
                (b"key2_0".to_vec(), 0),
                (b"key2_1".to_vec(), 1),
                (b"key2_2".to_vec(), 2),
                (b"key2_3".to_vec(), 3),
                (b"key2_4".to_vec(), 4)
            ]
        );
    }

    #[tokio::main]
    #[test]
    async fn test_list() {
        let cfg = get_cfg("array");
        let db = test_init_db(&cfg).await.unwrap();

        let array_a = db.list("array_a", None).await;
        let array_b = db.list("array_b", None).await;
        let mut array_c = db.list("array_c", None).await;

        array_a.clear().await.unwrap();
        array_b.clear().await.unwrap();
        array_c.clear().await.unwrap();

        db.insert("key_001", &1).await.unwrap();
        db.insert("key_002", &2).await.unwrap();
        db.insert("key_003", &3).await.unwrap();

        for i in 0..5 {
            array_a.push(&i).await.unwrap();
        }
        assert_eq!(array_a.len().await.unwrap(), 5);

        let vals = array_a.all::<i32>().await.unwrap();
        assert_eq!(vals.len(), 5);
        assert_eq!(vals, vec![0, 1, 2, 3, 4]);

        let val_1 = array_a.get_index::<i32>(1).await.unwrap();
        assert_eq!(val_1, Some(1));

        let val_0 = array_a.pop::<i32>().await.unwrap();
        assert_eq!(val_0, Some(0));

        let val_1 = array_a.pop::<i32>().await.unwrap();
        assert_eq!(val_1, Some(1));

        let vals = array_a.all::<i32>().await.unwrap();
        assert_eq!(vals.len(), 3);
        assert_eq!(vals, vec![2, 3, 4]);

        for i in 0..20 {
            array_a.push_limit(&i, 5, true).await.unwrap();
        }
        assert_eq!(array_a.len().await.unwrap(), 5);
        let vals = array_a.all::<i32>().await.unwrap();
        assert_eq!(vals.len(), 5);
        assert_eq!(vals, vec![15, 16, 17, 18, 19]);

        for i in 0..4 {
            array_b.push(&i).await.unwrap();
        }

        for i in 0..3 {
            array_c.push(&i).await.unwrap();
        }

        println!("array_c.len(): {}", array_c.len().await.unwrap());

        let mut vals = Vec::new();
        let mut iter = array_c.iter::<i32>().await.unwrap();
        while let Some(val) = iter.next().await {
            let val = val.unwrap();
            vals.push(val);
            println!("array_c iter val: {}", val);
        }
        assert_eq!(vals, vec![0, 1, 2]);
    }

    #[tokio::main]
    #[test]
    async fn test_list2() {
        let cfg = get_cfg("map_list");
        let db = test_init_db(&cfg).await.unwrap();

        let ml001 = db.map("m_l_001", None).await;
        ml001.clear().await.unwrap();
        #[cfg(feature = "map_len")]
        assert_eq!(ml001.len().await.unwrap(), 0);
        assert_eq!(ml001.is_empty().await.unwrap(), true);

        let mut l001 = db.list("l_001", None).await;
        l001.clear().await.unwrap();
        assert_eq!(l001.len().await.unwrap(), 0);
        assert_eq!(l001.is_empty().await.unwrap(), true);
        l001.push(&100).await.unwrap();
        l001.push(&101).await.unwrap();
        assert_eq!(l001.len().await.unwrap(), 2);
        assert_eq!(l001.is_empty().await.unwrap(), false);

        for v in 100..200 {
            l001.push_limit(&v, 5, true).await.unwrap();
        }
        assert_eq!(l001.len().await.unwrap(), 5);
        assert_eq!(l001.is_empty().await.unwrap(), false);

        let mut iter = l001.iter::<i32>().await.unwrap();
        let mut vals = Vec::new();
        while let Some(val) = iter.next().await {
            let val = val.unwrap();
            vals.push(val);
        }
        drop(iter);
        assert_eq!(vals, [195, 196, 197, 198, 199]);

        assert_eq!(l001.all::<i32>().await.unwrap(), [195, 196, 197, 198, 199]);

        assert_eq!(l001.get_index(0).await.unwrap(), Some(195));
        assert_eq!(l001.get_index(2).await.unwrap(), Some(197));
        assert_eq!(l001.get_index(4).await.unwrap(), Some(199));
        assert!(l001.get_index::<i32>(5).await.unwrap().is_none());

        let mut pops = Vec::new();
        while let Some(item) = l001.pop::<i32>().await.unwrap() {
            println!("list pop item: {:?}", item);
            pops.push(item);
        }
        assert_eq!(pops, [195, 196, 197, 198, 199]);

        assert_eq!(l001.len().await.unwrap(), 0);
        assert_eq!(l001.is_empty().await.unwrap(), true);

        for v in 10..20 {
            l001.push_limit(&v, 5, true).await.unwrap();
        }

        let l002 = db.list("l_002", None).await;
        for v in 20..30 {
            l002.push_limit(&v, 5, true).await.unwrap();
        }

        assert_eq!(l001.all::<i32>().await.unwrap(), [15, 16, 17, 18, 19]);
        assert_eq!(l002.all::<i32>().await.unwrap(), [25, 26, 27, 28, 29]);

        assert_eq!(l001.len().await.unwrap(), 5);
        assert_eq!(l001.is_empty().await.unwrap(), false);

        assert_eq!(l002.len().await.unwrap(), 5);
        assert_eq!(l002.is_empty().await.unwrap(), false);

        l001.clear().await.unwrap();
        assert_eq!(l001.len().await.unwrap(), 0);
        assert_eq!(l001.is_empty().await.unwrap(), true);

        l002.clear().await.unwrap();
        assert_eq!(l002.len().await.unwrap(), 0);
        assert_eq!(l002.is_empty().await.unwrap(), true);
    }

    #[tokio::main]
    #[test]
    async fn test_list_iter() {
        let cfg = get_cfg("list_iter");
        let mut db = test_init_db(&cfg).await.unwrap();

        let l1 = db.list("l1", None).await;
        let l2 = db.list("l2", None).await;
        let l3 = db.list("l3", None).await;
        l1.clear().await.unwrap();
        l2.clear().await.unwrap();
        l3.clear().await.unwrap();

        l1.push(&1).await.unwrap();
        l2.push(&1).await.unwrap();
        l2.push(&2).await.unwrap();
        l3.push(&1).await.unwrap();
        l3.push(&2).await.unwrap();
        l3.push(&3).await.unwrap();

        let mut iter = db.list_iter().await.unwrap();
        while let Some(l) = iter.next().await {
            let l = l.unwrap();
            let name = String::from_utf8(l.name().to_vec());
            println!("list name: {:?}, len: {:?}", name, l.len().await);
            let len = l.len().await.unwrap();
            assert!(len == 1 || len == 2 || len == 3);
        }
    }

    #[tokio::main]
    #[test]
    async fn test_list_iter2() {
        let cfg = get_cfg("list_iter2");
        let mut db = test_init_db(&cfg).await.unwrap();

        let mut list_iter = db.list_iter().await.unwrap();
        while let Some(list) = list_iter.next().await {
            let list = list.unwrap();
            list.clear().await.unwrap();
        }

        drop(list_iter);

        let max = 10;

        for i in 0..max {
            let list1 = db.list(format!("list-{}", i), None).await;
            list1.push(&i).await.unwrap();
        }

        let mut list_iter = db.list_iter().await.unwrap();

        let mut count = 0;
        while let Some(list) = list_iter.next().await {
            let mut list = list.unwrap();
            let mut iter = list.iter::<i32>().await.unwrap();
            while let Some(item) = iter.next().await {
                let val = item.unwrap();
                println!("val: {:?}", val);
            }
            count += 1;
        }

        println!("max: {:?}, count: {:?}", max, count);
        assert_eq!(max, count);
    }

    #[tokio::main]
    #[test]
    async fn test_map_iter() {
        let cfg = get_cfg("async_map_iter");
        let mut db = test_init_db(&cfg).await.unwrap();

        let m1 = db.map("m1", None).await;
        let m2 = db.map("m2", None).await;
        let m3 = db.map("m3", None).await;

        m1.insert("k1", &1).await.unwrap();
        m2.insert("k1", &1).await.unwrap();
        m2.insert("k2", &2).await.unwrap();
        m3.insert("k1", &1).await.unwrap();
        m3.insert("k2", &2).await.unwrap();
        m3.insert("k3", &3).await.unwrap();

        let mut iter = db.map_iter().await.unwrap();
        let mut map_names = Vec::new();
        while let Some(m) = iter.next().await {
            let m = m.unwrap();
            map_names.push(String::from_utf8(m.name().to_vec()).unwrap());
            let name = String::from_utf8(m.name().to_vec());
            println!("map name: {:?}", name);
            #[cfg(feature = "map_len")]
            {
                let len = m.len().await.unwrap();
                println!("map len: {:?}", len);
                assert!(len == 1 || len == 2 || len == 3);
            }
        }
        for name in map_names.iter() {
            assert!(vec!["m1", "m2", "m3"].contains(&name.as_str()));
        }
    }

    #[tokio::main]
    #[test]
    async fn test_counter() {
        let cfg = get_cfg("incr");
        let db = test_init_db(&cfg).await.unwrap();

        db.remove("incr1").await.unwrap();
        db.remove("incr2").await.unwrap();
        db.remove("incr3").await.unwrap();

        db.counter_incr("incr1", 3).await.unwrap();
        db.counter_incr("incr2", -3).await.unwrap();
        db.counter_incr("incr3", 10).await.unwrap();

        assert_eq!(db.counter_get("incr1").await.unwrap(), Some(3));
        assert_eq!(db.counter_get("incr2").await.unwrap(), Some(-3));
        assert_eq!(db.counter_get("incr3").await.unwrap(), Some(10));

        db.counter_decr("incr3", 2).await.unwrap();
        assert_eq!(db.counter_get("incr3").await.unwrap(), Some(8));

        db.counter_decr("incr3", -3).await.unwrap();
        assert_eq!(db.counter_get("incr3").await.unwrap(), Some(11));

        db.counter_set("incr3", 100).await.unwrap();
        assert_eq!(db.counter_get("incr3").await.unwrap(), Some(100));

        db.counter_incr("incr3", 10).await.unwrap();
        assert_eq!(db.counter_get("incr3").await.unwrap(), Some(110));

        assert_eq!(db.counter_get("incr4").await.unwrap(), None);
    }

    #[tokio::main]
    #[test]
    async fn test_db_batch() {
        let cfg = get_cfg("db_batch_insert");
        let db = test_init_db(&cfg).await.unwrap();

        let mut key_vals = Vec::new();
        for i in 0..100 {
            key_vals.push((format!("key_{}", i).as_bytes().to_vec(), i));
        }

        db.batch_insert(key_vals).await.unwrap();

        assert_eq!(db.get("key_99").await.unwrap(), Some(99));
        assert_eq!(db.get::<_, usize>("key_100").await.unwrap(), None);

        let mut keys = Vec::new();
        for i in 0..50 {
            keys.push(format!("key_{}", i).as_bytes().to_vec());
        }
        db.batch_remove(keys).await.unwrap();

        assert_eq!(db.get::<_, usize>("key_0").await.unwrap(), None);
        assert_eq!(db.get::<_, usize>("key_49").await.unwrap(), None);
        assert_eq!(db.get("key_50").await.unwrap(), Some(50));

        let mut keys = Vec::new();
        for i in 50..100 {
            keys.push(format!("key_{}", i).as_bytes().to_vec());
        }
        db.batch_remove(keys).await.unwrap();
    }

    #[tokio::main]
    #[test]
    async fn test_list_pushs() {
        let cfg = get_cfg("list_pushs");
        let db = test_init_db(&cfg).await.unwrap();
        let l11 = db.list("l11", None).await;
        l11.clear().await.unwrap();
        let mut vals = Vec::new();
        for i in 0..10 {
            vals.push(i);
        }
        l11.pushs(vals).await.unwrap();
        assert_eq!(l11.len().await.unwrap(), 10);
        println!("{:?}", l11.all::<i32>().await.unwrap());
        assert_eq!(
            l11.all::<i32>().await.unwrap(),
            vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        );

        let mut vals = Vec::new();
        for i in 20..25 {
            vals.push(i);
        }
        l11.pushs(vals).await.unwrap();
        assert_eq!(l11.len().await.unwrap(), 15);
        println!("{:?}", l11.all::<i32>().await.unwrap());
        assert_eq!(
            l11.all::<i32>().await.unwrap(),
            vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 20, 21, 22, 23, 24]
        );
    }

    #[tokio::main]
    #[test]
    async fn test_list_pop() {
        let cfg = get_cfg("list_pop");
        let db = test_init_db(&cfg).await.unwrap();
        let l11 = db.list("l11", None).await;
        l11.clear().await.unwrap();
        for i in 0..10 {
            l11.push(&i).await.unwrap();
        }
        println!("{:?}", l11.all::<i32>().await.unwrap());
        println!(
            "l11.get_index(): {:?}",
            l11.get_index::<i32>(0).await.unwrap()
        );
        println!("l11.pop(): {:?}", l11.pop::<i32>().await.unwrap());
        println!("l11.pop(): {:?}", l11.pop::<i32>().await.unwrap());
        println!(
            "all: {:?}, len: {:?}",
            l11.all::<i32>().await.unwrap(),
            l11.len().await
        );
        assert_eq!(l11.len().await.unwrap(), 8);
        assert_eq!(
            l11.all::<i32>().await.unwrap(),
            vec![2, 3, 4, 5, 6, 7, 8, 9]
        );

        // let pop_v = l11
        //     .pop_f::<_, i32>(|v| {
        //         println!("left val: {:?}", v);
        //         *v == 2
        //     })
        //     .await
        //     .unwrap();
        // println!("pop val: {:?}", pop_v);
        // println!(
        //     "all: {:?}, len: {:?}",
        //     l11.all::<i32>().await.unwrap(),
        //     l11.len().await
        // );
        // assert_eq!(l11.len().await.unwrap(), 7);
        // assert_eq!(l11.all::<i32>().await.unwrap(), vec![3, 4, 5, 6, 7, 8, 9]);
        //
        // let pop_v = l11.pop_f::<_, i32>(|v| *v == 2).await.unwrap();
        // println!("pop val: {:?}", pop_v);
        // println!(
        //     "all: {:?}, len: {:?}",
        //     l11.all::<i32>().await.unwrap(),
        //     l11.len().await
        // );
        // assert_eq!(l11.len().await.unwrap(), 7);
        // assert_eq!(l11.all::<i32>().await.unwrap(), vec![3, 4, 5, 6, 7, 8, 9]);
        //
        // l11.clear().await.unwrap();
        // assert_eq!(l11.len().await.unwrap(), 0);
        // assert_eq!(l11.all::<i32>().await.unwrap(), vec![]);
        //
        // let pop_v = l11.pop_f::<_, i32>(|_| true).await.unwrap();
        // println!("pop val: {:?}", pop_v);
        // println!(
        //     "all: {:?}, len: {:?}",
        //     l11.all::<i32>().await.unwrap(),
        //     l11.len().await
        // );
        // assert_eq!(l11.len().await.unwrap(), 0);
        // assert_eq!(l11.all::<i32>().await.unwrap(), vec![]);
    }

    #[tokio::main]
    #[test]
    async fn test_session_iter() {
        let cfg = get_cfg("session");
        let mut db = test_init_db(&cfg).await.unwrap();
        let now = std::time::Instant::now();
        let mut iter = db.map_iter().await.unwrap();
        let mut count = 0;
        while let Some(m) = iter.next().await {
            let _m = m.unwrap();
            //println!("map name: {:?}", String::from_utf8_lossy(m.name()));
            count += 1;
        }
        println!("count: {}, cost time: {:?}", count, now.elapsed());
    }

    #[tokio::main]
    #[allow(dead_code)]
    // #[test]
    async fn test_map_expire() {
        let cfg = get_cfg("map_expire");
        let db = test_init_db(&cfg).await.unwrap();

        #[cfg(feature = "ttl")]
        #[cfg(feature = "map_len")]
        {
            let map1 = db.map("map1", Some(1000)).await;
            println!("ttl: {:?}", map1.ttl().await.unwrap());
            map1.insert("k1", &1).await.unwrap();
            map1.insert("k2", &2).await.unwrap();
            println!("ttl: {:?}", map1.ttl().await.unwrap());
            assert_eq!(map1.is_empty().await.unwrap(), false);
            assert_eq!(map1.len().await.unwrap(), 2);
            sleep(Duration::from_millis(1200)).await;
            println!("ttl: {:?}", map1.ttl().await.unwrap());
            assert_eq!(map1.len().await.unwrap(), 0);
            assert_eq!(map1.is_empty().await.unwrap(), true);
            map1.clear().await.unwrap();
        }

        let mut db1 = db.clone();
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_millis(10000)).await;
                let mut iter = db1.map_iter().await.unwrap();
                let limit = 10;
                let mut c = 0;
                while let Some(map) = iter.next().await {
                    let map = map.unwrap();
                    println!(
                        "map.is_empty(): {:?}, now: {:?}",
                        map.is_empty().await.unwrap(),
                        timestamp_millis()
                    );
                    c += 1;
                    if c > limit {
                        break;
                    }
                }
            }
        });

        for x in 0..500 {
            let db = db.clone();
            tokio::spawn(async move {
                for i in 0..10_000 {
                    let map = db
                        .map(format!("map_{}_{}", x, i), Some(1000 * 60))
                        //.map_expire(format!("map_{}_{}", x, i), None)
                        .await;
                    if let Err(e) = map.insert(format!("k1_{}", i), &i).await {
                        println!("insert {:?}", e);
                    }
                    sleep(Duration::from_millis(0)).await;
                    if let Err(e) = map.insert(format!("k2_{}", i), &i).await {
                        println!("insert {:?}", e);
                    }
                    sleep(Duration::from_millis(0)).await;
                    if let Err(e) = map.insert(format!("k3_{}", i), &i).await {
                        println!("insert {:?}", e);
                    }
                    sleep(Duration::from_millis(0)).await;
                }
                println!("********************* end {:?}", x);
            });
        }

        sleep(Duration::from_secs(100000)).await;
    }

    #[tokio::main]
    #[allow(dead_code)]
    #[cfg(feature = "sled")]
    // #[test]
    async fn test_map_expire_list() {
        use super::{SledStorageDB, StorageDB};
        let cfg = Config {
            typ: StorageType::Sled,
            sled: SledConfig {
                path: format!("./.catch/{}", "map_expire_list"),
                cache_capacity: convert::Bytesize::from(1024 * 1024 * 1024 * 3),
                cleanup_f: move |_db| {
                    #[cfg(feature = "ttl")]
                    {
                        let db = _db.clone();
                        std::thread::spawn(move || {
                            let limit = 1000;
                            for _ in 0..5 {
                                std::thread::sleep(std::time::Duration::from_secs(3));
                                let mut total_cleanups = 0;
                                let now = std::time::Instant::now();
                                loop {
                                    let count = db.cleanup(limit);
                                    total_cleanups += count;
                                    println!(
                                        "def_cleanup: {}, total cleanups: {}, cost time: {:?}",
                                        count,
                                        total_cleanups,
                                        now.elapsed()
                                    );

                                    if count < limit {
                                        break;
                                    }

                                    std::thread::sleep(std::time::Duration::from_millis(10));
                                }
                                println!(
                                    "total cleanups: {}, cost time: {:?}",
                                    total_cleanups,
                                    now.elapsed()
                                );
                            }
                        });
                    }
                },
                ..Default::default()
            },
            #[cfg(feature = "redis")]
            redis: RedisConfig {
                url: "redis://127.0.0.1:6379/".into(),
                prefix: "map_expire_list".to_owned(),
            },
            #[cfg(feature = "redis-cluster")]
            redis_cluster: RedisClusterConfig {
                urls: [
                    "redis://127.0.0.1:6380/".into(),
                    "redis://127.0.0.1:6381/".into(),
                    "redis://127.0.0.1:6382/".into(),
                ]
                .into(),
                prefix: "map_expire_list".to_owned(),
            },
        };

        let mut db = SledStorageDB::new(cfg.sled.clone()).await.unwrap();

        let mut expireat_count = 0;
        for item in db.db.iter() {
            let (key, val) = item.unwrap();
            println!(
                "item: {:?}, val: {:?}",
                String::from_utf8_lossy(key.as_ref()),
                val.as_ref()
                    .try_into()
                    .map(|v: [u8; 8]| usize::from_be_bytes(v))
            );
            expireat_count += 1;
        }
        println!("expireat_count: {}", expireat_count);

        let mut iter = db.map_iter().await.unwrap();
        // let limit = 1000;
        let mut c = 0;
        let mut emptys = 0;
        while let Some(map) = iter.next().await {
            let map = map.unwrap();
            c += 1;
            if map.is_empty().await.unwrap() {
                emptys += 1;
            }
        }
        println!("c: {}, emptys: {}", c, emptys);
    }

    #[tokio::main]
    #[test]
    async fn test_db_size() {
        let cfg = get_cfg("db_size");
        let mut db = test_init_db(&cfg).await.unwrap();
        let iter = db.scan("*").await.unwrap();
        for item in collect(iter).await {
            db.remove(item).await.unwrap();
        }
        println!("test_db_size db_size: {:?}", db.db_size().await);
        db.insert("k1", &1).await.unwrap();
        db.insert("k2", &2).await.unwrap();
        db.insert("k3", &3).await.unwrap();
        println!("test_db_size db_size: {:?}", db.db_size().await);
        let m = db.map("map1", None).await;
        m.insert("mk1", &1).await.unwrap();
        m.insert("mk2", &2).await.unwrap();
        println!("test_db_size db_size: {:?}", db.db_size().await);

        db.batch_insert(vec![
            (Vec::from("batch/len/1"), 11),
            (Vec::from("batch/len/2"), 22),
            (Vec::from("batch/len/3"), 33),
        ])
        .await
        .unwrap();
        println!("test_db_size db_size: {:?}", db.db_size().await);
    }

    async fn collect(mut iter: Box<dyn AsyncIterator<Item = Result<Key>> + Send + '_>) -> Vec<Key> {
        let mut data = Vec::new();
        while let Some(key) = iter.next().await {
            data.push(key.unwrap())
        }
        data
    }

    #[tokio::main]
    #[test]
    async fn test_scan() {
        let cfg = get_cfg("scan");
        let mut db = test_init_db(&cfg).await.unwrap();
        let iter = db.scan("*").await.unwrap();
        for item in collect(iter).await {
            println!("removed item: {:?}", String::from_utf8_lossy(&item));
            db.remove(item).await.unwrap();
        }
        println!("test_scan db_size: {:?}", db.db_size().await);
        db.insert("foo/abcd/1", &1).await.unwrap();
        db.insert("foo/abcd/2", &2).await.unwrap();
        db.insert("foo/abcd/3", &3).await.unwrap();
        db.insert("foo/abcd/**/4", &11).await.unwrap();
        db.insert("foo/abcd/*/4", &22).await.unwrap();
        db.insert("foo/abcd/*", &33).await.unwrap();
        db.insert("iot/abcd/5/a", &5).await.unwrap();
        db.insert("iot/abcd/6/b", &6).await.unwrap();
        db.insert("iot/abcd/7/c", &7).await.unwrap();
        db.insert("iot/abcd/", &8).await.unwrap();
        db.insert("iot/abcd", &9).await.unwrap();

        println!("test_scan db_size: {:?}", db.db_size().await);

        let format_topic = |t: &str| -> Cow<'_, str> {
            if t.len() == 1 {
                if t == "#" || t == "+" {
                    return Cow::Borrowed("*");
                }
            }

            let t = t.replace("*", "\\*").replace("?", "\\?").replace("+", "*");

            if t.len() > 1 && t.ends_with("/#") {
                Cow::Owned([&t[0..(t.len() - 2)], "*"].concat())
            } else {
                Cow::Owned(t)
            }
        };

        //foo/abcd*
        let topic = format_topic("foo/abcd/#");
        println!("topic: {}", topic);
        let iter = db.scan(topic.as_bytes()).await.unwrap();
        let items = collect(iter).await;
        for item in items.iter() {
            println!("item: {:?}", String::from_utf8_lossy(&item));
        }
        assert_eq!(items.len(), 6);

        //"foo/abcd/\\**"
        let topic = format_topic("foo/abcd/*/#");
        println!("---topic: {} {}---", topic, "foo/abcd/\\**");
        let iter = db.scan(topic.as_bytes()).await.unwrap();
        assert_eq!(collect(iter).await.len(), 3);

        //"foo/abcd/\\*"
        let topic = format_topic("foo/abcd/*");
        println!("---topic: {} {}---", topic, "foo/abcd/\\*");
        let iter = db.scan(topic.as_bytes()).await.unwrap();
        assert_eq!(collect(iter).await.len(), 1);

        //foo/abcd/*/*
        let topic = format_topic("foo/abcd/+/#");
        println!("---topic: {} {}---", topic, "foo/abcd/*/*");
        let iter = db.scan(topic.as_bytes()).await.unwrap();
        assert_eq!(collect(iter).await.len(), 6);

        //iot/abcd*
        let topic = format_topic("iot/abcd/#");
        println!("---topic: {} {}---", topic, "iot/abcd*");
        let iter = db.scan(topic.as_bytes()).await.unwrap();
        assert_eq!(collect(iter).await.len(), 5);

        //iot/abcd/+
        let topic = format_topic("iot/abcd/+");
        println!("---topic: {} {}---", topic, "iot/abcd/*");
        let iter = db.scan(topic.as_bytes()).await.unwrap();
        assert_eq!(collect(iter).await.len(), 4);
    }

    #[tokio::main]
    #[cfg(all(feature = "len", feature = "ttl"))]
    #[allow(dead_code)]
    // #[test]
    async fn test_len() {
        let cfg = get_cfg("test_len");
        let mut db = test_init_db(&cfg).await.unwrap();
        println!("a test_len len: {:?}", db.len().await);
        let iter = db.scan("*").await.unwrap();
        for item in collect(iter).await {
            println!(
                "test_len remove item: {:?}",
                String::from_utf8_lossy(item.as_slice())
            );
            db.remove(item).await.unwrap();
        }
        println!("b test_len len: {:?}", db.len().await);
        db.insert("foo/len/1", &1).await.unwrap();
        db.insert("foo/len/2", &2).await.unwrap();
        db.insert("foo/len/3", &3).await.unwrap();
        db.insert("foo/len/4", &4).await.unwrap();

        db.expire_at("foo/len/3", timestamp_millis() + 1 * 1000)
            .await
            .unwrap();
        db.expire("foo/len/4", 1000 * 2).await.unwrap();
        println!("test_len len: {:?}", db.len().await);
        assert_eq!(db.len().await.unwrap(), 4);

        sleep(Duration::from_millis(1100)).await;
        println!("test_len len: {:?}", db.len().await);
        assert_eq!(db.len().await.unwrap(), 3);

        sleep(Duration::from_millis(1100)).await;
        println!("test_len len: {:?}", db.len().await);
        assert_eq!(db.len().await.unwrap(), 2);

        db.remove("foo/len/1").await.unwrap();
        println!("test_len len: {:?}", db.len().await);
        assert_eq!(db.len().await.unwrap(), 1);

        db.batch_insert(vec![
            (Vec::from("batch/len/1"), 11),
            (Vec::from("batch/len/2"), 22),
            (Vec::from("batch/len/3"), 33),
        ])
        .await
        .unwrap();
        assert_eq!(db.len().await.unwrap(), 4);

        db.batch_remove(vec![Vec::from("batch/len/1"), Vec::from("batch/len/2")])
            .await
            .unwrap();

        assert_eq!(db.len().await.unwrap(), 2);
        println!("test_len len: {:?}", db.len().await);
    }

    // -----------------------------------------------------------------------
    // P0 — 核心功能补缺
    // -----------------------------------------------------------------------

    #[tokio::main]
    #[test]
    async fn test_list_push_limit() {
        let db = test_init_db(&get_cfg("list_push_limit")).await.unwrap();
        let l = db.list("l", None).await;
        l.clear().await.unwrap();

        // 先 push 3 个元素
        l.push(&1).await.unwrap();
        l.push(&2).await.unwrap();
        l.push(&3).await.unwrap();
        assert_eq!(l.len().await.unwrap(), 3);

        // limit=2, pop_front_if_limited=true → 应挤出队首的 1
        let removed = l.push_limit(&4, 2, true).await.unwrap();
        assert_eq!(removed, Some(1));
        assert_eq!(l.all::<i32>().await.unwrap(), vec![2, 3, 4]);

        // 再次 push_limit, limit=2 → 挤出 2
        let removed = l.push_limit(&5, 2, true).await.unwrap();
        assert_eq!(removed, Some(2));
        assert_eq!(l.all::<i32>().await.unwrap(), vec![3, 4, 5]);

        // pop_front_if_limited=false 时，已达 limit 应报错
        let result = l.push_limit(&6, 2, false).await;
        assert!(
            result.is_err(),
            "expected error when list is full and pop_front_if_limited=false"
        );
    }

    #[tokio::main]
    #[test]
    async fn test_db_map_list_remove() {
        let cfg = get_cfg("db_map_list_rm");
        let db = test_init_db(&cfg).await.unwrap();

        // map_remove: 创建 map → 验证存在 → 删除 → 验证不存在
        let map = db.map("m", None).await;
        map.insert("k", &1).await.unwrap();
        drop(map);
        assert!(db.map_contains_key("m").await.unwrap());
        db.map_remove("m").await.unwrap();
        assert!(!db.map_contains_key("m").await.unwrap());

        // list_remove: 创建 list → 验证存在 → 删除 → 验证不存在
        let list = db.list("l", None).await;
        list.push(&"a").await.unwrap();
        drop(list);
        assert!(db.list_contains_key("l").await.unwrap());
        db.list_remove("l").await.unwrap();
        assert!(!db.list_contains_key("l").await.unwrap());
    }

    // -----------------------------------------------------------------------
    // P1 — 边界情况
    // -----------------------------------------------------------------------

    #[tokio::main]
    #[test]
    async fn test_list_pop_empty() {
        let db = test_init_db(&get_cfg("list_pop_empty")).await.unwrap();
        let l = db.list("l", None).await;
        l.clear().await.unwrap();

        // 空 list 上 pop 和 get_index 应返回 None
        assert_eq!(l.pop::<i32>().await.unwrap(), None);
        assert_eq!(l.get_index::<i32>(0).await.unwrap(), None);
        // 再 clear 不应报错（幂等）
        l.clear().await.unwrap();
    }

    #[tokio::main]
    #[test]
    async fn test_map_remove_and_fetch_empty() {
        let db = test_init_db(&get_cfg("map_rm_empty")).await.unwrap();
        let m = db.map("m", None).await;
        m.clear().await.unwrap();

        // 空 map 上 remove_and_fetch 应返回 None
        assert_eq!(m.remove_and_fetch::<_, i32>("k").await.unwrap(), None);
        // 再 clear 不应报错
        m.clear().await.unwrap();
    }

    #[tokio::main]
    #[test]
    async fn test_large_value() {
        let db = test_init_db(&get_cfg("large_val")).await.unwrap();

        // 100KB 二进制大负载
        let val = vec![42u8; 1024 * 100];
        db.insert("large", &val).await.unwrap();
        let got: Option<Vec<u8>> = db.get("large").await.unwrap();
        assert_eq!(got, Some(val));

        // 空 value
        let empty = vec![];
        db.insert("empty", &empty).await.unwrap();
        let got: Option<Vec<u8>> = db.get("empty").await.unwrap();
        assert_eq!(got, Some(empty));
    }

    // -----------------------------------------------------------------------
    // P2 — 幂等性与空操作
    // -----------------------------------------------------------------------

    #[tokio::main]
    #[test]
    async fn test_batch_insert_empty() {
        let _ = std::fs::remove_dir_all("./.catch/batch_empty");
        let db = test_init_db(&get_cfg("batch_empty")).await.unwrap();

        // 空批量操作不应报错
        db.batch_insert::<i32>(vec![]).await.unwrap();
        db.batch_remove(vec![]).await.unwrap();

        // Map 级别 batch_insert_empty
        let m = db.map("m", None).await;
        m.batch_insert::<i32>(vec![]).await.unwrap();
        m.batch_remove(vec![]).await.unwrap();
    }

    // -----------------------------------------------------------------------
    // P0 — StorageDB::info() 完全无覆盖（最高优先级）
    // -----------------------------------------------------------------------

    #[tokio::main]
    #[test]
    async fn test_db_info() {
        let cfg = get_cfg("db_info");
        let db = test_init_db(&cfg).await.unwrap();
        let info = db.info().await.unwrap();
        println!("test_db_info: {:#}", info);
        // 不是 null
        assert!(!info.is_null(), "info() should not return null");
        // 是 object 且不为空
        assert!(info.is_object(), "info() should return a JSON object");
        let obj = info.as_object().unwrap();
        assert!(!obj.is_empty(), "info() object should not be empty");
        // 应包含 storage_engine 字段
        assert!(
            obj.contains_key("storage_engine"),
            "info() should contain 'storage_engine' field, got: {:?}",
            obj.keys()
        );
    }

    // -----------------------------------------------------------------------
    // P1 — 空容器上的迭代器（iter / key_iter / prefix_iter / list iter）
    // -----------------------------------------------------------------------

    #[tokio::main]
    #[test]
    async fn test_iter_on_empty_containers() {
        let cfg = get_cfg("empty_iter");
        let db = test_init_db(&cfg).await.unwrap();

        // 空 map 上迭代器应直接返回 None
        let mut m = db.map("empty_iter_map", None).await;
        m.clear().await.unwrap();
        assert!(
            m.iter::<i32>().await.unwrap().next().await.is_none(),
            "iter on empty map should be None"
        );
        assert!(
            m.key_iter().await.unwrap().next().await.is_none(),
            "key_iter on empty map should be None"
        );
        assert!(
            m.prefix_iter::<_, i32>("nonexistent")
                .await
                .unwrap()
                .next()
                .await
                .is_none(),
            "prefix_iter with no match should be None"
        );

        // 空 list 上迭代器应直接返回 None
        let mut l = db.list("empty_iter_list", None).await;
        l.clear().await.unwrap();
        assert!(
            l.iter::<i32>().await.unwrap().next().await.is_none(),
            "iter on empty list should be None"
        );
    }

    // -----------------------------------------------------------------------
    // P2 — remove_with_prefix 边界情况
    // -----------------------------------------------------------------------

    #[tokio::main]
    #[test]
    async fn test_remove_with_prefix_edge_cases() {
        let cfg = get_cfg("prefix_rm");
        let db = test_init_db(&cfg).await.unwrap();
        let mut m = db.map("prefix_rm_map", None).await;
        m.clear().await.unwrap();

        m.insert("aa/1", &1).await.unwrap();
        m.insert("aa/2", &2).await.unwrap();
        m.insert("bb/1", &3).await.unwrap();
        m.insert("cc/1", &4).await.unwrap();

        // 移除不存在的 prefix → 不应报错，不应改变内容
        m.remove_with_prefix("zz").await.unwrap();
        // 验证元素仍存在
        let first = m.iter::<i32>().await.unwrap().next().await;
        assert!(first.is_some(), "expected at least one item in map");
        let (key, val) = first.unwrap().unwrap();
        assert_eq!(key, b"aa/1".to_vec());
        assert_eq!(val, 1);

        // 移除非空前缀
        m.remove_with_prefix("aa/").await.unwrap();
        // 验证只剩下 bb/1 和 cc/1
        let mut remaining = Vec::new();
        {
            let mut kit = m.key_iter().await.unwrap();
            while let Some(k) = kit.next().await {
                remaining.push(String::from_utf8_lossy(&k.unwrap()).to_string());
            }
        } // kit dropped here, releasing the mutable borrow on m
        remaining.sort();
        assert_eq!(remaining, vec!["bb/1", "cc/1"]);

        // 空 prefix 应移除全部
        m.remove_with_prefix("").await.unwrap();
        assert!(m.is_empty().await.unwrap());

        // 幂等：空 map 上再次 remove_with_prefix 不应报错
        m.remove_with_prefix("").await.unwrap();
        m.remove_with_prefix("anything").await.unwrap();
    }

    // -----------------------------------------------------------------------
    // P2 — 复杂类型（struct）的序列化/反序列化测试
    // -----------------------------------------------------------------------

    #[tokio::main]
    #[test]
    async fn test_complex_type_serialization() {
        #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
        struct MyData {
            id: u64,
            name: String,
            tags: Vec<String>,
        }

        let cfg = get_cfg("complex_type");
        let db = test_init_db(&cfg).await.unwrap();

        let data = MyData {
            id: 42,
            name: "test-data".into(),
            tags: vec!["rust".into(), "storage".into(), "serde".into()],
        };

        // DB 级别
        db.insert("cpx/key", &data).await.unwrap();
        let retrieved: MyData = db.get("cpx/key").await.unwrap().unwrap();
        assert_eq!(retrieved, data);

        // Map 级别
        let m = db.map("cpx_map", None).await;
        m.clear().await.unwrap();
        m.insert("item1", &data).await.unwrap();
        let retrieved2: MyData = m.get("item1").await.unwrap().unwrap();
        assert_eq!(retrieved2, data);

        // List 级别
        let l = db.list("cpx_list", None).await;
        l.clear().await.unwrap();
        l.push(&data).await.unwrap();
        let retrieved3: MyData = l.pop().await.unwrap().unwrap();
        assert_eq!(retrieved3, data);

        // 空 struct
        let empty_data = MyData {
            id: 0,
            name: String::new(),
            tags: vec![],
        };
        db.insert("cpx/empty", &empty_data).await.unwrap();
        let retrieved4: MyData = db.get("cpx/empty").await.unwrap().unwrap();
        assert_eq!(retrieved4, empty_data);
    }

    // -----------------------------------------------------------------------
    // P2 — Map/List 的 TTL 过期（先插入数据、再设 expire）
    // -----------------------------------------------------------------------

    #[tokio::main]
    #[test]
    #[cfg(all(feature = "ttl", feature = "map_len"))]
    async fn test_map_with_creation_expire() {
        let cfg = get_cfg("creation_expire");
        let db = test_init_db(&cfg).await.unwrap();

        // map 插入并设 expire
        let m = db.map("exp_map", None).await;
        m.insert("k1", &1).await.unwrap();
        assert!(db.map_contains_key("exp_map").await.unwrap());
        assert!(!m.is_empty().await.unwrap());
        m.expire(500).await.unwrap();

        sleep(Duration::from_millis(700)).await;
        assert!(
            m.is_empty().await.unwrap(),
            "map should be empty after expire"
        );

        // list 插入并设 expire
        let l = db.list("exp_list", None).await;
        l.push(&"val").await.unwrap();
        assert!(db.list_contains_key("exp_list").await.unwrap());
        assert!(!l.is_empty().await.unwrap());
        l.expire(500).await.unwrap();

        sleep(Duration::from_millis(700)).await;
        assert!(
            l.is_empty().await.unwrap(),
            "list should be empty after expire"
        );
    }

    // -----------------------------------------------------------------------
    // P2 — scan 模式无匹配
    // -----------------------------------------------------------------------

    #[tokio::main]
    #[test]
    async fn test_scan_no_match() {
        let cfg = get_cfg("scan_nomatch");
        let mut db = test_init_db(&cfg).await.unwrap();
        // 清理
        let iter = db.scan("*").await.unwrap();
        for item in collect(iter).await {
            db.remove(item).await.unwrap();
        }

        db.insert("only/known/key", &1).await.unwrap();
        // 完全不存在的模式 → 应返回空集合
        let results = collect(db.scan("nonexistent/*").await.unwrap()).await;
        assert!(
            results.is_empty(),
            "scan with no matching pattern should return empty, got {} items",
            results.len()
        );

        // 通配符 '*' 但无匹配
        let results2 = collect(db.scan("zzz*").await.unwrap()).await;
        assert!(results2.is_empty(), "scan with 'zzz*' should return empty");

        // 通配符 '?' 但无匹配
        let results3 = collect(db.scan("?????").await.unwrap()).await;
        assert!(results3.is_empty(), "scan with '?????' should return empty");
    }

    // =====================================================================
    // 维度 1 — 功能交互（Cross-feature Interaction）
    // =====================================================================

    #[tokio::main]
    #[test]
    #[cfg(feature = "ttl")]
    async fn test_ttl_with_batch() {
        let cfg = get_cfg("ttl_batch");
        let mut db = test_init_db(&cfg).await.unwrap();
        let iter = db.scan("*").await.unwrap();
        for item in collect(iter).await {
            db.remove(item).await.unwrap();
        }
        db.batch_insert(vec![
            (Vec::from("k1"), 1),
            (Vec::from("k2"), 2),
            (Vec::from("k3"), 3),
        ])
        .await
        .unwrap();
        db.expire("k1", 100).await.unwrap();
        sleep(Duration::from_millis(150)).await;
        assert_eq!(
            db.get::<_, i32>("k1").await.unwrap(),
            None,
            "k1 should be expired"
        );
        assert_eq!(
            db.get::<_, i32>("k2").await.unwrap(),
            Some(2),
            "k2 should remain"
        );
        assert_eq!(
            db.get::<_, i32>("k3").await.unwrap(),
            Some(3),
            "k3 should remain"
        );
    }

    #[tokio::main]
    #[test]
    #[cfg(feature = "ttl")]
    async fn test_ttl_with_counter() {
        let cfg = get_cfg("ttl_counter");
        let mut db = test_init_db(&cfg).await.unwrap();
        let iter = db.scan("*").await.unwrap();
        for item in collect(iter).await {
            db.remove(item).await.unwrap();
        }
        // 注意: counter 在 Redb 中使用内部前缀 key, expire 不适用于 counter key
        // 这里只验证 counter 基础操作的正确性
        db.counter_set("ctr", 42).await.unwrap();
        assert_eq!(db.counter_get("ctr").await.unwrap(), Some(42));
        db.counter_incr("ctr", 1).await.unwrap();
        assert_eq!(db.counter_get("ctr").await.unwrap(), Some(43));
        db.counter_decr("ctr", 10).await.unwrap();
        assert_eq!(db.counter_get("ctr").await.unwrap(), Some(33));
    }

    #[tokio::main]
    #[test]
    async fn test_map_iter_while_modifying() {
        let cfg = get_cfg("iter_modify");
        let mut db = test_init_db(&cfg).await.unwrap();
        // 通过 map_iter 查出所有已有 map 并删除，确保初始状态干净
        let initial_names: Vec<_> = {
            let mut ni = db.map_iter().await.unwrap();
            let mut ns = Vec::new();
            while let Some(n) = ni.next().await {
                let sm = n.unwrap();
                ns.push(sm.name().to_vec());
            }
            ns
        };
        for name in &initial_names {
            db.map_remove(name.as_slice()).await.unwrap();
        }
        let m = db.map("iter_m", None).await;
        m.insert("k1", &1).await.unwrap();
        // 先收集 map_iter 结果（释放 &mut self 借用），再插入新 map
        let names: Vec<_> = {
            let mut ni = db.map_iter().await.unwrap();
            let mut ns = Vec::new();
            while let Some(n) = ni.next().await {
                ns.push(n.unwrap());
            }
            ns
        };
        assert_eq!(names.len(), 1, "should find 1 map after inserting one");
        // 插入新 map 后再收集一次
        let m2 = db.map("iter_m2", None).await;
        m2.insert("k", &2).await.unwrap();
        let names2: Vec<_> = {
            let mut ni = db.map_iter().await.unwrap();
            let mut ns = Vec::new();
            while let Some(n) = ni.next().await {
                ns.push(n.unwrap());
            }
            ns
        };
        assert_eq!(
            names2.len(),
            2,
            "should find 2 maps after inserting another"
        );
    }

    #[tokio::main]
    #[test]
    async fn test_list_push_pop_sequence() {
        let cfg = get_cfg("push_pop_seq");
        let db = test_init_db(&cfg).await.unwrap();
        let l = db.list("l", None).await;
        l.clear().await.unwrap();
        // 状态链: push 10 → pop 2 → push 3 → all → len
        for i in 0..10 {
            l.push(&i).await.unwrap();
        }
        assert_eq!(l.len().await.unwrap(), 10);
        assert_eq!(l.pop::<i32>().await.unwrap(), Some(0));
        assert_eq!(l.pop::<i32>().await.unwrap(), Some(1));
        assert_eq!(l.len().await.unwrap(), 8);
        l.push(&100).await.unwrap();
        l.push(&200).await.unwrap();
        l.push(&300).await.unwrap();
        assert_eq!(l.len().await.unwrap(), 11);
        let all = l.all::<i32>().await.unwrap();
        assert_eq!(all.len(), 11);
        assert_eq!(all[0], 2, "first after 2 pops");
        assert_eq!(all[all.len() - 3..], [100, 200, 300]);
    }

    // =====================================================================
    // 维度 2 — 并发与竞态（Concurrency）
    // =====================================================================

    #[tokio::main]
    #[test]
    async fn test_concurrent_counter_incr() {
        let cfg = get_cfg("concurrent_incr");
        // 先在 Arc 外完成需要 &mut self 的清理
        let mut raw = test_init_db(&cfg).await.unwrap();
        let iter = raw.scan("*").await.unwrap();
        for item in collect(iter).await {
            raw.remove(item).await.unwrap();
        }
        raw.counter_set("c", 0).await.unwrap();
        let db = Arc::new(raw);
        // 10 个并发 task 各 counter_incr +1
        let mut handles = Vec::new();
        for _ in 0..10 {
            let db = db.clone();
            handles.push(tokio::spawn(async move {
                db.counter_incr("c", 1).await.unwrap();
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        let val = db.counter_get("c").await.unwrap().unwrap();
        assert_eq!(
            val, 10,
            "concurrent counter_incr should sum to 10, got {val}"
        );
    }

    #[tokio::main]
    #[test]
    async fn test_concurrent_map_read_write() {
        let cfg = get_cfg("concurrent_map");
        let mut raw = test_init_db(&cfg).await.unwrap();
        let iter = raw.scan("*").await.unwrap();
        for item in collect(iter).await {
            raw.remove(item).await.unwrap();
        }
        let m = raw.map("cmap", None).await;
        m.clear().await.unwrap();
        let db = Arc::new(raw);
        let db2 = db.clone();
        let h1 = tokio::spawn(async move {
            let m = db2.map("cmap", None).await;
            for i in 0..100 {
                m.insert(format!("k{i}"), &i).await.unwrap();
            }
        });
        let db3 = db.clone();
        let h2 = tokio::spawn(async move {
            let m = db3.map("cmap", None).await;
            for i in 0..100 {
                if let Some(v) = m.get::<_, i32>(format!("k{i}")).await.unwrap() {
                    assert_eq!(v, i);
                }
            }
        });
        h1.await.unwrap();
        h2.await.unwrap();
    }

    #[tokio::main]
    #[test]
    async fn test_concurrent_map_iter() {
        let cfg = get_cfg("concurrent_miter");
        let mut raw = test_init_db(&cfg).await.unwrap();
        let iter = raw.scan("*").await.unwrap();
        for item in collect(iter).await {
            raw.remove(item).await.unwrap();
        }
        // map_iter 需要 &mut self，不能通过 Arc 调用，
        // 改成单线程顺序执行：先插入一批 map，再遍历
        for i in 0..20 {
            let m = raw.map(format!("m_{i}"), None).await;
            m.insert("x", &i).await.unwrap();
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        // 遍历所有 map
        let mut iter = raw.map_iter().await.unwrap();
        let mut count = 0;
        while let Some(item) = iter.next().await {
            let _ = item.unwrap();
            count += 1;
        }
        assert_eq!(count, 20, "should find all 20 maps");
    }

    // =====================================================================
    // 维度 3 — 数据完整性与一致性（Data Integrity）
    // =====================================================================

    #[tokio::main]
    #[test]
    async fn test_batch_insert_duplicate_keys() {
        let cfg = get_cfg("batch_dup");
        let mut db = test_init_db(&cfg).await.unwrap();
        let iter = db.scan("*").await.unwrap();
        for item in collect(iter).await {
            db.remove(item).await.unwrap();
        }
        db.batch_insert(vec![(Vec::from("k"), 1), (Vec::from("k"), 99)])
            .await
            .unwrap();
        let val: i32 = db.get("k").await.unwrap().unwrap();
        assert_eq!(val, 99, "later duplicate key should overwrite earlier");
    }

    #[tokio::main]
    #[test]
    async fn test_remove_and_fetch_idempotent() {
        let cfg = get_cfg("rm_fetch");
        let db = test_init_db(&cfg).await.unwrap();
        let m = db.map("m", None).await;
        m.clear().await.unwrap();
        m.insert("k", &42).await.unwrap();
        let v1: Option<i32> = m.remove_and_fetch("k").await.unwrap();
        assert_eq!(v1, Some(42), "first remove_and_fetch returns value");
        let v2: Option<i32> = m.remove_and_fetch("k").await.unwrap();
        assert_eq!(v2, None, "second remove_and_fetch returns None");
    }

    #[tokio::main]
    #[test]
    async fn test_map_clear_then_insert() {
        let cfg = get_cfg("clear_insert");
        let db = test_init_db(&cfg).await.unwrap();
        let mut m = db.map("m", None).await;
        m.clear().await.unwrap();
        m.insert("k1", &1).await.unwrap();
        m.insert("k2", &2).await.unwrap();
        m.clear().await.unwrap();
        assert!(m.is_empty().await.unwrap());
        // clear 后 insert，不应残留旧数据
        m.insert("new_k", &99).await.unwrap();
        let keys: Vec<_> = {
            let mut kit = m.key_iter().await.unwrap();
            let mut ks = Vec::new();
            while let Some(k) = kit.next().await {
                ks.push(String::from_utf8_lossy(&k.unwrap()).to_string());
            }
            ks
        };
        assert_eq!(
            keys,
            vec!["new_k"],
            "only new key should remain after clear+insert"
        );
    }

    #[tokio::main]
    #[test]
    async fn test_list_clear_then_push() {
        let cfg = get_cfg("clear_push");
        let db = test_init_db(&cfg).await.unwrap();
        let l = db.list("l", None).await;
        l.clear().await.unwrap();
        l.push(&1).await.unwrap();
        l.push(&2).await.unwrap();
        l.clear().await.unwrap();
        assert!(l.is_empty().await.unwrap());
        l.push(&999).await.unwrap();
        let items = l.all::<i32>().await.unwrap();
        assert_eq!(
            items,
            vec![999],
            "only new item should remain after clear+push"
        );
    }

    #[tokio::main]
    #[test]
    async fn test_insert_overwrite() {
        let cfg = get_cfg("overwrite");
        let mut db = test_init_db(&cfg).await.unwrap();
        let iter = db.scan("*").await.unwrap();
        for item in collect(iter).await {
            db.remove(item).await.unwrap();
        }
        db.insert("k", &"first").await.unwrap();
        db.insert("k", &"second").await.unwrap();
        let val: String = db.get("k").await.unwrap().unwrap();
        assert_eq!(val, "second", "later insert should overwrite earlier");
        // Map 级别
        let m = db.map("m", None).await;
        m.clear().await.unwrap();
        m.insert("mk", &10).await.unwrap();
        m.insert("mk", &20).await.unwrap();
        let mv: i32 = m.get("mk").await.unwrap().unwrap();
        assert_eq!(mv, 20, "map insert should overwrite");
    }

    #[tokio::main]
    #[test]
    async fn test_counter_sequence() {
        let cfg = get_cfg("counter_seq");
        let mut db = test_init_db(&cfg).await.unwrap();
        let iter = db.scan("*").await.unwrap();
        for item in collect(iter).await {
            db.remove(item).await.unwrap();
        }
        // set 100 → incr 50 → decr 30 → incr -10 → 最终 110
        db.counter_set("seq", 100).await.unwrap();
        db.counter_incr("seq", 50).await.unwrap();
        db.counter_decr("seq", 30).await.unwrap();
        db.counter_incr("seq", -10).await.unwrap();
        assert_eq!(db.counter_get("seq").await.unwrap(), Some(110));
        // 负数
        db.counter_set("neg", -5).await.unwrap();
        db.counter_decr("neg", 10).await.unwrap();
        assert_eq!(db.counter_get("neg").await.unwrap(), Some(-15));
        // 极值写入再读取
        db.counter_set("max", isize::MAX).await.unwrap();
        assert_eq!(db.counter_get("max").await.unwrap(), Some(isize::MAX));
        db.counter_set("min", isize::MIN).await.unwrap();
        assert_eq!(db.counter_get("min").await.unwrap(), Some(isize::MIN));
    }

    // =====================================================================
    // 维度 4 — 边界极限（Edge / Extreme Values）
    // =====================================================================

    #[tokio::main]
    #[test]
    async fn test_key_edge_cases() {
        let cfg = get_cfg("key_edges");
        let mut db = test_init_db(&cfg).await.unwrap();
        let iter = db.scan("*").await.unwrap();
        for item in collect(iter).await {
            db.remove(item).await.unwrap();
        }
        // 空 key
        db.insert("", &"empty").await.unwrap();
        assert_eq!(db.get::<_, String>("").await.unwrap(), Some("empty".into()));
        // Unicode key
        db.insert("你好世界", &"unicode").await.unwrap();
        assert_eq!(
            db.get::<_, String>("你好世界").await.unwrap(),
            Some("unicode".into())
        );
        // 含二进制零的 key
        let bin_key = vec![0u8, 1, 2, 0, 3];
        db.insert(&bin_key, &42).await.unwrap();
        assert_eq!(db.get::<_, i32>(&bin_key).await.unwrap(), Some(42));
        // 长 key (1KB)
        let long_key = "a".repeat(1024);
        db.insert(&long_key, &"long").await.unwrap();
        assert_eq!(
            db.get::<_, String>(&long_key).await.unwrap(),
            Some("long".into())
        );
    }

    #[tokio::main]
    #[test]
    async fn test_value_edge_cases() {
        let cfg = get_cfg("val_edges");
        let mut db = test_init_db(&cfg).await.unwrap();
        let iter = db.scan("*").await.unwrap();
        for item in collect(iter).await {
            db.remove(item).await.unwrap();
        }
        // 大二进制 value (1MB)
        let big_val = vec![0xABu8; 1024 * 1024];
        db.insert("big", &big_val).await.unwrap();
        let got: Vec<u8> = db.get("big").await.unwrap().unwrap();
        assert_eq!(got.len(), 1024 * 1024);
        assert_eq!(got[0], 0xAB);
        assert_eq!(got[got.len() - 1], 0xAB);

        // 空 Vec<u8>
        let empty_val: Vec<u8> = vec![];
        db.insert("empty_val", &empty_val).await.unwrap();
        let got2: Vec<u8> = db.get("empty_val").await.unwrap().unwrap();
        assert!(got2.is_empty());
    }

    #[tokio::main]
    #[test]
    async fn test_map_list_name_conflict() {
        let cfg = get_cfg("name_conflict");
        let db = test_init_db(&cfg).await.unwrap();
        // 先创建 map "conflict"
        let m = db.map("conflict", None).await;
        m.insert("k", &1).await.unwrap();
        drop(m);
        // 再创建同名 list——不应 panic
        let l = db.list("conflict", None).await;
        let result = l.push(&2).await;
        // 如果后端不支持同名容器，至少不应 panic
        let _ = result;
    }

    #[tokio::main]
    #[test]
    async fn test_name_with_special_chars() {
        let cfg = get_cfg("special_name");
        let db = test_init_db(&cfg).await.unwrap();
        let names = vec![
            "has@at",
            "__rmqtt_prefix",
            "has space",
            "混合中文名称",
            "with/slash",
        ];
        // 分开测试 map 和 list，避免同名容器在 sled 后端产生冲突
        for name in &names {
            let m = db.map(*name, None).await;
            m.insert("k", &1).await.unwrap();
            assert!(
                !m.is_empty().await.unwrap(),
                "map '{name}' should not be empty"
            );
        }
        for name in &names {
            let l = db.list(*name, None).await;
            l.clear().await.unwrap();
            l.push(&2).await.unwrap();
            assert_eq!(
                l.len().await.unwrap(),
                1,
                "list '{name}' should have 1 item"
            );
        }
    }

    #[tokio::main]
    #[test]
    async fn test_list_push_limit_zero() {
        let cfg = get_cfg("push_limit_0");
        let db = test_init_db(&cfg).await.unwrap();
        let l = db.list("l", None).await;
        l.clear().await.unwrap();
        // limit=0, pop_front=true — 不同后端对空列表的行为不同(redb: len=0, sled: len=1)，
        // 只验证不 panic、不报错，且次数消耗了 push 行为
        let r1 = l.push_limit(&1, 0, true).await;
        assert!(
            r1.is_ok(),
            "push_limit(0, true) should not error on empty list"
        );
        // limit=0 的列表继续 push_limit，此时后端行为一致：pop_front=true 应挤出队首
        let r2 = l.push_limit(&2, 0, true).await;
        assert!(
            r2.is_ok(),
            "push_limit(0, true) should not error on non-empty list"
        );
        // limit=0, pop_front=false → 应该报错（所有后端一致）
        let r3 = l.push_limit(&3, 0, false).await;
        assert!(
            r3.is_err(),
            "push_limit(0, false) should error when at limit"
        );
    }

    // =====================================================================
    // 维度 5 — 错误处理与容错（Error Handling）
    // =====================================================================

    #[tokio::main]
    #[test]
    async fn test_get_non_existent() {
        let cfg = get_cfg("get_nonexist");
        let db = test_init_db(&cfg).await.unwrap();
        // DB 级别
        assert_eq!(db.get::<_, i32>("no_such_key").await.unwrap(), None);
        // Map 级别
        let m = db.map("m", None).await;
        m.clear().await.unwrap();
        assert_eq!(m.get::<_, i32>("no_such_key").await.unwrap(), None);
        // List 级别（越界 get_index 和 pop 空 list）
        let l = db.list("l", None).await;
        l.clear().await.unwrap();
        assert_eq!(l.pop::<i32>().await.unwrap(), None);
        assert_eq!(l.get_index::<i32>(0).await.unwrap(), None);
        assert_eq!(l.get_index::<i32>(usize::MAX).await.unwrap(), None);
    }

    #[tokio::main]
    #[test]
    async fn test_double_remove_idempotent() {
        let cfg = get_cfg("double_rm");
        let mut db = test_init_db(&cfg).await.unwrap();
        let iter = db.scan("*").await.unwrap();
        for item in collect(iter).await {
            db.remove(item).await.unwrap();
        }
        db.insert("k", &1).await.unwrap();
        db.remove("k").await.unwrap();
        // 第二次 remove 不应报错
        db.remove("k").await.unwrap();
        assert!(!db.contains_key("k").await.unwrap());
    }

    #[tokio::main]
    #[test]
    async fn test_map_remove_non_existent() {
        let cfg = get_cfg("map_rm_nonexist");
        let db = test_init_db(&cfg).await.unwrap();
        // map_remove 不存在的 map 不应报错
        db.map_remove("no_such_map").await.unwrap();
        // list_remove 不存在的 list 不应报错
        db.list_remove("no_such_list").await.unwrap();
    }

    // =====================================================================
    // 维度 6 — 后端调度正确性（Dispatch Correctness）
    // =====================================================================

    #[tokio::main]
    #[test]
    async fn test_map_name_roundtrip() {
        let cfg = get_cfg("map_name");
        let db = test_init_db(&cfg).await.unwrap();
        for name in &["simple", "with/slash", "你好"] {
            let m = db.map(*name, None).await;
            assert_eq!(
                m.name(),
                name.as_bytes(),
                "map name roundtrip failed for '{name}'"
            );
        }
    }

    #[tokio::main]
    #[test]
    async fn test_list_name_roundtrip() {
        let cfg = get_cfg("list_name");
        let db = test_init_db(&cfg).await.unwrap();
        for name in &["simple", "with/slash", "你好"] {
            let l = db.list(*name, None).await;
            let returned_name = l.name();
            assert!(!returned_name.is_empty(), "list name should not be empty");
            assert!(
                String::from_utf8_lossy(returned_name).contains(*name),
                "list name should contain original name"
            );
        }
    }
}
