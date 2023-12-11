#[macro_use]
extern crate serde;

use anyhow::Error;
use serde::de;

use storage_redis::RedisStorageDB;
use storage_sled::SledConfig;
use storage_sled::SledStorageDB;

mod storage;
mod storage_redis;
mod storage_sled;

use crate::storage_redis::RedisConfig;
pub use storage::{DefaultStorageDB, List, Map, StorageDB, StorageList, StorageMap};

pub type Result<T> = anyhow::Result<T>;

pub async fn init_db(cfg: &Config) -> Result<DefaultStorageDB> {
    match cfg.storage_type {
        StorageType::Sled => {
            let db = SledStorageDB::new(cfg.sled.clone()).await?;
            Ok(DefaultStorageDB::Sled(db))
        }
        StorageType::Redis => {
            let db = RedisStorageDB::new(cfg.redis.clone()).await?;
            Ok(DefaultStorageDB::Redis(db))
        }
    }
}
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    #[serde(default = "Config::storage_type_default")]
    pub storage_type: StorageType,
    #[serde(default)]
    pub sled: SledConfig,
    #[serde(default)]
    pub redis: RedisConfig,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            storage_type: Config::storage_type_default(),
            sled: SledConfig::default(),
            redis: RedisConfig::default(),
        }
    }
}

impl Config {
    fn storage_type_default() -> StorageType {
        StorageType::Sled
    }
}

#[derive(Debug, Clone, Serialize)]
pub enum StorageType {
    //sled: high-performance embedded database with BTreeMap-like API for stateful systems.
    Sled,
    //redis:
    Redis,
}

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
            "sled" => StorageType::Sled,
            "redis" => StorageType::Redis,
            _ => StorageType::Sled,
        };
        Ok(t)
    }
}

#[allow(dead_code)]
pub(crate) type TimestampMillis = i64;

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
mod tests {
    use super::storage::*;
    use super::*;
    use std::time::Duration;

    fn get_cfg(name: &str) -> Config {
        std::env::set_var("RUST_LOG", "rmqtt_storage=info");
        env_logger::init();

        let cfg = Config {
            storage_type: StorageType::Sled,
            sled: SledConfig {
                path: format!("./.catch/{}", name),
                gc_at_hour: 0,
                gc_at_minute: 1,
                ..Default::default()
            },
            redis: RedisConfig {
                url: "redis://127.0.0.1:6379/".into(),
                prefix: name.to_owned(),
            },
        };
        cfg
    }

    #[tokio::main]
    #[test]
    async fn test_stress() {
        let cfg = get_cfg("stress");
        let db = init_db(&cfg).await.unwrap();
        let now = std::time::Instant::now();
        for i in 0..10_000usize {
            db.insert(i.to_be_bytes(), &i).await.unwrap();
        }
        let k_9999_val = db.get::<_, usize>(9999usize.to_be_bytes()).await.unwrap();
        println!("9999: {:?}, cost time: {:?}", k_9999_val, now.elapsed());
        assert_eq!(k_9999_val, Some(9999));

        let s_m_1 = db.map("s_m_1");
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
            "s_m_1 9999: {:?}, cost time: {:?}",
            k_9999_val,
            now.elapsed()
        );
        assert_eq!(k_9999_val, Some(9999));

        let s_l_1 = db.list("s_l_1");
        s_l_1.clear().await.unwrap();
        let now = std::time::Instant::now();
        for i in 0..10_000usize {
            s_l_1.push(&i).await.unwrap();
        }
        println!("s_l_1: {:?}", s_l_1.len().await.unwrap());
        assert_eq!(s_l_1.len().await.unwrap(), 10_000);
        let l_9999_val = s_l_1.get_index::<usize>(9999).await.unwrap();
        println!(
            "s_l_1 9999: {:?}, cost time: {:?}",
            l_9999_val,
            now.elapsed()
        );
        assert_eq!(l_9999_val, Some(9999));

        tokio::time::sleep(Duration::from_secs(1)).await;

        let now = std::time::Instant::now();
        for i in 0..10_000usize {
            let s_m = db.map(format!("s_m_{}", i));
            s_m.insert(i.to_be_bytes(), &i).await.unwrap();
        }
        println!("s_m, cost time: {:?}", now.elapsed());

        let now = std::time::Instant::now();
        for i in 0..10_000usize {
            let s_l = db.list(format!("s_l_{}", i));
            s_l.push(&i).await.unwrap();
        }
        println!("s_l, cost time: {:?}", now.elapsed());
    }

    #[cfg(feature = "ttl")]
    #[tokio::main]
    #[test]
    async fn test_expiration_cleaning() {
        //Clear Expired Cleanup
        let cfg = get_cfg("expiration_cleaning");
        let db = init_db(&cfg).await.unwrap();
        for i in 0..3usize {
            let key = format!("k_{}", i);
            db.insert(key.as_bytes(), &format!("v_{}", (i * 10)))
                .await
                .unwrap();
            let res = db.expire(key, 1500).await.unwrap();
            println!("expire res: {:?}", res);
        }

        let m_1 = db.map("m_1");
        m_1.insert("m_k_1", &1).await.unwrap();
        m_1.insert("m_k_2", &2).await.unwrap();
        let res = m_1.expire(1500).await.unwrap();
        println!("m_1 expire res: {:?}", res);

        let l_1 = db.list("l_1");
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
        assert_eq!(l_all, vec![]);

        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    #[tokio::main]
    #[test]
    async fn test_db_insert() {
        let cfg = get_cfg("db_insert");

        let db = init_db(&cfg).await.unwrap();
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

        let map_1 = db.map("map_1");
        map_1.insert("m_k_1", &100).await.unwrap();
        assert!(db.map_contains_key("map_1").await.unwrap());

        let map_2 = db.map("map_2");
        map_2.clear().await.unwrap();
        println!(
            "test_db_insert contains_key(map_2) {:?}",
            db.map_contains_key("map_2").await
        );
        assert!(!db.map_contains_key("map_2").await.unwrap());

        let list_1 = db.list("list_1");
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

        let db = init_db(&cfg).await.unwrap();
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

        let m2 = db.map(db_key_2);
        m2.clear().await.unwrap();
        assert_eq!(db.contains_key(db_key_2).await.unwrap(), false);
        m2.insert("m_k_1", &100).await.unwrap();
        assert_eq!(db.map_contains_key(db_key_2).await.unwrap(), true);
        m2.clear().await.unwrap();
        assert_eq!(db.map_contains_key(db_key_2).await.unwrap(), false);
        m2.insert("m_k_1", &100).await.unwrap();
        assert_eq!(db.map_contains_key(db_key_2).await.unwrap(), true);
        m2.remove("m_k_1").await.unwrap();
        assert_eq!(db.map_contains_key(db_key_2).await.unwrap(), false);
    }

    #[tokio::main]
    #[test]
    async fn test_db_contains_key() {
        let cfg = get_cfg("db_contains_key");
        let db = init_db(&cfg).await.unwrap();
        db.remove("test_c_001").await.unwrap();
        let c_res = db.contains_key("test_c_001").await.unwrap();
        assert!(!c_res);

        db.insert("test_c_001", &"val_001").await.unwrap();
        let c_res = db.contains_key("test_c_001").await.unwrap();
        assert!(c_res);

        let map_001 = db.map("map_001");
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

        let l1 = db.list("list_001");
        l1.push(&"aa").await.unwrap();
        l1.push(&"bb").await.unwrap();
        assert_eq!(l1.is_empty().await.unwrap(), false);
        let c_res = db.list_contains_key("list_001").await.unwrap();
        assert!(c_res);

        let map_002 = db.map("map_002");
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

        let list_002 = db.list("list_002");
        let c_res = db.list_contains_key("list_002").await.unwrap();
        assert!(!c_res);
        assert_eq!(list_002.is_empty().await.unwrap(), true);
    }

    #[cfg(feature = "ttl")]
    #[tokio::main]
    #[test]
    async fn test_db_expire() {
        let cfg = get_cfg("expire");
        let db = init_db(&cfg).await.unwrap();

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
        assert_eq!(expire_res, expire_res);
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
        assert_eq!(expire_res, expire_res);
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
        let mut ttl_001 = db.map("ttl_001");
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

        let ttl_001_res = ttl_001.ttl().await.unwrap().unwrap();
        println!("4 test_db_expire map ttl_001_res: {:?}", ttl_001_res);
        assert!(ttl_001_res <= 1 * 1000);

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
        assert_eq!(expire_res, expire_res);
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
        let mut l_ttl_001 = db.list("l_ttl_001");
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
        assert_eq!(expire_res, expire_res);
        let l_ttl_001_res = l_ttl_001.ttl().await.unwrap().unwrap();
        println!("x0 test_db_expire list l_ttl_001_res: {:?}", l_ttl_001_res);
        assert!(l_ttl_001_res <= 1 * 1000);
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let l_ttl_001_res = l_ttl_001.ttl().await.unwrap().unwrap();
        println!("x1 test_db_expire list l_ttl_001_res: {:?}", l_ttl_001_res);
        assert!(l_ttl_001_res <= 500);
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
        let db = init_db(&cfg).await.unwrap();

        let map001 = db.map("001");
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
        let db = init_db(&cfg).await.unwrap();

        let map001 = db.map("m001");
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
        let db = init_db(&cfg).await.unwrap();

        let kv001 = db.map("tree_kv001");
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
    async fn test_map_retain() {
        let cfg = get_cfg("map_retain");
        let db = init_db(&cfg).await.unwrap();

        let map1 = db.map("map1");
        map1.clear().await.unwrap();
        for i in 0..100usize {
            map1.insert(format!("mk_{}", i), &i).await.unwrap();
        }
        #[cfg(feature = "map_len")]
        assert_eq!(map1.len().await.unwrap(), 100);
        map1.retain::<_, _, usize>(|item| async {
            match item {
                Ok((_k, v)) => v != 10,
                Err(e) => {
                    log::warn!("{:?}", e);
                    false
                }
            }
        })
        .await
        .unwrap();
        #[cfg(feature = "map_len")]
        assert_eq!(map1.len().await.unwrap(), 99);

        map1.retain_with_key(|item| async {
            match item {
                Ok(k) => k != b"mk_20",
                Err(e) => {
                    log::warn!("{:?}", e);
                    false
                }
            }
        })
        .await
        .unwrap();
        #[cfg(feature = "map_len")]
        assert_eq!(map1.len().await.unwrap(), 98);
    }

    #[tokio::main]
    #[test]
    async fn test_batch() {
        let cfg = get_cfg("batch");
        let db = init_db(&cfg).await.unwrap();

        let skv = db.map("batch_kv001");

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
        let db = init_db(&cfg).await.unwrap();

        let mut skv = db.map("iter_kv002");
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
        let db = init_db(&cfg).await.unwrap();

        let array_a = db.list("array_a");
        let array_b = db.list("array_b");
        let mut array_c = db.list("array_c");

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
        let db = init_db(&cfg).await.unwrap();

        let ml001 = db.map("m_l_001");
        ml001.clear().await.unwrap();
        #[cfg(feature = "map_len")]
        assert_eq!(ml001.len().await.unwrap(), 0);
        assert_eq!(ml001.is_empty().await.unwrap(), true);

        let mut l001 = db.list("l_001");
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

        let l002 = db.list("l_002");
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
        let mut db = init_db(&cfg).await.unwrap();

        let l1 = db.list("l1");
        let l2 = db.list("l2");
        let l3 = db.list("l3");
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
    async fn test_map_iter() {
        let cfg = get_cfg("async_map_iter");
        let mut db = init_db(&cfg).await.unwrap();

        let m1 = db.map("m1");
        let m2 = db.map("m2");
        let m3 = db.map("m3");

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
        let db = init_db(&cfg).await.unwrap();

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
        let db = init_db(&cfg).await.unwrap();

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
        let db = init_db(&cfg).await.unwrap();
        let l11 = db.list("l11");
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
        let db = init_db(&cfg).await.unwrap();
        let l11 = db.list("l11");
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

        let pop_v = l11
            .pop_f::<_, i32>(|v| {
                println!("left val: {:?}", v);
                *v == 2
            })
            .await
            .unwrap();
        println!("pop val: {:?}", pop_v);
        println!(
            "all: {:?}, len: {:?}",
            l11.all::<i32>().await.unwrap(),
            l11.len().await
        );
        assert_eq!(l11.len().await.unwrap(), 7);
        assert_eq!(l11.all::<i32>().await.unwrap(), vec![3, 4, 5, 6, 7, 8, 9]);

        let pop_v = l11.pop_f::<_, i32>(|v| *v == 2).await.unwrap();
        println!("pop val: {:?}", pop_v);
        println!(
            "all: {:?}, len: {:?}",
            l11.all::<i32>().await.unwrap(),
            l11.len().await
        );
        assert_eq!(l11.len().await.unwrap(), 7);
        assert_eq!(l11.all::<i32>().await.unwrap(), vec![3, 4, 5, 6, 7, 8, 9]);

        l11.clear().await.unwrap();
        assert_eq!(l11.len().await.unwrap(), 0);
        assert_eq!(l11.all::<i32>().await.unwrap(), vec![]);

        let pop_v = l11.pop_f::<_, i32>(|_| true).await.unwrap();
        println!("pop val: {:?}", pop_v);
        println!(
            "all: {:?}, len: {:?}",
            l11.all::<i32>().await.unwrap(),
            l11.len().await
        );
        assert_eq!(l11.len().await.unwrap(), 0);
        assert_eq!(l11.all::<i32>().await.unwrap(), vec![]);
    }
}
