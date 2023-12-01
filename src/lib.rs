#[macro_use]
extern crate serde;
extern crate core;

use anyhow::Error;
use serde::de;

use storage_redis::RedisStorageDB;
use storage_sled::SledStorageDB;

mod storage;
mod storage_redis;
mod storage_sled;

pub use storage::{DefaultStorageDB, List, Map, StorageDB};
pub use storage_sled::SledConfig;

pub type Result<T> = anyhow::Result<T>;

pub async fn init_db(cfg: &Config) -> Result<DefaultStorageDB> {
    match cfg.storage_type {
        StorageType::Sled => {
            let db = SledStorageDB::new(cfg.sled.clone())?;
            Ok(DefaultStorageDB::Sled(db))
        }
        StorageType::Redis => {
            let db = RedisStorageDB::new(&cfg.redis).await?;
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
    pub redis: String,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            storage_type: Config::storage_type_default(),
            sled: SledConfig::default(),
            redis: String::default(),
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

pub(crate) type TimestampMillis = usize;

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
    use super::*;
    use std::time::Duration;

    fn get_cfg(name: &str) -> Config {
        let cfg = Config {
            storage_type: StorageType::Sled,
            sled: SledConfig {
                path: format!("./.catch/{}", name),
                gc_at_hour: 13,
                gc_at_minute: 38,
                ..Default::default()
            },
            redis: "redis://127.0.0.1:6379/".into(),
        };
        cfg
    }

    #[tokio::main]
    #[test]
    async fn test_stress() {
        let cfg = get_cfg("stress");
        let mut db = init_db(&cfg).await.unwrap();
        let now = std::time::Instant::now();
        for i in 0..10_000usize {
            db.insert(i.to_be_bytes(), &i).await.unwrap();
        }
        let k_9999_val = db.get::<_, usize>(9999usize.to_be_bytes()).await.unwrap();
        println!("9999: {:?}, cost time: {:?}", k_9999_val, now.elapsed());
        assert_eq!(k_9999_val, Some(9999));

        let mut s_m_1 = db.map("s_m_1").unwrap();
        let now = std::time::Instant::now();
        for i in 0..10_000usize {
            s_m_1.insert(i.to_be_bytes(), &i).await.unwrap();
        }
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

        let s_l_1 = db.list("s_l_1").unwrap();
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
    }

    #[tokio::main]
    #[test]
    async fn test_expiration_cleaning() {
        //Clear Expired Cleanup
        let cfg = get_cfg("expiration_cleaning");
        let mut db = init_db(&cfg).await.unwrap();
        for i in 0..3usize {
            let key = format!("k_{}", i);
            db.insert(key.as_bytes(), &format!("v_{}", (i * 10)))
                .await
                .unwrap();
            let res = db.expire(key, 1500).await.unwrap();
            println!("expire res: {:?}", res);
        }

        let mut m_1 = db.map("m_1").unwrap();
        m_1.insert("m_k_1", &1).await.unwrap();
        m_1.insert("m_k_2", &2).await.unwrap();
        let res = db.expire("m_1", 1500).await.unwrap();
        println!("m_1 expire res: {:?}", res);

        let l_1 = db.list("l_1").unwrap();
        l_1.clear().await.unwrap();
        l_1.push(&11).await.unwrap();
        l_1.push(&22).await.unwrap();

        let res = db.expire("l_1", 1500).await.unwrap();
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

        let mut db = init_db(&cfg).await.unwrap();
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

        let mut map_1 = db.map("map_1").unwrap();
        map_1.insert("m_k_1", &100).await.unwrap();
        assert!(db.contains_key("map_1").await.unwrap());

        let mut map_2 = db.map("map_2").unwrap();
        map_2.clear().await.unwrap();
        println!(
            "test_db_insert contains_key(map_2) {:?}",
            db.contains_key("map_2").await
        );
        assert!(!db.contains_key("map_2").await.unwrap());

        let list_1 = db.list("list_1").unwrap();
        list_1.clear().await.unwrap();
        println!(
            "test_db_insert contains_key(list_1) {:?}",
            db.contains_key("list_1").await
        );
        assert!(!db.contains_key("list_1").await.unwrap());
        list_1.push(&20).await.unwrap();
        assert!(db.contains_key("list_1").await.unwrap());
    }

    #[tokio::main]
    #[test]
    async fn test_db_remove() {
        let cfg = get_cfg("db_remove");

        let mut db = init_db(&cfg).await.unwrap();
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

        let mut m2 = db.map(db_key_2).unwrap();
        m2.clear().await.unwrap();
        assert_eq!(db.contains_key(db_key_2).await.unwrap(), false);
        m2.insert("m_k_1", &100).await.unwrap();
        assert_eq!(db.contains_key(db_key_2).await.unwrap(), true);
        m2.clear().await.unwrap();
        assert_eq!(db.contains_key(db_key_2).await.unwrap(), false);
        m2.insert("m_k_1", &100).await.unwrap();
        assert_eq!(db.contains_key(db_key_2).await.unwrap(), true);
        m2.remove("m_k_1").await.unwrap();
        assert_eq!(db.contains_key(db_key_2).await.unwrap(), false);

        // let l1 = m2.list(db_key_2).unwrap();
        // l1.clear().await.unwrap();
        // assert_eq!(db.contains_key(db_key_2).await.unwrap(), false);
        // l1.push(&11).await.unwrap();
        // l1.push(&12).await.unwrap();
        // l1.push(&13).await.unwrap();
        // assert_eq!(db.contains_key(db_key_2).await.unwrap(), true);
        // l1.clear().await.unwrap();
        // assert_eq!(db.contains_key(db_key_2).await.unwrap(), false);
    }

    #[tokio::main]
    #[test]
    async fn test_db_contains_key() {
        let cfg = get_cfg("db_contains_key");
        let mut db = init_db(&cfg).await.unwrap();
        db.remove("test_c_001").await.unwrap();
        let c_res = db.contains_key("test_c_001").await.unwrap();
        assert!(!c_res);

        db.insert("test_c_001", &"val_001").await.unwrap();
        let c_res = db.contains_key("test_c_001").await.unwrap();
        assert!(c_res);

        let mut map_001 = db.map("map_001").unwrap();
        map_001.clear().await.unwrap();
        map_001.insert("k1", &1).await.unwrap();
        assert_eq!(map_001.is_empty().await.unwrap(), false);
        assert_eq!(map_001.len().await.unwrap(), 1);
        let c_res = db.contains_key("map_001").await.unwrap();
        assert!(c_res);
        map_001.clear().await.unwrap();
        assert_eq!(map_001.is_empty().await.unwrap(), true);
        assert_eq!(map_001.len().await.unwrap(), 0);
        let c_res = db.contains_key("map_001").await.unwrap();
        assert!(!c_res);

        let l1 = db.list("list_001").unwrap();
        l1.push(&"aa").await.unwrap();
        l1.push(&"bb").await.unwrap();
        assert_eq!(l1.is_empty().await.unwrap(), false);
        let c_res = db.contains_key("list_001").await.unwrap();
        assert!(c_res);

        let mut map_002 = db.map("map_002").unwrap();
        map_002.clear().await.unwrap();
        println!("test_db_contains_key len: {}", map_002.len().await.unwrap());
        println!(
            "test_db_contains_key is_empty: {}",
            map_002.is_empty().await.unwrap()
        );
        let c_res = db.contains_key("map_002").await.unwrap();
        assert!(!c_res);
        assert_eq!(map_002.is_empty().await.unwrap(), true);
        assert_eq!(map_002.len().await.unwrap(), 0);

        let list_002 = db.list("list_002").unwrap();
        let c_res = db.contains_key("list_002").await.unwrap();
        assert!(!c_res);
        assert_eq!(list_002.is_empty().await.unwrap(), true);
    }

    #[tokio::main]
    #[test]
    async fn test_db_expire() {
        let cfg = get_cfg("expire");
        let mut db = init_db(&cfg).await.unwrap();

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
        let mut ttl_001 = db.map("ttl_001").unwrap();
        ttl_001.clear().await.unwrap();
        let ttl_001_res_none = db.ttl("ttl_001").await.unwrap();
        println!(
            "1 test_db_expire map ttl_001_res_none: {:?}",
            ttl_001_res_none
        );
        assert_eq!(ttl_001_res_none, None);

        ttl_001.insert("k1", &11).await.unwrap();
        ttl_001.insert("k2", &22).await.unwrap();
        // let list_1 = ttl_001.list("list_1").unwrap();
        // list_1.push(&"a").await.unwrap();
        // list_1.push(&"b").await.unwrap();
        assert_eq!(ttl_001.is_empty().await.unwrap(), false);
        assert_eq!(ttl_001.len().await.unwrap(), 2);
        let ttl_001_res = db.ttl("ttl_001").await.unwrap();
        println!("2 test_db_expire map ttl_001_res: {:?}", ttl_001_res);
        assert_eq!(ttl_001_res.is_some(), true);

        let expire_res = db.expire("ttl_001", 1 * 1000).await.unwrap();
        println!("3 test_db_expire map expire_res: {:?}", expire_res);
        assert_eq!(expire_res, true);

        let ttl_001_res = db.ttl("ttl_001").await.unwrap().unwrap();
        println!("4 test_db_expire map ttl_001_res: {:?}", ttl_001_res);
        assert!(ttl_001_res <= 1 * 1000);

        let k1_v = ttl_001.get::<_, i32>("k1").await.unwrap();
        let k2_v = ttl_001.get::<_, i32>("k2").await.unwrap();
        println!("test_db_expire k1_v: {:?}", k1_v);
        println!("test_db_expire k2_v: {:?}", k2_v);
        assert_eq!(k1_v, Some(11));
        assert_eq!(k2_v, Some(22));
        // let list_1 = ttl_001.list("list_1").unwrap();
        // let alls = list_1.all::<String>().await.unwrap();
        // println!("alls: {:?}", alls);
        // assert_eq!(alls.len(), 2);
        // assert_eq!(alls, ["a", "b"]);

        tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
        assert_eq!(db.contains_key("ttl_001").await.unwrap(), false);
        assert_eq!(ttl_001.len().await.unwrap(), 0);
        assert_eq!(ttl_001.is_empty().await.unwrap(), true);
        assert_eq!(
            ttl_001.remove_and_fetch::<_, i32>("k1").await.unwrap(),
            None
        );
        assert!(ttl_001.iter::<i32>().await.unwrap().next().is_none());
        assert!(ttl_001.key_iter().await.unwrap().next().is_none());
        for item in ttl_001.prefix_iter::<_, i32>("k").await.unwrap() {
            let (name, val) = item.unwrap();
            println!(
                "Iter name: {:?}, val: {}",
                String::from_utf8_lossy(name.as_slice()),
                val
            );
        }
        assert!(ttl_001
            .prefix_iter::<_, i32>("k")
            .await
            .unwrap()
            .next()
            .is_none());
        let k1_v = ttl_001.get::<_, i32>("k1").await.unwrap();
        let k2_v = ttl_001.get::<_, i32>("k2").await.unwrap();
        println!("test_db_expire k1_v: {:?}", k1_v);
        println!("test_db_expire k2_v: {:?}", k2_v);
        assert_eq!(k1_v, None);
        assert_eq!(k2_v, None);
        // let alls = list_1.all::<String>().await.unwrap();
        // println!("alls: {:?}", alls);
        // assert_eq!(alls.len(), 0);

        let ttl_001_res = db.ttl("ttl_001").await.unwrap();
        println!("ttl_001_res: {:?}", ttl_001_res);
        assert!(ttl_001_res.is_none());
        ttl_001.insert("k1", &11).await.unwrap();
        let ttl_001_res = db.ttl("ttl_001").await.unwrap();
        println!("xxxx ttl_001_res: {:?}", ttl_001_res);
        assert!(ttl_001_res.is_some());
        let expire_res = db.expire("ttl_001", 1 * 1000).await.unwrap();
        println!("expire_res: {:?}", expire_res);
        assert_eq!(expire_res, expire_res);
        let ttl_001_res = db.ttl("ttl_001").await.unwrap().unwrap();
        println!("x0 ttl_001_res: {:?}", ttl_001_res);
        assert!(ttl_001_res <= 1 * 1000);
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let ttl_001_res = db.ttl("ttl_001").await.unwrap().unwrap();
        println!("x1 ttl_001_res: {:?}", ttl_001_res);
        assert!(ttl_001_res <= 500);
        ttl_001.insert("k1", &11).await.unwrap();
        let ttl_001_res = db.ttl("ttl_001").await.unwrap().unwrap();
        println!("x2 ttl_001_res: {:?}", ttl_001_res);
        assert!(ttl_001_res <= 500);
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        ttl_001.insert("k1", &11).await.unwrap();
        let ttl_001_res = db.ttl("ttl_001").await.unwrap().unwrap();
        println!(
            "x3 ttl_001_res: {:?}  {:?}",
            ttl_001_res,
            (TimestampMillis::MAX - ttl_001_res)
        );
        assert!(ttl_001_res >= 10000);
        assert_eq!(db.contains_key("ttl_001").await.unwrap(), true);

        //-----------------------------------------------------------------------------
        let l_ttl_001 = db.list("l_ttl_001").unwrap();
        l_ttl_001.clear().await.unwrap();
        let l_ttl_001_res_none = db.ttl("l_ttl_001").await.unwrap();
        println!(
            "1 test_db_expire list l_ttl_001_res_none: {:?}",
            l_ttl_001_res_none
        );
        assert_eq!(db.contains_key("l_ttl_001").await.unwrap(), false);
        assert_eq!(l_ttl_001.is_empty().await.unwrap(), true);
        assert_eq!(l_ttl_001.len().await.unwrap(), 0);
        assert_eq!(l_ttl_001_res_none, None);

        l_ttl_001.push(&11).await.unwrap();
        l_ttl_001.push(&22).await.unwrap();
        assert_eq!(db.contains_key("l_ttl_001").await.unwrap(), true);
        assert_eq!(l_ttl_001.is_empty().await.unwrap(), false);
        assert_eq!(l_ttl_001.len().await.unwrap(), 2);
        let l_ttl_001_res = db.ttl("l_ttl_001").await.unwrap();
        println!("2 test_db_expire list l_ttl_001_res: {:?}", l_ttl_001_res);
        assert_eq!(l_ttl_001_res.is_some(), true);

        let expire_res = db.expire("l_ttl_001", 1 * 1000).await.unwrap();
        println!("3 test_db_expire list expire_res: {:?}", expire_res);
        assert_eq!(expire_res, true);

        let l_ttl_001_res = db.ttl("l_ttl_001").await.unwrap().unwrap();
        println!("4 test_db_expire list l_ttl_001_res: {:?}", l_ttl_001_res);
        assert!(l_ttl_001_res <= 1 * 1000);

        let k1_v = l_ttl_001.get_index::<i32>(0).await.unwrap();
        let k2_v = l_ttl_001.get_index::<i32>(1).await.unwrap();
        println!("test_db_expire list k1_v: {:?}", k1_v);
        println!("test_db_expire list k2_v: {:?}", k2_v);
        assert_eq!(k1_v, Some(11));
        assert_eq!(k2_v, Some(22));

        tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
        assert_eq!(db.contains_key("l_ttl_001").await.unwrap(), false);
        assert_eq!(l_ttl_001.len().await.unwrap(), 0);
        assert_eq!(l_ttl_001.is_empty().await.unwrap(), true);
        assert_eq!(l_ttl_001.all::<i32>().await.unwrap().len(), 0);
        assert!(l_ttl_001.iter::<i32>().unwrap().next().is_none());
        assert_eq!(l_ttl_001.pop::<i32>().await.unwrap(), None);
        let k1_v = l_ttl_001.get_index::<i32>(0).await.unwrap();
        let k2_v = l_ttl_001.get_index::<i32>(1).await.unwrap();
        println!("test_db_expire list k1_v: {:?}", k1_v);
        println!("test_db_expire list k2_v: {:?}", k2_v);
        assert_eq!(k1_v, None);
        assert_eq!(k2_v, None);

        let l_ttl_001_res = db.ttl("l_ttl_001").await.unwrap();
        println!("test_db_expire list l_ttl_001_res: {:?}", l_ttl_001_res);
        assert!(l_ttl_001_res.is_none());
        l_ttl_001.push(&11).await.unwrap();
        let l_ttl_001_res = db.ttl("l_ttl_001").await.unwrap();
        println!(
            "xxxx test_db_expire list l_ttl_001_res: {:?}",
            l_ttl_001_res
        );
        assert!(l_ttl_001_res.is_some());
        let expire_res = db.expire("l_ttl_001", 1 * 1000).await.unwrap();
        println!("test_db_expire list expire_res: {:?}", expire_res);
        assert_eq!(expire_res, expire_res);
        let l_ttl_001_res = db.ttl("l_ttl_001").await.unwrap().unwrap();
        println!("x0 test_db_expire list l_ttl_001_res: {:?}", l_ttl_001_res);
        assert!(l_ttl_001_res <= 1 * 1000);
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let l_ttl_001_res = db.ttl("l_ttl_001").await.unwrap().unwrap();
        println!("x1 test_db_expire list l_ttl_001_res: {:?}", l_ttl_001_res);
        assert!(l_ttl_001_res <= 500);
        l_ttl_001.push(&11).await.unwrap();
        let l_ttl_001_res = db.ttl("l_ttl_001").await.unwrap().unwrap();
        println!("x2 test_db_expire list l_ttl_001_res: {:?}", l_ttl_001_res);
        assert!(l_ttl_001_res <= 500);
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        l_ttl_001.push(&11).await.unwrap();
        let l_ttl_001_res = db.ttl("l_ttl_001").await.unwrap().unwrap();
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
        let mut db = init_db(&cfg).await.unwrap();

        let mut map001 = db.map("001").unwrap();
        map001.clear().await.unwrap();
        assert_eq!(map001.len().await.unwrap(), 0);

        map001.insert("key_1", &1).await.unwrap();
        map001.insert("key_2", &2).await.unwrap();
        assert_eq!(map001.len().await.unwrap(), 2);

        let val = map001.get::<_, i32>("key_1").await.unwrap();
        println!("test_map_insert val: {:?}", val);
        assert_eq!(val, Some(1));

        map001.remove::<_>("key_1").await.unwrap();
        let val = map001.get::<_, i32>("key_1").await.unwrap();
        println!("test_map_insert val: {:?}", val);
        assert_eq!(val, None);

        println!("test_map_insert len: {:?}", map001.len().await.unwrap());
        assert_eq!(map001.len().await.unwrap(), 1);
    }

    #[tokio::main]
    #[test]
    async fn test_map_contains_key() {
        let cfg = get_cfg("map_contains_key");
        let mut db = init_db(&cfg).await.unwrap();

        let mut map001 = db.map("m001").unwrap();
        map001.clear().await.unwrap();
        assert_eq!(map001.len().await.unwrap(), 0);
        assert_eq!(map001.contains_key("k001").await.unwrap(), false);

        map001.insert("k001", &"val_001").await.unwrap();
        assert_eq!(map001.contains_key("k001").await.unwrap(), true);

        map001.remove::<_>("k001").await.unwrap();
        assert_eq!(map001.contains_key("k001").await.unwrap(), false);
        assert_eq!(map001.len().await.unwrap(), 0);

        // let list001 = map001.list("l001").unwrap();
        // list001.push(&100).await.unwrap();
        // assert_eq!(map001.len().await.unwrap(), 0);
    }

    #[tokio::main]
    #[test]
    async fn test_map() {
        let cfg = get_cfg("map");
        let mut db = init_db(&cfg).await.unwrap();

        let mut kv001 = db.map("tree_kv001").unwrap();
        let kv_key_1 = b"kv_key_1";

        kv001.clear().await.unwrap();
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
        assert_eq!(kv001.len().await.unwrap(), 1);
        assert_eq!(kv001.is_empty().await.unwrap(), false);

        assert!(kv001.contains_key(kv_key_1).await.unwrap());

        kv001.remove(kv_key_1).await.unwrap();
        assert_eq!(kv001.get::<_, String>(kv_key_1).await.unwrap(), None);
        assert!(!kv001.contains_key(kv_key_1).await.unwrap());
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
        assert_eq!(kv001.len().await.unwrap(), 4);
        kv001.remove_with_prefix("kv_key_").await.unwrap();
        assert_eq!(kv001.len().await.unwrap(), 0);
        assert_eq!(kv001.is_empty().await.unwrap(), true);
    }

    #[tokio::main]
    #[test]
    async fn test_map_retain() {
        let cfg = get_cfg("map_retain");
        let mut db = init_db(&cfg).await.unwrap();

        let mut map1 = db.map("map1").unwrap();
        map1.clear().await.unwrap();
        for i in 0..100usize {
            map1.insert(format!("mk_{}", i), &i).await.unwrap();
        }
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
        assert_eq!(map1.len().await.unwrap(), 98);
    }

    #[tokio::main]
    #[test]
    async fn test_batch() {
        let cfg = get_cfg("batch");
        let mut db = init_db(&cfg).await.unwrap();

        let mut skv = db.map("batch_kv001").unwrap();

        let mut kvs = Vec::new();
        for i in 0..100 {
            kvs.push((format!("key_{}", i).as_bytes().to_vec(), i));
        }
        skv.batch_insert(kvs.clone()).await.unwrap();
        assert_eq!(skv.len().await.unwrap(), 100);
        // let ks = kvs.into_iter().map(|(k, _v)| k).collect::<Vec<_>>();

        let mut ks = Vec::new();
        for i in 0..50 {
            ks.push(format!("key_{}", i).as_bytes().to_vec());
        }
        skv.batch_remove(ks).await.unwrap();
        assert_eq!(skv.len().await.unwrap(), 50);
    }

    #[tokio::main]
    #[test]
    async fn test_iter() {
        let cfg = get_cfg("iter");
        let mut db = init_db(&cfg).await.unwrap();

        let mut skv = db.map("iter_kv002").unwrap();
        skv.clear().await.unwrap();

        for i in 0..10 {
            skv.insert::<_, i32>(format!("key_{}", i), &i)
                .await
                .unwrap();
        }

        let iter_vals = skv.iter::<i32>().await.unwrap().map(|item| async move {
            let (key, val) = item.unwrap();
            (key, val)
        });
        let vals = futures::future::join_all(iter_vals).await;
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

        let keys = skv
            .key_iter()
            .await
            .unwrap()
            .map(|key| {
                let key = key.unwrap();
                String::from_utf8(key).unwrap()
            })
            .collect::<Vec<_>>();
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

        let iter_vals = skv
            .prefix_iter::<_, i32>("key2_")
            .await
            .unwrap()
            .map(|item| async move {
                let (key, val) = item.unwrap();
                (key, val)
            });
        let vals = futures::future::join_all(iter_vals).await;
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
        let mut db = init_db(&cfg).await.unwrap();

        // let mut skv = db.map("array_kv001").unwrap();
        let array_a = db.list("array_a").unwrap();
        let array_b = db.list("array_b").unwrap();
        let array_c = db.list("array_c").unwrap();

        array_a.clear().await.unwrap();
        array_b.clear().await.unwrap();
        array_c.clear().await.unwrap();

        db.insert("key_001", &1).await.unwrap();
        db.insert("key_002", &2).await.unwrap();
        db.insert("key_003", &3).await.unwrap();

        // for item in db.key_iter().await.unwrap() {
        //     let key = item.unwrap();
        //     println!("AAA key_iter key: {:?}", String::from_utf8(key));
        // }

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
        for val in array_c.iter::<i32>().unwrap() {
            let val = val.unwrap();
            println!("array_c iter val: {}", val);
        }

        assert_eq!(
            array_c
                .iter::<i32>()
                .unwrap()
                .collect::<Result<Vec<i32>>>()
                .unwrap(),
            vec![0, 1, 2]
        );

        // for key in db.list_key_iter().await.unwrap() {
        //     let key = key.unwrap();
        //     println!("list_key_iter key: {:?}", String::from_utf8(key));
        // }

        // let mut list_keys = db
        //     .list_key_iter()
        //     .await
        //     .unwrap()
        //     .collect::<Result<Vec<Key>>>()
        //     .unwrap();
        // list_keys.sort();

        // assert_eq!(list_keys, vec![b"array_a", b"array_b", b"array_c"]);

        // for item in db.list_iter().await.unwrap() {
        //     let (key, l) = item.unwrap();
        //     println!(
        //         "list_iter key: {:?}, vals: {:?}",
        //         String::from_utf8(key),
        //         l.all::<i32>().await.unwrap()
        //     );
        // }

        // let list_iter_vals = db.list_iter().await.unwrap().map(|item| async move {
        //     let (key, l) = item.unwrap();
        //     (
        //         String::from_utf8(key).unwrap(),
        //         l.all::<i32>().await.unwrap(),
        //     )
        // });
        // let mut vals = futures::future::join_all(list_iter_vals).await;
        // vals.sort();
        // assert_eq!(
        //     vals,
        //     vec![
        //         ("array_a".into(), vec![15, 16, 17, 18, 19]),
        //         ("array_b".into(), vec![0, 1, 2, 3]),
        //         ("array_c".into(), vec![0, 1, 2]),
        //     ]
        // );

        // for item in db.key_iter().await.unwrap() {
        //     let key = item.unwrap();
        //     println!("BBB key_iter key: {:?}", String::from_utf8(key));
        // }
        // let mut keys = db
        //     .key_iter()
        //     .await
        //     .unwrap()
        //     .collect::<Result<Vec<Key>>>()
        //     .unwrap();
        // keys.sort();
        // assert_eq!(
        //     keys,
        //     //vec![b"array_a", b"array_b", b"array_c", b"key_001", b"key_002", b"key_003"]
        //     vec![b"key_001", b"key_002", b"key_003"]
        // );

        // let iter_vals = db.iter::<i32>().await.unwrap().map(|item| async move {
        //     let (key, val) = item.unwrap();
        //     (String::from_utf8(key).unwrap(), val)
        // });
        // let mut vals = futures::future::join_all(iter_vals).await;
        // vals.sort();
        // assert_eq!(
        //     vals,
        //     vec![
        //         ("key_001".into(), 1),
        //         ("key_002".into(), 2),
        //         ("key_003".into(), 3)
        //     ]
        // );

        // let prefix_iter_vals =
        //     db.prefix_iter::<_, i32>("key_")
        //         .await
        //         .unwrap()
        //         .map(|item| async move {
        //             let (key, val) = item.unwrap();
        //             (key, val)
        //         });
        // let iter_vals = futures::future::join_all(prefix_iter_vals).await;
        //
        // assert_eq!(
        //     iter_vals,
        //     vec![
        //         (b"key_001".to_vec(), 1),
        //         (b"key_002".to_vec(), 2),
        //         (b"key_003".to_vec(), 3),
        //     ]
        // );

        // let prefix_iter_vals =
        //     skv.prefix_iter::<_, i32>("array_")
        //         .await
        //         .unwrap()
        //         .map(|item| async move {
        //             let (key, val) = item.unwrap();
        //             (key, val)
        //         });
        // let iter_vals = futures::future::join_all(prefix_iter_vals).await;
        //
        // assert_eq!(iter_vals, vec![]);
        //
        // let prefix_iter_vals =
        //     skv.prefix_iter::<_, i32>("array_b")
        //         .await
        //         .unwrap()
        //         .map(|item| async move {
        //             let (key, val) = item.unwrap();
        //             (key, val)
        //         });
        // let iter_vals = futures::future::join_all(prefix_iter_vals).await;
        //
        // assert_eq!(iter_vals, vec![]);
        //
        // skv.retain::<_, _, i32>(|item| async {
        //     let (key, val) = item.unwrap();
        //     println!(
        //         "FFF retain {:?}, is_list: {:?}",
        //         String::from_utf8_lossy(key.as_slice()),
        //         matches!(&val, &Value::List(_))
        //     );
        //     match val {
        //         Value::Val(v) => v != 2,
        //         Value::List(_l) => key != b"array_b",
        //     }
        // })
        // .await
        // .unwrap();
        //
        // let iter_vals = skv.iter::<i32>().await.unwrap().map(|item| async move {
        //     let (key, val) = item.unwrap();
        //     (key, val)
        // });
        // let vals = futures::future::join_all(iter_vals).await;
        //
        // assert_eq!(
        //     vals,
        //     vec![(b"key_001".to_vec(), 1), (b"key_003".to_vec(), 3),]
        // );
        //
        // skv.retain_with_key(|key| async {
        //     let (key, is_list) = key.unwrap();
        //     println!(
        //         "GGG retain_with_key {:?}, is_list: {:?}",
        //         String::from_utf8_lossy(key.as_slice()),
        //         is_list
        //     );
        //     if is_list && key == b"array_a" {
        //         false
        //     } else if key == b"key_003" {
        //         false
        //     } else {
        //         true
        //     }
        // })
        // .await
        // .unwrap();
        //
        // let iter_vals = skv.iter::<i32>().await.unwrap().map(|item| async move {
        //     let (key, val) = item.unwrap();
        //     println!(
        //         "III iter {:?}, val: {}",
        //         String::from_utf8_lossy(key.as_slice()),
        //         val
        //     );
        //     (key, val)
        // });
        // let vals = futures::future::join_all(iter_vals).await;
        // assert_eq!(vals, vec![(b"key_001".to_vec(), 1),]);
        //
        // let mut list_vals = Vec::new();
        // for item in skv.list_iter().await.unwrap() {
        //     let (key, l) = item.unwrap();
        //     let mut vals = l.all::<i32>().await.unwrap();
        //     vals.sort();
        //     list_vals.push((key, vals))
        // }
        // list_vals.sort();
        // assert_eq!(list_vals, vec![(b"array_c".to_vec(), vec![0, 1, 2]),]);
        //
        // skv.insert(b"array_c", &100).await.unwrap();
        // for item in skv.iter::<i32>().await.unwrap() {
        //     let (key, val) = item.unwrap();
        //     println!("HHH iter key: {:?}, val: {:?}", String::from_utf8(key), val);
        // }
    }

    #[tokio::main]
    #[test]
    async fn test_list2() {
        let cfg = get_cfg("map_list");
        let mut db = init_db(&cfg).await.unwrap();

        // let mut ml001 = db.map("m_l_001").unwrap();
        // ml001.clear().await.unwrap();
        // assert_eq!(ml001.len().await.unwrap(), 0);
        // assert_eq!(ml001.is_empty().await.unwrap(), true);

        let l001 = db.list("l_001").unwrap();
        l001.clear().await.unwrap();
        assert_eq!(l001.len().await.unwrap(), 0);
        assert_eq!(l001.is_empty().await.unwrap(), true);
        l001.push(&100).await.unwrap();
        l001.push(&101).await.unwrap();
        assert_eq!(l001.len().await.unwrap(), 2);
        assert_eq!(l001.is_empty().await.unwrap(), false);
        // assert_eq!(db.len().await.unwrap(), 0);
        // assert_eq!(db.is_empty().await.unwrap(), true);

        // db.insert("k_001", &"v001").await.unwrap();
        // db.insert("k_002", &"v002").await.unwrap();
        // assert_eq!(db.len().await.unwrap(), 2);
        // assert_eq!(db.is_empty().await.unwrap(), false);

        // db.clear().await.unwrap();
        // assert_eq!(db.len().await.unwrap(), 0);
        // assert_eq!(db.is_empty().await.unwrap(), true);
        // assert_eq!(l001.len().await.unwrap(), 0);
        // assert_eq!(l001.is_empty().await.unwrap(), true);

        for v in 100..200 {
            l001.push_limit(&v, 5, true).await.unwrap();
        }
        assert_eq!(l001.len().await.unwrap(), 5);
        assert_eq!(l001.is_empty().await.unwrap(), false);
        // assert_eq!(ml001.len().await.unwrap(), 0);
        // assert_eq!(ml001.is_empty().await.unwrap(), true);

        assert_eq!(
            l001.iter::<i32>()
                .unwrap()
                .collect::<Result<Vec<_>>>()
                .unwrap(),
            [195, 196, 197, 198, 199]
        );

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

        let l002 = db.list("l_002").unwrap();
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
}
