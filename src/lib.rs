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
            let sled_cfg = cfg.sled.to_sled_config()?;
            let db = SledStorageDB::new(sled_cfg)?;
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
            //std::time::Duration::new(now.timestamp() as u64, now.timestamp_subsec_nanos())
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{Key, Value};

    fn get_cfg(name: &str) -> Config {
        let cfg = Config {
            storage_type: StorageType::Redis,
            sled: SledConfig {
                path: format!("./.catch/{}", name),
                ..Default::default()
            },
            redis: "redis://127.0.0.1:6379/".into(),
        };
        cfg
    }

    #[tokio::main]
    #[test]
    async fn test_db() {
        let cfg = get_cfg("db");

        let db = init_db(&cfg).await.unwrap();
        let db_key_1 = b"db_key_1";
        let db_val_1 = String::from("db_val_001");
        db.insert::<_, String>(db_key_1, &db_val_1).await.unwrap();
        assert_eq!(
            db.get::<_, String>(db_key_1).await.unwrap(),
            Some(db_val_1.clone())
        );
        assert_eq!(db.get::<_, String>(b"db_key_2").await.unwrap(), None);

        db.remove(db_key_1).await.unwrap();
        assert_eq!(db.get::<_, String>(db_key_1).await.unwrap(), None);
    }

    #[tokio::main]
    #[test]
    async fn test_tree() {
        let cfg = get_cfg("tree");
        let mut db = init_db(&cfg).await.unwrap();

        let mut kv001 = db.map("tree_kv001").unwrap();
        let kv_key_1 = b"kv_key_1";

        kv001.clear().await.unwrap();

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
    async fn test_batch() {
        let cfg = get_cfg("batch");
        let mut db = init_db(&cfg).await.unwrap();

        let skv = db.map("batch_kv001").unwrap();

        let mut kvs = Vec::new();
        for i in 0..100 {
            kvs.push((format!("key_{}", i).as_bytes().to_vec(), i));
        }
        skv.batch_insert(kvs.clone()).await.unwrap();
        assert_eq!(skv.len().await.unwrap(), 100);
        let ks = kvs.into_iter().map(|(k, _v)| k).collect::<Vec<_>>();
        skv.batch_remove(ks).await.unwrap();
        assert_eq!(skv.len().await.unwrap(), 0);
    }

    #[tokio::main]
    #[test]
    async fn test_array() {
        let cfg = get_cfg("array");
        let mut db = init_db(&cfg).await.unwrap();

        let mut skv = db.map("array_kv001").unwrap();
        let array_a = skv.list("array_a").unwrap();
        let array_b = skv.list("array_b").unwrap();
        let array_c = skv.list("array_c").unwrap();

        skv.clear().await.unwrap();

        skv.insert("key_001", &1).await.unwrap();
        skv.insert("key_002", &2).await.unwrap();
        skv.insert("key_003", &3).await.unwrap();

        for item in skv.key_iter().await.unwrap() {
            let key = item.unwrap();
            println!("AAA key_iter key: {:?}", String::from_utf8(key));
        }

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

        for key in skv.list_key_iter().await.unwrap() {
            let key = key.unwrap();
            println!("list_key_iter key: {:?}", String::from_utf8(key));
        }

        let mut list_keys = skv
            .list_key_iter()
            .await
            .unwrap()
            .collect::<Result<Vec<Key>>>()
            .unwrap();
        list_keys.sort();
        assert_eq!(list_keys, vec![b"array_a", b"array_b", b"array_c"]);

        for item in skv.list_iter().await.unwrap() {
            let (key, l) = item.unwrap();
            println!(
                "list_iter key: {:?}, vals: {:?}",
                String::from_utf8(key),
                l.all::<i32>().await.unwrap()
            );
        }

        let list_iter_vals = skv.list_iter().await.unwrap().map(|item| async move {
            let (key, l) = item.unwrap();
            (
                String::from_utf8(key).unwrap(),
                l.all::<i32>().await.unwrap(),
            )
        });
        let mut vals = futures::future::join_all(list_iter_vals).await;
        vals.sort();
        assert_eq!(
            vals,
            vec![
                ("array_a".into(), vec![15, 16, 17, 18, 19]),
                ("array_b".into(), vec![0, 1, 2, 3]),
                ("array_c".into(), vec![0, 1, 2]),
            ]
        );

        for item in skv.key_iter().await.unwrap() {
            let key = item.unwrap();
            println!("BBB key_iter key: {:?}", String::from_utf8(key));
        }
        let mut keys = skv
            .key_iter()
            .await
            .unwrap()
            .collect::<Result<Vec<Key>>>()
            .unwrap();
        keys.sort();
        assert_eq!(
            keys,
            //vec![b"array_a", b"array_b", b"array_c", b"key_001", b"key_002", b"key_003"]
            vec![b"key_001", b"key_002", b"key_003"]
        );

        let iter_vals = skv.iter::<i32>().await.unwrap().map(|item| async move {
            let (key, val) = item.unwrap();
            (String::from_utf8(key).unwrap(), val)
        });
        let mut vals = futures::future::join_all(iter_vals).await;
        vals.sort();
        assert_eq!(
            vals,
            vec![
                ("key_001".into(), 1),
                ("key_002".into(), 2),
                ("key_003".into(), 3)
            ]
        );

        let prefix_iter_vals =
            skv.prefix_iter::<_, i32>("key_")
                .await
                .unwrap()
                .map(|item| async move {
                    let (key, val) = item.unwrap();
                    (key, val)
                });
        let iter_vals = futures::future::join_all(prefix_iter_vals).await;

        assert_eq!(
            iter_vals,
            vec![
                (b"key_001".to_vec(), 1),
                (b"key_002".to_vec(), 2),
                (b"key_003".to_vec(), 3),
            ]
        );

        let prefix_iter_vals =
            skv.prefix_iter::<_, i32>("array_")
                .await
                .unwrap()
                .map(|item| async move {
                    let (key, val) = item.unwrap();
                    (key, val)
                });
        let iter_vals = futures::future::join_all(prefix_iter_vals).await;

        assert_eq!(iter_vals, vec![]);

        let prefix_iter_vals =
            skv.prefix_iter::<_, i32>("array_b")
                .await
                .unwrap()
                .map(|item| async move {
                    let (key, val) = item.unwrap();
                    (key, val)
                });
        let iter_vals = futures::future::join_all(prefix_iter_vals).await;

        assert_eq!(iter_vals, vec![]);

        skv.retain::<_, _, i32>(|item| async {
            let (key, val) = item.unwrap();
            match val {
                Value::Val(v) => v != 2,
                Value::List(_l) => key != b"array_b",
            }
        })
        .await
        .unwrap();

        let iter_vals = skv.iter::<i32>().await.unwrap().map(|item| async move {
            let (key, val) = item.unwrap();
            (key, val)
        });
        let vals = futures::future::join_all(iter_vals).await;

        assert_eq!(
            vals,
            vec![(b"key_001".to_vec(), 1), (b"key_003".to_vec(), 3),]
        );

        skv.retain_with_key(|key| async {
            let (key, is_list) = key.unwrap();
            if is_list && key == b"array_a" {
                false
            } else if key == b"key_003" {
                false
            } else {
                true
            }
        })
        .await
        .unwrap();

        let iter_vals = skv.iter::<i32>().await.unwrap().map(|item| async move {
            let (key, val) = item.unwrap();
            (key, val)
        });
        let vals = futures::future::join_all(iter_vals).await;
        assert_eq!(vals, vec![(b"key_001".to_vec(), 1),]);

        let mut list_vals = Vec::new();
        for item in skv.list_iter().await.unwrap() {
            let (key, l) = item.unwrap();
            let mut vals = l.all::<i32>().await.unwrap();
            vals.sort();
            list_vals.push((key, vals))
        }
        list_vals.sort();
        assert_eq!(list_vals, vec![(b"array_c".to_vec(), vec![0, 1, 2]),]);

        skv.insert(b"array_c", &100).await.unwrap();
        for item in skv.iter::<i32>().await.unwrap() {
            let (key, val) = item.unwrap();
            println!("HHH iter key: {:?}, val: {:?}", String::from_utf8(key), val);
        }
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
    async fn test_expire() {
        let cfg = get_cfg("expire");
        let mut db = init_db(&cfg).await.unwrap();

        let res_none = db.ttl("test_k001").await.unwrap();
        println!("ttl res: {:?}", res_none);
        assert_eq!(res_none, None);

        let mut ttl_001 = db.map("ttl_001").unwrap();
        ttl_001.clear().await.unwrap();
        let ttl_001_res_none = db.ttl("ttl_001").await.unwrap();
        println!("ttl_001_res_none: {:?}", ttl_001_res_none);
        assert_eq!(ttl_001_res_none, None);

        ttl_001.insert("k1", &1).await.unwrap();
        ttl_001.insert("k2", &2).await.unwrap();
        let list_1 = ttl_001.list("list_1").unwrap();
        list_1.push(&"a").await.unwrap();
        list_1.push(&"b").await.unwrap();

        let ttl_001_res = db.ttl("ttl_001").await.unwrap();
        println!("ttl_001_res: {:?}", ttl_001_res);
        assert!(ttl_001_res.is_some());

        let expire_res = db.expire("ttl_001", 60 * 1000).await.unwrap();
        println!("expire_res: {:?}", expire_res);
        assert_eq!(expire_res, expire_res);

        let ttl_001_res = db.ttl("ttl_001").await.unwrap().unwrap();
        println!("ttl_001_res: {:?}", ttl_001_res);
        assert!(ttl_001_res <= 60 * 1000);
    }

    #[tokio::main]
    #[test]
    async fn test_db_contains_key() {
        let cfg = get_cfg("contains_key");
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
        let c_res = db.contains_key("map_001").await.unwrap();
        assert!(c_res);
        map_001.clear().await.unwrap();
        //println!("map_001.len: {:?}", map_001.len().await.unwrap());
        let c_res = db.contains_key("map_001").await.unwrap();
        assert!(!c_res);
        let l1 = map_001.list("l1").unwrap();
        l1.push(&"aa").await.unwrap();
        l1.push(&"bb").await.unwrap();

        let c_res = db.contains_key("map_001").await.unwrap();
        assert!(c_res);

        let _ = db.map("map_002").unwrap();
        let c_res = db.contains_key("map_002").await.unwrap();
        assert!(!c_res);
    }

    #[tokio::main]
    #[test]
    async fn test_map() {
        let cfg = get_cfg("map");
        let mut db = init_db(&cfg).await.unwrap();
        let now = std::time::Instant::now();
        for i in 0..100_000 {
            let sess = db.map(format!("session_id_{}", i)).unwrap();
            sess.insert("k001", &i).await.unwrap();
        }
        println!("test_map cost time: {:?}", now.elapsed());
    }
}
