//! # rmqtt-storage example: MQTT Session Storage Simulation
//!
//! This example demonstrates the usage of all three core data structures in
//! `rmqtt-storage`, using a simplified MQTT Broker session management scenario:
//!
//! - **DB (KV)**: client session info + counters
//! - **Map**: topic subscription management
//! - **List**: offline message queue
//!
//! ## Usage
//!
//! Pass the storage backend as the first CLI argument; defaults to `sled`.
//!
//! ```bash
//! # sled backend (default)
//! cargo run --example session_storage_demo --features "sled,ttl,map_len,len"
//!
//! # redb backend
//! cargo run --example session_storage_demo --features "redb,ttl,map_len,len" -- redb
//!
//! # explicit backend
//! cargo run --example session_storage_demo --features "sled,ttl,map_len,len" -- sled
//! ```
//!
//! All progress messages are prefixed with `>>>` (step) or `<<<` (result).

#[cfg(feature = "ttl")]
use std::time::Duration;

#[cfg(feature = "redb")]
use rmqtt_storage::RedbConfig;
#[cfg(feature = "sled")]
use rmqtt_storage::SledConfig;
use rmqtt_storage::{Config, DefaultStorageDB, List, Map, StorageList, StorageMap, StorageType};

// ---------------------------------------------------------------------------
// Helper macros
// ---------------------------------------------------------------------------
macro_rules! info {
    ($fmt:literal $(, $arg:expr)* $(,)?) => {
        println!(concat!(">>> ", $fmt) $(, $arg)*)
    };
}
macro_rules! ok {
    ($fmt:literal $(, $arg:expr)* $(,)?) => {
        println!(concat!("<<< OK - ", $fmt) $(, $arg)*)
    };
}
macro_rules! err {
    ($fmt:literal $(, $arg:expr)* $(,)?) => {
        eprintln!(concat!("<<< ERROR - ", $fmt) $(, $arg)*)
    };
}

// ---------------------------------------------------------------------------
// Part 1: Database initialization
// ---------------------------------------------------------------------------
async fn init_database() -> DefaultStorageDB {
    // Read backend type from first CLI argument; default to "sled"
    let backend = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "sled".into())
        .to_ascii_lowercase();

    info!("===== 1. Initialize database ({} backend) =====", backend);
    info!("Creating temp directory");

    let temp_dir = std::env::temp_dir().join(format!("rmqtt_storage_demo_{}", std::process::id()));
    // Clean up stale data if any
    // let _ = std::fs::remove_dir_all(&temp_dir);

    let db = match backend.as_str() {
        #[cfg(feature = "sled")]
        "sled" => {
            let cfg = Config {
                typ: StorageType::Sled,
                sled: SledConfig {
                    path: temp_dir.to_string_lossy().to_string(),
                    ..Default::default()
                },
            };
            rmqtt_storage::init_db(&cfg)
                .await
                .expect("sled init failed")
        }
        #[cfg(feature = "redb")]
        "redb" => {
            let cfg = Config {
                typ: StorageType::Redb,
                redb: RedbConfig {
                    path: format!("{}.redb", temp_dir.to_string_lossy()),
                    ..Default::default()
                },
            };
            rmqtt_storage::init_db(&cfg)
                .await
                .expect("redb init failed")
        }
        _ => {
            let available: Vec<&str> = vec![
                #[cfg(feature = "sled")]
                "sled",
                #[cfg(feature = "redb")]
                "redb",
            ];
            panic!(
                "unsupported backend: '{}', available: {} (enable the corresponding feature in Cargo.toml)",
                backend,
                available.join(", ")
            );
        }
    };

    ok!(
        "database initialized ({}) at {}",
        backend,
        temp_dir.display()
    );
    db
}

// ---------------------------------------------------------------------------
// Part 2: Client session management (KV + Counter)
// ---------------------------------------------------------------------------
async fn session_management(db: &DefaultStorageDB) {
    info!("\n===== 2. Client session management (KV + Counter) =====");

    // --- 2a. KV insert / get / contains_key ---
    info!("--- 2a. Client session CRUD ---");
    let client_id = "client_001";

    #[derive(serde::Serialize, serde::Deserialize, Debug)]
    struct SessionInfo {
        username: String,
        connected_at: u64,
        protocol: String,
        keepalive: u16,
    }

    let session = SessionInfo {
        username: "alice".into(),
        connected_at: 1_720_000_000_000,
        protocol: "MQTT 5.0".into(),
        keepalive: 60,
    };

    let key = format!("session:{}", client_id);
    db.insert(&key, &session)
        .await
        .expect("insert session failed");
    ok!("inserted session: {}", client_id);

    let exists = db.contains_key(&key).await.expect("contains_key failed");
    assert!(exists);
    ok!("session key exists: {:?}", exists);

    let loaded: Option<SessionInfo> = db.get(&key).await.expect("get session failed");
    if let Some(s) = loaded {
        ok!("read session: {:?}", s);
    }

    // --- 2b. Counter ---
    info!("--- 2b. Connection counters ---");
    let counter_key = "stat:connections";

    for _ in 0..5 {
        db.counter_incr(counter_key, 1)
            .await
            .expect("counter_incr failed");
    }
    let count: Option<isize> = db
        .counter_get(counter_key)
        .await
        .expect("counter_get failed");
    assert_eq!(count, Some(5));
    ok!("counter value: {:?}", count);

    db.counter_decr(counter_key, 2)
        .await
        .expect("counter_decr failed");
    let count: Option<isize> = db
        .counter_get(counter_key)
        .await
        .expect("counter_get failed");
    assert_eq!(count, Some(3));
    ok!("counter after decrement: {:?}", count);

    db.counter_set(counter_key, 100)
        .await
        .expect("counter_set failed");
    let count: Option<isize> = db
        .counter_get(counter_key)
        .await
        .expect("counter_get failed");
    assert_eq!(count, Some(100));
    ok!("counter after reset: {:?}", count);

    // --- 2c. Batch insert ---
    info!("--- 2c. Batch insert sessions ---");
    let mut batch = Vec::new();
    for i in 1..=5 {
        let k = format!("session:client_{:03}", i);
        let v = serde_json::to_vec(&SessionInfo {
            username: format!("user_{}", i),
            connected_at: 1_720_000_000 + i as u64,
            protocol: "MQTT 5.0".into(),
            keepalive: 60,
        })
        .expect("serialize failed");
        batch.push((k.into_bytes(), v));
    }
    db.batch_insert(batch).await.expect("batch_insert failed");
    ok!("batch inserted 5 client sessions");

    // --- 2d. Remove ---
    info!("--- 2d. Remove sessions ---");
    db.remove("session:client_003")
        .await
        .expect("remove failed");
    let exists = db
        .contains_key("session:client_003")
        .await
        .expect("contains_key failed");
    assert!(!exists);
    ok!("removed session:client_003");

    let keys_to_remove: Vec<_> = (4..=5)
        .map(|i| format!("session:client_{:03}", i).into_bytes())
        .collect();
    db.batch_remove(keys_to_remove)
        .await
        .expect("batch_remove failed");
    ok!("batch removed session:client_004, client_005");
}

// ---------------------------------------------------------------------------
// Part 3: Topic subscription management (Map)
// ---------------------------------------------------------------------------
async fn subscription_management(db: &DefaultStorageDB) {
    info!("\n===== 3. Topic subscription management (Map) =====");

    // --- 3a. Create Map & insert subscriptions ---
    info!("--- 3a. Create subscription Map and insert topics ---");
    let mut sub_map: StorageMap = db.map("subscriptions:client_001", None).await;

    let topics = vec![
        ("sensor/temperature", 1),
        ("sensor/humidity", 1),
        ("home/livingroom/light", 2),
        ("home/livingroom/temp", 1),
        ("home/kitchen/light", 0),
        ("system/alerts", 2),
    ];

    for (topic, qos) in &topics {
        sub_map
            .insert(topic, qos)
            .await
            .expect("insert subscription failed");
    }
    ok!("inserted {} topic subscriptions", topics.len());

    // --- 3b. Query single subscription ---
    info!("--- 3b. Query single subscription ---");
    let qos: Option<i32> = sub_map.get("sensor/temperature").await.expect("get failed");
    assert_eq!(qos, Some(1));
    ok!("sensor/temperature QoS: {:?}", qos);

    let exists = sub_map
        .contains_key("home/kitchen/light")
        .await
        .expect("contains_key failed");
    assert!(exists);
    ok!("home/kitchen/light exists: {:?}", exists);

    // --- 3c. Iterate all subscriptions ---
    info!("--- 3c. Iterate all subscriptions ---");
    {
        let mut iter = sub_map.iter::<i32>().await.expect("iter failed");
        let mut count = 0;
        while let Some(item) = iter.next().await {
            match item {
                Ok((key, qos)) => {
                    let topic = String::from_utf8_lossy(&key);
                    println!("     subscription: {} -> QoS {}", topic, qos);
                    count += 1;
                }
                Err(e) => err!("iter error: {}", e),
            }
        }
        ok!("{} topic subscriptions total", count);
    }

    // --- 3d. Prefix iteration ---
    info!("--- 3d. Prefix iteration (home/) ---");
    {
        let mut prefix_iter = sub_map
            .prefix_iter::<_, i32>("home/")
            .await
            .expect("prefix_iter failed");
        while let Some(item) = prefix_iter.next().await {
            if let Ok((key, qos)) = item {
                let topic = String::from_utf8_lossy(&key);
                println!("     home/ prefix: {} -> QoS {}", topic, qos);
            }
        }
    }

    // --- 3e. Key-only iteration ---
    info!("--- 3e. Key-only iteration ---");
    {
        let mut key_iter = sub_map.key_iter().await.expect("key_iter failed");
        while let Some(item) = key_iter.next().await {
            if let Ok(key) = item {
                println!("     key: {}", String::from_utf8_lossy(&key));
            }
        }
    }

    // --- 3f. remove_and_fetch ---
    info!("--- 3f. remove_and_fetch ---");
    let removed: Option<i32> = sub_map
        .remove_and_fetch("system/alerts")
        .await
        .expect("remove_and_fetch failed");
    assert_eq!(removed, Some(2));
    ok!("remove_and_fetch system/alerts: {:?}", removed);

    // --- 3g. remove_with_prefix ---
    info!("--- 3g. remove_with_prefix (home/) ---");
    sub_map
        .remove_with_prefix("home/")
        .await
        .expect("remove_with_prefix failed");
    let exists = sub_map
        .contains_key("home/livingroom/light")
        .await
        .expect("contains_key failed");
    assert!(!exists);
    ok!("all subscriptions under home/ removed");

    // --- 3h. batch_insert ---
    info!("--- 3h. batch_insert subscriptions ---");
    let mut batch = Vec::new();
    for i in 0..100 {
        let k = format!("device/{}/status", i);
        batch.push((k.into_bytes(), i));
    }
    sub_map
        .batch_insert(batch)
        .await
        .expect("batch_insert failed");
    #[cfg(feature = "map_len")]
    {
        let map_len = sub_map.len().await.expect("map len failed");
        ok!("Map length after batch_insert: {}", map_len);
    }

    // batch_remove
    let batch_keys: Vec<_> = (0..50)
        .map(|i| format!("device/{}/status", i).into_bytes())
        .collect();
    sub_map
        .batch_remove(batch_keys)
        .await
        .expect("batch_remove failed");
    ok!("batch removed 50 subscriptions");

    // --- 3i. is_empty & clear ---
    info!("--- 3i. Clear Map ---");
    let empty = sub_map.is_empty().await.expect("is_empty failed");
    ok!("Map empty before clear: {}", empty);
    sub_map.clear().await.expect("clear failed");
    let empty = sub_map.is_empty().await.expect("is_empty failed");
    assert!(empty);
    ok!("Map cleared");
}

// ---------------------------------------------------------------------------
// Part 4: Offline message queue (List)
// ---------------------------------------------------------------------------
async fn offline_message_queue(db: &DefaultStorageDB) {
    info!("\n===== 4. Offline message queue (List) =====");

    let mut msg_list: StorageList = db.list("offline:client_001", None).await;

    // --- 4a. Push messages ---
    info!("--- 4a. Push offline messages ---");

    #[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
    struct OfflineMsg {
        topic: String,
        payload: String,
        qos: u8,
        timestamp: u64,
    }

    for i in 0..10 {
        let msg = OfflineMsg {
            topic: format!("sensor/temperature"),
            payload: format!("value={}", 20.0 + i as f64 * 0.5),
            qos: 1,
            timestamp: 1_720_000_000 + i,
        };
        msg_list.push(&msg).await.expect("push failed");
    }
    ok!("pushed 10 offline messages");

    let len = msg_list.len().await.expect("list len failed");
    ok!("current queue length: {}", len);

    // --- 4b. Batch push (pushs) ---
    info!("--- 4b. Batch push (pushs) ---");
    let msgs: Vec<OfflineMsg> = (0..5)
        .map(|i| OfflineMsg {
            topic: "system/alert".into(),
            payload: format!("alert_code={}", 100 + i),
            qos: 2,
            timestamp: 1_720_000_100 + i,
        })
        .collect();
    msg_list.pushs(msgs).await.expect("pushs failed");
    ok!("batch pushed 5 alert messages");

    // --- 4c. Pop messages ---
    info!("--- 4c. Pop (consume) messages ---");
    for i in 0..3 {
        let popped: Option<OfflineMsg> = msg_list.pop().await.expect("pop failed");
        match popped {
            Some(msg) => println!("     pop #{}: {:?}", i, msg),
            None => println!("     pop #{}: queue empty", i),
        }
    }

    let remaining = msg_list.len().await.expect("len failed");
    ok!("remaining after popping 3: {}", remaining);

    // --- 4d. Read all ---
    info!("--- 4d. Read all remaining messages ---");
    let all: Vec<OfflineMsg> = msg_list.all().await.expect("all failed");
    ok!("{} remaining messages:", all.len());
    for msg in &all {
        println!("     {:?}", msg);
    }

    // --- 4e. get_index ---
    info!("--- 4e. get_index ---");
    let first: Option<OfflineMsg> = msg_list.get_index(0).await.expect("get_index failed");
    if let Some(msg) = first {
        ok!("message at index 0: {:?}", msg);
    }

    // --- 4f. push_limit ---
    info!("--- 4f. push_limit ---");
    let overflow_msg = OfflineMsg {
        topic: "system/overflow".into(),
        payload: "drop_test".into(),
        qos: 0,
        timestamp: 1_720_000_999,
    };
    // Limit queue to 15 items; pop front when exceeded
    let popped: Option<OfflineMsg> = msg_list
        .push_limit(&overflow_msg, 15, true)
        .await
        .expect("push_limit failed");
    let list_len = msg_list.len().await.expect("len failed");
    ok!(
        "queue length after push_limit: {}, popped message: {:?}",
        list_len,
        popped
    );

    // --- 4g. Iterate ---
    info!("--- 4g. Iterate all messages ---");
    {
        let mut iter = msg_list.iter::<OfflineMsg>().await.expect("iter failed");
        let mut count = 0;
        while let Some(item) = iter.next().await {
            match item {
                Ok(msg) => {
                    println!("     iter #{}: topic={}", count, msg.topic);
                    count += 1;
                }
                Err(e) => err!("iter error: {}", e),
            }
        }
        ok!("iteration done, {} items", count);
    }

    // --- 4h. clear ---
    info!("--- 4h. Clear queue ---");
    msg_list.clear().await.expect("clear failed");
    let empty = msg_list.is_empty().await.expect("is_empty failed");
    assert!(empty);
    ok!("queue cleared");
}

// ---------------------------------------------------------------------------
// Part 5: TTL & expiration (feature-gated)
// ---------------------------------------------------------------------------
#[cfg(feature = "ttl")]
async fn ttl_demo(db: &DefaultStorageDB) {
    info!("\n===== 5. TTL & expiration demo =====");

    // --- 5a. DB-level TTL ---
    info!("--- 5a. DB-level TTL ---");
    db.insert("temp_key", &"will_expire".to_string())
        .await
        .expect("insert failed");

    let ttl_before = db.ttl("temp_key").await.expect("ttl failed");
    info!("TTL before setting: {:?}", ttl_before);

    // Set 2-second expiration
    db.expire("temp_key", 2000).await.expect("expire failed");
    let ttl_after = db.ttl("temp_key").await.expect("ttl failed");
    ok!("TTL after expire: {:?} (~2000ms)", ttl_after);

    // --- 5b. Map-level TTL ---
    info!("--- 5b. Map-level TTL ---");
    let m: StorageMap = db.map("temp_map", None).await;
    m.insert("k1", &"v1").await.expect("insert failed");
    m.expire(1000).await.expect("map expire failed");
    let map_ttl = m.ttl().await.expect("map ttl failed");
    ok!("Map TTL: {:?} (~1000ms)", map_ttl);

    // --- 5c. List-level TTL ---
    info!("--- 5c. List-level TTL ---");
    let l: StorageList = db.list("temp_list", None).await;
    l.push(&"item").await.expect("push failed");
    l.expire(1000).await.expect("list expire failed");
    let list_ttl = l.ttl().await.expect("list ttl failed");
    ok!("List TTL: {:?} (~1000ms)", list_ttl);

    // Wait for expiration and verify
    info!("Sleeping 2.5s to wait for expiration...");
    tokio::time::sleep(Duration::from_millis(2500)).await;

    let exists = db
        .contains_key("temp_key")
        .await
        .expect("contains_key failed");
    ok!("temp_key expired: {:?}", !exists);

    // expire_at
    info!("--- expire_at usage ---");
    let future_ts = (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
        + 5000) as i64;
    let m2: StorageMap = db.map("map_expire_at", None).await;
    m2.insert("k2", &"v2").await.expect("insert failed");
    m2.expire_at(future_ts).await.expect("expire_at failed");
    let m2_ttl = m2.ttl().await.expect("ttl failed");
    ok!("expire_at TTL: {:?} (~5000ms)", m2_ttl);
}

// ---------------------------------------------------------------------------
// Part 6: Scan & statistics
// ---------------------------------------------------------------------------
async fn scan_and_stats(db: &mut DefaultStorageDB) {
    info!("\n===== 6. Scan & statistics =====");

    // --- 6a. scan ---
    info!("--- 6a. Scan keys by pattern (session:*) ---");
    let scan_keys = {
        let mut scan_iter = db.scan("session:*").await.expect("scan failed");
        let mut keys = Vec::new();
        while let Some(item) = scan_iter.next().await {
            match item {
                Ok(k) => keys.push(k),
                Err(e) => err!("scan error: {}", e),
            }
        }
        keys
    };
    ok!("scan session:* matched {} keys", scan_keys.len());
    for key in &scan_keys {
        println!("     {}", String::from_utf8_lossy(key));
    }

    // --- 6b. db_size ---
    info!("--- 6b. Database size ---");
    let size = db.db_size().await.expect("db_size failed");
    ok!("db_size: {} bytes", size);

    // --- 6c. info ---
    info!("--- 6c. Storage info ---");
    let info_val = db.info().await.expect("info failed");
    ok!("info: {}", serde_json::to_string_pretty(&info_val).unwrap());

    // --- 6d. map_iter ---
    info!("--- 6d. Iterate all Maps ---");
    {
        let mut map_iter = db.map_iter().await.expect("map_iter failed");
        let mut count = 0;
        while let Some(item) = map_iter.next().await {
            if let Ok(map) = item {
                println!("     Map: {}", String::from_utf8_lossy(map.name()));
                count += 1;
            }
        }
        ok!("{} Maps total", count);
    }

    info!("--- 6e. Iterate all Lists ---");
    {
        let mut list_iter = db.list_iter().await.expect("list_iter failed");
        let mut count = 0;
        while let Some(item) = list_iter.next().await {
            if let Ok(list) = item {
                println!("     List: {:?}", list);
                count += 1;
            }
        }
        ok!("{} Lists total", count);
    }

    #[cfg(feature = "len")]
    {
        let total_items = db.len().await.expect("len failed");
        ok!("total items: {}", total_items);
    }
}

// ---------------------------------------------------------------------------
// Part 7: Cleanup
// ---------------------------------------------------------------------------
async fn cleanup_map_and_list(db: &DefaultStorageDB) {
    info!("\n===== 7. Cleanup Map and List =====");

    info!("--- 7a. Remove Map ---");
    db.map_remove("subscriptions:client_001")
        .await
        .expect("map_remove failed");
    let exists = db
        .map_contains_key("subscriptions:client_001")
        .await
        .expect("map_contains_key failed");
    assert!(!exists);
    ok!("Map removed");

    info!("--- 7b. Remove List ---");
    db.list_remove("offline:client_001")
        .await
        .expect("list_remove failed");
    let exists = db
        .list_contains_key("offline:client_001")
        .await
        .expect("list_contains_key failed");
    assert!(!exists);
    ok!("List removed");
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
#[tokio::main]
async fn main() {
    println!(
        "╔══════════════════════════════════════════════╗\n\
         ║   rmqtt-storage Example                      ║\n\
         ║   MQTT Session Storage Simulation            ║\n\
         ╚══════════════════════════════════════════════╝"
    );

    // Part 1: Initialize
    let db = init_database().await;
    let mut db_clone = db.clone();

    // Part 2: Session management
    session_management(&db).await;

    // Part 3: Subscription management
    subscription_management(&db).await;

    // Part 4: Offline message queue
    offline_message_queue(&db).await;

    // Part 5: TTL demo (feature-gated)
    #[cfg(feature = "ttl")]
    ttl_demo(&db).await;

    #[cfg(not(feature = "ttl"))]
    info!("\n===== 5. TTL not enabled (add --features ttl) =====");

    // Part 6: Scan & statistics
    scan_and_stats(&mut db_clone).await;

    // Part 7: Cleanup
    cleanup_map_and_list(&db).await;

    // Part 8: Performance benchmark — insert 10,000 items
    info!("\n===== 8. Benchmark: insert 10,000 items =====");
    {
        let now = std::time::Instant::now();
        for i in 0..10_000usize {
            let key = format!("bench:{}", i);
            db.insert(&key, &i).await.expect("bench insert failed");
        }
        let elapsed = now.elapsed();
        info!(
            "sequential insert 10,000 items: {:?} ({:.0} ops/s)",
            elapsed,
            10_000.0 / elapsed.as_secs_f64()
        );

        // Verify
        let v: Option<usize> = db.get("bench:9999").await.expect("bench verify failed");
        assert_eq!(v, Some(9999));
        ok!("verified bench:9999 = {}", v.unwrap());

        // Compare with batch_insert
        let now = std::time::Instant::now();
        let mut batch: Vec<(Vec<u8>, usize)> = Vec::with_capacity(10_000);
        for i in 10_000..20_000usize {
            let key = format!("bench:{}", i);
            batch.push((key.into_bytes(), i));
        }
        db.batch_insert(batch)
            .await
            .expect("batch bench insert failed");
        let elapsed = now.elapsed();
        info!(
            "batch insert 10,000 items: {:?} ({:.0} ops/s)",
            elapsed,
            10_000.0 / elapsed.as_secs_f64()
        );

        let v: Option<usize> = db
            .get("bench:19999")
            .await
            .expect("batch bench verify failed");
        assert_eq!(v, Some(19999));
        ok!("verified bench:19999 = {}", v.unwrap());
    }

    println!(
        "\n╔══════════════════════════════════════════════╗\n\
         ║   All examples completed ✓                  ║\n\
         ╚══════════════════════════════════════════════╝"
    );

    // info!("Press Enter to exit...");
    // let mut _input = String::new();
    // let _ = std::io::stdin().read_line(&mut _input);
}
