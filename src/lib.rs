#![forbid(unsafe_code)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    unused_crate_dependencies,
    clippy::missing_const_for_fn,
    unused_extern_crates
)]

use moka::notification::RemovalCause;
pub use storage_hal_derive::StorageData;

use std::time::Duration;
use std::{fmt::Debug, sync::Arc};

use bytes::Bytes;
use moka::sync::SegmentedCache;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use sled::Db;

pub trait StorageData: Debug + Clone + Default + for<'a> Deserialize<'a> + Serialize {
    fn name() -> String;
}

const SEQUENCE_TREE_NAME: &str = "SEQUENCE";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    pub db_path: String,
    pub cache_num_segments: usize,
    pub cache_max_capacity: Option<u64>,
    pub cache_time_to_live: Option<u64>,
    pub cache_time_to_idle: Option<u64>,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            db_path: "default.db".to_string(),
            cache_num_segments: 1,
            cache_max_capacity: None,
            cache_time_to_live: None,
            cache_time_to_idle: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Storage {
    pub cache: SegmentedCache<String, Bytes>,
    pub db: Db,
}

impl Default for Storage {
    fn default() -> Self {
        // By default, This cache will hold up to 1GiB of values.
        Self::new(&StorageConfig::default())
    }
}

impl Storage {
    pub fn new(config: &StorageConfig) -> Self {
        let db = sled::open(&config.db_path).unwrap();
        let db_clone = Arc::new(Mutex::new(db.clone()));

        let mut builder = SegmentedCache::builder(config.cache_num_segments)
            .weigher(|k: &String, v: &Bytes| (k.len() + v.len()) as u32)
            .eviction_listener(move |key, value, cause| {
                tracing::debug!(
                    "Evicted ({:?},{:?}) because {:?} by cache",
                    key,
                    value,
                    cause
                );
                match cause {
                    RemovalCause::Explicit | RemovalCause::Expired => {
                        if let Some(real_key) = key.strip_prefix(":/") {
                            let (tree, key) = real_key.split_once('/').unwrap();
                            let tree = db_clone.lock().open_tree(tree).unwrap();
                            tree.remove(key).unwrap();
                        } else {
                            db_clone.lock().remove(key.as_str()).unwrap();
                        }
                        tracing::debug!(
                            "Evicted ({:?},{:?}) because {:?} by db",
                            key,
                            value,
                            cause
                        );
                    }
                    _ => {}
                }
            });
        if let Some(v) = config.cache_max_capacity {
            builder = builder.max_capacity(v)
        }
        if let Some(v) = config.cache_time_to_live {
            builder = builder.time_to_live(Duration::from_secs(v))
        }
        if let Some(v) = config.cache_time_to_idle {
            builder = builder.time_to_idle(Duration::from_secs(v))
        }

        Self {
            cache: builder.build(),
            db,
        }
    }
}

// SEQUENCE
impl Storage {
    pub fn next(&self, name: &str) -> u32 {
        let tree = self.db.open_tree(SEQUENCE_TREE_NAME).unwrap();
        match tree.get(name).ok().and_then(|v| {
            v.and_then(|v| match v.to_vec().try_into() {
                Ok(v) => Some(u32::from_be_bytes(v)),
                Err(_) => None,
            })
        }) {
            Some(next) => {
                if tree.insert(name, (next + 1).to_be_bytes().to_vec()).is_ok() {
                    next + 1
                } else {
                    0
                }
            }
            None => {
                if tree.insert(name, 1u32.to_be_bytes().to_vec()).is_ok() {
                    1
                } else {
                    0
                }
            }
        }
    }

    pub fn current(&self, name: &str) -> u32 {
        let tree = self.db.open_tree(SEQUENCE_TREE_NAME).unwrap();
        if let Ok(Some(v)) = tree.get(name) {
            if let Ok(v) = v.to_vec().try_into() {
                return u32::from_be_bytes(v);
            }
        }
        0
    }
}

// structured data key used in cache
fn ckey<T: for<'a> Deserialize<'a> + StorageData>(key: &str) -> String {
    format!(":/{}/{}", T::name(), key)
}

// structured data
impl Storage {
    pub fn contains_key<T: StorageData>(&self, key: &str) -> bool {
        if self.cache.contains_key(&ckey::<T>(key)) {
            return true;
        }

        let tree = self.db.open_tree(T::name()).unwrap();
        if let Ok(r) = tree.contains_key(key) {
            return r;
        }

        false
    }

    pub fn get<T: for<'a> Deserialize<'a> + StorageData>(&self, key: &str) -> Option<T> {
        if let Some(v) = self.cache.get(&ckey::<T>(key)) {
            return bincode::deserialize(&v).ok();
        }

        let tree = self.db.open_tree(T::name()).unwrap();
        if let Ok(Some(v)) = tree.get(key) {
            let value = bincode::deserialize(&v).ok();
            self.cache.insert(ckey::<T>(key), Bytes::from(v.to_vec()));
            return value;
        }

        None
    }

    pub fn insert<T: Serialize + StorageData>(&self, key: &str, value: T) -> Option<T> {
        let tree = self.db.open_tree(T::name()).unwrap();
        if let Ok(value_bytes) = bincode::serialize(&value) {
            if tree.insert(key, value_bytes.clone()).is_ok() {
                self.cache.insert(ckey::<T>(key), Bytes::from(value_bytes));
                return Some(value);
            }
        }
        None
    }

    pub fn remove<T: StorageData>(&self, key: &str) {
        self.cache.remove(&ckey::<T>(key));
    }
}

#[test]
fn sequence() {
    let store: Storage = Storage::default();
    println!("{}", store.current("test"));
    for _ in 0..100 {
        println!("{}", store.next("test"));
    }
}

#[test]
fn eviction() {
    let store: Storage = Storage::new(&StorageConfig {
        cache_time_to_live: Some(1),
        cache_max_capacity: Some(1024 * 1024 * 1024),
        cache_num_segments: 10,
        ..Default::default()
    });
    let store_clone = store.clone();
    async_std::task::spawn(async move {
        for i in 0..10000u32 {
            store_clone
                .cache
                .insert(i.to_string(), Bytes::from(i.to_string()));
            store_clone.db.insert(i.to_string(), i.to_string()).unwrap();
        }

        println!("{:?}", store_clone.cache.get(&9999u32.to_string()));
        println!("{:?}", store_clone.db.get(9999u32.to_string()));
        async_std::task::sleep(Duration::from_millis(1100)).await;
        println!("{:?}", store.cache.get(&9999u32.to_string()));
        println!("{:?}", store.db.get(9999u32.to_string()));
        async_std::task::sleep(Duration::from_millis(300)).await;
        println!("{:?}", store.cache.get(&9999u32.to_string()));
        println!("{:?}", store.db.get(9999u32.to_string()));
        async_std::task::sleep(Duration::from_millis(10)).await;
        println!("{:?}", store.cache.get(&9999u32.to_string()));
        println!("{:?}", store.db.get(9999u32.to_string()));
    });
    async_std::task::block_on(async_std::task::sleep(Duration::from_secs(10)));
}

#[test]
fn structured_data() {
    #[derive(StorageData, Debug, Clone, Default, Deserialize, Serialize)]
    struct Test {
        a: u32,
        b: String,
    }

    let store: Storage = Storage::default();
    let test = Test {
        a: 1,
        b: "test".to_string(),
    };
    store.insert("test", test.clone());
    println!("{:?}", store.get::<Test>("test"));
    store.insert("test", test);
    println!("{:?}", store.get::<Test>("test"));
    store.remove::<Test>("test");
    println!("{:?}", store.get::<Test>("test"));
}
