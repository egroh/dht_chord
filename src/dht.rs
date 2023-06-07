use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use serde::Serialize;

pub struct SChord<K: Serialize, V: Serialize> {
    hash_map: HashMap<K, V>,
    default_store_duration: Duration,
    max_store_duration: Duration,
}

impl<K: Serialize, V: Serialize> SChord<K, V> {
    pub fn new(initial_peers: &[SocketAddr]) -> Self {
        SChord {
            hash_map: HashMap::new(),
            default_store_duration: Duration::from_secs(60),
            max_store_duration: Duration::from_secs(600),
        }
    }

    pub async fn insert(&self, key: K, value: V) {
        self.insert_with_ttl(key, value, self.default_store_duration)
            .await;
    }
    pub async fn insert_with_ttl(&self, key: K, value: V, ttl: Duration) {}

    pub async fn get(&self, key: &K) -> Option<&V> {
        None
    }
}
