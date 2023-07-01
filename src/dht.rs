use std::hash::Hash;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use crate::peer_communication;
use dashmap::DashMap;
use num_traits::Bounded;
use serde::Serialize;
use tokio::net::TcpListener;

pub struct SChord<K: SChordKey, V: SChordValue> {
    state: Arc<SChordState<K, V>>,
}

struct SChordState<K: SChordKey, V: SChordValue> {
    default_store_duration: Duration,
    max_store_duration: Duration,
    local_storage: DashMap<K, V>,
}

impl<K: SChordKey, V: SChordValue> SChord<K, V> {
    fn start_server_socket(state: Arc<SChordState<K, V>>, server_address: SocketAddr) {
        tokio::spawn(async move {
            let listener = TcpListener::bind(&server_address)
                .await
                .expect("Failed to bind SChord server socket");
            println!("SChord listening for peers on {}", server_address);
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let state = state.clone();
                tokio::spawn(async move {
                    peer_communication::handle_incoming_stream(stream).await?;
                });
            }
        });
    }

    pub fn new(initial_peer: Option<SocketAddr>, server_address: SocketAddr) -> Self {
        let state = Arc::new(SChordState {
            default_store_duration: Duration::from_secs(60),
            max_store_duration: Duration::from_secs(600),
            local_storage: DashMap::new(),
        });
        SChord::start_server_socket(state.clone(), server_address);
        SChord { state }
    }

    pub async fn insert(&self, key: K, value: V) {
        self.insert_with_ttl(key, value, self.state.default_store_duration)
            .await;
    }
    pub async fn insert_with_ttl(&self, key: K, value: V, ttl: Duration) {}

    pub async fn get(&self, key: &K) -> Option<&V> {
        None
    }
}

pub trait SChordKey: Serialize + Eq + Hash + Bounded + Send + Sync + 'static {}
pub trait SChordValue: Serialize + Send + Sync + 'static {}
