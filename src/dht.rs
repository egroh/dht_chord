use std::hash::Hash;
use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use num_traits::Bounded;
use serde::Serialize;

use crate::peer_communication;
use crate::peer_communication::join;

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
            let listener =
                TcpListener::bind(&server_address).expect("Failed to bind SChord server socket");
            println!("SChord listening for peers on {}", server_address);
            loop {
                let (stream, _address) = listener.accept().unwrap();
                let state = state.clone();
                tokio::spawn(async move {
                    peer_communication::accept_peer_connection(stream)
                        .await
                        .unwrap();
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
        if let Some(initial_peer) = initial_peer {
            tokio::spawn(async move {
                join(initial_peer)
                    .await
                    .expect("Failed to join SChord network");
            });
        }
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

impl SChordKey for u64 {}
impl SChordValue for Vec<u8> {}
