use std::collections::hash_map::DefaultHasher;
use std::error::Error;
use std::hash::{Hash, Hasher};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use num_traits::Bounded;
use parking_lot::RwLock;
use serde::Serialize;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

use crate::s_chord::peer_messages::PeerMessage;

pub struct SChord<K: SChordKey, V: SChordValue> {
    state: Arc<SChordState<K, V>>,
}

struct SChordState<K: SChordKey, V: SChordValue> {
    default_store_duration: Duration,
    max_store_duration: Duration,

    node_id: u64,
    finger_table: Vec<RwLock<(u64, SocketAddr)>>,

    local_storage: DashMap<K, V>,
}

impl<K: SChordKey, V: SChordValue> SChord<K, V> {
    pub fn start_server_socket(&self, server_address: SocketAddr) -> JoinHandle<()> {
        let self_clone = SChord {
            state: self.state.clone(),
        };
        tokio::spawn(async move {
            let listener = TcpListener::bind(server_address)
                .await
                .expect("Failed to bind SChord server socket");
            println!("SChord listening for peers on {}", server_address);
            loop {
                let (stream, connecting_address) = listener.accept().await.unwrap();
                let self_clone = SChord {
                    state: self_clone.state.clone(),
                };
                tokio::spawn(async move {
                    self_clone
                        .accept_peer_connection(stream, connecting_address)
                        .await
                        .unwrap();
                });
            }
        })
    }

    pub async fn new(initial_peer: Option<SocketAddr>, server_address: SocketAddr) -> Self {
        let mut hasher = DefaultHasher::new();
        server_address.hash(&mut hasher);
        let node_id = hasher.finish();
        let finger_table: Vec<RwLock<(u64, SocketAddr)>> = (0..63)
            .map(|_| RwLock::new((node_id, server_address)))
            .collect();
        if let Some(initial_peer) = initial_peer {
            let mut stream = TcpStream::connect(initial_peer).await.unwrap();
            let (reader, writer) = stream.split();
            let (mut tx, mut rx) = channels::channel(reader, writer);
            for (i, entry) in finger_table.iter().enumerate() {
                tx.send(PeerMessage::GetNode(node_id + 2u64.pow(i as u32)))
                    .await
                    .unwrap();
                match rx.recv().await.unwrap() {
                    PeerMessage::GetNodeResponse(id, ip, port) => {
                        *entry.write() = (id, SocketAddr::new(ip, port));
                    }
                    _ => {
                        panic!("Unexpected response to get_node from initial peer");
                    }
                }
            }
        }
        SChord {
            state: Arc::new(SChordState {
                default_store_duration: Duration::from_secs(60),
                max_store_duration: Duration::from_secs(600),
                local_storage: DashMap::new(),
                finger_table,
                node_id,
            }),
        }
    }

    pub async fn insert(&self, key: K, value: V) {
        self.insert_with_ttl(key, value, self.state.default_store_duration)
            .await;
    }
    pub async fn insert_with_ttl(&self, key: K, value: V, ttl: Duration) {}

    pub async fn get(&self, key: &K) -> Option<&V> {
        None
    }

    /// Handle incoming requests from peers
    async fn accept_peer_connection(
        &self,
        mut stream: TcpStream,
        connecting_address: SocketAddr,
    ) -> Result<(), Box<dyn Error>> {
        let (reader, writer) = stream.split();
        let (mut tx, mut rx) = channels::channel(reader, writer);
        loop {
            let request: PeerMessage = rx.recv().await?;
            match request {
                PeerMessage::GetNode(id) => {
                    let response_node_key = self.state.finger_table[0].read().0;
                    let response_node_address = self.state.finger_table[0].read().1.ip();
                    let response_node_port = self.state.finger_table[0].read().1.port();
                    tx.send(PeerMessage::GetNodeResponse(
                        response_node_key,
                        response_node_address,
                        response_node_port,
                    ))
                    .await?;
                    todo!("GetNode");
                }
                _ => {
                    panic!("Unexpected message type");
                }
            }
        }
    }
}

pub trait SChordKey: Serialize + Eq + Hash + Bounded + Send + Sync + 'static {}
pub trait SChordValue: Serialize + Send + Sync + 'static {}

impl SChordKey for u64 {}
impl SChordValue for Vec<u8> {}
