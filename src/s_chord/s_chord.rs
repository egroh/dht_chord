use dashmap::DashMap;
use parking_lot::RwLock;
use std::collections::hash_map::DefaultHasher;
use std::error::Error;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

use crate::s_chord::peer_messages::PeerMessage;

pub struct SChord {
    state: Arc<SChordState>,
}

struct SChordState {
    default_store_duration: Duration,
    max_store_duration: Duration,

    node_id: u64,
    address: SocketAddr,
    finger_table: Vec<RwLock<ChordPeer>>,
    successors: Vec<ChordPeer>,
    predecessors: Vec<ChordPeer>,

    local_storage: DashMap<u64, Vec<u8>>,
}

struct ChordPeer {
    id: u64,
    address: SocketAddr,
}

impl SChord {
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
        let finger_table: Vec<RwLock<ChordPeer>> = (0..63)
            .map(|_| {
                RwLock::new(ChordPeer {
                    id: node_id,
                    address: server_address,
                })
            })
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
                        *entry.write() = ChordPeer {
                            id,
                            address: SocketAddr::new(ip, port),
                        };
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
                successors: vec![],
                node_id,
                predecessors: vec![],
                address: server_address,
            }),
        }
    }

    pub async fn insert(&self, key: u64, value: Vec<u8>) {
        self.insert_with_ttl(key, value, self.state.default_store_duration)
            .await;
    }
    pub async fn insert_with_ttl(&self, key: u64, value: Vec<u8>, ttl: Duration) {}

    pub async fn get(&self, key: u64) -> Option<Vec<u8>> {
        self.state
            .local_storage
            .get(&key)
            .map(|entry| entry.value().clone())
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
                    if id <= self.state.node_id && id > self.state.predecessors[0].id {
                        tx.send(PeerMessage::GetNodeResponse(
                            self.state.node_id,
                            self.state.address.ip(),
                            self.state.address.port(),
                        ))
                        .await?;
                    } else {
                        let diff = id.wrapping_sub(self.state.node_id);
                        let entry = diff.leading_zeros();
                        let finger_table_index = usize::try_from(entry).unwrap();

                        let response_node_id =
                            self.state.finger_table[finger_table_index].read().id; // todo: we should probably only lock once
                        let response_node_address = self.state.finger_table[finger_table_index]
                            .read()
                            .address
                            .ip();
                        let response_node_port = self.state.finger_table[finger_table_index]
                            .read()
                            .address
                            .port();
                        tx.send(PeerMessage::GetNodeResponse(
                            response_node_id,
                            response_node_address,
                            response_node_port,
                        ))
                        .await?;
                    }
                }
                PeerMessage::GetValue(key) => {
                    tx.send(PeerMessage::GetValueResponse(
                        self.state
                            .local_storage
                            .get(&key)
                            .map(|entry| entry.value().clone()),
                    ))
                    .await?;
                }
                _ => {
                    panic!("Unexpected message type");
                }
            }
        }
    }
}
