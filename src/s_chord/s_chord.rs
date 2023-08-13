use dashmap::DashMap;
use parking_lot::RwLock;
use std::collections::hash_map::DefaultHasher;
use std::error::Error;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::s_chord::peer_messages::{ChordPeer, PeerMessage};

pub struct SChord {
    state: Arc<SChordState>,
}

struct SChordState {
    default_store_duration: Duration,
    max_store_duration: Duration,

    node_id: u64,
    my_address: SocketAddr,
    finger_table: Vec<RwLock<ChordPeer>>,
    successors: Vec<RwLock<ChordPeer>>,
    predecessors: Vec<RwLock<ChordPeer>>,

    local_storage: DashMap<u64, Vec<u8>>,
}

impl SChord {
    pub fn get_address(&self) -> SocketAddr {
        self.state.my_address
    }

    pub async fn start_server_socket(&self, server_address: SocketAddr) -> JoinHandle<()> {
        let self_clone = SChord {
            state: self.state.clone(),
        };
        let listener = TcpListener::bind(server_address)
            .await
            .expect("Failed to bind SChord server socket");
        // Open channel for inter thread communication
        let (tx, mut rx) = mpsc::channel(1);

        let handle = tokio::spawn(async move {
            // Send signal that we are running
            tx.send(true).await.expect("Unable to send message");
            println!("SChord listening for peers on {}", server_address);
            loop {
                let (stream, connecting_address) = listener.accept().await.unwrap();
                let self_clone = SChord {
                    state: self_clone.state.clone(),
                };
                tokio::spawn(async move {
                    // todo: maybe dont ignore error
                    let _ = self_clone
                        .accept_peer_connection(stream, connecting_address)
                        .await;
                });
            }
        });
        // Await thread spawn, to avoid EOF errors because the thread is not ready to accept messages
        rx.recv().await.unwrap();

        handle
    }

    pub async fn new(initial_peer: Option<SocketAddr>, server_address: SocketAddr) -> Self {
        let mut hasher = DefaultHasher::new();
        server_address.hash(&mut hasher);
        let node_id = hasher.finish();

        let mut finger_table = vec![];
        let mut predecessors = vec![];

        if let Some(initial_peer) = initial_peer {
            // Connect to initial node
            let mut stream = TcpStream::connect(initial_peer).await.unwrap();
            let (reader, writer) = stream.split();
            let (mut tx, mut rx) = channels::channel(reader, writer);

            // Acquire node responsible for the location of our id
            // this node is automatically our successor
            tx.send(PeerMessage::GetNode(node_id)).await.unwrap();
            match rx.recv().await.unwrap() {
                PeerMessage::GetNodeResponse(successor) => {
                    // Connect to successor
                    let mut stream = TcpStream::connect(successor.address).await.unwrap();
                    let (reader, writer) = stream.split();
                    let (mut tx, mut rx) = channels::channel(reader, writer);

                    // Ask successor about predecessor
                    tx.send(PeerMessage::GetPredecessor()).await.unwrap();
                    match rx.recv().await.unwrap() {
                        PeerMessage::GetPredecessorResponse(predecessor) => {
                            // Add predecessor to list
                            predecessors.push(RwLock::new(predecessor));

                            // Inform successor to split the node, as we now control part of his key space
                            tx.send(PeerMessage::SplitNode(node_id)).await.unwrap();

                            // Initialize finger table
                            // todo: use predecessor for this
                            for i in 0..63 {
                                tx.send(PeerMessage::GetNode(
                                    node_id.wrapping_add(2u64.pow(i as u32)),
                                ))
                                .await
                                .unwrap();
                                match rx.recv().await.unwrap() {
                                    PeerMessage::GetNodeResponse(finger_peer) => {
                                        finger_table.push(RwLock::new(finger_peer));
                                    }
                                    _ => {
                                        panic!("Unexpected response to get_node from initial peer");
                                    }
                                }
                            }
                        }
                        _ => {
                            panic!("Unexpected response to get_node from initial peer");
                        }
                    }
                }
                _ => {
                    panic!("Unexpected response to get_node from initial peer");
                }
            }
        } else {
            // If no initial peer was provided, we assume we are the first node and do not initialize any state until a node joins
        }
        SChord {
            state: Arc::new(SChordState {
                default_store_duration: Duration::from_secs(60),
                max_store_duration: Duration::from_secs(600),
                local_storage: DashMap::new(),
                finger_table,
                successors: vec![],
                node_id,
                predecessors,
                my_address: server_address,
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
                    // if we do not have a predecessor we are responsible for all keys
                    // otherwise check if the key is between us and our predecessor in which case we are also responsible
                    if self.state.predecessors.is_empty()
                        || (id <= self.state.node_id && id > self.state.predecessors[0].read().id)
                    {
                        tx.send(PeerMessage::GetNodeResponse(ChordPeer {
                            id: self.state.node_id,
                            address: self.state.my_address,
                        }))
                        .await?;
                    } else {
                        let diff = id.wrapping_sub(self.state.node_id);
                        let entry = diff.leading_zeros();
                        let finger_table_index = usize::try_from(entry).unwrap();

                        let response_node = *self.state.finger_table[finger_table_index].read();
                        tx.send(PeerMessage::GetNodeResponse(response_node)).await?;
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
                PeerMessage::GetPredecessor() => {
                    // Initialize answer to self
                    let mut predecessor = ChordPeer {
                        id: self.state.node_id,
                        address: self.state.my_address,
                    };

                    if !self.state.predecessors.is_empty() {
                        // Replace with our predecessor if we have any
                        predecessor = *self.state.predecessors[0].read();
                    }
                    tx.send(PeerMessage::GetPredecessorResponse(predecessor))
                        .await?;
                }
                PeerMessage::SplitNode(..) => {
                    // todo: actually do something
                }
                _ => {
                    panic!("Unexpected message type");
                }
            }
        }
    }
}
