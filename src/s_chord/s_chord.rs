use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use dashmap::DashMap;
use parking_lot::RwLock;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::s_chord::peer_messages::{ChordPeer, PeerMessage};

macro_rules! connect_to_peer {
    ($address:expr) => {{
        let stream = TcpStream::connect($address).await?;
        let (reader, writer) = stream.into_split();
        channels::channel(reader, writer)
    }};
}

pub struct SChord {
    pub state: Arc<SChordState>,
}

pub struct SChordState {
    default_store_duration: Duration,
    max_store_duration: Duration,

    node_id: u64,
    address: SocketAddr,
    finger_table: Vec<RwLock<ChordPeer>>,
    successors: RwLock<Vec<ChordPeer>>,
    predecessors: RwLock<Vec<ChordPeer>>,

    pub local_storage: DashMap<u64, Vec<u8>>,
}

impl SChord {
    pub fn get_address(&self) -> SocketAddr {
        self.state.address
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
                let (stream, _socket_address) = listener.accept().await.unwrap();
                let self_clone = SChord {
                    state: self_clone.state.clone(),
                };
                tokio::spawn(async move {
                    match self_clone.accept_peer_connection(stream).await {
                        Ok(_) => {
                            // Everything fine, no need to do anything
                        }
                        Err(e) => {
                            // todo maybe send error message to peer
                            eprintln!("Error in peer {}: {:?}", self_clone.state.address, e);
                        }
                    }
                });
            }
        });
        // Await thread spawn, to avoid EOF errors because the thread is not ready to accept messages
        rx.recv().await.unwrap();
        handle
    }

    pub async fn split_node(&self) -> Result<()> {
        let successor = self.state.finger_table[0].read();
        let (mut tx, _) = connect_to_peer!(successor.address);

        tx.send(PeerMessage::SplitNode(ChordPeer {
            id: self.state.node_id,
            address: self.state.address,
        }))
        .await?;
        // todo: get relevant data from node
        Ok(())
    }

    pub async fn new(initial_peer: Option<SocketAddr>, server_address: SocketAddr) -> Self {
        let mut hasher = DefaultHasher::new();
        server_address.hash(&mut hasher);
        let node_id = hasher.finish();

        if let Some(initial_peer) = initial_peer {
            let initial_peer_connection_result = async || -> Result<SChord> {
                // Connect to initial node
                let mut stream = TcpStream::connect(initial_peer).await?;
                let (reader, writer) = stream.split();
                let (mut tx, mut rx) = channels::channel(reader, writer);

                // Acquire node responsible for the location of our id
                // this node is automatically our successor
                tx.send(PeerMessage::GetNode(node_id)).await?;
                match rx.recv().await? {
                    PeerMessage::GetNodeResponse(successor) => {
                        // Connect to successor
                        let (mut tx, mut rx) = connect_to_peer!(successor.address);

                        // Ask successor about predecessor
                        let mut finger_table = Vec::new();
                        let mut predecessors = Vec::new();
                        tx.send(PeerMessage::GetPredecessor).await?;
                        match rx.recv().await? {
                            PeerMessage::GetPredecessorResponse(predecessor) => {
                                // Add predecessor to list
                                predecessors.push(predecessor);

                                // Initialize finger table
                                // todo: use predecessor for this -- why?
                                for i in 1..64 {
                                    // todo: is this index correct?
                                    tx.send(PeerMessage::GetNode(
                                        node_id.wrapping_add(2u64.pow(i as u32)),
                                    ))
                                    .await?;
                                    match rx.recv().await? {
                                        PeerMessage::GetNodeResponse(finger_peer) => {
                                            finger_table.push(finger_peer);
                                        }
                                        _ => {
                                            return Err(anyhow!(
                                                "Unexpected response to get_node from initial peer"
                                            ));
                                        }
                                    }
                                }
                            }
                            _ => {
                                return Err(anyhow!(
                                    "Unexpected response to get_predecessor from initial peer"
                                ));
                            }
                        }
                        Ok(SChord {
                            state: Arc::new(SChordState {
                                default_store_duration: Duration::from_secs(60),
                                max_store_duration: Duration::from_secs(600),
                                local_storage: DashMap::new(),
                                finger_table: (1..64)
                                    .map(|_| {
                                        RwLock::new(ChordPeer {
                                            id: node_id,
                                            address: server_address,
                                        })
                                    })
                                    .collect(),
                                successors: RwLock::new(Vec::new()),
                                node_id,
                                predecessors: RwLock::new(predecessors),
                                address: server_address,
                            }),
                        })
                    }
                    _ => Err(anyhow!("Unexpected response to get_node from initial peer")),
                }
            };
            match initial_peer_connection_result().await {
                Ok(s_chord) => s_chord,
                Err(e) => {
                    panic!("Failed communication with bootstrap peer: {}", e);
                }
            }
        } else {
            SChord {
                state: Arc::new(SChordState {
                    default_store_duration: Duration::from_secs(60),
                    max_store_duration: Duration::from_secs(600),
                    local_storage: DashMap::new(),
                    finger_table: Vec::new(),
                    successors: RwLock::new(Vec::new()),
                    node_id,
                    predecessors: RwLock::new(Vec::new()),
                    address: server_address,
                }),
            }
        }
    }

    pub async fn insert(&self, key: u64, value: Vec<u8>) -> Result<()> {
        self.insert_with_ttl(key, value, self.state.default_store_duration)
            .await
    }
    pub async fn insert_with_ttl(&self, key: u64, value: Vec<u8>, ttl: Duration) -> Result<()> {
        if self.is_responsible_for_key(key) {
            self.state.local_storage.insert(key, value);
            Ok(())
        } else {
            let peer = self.get_responsible_node(key).await?;

            let mut stream = TcpStream::connect(peer.address).await?;
            let (reader, writer) = stream.split();
            let (mut tx, _) = channels::channel(reader, writer);

            tx.send(PeerMessage::InsertValue(key, value)).await?;
            // todo maybe wait for success answer?
            Ok(())
        }
    }

    pub async fn get(&self, key: u64) -> Result<Vec<u8>> {
        if self.is_responsible_for_key(key) {
            if self
                .state
                .local_storage
                .get(&key)
                .map(|entry| entry.value().clone())
                .is_none()
            {
                eprintln!("Not found locally {}", self.state.address);
            }

            self.state
                .local_storage
                .get(&key)
                .map(|entry| entry.value().clone())
                .ok_or(anyhow!("Value not found locally"))
        } else {
            let peer = self.get_responsible_node(key).await?;
            let (mut tx, mut rx) = connect_to_peer!(peer.address);

            tx.send(PeerMessage::GetValue(key)).await?;
            match rx.recv().await? {
                PeerMessage::GetValueResponse(option) => {
                    option.ok_or(anyhow!("Peer does not know value"))
                }
                _ => Err(anyhow!("Wrong response")),
            }
        }
    }

    fn is_responsible_for_key(&self, key: u64) -> bool {
        if let Some(predecessor) = self.state.predecessors.read().first() {
            let self_id = self.state.node_id;

            let range_length = self_id.wrapping_sub(predecessor.id);
            let wrapped_distance = key.wrapping_sub(predecessor.id);
            wrapped_distance < range_length && wrapped_distance > 0
        } else {
            true
        }
    }

    async fn get_responsible_node(&self, key: u64) -> Result<ChordPeer> {
        let diff = key.wrapping_sub(self.state.node_id);
        let entry = diff.leading_zeros();
        let finger_table_index = usize::try_from(entry)?;

        // Connect to successor
        let address = self.state.finger_table[finger_table_index].read().address;
        let (mut tx, mut rx) = connect_to_peer!(address);

        // Ask successor about predecessor
        tx.send(PeerMessage::GetNode(key)).await?;
        match rx.recv().await? {
            PeerMessage::GetNodeResponse(peer) => Ok(peer),
            _ => Err(anyhow!("Wrong response")),
        }
    }

    fn get_predecessor(&self) -> ChordPeer {
        *self
            .state
            .predecessors
            .read()
            .first()
            .unwrap_or(&ChordPeer {
                id: self.state.node_id,
                address: self.state.address,
            })
    }

    pub async fn inform_predecessor_existence(&self) -> Result<()> {
        // Connect to successor
        let (mut tx, _) = connect_to_peer!(
            self.state
                .predecessors
                .read()
                .first()
                .ok_or(anyhow!("No predecessor"))?
                .address
        );

        // Ask successor about predecessor // todo: do these comments use successor / predecessor correctly?
        tx.send(PeerMessage::SetSuccessor(ChordPeer {
            id: self.state.node_id,
            address: self.state.address,
        }))
        .await?;
        // todo maybe response?
        Ok(())
    }

    /// Handle incoming requests from peers
    async fn accept_peer_connection(&self, mut stream: TcpStream) -> Result<()> {
        let (reader, writer) = stream.split();
        let (mut tx, mut rx) = channels::channel(reader, writer);
        loop {
            match rx.recv().await? {
                PeerMessage::GetNode(id) => {
                    // if we do not have a predecessor we are responsible for all keys
                    // otherwise check if the key is between us and our predecessor in which case we are also responsible
                    if self.is_responsible_for_key(id) {
                        tx.send(PeerMessage::GetNodeResponse(ChordPeer {
                            id: self.state.node_id,
                            address: self.state.address,
                        }))
                        .await?;
                    } else {
                        let response_node = self.get_responsible_node(id).await?;
                        tx.send(PeerMessage::GetNodeResponse(response_node)).await?
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
                PeerMessage::GetPredecessor => {
                    let predecessor = self.get_predecessor();
                    tx.send(PeerMessage::GetPredecessorResponse(predecessor))
                        .await?;
                }
                PeerMessage::SplitNode(predecessor) => {
                    println!(
                        "{}: Split pred: {}",
                        self.state.address, predecessor.address
                    );

                    // todo transfer keys

                    // todo update predecessor and finger table
                }
                PeerMessage::InsertValue(key, value) => {
                    if !self.is_responsible_for_key(key) {
                        // todo maybe make this more failsafe?
                        return Err(anyhow!(
                            "Peer tried to insert key which we are not responsible for"
                        ));
                    }

                    self.state.local_storage.insert(key, value);
                }
                PeerMessage::SetSuccessor(successor) => {
                    println!(
                        "{}: Set Successor: {}",
                        self.state.address, successor.address
                    );

                    // todo update finger table
                }

                _ => {
                    return Err(anyhow!("Unexpected message type"));
                }
            }
        }
    }

    pub fn print_short(&self) {
        println!(" P:{}", self.state.predecessors.read()[0].address);
        println!(" S:{}", self.state.finger_table[0].read().address);
    }

    pub fn print(&self) {
        println!("Id {:x}: {}", self.state.node_id, self.state.address);
        for predecessor in self.state.predecessors.read().iter() {
            println!(" P {:x}: {}", predecessor.id, predecessor.address);
        }
        for entry in self
            .state
            .finger_table
            .iter()
            .take(10)
            .map(|entry| entry.read())
        {
            println!(" F {:x}: {}", entry.id, entry.address);
        }
    }
}
