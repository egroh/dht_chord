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

use crate::s_chord::peer_messages::{ChordPeer, PeerMessage, SplitResponse};

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

    pub node_id: u64,
    address: SocketAddr,
    pub finger_table: Vec<RwLock<ChordPeer>>,
    predecessors: RwLock<Vec<ChordPeer>>,

    pub local_storage: DashMap<u64, Vec<u8>>,
}

impl SChord {
    pub fn get_address(&self) -> SocketAddr {
        self.state.address
    }

    pub async fn start_server_socket(&self, server_address: SocketAddr) -> JoinHandle<()> {
        println!("Starting SChord server on {}", server_address);
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
                let (stream, socket_address) = listener.accept().await.unwrap();
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
                            eprintln!("Error in connection with peer {}: {:?}", socket_address, e);
                        }
                    }
                });
            }
        });
        // Await thread spawn, to avoid EOF errors because the thread is not ready to accept messages
        rx.recv().await.unwrap();
        handle
    }

    pub async fn new(initial_peer: Option<SocketAddr>, server_address: SocketAddr) -> Self {
        println!("Creating new SChord node on: {}", server_address);
        let mut hasher = DefaultHasher::new();
        server_address.hash(&mut hasher);
        let node_id = hasher.finish();

        if let Some(initial_peer) = initial_peer {
            let initial_peer_connection_result = async || -> Result<SChord> {
                // Connect to initial node
                let (mut tx, mut rx) = connect_to_peer!(initial_peer);

                // Acquire node responsible for the location of our id
                // this node is automatically our successor
                // this works because no node is aware of our existence yet
                tx.send(PeerMessage::GetNode(node_id)).await?;
                match rx.recv().await? {
                    PeerMessage::GetNodeResponse(mut successor) => {
                        // Close connection to initial peer
                        tx.send(PeerMessage::CloseConnection).await?;

                        let mut predecessors = Vec::new();
                        let mut local_storage = DashMap::new();

                        // Connect to successor
                        let (mut tx, mut rx) = connect_to_peer!(successor.address);
                        loop {
                            // Ask predecessor
                            tx.send(PeerMessage::GetPredecessor).await?;
                            match rx.recv().await? {
                                PeerMessage::GetPredecessorResponse(predecessor) => {
                                    // initialize our predecessor
                                    predecessors.push(predecessor);
                                }
                                _ => {
                                    return Err(anyhow!(
                                        "Unexpected response to get_predecessor from initial peer"
                                    ));
                                }
                            }

                            // Ask successor to split
                            tx.send(PeerMessage::SplitRequest(ChordPeer {
                                id: node_id,
                                address: server_address,
                            }))
                            .await?;
                            match rx.recv().await? {
                                PeerMessage::SplitResponse(SplitResponse::Success(new_keys)) => {
                                    for (key, value) in new_keys {
                                        local_storage.insert(key, value);
                                    }

                                    // Inform predecessor that we are his successor now
                                    // todo maybe fix race condition that predecessor may expect us to asnwer queries but server socket is not yet started
                                    let (mut pre_tx, _) = connect_to_peer!(predecessors[0].address);
                                    pre_tx
                                        .send(PeerMessage::SetSuccessor(ChordPeer {
                                            id: node_id,
                                            address: server_address,
                                        }))
                                        .await?;

                                    // todo maybe await response?

                                    break;
                                }
                                PeerMessage::SplitResponse(SplitResponse::Failure(
                                    responsible_successor,
                                )) => {
                                    // originally acquired successor is no longer responsible
                                    // Therefore use the actually responsible successor and start over
                                    successor = responsible_successor;
                                    predecessors.clear();
                                    local_storage.clear();
                                    tx.send(PeerMessage::CloseConnection).await?;
                                }
                                _ => {
                                    return Err(anyhow!(
                                        "Unexpected response from successor while requesting split"
                                    ))
                                }
                            }
                        }
                        // Initialize finger table
                        // We point everything to our successor, which ensures proper functionality
                        // Stabilize will later update the fingers with better values
                        let finger_table = (0..64)
                            .map(|_| {
                                RwLock::new(ChordPeer {
                                    id: successor.id,
                                    address: successor.address,
                                })
                            })
                            .collect();
                        // todo: stabilize
                        // Close connection to successor
                        tx.send(PeerMessage::CloseConnection).await?;
                        Ok(SChord {
                            state: Arc::new(SChordState {
                                default_store_duration: Duration::from_secs(60),
                                max_store_duration: Duration::from_secs(600),
                                local_storage,
                                finger_table,
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
                    finger_table: (0..64)
                        .map(|_| {
                            RwLock::new(ChordPeer {
                                id: node_id,
                                address: server_address,
                            })
                        })
                        .collect(),
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
        println!("{} API storage insert key {}", self.state.address, key);
        if self.is_responsible_for_key(key) {
            println!("Storing locally!");
            self.internal_insert(key, value, ttl).await
        } else {
            let peer = self.get_responsible_node(key).await?;
            println!("Storing remotely! (on {})", peer.address);

            let mut stream = TcpStream::connect(peer.address).await?;
            let (reader, writer) = stream.split();
            let (mut tx, _) = channels::channel(reader, writer);

            tx.send(PeerMessage::InsertValue(key, value, ttl)).await?;
            tx.send(PeerMessage::CloseConnection).await?;
            Ok(())
        }
    }

    async fn internal_insert(&self, key: u64, value: Vec<u8>, ttl: Duration) -> Result<()> {
        debug_assert!(self.is_responsible_for_key(key));
        self.state.local_storage.insert(key, value);
        Ok(())
    }

    pub async fn get(&self, key: u64) -> Result<Vec<u8>> {
        println!("Retrieving key {} from {}", key, self.state.address);
        if self.is_responsible_for_key(key) {
            self.state
                .local_storage
                .get(&key)
                .map(|entry| entry.value().clone())
                .ok_or(anyhow!(
                    "Key {} not found locally on node {}",
                    key,
                    self.state.address
                ))
        } else {
            let peer = self.get_responsible_node(key).await?;
            let (mut tx, mut rx) = connect_to_peer!(peer.address);

            tx.send(PeerMessage::GetValue(key)).await?;
            match rx.recv().await? {
                PeerMessage::GetValueResponse(option) => {
                    tx.send(PeerMessage::CloseConnection).await?;
                    option.ok_or(anyhow!("Peer does not know value"))
                }
                _ => Err(anyhow!("Wrong response")),
            }
        }
    }

    fn is_responsible_for_key(&self, key: u64) -> bool {
        if let Some(predecessor) = self.state.predecessors.read().first() {
            is_between_on_ring(key, predecessor.id, self.state.node_id)
        } else {
            true
        }
    }

    async fn get_responsible_node(&self, key: u64) -> Result<ChordPeer> {
        // This method should never be called when we are responsible for this key
        assert!(!self.is_responsible_for_key(key));

        // Check if successor is responsible
        let successor = *self
            .state
            .finger_table
            .first()
            .ok_or(anyhow!["Finger table is empty"])?
            .read();

        // If successor is responsible, we return our successor
        // This is required, as otherwise we will not consider routing to our successor,
        // since he is not before the key (since we are the node before the key)
        let (finger, finger_index) = if is_between_on_ring(key, self.state.node_id, successor.id) {
            (successor, 99)
        } else {
            // Default to largest finger, this is the case if no finger is between ourselves and the key
            // Save largest finger before checking for smaller, as the last entry might be
            // modified to be after our key while checking
            let mut finger = *self
                .state
                .finger_table
                .last()
                .ok_or(anyhow!("Last entry in finger table does not exist"))?
                .read();
            let mut finger_index = 100;

            // Find the peer with the largest distance from us which is between the key and us
            // This ensures that the responsible node is after the node which we attempt to contact
            // Otherwise we might build a routing loop since we "skipped" the actual responsible node
            for (finger_table_index, value) in self.state.finger_table.iter().rev().enumerate() {
                // Copy finger, since other threads might attempt to change it
                // It does not matter if this finger is outdated, as this does not impact functionality
                let finger_iter = *value.read();

                // Check if finger is between key and us, so responsible node is after the finger
                if is_between_on_ring(finger_iter.id, self.state.node_id, key) {
                    finger = finger_iter;
                    finger_index = finger_table_index;
                    break;
                }
            }

            // Assert that the key is after the node we try to contact, otherwise we might get routing loops
            assert!(!is_between_on_ring(key, self.state.node_id, finger.id));

            (finger, finger_index)
        };

        println!(
            "{} - {:x} looking up node responsible for {:x}, will now ask finger index {}, {} - {:x}",
            self.state.address, self.state.node_id, key, finger_index, finger.address, finger.id,
        );

        // Assert that we do not try to route things to ourselves
        assert_ne!(finger.id, self.state.node_id);

        let (mut tx, mut rx) = connect_to_peer!(finger.address);

        // contact finger and ask for responsible node
        // finger will recursively find out responsible node
        tx.send(PeerMessage::GetNode(key)).await?;
        match rx.recv().await? {
            PeerMessage::GetNodeResponse(peer) => {
                tx.send(PeerMessage::CloseConnection).await?;
                Ok(peer)
            }
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
                        println!(
                            "{} - {:x} was asked for node responsible  {:x}, which is us",
                            self.state.address, self.state.node_id, id
                        );

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
                    let value = self
                        .state
                        .local_storage
                        .get(&key)
                        .map(|entry| entry.value().clone());
                    tx.send(PeerMessage::GetValueResponse(value)).await?;
                }
                PeerMessage::GetPredecessor => {
                    let predecessor = self.get_predecessor();
                    tx.send(PeerMessage::GetPredecessorResponse(predecessor))
                        .await?;
                }
                PeerMessage::SplitRequest(new_peer) => {
                    println!("{}: Split pred: {}", self.state.address, new_peer.address);
                    let predecessor = {
                        // Aquire write lock for predecessors
                        let mut predecessors = self.state.predecessors.write();

                        // Get first predecessor, and if not present ourselves
                        let predecessor = *predecessors.first().unwrap_or(&ChordPeer {
                            id: self.state.node_id,
                            address: self.state.address,
                        });

                        // Check if the new peer is inbetween our predecessor and ourselves
                        // If we the predecessor is us, this method also returns true as we then control all keys and are alone
                        if is_between_on_ring(new_peer.id, predecessor.id, self.state.node_id) {
                            // insert as new predecessor
                            predecessors.insert(0, new_peer);
                        }
                        predecessor
                    };

                    if is_between_on_ring(new_peer.id, predecessor.id, self.state.node_id) {
                        let mut values = Vec::new();
                        for entry in self.state.local_storage.iter() {
                            let key = *entry.key();
                            let value = entry.value();
                            if is_between_on_ring(key, predecessor.id, new_peer.id) {
                                values.push((key, value.clone()));
                            }
                        }
                        tx.send(PeerMessage::SplitResponse(SplitResponse::Success(values)))
                            .await?;
                    } else {
                        tx.send(PeerMessage::SplitResponse(SplitResponse::Failure(
                            predecessor,
                        )))
                        .await?;
                    }
                }
                PeerMessage::InsertValue(key, value, ttl) => {
                    println!("{} asked to store {:x}", self.state.address, key);
                    self.internal_insert(key, value, ttl).await?;
                }
                PeerMessage::SetSuccessor(successor) => {
                    println!(
                        "{}: Set Successor: {}",
                        self.state.address, successor.address
                    );

                    // Update finger table
                    // We update all entries which should now point to our successor
                    for (i, entry) in self.state.finger_table.iter().enumerate() {
                        if is_between_on_ring(
                            self.state.node_id.wrapping_add(2u64.pow(i as u32)),
                            self.state.node_id,
                            successor.id,
                        ) {
                            *entry.write() = successor;
                        } else {
                            // Assert that we updated at least one entry, otherwise something is wrong
                            assert_ne!(i, 0);
                            break;
                        }
                    }
                    // todo maybe send answer
                    return Ok(());
                }
                PeerMessage::CloseConnection => {
                    return Ok(());
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
fn is_between_on_ring(value: u64, lower: u64, upper: u64) -> bool {
    if lower == upper {
        return true;
    } else if lower < upper {
        // No wrap-around needed
        value >= lower && value <= upper
    } else {
        // Wrap-around case
        value >= lower || value <= upper
    }
}

#[test]
fn test_is_between_on_ring() {
    // using common code.
    assert_ne!(
        is_between_on_ring(
            15203477739406956416,
            16189758198029460966,
            7565083766968620880
        ),
        is_between_on_ring(
            15203477739406956416,
            7565083766968620880,
            16189758198029460966
        ),
    );
}
