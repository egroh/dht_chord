//! Implementation of a distributed hash table based on [Chord](https://en.wikipedia.org/wiki/Chord_(peer-to-peer))
//!
//! It can operate independently from the API module and is usable as a stand-alone crate.
//!
//! # Features:
//! - Key-Value storage
//! - Distributed, if more than one node available (but fully functional with one node only)
//! - Built-in replication
//! - IPv4 and IPv6 support
//! - Automatic node discovery
//! - Stabilization if nodes leave or join
//! - Housekeeping thread to remove expired entries and refresh
//! - Completely asynchronous and multi-threaded
//! - Requests from the API and from other nodes are processed and answered concurrently
//! - Free of race conditions due to Rusts ownership model
//! - Performance optimized implementation, capable of establishing a fully connected network with 2000
//! nodes running on a single machine in under 10 seconds (without PoW)
//!
//! # Security measures:
//! - [SHA-3-512](https://docs.rs/sha3/0.10.8/sha3/) proof of work challenges with adjustable difficulty for requests
//!     - Does not prevent [byzantine](https://en.wikipedia.org/wiki/Byzantine_fault) nodes from splitting the network or eclipsing nodes,
//!     but prevents greedy nodes from abusing the storage system
//!     - Difficulty is independently adjustable for each request type
//!     - Currently we maintain two difficulty settings,
//!     one for get requests and one for (potentially disruptive) write/storage requests
//! - Addresses of nodes assigned based on hash of IP + port
//!     - This could be easily adjusted to only hash IPs,
//!     providing limited [sybil defense](https://en.wikipedia.org/wiki/Sybil_attack)
//!     - In case of IPv6, we could only hash a masked version of the IP
//!     - We have currently not implemented this,
//!     as it would interfere with development and testing
//! - Limited defence against nodes refusing to store values or disconnecting
//!     - Our housekeeping thread continuously refreshes values that have been stored in the DHT upon our own request
//!
//! # Security evaluation: // todo
//! - The inherent structure of the overlay offers some limited defense against id mapping attacks
//!     - It is not easily possible to fool the peers around the intimidated identity, as they are already in direct contact
//!     - Other nodes usually try to route requests as fast as possible into proximity of the intimidated identity
//! - Likewise the finger table offers resistance against eclipse attacks
//!     - Nodes regularly check in with all their fingers, which makes it hard to eclipse them fully with ongoing churn
//!  - Stabilization method, which regularly checks the health of peers in neighborhood and fixes the overlay if necessary
//!  - Storage and Retrieval attacks are partially mitigated by built in replication
//!  - Inconsistent behaviour also partially mitigated by built in replication
//!  - DoS Attacks such as content pollution and index poisoning are hardened against by PoW for insertion of values
//!
//! # Architecture
//!  - Our architecture separates the Chord module from the API communication.
//!  - The API simply makes the features of the Chord module accessible via network communication
//!  - We make heavy use of multithreading/processing, using tokio's (green) threads for all asynchronous workloads, like
//!       I/O.
//!     
//! ## Peer-to-peer communication
//!  - We are using Rust channels for inter-node communication (this allows us to serialize/deserialize entire structs
//!    typesafe and integrity checked)
//!  - All inter-node messages are specified in [`PeerMessage`]
//!  - Errors are passed up the stack and are processed accordingly. This means that for example an
//!  unexpected error while communicating with a node only leads to the unexpected end of this connection
//!
//! ## Peer-to-peer protocol
//! Short sketch of the messages exchanged to give a basic understanding how the protocol works.
//!
//! For understandability the processes are reduced and do not server as complete description.
//!
//! All processes are terminated by a [PeerMessage::CloseConnection]. This allows to send other requests
//! after completing a process without needing to create a new connection.
//!       
//! ### Joining the Network
//!
//! - send [PeerMessage::GetNode] to obtain node responsible for own identifier
//! - send [PeerMessage::SplitRequest] to obtain either the keys the new node is responsible or the
//! actual responsible node if the overlay changed since the first request
//! - send [PeerMessage::SetSuccessor] to the predecessor of the responsible node
//!
//! ### Retrieving or String a Value
//! - send [PeerMessage::GetNode] to obtain the node responsible for the key
//! - send [PeerMessage::GetValue] or [PeerMessage::InsertValue] to retrieve or set the value
//!
//! # Future Work
//!
//! ## Improved Sybil defence
//! - It would likely be sensible to introduce further hardware such as bandwidth(with respective scanners)
//! - New nodes should be treated differently, i.e. not as trustworthy until they stayed some time in the network
//! - This would also help with other attacks
//!
//! ## Misbehaviour defence
//! Currently only crash faults are dealt with. Malicious faults will go undetected.
//! Local nodes could determine if a peer is misbehaving and exclude it from the overlay.
//! However peers know their neighbors and could therefore act differently to different peers.
//! Defending against such behaviour is hard, as for example a reporting system or similarly can
//! be abused by malicious peers to get "good" nodes excluded from the network.
//!
//! ## Better Stabilize
//! Stabilize in its current form relies on each node to realize that a peer disconnected from the network
//! This sometimes incorrectly invalidates `SetPredecessor` and `SetSuccessor` Requests, as they are
//! denied on the grounds that the receiving node does not know yet that its current successor/predecessor
//! no longer exists.
//!
//! # Work Distribution
//!     - We usually worked closely together on the project, even including pair-programming
//!     - Frequent communication and git allowed us to agilely distribute open tasks, further supported by automatic tests
//!     - We helped each other out in solving open problems, bugs and making design decision
//!     - In since the last report Eddie generally focused on core functionality like node joining, routing and stabilization
//!     - Valentin on the other hand mostly focused on security features, housekeeping, and CI/CD Pipline for automatic testing and documentation deployment
//!
//! # Effort Spend
//!     - Since the midterm report, we were able to extend our framework without any major changes
//!     - Therefore we were able to spend all effort on implementing new features
//!     - Our effort therefore was spend on implementing everything named in the work distribution
//!     - Additionally significant effort was spend on debugging multithreaded asynchronous code over multiple nodes
//!     to realize the expected functionality of routing and stabilization
//!     - Specifically for stabilize an own approach was developed to ensure proper functionality
//!     with our implementation, augmented by the fact that there are few resources on how to handle
//!     churn in chord
//!     - Otherwise there has also been design effort to design and evaluate different approaches
//!     for security to arrive at the system we implemented

use std::cmp::Ordering;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use channels::serdes::Bincode;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use log::{debug, error, info, trace, warn};
use parking_lot::RwLock;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf, ReadHalf, WriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{sleep_until, Instant};
use tokio_util::sync::CancellationToken;

use crate::chord::peer_messages::{ChordPeer, PeerMessage, ProofOfWorkChallenge, SplitResponse};

pub mod peer_messages;

macro_rules! connect_to_peer {
    ($address:expr) => {{
        let stream = TcpStream::connect($address).await?;
        let (reader, writer) = stream.into_split();
        channels::channel(reader, writer)
    }};
}

/// Distributed Hash Table
///
/// This struct provides methods necessary to interact with the DHT.
///
/// As the DHT is heavily multithreaded,
/// its internal state is held in an atomic reference counter (Arc).
#[derive(Clone)]
pub struct Chord {
    state: Arc<ChordState>,
}

/// All data necessary for DHT operation
struct ChordState {
    /// Our own Node-ID.
    node_id: u64,

    /// Our outward facing address.
    ///
    /// Note that as it is used to calculate the node id,
    /// it must be the address we are reachable under to other nodes.
    address: SocketAddr,

    /// The characteristic [Chord](https://en.wikipedia.org/wiki/Chord_(peer-to-peer)) jump table.
    /// We maintain 64 entries with exponentially growing distance between them.
    /// As we are expected to do maintenance and handle churn - even under heavy load,
    /// entries are lockable individually.
    /// Small inconsistencies are tolerable, as they only affect routing efficiency.
    finger_table: Vec<RwLock<ChordPeer>>,

    /// List of predecessors do determine key responsibility
    /// and provide limited fault tolerance in case of churn.
    predecessors: RwLock<Vec<ChordPeer>>,

    /// Default storage duration of entries, if not specified otherwise.
    default_storage_duration: Duration,

    /// The maximum duration for which we keep an item
    /// we have been tasked to store by the network in our [`node_storage`](ChordState::node_storage).
    /// Does not apply to [`personal_storage`](ChordState::personal_storage).
    max_storage_duration: Duration,

    /// The default amount of replications which are done for each entry.
    /// Replications are performed
    /// by increasing the key the entry is stored under by 1 and re-hashing the result.
    default_replication_amount: u8,

    /// The personal storage keeps track of entries we have been asked to store by the local user.
    /// It is used as replication source by the housekeeping thread,
    /// ensuring its contents are continuously re-spread across the network.
    /// It maps a key to a tuple of the value, the expiration time and the replication amount.
    /// It is also used as cache for fast local lookups.
    personal_storage: DashMap<u64, (Vec<u8>, DateTime<Utc>, u8)>, // Key, (Value, Expiration time, Replication Amount)

    /// The node storage keeps track of entries that have been assigned to the local DHT node by the network.
    /// Once the stored DateTime is hit, the entry expires and is removed by the housekeeping thread.
    node_storage: DashMap<u64, (Vec<u8>, DateTime<Utc>)>,
}

impl Chord {
    /// Construct new instance of chord.
    ///
    /// If no initial peer is provided, it is assumed that this node is the only node present.
    ///
    /// Otherwise the initial peer is contacted to determine the successor and predecessor of this node.
    /// They are then contacted to transfer responsibility of the entries this node is responsible for.
    ///
    /// The finger table will be initialized with all entries
    /// pointing to our successor until the first run of housekeeping.
    pub async fn new(
        initial_peer: Option<SocketAddr>,
        server_address: SocketAddr,
        default_storage_duration: Duration,
        max_storage_duration: Duration,
    ) -> Self {
        let mut hasher = DefaultHasher::new();
        server_address.hash(&mut hasher);
        let node_id = hasher.finish();

        if let Some(initial_peer) = initial_peer {
            info!("Connecting to bootstrap node: {}", initial_peer);
            let initial_peer_connection_result = async || -> Result<Chord> {
                // Connect to initial node
                let (mut tx, mut rx) = connect_to_peer!(initial_peer);

                // Acquire node responsible for the location of our id
                // this node is automatically our successor
                // this works because no node is aware of our existence yet
                tx.send(PeerMessage::GetNode(node_id)).await?;
                solve_proof_of_work(&mut tx, &mut rx).await?;
                match rx.recv().await? {
                    PeerMessage::GetNodeResponse(mut successor) => {
                        // Close connection to initial peer
                        tx.send(PeerMessage::CloseConnection).await?;

                        let mut predecessors = Vec::new();
                        let local_storage = DashMap::new();

                        // Connect to successor
                        let (mut tx, mut rx) = connect_to_peer!(successor.address);
                        loop {
                            // Ask predecessor
                            tx.send(PeerMessage::GetPredecessor).await?;
                            solve_proof_of_work(&mut tx, &mut rx).await?;
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
                            solve_proof_of_work(&mut tx, &mut rx).await?;
                            match rx.recv().await? {
                                PeerMessage::SplitResponse(SplitResponse::Success(new_keys)) => {
                                    for (key, value) in new_keys {
                                        local_storage.insert(
                                            key,
                                            (value, Utc::now() + default_storage_duration),
                                        );
                                    }

                                    // Inform predecessor that we are his successor now
                                    let (mut tx, mut rx) =
                                        connect_to_peer!(predecessors[0].address);
                                    tx.send(PeerMessage::SetSuccessor(ChordPeer {
                                        id: node_id,
                                        address: server_address,
                                    }))
                                    .await?;
                                    solve_proof_of_work(&mut tx, &mut rx).await?;
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
                        // Close connection to successor
                        tx.send(PeerMessage::CloseConnection).await?;
                        let chord = Chord {
                            state: Arc::new(ChordState {
                                default_storage_duration,
                                max_storage_duration,
                                default_replication_amount: 4,
                                personal_storage: Default::default(),
                                node_storage: local_storage,
                                finger_table,
                                node_id,
                                predecessors: RwLock::new(predecessors),
                                address: server_address,
                            }),
                        };
                        Ok(chord)
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
            // Construct chord which is alone, i.e. no other nodes exists
            info!("No bootstrap node provided; starting alone");
            Chord {
                state: Arc::new(ChordState {
                    default_storage_duration,
                    max_storage_duration,
                    default_replication_amount: 4,
                    personal_storage: Default::default(),
                    node_storage: DashMap::new(),
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

    /// Starts the server socket to listen for incoming peer connections
    /// The cancellation token can be used to gracefully stop the server socket
    pub async fn start_server_socket(
        &self,
        cancellation_token: CancellationToken,
    ) -> JoinHandle<()> {
        let self_clone = Chord {
            state: self.state.clone(),
        };
        let listener = TcpListener::bind(self.state.address)
            .await
            .expect("Failed to bind Chord server socket");
        // Open channel for inter thread communication
        let (tx, mut rx) = mpsc::channel(1);

        let handle = tokio::spawn(async move {
            // Send signal that we are running
            tx.send(true).await.expect("Unable to send message");
            info!("Chord listening for peers on {}", self_clone.state.address);
            loop {
                tokio::select! {
                    result = listener.accept() => {
                    let (stream, address) = result.unwrap();
                    let self_clone = Chord {
                        state: self_clone.state.clone(),
                    };
                    trace!("{}: New connection from: {}", self_clone.state.address, address);
                    tokio::spawn(async move {
                            if let Err(e) = self_clone.accept_peer_connection(stream).await {
                                error!("Error in connection with connecting peer: {:?}", e);
                            }
                    });
                    }
                    _ = cancellation_token.cancelled() => {
                        info!("{}: Stopped accepting new peer connections.", self_clone.state.address);
                        break;
                    }
                }
            }
        });
        // Await thread spawn, to avoid EOF errors because the thread is not ready to accept messages
        rx.recv().await.unwrap();
        handle
    }

    /// Method to start the housekeeping thread
    ///
    /// This thread will stop when the cancellation token is cancelled
    pub async fn start_housekeeping_thread(
        &self,
        cancellation_token: &CancellationToken,
    ) -> JoinHandle<()> {
        let chord = self.clone();
        let cancellation_token = cancellation_token.clone();
        tokio::spawn(async move {
            chord.housekeeping(cancellation_token).await;
        })
    }

    /// Performs the housekeeping which performs some periodic tasks
    /// - Cleanup of expired values in
    /// [`ChordState::personal_storage`] and [`ChordState::node_storage`]
    /// - Refresh keys and their replications on other nodes
    /// - Stabilize to account for churn
    /// - Update finger table to optimize routing
    async fn housekeeping(&self, cancellation_token: CancellationToken) {
        while !cancellation_token.is_cancelled() {
            let sleep_target_time = Instant::now() + Duration::from_secs(60);

            // Cleanup
            self.state.personal_storage.retain(|key, value| {
                if value.1 < Utc::now() {
                    debug!("Removing expired key {:x} from personal storage", key);
                    // discard element
                    false
                } else {
                    // keep element
                    true
                }
            });

            self.state.node_storage.retain(|key, value| {
                if value.1 < Utc::now() {
                    debug!("Removing expired key {:x} from node storage", key);
                    // discard element
                    false
                } else {
                    // keep element
                    true
                }
            });

            // Replication
            let replication_result = async || -> Result<()> {
                for entry in self.state.personal_storage.iter() {
                    for i in 0..=entry.value().2 {
                        let key = calculate_hash(&(entry.key() + i as u64));
                        let ttl = (entry.value().1 - Utc::now())
                            .to_std()
                            .unwrap_or(self.state.default_storage_duration);
                        if self.am_responsible_for_key(key) {
                            debug!("Storing key {} locally!", key);
                            self.internal_insert(key, entry.value().0.clone(), ttl)
                                .await?;
                        } else {
                            let peer = self.get_responsible_node(key).await?;
                            debug!("Storing key {} remotely! (on {})", key, peer.address);
                            let (mut tx, mut rx) = connect_to_peer!(peer.address);
                            tx.send(PeerMessage::InsertValue(key, entry.value().0.clone(), ttl))
                                .await?;
                            solve_proof_of_work(&mut tx, &mut rx).await?;
                            tx.send(PeerMessage::CloseConnection).await?;
                        }
                    }
                }
                Ok(())
            };
            if let Err(e) = replication_result().await {
                warn!("Error during replication: {}", e);
            }

            if let Err(e) = self.stabilize().await {
                warn!("Error during stabilization: {}", e);
            }

            if let Err(e) = self.fix_fingers().await {
                warn!("Error during fix_fingers: {}", e);
            }

            // Sleep until the previously set target
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    return;
                }
                _ = sleep_until(sleep_target_time) => {
                    // Do nothing, continue loop
                }
            }
        }
    }

    /// Inserts a key value pair into the network
    ///
    /// If specified, multiple replications of the value will be inserted.
    /// All inserted values are periodically rebroadcasted to counter churn.
    pub async fn insert(
        &self,
        key: u64,
        value: Vec<u8>,
        ttl: Duration,
        replication_amount: u8,
    ) -> Result<()> {
        debug!(
            "{} received API storage request for key {}",
            self.state.address, key
        );
        self.state
            .personal_storage
            .insert(key, (value.clone(), Utc::now() + ttl, replication_amount));
        for i in 0..=replication_amount {
            let key = calculate_hash(&(key + i as u64));
            if self.am_responsible_for_key(key) {
                debug!("Storing key {} locally!", key);
                self.internal_insert(key, value.clone(), ttl).await?;
            } else {
                let peer = self.get_responsible_node(key).await?;
                debug!("Storing key {} remotely! (on {})", key, peer.address);
                let (mut tx, mut rx) = connect_to_peer!(peer.address);
                tx.send(PeerMessage::InsertValue(key, value.clone(), ttl))
                    .await?;
                solve_proof_of_work(&mut tx, &mut rx).await?;
                tx.send(PeerMessage::CloseConnection).await?;
            }
        }
        Ok(())
    }

    /// Inserts an entry into our local [`node_storage`](ChordState::node_storage),
    /// alongside its expiry time.
    async fn internal_insert(&self, key: u64, value: Vec<u8>, ttl: Duration) -> Result<()> {
        debug_assert!(self.am_responsible_for_key(key));
        if self.state.max_storage_duration > ttl {
            self.state
                .node_storage
                .insert(key, (value, Utc::now() + ttl));
        } else {
            self.state
                .node_storage
                .insert(key, (value, Utc::now() + self.state.max_storage_duration));
        }
        Ok(())
    }

    /// Queries the network for a given key and returns its corresponding value.
    ///
    /// If the value can not be found on the first try,
    /// we also query for replicated backup keys.
    pub async fn get(&self, key: u64) -> Option<Vec<u8>> {
        debug!(
            "{} received API get request for key {}",
            self.state.address, key
        );
        if let Some(entry) = self.state.personal_storage.get(&key) {
            debug!("Key {} found in personal storage", key);
            return Some((*entry.value().0).to_owned());
        }
        for i in 0..=self.state.default_replication_amount {
            let key = calculate_hash(&(key + i as u64));
            if let Some(value) = self
                .state
                .node_storage
                .get(&key)
                .map(|entry| entry.value().0.to_owned())
            {
                debug!("Key {} found in node storage", key);
                return Some(value);
            } else if !self.am_responsible_for_key(key) {
                let Ok(peer) = self.get_responsible_node(key).await else {
                    continue;
                };
                async fn query_key(key: u64, peer: ChordPeer) -> Result<Vec<u8>> {
                    let (mut tx, mut rx) = connect_to_peer!(peer.address);
                    tx.send(PeerMessage::GetValue(key)).await?;
                    solve_proof_of_work(&mut tx, &mut rx).await?;
                    let response = rx.recv().await?;
                    tx.send(PeerMessage::CloseConnection).await?;
                    if let PeerMessage::GetValueResponse(Some(value)) = response {
                        debug!("Key {} found on peer {}", key, peer.address);
                        return Ok(value);
                    }
                    Err(anyhow!("Key not found"))
                }
                if let Ok(value) = query_key(key, peer).await {
                    return Some(value);
                }
            }
        }
        None
    }

    /// Returns whether this node is responsible for a given key.
    ///
    /// This is the case if the key lies between us (inclusive) and our predecessor (exclusive)
    /// or no predecessor is currently known,
    /// in which case this method returns true.
    fn am_responsible_for_key(&self, key: u64) -> bool {
        if let Some(predecessor) = self.state.predecessors.read().first() {
            is_between_on_ring(key, predecessor.id, self.state.node_id)
        } else {
            true
        }
    }

    /// Returns the [`ChordPeer`] responsible for a given key.
    ///
    /// This method attempts to recursively find the node responsible for a given key
    /// by asking the most closely responsible node in our finger table to find it for us.
    async fn get_responsible_node(&self, key: u64) -> Result<ChordPeer> {
        // This method should never be called when we are responsible for this key
        debug_assert!(!self.am_responsible_for_key(key));

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
        let (mut finger, mut finger_index) =
            if is_between_on_ring(key, self.state.node_id, successor.id) {
                // Assert that we do not try to route things to ourselves
                debug_assert_ne!(successor.id, self.state.node_id);
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
                for (finger_table_index, value) in self.state.finger_table.iter().rev().enumerate()
                {
                    // Copy finger, since other threads might attempt to change it
                    // It does not matter if this finger is outdated, as this does not impact functionality
                    let finger_iter = *value.read();

                    // Check if finger is between key and us, so responsible node is after the finger
                    if is_between_on_ring(finger_iter.id, self.state.node_id, key) {
                        finger = finger_iter;
                        // Index is reversed because we iterate in reverse
                        finger_index = 64 - finger_table_index;
                        break;
                    }
                }

                // Assert that we do not try to route things to ourselves
                debug_assert_ne!(finger.id, self.state.node_id);

                // Assert that the key is after the node we try to contact, otherwise we might get routing loops
                debug_assert!(!is_between_on_ring(key, self.state.node_id, finger.id));

                (finger, finger_index)
            };

        debug!(
            "{} - {:x} looking up node responsible for {:x}, will now ask finger index {}, {} - {:x}",
            self.state.address, self.state.node_id, key, finger_index, finger.address, finger.id,
        );

        loop {
            let stream = TcpStream::connect(finger.address).await;
            match stream {
                Ok(stream) => {
                    let (reader, writer) = stream.into_split();
                    let (mut tx, mut rx) = channels::channel(reader, writer);

                    // contact finger and ask for responsible node
                    // finger will recursively find out responsible node
                    tx.send(PeerMessage::GetNode(key)).await?;
                    solve_proof_of_work(&mut tx, &mut rx).await?;
                    return match rx.recv().await? {
                        PeerMessage::GetNodeResponse(peer) => {
                            tx.send(PeerMessage::CloseConnection).await?;
                            Ok(peer)
                        }
                        _ => Err(anyhow!("Wrong response")),
                    };
                }
                Err(e) => {
                    debug!("Cant connect to finger {}", e);
                    assert!(finger_index < self.state.finger_table.len());
                    assert!(finger_index > 0);
                    finger = *self.state.finger_table[finger_index - 1].read();
                    finger_index -= 1;
                }
            }
        }
    }

    /// Stabilized this node
    ///
    /// We contact our predecessor:
    /// - If the predecessor is reachable, we check that this node and the predecessor are in
    /// agreement about no nodes existing between them.
    /// If there are any, we set them as our predecessor
    /// - If the predecessor is not reachable,
    /// we will iteratively step through our predecessor list in [`ChordState::predecessors`],
    /// until an active node is found.
    /// We then send this predecessor the message that we are his successor.
    ///
    /// Once the predecessor has been stabilized,
    /// three more predecessors are recursively acquired to guard against churn.
    ///
    /// We contact our successor (the first node in our [`ChordState::finger_table`]):
    /// - If the successor is reachable, we again ensure that there are no nodes between us
    /// or update the finger table accordingly.
    /// - If the successor is not reachable, the next peer in the finger table is contacted,
    /// until we either find a working node or reach the end of the finger table.
    pub(crate) async fn stabilize(&self) -> Result<()> {
        if self.state.predecessors.read().is_empty() {
            // If we have no predecessor, we are alone and there is nothing to stabilize
            return Ok(());
        }

        // fixing immediate predecessor
        loop {
            let predecessor = {
                let predecessors = self.state.predecessors.read();
                if !predecessors.is_empty() {
                    break;
                }
                *predecessors.first().ok_or(anyhow!("No predecessor"))?
            };

            // Ask our predecessor for its successor which should be us
            match self.ask_for_successor(predecessor).await {
                Ok(supposed_predecessor) => {
                    if supposed_predecessor.address != self.state.address {
                        if is_between_on_ring(
                            supposed_predecessor.id,
                            predecessor.id,
                            self.state.node_id,
                        ) {
                            // our predecessor thinks this is his successor, which should therefore be our predecessor
                            let mut predecessors_write = self.state.predecessors.write();
                            // Assert that list has not changed since we last read it
                            assert!(!predecessors_write.is_empty());
                            assert_eq!(predecessors_write.first().unwrap().id, predecessor.id);
                            predecessors_write.insert(0, supposed_predecessor);
                        } else {
                            debug!("{}: Predecessor does not know us", self.state.address);
                            // Send predecessor that we are his successor
                            let (mut tx, mut rx) = connect_to_peer!(predecessor.address);
                            tx.send(PeerMessage::SetSuccessor(self.as_chord_peer()))
                                .await?;
                            solve_proof_of_work(&mut tx, &mut rx).await?;
                            tx.send(PeerMessage::CloseConnection).await?;
                        }
                    }
                }
                Err(e) => {
                    debug!("Encountered problem when contacting predecessor, assuming its no longer with us: {}", e);

                    let mut predecessors_write = self.state.predecessors.write();
                    // Assert that list has not changed since we last read it
                    assert!(!predecessors_write.is_empty());
                    assert_eq!(
                        predecessors_write.first().unwrap().address,
                        predecessor.address
                    );
                    predecessors_write.remove(0);
                }
            }
        }

        assert!(!self.state.predecessors.read().is_empty());
        // Add backup predecessors
        for i in 0..3 {
            if let Err(e) = self.stabilize_predecessor(i).await {
                warn!("Encountered problem when contacting predecessor, assuming its no longer with us: {}", e);
                self.state.predecessors.write().remove(i);
                break;
            }
        }

        // Fix Successors
        let mut previous_finger = None;
        for i in 0..self.state.finger_table.len() {
            // Looping though the finger table until we found a node which we can contact
            let current_finger = *self.state.finger_table[i].read();

            // Skip nodes which we already asked, and didnt respond
            match previous_finger {
                None => previous_finger = Some(current_finger),
                Some(previous_finger) => {
                    if previous_finger.address == current_finger.address {
                        // We already contacted this finger, so we skip it
                        continue;
                    }
                }
            }

            // Contact finger, if connection is successful the method will
            // loop asking the current peer for its successor

            // This gets us the closest successor in an unbroken connection chain from one of our fingers
            match self.stabilize_successor(current_finger).await {
                Ok(successor) => {
                    if successor.address == self.state.address {
                        // Nothing to do, successor is alive and knows about us
                        return Ok(());
                    }

                    // Fix all previous entries of the finger table
                    for j in 0..=i {
                        *self.state.finger_table[j].write() = successor;
                    }
                    // Fix all entries after which still use the old successor
                    for entry in self.state.finger_table.iter() {
                        if entry.read().address == current_finger.address {
                            *entry.write() = successor;
                        }
                    }
                    self.assert_finger_table_invariants_correct();

                    // Finished nothing more to do
                    return Ok(());
                }
                Err(e) => {
                    // Go to next finger, assuming its dead
                    debug!("{}: successor dead, {}", self.state.address, e);
                }
            }
        }
        Err(anyhow!("Did not find any contactable node in finger table"))
    }

    /// Attempts to contact the predecessor at the given index.
    /// If this predecessor is not reachable, or any other issue occurs, an error is returned.
    /// If this connection is successful the predecessor list is modified accordingly.
    async fn stabilize_predecessor(&self, predecessor_index: usize) -> Result<()> {
        let some_predecessor = self.state.predecessors.read()[predecessor_index];

        let (mut tx, mut rx) = connect_to_peer!(some_predecessor.address);
        // Ask the selected predecessor for its predecessor
        tx.send(PeerMessage::GetPredecessor).await?;
        solve_proof_of_work(&mut tx, &mut rx).await?;
        match rx.recv().await? {
            PeerMessage::GetPredecessorResponse(preceding_predecessor) => {
                tx.send(PeerMessage::CloseConnection).await?;

                // Insert predecessor into list, if the predecessors before fail
                let mut write_predecessors = self.state.predecessors.write();
                if write_predecessors.len() > predecessor_index + 1
                    && write_predecessors[predecessor_index + 1].address
                        != preceding_predecessor.address
                {
                    write_predecessors.insert(predecessor_index + 1, preceding_predecessor);
                } else {
                    write_predecessors.push(preceding_predecessor);
                }
                Ok(())
            }
            _ => {
                tx.send(PeerMessage::CloseConnection).await?;
                Err(anyhow!("Node answered unexpected message"))
            }
        }
    }

    /// Attempts to contact the given successor.
    /// If the successor is not reachable, or any other issue occurs, an error is returned.
    /// If the successor is reachable,
    /// this method will iteratively find any reachable predecessor of the successor,
    /// and return the last reachable successor before this node.
    async fn stabilize_successor(&self, possible_successor: ChordPeer) -> Result<ChordPeer> {
        let mut closest_reachable_successor = possible_successor;
        let mut next_possible_successor = closest_reachable_successor;

        // Loop until we found the closest successor we can reach
        loop {
            match self.ask_for_predecessor(next_possible_successor).await {
                Ok(peer) => {
                    if peer.address != self.state.address {
                        closest_reachable_successor = next_possible_successor;
                        next_possible_successor = peer;
                    } else {
                        // Found ourselves, so this has to be our successor
                        break;
                    }
                }
                Err(e) => {
                    if closest_reachable_successor.address == next_possible_successor.address {
                        return Err(anyhow!(
                            "{}: Cannot contact possible successor {}",
                            self.state.address,
                            closest_reachable_successor.address
                        ));
                    }

                    debug!(
                        "{}: found successor {} in chain which is dead {}",
                        self.state.address, next_possible_successor.address, e
                    );

                    // Found last reachable successor
                    break;
                }
            }
        }
        debug!(
            "{}: Contacting reachable successor {}, setting ourself as predecessor",
            self.state.address, closest_reachable_successor.address
        );

        let (mut tx_suc, mut rx_suc) = connect_to_peer!(closest_reachable_successor.address);

        // Send our successor that we are his predecessor
        tx_suc
            .send(PeerMessage::SetPredecessor(self.as_chord_peer()))
            .await?;
        solve_proof_of_work(&mut tx_suc, &mut rx_suc).await?;
        tx_suc.send(PeerMessage::CloseConnection).await?;
        Ok(closest_reachable_successor)
    }

    /// Iterates through all finger table entries
    /// and asks the previous entry about the peer in the next entry.
    ///
    /// This ensures that all finger table entries point to the successor of `2^index`,
    /// where index is the index in the finger table.
    pub(crate) async fn fix_fingers(&self) -> Result<()> {
        if self.state.predecessors.read().is_empty() {
            // If we have no predecessor, we are alone and there is nothing to stabilize
            return Ok(());
        }

        // Fix fingers
        for (i, entry) in self.state.finger_table.iter().enumerate() {
            // Check if the next entry is out of bounds
            if i + 1 >= self.state.finger_table.len() {
                break;
            }
            let finger = *entry.read();
            let next_finger_id = self.id_at_finger_index(i + 1);

            let (mut tx, mut rx) = connect_to_peer!(finger.address);
            // Ask the finger, for the next finger
            tx.send(PeerMessage::GetNode(next_finger_id)).await?;
            solve_proof_of_work(&mut tx, &mut rx).await?;

            match rx.recv().await? {
                PeerMessage::GetNodeResponse(peer) => {
                    // Set response as next finger
                    *self.state.finger_table[i + 1].write() = peer;
                    tx.send(PeerMessage::CloseConnection).await?;
                }
                _ => {
                    tx.send(PeerMessage::CloseConnection).await?;
                    return Err(anyhow!("Node answered unexpected message"));
                }
            }
        }

        self.assert_finger_table_invariants_correct();

        Ok(())
    }

    /// Returns a [`ChordPeer`] representing this node; useful for peer communication.
    fn as_chord_peer(&self) -> ChordPeer {
        ChordPeer {
            id: self.state.node_id,
            address: self.state.address,
        }
    }

    /// Returns a result with the predecessor of `node_to_ask`.
    /// If the provided peer is not reachable an error is returned
    ///
    ///
    /// # Arguments
    ///
    /// * `node_to_ask` - the node to ask for its predecessor
    async fn ask_for_predecessor(&self, node_to_ask: ChordPeer) -> Result<ChordPeer> {
        let (mut tx, mut rx) = connect_to_peer!(node_to_ask.address);
        // Add one to the id we ask for, as the successor is responsible for this key
        tx.send(PeerMessage::GetPredecessor).await?;
        solve_proof_of_work(&mut tx, &mut rx).await?;
        match rx.recv().await? {
            PeerMessage::GetPredecessorResponse(peer) => {
                tx.send(PeerMessage::CloseConnection).await?;

                Ok(peer)
            }
            _ => {
                tx.send(PeerMessage::CloseConnection).await?;
                Err(anyhow!("Node answered unexpected message"))
            }
        }
    }

    /// Asks a [`ChordPeer`] for its successor
    async fn ask_for_successor(&self, node_to_ask: ChordPeer) -> Result<ChordPeer> {
        let (mut tx, mut rx) = connect_to_peer!(node_to_ask.address);
        // Add one to the id we ask for, as the successor is responsible for this key
        tx.send(PeerMessage::GetNode(node_to_ask.id.wrapping_add(1)))
            .await?;
        solve_proof_of_work(&mut tx, &mut rx).await?;
        match rx.recv().await? {
            PeerMessage::GetNodeResponse(peer) => {
                tx.send(PeerMessage::CloseConnection).await?;
                Ok(peer)
            }
            _ => {
                tx.send(PeerMessage::CloseConnection).await?;
                Err(anyhow!("Node answered unexpected message"))
            }
        }
    }

    /// Returns the address on which this node is listening for incoming connections.
    #[cfg(test)]
    pub(crate) fn get_address(&self) -> SocketAddr {
        self.state.address
    }

    /// Returns the predecessor of this node.
    /// If we do not have a successor, we return ourselves as [`ChordPeer`].
    fn get_predecessor(&self) -> ChordPeer {
        *self
            .state
            .predecessors
            .read()
            .first()
            .unwrap_or(&self.as_chord_peer())
    }

    /// Handle incoming request from a connecting peer.
    async fn accept_peer_connection(&self, mut stream: TcpStream) -> Result<()> {
        let (reader, writer) = stream.split();
        let (mut tx, mut rx) = channels::channel(reader, writer);
        loop {
            match rx.recv().await? {
                PeerMessage::GetNode(id) => {
                    require_proof_of_work(&mut tx, &mut rx, 1).await?;
                    // if we do not have a predecessor we are responsible for all keys
                    // otherwise check if the key is between us and our predecessor in which case we are also responsible
                    if self.am_responsible_for_key(id) {
                        debug!(
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
                    require_proof_of_work(&mut tx, &mut rx, 1).await?;
                    let value = self
                        .state
                        .node_storage
                        .get(&key)
                        .map(|entry| entry.value().0.clone());
                    tx.send(PeerMessage::GetValueResponse(value)).await?;
                }
                PeerMessage::GetPredecessor => {
                    require_proof_of_work(&mut tx, &mut rx, 1).await?;
                    let predecessor = self.get_predecessor();
                    tx.send(PeerMessage::GetPredecessorResponse(predecessor))
                        .await?;
                }
                PeerMessage::SplitRequest(new_peer) => {
                    require_proof_of_work(&mut tx, &mut rx, 2).await?;
                    debug!("{}: Split pred: {}", self.state.address, new_peer.address);
                    let predecessor = {
                        // Acquire write lock for predecessors
                        let mut predecessors = self.state.predecessors.write();

                        // Get first predecessor, and if not present ourselves
                        let had_predecessor = predecessors.len() > 0;
                        let predecessor_or_self = *predecessors.first().unwrap_or(&ChordPeer {
                            id: self.state.node_id,
                            address: self.state.address,
                        });

                        // Check if the new peer is inbetween our predecessor and ourselves
                        // If we the predecessor is us, this method also returns true as we then control all keys and are alone
                        if is_between_on_ring(
                            new_peer.id,
                            predecessor_or_self.id,
                            self.state.node_id,
                        ) {
                            // Special case where we did not have a predecessor before
                            if !had_predecessor {
                                // in that case we did control the entire address space and our finger table is filled
                                // With entries pointing to ourselves
                                // Therefore our successor is also our predecessor and we update all entries
                                // with a reference to our predecessor
                                for entry in self.state.finger_table.iter() {
                                    let mut entry = entry.write();

                                    // Only update if it is a reference to ourselves
                                    if entry.id == self.state.node_id {
                                        *entry = new_peer;
                                    }
                                }
                                self.assert_finger_table_invariants_correct();
                            }

                            // insert as new predecessor
                            predecessors.insert(0, new_peer);
                        }
                        predecessor_or_self
                    };

                    if is_between_on_ring(new_peer.id, predecessor.id, self.state.node_id) {
                        let mut values = Vec::new();
                        for entry in self.state.node_storage.iter() {
                            let key = *entry.key();
                            let value = entry.value();
                            if is_between_on_ring(key, predecessor.id, new_peer.id) {
                                values.push((key, value.0.clone()));
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
                    require_proof_of_work(&mut tx, &mut rx, 2).await?;
                    debug!("{} asked to store {:x}", self.state.address, key);
                    self.internal_insert(key, value, ttl).await?;
                }
                PeerMessage::SetSuccessor(successor) => {
                    require_proof_of_work(&mut tx, &mut rx, 2).await?;
                    debug!(
                        "{}: Set Successor: {}",
                        self.state.address, successor.address
                    );

                    // Update finger table
                    // We update all entries which should now point to our successor
                    for (i, entry) in self.state.finger_table.iter().enumerate() {
                        if is_between_on_ring(
                            self.id_at_finger_index(i),
                            self.state.node_id,
                            successor.id,
                        ) {
                            *entry.write() = successor;
                        } else {
                            // Assert that we updated at least one entry, otherwise something is wrong
                            debug_assert_ne!(i, 0);
                            break;
                        }
                    }
                    self.assert_finger_table_invariants_correct();
                    return Ok(());
                }
                PeerMessage::SetPredecessor(supposed_predecessor) => {
                    require_proof_of_work(&mut tx, &mut rx, 2).await?;
                    debug!(
                        "{}: Set Predecessor: {}",
                        self.state.address, supposed_predecessor.address
                    );

                    let mut predecessors = self.state.predecessors.write();
                    assert!(!predecessors.is_empty());
                    let current_predecessor = predecessors.first().unwrap();

                    if current_predecessor.address == supposed_predecessor.address {
                        // do nothing
                        debug!("Predecessor tried to set itself as predecessor");
                    } else {
                        assert_ne!(supposed_predecessor.address, self.state.address);
                        // Check if this predecessor is actually between us and our previous predecessor
                        if is_between_on_ring(
                            supposed_predecessor.id,
                            current_predecessor.id,
                            self.state.node_id,
                        ) {
                            // Insert predecessor at 0 position
                            predecessors.insert(0, supposed_predecessor);
                        } else {
                            debug!("Tried to set invalid predecessor");
                        }
                    }

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

    /// Returns the id which is `2^index` after this node
    fn id_at_finger_index(&self, index: usize) -> u64 {
        self.state.node_id.wrapping_add(2u64.pow(index as u32))
    }

    /// Asserts that we do not contain ourselves in the finger table.
    ///
    /// If a node would contain itself, there is a possibility of endless routing loops.
    fn assert_finger_table_invariants_correct(&self) {
        if cfg!(debug_assertions) {
            for (i, entry) in self.state.finger_table.iter().enumerate() {
                // Checks if the routing table contains ourselves, which should never be the case
                let finger = entry.read();
                if finger.id == self.state.node_id {
                    panic!("Found self in finger table at index {}", i);
                }
            }
        }
    }
}

/// Check if a key is between two keys/nodes on the ring
fn is_between_on_ring(value: u64, lower: u64, upper: u64) -> bool {
    match lower.cmp(&upper) {
        Ordering::Equal => true,
        Ordering::Less => value >= lower && value <= upper,
        Ordering::Greater => value >= lower || value <= upper, // Wrap-around
    }
}

/// Sends a [`ProofOfWorkChallenge`] to the connected peer
/// and waits for the corresponding [`peer_messages::ProofOfWorkResponse`].
async fn require_proof_of_work<'a>(
    tx: &mut channels::Sender<PeerMessage, WriteHalf<'a>, Bincode>,
    rx: &mut channels::Receiver<PeerMessage, ReadHalf<'a>, Bincode>,
    difficulty: usize,
) -> Result<()> {
    let challenge = ProofOfWorkChallenge::new(difficulty);
    tx.send(PeerMessage::ProofOfWorkChallenge(challenge))
        .await?;
    trace!(
        "Sending proof of work challenge of difficulty {} to {}",
        difficulty,
        tx.get().peer_addr()?
    );
    if let PeerMessage::ProofOfWorkResponse(r) = rx.recv().await? {
        return if challenge.check(r) {
            Ok(())
        } else {
            Err(anyhow!("Invalid proof of work"))
        };
    }
    Err(anyhow!("Invalid response to proof of work challenge"))
}

/// Solves a [`ProofOfWorkChallenge`]
/// and sends the corresponding [`peer_messages::ProofOfWorkResponse`].
async fn solve_proof_of_work(
    tx: &mut channels::Sender<PeerMessage, OwnedWriteHalf, Bincode>,
    rx: &mut channels::Receiver<PeerMessage, OwnedReadHalf, Bincode>,
) -> Result<()> {
    let challenge = match rx.recv().await? {
        PeerMessage::ProofOfWorkChallenge(challenge) => challenge,
        _ => return Err(anyhow!("Invalid message")),
    };
    trace!(
        "Received proof of work challenge of difficulty {} from {}",
        challenge.difficulty,
        rx.get().peer_addr()?
    );
    let response = challenge.solve();
    trace!(
        "Solved proof of work challenge of difficulty {} from {}",
        challenge.difficulty,
        rx.get().peer_addr()?
    );
    tx.send(PeerMessage::ProofOfWorkResponse(response)).await?;
    Ok(())
}

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

/// Simple test case for the [`is_between_on_ring`] method
#[test]
fn test_is_between_on_ring() {
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
