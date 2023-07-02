use channels::{Receiver, Sender};
use std::error::Error;
use std::hash::Hash;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::time::Duration;

use crate::s_chord::peer_messages::{JoinRequest, JoinSuccess, PeerMessage};
use dashmap::DashMap;
use num_traits::Bounded;
use serde::Serialize;
use tokio::time::sleep;

pub struct SChord<K: SChordKey, V: SChordValue> {
    state: Arc<SChordState<K, V>>,
}

struct SChordState<K: SChordKey, V: SChordValue> {
    default_store_duration: Duration,
    max_store_duration: Duration,
    local_storage: DashMap<K, V>,
    connected_clients: DashMap<u64, PeerConnection>,
}

struct PeerConnection {
    tx: Sender<PeerMessage, TcpStream>,
    rx: Receiver<PeerMessage, TcpStream>,
    connected_back: bool,
}

impl<K: SChordKey, V: SChordValue> SChord<K, V> {
    fn start_server_socket(&self, server_address: SocketAddr) {
        let self_clone = SChord {
            state: self.state.clone(),
        };
        tokio::spawn(async move {
            let listener =
                TcpListener::bind(server_address).expect("Failed to bind SChord server socket");
            println!("SChord listening for peers on {}", server_address);
            loop {
                let (stream, connecting_address) = listener.accept().unwrap();
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
        });
    }

    pub fn new(initial_peer: Option<SocketAddr>, server_address: SocketAddr) -> Self {
        let s_chord = SChord {
            state: Arc::new(SChordState {
                default_store_duration: Duration::from_secs(60),
                max_store_duration: Duration::from_secs(600),
                local_storage: DashMap::new(),
                connected_clients: DashMap::new(),
            }),
        };
        s_chord.start_server_socket(server_address);
        if let Some(initial_peer) = initial_peer {
            let s_chord = SChord {
                state: s_chord.state.clone(),
            };
            tokio::spawn(async move {
                s_chord
                    .join(initial_peer, server_address.port())
                    .await
                    .expect("Failed to join SChord network");
            });
        }
        s_chord
    }

    pub async fn insert(&self, key: K, value: V) {
        self.insert_with_ttl(key, value, self.state.default_store_duration)
            .await;
    }
    pub async fn insert_with_ttl(&self, key: K, value: V, ttl: Duration) {}

    pub async fn get(&self, key: &K) -> Option<&V> {
        None
    }

    /// Ask initial peer to join us into the network
    async fn join(
        &self,
        initial_peer: SocketAddr,
        our_port_number: u16,
    ) -> Result<(), Box<dyn Error>> {
        let stream = TcpStream::connect(initial_peer)?;
        let (mut tx, mut rx) = channels::channel(stream.try_clone()?, stream);
        tx.send(PeerMessage::JoinRequest(JoinRequest {
            my_port_number: our_port_number,
        }))?;
        let answer: PeerMessage = rx.recv()?;
        match answer {
            PeerMessage::JoinSuccess(message) => {
                println!("Joined at position {}", message.assigned_position);
                let connection = PeerConnection {
                    tx,
                    rx,
                    connected_back: false,
                };
                self.state
                    .connected_clients
                    .insert(message.assigned_position, connection);
                Ok(())
            }
            _ => {
                panic!("Unexpected message type");
            }
        }
    }

    /// Handle incoming requests from peers
    async fn accept_peer_connection(
        &self,
        stream: TcpStream,
        connecting_address: SocketAddr,
    ) -> Result<(), Box<dyn Error>> {
        let (mut tx, mut rx) = channels::channel(stream.try_clone()?, stream);
        loop {
            let request: PeerMessage = rx.recv()?;
            match request {
                PeerMessage::JoinRequest(jr) => {
                    // todo: prevent double joins
                    let assigned_position = 0; // todo: calculate this
                    tx.send(PeerMessage::JoinSuccess(JoinSuccess { assigned_position }))?;
                    sleep(Duration::from_millis(100)).await; // Client has 100ms to prepare for connect back
                    todo!("Connect back to client");
                }
                PeerMessage::JoinConnectBackRequest(cbr) => {
                    match self.state.connected_clients.get_mut(&cbr.id) {
                        Some(mut entry) => {
                            if entry.connected_back {
                                tx.send(PeerMessage::JoinConnectBackFailure)?;
                                return Ok(());
                            } else {
                                entry.connected_back = true;
                                tx.send(PeerMessage::JoinConnectBackSuccess)?;
                            }
                        }
                        None => {
                            tx.send(PeerMessage::JoinConnectBackFailure)?;
                            return Ok(());
                        }
                    }
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
