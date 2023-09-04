//! Provides a server socket for API communication
//!
//! The API-communications module is responsible for communication with other modules.
//! Incoming requests are dispatched to a worker thread pool,
//! that will process each request independently and asynchronously.
//!
//! Answer to messages received are not guaranteed to be sent in the same order as the requests were received.
//!
//! As specified in the assignment for DHT, the following requests are accepted and processed:
//!
//! - DHT PUT
//! - DHT GET
//! - DHT SUCCESS
//! - DHT FAILURE
//!
//!  We introduced an additional API Message, [`API_DHT_SHUTDOWN`] which allows us to shut down a node gracefully through
//!  the API.
//!  The package has a fixed size and does not contain any other information except the header:
//!  ```markdown
//!  +--------+--------+--------+--------+
//!  |        4        |  DHT SHUTDOWN   |
//!  +--------+--------+--------+--------+
//!  ```
//!
//! All [`ApiPacket`] s we receive, are first parsed by their [`ApiPacketHeader`].
//! Depending on the header,
//! we then parse the rest of the message into the corresponding [`ApiPacketMessage`].
use std::collections::hash_map::DefaultHasher;
use std::error::Error;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bincode::{DefaultOptions, Options};
use log::{debug, info, warn};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::chord::Chord;

/// Requests the DHT to store a value
pub const API_DHT_PUT: u16 = 650;

/// Requests the DHT to retrieve a value
pub const API_DHT_GET: u16 = 651;

/// Answer to a successful [`API_DHT_GET`] request
pub const API_DHT_SUCCESS: u16 = 652;

/// Answer to a failed [`API_DHT_GET`] request
pub const API_DHT_FAILURE: u16 = 653;

/// Requests our DHT node to shutdown
///
/// The corresponding packet consists only of a message-header, with the message-type set to `654`.
pub const API_DHT_SHUTDOWN: u16 = 654;

/// Internal representation of all packages received on the API socket
///
/// After 4 header bytes are received and parsed into an [`ApiPacketHeader`],
/// the rest of the message is parsed into the corresponding [`ApiPacketMessage`].
pub struct ApiPacket {
    header: ApiPacketHeader,
    message: ApiPacketMessage,
}
/// Header of an [`ApiPacket`]
#[derive(Serialize, Deserialize, Debug)]
pub struct ApiPacketHeader {
    /// The size indicates the total length of a message, *including* the header
    pub(crate) size: u16,
    /// Indicates the type of the message with a well-known constant
    pub(crate) message_type: u16,
}

/// Content of an [`ApiPacket`]
///
/// Once enough bytes have been received,
/// the message is parsed fom `Unparsed` into the matching request.
pub enum ApiPacketMessage {
    Put(DhtPut),
    Get(DhtGet),
    Shutdown,
    Unparsed(Vec<u8>),
}

#[derive(Debug)]
/// Internal representation of a `DHT_PUT` request
pub struct DhtPut {
    /// Time-to-live of the value in seconds
    pub(crate) ttl: u16,
    /// How many copies of the value should be stored in the DHT
    pub(crate) replication: u8,
    /// Reserved for future use
    pub(crate) _reserved: u8,
    /// Fixed size 256 bit key (we hash all keys into a 64 bit integer before storing them)
    pub(crate) key: [u8; 32],
    /// Value of arbitrary size
    pub(crate) value: Vec<u8>,
}

/// Internal representation of a `DHT_GET` request
#[derive(Deserialize, Debug)]
pub struct DhtGet {
    key: [u8; 32],
}

/// Internal representation of a `DHT_SUCCESS` response
#[derive(Debug)]
pub struct DhtGetSuccess {
    key: [u8; 32],
    value: Vec<u8>,
}

/// Internal representation of a `DHT_FAILURE` response
#[derive(Serialize, Debug)]
pub struct DhtGetFailure {
    key: [u8; 32],
}

impl ApiPacket {
    fn default() -> Self {
        ApiPacket {
            header: ApiPacketHeader {
                size: 0,
                message_type: 0,
            },
            message: ApiPacketMessage::Unparsed(Vec::new()),
        }
    }
    fn parse(&mut self, byte: u8) -> Result<(), Box<dyn Error>> {
        if let ApiPacketMessage::Unparsed(v) = &mut self.message {
            v.push(byte);
            if self.header.size as usize <= v.len() + 4 {
                match self.header.message_type {
                    API_DHT_PUT => {
                        if self.header.size < 4 + 4 + 32 + 1 {
                            return Err(
                                format!["DHT PUT invalid size: {}", self.header.size].into()
                            );
                        }
                        let ttl = u16::from_be_bytes([v[0], v[1]]);
                        let replication = v[2];
                        let reserved = v[3];
                        let key = &v[4..36];
                        let value = v[36..].to_vec();
                        debug_assert!(value.len() == self.header.size as usize - 40);
                        self.message = ApiPacketMessage::Put(DhtPut {
                            ttl,
                            replication,
                            _reserved: reserved,
                            key: key.try_into().unwrap(),
                            value,
                        });
                    }
                    API_DHT_GET => {
                        if self.header.size != 4 + 32 {
                            return Err(
                                format!["DHT GET invalid size: {}", self.header.size].into()
                            );
                        }
                        self.message = ApiPacketMessage::Get(
                            DefaultOptions::new()
                                .with_fixint_encoding()
                                .with_big_endian()
                                .deserialize(v)?,
                        );
                    }
                    API_DHT_SHUTDOWN => {
                        if self.header.size != 4 {
                            return Err("Invalid size".into());
                        }
                        self.message = ApiPacketMessage::Shutdown;
                    }
                    API_DHT_FAILURE => {
                        panic!("We should only send these");
                    }
                    API_DHT_SUCCESS => {
                        panic!("We should only send these");
                    }
                    _ => return Err("Unknown message type".into()),
                }
            }
        } else {
            panic!("Message already parsed");
        }
        Ok(())
    }
}

pub(crate) fn hash_key_bytes(vec_bytes: &[u8]) -> u64 {
    let mut hasher = DefaultHasher::new();
    vec_bytes.hash(&mut hasher);
    hasher.finish()
}
pub(crate) async fn process_api_put_request(dht: Chord, put: DhtPut) {
    let hashed_key = hash_key_bytes(&put.key);
    if let Err(e) = dht
        .insert(
            hashed_key,
            put.value,
            Duration::from_secs(put.ttl as u64),
            put.replication,
        )
        .await
    {
        warn!("Error inserting key {:?} into DHT: {}", &put.key, e);
    }
}
async fn process_api_get_request(
    dht: Chord,
    get: &DhtGet,
    response_stream: &Arc<Mutex<OwnedWriteHalf>>,
) {
    let hashed_key = hash_key_bytes(&get.key);
    match dht.get(hashed_key).await {
        Some(value) => {
            let header = ApiPacketHeader {
                size: 4 + get.key.len() as u16 + value.len() as u16,
                message_type: API_DHT_SUCCESS,
            };
            let mut buf = DefaultOptions::new()
                .with_fixint_encoding()
                .with_big_endian()
                .serialize(&header)
                .unwrap();
            buf.extend(get.key);
            buf.extend(value);

            if let Err(e) = response_stream.lock().await.write_all(&buf).await {
                warn!("Error writing to socket: {}", e);
            }
        }
        None => {
            let header = ApiPacketHeader {
                size: 4 + get.key.len() as u16,
                message_type: API_DHT_FAILURE,
            };
            let mut buf = DefaultOptions::new()
                .with_fixint_encoding()
                .with_big_endian()
                .serialize(&header)
                .unwrap();
            buf.extend(get.key);

            if let Err(e) = response_stream.lock().await.write_all(&buf).await {
                warn!("Error writing to socket: {}", e);
            }
        }
    }
}
pub(crate) async fn start_api_server(
    dht: Chord,
    api_address: SocketAddr,
    cancellation_token: CancellationToken,
) -> JoinHandle<()> {
    let api_listener = TcpListener::bind(api_address)
        .await
        .expect("Failed to bind API server socket");

    // Open channel for inter thread communication
    let (thread_start_tx, mut thread_start_rx) = mpsc::channel(1);

    info!("Listening for API Calls on {}", api_address);
    let handle = tokio::spawn(async move {
        // Send signal that we are running
        thread_start_tx
            .send(true)
            .await
            .expect("Unable to send message");

        loop {
            tokio::select! {
                result = api_listener.accept() => {
                    if let Ok((stream, _)) = result {

                    let (mut reader, writer) = stream.into_split();
                    let writer = Arc::new(Mutex::new(writer));

                    let dht = dht.clone();
                    let cancellation_token_clone = cancellation_token.clone();

                    tokio::spawn(async move {
                        let connection_result = async move || -> Result<(), Box<dyn Error>> {
                            let mut buf = [0; 1024];

                            let mut header_bytes = Vec::new();
                            let mut packet: ApiPacket = ApiPacket::default();

                            // Read data from socket
                            loop {
                                let n = match reader.read(&mut buf).await {
                                    // socket closed
                                    Ok(0) => return Ok(()),
                                    Ok(n) => n,
                                    Err(e) => return Err(e.into()),
                                };
                                for byte in &buf[0..n] {
                                    if header_bytes.len() < 4 {
                                        header_bytes.push(*byte);
                                        if header_bytes.len() == 4 {
                                            if let Ok(header_success) =
                                                DefaultOptions::new().with_fixint_encoding().with_big_endian().deserialize(&header_bytes)
                                            {
                                                debug!("Deserialized header: {:?}", header_success);
                                                packet.header = header_success;
                                            } else {
                                                return Err("Could not deserialize header".into());
                                            }
                                        }
                                    } else {
                                        packet.parse(*byte)?;
                                        match packet.message {
                                            ApiPacketMessage::Put(p) => {
                                                debug!("Received put request: {:?}", p);
                                                let dht = dht.clone();
                                                tokio::spawn(async move {
                                                    process_api_put_request(dht, p).await;
                                                });
                                                header_bytes.clear();
                                                packet = ApiPacket::default();
                                            }
                                            ApiPacketMessage::Get(g) => {
                                                debug!("Received get request: {:?}", g);
                                                let writer = writer.clone();
                                                let dht = dht.clone();
                                                tokio::spawn(async move {
                                                    process_api_get_request(dht, &g, &writer).await;
                                                });
                                                header_bytes.clear();
                                                packet = ApiPacket::default();
                                            }
                                            ApiPacketMessage::Shutdown => {
                                                debug!("Received shutdown request!");
                                                cancellation_token_clone.cancel();
                                                return Ok(());
                                            }
                                            ApiPacketMessage::Unparsed(_) => {}
                                        }
                                    }
                                }
                            }
                        };
                        if let Err(e) = connection_result().await {
                            warn!("Error in API connection on port {}: {}", api_address, e)
                        }
                    });
                    }
                }
                _ = cancellation_token.cancelled() => {
                    info!("{}: Stopped accepting new api connections.", api_address);
                    break;
                }
            }
        }
    });
    // Await thread spawn, to avoid EOF errors because the thread is not ready to accept messages
    thread_start_rx.recv().await.unwrap();

    handle
}
