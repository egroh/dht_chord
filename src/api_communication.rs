use std::collections::hash_map::DefaultHasher;
use std::error::Error;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bincode::config::{BigEndian, FixintEncoding, WithOtherEndian, WithOtherIntEncoding};
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

use crate::chord::SChord;

pub(crate) const API_DHT_PUT: u16 = 650;
pub const API_DHT_GET: u16 = 651;
const API_DHT_SUCCESS: u16 = 652;
const API_DHT_FAILURE: u16 = 653;
pub(crate) const API_DHT_SHUTDOWN: u16 = 654;

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct ApiPacketHeader {
    pub(crate) size: u16,
    pub(crate) message_type: u16,
}

enum ApiPacketMessage {
    Put(DhtPut),
    Get(DhtGet),
    Failure(DhtGetFailure),
    Success(DhtGetResponse),
    Shutdown,
    Unparsed(Vec<u8>),
}

#[derive(Debug)]
pub(crate) struct DhtPut {
    pub(crate) ttl: u16,
    pub(crate) replication: u8,
    pub(crate) reserved: u8,
    pub(crate) key: [u8; 32],
    pub(crate) value: Vec<u8>,
}

#[derive(Deserialize, Debug)]
struct DhtGet {
    key: [u8; 32],
}

#[derive(Debug)]
struct DhtGetResponse {
    key: [u8; 32],
    value: Vec<u8>,
}

#[derive(Serialize, Debug)]
struct DhtGetFailure {
    key: [u8; 32],
}

struct ApiPacket {
    header: ApiPacketHeader,
    message: ApiPacketMessage,
}

pub(crate) fn with_big_endian(
) -> WithOtherEndian<WithOtherIntEncoding<DefaultOptions, FixintEncoding>, BigEndian> {
    DefaultOptions::new()
        .with_fixint_encoding()
        .with_big_endian()
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
                            reserved,
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
                        self.message = ApiPacketMessage::Get(with_big_endian().deserialize(v)?);
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

pub(crate) fn hash_vec_bytes(vec_bytes: &[u8]) -> u64 {
    let mut hasher = DefaultHasher::new();
    vec_bytes.hash(&mut hasher);
    hasher.finish()
}
pub(crate) async fn process_api_put_request(dht: SChord, put: DhtPut) {
    let hashed_key = hash_vec_bytes(&put.key);
    // todo maybe not ignore error
    let _ = dht
        .insert(hashed_key, put.value, Duration::from_secs(put.ttl as u64))
        .await;
}
async fn process_api_get_request(
    dht: SChord,
    get: &DhtGet,
    response_stream: &Arc<Mutex<OwnedWriteHalf>>,
) {
    let hashed_key = hash_vec_bytes(&get.key);
    match dht.get(hashed_key).await {
        Ok(value) => {
            let header = ApiPacketHeader {
                size: 4 + get.key.len() as u16 + value.len() as u16,
                message_type: API_DHT_SUCCESS,
            };
            let mut buf = with_big_endian().serialize(&header).unwrap();
            buf.extend(get.key);
            buf.extend(value);

            if let Err(e) = response_stream.lock().await.write_all(&buf).await {
                warn!("Error writing to socket: {}", e);
            }
        }
        Err(e) => {
            // todo maybe remove this
            warn!("{}", e);

            let header = ApiPacketHeader {
                size: 4 + get.key.len() as u16,
                message_type: API_DHT_FAILURE,
            };
            let mut buf = with_big_endian().serialize(&header).unwrap();
            buf.extend(get.key);

            if let Err(e) = response_stream.lock().await.write_all(&buf).await {
                warn!("Error writing to socket: {}", e);
            }
        }
    }
}
pub(crate) async fn start_api_server(
    dht: SChord,
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

                    tokio::spawn(async move {
                        let connection_result = async move || -> Result<(), Box<dyn Error>> {
                            let mut buf = [0; 1024];

                            let mut header_bytes = Vec::new();
                            let mut packet: ApiPacket = ApiPacket::default();

                            // Read data from socket
                            loop {
                                let n = match reader.read(&mut buf).await {
                                    // socket closed
                                    Ok(n) if n == 0 => return Ok(()),
                                    Ok(n) => n,
                                    Err(e) => return Err(e.into()),
                                };
                                for byte in &buf[0..n] {
                                    if header_bytes.len() < 4 {
                                        header_bytes.push(*byte);
                                        if header_bytes.len() == 4 {
                                            if let Ok(header_success) =
                                                with_big_endian().deserialize(&header_bytes)
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
                                                // todo: shutdown dht server
                                                return Ok(());
                                            }
                                            ApiPacketMessage::Unparsed(_) => {}
                                            _ => return Err("Unknown api package received".into()),
                                        }
                                    }
                                }
                            }
                        };
                        if let Err(e) = connection_result().await {
                            warn!("Error in API connection on port {}: {}", api_address, e)
                        }
                    });
                    } else {
                        // todo: housekeeping thread
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
