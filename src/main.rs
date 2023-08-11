#![feature(async_closure)]

use std::collections::hash_map::DefaultHasher;
use std::env;
use std::error::Error;
use std::hash::{Hash, Hasher};
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::time::Duration;

use bincode::Options;
use ini::ini;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use distributed_hash_table::s_chord::s_chord::SChord;

use crate::api_communication::with_big_endian;

mod api_communication;

const API_DHT_PUT: u16 = 650;
const API_DHT_GET: u16 = 651;
const API_DHT_SUCCESS: u16 = 652;
const API_DHT_FAILURE: u16 = 653;
const API_DHT_SHUTDOWN: u16 = 654;

#[derive(Serialize, Deserialize, Debug)]
struct ApiPacketHeader {
    size: u16,
    message_type: u16,
}

enum ApiPacketMessage {
    Put(DhtPut),
    Get(DhtGet),
    Failure(DhtFailure),
    Success(DhtSuccess),
    Shutdown,
    Unparsed(Vec<u8>),
}

#[derive(Deserialize, Debug)]
struct DhtPut {
    ttl: u16,
    _replication: u8,
    _reserved: u8,
    key: [u8; 32],
    value: Vec<u8>,
}

#[derive(Deserialize, Debug)]
struct DhtGet {
    key: [u8; 32],
}

#[derive(Serialize, Deserialize, Debug)]
struct DhtSuccess {
    key: [u8; 32],
    value: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug)]
struct DhtFailure {
    key: [u8; 32],
}

struct ApiPacket {
    header: ApiPacketHeader,
    message: ApiPacketMessage,
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
                            return Err("Invalid size".into());
                        }
                        self.message = ApiPacketMessage::Put(with_big_endian().deserialize(v)?);
                    }
                    API_DHT_GET => {
                        if self.header.size != 4 + 32 {
                            return Err("Invalid size".into());
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
                        if self.header.size != 4 + 32 {
                            return Err("Invalid size".into());
                        }
                        self.message = ApiPacketMessage::Failure(with_big_endian().deserialize(v)?);
                    }
                    API_DHT_SUCCESS => {
                        if self.header.size != 4 + 32 {
                            return Err("Invalid size".into());
                        }
                        self.message = ApiPacketMessage::Success(with_big_endian().deserialize(v)?);
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

struct P2pDht {
    default_store_duration: Duration,
    max_store_duration: Duration,
    public_server_address: SocketAddr,
    api_address: SocketAddr,
    dht: SChord,
    server_thread: JoinHandle<()>,
}

impl P2pDht {
    async fn new(
        default_store_duration: Duration,
        max_store_duration: Duration,
        public_server_address: SocketAddr,
        api_address: SocketAddr,
        initial_peer: Option<SocketAddr>,
    ) -> Self {
        let chord = SChord::new(initial_peer, public_server_address).await;
        let thread = chord.start_server_socket(public_server_address);
        P2pDht {
            default_store_duration,
            max_store_duration,
            public_server_address,
            api_address,
            dht: chord,
            server_thread: thread,
        }
    }

    fn hash_vec_bytes(vec_bytes: &[u8]) -> u64 {
        let mut hasher = DefaultHasher::new();
        vec_bytes.hash(&mut hasher);
        hasher.finish()
    }

    async fn put(&self, put: DhtPut) {
        let hashed_key = P2pDht::hash_vec_bytes(&put.key);
        self.dht
            .insert_with_ttl(hashed_key, put.value, Duration::from_secs(put.ttl as u64))
            .await;
    }
    async fn get(&self, get: &DhtGet, response_stream: &Arc<Mutex<OwnedWriteHalf>>) {
        let hashed_key = P2pDht::hash_vec_bytes(&get.key);
        match self.dht.get(hashed_key).await {
            Some(value) => {
                let header = ApiPacketHeader {
                    size: 4 + get.key.len() as u16 + value.len() as u16,
                    message_type: API_DHT_SUCCESS,
                };
                let mut buf = with_big_endian().serialize(&header).unwrap();
                buf.extend(get.key);
                buf.extend(value);

                if let Err(e) = response_stream.lock().await.write_all(&buf).await {
                    eprintln!("Error writing to socket: {}", e);
                }
            }
            None => {
                let header = ApiPacketHeader {
                    size: 4 + get.key.len() as u16,
                    message_type: API_DHT_FAILURE,
                };
                let mut buf = with_big_endian().serialize(&header).unwrap();
                buf.extend(get.key);

                if let Err(e) = response_stream.lock().await.write_all(&buf).await {
                    eprintln!("Error writing to socket: {}", e);
                }
            }
        }
    }
}

async fn create_dht_from_command_line_arguments() -> P2pDht {
    let args = env::args().collect::<Vec<String>>();
    assert!(args.len() >= 2);
    assert_eq!(args[1], "-c");
    let config = ini!(&args[2]);
    let default_store_duration = Duration::from_secs(
        config["dht"]["default_store_duration"]
            .clone()
            .unwrap()
            .parse()
            .unwrap(),
    );
    let max_store_duration = Duration::from_secs(
        config["dht"]["max_store_duration"]
            .clone()
            .unwrap()
            .parse()
            .unwrap(),
    );

    let p2p_address = config["dht"]["p2p_address"]
        .clone()
        .unwrap()
        .to_socket_addrs()
        .unwrap()
        .next()
        .unwrap();
    let api_address = config["dht"]["api_address"]
        .clone()
        .unwrap()
        .to_socket_addrs()
        .unwrap()
        .next()
        .unwrap();

    // todo
    let initial_peer = None;

    P2pDht::new(
        default_store_duration,
        max_store_duration,
        p2p_address,
        api_address,
        initial_peer,
    )
    .await
}

async fn start_dht(dht: P2pDht) -> Result<(), Box<dyn Error>> {
    let api_listener = TcpListener::bind(dht.api_address).await?;
    println!("Listening for API Calls on {}", dht.api_address);
    let dht = Arc::new(dht);
    loop {
        let (stream, _socket_address) = api_listener.accept().await?;
        let dht = Arc::clone(&dht);
        let (mut reader, writer) = stream.into_split();
        let writer = Arc::new(Mutex::new(writer));
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
                                    packet.header = header_success;
                                } else {
                                    return Err("Could not deserialize header".into());
                                }
                            }
                        } else {
                            packet.parse(*byte)?;
                            match packet.message {
                                ApiPacketMessage::Put(p) => {
                                    let dht = dht.clone();
                                    tokio::spawn(async move {
                                        dht.put(p).await;
                                    });
                                    header_bytes.clear();
                                    packet = ApiPacket::default();
                                }
                                ApiPacketMessage::Get(g) => {
                                    let dht = dht.clone();
                                    let writer = writer.clone();
                                    tokio::spawn(async move {
                                        dht.get(&g, &writer).await;
                                    });
                                    header_bytes.clear();
                                    packet = ApiPacket::default();
                                }
                                ApiPacketMessage::Shutdown => {
                                    // todo: shutdown dht server
                                    return Ok(());
                                }
                                ApiPacketMessage::Unparsed(_) => {}
                                _ => {}
                            }
                        }
                    }
                }
            };
            if let Err(e) = connection_result().await {
                eprintln!("Error on connection: {}", e)
            }
        });
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // todo: RPS communication for bootstrap peers or get from config file
    start_dht(create_dht_from_command_line_arguments().await).await
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::time::Duration;

    use bincode::Options;
    use serde::Deserialize;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tokio::time::sleep;

    use crate::api_communication::with_big_endian;
    use crate::{start_dht, ApiPacketHeader, DhtFailure, P2pDht, API_DHT_GET, API_DHT_SHUTDOWN};

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_main() {
        let dht_0 = P2pDht::new(
            Duration::from_secs(60),
            Duration::from_secs(60),
            "127.0.0.1:40000".parse::<SocketAddr>().unwrap(),
            "127.0.0.1:3000".parse::<SocketAddr>().unwrap(),
            None,
        )
        .await;

        let handle_0 = tokio::spawn(async move {
            start_dht(dht_0).await.unwrap();
        });

        let dht_1 = P2pDht::new(
            Duration::from_secs(60),
            Duration::from_secs(60),
            "127.0.0.1:40001".parse::<SocketAddr>().unwrap(),
            "127.0.0.1:3000".parse::<SocketAddr>().unwrap(),
            Some("127.0.0.1:40000".parse::<SocketAddr>().unwrap()),
        )
        .await;

        let handle_1 = tokio::spawn(async move {
            start_dht(dht_1).await.unwrap();
        });

        handle_0.await.unwrap();
        handle_1.await.unwrap();
    }

    #[derive(Deserialize, Debug)]
    struct Failure {
        header: ApiPacketHeader,
        payload: DhtFailure,
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_api_get() {
        let dht_0 = P2pDht::new(
            Duration::from_secs(60),
            Duration::from_secs(60),
            "127.0.0.1:40000".parse::<SocketAddr>().unwrap(),
            "127.0.0.1:3000".parse::<SocketAddr>().unwrap(),
            None,
        )
        .await;

        let handle_0 = tokio::spawn(async move {
            start_dht(dht_0).await.unwrap();
        });

        // Wait for socket to open
        sleep(Duration::from_secs(1)).await;

        let mut stream = TcpStream::connect("127.0.0.1:3000").await.unwrap();

        println!("Connected to API");

        let key = [0x1; 32];
        let header = ApiPacketHeader {
            size: 4 + key.len() as u16,
            message_type: API_DHT_GET,
        };

        let mut buf = with_big_endian().serialize(&header).unwrap();
        buf.extend(key);

        println!("Sending get request");
        stream.write_all(&buf).await.unwrap();
        println!("Awaiting answer to get request");
        let bytes_read = stream.read(&mut buf).await.unwrap();

        let received_data: Failure = bincode::deserialize(&buf[..bytes_read]).unwrap();

        println!("Data received: {:?}", received_data);

        let buf = with_big_endian()
            .serialize(&ApiPacketHeader {
                size: 4,
                message_type: API_DHT_SHUTDOWN,
            })
            .unwrap();

        stream.write_all(&buf).await.unwrap();

        handle_0.abort();

        assert!(handle_0.await.unwrap_err().is_cancelled())
    }
}
