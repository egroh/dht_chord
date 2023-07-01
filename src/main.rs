#![feature(async_closure)]

use std::env;
use std::error::Error;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::time::Duration;

use bincode::Options;
use ini::ini;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

use crate::api_communication::get_bincode_options;

mod peer_communication;

mod api_communication;
mod dht;

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

#[derive(Serialize, Debug)]
struct DhtSuccess {
    key: [u8; 32],
    value: Vec<u8>,
}

#[derive(Serialize, Debug)]
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
                        self.message = ApiPacketMessage::Put(get_bincode_options().deserialize(v)?);
                    }
                    API_DHT_GET => {
                        if self.header.size != 4 + 32 {
                            return Err("Invalid size".into());
                        }
                        self.message = ApiPacketMessage::Get(get_bincode_options().deserialize(v)?);
                    }
                    API_DHT_SHUTDOWN => {
                        if self.header.size != 4 {
                            return Err("Invalid size".into());
                        }
                        self.message = ApiPacketMessage::Shutdown;
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
    dht: dht::SChord<[u8; 32], Vec<u8>>,
}

impl P2pDht {
    fn new(
        default_store_duration: Duration,
        max_store_duration: Duration,
        public_server_address: SocketAddr,
        api_address: SocketAddr,
        initial_peer: Option<SocketAddr>,
    ) -> Self {
        P2pDht {
            default_store_duration,
            max_store_duration,
            public_server_address,
            api_address,
            dht: dht::SChord::new(initial_peer, public_server_address),
        }
    }
    async fn put(&self, put: DhtPut) {
        self.dht
            .insert_with_ttl(put.key, put.value, Duration::from_secs(put.ttl as u64))
            .await;
    }
    async fn get(&self, get: &DhtGet, response_socket: &Arc<Mutex<TcpStream>>) {
        let key = &get.key;
        match self.dht.get(key).await {
            Some(value) => {
                let header = ApiPacketHeader {
                    size: 4 + key.len() as u16 + value.len() as u16,
                    message_type: API_DHT_SUCCESS,
                };
                let mut buf = get_bincode_options().serialize(&header).unwrap();
                buf.extend(key);
                buf.extend(value);

                if let Err(e) = response_socket.lock().await.write_all(&buf).await {
                    eprintln!("Error writing to socket: {}", e);
                }
            }
            None => {
                let header = ApiPacketHeader {
                    size: 4 + key.len() as u16,
                    message_type: API_DHT_FAILURE,
                };
                let mut buf = get_bincode_options().serialize(&header).unwrap();
                buf.extend(key);
                if let Err(e) = response_socket.lock().await.write_all(&buf).await {
                    eprintln!("Error writing to socket: {}", e);
                }
            }
        }
    }
}

fn create_dht_from_command_line_arguments() -> P2pDht {
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
}

async fn start_dht(dht: P2pDht) -> Result<(), Box<dyn Error>> {
    let api_listener = TcpListener::bind(dht.api_address).await?;
    let dht = Arc::new(dht);
    loop {
        let (socket, _socket_address) = api_listener.accept().await?;
        let dht = Arc::clone(&dht);
        let socket = Arc::new(Mutex::new(socket));
        tokio::spawn(async move {
            let connection_result = async move || -> Result<(), Box<dyn Error>> {
                let mut buf = [0; 1024];

                let mut header_bytes = Vec::new();
                let mut packet: ApiPacket = ApiPacket::default();

                // Read data from socket
                loop {
                    let n = match socket.lock().await.read(&mut buf).await {
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
                                    get_bincode_options().deserialize(&header_bytes)
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
                                    let socket = socket.clone();
                                    tokio::spawn(async move {
                                        dht.get(&g, &socket).await;
                                    });
                                    header_bytes.clear();
                                    packet = ApiPacket::default();
                                }
                                ApiPacketMessage::Shutdown => {
                                    return Ok(());
                                }
                                ApiPacketMessage::Unparsed(_) => {}
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
    start_dht(create_dht_from_command_line_arguments()).await
}

#[tokio::test]
async fn test_main() {
    let dht_0 = P2pDht::new(
        Duration::from_secs(60),
        Duration::from_secs(60),
        "127.0.0.1:40000".parse::<SocketAddr>().unwrap(),
        "127.0.0.1:3000".parse::<SocketAddr>().unwrap(),
        None,
    );

    let handle_0 = tokio::spawn(async move {
        start_dht(dht_0).await.unwrap();
    });

    let dht_1 = P2pDht::new(
        Duration::from_secs(60),
        Duration::from_secs(60),
        "127.0.0.1:40001".parse::<SocketAddr>().unwrap(),
        "127.0.0.1:3000".parse::<SocketAddr>().unwrap(),
        Some("127.0.0.1:40000".parse::<SocketAddr>().unwrap()),
    );

    let handle_1 = tokio::spawn(async move {
        start_dht(dht_1).await.unwrap();
    });

    handle_0.await.unwrap();
    handle_1.await.unwrap();
}
