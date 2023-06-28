#![feature(async_closure)]

use std::env;
use std::error::Error;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::time::Duration;
mod peer_communication;

use crate::api_communication::get_bincode_options;
use bincode::config::{BigEndian, FixintEncoding, WithOtherEndian, WithOtherIntEncoding};
use bincode::Options;
use ini::ini;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

mod api_communication;
mod dht;

const API_DHT_PUT: u16 = 650;
const API_DHT_GET: u16 = 651;
const API_DHT_SUCCESS: u16 = 652;
const API_DHT_FAILURE: u16 = 653;

#[derive(Serialize, Deserialize, Debug)]
struct ApiPacketHeader {
    size: u16,
    message_type: u16,
}

enum ApiPacketMessage {
    Put(DhtPut),
    Get(DhtGet),
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
                    // DHT PUT
                    API_DHT_PUT => {
                        if self.header.size < 4 + 4 + 32 + 1 {
                            return Err("Invalid size".into());
                        }
                        self.message = ApiPacketMessage::Put(get_bincode_options().deserialize(v)?);
                    }
                    // DHT GET
                    API_DHT_GET => {
                        if self.header.size != 4 + 32 {
                            return Err("Invalid size".into());
                        }
                        self.message = ApiPacketMessage::Get(get_bincode_options().deserialize(v)?);
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

struct Dht {
    default_store_duration: Duration,
    max_store_duration: Duration,
    bind_address: SocketAddr,
    dht: dht::SChord<[u8; 32], Vec<u8>>,
}

impl Dht {
    fn new(
        default_store_duration: Duration,
        max_store_duration: Duration,
        bind_address: SocketAddr,
        initial_peers: &[SocketAddr],
    ) -> Self {
        Dht {
            default_store_duration,
            max_store_duration,
            bind_address,
            dht: dht::SChord::new(initial_peers),
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

fn parse_command_line_arguments() -> (Dht, SocketAddr) {
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

    (
        Dht::new(default_store_duration, max_store_duration, p2p_address, &[]),
        api_address,
    )
}

fn main2() {
    /*
    let my_struct2 = MyStruct::Variant2(Struct2 {
        field3: 20.0,
        field4: false,
    });

    let bytes = bincode_options.serialize(&my_struct2).unwrap();

    let result: Result<MyStruct, _> = bincode_options.deserialize(&bytes);
    match result {
        Ok(my_struct) => {
            // Successfully deserialized the struct
            match my_struct {
                MyStruct::Variant1(struct1) => {
                    println!("Variant1: {:?}", struct1);
                }
                MyStruct::Variant2(struct2) => {
                    println!("Variant2: {:?}", struct2);
                } // ... handle other variants
            }
        }
        Err(err) => {
            // Failed to deserialize the struct
            eprintln!("Deserialization error: {}", err);
        }
    }
    */
}

#[tokio::main]
async fn main() {
    // todo: RPS communication for bootstrap peers or get from config file
    // let (dht, api_address) = parse_command_line_arguments();

    for value in 0..=1 {
        let dht = Dht::new(
            Duration::from_secs(60),
            Duration::from_secs(60),
            format!("{}{}", "127.0.0.1:4000", value)
                .parse::<SocketAddr>()
                .unwrap(),
            &[],
        );

        tokio::spawn(async move {
            let _ = peer_communication::start_peer_server(dht.bind_address).await;
        });
        tokio::spawn(async move {
            let _ = program_main(
                dht,
                format!("{}{}", "127.0.0.1:3000", value)
                    .parse::<SocketAddr>()
                    .unwrap(),
            )
            .await;
        });
        if value == 1 {
            let _ = peer_communication::send_and_receive().await;
        }
    }
}

async fn program_main(dht: Dht, api_address: SocketAddr) -> Result<(), Box<dyn Error>> {
    let dht = Arc::new(dht);
    let listener = TcpListener::bind(api_address).await?;
    loop {
        let (socket, _socket_address) = listener.accept().await?;
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
