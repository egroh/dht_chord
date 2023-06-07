#![feature(async_closure)]

use bincode::{deserialize, Options};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

struct Dht {
    data: (),
}

struct ApiPacket {
    header: ApiPacketHeader,
    message: ApiPacketMessage,
}

#[derive(Serialize, Deserialize, Debug)]
struct ApiPacketHeader {
    size: u16,
    message_type: u16,
}

enum ApiPacketMessage {
    Unparsed(Vec<u8>),
    Put(DhtPut),
    Get(DhtGet),
    Success(DhtFailure),
    Failure(DhtSuccess),
}

#[derive(Deserialize, Debug)]
struct DhtPut {
    ttl: u16,
    replication: u8,
    reserved: u8,
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

impl ApiPacket {
    fn parse(&mut self, byte: u8) -> Result<(), Box<dyn Error>> {
        if let ApiPacketMessage::Unparsed(v) = &mut self.message {
            v.push(byte);
            if self.header.size as usize <= v.len() + 4 {
                match self.header.message_type {
                    // DHT PUT
                    650 => {
                        if self.header.size < 4 + 4 + 32 + 1 {
                            return Err("Invalid size".into());
                        }
                        self.message = ApiPacketMessage::Put(deserialize(v)?);
                    }
                    // DHT GET
                    651 => {
                        if self.header.size != 4 + 32 {
                            return Err("Invalid size".into());
                        }
                        self.message = ApiPacketMessage::Get(deserialize(v)?);
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

impl Dht {
    fn put(&self, put: &DhtPut) {}
    fn get(&self, get: &DhtGet, response_socket: &mut TcpStream) -> Result<(), Box<dyn Error>> {
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let dht = Arc::new(Dht { data: () });
    let listener = TcpListener::bind("127.0.0.1:7401").await?;
    loop {
        let (mut socket, _) = listener.accept().await?;
        let dht = Arc::clone(&dht);
        tokio::spawn(async move {
            let connection_result = async move || -> Result<(), Box<dyn Error>> {
                let mut buf = [0; 1024];

                let mut header_bytes = Vec::new();
                let mut packet: ApiPacket = ApiPacket {
                    header: ApiPacketHeader {
                        size: 0,
                        message_type: 0,
                    },
                    message: ApiPacketMessage::Unparsed(Vec::new()),
                };

                // Read data from socket
                loop {
                    let n = match socket.read(&mut buf).await {
                        // socket closed
                        Ok(n) if n == 0 => return Ok(()),
                        Ok(n) => n,
                        Err(e) => return Err(e.into()),
                    };
                    for byte in &buf[0..n] {
                        if header_bytes.len() < 4 {
                            header_bytes.push(*byte);
                            if header_bytes.len() == 4 {
                                if let Ok(header_success) = deserialize(&header_bytes) {
                                    packet.header = header_success;
                                } else {
                                    return Err("Could not deserialize header".into());
                                }
                            }
                        } else {
                            packet.parse(*byte)?;
                            match &packet.message {
                                ApiPacketMessage::Unparsed(_) => {}
                                // todo: async
                                ApiPacketMessage::Put(p) => {
                                    dht.put(p);
                                }
                                // todo: async
                                ApiPacketMessage::Get(g) => {
                                    dht.get(g, &mut socket)?;
                                }
                                _ => unreachable!("Other package types will not parse"),
                            }
                        }
                    }
                }
            };
            if let Err(e) = connection_result().await {
                println!("Error on connection: {}", e)
            }
        });
    }
}
