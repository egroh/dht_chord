#![feature(async_closure)]

use std::error::Error;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};

enum Message {
    Put(DhtPut),
    Get(DhtGet),
    Success,
    Failure,
}

struct DhtPut {
    key: Vec<u8>,
    value: Vec<u8>,
}

struct DhtGet {
    key: Vec<u8>,
}

struct DhtSuccess {
    key: Vec<u8>,
    value: Vec<u8>,
}

struct DhtFailure {
    key: Vec<u8>,
}

struct Dht {
    data: (),
}

impl Dht {
    fn put(&self, key: &Vec<u8>, value: &Vec<u8>) {}
    fn get(&self, key: &Vec<u8>, response_socket: &mut TcpStream) -> Result<(), Box<dyn Error>> {
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let dht = Arc::new(Dht { data: () });
    let listener = TcpListener::bind("127.0.0.1:13337").await?;
    loop {
        let (mut socket, _) = listener.accept().await?;
        let dht = Arc::clone(&dht);
        tokio::spawn(async move {
            let connection_result = async move || -> Result<(), Box<dyn Error>> {
                let mut buf = [0; 1024];

                let mut size_bytes = Vec::new();
                let mut message_type_bytes = Vec::new();

                let mut size = 0;
                let mut message_type: Option<Message> = None;

                // Read data from socket
                loop {
                    let n = match socket.read(&mut buf).await {
                        // socket closed
                        Ok(n) if n == 0 => return Ok(()),
                        Ok(n) => n,
                        Err(e) => return Err(e.into()),
                    };
                    for byte in &buf[0..n] {
                        // Read size
                        if size_bytes.len() < 2 {
                            size_bytes.push(*byte);
                        // Read type
                        } else if message_type_bytes.len() < 2 {
                            message_type_bytes.push(*byte);
                            // Parse size + type
                            if message_type_bytes.len() == 2 {
                                size = u16::from_be_bytes([size_bytes[0], size_bytes[1]]);
                                let message_type_number = u16::from_be_bytes([
                                    message_type_bytes[0],
                                    message_type_bytes[1],
                                ]);
                                match message_type_number {
                                    // DHT PUT
                                    650 => {
                                        message_type = Some(Message::Put(DhtPut {
                                            key: Vec::new(),
                                            value: Vec::new(),
                                        }));
                                        if size < 4 + 4 + 8 + 1 {
                                            return Err("Invalid size".into());
                                        }
                                    }
                                    // DHT GET
                                    651 => {
                                        message_type =
                                            Some(Message::Get(DhtGet { key: Vec::new() }));
                                        if size != 4 + 8 {
                                            return Err("Invalid size".into());
                                        }
                                    }
                                    _ => return Err("Unknown message type".into()),
                                }
                                size -= 4;
                            }
                        } else {
                            size -= 1;
                            match &mut message_type {
                                Some(Message::Put(p)) => {
                                    if p.key.len() < 32 {
                                        p.key.push(*byte);
                                    } else if size > 0 {
                                        p.value.push(*byte);
                                    } else {
                                        dht.put(&p.key, &p.value);
                                    }
                                }
                                Some(Message::Get(g)) => {
                                    if g.key.len() < 32 {
                                        g.key.push(*byte);
                                    } else {
                                        dht.get(&g.key, &mut socket)?;
                                    }
                                }
                                _ => {
                                    unreachable!()
                                }
                            }
                            if size == 0 {
                                size_bytes.clear();
                                message_type_bytes.clear();
                                message_type = None;
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
