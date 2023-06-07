#![feature(async_closure)]

use bincode::Options;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
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

#[derive(Serialize, Deserialize, Debug)]
struct DhtFailure {
    size: u16,
    message_type: u16,
    key: [u8; 32],
}

struct DhtFailure2 {
    key: Vec<u8>,
}

struct Dht {
    data: (),
}

impl Dht {
    fn put(&self, key: &Vec<u8>, value: &Vec<u8>) {
        // Convert bytes into ASCII String for better readability
        let ascii_key: String = key.iter().map(|&byte| byte as char).collect();
        let ascii_value: String = value.iter().map(|&byte| byte as char).collect();
        // Print debug information
        println!("DHT PUT {:?}, {:?}", ascii_key, ascii_value);
    }
    async fn get(
        &self,
        key: &Vec<u8>,
        response_socket: &mut TcpStream,
    ) -> Result<(), Box<dyn Error>> {
        // Convert bytes into ASCII String for better readability
        let ascii_key: String = key.iter().map(|&byte| byte as char).collect();
        // Print debug information
        println!("DHT GET {:?}", ascii_key);

        // Ensure the vector contains exactly 32 elements
        if key.len() != 32 {
            // Maybe dont panic here, convert other code such that this never happens
            panic!("Key size must be exactly 32 bytes");
        }

        // Convert the vector reference to an array
        let key_array: &[u8; 32] = {
            let key_slice = key.as_slice();

            // This will cause a panic if the vector size is not exactly 32
            let key_arr = key_slice.try_into().unwrap();

            key_arr
        };

        let encode_options = bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .with_big_endian();

        // Answer with failure for now, every time
        let response = encode_options
            .serialize(&DhtFailure {
                size: 4 + 32,
                message_type: 653,
                key: *key_array,
            })
            .unwrap();
        response_socket.write_all(&response).await?;

        println!("Sending {:?}", response);

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
                                        if size != 4 + 32 {
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
                                    if g.key.len() < 31 {
                                        g.key.push(*byte);
                                    } else {
                                        g.key.push(*byte);
                                        dht.get(&g.key, &mut socket).await?;
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
