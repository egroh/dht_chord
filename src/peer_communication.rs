use crate::api_communication::get_bincode_options;
use bincode::Options;
use std::net::SocketAddr;
use std::thread;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

mod peer_messages;
use crate::peer_communication::peer_messages::{PeerACK, PeerHello, PeerMessageEnum};

pub async fn send_and_receive() {
    let mut stream1 = TcpStream::connect("127.0.0.1:40000").await;
    let mut stream = stream1.unwrap();

    // Sending bytes
    let message = PeerMessageEnum::PeerHello(PeerHello {
        message: "This is a question".parse().unwrap(),
    });
    stream
        .write_all(&*get_bincode_options().serialize(&message).unwrap())
        .await;

    // Receiving response
    let mut buffer = vec![0; 1024];
    let bytes_read = stream.read(&mut buffer).await.unwrap();

    // Trimming the buffer to the actual received bytes
    buffer.resize(bytes_read, 0);

    let result: Result<PeerMessageEnum, _> = get_bincode_options().deserialize(&*buffer);
    match result {
        Ok(peer_message_enum) => match peer_message_enum {
            PeerMessageEnum::PeerACK(ack) => {
                println!("Received ack {}", ack.message);
            }
            _ => {}
        },
        Err(err) => {
            panic!();
        }
    }
}

pub async fn start_peer_server(
    server_address: SocketAddr,
) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(&server_address).await?;
    println!("Peer Server listening on {}", server_address);

    loop {
        let (mut socket, _) = listener.accept().await?;
        tokio::spawn(async move {
            if let Err(err) = handle_connection(&mut socket).await {
                eprintln!("Connection error: {}", err);
            }
        });
    }
}

async fn handle_connection(socket: &mut TcpStream) -> Result<(), Box<dyn std::error::Error>> {
    let mut buffer = [0u8; 1024];
    let bytes_read = socket.read(&mut buffer).await?;
    let request_bytes = &buffer[..bytes_read];

    let result: Result<PeerMessageEnum, _> = get_bincode_options().deserialize(request_bytes);
    match result {
        Ok(peer_message_enum) => match peer_message_enum {
            PeerMessageEnum::PeerHello(_) => {
                let answer = PeerMessageEnum::PeerACK(PeerACK {
                    message: "This is an answer".parse().unwrap(),
                });

                let response = socket
                    .write_all(&*get_bincode_options().serialize(&answer).unwrap())
                    .await?;
            }
            _ => {}
        },
        Err(err) => {
            panic!();
        }
    }

    Ok(())
}
