use crate::api_communication::get_bincode_options;
use bincode::{Error, Options};
use std::net::SocketAddr;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

mod peer_messages;
use crate::peer_communication::peer_messages::PeerMessageEnum::JoinSuccessfulEnum;
use crate::peer_communication::peer_messages::{JoinRequest, JoinSuccessful, PeerMessageEnum};

async fn send_message(mut stream: &TcpStream, message: PeerMessageEnum) {
    stream
        .write_all(&*get_bincode_options().serialize(&message).unwrap())
        .await?
}

async fn receive_message(mut stream: &TcpStream) -> Result<PeerMessageEnum, Error> {
    // Receiving response
    let mut buffer = vec![0; 1024];
    let bytes_read = stream.read(&mut buffer).await.unwrap();

    // Trimming the buffer to the actual received bytes
    buffer.resize(bytes_read, 0);

    let result: Result<PeerMessageEnum, _> = get_bincode_options().deserialize(&*buffer);
    return result;
}

pub async fn join(initial_peer: SocketAddr) {
    let mut stream = TcpStream::connect(initial_peer).await?;

    // Sending join request
    send_message(&stream, PeerMessageEnum::JoinRequestEnum(JoinRequest {})).await?;

    let answer = receive_message(&stream);
    match answer {
        PeerMessageEnum::JoinSuccessfulEnum(message) => {
            println!("Joined at position {}", message.position);
            todo!()
        }
        _ => {
            panic!()
        }
    }
}

pub async fn handle_incoming_stream(
    mut stream: TcpStream,
) -> Result<(), Box<dyn std::error::Error>> {
    let request = receive_message(&stream)?;
    match request {
        PeerMessageEnum::JoinRequestEnum(message) => {
            if (true) {
                send_message(&stream, JoinSuccessfulEnum(JoinSuccessful { position: 1 })).await?
            } else {
                todo!()
            }
        }
        _ => {
            panic!()
        }
    }

    Ok(())
}
