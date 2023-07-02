use std::error::Error;
use std::net::{SocketAddr, TcpStream};

use crate::peer_communication::peer_messages::{JoinSuccessful, PeerMessage};

mod peer_messages;

pub async fn join(initial_peer: SocketAddr) -> Result<(), Box<dyn Error>> {
    let stream = TcpStream::connect(initial_peer)?;
    let (mut tx, mut rx) = channels::channel(stream.try_clone()?, stream);
    tx.send(PeerMessage::JoinRequest)?;
    let answer: PeerMessage = rx.recv()?;
    match answer {
        PeerMessage::JoinSuccessful(message) => {
            println!("Joined at position {}", message.position);
            todo!()
        }
        _ => {
            panic!("Unexpected message type");
        }
    }
}

pub async fn accept_peer_connection(stream: TcpStream) -> Result<(), Box<dyn Error>> {
    let (mut tx, mut rx) = channels::channel(stream.try_clone()?, stream);
    loop {
        let request: PeerMessage = rx.recv()?;
        match request {
            PeerMessage::JoinRequest => {
                tx.send(PeerMessage::JoinSuccessful(JoinSuccessful { position: 0 }))?;
            }
            _ => {
                panic!("Unexpected message type");
            }
        }
    }
}
