use crate::s_chord::peer_messages::{JoinSuccessful, PeerMessage};
use std::error::Error;
use std::net::{SocketAddr, TcpStream};

pub(crate) async fn join(initial_peer: SocketAddr) -> Result<(), Box<dyn Error>> {
    let stream = TcpStream::connect(initial_peer)?;
    let (mut tx, mut rx) = channels::channel(stream.try_clone()?, stream);
    tx.send(PeerMessage::JoinRequest)?;
    let answer: PeerMessage = rx.recv()?;
    match answer {
        PeerMessage::JoinSuccess(message) => {
            println!("Joined at position {}", message.position);
            todo!()
        }
        _ => {
            panic!("Unexpected message type");
        }
    }
}

pub(crate) async fn accept_peer_connection(stream: TcpStream) -> Result<(), Box<dyn Error>> {
    let (mut tx, mut rx) = channels::channel(stream.try_clone()?, stream);
    loop {
        let request: PeerMessage = rx.recv()?;
        match request {
            PeerMessage::JoinRequest => {
                tx.send(PeerMessage::JoinSuccess(JoinSuccessful { position: 0 }))?;
            }
            _ => {
                panic!("Unexpected message type");
            }
        }
    }
}
