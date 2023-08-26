use std::net::SocketAddr;
use std::time::Duration;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Copy, Clone)]
pub struct ChordPeer {
    pub(crate) id: u64,
    pub(crate) address: SocketAddr,
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) enum PeerMessage {
    GetNode(u64),
    GetNodeResponse(ChordPeer),
    GetValue(u64),
    GetValueResponse(Option<Vec<u8>>),
    InsertValue(u64, Vec<u8>, Duration),
    SplitRequest(ChordPeer),
    SplitResponse(SplitResponse),
    GetPredecessor,
    GetPredecessorResponse(ChordPeer),
    SetPredecessor(ChordPeer),
    SetSuccessor(ChordPeer),
    CloseConnection,
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) enum SplitResponse {
    Success(Vec<(u64, Vec<u8>)>),
    Failure(ChordPeer), // Predecessor that is responsible instead
}
