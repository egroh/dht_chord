use std::net::SocketAddr;

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
    InsertValue(u64, Vec<u8>),
    SplitNode(u64),
    GetPredecessor,
    GetPredecessorResponse(ChordPeer),
}
