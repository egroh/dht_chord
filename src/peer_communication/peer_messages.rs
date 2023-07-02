use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct PeerHello {
    pub(crate) message: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PeerACK {
    pub(crate) message: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct JoinSuccessful {
    pub(crate) position: u128,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum PeerMessage {
    PeerHello(PeerHello),
    PeerACK(PeerACK),
    JoinRequest,
    JoinSuccessful(JoinSuccessful),
}
