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
pub enum PeerMessageEnum {
    PeerHello(PeerHello),
    PeerACK(PeerACK),
}
