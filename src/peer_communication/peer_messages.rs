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
pub struct JoinRequest {
    // No further information needed
}

#[derive(Serialize, Deserialize, Debug)]
pub struct JoinSuccessful {
    pub(crate) position: u128,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum PeerMessageEnum {
    PeerHelloEnum(PeerHello),
    PeerACKEnum(PeerACK),
    JoinRequestEnum(JoinRequest),
    JoinSuccessfulEnum(JoinSuccessful),
}
