use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct PeerHello {
    pub(crate) message: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct PeerACK {
    pub(crate) message: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct JoinSuccessful {
    pub(crate) position: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) enum PeerMessage {
    PeerHello(PeerHello),
    PeerACK(PeerACK),
    JoinRequest,
    JoinSuccess(JoinSuccessful),
    JoinFailure,
    JoinCompletionRequest,
    JoinCompletionSuccess,
    JoinCompletionFailure,
}
