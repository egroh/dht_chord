use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct JoinRequest {
    pub(crate) my_port_number: u16,
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct JoinSuccess {
    pub(crate) assigned_id: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct JoinConnectBackRequest {
    pub(crate) id: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) enum PeerMessage {
    JoinRequest(JoinRequest),
    JoinSuccess(JoinSuccess),
    JoinFailure,
}
