use std::net::SocketAddr;
use std::time::Duration;

use rand::random;
use serde::{Deserialize, Serialize};
use sha3::Digest;
use sha3::Sha3_512;

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
    ProofOfWorkChallenge(ProofOfWorkChallenge),
    ProofOfWorkResponse(ProofOfWorkResponse),
    CloseConnection,
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) enum SplitResponse {
    Success(Vec<(u64, Vec<u8>)>),
    Failure(ChordPeer), // Predecessor that is responsible instead
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone)]
pub(crate) struct ProofOfWorkChallenge {
    nonce: u128,
    pub(crate) difficulty: usize,
}

impl ProofOfWorkChallenge {
    pub(crate) fn new(difficulty: usize) -> Self {
        Self {
            nonce: random(),
            difficulty,
        }
    }
    pub(crate) fn solve(&self) -> ProofOfWorkResponse {
        let mut hasher = Sha3_512::new();
        hasher.update(self.nonce.to_le_bytes());

        loop {
            let mut hasher = hasher.clone();
            let random_number = random::<u128>();
            hasher.update(random_number.to_le_bytes());
            if hasher
                .finalize()
                .as_slice()
                .iter()
                .take(self.difficulty)
                .all(|byte| *byte == 0)
            {
                return ProofOfWorkResponse {
                    solution: random_number,
                };
            }
        }
    }

    pub(crate) fn check(&self, response: ProofOfWorkResponse) -> bool {
        let mut hasher = Sha3_512::new();
        hasher.update(self.nonce.to_le_bytes());
        hasher.update(response.solution.to_le_bytes());
        hasher
            .finalize()
            .as_slice()
            .iter()
            .take(self.difficulty)
            .all(|byte| *byte == 0)
    }
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone)]
pub(crate) struct ProofOfWorkResponse {
    solution: u128,
}

#[test]
fn proof_of_work_test() {
    let challenge = ProofOfWorkChallenge::new(2);
    let response = challenge.solve();
    assert!(challenge.check(response));
    assert!(!challenge.check(ProofOfWorkResponse { solution: 0 }));
}
