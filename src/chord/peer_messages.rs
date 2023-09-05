//! Communication between peers
//!
//! We are using a data serialization library called "[channels-rs](https://crates.io/crates/channels)" to serialize and
//! deserialize our data.
//! This library allows us to transmit a struct/enum over TCP directly.
//!
//! Packet layout:
//!
//! ![](https://raw.githubusercontent.com/threadexio/channels-rs/master/spec/assets/packet-diagram-dark.svg)
//!
//! The exact protocol specification can be found [here](https://github.com/threadexio/channels-rs/blob/master/spec/PROTOCOL.md).
//!
//! More importantly, this means that we do *not* have to interact with a raw byte-level TCP socket.
//! Instead, we simply transmit and match a [`PeerMessage`].
use std::net::SocketAddr;
use std::time::Duration;

use rand::random;
use serde::{Deserialize, Serialize};
use sha3::Digest;
use sha3::Sha3_512;

/// Uniquely identifies a peer in our network
#[derive(Serialize, Deserialize, Debug, Copy, Clone)]
pub struct ChordPeer {
    /// Node-ID of the peer (currently a hash of the address a peer announces itself with)
    pub(crate) id: u64,
    /// Address under which we can reach the peer
    pub(crate) address: SocketAddr,
}

/// All communication messages sent between peers
///
/// Whenever we transmit a message between nodes,
/// we transmit this enum and match on it on the receiving end.
#[derive(Serialize, Deserialize, Debug)]
pub enum PeerMessage {
    /// Get node responsible for key
    ///
    /// This is the "backbone message" of our Chord-DHT. It can be sent to *any node* in the network,
    /// which will then respond with the IP address of the node responsible for the given key.
    /// As we can use this message to lookup both keys and nodes,
    /// it is the only message type that needs to propagate recursively through the network.
    GetNode(u64),
    /// Response to [`PeerMessage::GetNode`]
    GetNodeResponse(ChordPeer),

    /// Get value from responsible node
    ///
    /// Retrieves content from *one node and one node only*.
    /// It *must* be sent to the node that is responsible for storing *that specific id*
    /// and will **not** propagate through the network.
    GetValue(u64),
    /// Response to [`PeerMessage::GetValue`]
    GetValueResponse(Option<Vec<u8>>),

    /// Insert value into responsible node
    ///
    /// Inserts content into the network; counterpart to GetValue(id).
    /// It must only be sent to the node responsible for the key that the value is to be stored under.
    InsertValue(u64, Vec<u8>, Duration),

    /// Requests a node to split
    ///
    /// The target node performs some sanity checks,
    /// ensuring that there is no other node between the new node and itself.
    SplitRequest(ChordPeer),
    /// Response to [`PeerMessage::SplitRequest`]
    ///
    /// If a node is between the new node and the target node,
    /// it returns the address of the node between them.
    /// Otherwise it returns a list of values the new node is now responsible for.
    SplitResponse(SplitResponse),

    /// Retrieves the predecessor of a node
    GetPredecessor,
    /// Response to [`PeerMessage::GetPredecessor`]
    GetPredecessorResponse(ChordPeer),

    /// Sets the predecessor of a node
    SetPredecessor(ChordPeer),
    /// Sets the successor of a node
    SetSuccessor(ChordPeer),

    /// Requests a node to solve a [`ProofOfWorkChallenge`]
    ProofOfWorkChallenge(ProofOfWorkChallenge),
    /// Response to [`PeerMessage::ProofOfWorkChallenge`]
    ProofOfWorkResponse(ProofOfWorkResponse),

    /// Signals intent to gracefully close the connection; acting as EOF
    CloseConnection,
}

/// Response to [`PeerMessage::SplitRequest`]
///
/// Contains either a list of key-value pairs the new node is now responsible for,
/// or the node that should be asked to split instead.
#[derive(Serialize, Deserialize, Debug)]
pub enum SplitResponse {
    Success(Vec<(u64, Vec<u8>)>),
    Failure(ChordPeer), // Predecessor that is responsible instead
}

/// [SHA-3-512](https://en.wikipedia.org/wiki/SHA-3) based proof-of-work challenge
///
/// The challenge consists of a difficulty setting and a random nonce.
#[derive(Serialize, Deserialize, Debug, Copy, Clone)]
pub struct ProofOfWorkChallenge {
    nonce: u128,
    pub(crate) difficulty: usize,
}

impl ProofOfWorkChallenge {
    /// Creates a new proof-of-work challenge with the given difficulty.
    pub fn new(difficulty: usize) -> Self {
        Self {
            nonce: random(),
            difficulty,
        }
    }
    /// To solve the challenge, an integer must be found,
    /// that when concatenated with our nonce,
    /// results in an [SHA-3-512](https://en.wikipedia.org/wiki/SHA-3) hash
    /// with `difficulty` leading zero bytes.
    pub fn solve(&self) -> ProofOfWorkResponse {
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

    /// Checks if the given response is a valid solution to this challenge.
    pub fn check(&self, response: ProofOfWorkResponse) -> bool {
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

/// Response to [`PeerMessage::ProofOfWorkChallenge`]
#[derive(Serialize, Deserialize, Debug, Copy, Clone)]
pub struct ProofOfWorkResponse {
    solution: u128,
}

#[test]
fn proof_of_work_test() {
    let challenge = ProofOfWorkChallenge::new(2);
    let response = challenge.solve();
    assert!(challenge.check(response));
    assert!(!challenge.check(ProofOfWorkResponse { solution: 0 }));
}
