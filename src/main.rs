#![feature(async_closure)]

use std::env;
use std::error::Error;

use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::time::Duration;

use crate::chord::SChord;

use env_logger::Env;
use ini::ini;

use tokio::net::TcpListener;
use tokio::task::JoinHandle;

mod api_communication;
mod chord;

struct P2pDht {
    default_store_duration: Duration,
    max_store_duration: Duration,
    public_server_address: SocketAddr,
    api_address: SocketAddr,
    dht: SChord,
    server_thread: JoinHandle<()>,
}

impl P2pDht {
    async fn new(
        default_store_duration: Duration,
        max_store_duration: Duration,
        public_server_address: SocketAddr,
        api_address: SocketAddr,
        initial_peer: Option<SocketAddr>,
    ) -> Self {
        let chord = SChord::new(initial_peer, public_server_address).await;
        let thread = chord.start_server_socket(public_server_address).await;
        P2pDht {
            default_store_duration,
            max_store_duration,
            public_server_address,
            api_address,
            dht: chord,
            server_thread: thread,
        }
    }
}

async fn create_dht_from_command_line_arguments() -> Arc<P2pDht> {
    let args = env::args().collect::<Vec<String>>();
    assert!(args.len() >= 2, "Usage: {} -c <config>", args[0]);
    assert_eq!(args[1], "-c", "Usage: {} -c <config>", args[0]);
    let config = ini!(&args[2]);
    let default_store_duration = Duration::from_secs(
        config["dht"]["default_store_duration"]
            .clone()
            .unwrap()
            .parse()
            .unwrap(),
    );
    let max_store_duration = Duration::from_secs(
        config["dht"]["max_store_duration"]
            .clone()
            .unwrap()
            .parse()
            .unwrap(),
    );

    let p2p_address = config["dht"]["p2p_address"]
        .clone()
        .unwrap()
        .to_socket_addrs()
        .unwrap()
        .next()
        .unwrap();
    let api_address = config["dht"]["api_address"]
        .clone()
        .unwrap()
        .to_socket_addrs()
        .unwrap()
        .next()
        .unwrap();

    // todo
    let initial_peer = None;

    Arc::new(
        P2pDht::new(
            default_store_duration,
            max_store_duration,
            p2p_address,
            api_address,
            initial_peer,
        )
        .await,
    )
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // todo: RPS communication for bootstrap peers or get from config file

    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    let dht = create_dht_from_command_line_arguments().await;
    let api_listener = TcpListener::bind(dht.api_address).await.unwrap();
    api_communication::start_dht(dht.dht.clone(), dht.api_address, api_listener).await
}

#[cfg(test)]
mod testing;
