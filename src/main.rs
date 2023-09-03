#![feature(async_closure)]

use std::env;
use std::error::Error;

use std::net::{SocketAddr, ToSocketAddrs};
use std::time::Duration;

use crate::chord::Chord;

use env_logger::Env;
use ini::ini;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

mod api_communication;
mod chord;

struct P2pDht {
    default_storage_duration: Duration,
    max_storage_duration: Duration,
    public_server_address: SocketAddr,
    api_address: SocketAddr,
    dht: Chord,
    peer_server_thread: Option<JoinHandle<()>>,
    api_server_thread: Option<JoinHandle<()>>,
    housekeeping_thread: Option<JoinHandle<()>>,
    cancellation_token: CancellationToken,
}

impl P2pDht {
    async fn new(
        default_storage_duration: Duration,
        max_storage_duration: Duration,
        public_server_address: SocketAddr,
        api_address: SocketAddr,
        initial_peer: Option<SocketAddr>,
        start_api_server: bool,
        start_housekeeping: bool,
    ) -> Self {
        let cancellation_token = CancellationToken::new();
        let chord = Chord::new(
            initial_peer,
            public_server_address,
            default_storage_duration,
            max_storage_duration,
        )
        .await;
        let peer_server_thread = Some(
            chord
                .start_server_socket(public_server_address, cancellation_token.clone())
                .await,
        );
        let api_server_thread = match start_api_server {
            true => Some(
                api_communication::start_api_server(
                    chord.clone(),
                    api_address,
                    cancellation_token.clone(),
                )
                .await,
            ),
            false => None,
        };
        let housekeeping_thread = match start_housekeeping {
            true => Some(chord.start_housekeeping_thread(&cancellation_token).await),
            false => None,
        };

        P2pDht {
            default_storage_duration,
            max_storage_duration,
            public_server_address,
            api_address,
            dht: chord,
            peer_server_thread,
            api_server_thread,
            housekeeping_thread,
            cancellation_token,
        }
    }

    async fn await_termination(&mut self) {
        self.peer_server_thread
            .as_mut()
            .unwrap()
            .await
            .expect("Did encounter error while awaiting termination of peer server thread");

        match self.api_server_thread.as_mut() {
            None => {}
            Some(api_thread) => {
                api_thread
                    .await
                    .expect("Did encounter error while awaiting termination of api server thread");
            }
        }

        match self.housekeeping_thread.as_mut() {
            None => {}
            Some(thread) => {
                thread.await.expect(
                    "Did encounter error while awaiting termination of housekeeping thread",
                );
            }
        }
    }

    #[cfg(test)]
    fn initiate_shutdown(&self) {
        self.cancellation_token.cancel();
    }
}

async fn create_dht_from_command_line_arguments() -> P2pDht {
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

    P2pDht::new(
        default_store_duration,
        max_store_duration,
        p2p_address,
        api_address,
        initial_peer,
        true,
        true,
    )
    .await
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // todo: RPS communication for bootstrap peers or get from config file

    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    let mut dht = create_dht_from_command_line_arguments().await;

    dht.await_termination().await;

    Ok(())
}

#[cfg(test)]
mod testing;
