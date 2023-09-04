#![feature(async_closure)]
#![allow(dead_code)]

use std::collections::HashMap;
use std::env;
use std::error::Error;

use std::net::{SocketAddr, ToSocketAddrs};
use std::time::Duration;

use crate::chord::Chord;
use anyhow::{anyhow, Result};
use env_logger::Env;
use ini::macro_safe_load;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

type IniMap = HashMap<String, HashMap<String, Option<String>>>;

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
        let peer_server_thread = Some(chord.start_server_socket(cancellation_token.clone()).await);
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
            .expect("Encountered error while awaiting termination of peer server thread");

        match self.api_server_thread.as_mut() {
            None => {}
            Some(api_thread) => {
                api_thread
                    .await
                    .expect("Encountered error while awaiting termination of api server thread");
            }
        }

        match self.housekeeping_thread.as_mut() {
            None => {}
            Some(thread) => {
                thread
                    .await
                    .expect("Encountered error while awaiting termination of housekeeping thread");
            }
        }
    }

    #[cfg(test)]
    fn initiate_shutdown(&self) {
        self.cancellation_token.cancel();
    }
}

fn load_duration(key1: &str, key2: &str, config: &IniMap) -> Result<u64> {
    Ok(config
        .get(key1)
        .ok_or(anyhow!("Category {} not found", key1))?
        .get(key2)
        .ok_or(anyhow!("Key {} not found", key2))?
        .clone()
        .ok_or(anyhow!("Key not found in config"))?
        .parse::<u64>()?)
}

fn load_address(key1: &str, key2: &str, config: &IniMap) -> Result<SocketAddr> {
    config
        .get(key1)
        .ok_or(anyhow!("Category {} not found", key1))?
        .get(key2)
        .ok_or(anyhow!("Key {} not found", key2))?
        .clone()
        .ok_or(anyhow!("Key not found in config"))?
        .to_socket_addrs()?
        .next()
        .ok_or(anyhow!("No socket address could be parsed"))
}

async fn create_dht_from_command_line_arguments() -> Option<P2pDht> {
    let args = env::args().collect::<Vec<String>>();

    if args.len() == 2 {
        println!("Error: Expected two arguments");
        println!("Usage: {} -c <config>", args[0]);
        return None;
    }

    if args[1] != "-c" {
        println!("Error: Unsupported option {}", args[1]);
        println!("Usage: {} -c <config>", args[0]);
        return None;
    }

    let config = match macro_safe_load(&args[2]) {
        Ok(conf) => conf,
        Err(e) => {
            println!("Error: Cannot load file {}", args[2]);
            println!("{}", e);
            return None;
        }
    };

    let default_store_duration = Duration::from_secs(
        match load_duration("dht", "default_store_duration", &config) {
            Ok(value) => value,
            Err(e) => {
                println!("Error: Cannot load \"default_store_duration\" from the config");
                println!("{}", e);
                return None;
            }
        },
    );

    let max_store_duration =
        Duration::from_secs(match load_duration("dht", "max_store_duration", &config) {
            Ok(value) => value,
            Err(e) => {
                println!("Error: Cannot load \"max_store_duration\"  from the config");
                println!("{}", e);
                return None;
            }
        });

    let p2p_address = match load_address("dht", "p2p_address", &config) {
        Ok(value) => value,
        Err(e) => {
            println!("Error: Cannot load \"p2p_address\" from the config");
            println!("{}", e);
            return None;
        }
    };

    let api_address = match load_address("dht", "api_address", &config) {
        Ok(value) => value,
        Err(e) => {
            println!("Error: Cannot load \"api_address\" from the config");
            println!("{}", e);
            return None;
        }
    };

    let initial_peer = config["dht"]
        .get("bootstrap_node")
        .and_then(|address| address.clone().unwrap().to_socket_addrs().unwrap().next());

    Some(
        P2pDht::new(
            default_store_duration,
            max_store_duration,
            p2p_address,
            api_address,
            initial_peer,
            true,
            true,
        )
        .await,
    )
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    match create_dht_from_command_line_arguments().await {
        None => {
            // Nothing to do but terminate
        }
        Some(mut dht) => {
            // DHT has been created and is running, we wait for its termination
            dht.await_termination().await
        }
    }

    Ok(())
}

#[cfg(test)]
mod testing;
