#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::time::Duration;

    use bincode::{DefaultOptions, Options};
    use env_logger::Env;
    use log::{debug, info};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    use crate::api_communication;
    use crate::P2pDht;

    /// Method to easily create peers for testing purposes
    ///
    /// Allows to deactivate the api socket for faster testing when it is not needed
    async fn start_peers(
        amount: usize,
        start_api_socket: bool,
        mut port_start: u16,
    ) -> Vec<P2pDht> {
        let mut dhts: Vec<P2pDht> = vec![];

        for i in 0..amount {
            let port_counter = port_start;
            port_start += 1;
            let dht = P2pDht::new(
                Duration::from_secs(60),
                Duration::from_secs(60),
                format!("127.0.0.1:2{:0>4}", port_counter)
                    .parse::<SocketAddr>()
                    .unwrap(),
                format!("127.0.0.1:1{:0>4}", port_counter)
                    .parse::<SocketAddr>()
                    .unwrap(),
                if i == 0 {
                    None
                } else {
                    Some(dhts[0].dht.get_address())
                },
                start_api_socket,
                false,
            )
            .await;

            dhts.push(dht);
        }

        dhts
    }

    /// Sends a shutdown signal to all dhts in the list, and then awaits the termination of all threads
    async fn stop_dhts(mut dhts: Vec<P2pDht>) {
        for dht in dhts.iter() {
            dht.initiate_shutdown();
        }

        while let Some(mut dht) = dhts.drain(..).next() {
            dht.await_termination().await;
        }
    }

    /// Very simple test which starts two peers to check for any errors in a simple startup
    ///
    /// They should start talking with each other, but this is not tested here as this is designed
    /// to catch any obvious bugs when changing functionality
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_start_two_peers() {
        let dhts = start_peers(2, true, 1).await;
        stop_dhts(dhts).await;
    }

    /// Starts a single peer, and asks for a non-present key via the api socket.
    ///
    /// Expects a correctly formatted DHT_FAILURE answer
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_api_get_failure() {
        let _ = env_logger::Builder::from_env(Env::default().default_filter_or("debug")).try_init();
        let dhts = start_peers(1, true, 3).await;

        let dht = &dhts[0];
        let mut stream = TcpStream::connect(dht.api_address).await.unwrap();

        let key = [0x1; 32];
        // Send Get Key Request
        let header = api_communication::ApiPacketHeader {
            size: 4 + key.len() as u16,
            message_type: api_communication::API_DHT_GET,
        };

        let mut buf = DefaultOptions::new()
            .with_fixint_encoding()
            .with_big_endian()
            .serialize(&header)
            .unwrap();
        buf.extend(key);
        stream.write_all(&buf).await.unwrap();

        // Read response
        let mut response = [0; 36];
        let bytes_read = stream.read_exact(&mut response).await.unwrap();
        assert_eq!(
            u16::from_be_bytes((&response[2..4]).try_into().unwrap()),
            653
        );
        assert_eq!(response[4..], key);
        assert_eq!(bytes_read, 36);

        // Send shutdown request
        let buf = DefaultOptions::new()
            .with_fixint_encoding()
            .with_big_endian()
            .serialize(&api_communication::ApiPacketHeader {
                size: 4,
                message_type: api_communication::API_DHT_SHUTDOWN,
            })
            .unwrap();
        stream.write_all(&buf).await.unwrap();

        stop_dhts(dhts).await;
    }

    /// Starts a single peer and then communicates via the api socket:
    /// - Sends a DHT_PUT request to store a value
    /// - Sends a DHT_GET request to retrieve the stored value
    /// - Expects a DHT_SUCCESS message with the correct key and value originally stored
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_api_store_get() {
        let _ = env_logger::Builder::from_env(Env::default().default_filter_or("debug")).try_init();
        let dhts = start_peers(1, true, 4).await;

        let dht = &dhts[0];
        let mut stream = TcpStream::connect(dht.api_address).await.unwrap();

        let key = [0x1; 32];
        let value = vec![0x1, 0x2, 0x3];

        // Send Put Key Request
        info!("Sending put");
        let header = api_communication::ApiPacketHeader {
            size: 4 + 4 + key.len() as u16 + value.len() as u16,
            message_type: api_communication::API_DHT_PUT,
        };
        let payload = api_communication::DhtPut {
            ttl: 3600,
            replication: 4,
            _reserved: 0,
            key,
            value: value.clone(),
        };
        let mut buf = Vec::new();
        buf.extend(
            DefaultOptions::new()
                .with_fixint_encoding()
                .with_big_endian()
                .serialize(&header)
                .unwrap(),
        );
        buf.extend(
            DefaultOptions::new()
                .with_fixint_encoding()
                .with_big_endian()
                .serialize(&payload.ttl)
                .unwrap(),
        );
        buf.extend(
            DefaultOptions::new()
                .with_fixint_encoding()
                .with_big_endian()
                .serialize(&payload.replication)
                .unwrap(),
        );
        buf.extend(
            DefaultOptions::new()
                .with_fixint_encoding()
                .with_big_endian()
                .serialize(&payload._reserved)
                .unwrap(),
        );
        buf.extend(payload.key);
        buf.extend(payload.value);

        debug!("Content of put request: {:?}", buf);
        debug!("Length of put request: {:?}", buf.len());
        assert_eq!(header.size as usize, buf.len());
        stream.write_all(&buf).await.unwrap();

        tokio::time::sleep(Duration::from_millis(50)).await;

        // Send Get Key Request
        info!("Sending get");
        let header = api_communication::ApiPacketHeader {
            size: 4 + key.len() as u16,
            message_type: api_communication::API_DHT_GET,
        };
        let mut buf = DefaultOptions::new()
            .with_fixint_encoding()
            .with_big_endian()
            .serialize(&header)
            .unwrap();
        buf.extend(key);
        debug!("Content of get request: {:?}", buf);
        debug!("Length of get request: {:?}", buf.len());
        assert_eq!(header.size as usize, buf.len());
        stream.write_all(&buf).await.unwrap();

        // Read response
        let mut response = [0; 39];
        let bytes_read = stream.read_exact(&mut response).await.unwrap();
        assert_eq!(
            u16::from_be_bytes((&response[2..4]).try_into().unwrap()),
            652
        );
        assert_eq!(response[4..36], key);
        assert_eq!(response[36..], value);
        assert_eq!(bytes_read, 39);

        info!("Sending close");
        let header = api_communication::ApiPacketHeader {
            size: 4,
            message_type: api_communication::API_DHT_SHUTDOWN,
        };
        let mut buf = DefaultOptions::new()
            .with_fixint_encoding()
            .with_big_endian()
            .serialize(&header)
            .unwrap();
        buf.extend(key);
        stream.write_all(&buf).await.unwrap();

        stop_dhts(dhts).await;
    }

    /// Checks if a single peer can store a value. Does not involve the api socket
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_store_get() {
        let _ = env_logger::Builder::from_env(Env::default().default_filter_or("debug")).try_init();
        let dhts = start_peers(1, false, 4).await;

        let key = [0x1; 32];
        let value = vec![0x1, 0x2, 0x3];

        // Put Value
        let dht = &dhts[0];
        api_communication::process_api_put_request(
            dht.dht.clone(),
            api_communication::DhtPut {
                ttl: 3600,
                replication: 4,
                _reserved: 0,
                key,
                value: value.clone(),
            },
        )
        .await;

        // Get
        let hashed_key = api_communication::hash_key_bytes(&key);
        let value_back = dht.dht.get(hashed_key).await.unwrap();

        assert_eq!(value_back, value);
        stop_dhts(dhts).await;
    }

    /// This test stores 16 entries on two nodes and checks if both nodes can retrieve all values
    ///
    /// This is done by storing 8 entries on one node and 8 entries on the other node.
    /// If working correctly the nodes distribute the entries based on the key, and are later able
    /// to retrieve all values
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_multiple_store_get() {
        let _ = env_logger::Builder::from_env(Env::default().default_filter_or("debug")).try_init();
        let dhts = start_peers(2, false, 20).await;

        tokio::time::sleep(Duration::from_millis(50)).await;

        let dht0 = &dhts[0];
        let dht1 = &dhts[1];

        let pairs0 = [
            ([0x01; 32], vec![0x01]),
            ([0x02; 32], vec![0x02]),
            ([0x03; 32], vec![0x03]),
            ([0x04; 32], vec![0x04]),
            ([0x05; 32], vec![0x05]),
            ([0x06; 32], vec![0x06]),
            ([0x07; 32], vec![0x07]),
            ([0x08; 32], vec![0x08]),
        ];
        let pairs1 = [
            ([0x0a; 32], vec![0x0a]),
            ([0x0b; 32], vec![0x0b]),
            ([0x0c; 32], vec![0x0c]),
            ([0x0d; 32], vec![0x0d]),
            ([0x0e; 32], vec![0x0e]),
            ([0x0f; 32], vec![0x0f]),
            ([0x10; 32], vec![0x10]),
            ([0x11; 32], vec![0x11]),
        ];

        for (key, value) in &pairs0 {
            // Put Value
            api_communication::process_api_put_request(
                dht0.dht.clone(),
                api_communication::DhtPut {
                    ttl: 3600,
                    replication: 4,
                    _reserved: 0,
                    key: *key,
                    value: value.clone(),
                },
            )
            .await;
        }

        for (key, value) in &pairs1 {
            // Put Value
            api_communication::process_api_put_request(
                dht1.dht.clone(),
                api_communication::DhtPut {
                    ttl: 3600,
                    replication: 4,
                    _reserved: 0,
                    key: *key,
                    value: value.clone(),
                },
            )
            .await;
        }

        tokio::time::sleep(Duration::from_millis(20)).await;

        let pairs_all = [pairs0, pairs1].concat();

        check_all_keys(&dhts, pairs_all).await;
        stop_dhts(dhts).await;
    }

    /// Larger integration test
    ///
    /// Starts 10 Nodes and then distributes insert calls over all nodes. Checks afterwards if all nodes
    /// are able to find all entries. This demonstrates that the nodes are working correctly together
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_hammer_store_get() {
        let _ = env_logger::Builder::from_env(Env::default().default_filter_or("debug")).try_init();
        let amount_peers: usize = 10;
        let dhts = start_peers(amount_peers, false, 40).await;

        // tokio::time::sleep(Duration::from_millis(50)).await;

        let pairs_size: usize = 16;
        let mut pairs: Vec<([u8; 32], Vec<u8>)> = Vec::new();
        for i in 0..pairs_size {
            pairs.push(([i as u8; 32], vec![i as u8]));
        }

        for (i, pair_chunk) in pairs.chunks(pairs_size / (amount_peers - 2)).enumerate() {
            for (key, value) in pair_chunk {
                // Put Value
                api_communication::process_api_put_request(
                    dhts[i].dht.clone(),
                    api_communication::DhtPut {
                        ttl: 3600,
                        replication: 4,
                        _reserved: 0,
                        key: *key,
                        value: value.clone(),
                    },
                )
                .await;
            }
        }

        tokio::time::sleep(Duration::from_millis(20)).await;
        check_all_keys(&dhts, pairs).await;
        stop_dhts(dhts).await;
    }

    /// Similar to the `test_hammer_store_get` test, but after creating a network with 10 nodes, two
    /// nodes are disconnected from the network.
    ///
    /// The network afterwards should still be able to store values and retrieve them
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_stabilize() {
        let _ = env_logger::Builder::from_env(Env::default().default_filter_or("debug")).try_init();
        let amount_peers: usize = 10;
        let mut dhts = start_peers(amount_peers, false, 30).await;

        stabilize_all(&dhts).await;
        fix_fingers_all(&dhts).await;

        // Shutdown two nodes and remove them from list
        dhts[0].initiate_shutdown();
        dhts[2].initiate_shutdown();
        dhts.remove(2);
        dhts.remove(0);

        stabilize_all(&dhts).await;
        fix_fingers_all(&dhts).await;

        let pairs_size: usize = 16;
        let mut pairs: Vec<([u8; 32], Vec<u8>)> = Vec::new();
        for i in 0..pairs_size {
            pairs.push(([i as u8; 32], vec![i as u8]));
        }

        for (i, pair_chunk) in pairs.chunks(pairs_size / (amount_peers - 2)).enumerate() {
            for (key, value) in pair_chunk {
                // Put Value
                api_communication::process_api_put_request(
                    dhts[i].dht.clone(),
                    api_communication::DhtPut {
                        ttl: 3600,
                        replication: 4,
                        _reserved: 0,
                        key: *key,
                        value: value.clone(),
                    },
                )
                .await;
            }
        }

        tokio::time::sleep(Duration::from_millis(20)).await;
        check_all_keys(&dhts, pairs).await;
        stop_dhts(dhts).await;
    }

    /// Triggers stabilization method on all dhts
    async fn stabilize_all(dhts: &Vec<P2pDht>) {
        for wrapper in dhts {
            if wrapper.cancellation_token.is_cancelled() {
                // dont stabilize when stopped
                continue;
            }

            debug!("Stabilizing {}", wrapper.api_address);

            wrapper
                .dht
                .stabilize()
                .await
                .expect("Stabilize resulted in an unexpected error");
        }
    }

    /// Triggers the fix finger method on all dhts
    async fn fix_fingers_all(dhts: &Vec<P2pDht>) {
        for wrapper in dhts {
            if wrapper.cancellation_token.is_cancelled() {
                // dont stabilize when stopped
                continue;
            }
            wrapper
                .dht
                .fix_fingers()
                .await
                .expect("Fix fingers resulted in an unexpected error");
        }
    }

    /// Checks if all dhts are able to successfully retrieve all entries provided in the argument
    async fn check_all_keys(dhts: &Vec<P2pDht>, original_pairs: Vec<([u8; 32], Vec<u8>)>) {
        for (key, value) in original_pairs {
            // Get
            let hashed_key = api_communication::hash_key_bytes(&key);

            for dht in dhts {
                match dht.dht.get(hashed_key).await {
                    Some(value_back) => {
                        assert_eq!(value_back, value);
                    }
                    None => panic!("Value has not been found"),
                }
            }
        }
    }
}
