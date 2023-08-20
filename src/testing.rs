mod tests {
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;
    use std::thread::sleep;
    use std::time::Duration;

    use bincode::Options;
    use serde::{Deserialize, Serialize};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::mpsc;
    use tokio::task::JoinHandle;

    use crate::api_communication::with_big_endian;
    use crate::{
        start_dht, ApiPacketHeader, DhtGetFailure, DhtGetResponse, DhtPut, P2pDht, API_DHT_GET,
        API_DHT_PUT, API_DHT_SHUTDOWN,
    };

    async fn start_peers(amount: usize) -> Vec<(Arc<P2pDht>, JoinHandle<()>)> {
        let mut handles: Vec<(Arc<P2pDht>, JoinHandle<()>)> = vec![];

        static COUNTER: AtomicU32 = AtomicU32::new(1);

        for i in 0..amount {
            let port_counter = COUNTER.fetch_add(1, Ordering::SeqCst);
            let dht = Arc::new(
                P2pDht::new(
                    Duration::from_secs(60),
                    Duration::from_secs(60),
                    format!("127.0.0.1:4{:0>4}", port_counter)
                        .parse::<SocketAddr>()
                        .unwrap(),
                    format!("127.0.0.1:3{:0>4}", port_counter)
                        .parse::<SocketAddr>()
                        .unwrap(),
                    if i == 0 {
                        None
                    } else {
                        Some(handles[0].0.dht.get_address())
                    },
                )
                .await,
            );

            let api_listener = TcpListener::bind(dht.api_address).await.unwrap();
            // Open channel for inter thread communication
            let (tx, mut rx) = mpsc::channel(1);

            handles.push((
                dht.clone(),
                tokio::spawn(async move {
                    // Send signal that we are running
                    tx.send(true).await.expect("Unable to send message");
                    start_dht(dht, api_listener).await.unwrap();
                }),
            ));
            // Await thread spawn, to avoid EOF errors because the thread is not ready to accept messages
            rx.recv().await.unwrap();
        }

        handles
    }

    async fn stop_dhts(mut dhts: Vec<(Arc<P2pDht>, JoinHandle<()>)>) {
        for (_, handle) in dhts.iter() {
            handle.abort();
        }

        // Iterate over all entrys and take ownership over the handles to terminate them
        while let Some((_dht, handle)) = dhts.drain(..).next() {
            // Wait for handles to join and ignore all errors
            let _ = handle.await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_start_two_peers() {
        let dhts = start_peers(2).await;
        stop_dhts(dhts).await;
    }

    #[derive(Serialize, Debug)]
    struct Put {
        header: ApiPacketHeader,
        payload: DhtPut,
    }
    #[derive(Deserialize, Debug)]
    struct GetFailure {
        _header: ApiPacketHeader,
        payload: DhtGetFailure,
    }

    #[derive(Deserialize, Debug)]
    struct GetResponse {
        _header: ApiPacketHeader,
        payload: DhtGetResponse,
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_api_get_failure() {
        let dhts = start_peers(1).await;

        let (dht, _) = &dhts[0];
        let mut stream = TcpStream::connect(dht.api_address).await.unwrap();

        let key = [0x1; 32];
        // Send Get Key Request
        let header = ApiPacketHeader {
            size: 4 + key.len() as u16,
            message_type: API_DHT_GET,
        };

        let mut buf = with_big_endian().serialize(&header).unwrap();
        buf.extend(key);
        stream.write_all(&buf).await.unwrap();

        // Read response
        let bytes_read = stream.read(&mut buf).await.unwrap();
        let received_data: GetFailure = bincode::deserialize(&buf[..bytes_read]).unwrap();

        assert_eq!(received_data.payload.key, key);

        // Send shutdown request
        let buf = with_big_endian()
            .serialize(&ApiPacketHeader {
                size: 4,
                message_type: API_DHT_SHUTDOWN,
            })
            .unwrap();
        stream.write_all(&buf).await.unwrap();

        stop_dhts(dhts).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_api_store_get() {
        let dhts = start_peers(2).await;

        let (dht, _) = &dhts[0];
        let mut stream = TcpStream::connect(dht.api_address).await.unwrap();

        let key = [0x1; 32];
        let value = vec![0x1, 0x2, 0x3];

        // Send Put Key Request
        let put_message = Put {
            header: ApiPacketHeader {
                size: 4 + 4 + key.len() as u16 + value.len() as u16,
                message_type: API_DHT_PUT,
            },
            payload: DhtPut {
                ttl: 0,
                replication: 0,
                reserved: 0,
                key,
                value,
            },
        };
        stream
            .write_all(&with_big_endian().serialize(&put_message).unwrap())
            .await
            .unwrap();

        println!("Send store");

        // Send Get Key Request
        let header = ApiPacketHeader {
            size: 4 + key.len() as u16,
            message_type: API_DHT_GET,
        };
        let mut buf = with_big_endian().serialize(&header).unwrap();
        buf.extend(key);
        stream.write_all(&buf).await.unwrap();

        println!("Send get");

        // Read response
        let bytes_read = stream.read(&mut buf).await.unwrap();
        let received_data: GetResponse = bincode::deserialize(&buf[..bytes_read]).unwrap();

        assert_eq!(received_data.payload.key, key);
        stop_dhts(dhts).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_store_get() {
        let dhts = start_peers(2).await;

        let key = [0x1; 32];
        let value = vec![0x1, 0x2, 0x3];

        // Put Value
        let (dht, _) = &dhts[0];
        dht.put(DhtPut {
            ttl: 0,
            replication: 0,
            reserved: 0,
            key,
            value: value.clone(),
        })
        .await;

        // Get
        let hashed_key = P2pDht::hash_vec_bytes(&key);
        let value_back = dht.dht.get(hashed_key).await.unwrap();

        assert_eq!(value_back, value);
        stop_dhts(dhts).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_multiple_store_get() {
        let dhts = start_peers(2).await;

        tokio::time::sleep(Duration::from_millis(20)).await;

        let (dht0, _) = &dhts[0];
        let (dht1, _) = &dhts[1];

        let pairs0 = [
            ([0x1; 32], vec![0x1]),
            ([0x2; 32], vec![0x2]),
            ([0x3; 32], vec![0x3]),
            ([0x4; 32], vec![0x4]),
        ];
        let pairs1 = [
            ([0xa; 32], vec![0xa]),
            ([0xb; 32], vec![0xb]),
            ([0xc; 32], vec![0xc]),
            ([0xd; 32], vec![0xd]),
        ];

        for (key, value) in &pairs0 {
            // Put Value
            dht0.put(DhtPut {
                ttl: 0,
                replication: 0,
                reserved: 0,
                key: *key,
                value: value.clone(),
            })
            .await;
        }

        for (key, value) in &pairs1 {
            // Put Value
            dht1.put(DhtPut {
                ttl: 0,
                replication: 0,
                reserved: 0,
                key: *key,
                value: value.clone(),
            })
            .await;
        }

        tokio::time::sleep(Duration::from_millis(20)).await;
        print_dhts(&dhts);

        let pairs_all = [pairs0, pairs1].concat();

        for (key, value) in pairs_all {
            // Get
            let hashed_key = P2pDht::hash_vec_bytes(&key);

            for dht in [dht0, dht1] {
                match dht.dht.get(hashed_key).await {
                    Err(e) => {
                        eprintln!("{:?}", e);
                        panic!("Value has not been found")
                    }
                    Ok(value_back) => {
                        assert_eq!(value_back, value);
                    }
                }
            }
        }
        stop_dhts(dhts).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_hammer_store_get() {
        let amount_peers: usize = 4;
        let dhts = start_peers(amount_peers).await;

        tokio::time::sleep(Duration::from_millis(20)).await;

        let pairs_size: usize = 16;
        let mut pairs: Vec<([u8; 32], Vec<u8>)> = Vec::new();
        for i in 0..pairs_size {
            pairs.push(([i as u8; 32], vec![i as u8]));
        }

        for (i, pair_chunk) in pairs.chunks(pairs_size / (amount_peers - 2)).enumerate() {
            for (key, value) in pair_chunk {
                // Put Value
                dhts[i]
                    .0
                    .put(DhtPut {
                        ttl: 0,
                        replication: 0,
                        reserved: 0,
                        key: *key,
                        value: value.clone(),
                    })
                    .await;
            }
        }

        tokio::time::sleep(Duration::from_millis(20)).await;
        print_dhts(&dhts);
        for (key, value) in pairs {
            // Get
            let hashed_key = P2pDht::hash_vec_bytes(&key);

            for (dht, _) in &dhts {
                match dht.dht.get(hashed_key).await {
                    Err(e) => {
                        eprintln!("{:?}", e);
                        panic!("Value has not been found")
                    }
                    Ok(value_back) => {
                        assert_eq!(value_back, value);
                    }
                }
            }
        }
        stop_dhts(dhts).await;
    }

    fn print_dhts(dhts: &Vec<(Arc<P2pDht>, JoinHandle<()>)>) {
        for (dht, _) in dhts {
            println!("{}", dht.api_address);
            dht.dht.print_short();
            for (key, value) in dht.dht.state.local_storage.clone() {
                println!("  {:x}: {:?}", key, value);
            }
        }
    }
}
