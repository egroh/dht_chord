#![feature(async_closure)]

use std::error::Error;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:13337").await?;
    loop {
        let (mut socket, _) = listener.accept().await?;
        tokio::spawn(async move {
            let mut connection_result = async move || -> Result<(), Box<dyn Error>> {
                // In a loop, read data from the socket
                let mut buf = [0; 1024];
                loop {
                    let n = match socket.read(&mut buf).await {
                        // socket closed
                        Ok(n) if n == 0 => return Ok(()),
                        Ok(n) => n,
                        Err(e) => return Err(e.into()),
                    };
                }
            };
            if let Err(e) = connection_result().await {
                println!("Error on connection: {}", e)
            }
        });
    }
}
