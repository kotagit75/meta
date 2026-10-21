use std::net::SocketAddr;

use origamic::overlay::{context::OverlayContext, factory::OverlayFactory};
use origamic_tcp::{factory::TcpFactory, listener::start_tcp_listener};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listen_addr: SocketAddr = "127.0.0.1:8080".parse()?;
    let tcp_peers: Vec<SocketAddr> = vec!["127.0.0.1:8081".parse()?];

    let context =
        OverlayContext::with_advertise(uuid::Uuid::new_v4(), TcpFactory, tcp_peers, listen_addr);

    println!("Network Id: {}", context.network_id());
    println!("Server Addr: {}", context.address());

    start_tcp_listener(listen_addr, context.clone()).await;
    let factory = OverlayFactory::new(context.clone());
    let mut listener = factory.listen();

    loop {
        let (mut stream, addr) = listener.accept().await?;
        println!("Accepted connection from {}", addr);

        tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            loop {
                match stream.read(&mut buf).await {
                    Ok(0) => {
                        println!("Client disconnected.");
                        break;
                    }
                    Ok(n) => {
                        let received = String::from_utf8_lossy(&buf[..n]);
                        println!("Received: {}", received);
                        if let Err(e) = stream.write_all(&buf[..n]).await {
                            eprintln!("Failed to echo back: {}", e);
                            break;
                        }
                    }
                    Err(e) => {
                        eprintln!("Read error: {}", e);
                        break;
                    }
                }
            }
        });
    }
}
