use std::net::SocketAddr;

use meta::layers::{overlay::{context::OverlayContext, factory::OverlayFactory}, tcp::factory::TcpFactory};
use uuid::{Uuid, uuid};

const NETWORK_ID: Uuid = uuid!("b52d6ad5-d2e2-42a3-a90d-6d3d0ce80bcd");

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let tcp_peers: Vec<SocketAddr> = vec!["127.0.0.1:19001".parse()?, "127.0.0.1:19002".parse()?, "127.0.0.1:19003".parse()?];

    let context = OverlayContext::with_network(NETWORK_ID, TcpFactory, lower_peers);
    let factory = OverlayFactory::new(context);

    Ok(())
}
