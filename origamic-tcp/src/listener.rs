use std::net::SocketAddr;

use origamic::{gossip::node::process_inbound_stream, overlay::context::OverlayContext};
use tokio::net::TcpListener;

use crate::factory::TcpFactory;

pub async fn start_tcp_listener(addr: SocketAddr, context: OverlayContext<TcpFactory>) {
    let listener = TcpListener::bind(addr)
        .await
        .expect("Failed to bind TCP listener");
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let peer_tx = context.peer_tx.clone();
            tokio::spawn(async move {
                let _ = process_inbound_stream(stream, peer_tx).await;
            });
        }
    });
}
