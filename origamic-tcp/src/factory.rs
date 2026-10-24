use origamic_core::{
    factory::Factory, gossip::node::process_inbound_stream, overlay::message::GossipMessage,
};
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

#[derive(Clone, Copy, Debug, Default)]
pub struct TcpFactory {
    bind_addr: Option<SocketAddr>,
}

impl TcpFactory {
    pub fn new() -> Self {
        Self { bind_addr: None }
    }

    pub fn listening(bind_addr: SocketAddr) -> Self {
        Self {
            bind_addr: Some(bind_addr),
        }
    }

    pub fn bind_addr(&self) -> Option<SocketAddr> {
        self.bind_addr
    }
}

impl Factory for TcpFactory {
    type Stream = TcpStream;
    type Address = SocketAddr;

    async fn connect(&self, addr: Self::Address) -> std::io::Result<Self::Stream> {
        Self::Stream::connect(addr).await
    }

    async fn start_listener(
        &self,
        bind_addr: Option<Self::Address>,
        peer_tx: mpsc::Sender<GossipMessage>,
    ) -> std::io::Result<()> {
        let target_addr = self.bind_addr.or(bind_addr);
        let Some(addr) = target_addr else {
            return Ok(());
        };

        let listener = TcpListener::bind(addr).await?;
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let tx = peer_tx.clone();
                tokio::spawn(async move {
                    let _ = process_inbound_stream(stream, tx).await;
                });
            }
        });
        Ok(())
    }
}
