use origamic::factory::Factory;
use std::net::SocketAddr;
use tokio::net::TcpStream;

#[derive(Clone, Copy, Debug, Default)]
pub struct TcpFactory;

impl TcpFactory {
    pub fn new() -> Self {
        Self
    }
}

impl Factory for TcpFactory {
    type Stream = TcpStream;
    type Address = SocketAddr;

    async fn connect(&self, addr: Self::Address) -> std::io::Result<Self::Stream> {
        Self::Stream::connect(addr).await
    }
}
