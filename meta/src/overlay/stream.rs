use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf};
use tokio::sync::{Mutex, mpsc};

use crate::{factory::Factory, overlay::address::OverlayAddress};

pub struct OverlayStream<F: Factory> {
    pub(crate) inner: DuplexStream,
    pub(crate) peer_addr: OverlayAddress,
    pub(crate) _phantom: PhantomData<fn() -> F>,
}

impl<F: Factory> Unpin for OverlayStream<F> {}

impl<F: Factory> OverlayStream<F> {
    pub fn new(inner: DuplexStream, peer_addr: OverlayAddress) -> Self {
        Self {
            inner,
            peer_addr,
            _phantom: PhantomData,
        }
    }

    pub fn peer_address(&self) -> OverlayAddress {
        self.peer_addr
    }
}

impl<F: Factory> AsyncRead for OverlayStream<F> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

impl<F: Factory> AsyncWrite for OverlayStream<F> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

pub struct OverlayListener<F: Factory> {
    pub(crate) inbound_rx: Arc<Mutex<mpsc::Receiver<(OverlayStream<F>, OverlayAddress)>>>,
}

impl<F: Factory> OverlayListener<F> {
    pub fn new(inbound_rx: Arc<Mutex<mpsc::Receiver<(OverlayStream<F>, OverlayAddress)>>>) -> Self {
        Self { inbound_rx }
    }

    pub async fn accept(&mut self) -> std::io::Result<(OverlayStream<F>, OverlayAddress)> {
        let mut rx = self.inbound_rx.lock().await;
        rx.recv().await.ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::BrokenPipe, "OverlayListener closed")
        })
    }
}
