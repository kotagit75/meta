use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf};
use tokio::sync::{Mutex, mpsc, oneshot};

use crate::{factory::Factory, overlay::address::OverlayAddress};

pub struct OverlayStream<F: Factory> {
    pub(crate) inner: DuplexStream,
    pub(crate) peer_addr: OverlayAddress,
    pub(crate) shutdown_rx: Option<oneshot::Receiver<()>>,
    pub(crate) _phantom: PhantomData<fn() -> F>,
}

impl<F: Factory> Unpin for OverlayStream<F> {}

impl<F: Factory> OverlayStream<F> {
    pub fn new(inner: DuplexStream, peer_addr: OverlayAddress) -> Self {
        Self {
            inner,
            peer_addr,
            shutdown_rx: None,
            _phantom: PhantomData,
        }
    }

    pub fn with_shutdown_rx(
        inner: DuplexStream,
        peer_addr: OverlayAddress,
        shutdown_rx: oneshot::Receiver<()>,
    ) -> Self {
        Self {
            inner,
            peer_addr,
            shutdown_rx: Some(shutdown_rx),
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

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let _ = Pin::new(&mut self.inner).poll_shutdown(cx)?;
        if let Some(rx) = self.shutdown_rx.as_mut() {
            match Pin::new(rx).poll(cx) {
                Poll::Ready(_) => {
                    self.shutdown_rx = None;
                    Poll::Ready(Ok(()))
                }
                Poll::Pending => Poll::Pending,
            }
        } else {
            Poll::Ready(Ok(()))
        }
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
