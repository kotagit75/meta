use std::future::Future;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

use crate::{
    factory::Factory,
    gossip::node::{OutgoingLocalMessage, process_inbound_stream},
    overlay::{
        address::OverlayAddress,
        context::OverlayContext,
        message::{GossipMessage, StreamFrame},
        stream::{OverlayListener, OverlayStream},
    },
};

#[derive(Clone)]
pub struct OverlayFactory<F: Factory> {
    context: OverlayContext<F>,
}

impl<F: Factory> OverlayFactory<F> {
    pub fn new(context: OverlayContext<F>) -> Self {
        Self { context }
    }

    pub fn context(&self) -> &OverlayContext<F> {
        &self.context
    }

    /// インバウンド接続を受け付けるリスナーを取得
    pub fn listen(&self) -> OverlayListener<F> {
        OverlayListener {
            inbound_rx: self.context.inbound_stream_rx.clone(),
        }
    }
}

impl<F: Factory> Factory for OverlayFactory<F> {
    type Stream = OverlayStream<F>;
    type Address = OverlayAddress;

    fn connect(
        &self,
        addr: Self::Address,
    ) -> impl Future<Output = std::io::Result<Self::Stream>> + Send {
        let (user_stream, internal_stream) = tokio::io::duplex(65536);
        let stream_id: u64 = rand::random();
        let (to_stream_tx, from_router_rx) = mpsc::channel(256);
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

        spawn_outbound_stream_bridge(
            addr,
            stream_id,
            internal_stream,
            to_stream_tx,
            from_router_rx,
            shutdown_tx,
            self.context.active_streams.clone(),
            self.context.local_tx.clone(),
        );

        let overlay_stream = OverlayStream::with_shutdown_rx(user_stream, addr, shutdown_rx);
        async move { Ok(overlay_stream) }
    }

    async fn start_listener(
        &self,
        _bind_addr: Option<Self::Address>,
        peer_tx: mpsc::Sender<GossipMessage>,
    ) -> std::io::Result<()> {
        let mut listener = self.listen();
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

/// アウトバウンド仮想ストリームの送受信ブリッジタスクを起動
fn spawn_outbound_stream_bridge(
    addr: OverlayAddress,
    stream_id: u64,
    internal_stream: tokio::io::DuplexStream,
    to_stream_tx: mpsc::Sender<Vec<u8>>,
    mut from_router_rx: mpsc::Receiver<Vec<u8>>,
    shutdown_tx: tokio::sync::oneshot::Sender<()>,
    active_streams: std::sync::Arc<
        tokio::sync::Mutex<std::collections::HashMap<(OverlayAddress, u64), mpsc::Sender<Vec<u8>>>>,
    >,
    local_tx: mpsc::Sender<OutgoingLocalMessage>,
) {
    tokio::spawn(async move {
        {
            let mut map = active_streams.lock().await;
            map.insert((addr, stream_id), to_stream_tx);
        }

        let (mut read_half, mut write_half) = tokio::io::split(internal_stream);

        // 送信ブリッジ: user_stream への write を相手宛ての StreamFrame としてゴシップ網へ発信
        let mut shutdown_tx = Some(shutdown_tx);
        let send_task = async {
            let mut buf = vec![0u8; 4096];
            let mut is_first = true;
            loop {
                match read_half.read(&mut buf).await {
                    Ok(0) => {
                        let close_frame = StreamFrame::new_close(stream_id);
                        if let Ok(encoded) = close_frame.encode() {
                            let (done_tx, done_rx) = tokio::sync::oneshot::channel();
                            let out = OutgoingLocalMessage::to_with_done(addr, encoded, done_tx);
                            let _ = local_tx.send(out).await;
                            let _ = done_rx.await;
                        }
                        if let Some(tx) = shutdown_tx.take() {
                            let _ = tx.send(());
                        }
                        break;
                    }
                    Ok(n) => {
                        let frame = if is_first {
                            is_first = false;
                            StreamFrame::new_init(stream_id, buf[..n].to_vec())
                        } else {
                            StreamFrame::new_data(stream_id, buf[..n].to_vec())
                        };

                        if let Ok(encoded) = frame.encode() {
                            let out = OutgoingLocalMessage::to(addr, encoded);
                            if local_tx.send(out).await.is_err() {
                                break;
                            }
                        }
                    }
                    Err(_) => {
                        let close_frame = StreamFrame::new_close(stream_id);
                        if let Ok(encoded) = close_frame.encode() {
                            let (done_tx, done_rx) = tokio::sync::oneshot::channel();
                            let out = OutgoingLocalMessage::to_with_done(addr, encoded, done_tx);
                            let _ = local_tx.send(out).await;
                            let _ = done_rx.await;
                        }
                        if let Some(tx) = shutdown_tx.take() {
                            let _ = tx.send(());
                        }
                        break;
                    }
                }
            }
        };

        // 受信ブリッジ: 相手から届いたストリーム返信を user_stream の read 側へ供給
        let recv_task = async {
            while let Some(data) = from_router_rx.recv().await {
                if write_half.write_all(&data).await.is_err() {
                    break;
                }
            }
            let _ = write_half.shutdown().await;
        };

        tokio::select! {
            _ = send_task => {},
            _ = recv_task => {},
        }

        // ストリーム終了時の後片付け
        let mut map = active_streams.lock().await;
        map.remove(&(addr, stream_id));
    });
}
