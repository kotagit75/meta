use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{Mutex, mpsc};
use uuid::Uuid;

use crate::{
    factory::Factory,
    gossip::node::{GossipNode, GossipNodeHandle, OutgoingLocalMessage},
    overlay::{
        address::{Keypair, OverlayAddress},
        message::{GossipMessage, StreamFrame},
        stream::{OverlayListener, OverlayStream},
    },
};

type ActiveStreamMap = HashMap<(OverlayAddress, u64), mpsc::Sender<Vec<u8>>>;

#[derive(Clone)]
pub struct OverlayContext<F: Factory> {
    pub network_id: Uuid,
    pub keypair: Arc<Keypair>,
    pub lower_factory: F,
    pub lower_peers: Vec<F::Address>,
    pub local_tx: mpsc::Sender<OutgoingLocalMessage>,
    pub peer_tx: mpsc::Sender<GossipMessage>,
    pub active_streams: Arc<Mutex<ActiveStreamMap>>,
    pub inbound_stream_rx: Arc<Mutex<mpsc::Receiver<(OverlayStream<F>, OverlayAddress)>>>,
}

const DEFAULT_GOSSIP_TTL: u8 = 7;
const DEFAULT_GOSSIP_BUFFER_SIZE: usize = 256;

impl<F: Factory> OverlayContext<F> {
    /// 新規ネットワーク（ランダムなUuid）を立ち上げてノードを生成
    pub fn new(lower_factory: F, lower_peers: Vec<F::Address>) -> Self {
        Self::with_network(Uuid::new_v4(), lower_factory, lower_peers)
    }

    /// 既存のネットワークID（Uuid）に参加するノードを生成
    pub fn with_network(network_id: Uuid, lower_factory: F, lower_peers: Vec<F::Address>) -> Self {
        Self::with_network_and_keypair(network_id, Keypair::generate(), lower_factory, lower_peers)
    }

    /// ネットワークIDおよび鍵ペアを明示指定してノードを生成
    pub fn with_network_and_keypair(
        network_id: Uuid,
        keypair: Keypair,
        lower_factory: F,
        lower_peers: Vec<F::Address>,
    ) -> Self {
        let address = keypair.address;
        let GossipNodeHandle {
            address: _,
            network_id: _,
            local_tx,
            upper_rx,
            peer_tx,
        } = GossipNode::spawn(
            network_id,
            keypair.clone(),
            lower_factory.clone(),
            lower_peers.clone(),
            DEFAULT_GOSSIP_TTL,
            DEFAULT_GOSSIP_BUFFER_SIZE,
        );

        let active_streams: Arc<Mutex<ActiveStreamMap>> = Arc::new(Mutex::new(HashMap::new()));
        let (inbound_tx, inbound_rx) = mpsc::channel(128);

        start_stream_router_task(
            address,
            upper_rx,
            active_streams.clone(),
            local_tx.clone(),
            inbound_tx,
        );

        Self {
            network_id,
            keypair: Arc::new(keypair),
            lower_factory,
            lower_peers,
            local_tx,
            peer_tx,
            active_streams,
            inbound_stream_rx: Arc::new(Mutex::new(inbound_rx)),
        }
    }

    pub fn network_id(&self) -> Uuid {
        self.network_id
    }

    pub fn address(&self) -> OverlayAddress {
        self.keypair.address
    }

    /// インバウンド仮想ストリームを待受けるリスナーを取得
    pub fn listen(&self) -> OverlayListener<F> {
        OverlayListener::new(self.inbound_stream_rx.clone())
    }

    /// 自ネットワーク全体へブロードキャスト発信
    pub async fn broadcast(
        &self,
        payload: Vec<u8>,
    ) -> Result<(), mpsc::error::SendError<OutgoingLocalMessage>> {
        self.local_tx
            .send(OutgoingLocalMessage::broadcast(payload))
            .await
    }

    /// 自ネットワーク内の特定公開鍵宛てにメッセージ発信
    pub async fn send_to(
        &self,
        recipient: OverlayAddress,
        payload: Vec<u8>,
    ) -> Result<(), mpsc::error::SendError<OutgoingLocalMessage>> {
        self.local_tx
            .send(OutgoingLocalMessage::to(recipient, payload))
            .await
    }
}

/// 仮想ストリーム多重化ルータタスク（上位受信メッセージを各ストリームへ振り分け）
fn start_stream_router_task<F: Factory>(
    self_address: OverlayAddress,
    mut upper_rx: mpsc::Receiver<GossipMessage>,
    active_streams: Arc<Mutex<ActiveStreamMap>>,
    local_tx: mpsc::Sender<OutgoingLocalMessage>,
    inbound_tx: mpsc::Sender<(OverlayStream<F>, OverlayAddress)>,
) {
    tokio::spawn(async move {
        while let Some(msg) = upper_rx.recv().await {
            // 宛先チェック: 自分宛て、またはブロードキャストのみ処理
            if let Some(target) = msg.recipient
                && target != self_address
            {
                continue;
            }

            // StreamFrame のデコード
            let Ok(frame) = StreamFrame::decode(&msg.payload) else {
                continue;
            };

            let key = (msg.sender, frame.stream_id);
            let mut streams = active_streams.lock().await;

            if let Some(stream_tx) = streams.get(&key) {
                let _ = stream_tx.send(frame.data).await;
            } else if frame.is_init || !frame.data.is_empty() {
                // 新規インバウンドストリームの生成
                spawn_inbound_stream_bridge(
                    key,
                    frame.data,
                    &mut streams,
                    local_tx.clone(),
                    &inbound_tx,
                )
                .await;
            }
        }
    });
}

/// 新規インバウンド接続用の duplex ストリームおよび送受信ブリッジを起動
async fn spawn_inbound_stream_bridge<F: Factory>(
    (sender_addr, stream_id): (OverlayAddress, u64),
    initial_data: Vec<u8>,
    streams: &mut ActiveStreamMap,
    local_tx: mpsc::Sender<OutgoingLocalMessage>,
    inbound_tx: &mpsc::Sender<(OverlayStream<F>, OverlayAddress)>,
) {
    let (user_stream, internal_stream) = tokio::io::duplex(65536);
    let (to_stream_tx, mut from_router_rx) = mpsc::channel(256);

    let _ = to_stream_tx.send(initial_data).await;
    streams.insert((sender_addr, stream_id), to_stream_tx);

    tokio::spawn(async move {
        let (mut read_half, mut write_half) = tokio::io::split(internal_stream);

        let send_task = async {
            let mut buf = vec![0u8; 4096];
            loop {
                match read_half.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => {
                        let sf = StreamFrame::new_data(stream_id, buf[..n].to_vec());
                        if let Ok(enc) = sf.encode() {
                            let out = OutgoingLocalMessage::to(sender_addr, enc);
                            if local_tx.send(out).await.is_err() {
                                break;
                            }
                        }
                    }
                    Err(_) => break,
                }
            }
        };

        let recv_task = async {
            while let Some(data) = from_router_rx.recv().await {
                if write_half.write_all(&data).await.is_err() {
                    break;
                }
            }
        };

        tokio::select! {
            _ = send_task => {},
            _ = recv_task => {},
        }
    });

    let new_stream = OverlayStream::new(user_stream, sender_addr);
    let _ = inbound_tx.send((new_stream, sender_addr)).await;
}
