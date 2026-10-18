use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use uuid::Uuid;

use crate::{
    factory::Factory,
    gossip::cache::SeenCache,
    overlay::{
        address::{Keypair, OverlayAddress},
        message::GossipMessage,
    },
};

#[derive(Clone, Debug)]
pub struct OutgoingLocalMessage {
    pub recipient: Option<OverlayAddress>,
    pub payload: Vec<u8>,
}

impl OutgoingLocalMessage {
    pub fn broadcast(payload: Vec<u8>) -> Self {
        Self {
            recipient: None,
            payload,
        }
    }

    pub fn to(recipient: OverlayAddress, payload: Vec<u8>) -> Self {
        Self {
            recipient: Some(recipient),
            payload,
        }
    }
}

pub struct GossipNodeHandle {
    pub address: OverlayAddress,
    pub network_id: Uuid,
    pub local_tx: mpsc::Sender<OutgoingLocalMessage>,
    pub upper_rx: mpsc::Receiver<GossipMessage>,
    pub peer_tx: mpsc::Sender<GossipMessage>,
}

pub struct GossipNode<F: Factory> {
    pub network_id: Uuid,
    pub keypair: Arc<Keypair>,
    pub lower_factory: F,
    pub lower_peers: Vec<F::Address>,
    pub cache: SeenCache,
    pub local_rx: mpsc::Receiver<OutgoingLocalMessage>,
    pub peer_rx: mpsc::Receiver<GossipMessage>,
    pub upper_tx: mpsc::Sender<GossipMessage>,
    pub default_ttl: u8,
}

impl<F: Factory> GossipNode<F> {
    pub fn spawn(
        network_id: Uuid,
        keypair: Keypair,
        lower_factory: F,
        lower_peers: Vec<F::Address>,
        default_ttl: u8,
        buffer_size: usize,
    ) -> GossipNodeHandle {
        let address = keypair.address;
        let keypair = Arc::new(keypair);

        let (local_tx, local_rx) = mpsc::channel(buffer_size);
        let (peer_tx, peer_rx) = mpsc::channel(buffer_size);
        let (upper_tx, upper_rx) = mpsc::channel(buffer_size);

        let node = Self {
            network_id,
            keypair,
            lower_factory,
            lower_peers,
            cache: SeenCache::with_default(),
            local_rx,
            peer_rx,
            upper_tx,
            default_ttl,
        };

        tokio::spawn(node.run());

        GossipNodeHandle {
            address,
            network_id,
            local_tx,
            upper_rx,
            peer_tx,
        }
    }

    /// イベントループの実行
    pub async fn run(mut self) {
        loop {
            tokio::select! {
                Some(local_msg) = self.local_rx.recv() => {
                    self.handle_local(local_msg).await;
                }
                Some(msg) = self.peer_rx.recv() => {
                    self.handle_peer(msg).await;
                }
                else => break,
            }
        }
    }

    /// 自ノードからの新規メッセージ発信処理 (FromLocal)
    async fn handle_local(&mut self, local_msg: OutgoingLocalMessage) {
        let msg = self.create_signed_message(local_msg);
        self.cache.check_and_insert(&msg.id);
        self.broadcast(&msg);
    }

    /// ピアから受信したメッセージのパイプライン処理 (FromPeer)
    async fn handle_peer(&mut self, msg: GossipMessage) {
        // 1. 署名検証
        if !msg.verify() {
            return;
        }

        // 2. 既読チェック（重複排除）
        if !self.cache.check_and_insert(&msg.id) {
            return;
        }

        // 3. 他ネットワークのパケットであってもゴシップ網の接続性のためにリレー
        self.maybe_relay(&msg);

        // 4. 自ネットワークに属するメッセージのみ上位層へ配送
        self.maybe_deliver_upper(msg).await;
    }

    /// メッセージ作成ヘルパー
    fn create_signed_message(&self, local_msg: OutgoingLocalMessage) -> GossipMessage {
        let id = Uuid::new_v4().to_string();
        GossipMessage::new_signed(
            &self.keypair.signing_key,
            self.network_id,
            self.keypair.address,
            local_msg.recipient,
            id,
            local_msg.payload,
            self.default_ttl,
        )
    }

    /// TTLが残っていれば減衰させて再拡散
    fn maybe_relay(&self, msg: &GossipMessage) {
        if msg.ttl > 1 {
            let mut relay_msg = msg.clone();
            relay_msg.ttl -= 1;
            self.broadcast(&relay_msg);
        }
    }

    /// 自ネットワークIDと一致する場合のみ上位層へ配信
    async fn maybe_deliver_upper(&self, msg: GossipMessage) {
        if msg.network_id == self.network_id {
            let _ = self.upper_tx.send(msg).await;
        }
    }

    /// 下位ピアへのメッセージブロードキャスト
    fn broadcast(&self, msg: &GossipMessage) {
        let Ok(encoded) = msg.encode() else {
            return;
        };
        let bytes = Bytes::from(encoded);

        for peer_addr in self.lower_peers.clone() {
            let factory = self.lower_factory.clone();
            let data = bytes.clone();

            tokio::spawn(async move {
                if let Ok(stream) = factory.connect(peer_addr).await {
                    let mut framed = Framed::new(stream, LengthDelimitedCodec::new());
                    let _ = framed.send(data).await;
                }
            });
        }
    }
}

/// 下位ストリームから受信したフレームをデコードし、ゴシップノードの peer_tx に渡すリスナー関数
pub async fn process_inbound_stream<S>(
    stream: S,
    peer_tx: mpsc::Sender<GossipMessage>,
) -> std::io::Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let mut framed = Framed::new(stream, LengthDelimitedCodec::new());
    while let Some(result) = framed.next().await {
        match result {
            Ok(bytes) => {
                if let Ok(msg) = GossipMessage::decode(&bytes)
                    && peer_tx.send(msg).await.is_err()
                {
                    break;
                }
            }
            Err(e) => return Err(e),
        }
    }
    Ok(())
}
