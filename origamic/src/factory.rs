use crate::{overlay::message::GossipMessage, stream::LayerStream};
use serde::{Serialize, de::DeserializeOwned};
use std::fmt::Debug;
use std::future::Future;
use std::hash::Hash;
use tokio::sync::mpsc;

pub trait Factory: Clone + Send + Sync + Unpin + 'static {
    type Stream: LayerStream + Send + Unpin + 'static;
    type Address: Clone
        + Send
        + Sync
        + Unpin
        + Serialize
        + DeserializeOwned
        + Eq
        + Hash
        + Debug
        + 'static;

    fn connect(
        &self,
        addr: Self::Address,
    ) -> impl Future<Output = std::io::Result<Self::Stream>> + Send;

    /// インバウンドリスナーを起動して、受信した下位ストリームを peer_tx に供給する。
    fn start_listener(
        &self,
        bind_addr: Option<Self::Address>,
        peer_tx: mpsc::Sender<GossipMessage>,
    ) -> impl Future<Output = std::io::Result<()>> + Send {
        let _ = (bind_addr, peer_tx);
        async move { Ok(()) }
    }
}
