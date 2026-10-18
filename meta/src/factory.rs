use crate::stream::LayerStream;
use std::future::Future;

pub trait Factory: Clone + Send + Sync + Unpin + 'static {
    type Stream: LayerStream + Send + Unpin + 'static;
    type Address: Clone + Send + Sync + Unpin + 'static;

    fn connect(
        &self,
        addr: Self::Address,
    ) -> impl Future<Output = std::io::Result<Self::Stream>> + Send;
}
