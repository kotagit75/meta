use crate::stream::LayerStream;
use serde::{de::DeserializeOwned, Serialize};
use std::fmt::Debug;
use std::future::Future;
use std::hash::Hash;

pub trait Factory: Clone + Send + Sync + Unpin + 'static {
    type Stream: LayerStream + Send + Unpin + 'static;
    type Address: Clone + Send + Sync + Unpin + Serialize + DeserializeOwned + Eq + Hash + Debug + 'static;

    fn connect(
        &self,
        addr: Self::Address,
    ) -> impl Future<Output = std::io::Result<Self::Stream>> + Send;
}
