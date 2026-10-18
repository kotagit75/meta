use tokio::io::{AsyncRead, AsyncWrite};

pub trait LayerStream: AsyncRead + AsyncWrite {}

impl<T> LayerStream for T where T: AsyncRead + AsyncWrite {}
