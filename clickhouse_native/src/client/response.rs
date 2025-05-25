use std::pin::Pin;

use futures_util::Stream;
use futures_util::stream::StreamExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::trace;

use super::ClientFormat;
use crate::prelude::{ATT_CID, ATT_QID};
use crate::{Qid, Result};

pub(crate) fn create_response_stream<T: ClientFormat>(
    rx: mpsc::Receiver<Result<T::Data>>,
    qid: Qid,
    client_id: u16,
) -> impl Stream<Item = Result<T::Data>> + 'static {
    ReceiverStream::new(rx).inspect(move |response| {
        trace!(?response, { ATT_CID } = client_id, { ATT_QID } = %qid, "received response");
    })
}

pub(crate) fn handle_insert_response<T: ClientFormat>(
    rx: mpsc::Receiver<Result<T::Data>>,
    qid: Qid,
    client_id: u16,
) -> impl Stream<Item = Result<()>> + 'static {
    ReceiverStream::new(rx).filter_map(move |response| async move {
        trace!(
            ?response,
            { ATT_CID } = client_id,
            { ATT_QID } = %qid,
            "received insert response"
        );
        match response {
            Ok(_) => None,
            Err(e) => Some(Err(e)),
        }
    })
}

#[pin_project::pin_project]
pub struct ClickhouseResponse<T> {
    #[pin]
    stream: Pin<Box<dyn Stream<Item = Result<T>> + Send + 'static>>,
}

impl<T> ClickhouseResponse<T> {
    pub fn new(stream: Pin<Box<dyn Stream<Item = Result<T>> + Send + 'static>>) -> Self {
        Self { stream }
    }

    pub fn from_stream<S>(stream: S) -> Self
    where
        S: Stream<Item = Result<T>> + Send + 'static,
    {
        Self::new(Box::pin(stream))
    }
}

impl<T> Stream for ClickhouseResponse<T>
where
    T: Send + 'static,
{
    type Item = Result<T>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.project().stream.poll_next(cx)
    }
}
