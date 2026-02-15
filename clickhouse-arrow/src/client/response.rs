use std::pin::Pin;

use futures_util::stream::StreamExt;
use futures_util::{Stream, TryStreamExt};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{error, trace};

use super::ClientFormat;
use crate::explain::ExplainResult;
use crate::prelude::{ATT_CID, ATT_QID};
use crate::{Qid, Result};

pub(crate) fn create_response_stream<T: ClientFormat>(
    rx: mpsc::Receiver<Result<T::Data>>,
    qid: Qid,
    cid: u16,
) -> impl Stream<Item = Result<T::Data>> + 'static {
    ReceiverStream::new(rx)
        .inspect_ok(move |_| trace!({ ATT_CID } = cid, { ATT_QID } = %qid, "response"))
        .inspect_err(move |error| error!(?error, { ATT_CID } = cid, { ATT_QID } = %qid, "response"))
}

pub(crate) fn handle_insert_response<T: ClientFormat>(
    rx: mpsc::Receiver<Result<T::Data>>,
    qid: Qid,
    cid: u16,
) -> impl Stream<Item = Result<()>> + 'static {
    ReceiverStream::new(rx)
        .inspect_ok(move |_| trace!({ ATT_CID } = cid, { ATT_QID } = %qid, "response"))
        .inspect_err(move |error| error!(?error, { ATT_CID } = cid, { ATT_QID } = %qid, "response"))
        .filter_map(move |response| async move {
            match response {
                Ok(_) => None,
                Err(e) => Some(Err(e)),
            }
        })
}

#[pin_project::pin_project]
pub struct ClickHouseResponse<T> {
    #[pin]
    stream: Pin<Box<dyn Stream<Item = Result<T>> + Send + 'static>>,
    explain_receiver: Option<oneshot::Receiver<Result<ExplainResult>>>,
}

impl<T> ClickHouseResponse<T> {
    pub fn new(stream: Pin<Box<dyn Stream<Item = Result<T>> + Send + 'static>>) -> Self {
        Self { stream, explain_receiver: None }
    }

    pub fn with_explain(
        stream: Pin<Box<dyn Stream<Item = Result<T>> + Send + 'static>>,
        explain_receiver: oneshot::Receiver<Result<ExplainResult>>,
    ) -> Self {
        Self { stream, explain_receiver: Some(explain_receiver) }
    }

    pub fn from_stream<S>(stream: S) -> Self
    where
        S: Stream<Item = Result<T>> + Send + 'static,
    {
        Self::new(Box::pin(stream))
    }

    pub fn from_stream_with_explain<S>(
        stream: S,
        explain_receiver: oneshot::Receiver<Result<ExplainResult>>,
    ) -> Self
    where
        S: Stream<Item = Result<T>> + Send + 'static,
    {
        Self::with_explain(Box::pin(stream), explain_receiver)
    }

    #[must_use]
    pub fn has_explain(&self) -> bool {
        self.explain_receiver.is_some()
    }

    pub async fn explain(&mut self) -> Option<Result<ExplainResult>> {
        let receiver = self.explain_receiver.take()?;
        match receiver.await {
            Ok(result) => Some(result),
            Err(_) => Some(Err(crate::Error::ChannelClosed)),
        }
    }
}

impl<T> Stream for ClickHouseResponse<T>
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
