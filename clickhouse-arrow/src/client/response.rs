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

#[cfg(test)]
mod tests {
    use futures_util::StreamExt;
    use tokio::sync::{mpsc, oneshot};

    use super::*;
    use crate::Error;
    use crate::formats::NativeFormat;
    use crate::native::block::Block;

    #[tokio::test]
    async fn response_stream_passthrough_and_insert_error_filtering() {
        let qid = Qid::default();

        let (tx_data, rx_data) = mpsc::channel(4);
        tx_data.send(Ok(Block::default())).await.unwrap();
        tx_data.send(Err(Error::Protocol("boom".into()))).await.unwrap();
        drop(tx_data);

        let mut data_stream = Box::pin(create_response_stream::<NativeFormat>(rx_data, qid, 7));
        assert!(data_stream.next().await.unwrap().is_ok());
        assert!(matches!(
            data_stream.next().await.unwrap(),
            Err(Error::Protocol(msg)) if msg == "boom"
        ));
        assert!(data_stream.next().await.is_none());

        let (tx_insert, rx_insert) = mpsc::channel(4);
        tx_insert.send(Ok(Block::default())).await.unwrap();
        tx_insert.send(Err(Error::Protocol("insert-err".into()))).await.unwrap();
        drop(tx_insert);

        let mut insert_stream = Box::pin(handle_insert_response::<NativeFormat>(rx_insert, qid, 8));
        let item = insert_stream.next().await.expect("expected one error item");
        assert!(matches!(item, Err(Error::Protocol(msg)) if msg == "insert-err"));
        assert!(insert_stream.next().await.is_none());
    }

    #[tokio::test]
    async fn clickhouse_response_explain_roundtrip_and_channel_closed() {
        let (tx_explain, rx_explain) = oneshot::channel();
        tx_explain
            .send(Ok(ExplainResult::Text("plan".to_string())))
            .expect("oneshot send");

        let mut response =
            ClickHouseResponse::from_stream_with_explain(futures_util::stream::iter(vec![Ok(1)]), rx_explain);

        assert!(response.has_explain());
        let explain = response.explain().await.expect("explain should exist").unwrap();
        assert_eq!(explain.as_text(), Some("plan"));
        assert!(!response.has_explain());
        assert!(response.explain().await.is_none());
        assert_eq!(response.next().await.unwrap().unwrap(), 1);
        assert!(response.next().await.is_none());

        let (tx_dropped, rx_dropped) = oneshot::channel::<Result<ExplainResult>>();
        drop(tx_dropped);
        let mut dropped =
            ClickHouseResponse::<i32>::from_stream_with_explain(futures_util::stream::empty(), rx_dropped);
        let err = dropped.explain().await.expect("expected channel close error").unwrap_err();
        assert!(matches!(err, Error::ChannelClosed));
    }

    #[tokio::test]
    async fn clickhouse_response_new_and_from_stream_without_explain() {
        let mut from_stream = ClickHouseResponse::from_stream(futures_util::stream::iter(vec![
            Ok::<_, Error>(10),
            Ok::<_, Error>(20),
        ]));
        assert!(!from_stream.has_explain());
        assert_eq!(from_stream.next().await.unwrap().unwrap(), 10);
        assert_eq!(from_stream.next().await.unwrap().unwrap(), 20);
        assert!(from_stream.next().await.is_none());

        let stream = Box::pin(futures_util::stream::iter(vec![Ok::<_, Error>(30)]));
        let mut from_new = ClickHouseResponse::new(stream);
        assert_eq!(from_new.next().await.unwrap().unwrap(), 30);
        assert!(from_new.next().await.is_none());
    }
}
