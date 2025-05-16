use std::pin::Pin;
use std::sync::Arc;

use futures_util::Stream;
use futures_util::stream::StreamExt;
use strum::AsRefStr;
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tracing::trace;

use super::{ClientFormat, ProfileEvent};
use crate::prelude::ATT_CID;
use crate::{ClickhouseEvent, ClickhouseNativeError, Event, Progress, Qid, Result, ServerError};

/// Internal response type
#[derive(Debug, AsRefStr)]
pub(crate) enum Response<T: Send + Sync + 'static> {
    Data(T),
    Profile(Vec<ProfileEvent>),
    Progress(Progress),
    Exception(ServerError),
    ClientError(ClickhouseNativeError),
}

pub(crate) fn create_response_stream<T: ClientFormat>(
    rx: mpsc::Receiver<Response<T::Data>>,
    events: Arc<broadcast::Sender<Event>>,
    qid: Qid,
    client_id: u16,
) -> impl Stream<Item = Result<T::Data>> + 'static {
    ReceiverStream::new(rx).map(move |packet| (packet, Arc::clone(&events))).filter_map(
        move |(response, events)| async move {
            trace!(response = response.as_ref(), { ATT_CID } = client_id, "received response");
            match response {
                Response::Data(data) => Some(Ok(data)),
                Response::Exception(exception) => Some(Err(exception.into())),
                Response::ClientError(error) => Some(Err(error)),
                Response::Progress(progress) => {
                    let event = ClickhouseEvent::Progress(progress);
                    let _ = events.send(Event { event, qid, client_id }).ok();
                    None
                }
                Response::Profile(profile) => {
                    let event = ClickhouseEvent::Profile(profile);
                    let _ = events.send(Event { event, qid, client_id }).ok();
                    None
                }
            }
        },
    )
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
