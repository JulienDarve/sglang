//! The `sglang.kv.v1.KvEvents` service: this listener's scheduler's KV cache
//! events, with the sequence numbers of its ZMQ publisher.

mod relay;
#[cfg(test)]
mod tests;
mod wire;

use std::sync::Arc;

use sglang_kv_types::kv::v1 as pb;
use sglang_kv_types::kv::v1::kv_events_server::KvEvents;
use sglang_kv_types::kv::v1::subscribe_kv_events_response::Item;
use tokio::sync::watch;
use tonic::{Request, Response, Status};

pub(crate) use self::relay::KvEventRelay;
use self::relay::{KvEventReader, ReadError};
use super::service::ResponseStream;

pub(crate) struct KvEventsService {
    /// `None` when this listener's scheduler publishes no KV events over ZMQ.
    relay: Option<KvEventRelay>,
    /// Turns true to end every stream, which a graceful shutdown would
    /// otherwise wait for.
    stop: watch::Receiver<bool>,
}

impl KvEventsService {
    pub(crate) fn new(relay: Option<KvEventRelay>, stop: watch::Receiver<bool>) -> Self {
        Self { relay, stop }
    }
}

#[tonic::async_trait]
impl KvEvents for KvEventsService {
    type SubscribeKvEventsStream = ResponseStream<pb::SubscribeKvEventsResponse>;

    async fn subscribe_kv_events(
        &self,
        request: Request<pb::SubscribeKvEventsRequest>,
    ) -> Result<Response<Self::SubscribeKvEventsStream>, Status> {
        let request = request.into_inner();
        let relay = self.relay.as_ref().ok_or_else(|| {
            Status::failed_precondition(
                "this server's scheduler publishes no KV cache events over ZMQ; enable them \
                 with --kv-events-config and the zmq publisher",
            )
        })?;
        if request.data_parallel_rank != relay.dp_rank {
            return Err(Status::invalid_argument(format!(
                "this listener serves data parallel rank {}, not {}",
                relay.dp_rank, request.data_parallel_rank
            )));
        }
        let reader = relay.reader(request.resume).await.map_err(read_status)?;
        let start = pb::KvEventStreamStart {
            stream_id: relay.stream_id.clone(),
            data_parallel_rank: relay.dp_rank,
        };
        Ok(Response::new(Box::pin(responses(
            start,
            reader,
            self.stop.clone(),
        ))))
    }
}

/// `start`, then the reader's batches until an error or shutdown.
fn responses(
    start: pb::KvEventStreamStart,
    mut reader: KvEventReader,
    mut stop: watch::Receiver<bool>,
) -> impl futures::Stream<Item = Result<pb::SubscribeKvEventsResponse, Status>> + Send + 'static {
    async_stream::try_stream! {
        yield response(Item::Start(start));
        loop {
            let batch = tokio::select! {
                _ = stop.wait_for(|stop| *stop) => None,
                batch = reader.next_batch() => Some(batch),
            };
            let batch = match batch {
                Some(batch) => batch.map_err(read_status)?,
                None => Err(Status::unavailable("the server is shutting down"))?,
            };
            yield response(Item::Batch(Arc::unwrap_or_clone(batch)));
        }
    }
}

fn read_status(error: ReadError) -> Status {
    match error {
        ReadError::OutOfRange => Status::out_of_range(
            "the cursor's KV event batches are no longer in the engine's replay buffer, or its \
             stream is not the current one",
        ),
        ReadError::DataLoss => Status::data_loss(
            "KV event batches after the last one sent are no longer in the engine's replay buffer",
        ),
        ReadError::ReplayFailed(error) => {
            Status::unavailable(format!("the engine's KV event replay failed: {error}"))
        }
        ReadError::RelayStopped => Status::unavailable("the KV event relay stopped"),
    }
}

fn response(item: Item) -> pb::SubscribeKvEventsResponse {
    pb::SubscribeKvEventsResponse { item: Some(item) }
}
