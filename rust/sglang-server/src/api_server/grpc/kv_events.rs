//! The `sglang.kv.v1.KvEvents` service: this listener's scheduler's KV cache
//! events, with the sequence numbers of its ZMQ publisher.

mod relay;
#[cfg(test)]
mod tests;
mod wire;

use std::sync::Arc;

use bytes::Bytes;
use sglang_kv_types::kv::v1 as pb;
use sglang_kv_types::kv::v1::kv_events_server::KvEvents;
use sglang_kv_types::kv::v1::subscribe_kv_events_response::Item;
use tokio::sync::{broadcast, watch};
use tonic::{Request, Response, Status};

pub(crate) use self::relay::KvEventRelay;
use self::relay::{ReplayError, Subscription};
use super::service::ResponseStream;

pub(crate) struct KvEventsService {
    /// `None` when this listener's scheduler publishes no KV events.
    relay: Option<Arc<KvEventRelay>>,
    /// Turns true to end every stream, which a graceful shutdown would
    /// otherwise wait for.
    stop: watch::Receiver<bool>,
}

impl KvEventsService {
    pub(crate) fn new(relay: Option<Arc<KvEventRelay>>, stop: watch::Receiver<bool>) -> Self {
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
        let relay = self.relay.clone().ok_or_else(|| {
            Status::failed_precondition(
                "KV cache events are off; enable them with --kv-events-config",
            )
        })?;
        if request.data_parallel_rank != relay.dp_rank {
            return Err(Status::invalid_argument(format!(
                "this listener serves data parallel rank {}, not {}",
                relay.dp_rank, request.data_parallel_rank
            )));
        }
        let subscription = relay
            .subscribe()
            .ok_or_else(|| Status::unavailable("the KV event relay stopped"))?;
        let mut next = subscription.next;
        let mut replayed = Vec::new();
        if let Some(cursor) = request.resume {
            if !cursor.stream_id.is_empty() && cursor.stream_id != subscription.stream_id {
                return Err(Status::out_of_range(format!(
                    "stream {} is not the current KV event stream",
                    cursor.stream_id
                )));
            }
            if cursor.next_sequence_number < next {
                replayed = relay
                    .replay(cursor.next_sequence_number)
                    .await
                    .map_err(replay_failed)?;
                if replayed.first().map(|(first, _)| *first) != Some(cursor.next_sequence_number) {
                    return Err(Status::out_of_range(format!(
                        "KV event batch {} is no longer in the engine's replay buffer",
                        cursor.next_sequence_number
                    )));
                }
            }
            next = cursor.next_sequence_number;
        }
        Ok(Response::new(Box::pin(responses(
            relay,
            subscription,
            next,
            replayed,
            self.stop.clone(),
        ))))
    }
}

/// `start`, the batches from `next` on: first `replayed`, then live ones,
/// caught up from the engine's replay buffer whenever the subscriber fell
/// behind.
fn responses(
    relay: Arc<KvEventRelay>,
    subscription: Subscription,
    mut next: u64,
    replayed: Vec<(u64, Bytes)>,
    mut stop: watch::Receiver<bool>,
) -> impl futures::Stream<Item = Result<pb::SubscribeKvEventsResponse, Status>> + Send + 'static {
    let Subscription {
        stream_id,
        mut live,
        ..
    } = subscription;
    async_stream::try_stream! {
        yield response(Item::Start(pb::KvEventStreamStart {
            stream_id,
            data_parallel_rank: relay.dp_rank,
        }));
        let replayed = from_next(next, replayed);
        next += replayed.len() as u64;
        for (sequence_number, payload) in replayed {
            yield response(Item::Batch(decode(sequence_number, &payload)?));
        }
        loop {
            let received = tokio::select! {
                _ = stop.wait_for(|stop| *stop) => None,
                received = live.recv() => Some(received),
            };
            let batch = match received {
                Some(Ok(batch)) => batch,
                // The next batch received is ahead, which catches up below.
                Some(Err(broadcast::error::RecvError::Lagged(_))) => continue,
                Some(Err(broadcast::error::RecvError::Closed)) => {
                    Err(Status::unavailable("the KV event relay stopped"))?
                }
                None => Err(Status::unavailable("the server is shutting down"))?,
            };
            if batch.sequence_number > next {
                let replayed = from_next(next, relay.replay(next).await.map_err(replay_failed)?);
                next += replayed.len() as u64;
                for (sequence_number, payload) in replayed {
                    yield response(Item::Batch(decode(sequence_number, &payload)?));
                }
                if batch.sequence_number > next {
                    Err(Status::data_loss(format!(
                        "KV event batch {next} is no longer in the engine's replay buffer"
                    )))?;
                }
            }
            if batch.sequence_number == next {
                next += 1;
                yield response(Item::Batch(Arc::unwrap_or_clone(batch)));
            }
        }
    }
}

/// The replayed batches from `next` on, up to the first one missing.
fn from_next(next: u64, replayed: Vec<(u64, Bytes)>) -> Vec<(u64, Bytes)> {
    replayed
        .into_iter()
        .skip_while(|(sequence_number, _)| *sequence_number < next)
        .zip(next..)
        .take_while(|((sequence_number, _), expected)| sequence_number == expected)
        .map(|(batch, _)| batch)
        .collect()
}

fn decode(sequence_number: u64, payload: &[u8]) -> Result<pb::KvEventBatch, Status> {
    wire::decode_batch(sequence_number, payload).map_err(|error| {
        Status::data_loss(format!(
            "KV event batch {sequence_number} cannot be decoded: {error}"
        ))
    })
}

fn response(item: Item) -> pb::SubscribeKvEventsResponse {
    pb::SubscribeKvEventsResponse { item: Some(item) }
}

fn replay_failed(error: ReplayError) -> Status {
    Status::unavailable(format!("KV event replay failed: {error}"))
}
