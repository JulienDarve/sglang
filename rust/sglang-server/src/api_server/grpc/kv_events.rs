//! The `sglang.kv.v1.KvEvents` service: this listener's scheduler's KV cache
//! events, with the sequence numbers of its ZMQ publisher, and a snapshot of
//! its device cache's blocks.

mod ledger;
mod relay;
mod snapshot;
#[cfg(test)]
mod tests;
mod wire;

use std::convert::Infallible;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use sglang_kv_types::kv::v1 as pb;
use sglang_kv_types::kv::v1::kv_events_server::KvEvents;
use sglang_kv_types::kv::v1::subscribe_kv_events_response::Item;
use tokio::sync::{mpsc, watch};
use tonic::{Request, Response, Status};

pub(crate) use self::relay::KvEventRelay;
use self::relay::{KvEventReader, RETRY_DELAY, ReadError};
use self::snapshot::Blocks;
use super::service::ResponseStream;

/// Snapshot responses built ahead of the client reading them. Arbitrary.
const SNAPSHOT_RESPONSES_AHEAD: usize = 2;
/// How long a client may take to read a snapshot response before the snapshot
/// is dropped. Arbitrary.
const SNAPSHOT_READ_TIMEOUT: Duration = Duration::from_secs(30);
/// How long a snapshot waits for the blocks to hold every batch the relay has
/// forwarded. Arbitrary.
const SNAPSHOT_FOLD_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) struct KvEventsService {
    /// `None` when this listener's scheduler publishes no KV events over ZMQ.
    relay: Option<Arc<KvEventRelay>>,
    /// The device cache's blocks, kept by `follow_blocks`, and the number after
    /// the last batch folded into them; `None` when `grpc_snapshot` is false.
    blocks: Option<(Arc<Mutex<Blocks>>, watch::Receiver<u64>)>,
    /// Held while a snapshot is sent: each holds a copy of `blocks`.
    snapshot_gate: Arc<tokio::sync::Mutex<()>>,
    /// Turns true to end every stream, which a graceful shutdown would
    /// otherwise wait for.
    stop: watch::Receiver<bool>,
}

impl KvEventsService {
    /// Call from within the runtime: spawns `follow_blocks`.
    pub(crate) fn new(relay: Option<KvEventRelay>, stop: watch::Receiver<bool>) -> Self {
        let relay = relay.map(Arc::new);
        let snapshot_relay = relay.as_ref().filter(|relay| relay.grpc_snapshot);
        let blocks = snapshot_relay.map(|relay| {
            let blocks = Arc::<Mutex<Blocks>>::default();
            let (folded, folded_to) = watch::channel(0);
            tokio::spawn(follow_blocks(relay.clone(), blocks.clone(), folded));
            (blocks, folded_to)
        });
        Self {
            relay,
            blocks,
            snapshot_gate: Arc::default(),
            stop,
        }
    }

    /// The relay, when it serves `data_parallel_rank`.
    fn relay_for(&self, data_parallel_rank: u32) -> Result<&KvEventRelay, Status> {
        let relay = self.relay.as_ref().ok_or_else(|| {
            Status::failed_precondition(
                "this server's scheduler publishes no KV cache events over ZMQ; enable them \
                 with --kv-events-config and the zmq publisher",
            )
        })?;
        if data_parallel_rank != relay.dp_rank {
            return Err(Status::invalid_argument(format!(
                "this listener serves data parallel rank {}, not {data_parallel_rank}",
                relay.dp_rank
            )));
        }
        Ok(relay)
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
        let relay = self.relay_for(request.data_parallel_rank)?;
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

    type GetKvSnapshotStream = ResponseStream<pb::GetKvSnapshotResponse>;

    async fn get_kv_snapshot(
        &self,
        request: Request<pb::GetKvSnapshotRequest>,
    ) -> Result<Response<Self::GetKvSnapshotStream>, Status> {
        let relay = self.relay_for(request.into_inner().data_parallel_rank)?;
        let (blocks, mut folded_to) = self.blocks.clone().ok_or_else(|| {
            Status::failed_precondition(
                "KV cache snapshots are off: `grpc_snapshot` is false in --kv-events-config",
            )
        })?;
        // A client that subscribed before this call drops the batches below the
        // cursor, so the snapshot must hold every batch forwarded by now.
        let forwarded = relay.forwarded().unwrap_or(0);
        let caught_up = folded_to.wait_for(|folded_to| *folded_to >= forwarded);
        if !matches!(
            tokio::time::timeout(SNAPSHOT_FOLD_TIMEOUT, caught_up).await,
            Ok(Ok(_))
        ) {
            return Err(Status::unavailable(
                "the KV cache snapshot is behind the KV event stream; retry",
            ));
        }
        let gate = self.snapshot_gate.clone().lock_owned().await;
        let stream_id = relay.stream_id.clone();
        let (built, received) = mpsc::channel(SNAPSHOT_RESPONSES_AHEAD);
        let runtime = tokio::runtime::Handle::current();
        // The snapshot copies the ledger and walks every block.
        tokio::task::spawn_blocking(move || {
            let _gate = gate;
            let snapshot = lock(&blocks).snapshot(stream_id);
            for response in snapshot {
                // A client that went away or stopped reading must not hold up
                // the next snapshot.
                let sent = built.send_timeout(response, SNAPSHOT_READ_TIMEOUT);
                if runtime.block_on(sent).is_err() {
                    break;
                }
            }
        });
        Ok(Response::new(Box::pin(snapshot_responses(
            received,
            self.stop.clone(),
        ))))
    }
}

/// Keeps `blocks` in step with the relay's batches, from the first.
async fn follow_blocks(
    relay: Arc<KvEventRelay>,
    blocks: Arc<Mutex<Blocks>>,
    folded: watch::Sender<u64>,
) {
    // As the blocks' cursor: it never moves back.
    let folded_to = |next: u64| folded.send_modify(|folded_to| *folded_to = (*folded_to).max(next));
    // The batch to read on from; `None` once the batches before the live ones are lost.
    let mut next = Some(0);
    loop {
        let read: Result<Infallible, ReadError> = async {
            let cursor = next.map(|next_sequence_number| pb::KvEventCursor {
                stream_id: String::new(),
                next_sequence_number,
            });
            let mut reader = relay.reader(cursor).await?;
            if next.is_none() {
                tracing::warn!(
                    "lost KV event batches; snapshots are incomplete until the cache is cleared"
                );
                let live = reader.next_sequence_number().unwrap_or(0);
                lock(&blocks).lost(live);
                folded_to(live);
            }
            loop {
                let batch = reader.next_batch().await?;
                let after = batch.sequence_number + 1;
                next = Some(after);
                // The fold walks every block of the batch: keep it off the API
                // runtime. One that does not finish leaves the blocks unknown.
                let blocks = blocks.clone();
                let fold = tokio::task::spawn_blocking(move || lock(&blocks).apply(&batch));
                fold.await.map_err(|_| ReadError::DataLoss)?;
                folded_to(after);
            }
        }
        .await;
        let Err(error) = read;
        match error {
            ReadError::OutOfRange | ReadError::DataLoss => next = None,
            ReadError::ReplayFailed(error) => {
                tracing::warn!(%error, "the engine's KV event replay failed; retrying");
                tokio::time::sleep(RETRY_DELAY).await;
            }
            ReadError::RelayStopped => return,
        }
    }
}

/// The blocks, also after a fold panicked with the lock: `follow_blocks` then
/// marks them lost.
fn lock(blocks: &Mutex<Blocks>) -> MutexGuard<'_, Blocks> {
    blocks.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The snapshot's responses as they are built, until the last or shutdown.
fn snapshot_responses(
    mut responses: mpsc::Receiver<pb::GetKvSnapshotResponse>,
    mut stop: watch::Receiver<bool>,
) -> impl futures::Stream<Item = Result<pb::GetKvSnapshotResponse, Status>> + Send + 'static {
    async_stream::try_stream! {
        loop {
            let response = tokio::select! {
                // Shutdown first, so ready responses cannot delay it.
                biased;
                _ = stop.wait_for(|stop| *stop) => {
                    Err(Status::unavailable("the server is shutting down"))
                }
                response = responses.recv() => Ok(response),
            }?;
            match response {
                Some(response) => yield response,
                None => break,
            }
        }
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
                _ = stop.wait_for(|stop| *stop) => {
                    Err(Status::unavailable("the server is shutting down"))
                }
                batch = reader.next_batch() => batch.map_err(read_status),
            }?;
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
            "KV event batches after the last one sent can no longer be recovered from the \
             engine's replay buffer",
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
