//! Follows the scheduler's `ZmqEventPublisher` over its private PUB and ROUTER
//! sockets (`python/sglang/srt/disaggregation/kv_events.py`). The relay
//! forwards batches as the PUB socket delivers them and keeps no history:
//! each reader fills its own gaps from the publisher's replay buffer.
//!
//! The publisher runs in this process, yet the relay reads it over ZeroMQ so
//! that its replay buffer stays the only history and it gains only two binds.
//! vLLM's Rust server, a separate process from its engine, relays the same
//! way, so one design and one client contract serve both engines.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use sglang_kv_types::kv::v1 as pb;
use tokio::sync::{broadcast, watch};
use zeromq::{DealerSocket, Socket, SocketRecv, SocketSend, SubSocket, ZmqMessage};

use super::wire::decode_batch;
use crate::message::config::KvEventRelayConfig;

/// Live batches a reader may fall behind by before it is caught up from the
/// publisher's replay buffer. Arbitrary.
const LIVE_CAPACITY: usize = 1024;
/// Pause after a failed read from the publisher. Arbitrary.
pub(super) const RETRY_DELAY: Duration = Duration::from_secs(1);
/// How long the publisher may take to send the next frame of a replay. Arbitrary.
const REPLAY_TIMEOUT: Duration = Duration::from_secs(5);
/// `ZmqEventPublisher.END_SEQ`, -1 as a big-endian i64: ends a replay.
const REPLAY_END: u64 = u64::MAX;

/// Why a reader cannot start or continue.
#[derive(Debug)]
pub(super) enum ReadError {
    /// The cursor's batches are no longer buffered, or its stream is not the
    /// current one.
    OutOfRange,
    /// Batches after the last one read can no longer be recovered.
    DataLoss,
    /// The publisher's replay socket did not answer.
    ReplayFailed(PublisherError),
    /// The relay stopped with the server.
    RelayStopped,
}

pub(crate) struct KvEventRelay {
    pub(super) dp_rank: u32,
    /// Fixed for the life of the relay, which is that of the scheduler.
    pub(super) stream_id: String,
    /// Whether `GetKvSnapshot` is served.
    pub(super) grpc_snapshot: bool,
    live: broadcast::Sender<Arc<pb::KvEventBatch>>,
    /// The number after the last batch forwarded; `None` before the first.
    forwarded: watch::Receiver<Option<u64>>,
    replay_endpoint: Arc<str>,
}

impl KvEventRelay {
    /// Follow the publisher until the runtime stops.
    pub(crate) fn start(config: &KvEventRelayConfig) -> Self {
        let (live, _) = broadcast::channel(LIVE_CAPACITY);
        let (forwarded_sender, forwarded) = watch::channel(None);
        tokio::spawn(follow(
            config.endpoint.clone(),
            live.clone(),
            forwarded_sender,
        ));
        Self {
            dp_rank: config.dp_rank,
            stream_id: uuid::Uuid::new_v4().to_string(),
            grpc_snapshot: config.grpc_snapshot,
            live,
            forwarded,
            replay_endpoint: config.replay_endpoint.as_str().into(),
        }
    }

    /// A reader of live batches, first replaying the publisher's buffered
    /// batches from `resume` when given.
    pub(super) async fn reader(
        &self,
        resume: Option<pb::KvEventCursor>,
    ) -> Result<KvEventReader, ReadError> {
        // Subscribe before replaying, so no batch published meanwhile is missed.
        let live = self.live.subscribe();
        let mut reader = KvEventReader {
            live,
            replay_endpoint: self.replay_endpoint.clone(),
            // Live from the next batch the relay forwards.
            next: *self.forwarded.borrow(),
            forwarded: self.forwarded.clone(),
            pending: VecDeque::new(),
        };
        if let Some(cursor) = resume {
            if !cursor.stream_id.is_empty() && cursor.stream_id != self.stream_id {
                return Err(ReadError::OutOfRange);
            }
            reader.next = Some(cursor.next_sequence_number);
            reader
                .catch_up(cursor.next_sequence_number, ReadError::OutOfRange)
                .await?;
        }
        Ok(reader)
    }
}

async fn follow(
    endpoint: String,
    live: broadcast::Sender<Arc<pb::KvEventBatch>>,
    forwarded: watch::Sender<Option<u64>>,
) {
    // The publisher binds before this server starts, so no failure is expected;
    // this task has no caller to return one to, so it logs and retries.
    let mut socket = loop {
        match subscribe(&endpoint).await {
            Ok(socket) => break socket,
            Err(error) => {
                tracing::warn!(%error, endpoint, "cannot subscribe to the KV event publisher");
                tokio::time::sleep(RETRY_DELAY).await;
            }
        }
    };
    loop {
        let (sequence_number, payload) = match next_published(&mut socket).await {
            Ok(published) => published,
            Err(error) => {
                tracing::warn!(%error, "failed to read KV events from the scheduler");
                tokio::time::sleep(RETRY_DELAY).await;
                continue;
            }
        };
        forwarded.send_replace(Some(sequence_number + 1));
        // A reader subscribes before it reads `forwarded`: it is counted here,
        // or it starts after this batch.
        if live.receiver_count() == 0 {
            continue;
        }
        // A batch that cannot be decoded is a gap that readers cannot fill.
        let batch = match decode_batch(sequence_number, &payload) {
            Ok(batch) => batch,
            Err(error) => {
                tracing::warn!(sequence_number, %error, "dropping an undecodable KV event batch");
                continue;
            }
        };
        // No receiver is not an error: readers come and go.
        let _ = live.send(Arc::new(batch));
    }
}

async fn subscribe(endpoint: &str) -> Result<SubSocket, zeromq::ZmqError> {
    let mut socket = SubSocket::new();
    socket.connect(endpoint).await?;
    socket.subscribe("").await?;
    Ok(socket)
}

/// The next batch the publisher sends, as `(sequence number, payload)`.
async fn next_published(socket: &mut SubSocket) -> Result<(u64, Bytes), PublisherError> {
    let message = socket.recv().await?;
    published_batch(&message.into_vec()).ok_or(PublisherError::Malformed)
}

/// One subscriber's position in the relay's stream.
pub(super) struct KvEventReader {
    live: broadcast::Receiver<Arc<pb::KvEventBatch>>,
    replay_endpoint: Arc<str>,
    /// The sequence number of the next batch to yield; `None` until the relay
    /// forwards its first batch.
    next: Option<u64>,
    forwarded: watch::Receiver<Option<u64>>,
    /// Replayed batches to yield before live ones, decoded when yielded.
    pending: VecDeque<(u64, Bytes)>,
}

impl KvEventReader {
    /// The reader's `next`.
    pub(super) fn next_sequence_number(&self) -> Option<u64> {
        self.next
    }

    /// The next batch, each sequence number once and in order.
    pub(super) async fn next_batch(&mut self) -> Result<Arc<pb::KvEventBatch>, ReadError> {
        loop {
            if let Some((sequence_number, payload)) = self.pending.pop_front() {
                // As in `follow`, an undecodable batch is a gap that cannot be filled.
                let batch =
                    decode_batch(sequence_number, &payload).map_err(|_| ReadError::DataLoss)?;
                self.next = Some(sequence_number + 1);
                return Ok(Arc::new(batch));
            }
            match self.live.recv().await {
                Ok(batch) => match self.next {
                    // A replay already yielded it.
                    Some(next) if batch.sequence_number < next => continue,
                    // The PUB socket dropped the batches before this one.
                    Some(next) if batch.sequence_number > next => {}
                    _ => {
                        self.next = Some(batch.sequence_number + 1);
                        return Ok(batch);
                    }
                },
                // The subscriber read slower than the relay forwarded.
                Err(broadcast::error::RecvError::Lagged(_)) => {}
                Err(broadcast::error::RecvError::Closed) => return Err(ReadError::RelayStopped),
            }
            // A subscriber that lagged before its first batch has no position.
            let next = self.next.ok_or(ReadError::DataLoss)?;
            // The publisher buffers a batch before it serves the next replay,
            // so the replay holds the live batches this reader has received.
            self.catch_up(next, ReadError::DataLoss).await?;
        }
    }

    /// Queue the publisher's buffered batches from `next` on. `missing` is the
    /// error when the buffer cannot continue the stream from `next`.
    async fn catch_up(&mut self, next: u64, missing: ReadError) -> Result<(), ReadError> {
        // Read before the replay: a batch published after it is not missing.
        let published = self.forwarded.borrow().is_some_and(|end| next < end);
        let batches = replay(&self.replay_endpoint, next)
            .await
            .map_err(ReadError::ReplayFailed)?;
        // Not so when the buffer starts later or the publisher skipped a number.
        let contiguous = batches
            .iter()
            .zip(next..)
            .all(|((sequence_number, _), expected)| *sequence_number == expected);
        if !contiguous || (batches.is_empty() && published) {
            return Err(missing);
        }
        self.pending = batches.into();
        Ok(())
    }
}

/// Why the publisher's next batch cannot be read.
#[derive(Debug, thiserror::Error)]
pub(super) enum PublisherError {
    #[error(transparent)]
    Zmq(#[from] zeromq::ZmqError),
    #[error("the publisher sent no replay frame within {REPLAY_TIMEOUT:?}")]
    Timeout,
    #[error("malformed frame")]
    Malformed,
}

/// The publisher's buffered batches from `start` on, in order. Read in full
/// before any is used, so a slow gRPC client cannot hold up the publisher.
async fn replay(endpoint: &str, start: u64) -> Result<Vec<(u64, Bytes)>, PublisherError> {
    let mut socket = DealerSocket::new();
    socket.connect(endpoint).await?;
    // The empty frame is the REQ envelope the publisher's ROUTER expects.
    let request = vec![Bytes::new(), Bytes::copy_from_slice(&start.to_be_bytes())];
    socket
        .send(ZmqMessage::try_from(request).expect("two frames"))
        .await?;
    let mut batches = Vec::new();
    loop {
        let message = tokio::time::timeout(REPLAY_TIMEOUT, socket.recv())
            .await
            .map_err(|_| PublisherError::Timeout)??;
        // A published batch's frames follow the empty REP envelope.
        match message.into_vec().get(1..).and_then(published_batch) {
            Some((REPLAY_END, _)) => return Ok(batches),
            Some(batch) => batches.push(batch),
            None => return Err(PublisherError::Malformed),
        }
    }
}

/// `(sequence number, payload)` of the frames `[topic, sequence number, payload]`.
fn published_batch(frames: &[Bytes]) -> Option<(u64, Bytes)> {
    let [_topic, sequence_number, payload] = frames else {
        return None;
    };
    let sequence_number = u64::from_be_bytes(sequence_number.as_ref().try_into().ok()?);
    Some((sequence_number, payload.clone()))
}
