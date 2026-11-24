//! Follows the scheduler's `ZmqEventPublisher` over its private PUB and ROUTER
//! sockets (`python/sglang/srt/disaggregation/kv_events.py`). The relay
//! forwards batches as the PUB socket delivers them and keeps no history:
//! each reader fills its own gaps from the publisher's replay buffer.

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
/// publisher's replay buffer.
const LIVE_CAPACITY: usize = 1024;
/// Pause after a failed read from the publisher. Arbitrary.
const RETRY_DELAY: Duration = Duration::from_secs(1);
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
    ReplayFailed(String),
    /// The relay stopped with the server.
    RelayStopped,
}

/// The sequence numbers the relay has forwarded.
#[derive(Clone, Copy)]
struct Forwarded {
    first: u64,
    next: u64,
}

pub(crate) struct KvEventRelay {
    pub(super) dp_rank: u32,
    /// Fixed for the life of the relay, which is that of the scheduler.
    stream_id: Arc<str>,
    live: broadcast::Sender<Arc<pb::KvEventBatch>>,
    forwarded: watch::Receiver<Option<Forwarded>>,
    replay_endpoint: Arc<str>,
}

impl KvEventRelay {
    /// Follow the publisher until the runtime stops.
    pub(crate) fn start(config: &KvEventRelayConfig) -> Arc<Self> {
        Self::start_with_capacity(config, LIVE_CAPACITY)
    }

    pub(super) fn start_with_capacity(
        config: &KvEventRelayConfig,
        live_capacity: usize,
    ) -> Arc<Self> {
        let (live, _) = broadcast::channel(live_capacity);
        let (forwarded_sender, forwarded) = watch::channel(None);
        tokio::spawn(follow(
            config.endpoint.clone(),
            live.clone(),
            forwarded_sender,
        ));
        Arc::new(Self {
            dp_rank: config.dp_rank,
            stream_id: uuid::Uuid::new_v4().to_string().into(),
            live,
            forwarded,
            replay_endpoint: config.replay_endpoint.as_str().into(),
        })
    }

    pub(super) fn stream_id(&self) -> &str {
        &self.stream_id
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
            next: self.forwarded.borrow().map(|forwarded| forwarded.next),
            forwarded: self.forwarded.clone(),
            pending: VecDeque::new(),
        };
        if let Some(cursor) = resume {
            if !cursor.stream_id.is_empty() && *cursor.stream_id != *self.stream_id {
                return Err(ReadError::OutOfRange);
            }
            reader.next = Some(cursor.next_sequence_number);
            reader.catch_up(ReadError::OutOfRange).await?;
        }
        Ok(reader)
    }

    #[cfg(test)]
    pub(super) fn forwarded_next(&self) -> u64 {
        self.forwarded
            .borrow()
            .map_or(0, |forwarded| forwarded.next)
    }
}

async fn follow(
    endpoint: String,
    live: broadcast::Sender<Arc<pb::KvEventBatch>>,
    forwarded: watch::Sender<Option<Forwarded>>,
) {
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
        let message = match socket.recv().await {
            Ok(message) => message,
            Err(error) => {
                tracing::warn!(%error, "failed to read KV events from the scheduler");
                tokio::time::sleep(RETRY_DELAY).await;
                continue;
            }
        };
        let Some((sequence_number, payload)) = published_batch(message, 0) else {
            tracing::warn!("dropping a malformed KV event message");
            continue;
        };
        // A batch that cannot be decoded is a gap that readers cannot fill.
        let batch = match decode_batch(sequence_number, &payload) {
            Ok(batch) => batch,
            Err(error) => {
                tracing::warn!(sequence_number, %error, "dropping an undecodable KV event batch");
                continue;
            }
        };
        forwarded.send_modify(|forwarded| {
            let first = forwarded.map_or(sequence_number, |forwarded| forwarded.first);
            *forwarded = Some(Forwarded {
                first,
                next: sequence_number + 1,
            });
        });
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

/// One subscriber's position in the relay's stream.
pub(super) struct KvEventReader {
    live: broadcast::Receiver<Arc<pb::KvEventBatch>>,
    replay_endpoint: Arc<str>,
    /// The sequence number of the next batch to yield; `None` until the relay
    /// forwards its first batch.
    next: Option<u64>,
    forwarded: watch::Receiver<Option<Forwarded>>,
    /// Replayed batches to yield before live ones.
    pending: VecDeque<Arc<pb::KvEventBatch>>,
}

impl KvEventReader {
    /// The next batch, each sequence number once and in order.
    pub(super) async fn next_batch(&mut self) -> Result<Arc<pb::KvEventBatch>, ReadError> {
        loop {
            if let Some(batch) = self.pending.pop_front() {
                self.next = Some(batch.sequence_number + 1);
                return Ok(batch);
            }
            let batch = match self.live.recv().await {
                Ok(batch) => batch,
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    // A reader that started before the relay's first batch
                    // missed everything from that one on.
                    let first = self.forwarded.borrow().map(|forwarded| forwarded.first);
                    self.next = self.next.or(first);
                    self.catch_up(ReadError::DataLoss).await?;
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => return Err(ReadError::RelayStopped),
            };
            let sequence_number = batch.sequence_number;
            match self.next {
                Some(next) if sequence_number < next => {}
                Some(next) if sequence_number > next => {
                    self.catch_up(ReadError::DataLoss).await?;
                    if self
                        .pending
                        .back()
                        .is_none_or(|last| last.sequence_number < sequence_number)
                    {
                        self.pending.push_back(batch);
                    }
                }
                _ => {
                    self.next = Some(sequence_number + 1);
                    return Ok(batch);
                }
            }
        }
    }

    /// Queue the publisher's buffered batches from `next` on. `missing` is the
    /// error when the buffer no longer holds `next` although it was published.
    async fn catch_up(&mut self, missing: ReadError) -> Result<(), ReadError> {
        let next = self.next.expect("catch-up needs a position");
        let batches = replay(&self.replay_endpoint, next)
            .await
            .map_err(|error| ReadError::ReplayFailed(error.to_string()))?;
        let published = self
            .forwarded
            .borrow()
            .is_some_and(|forwarded| next < forwarded.next);
        match batches.first() {
            Some((sequence_number, _)) if *sequence_number != next => return Err(missing),
            None if published => return Err(missing),
            _ => {}
        }
        self.pending.clear();
        for (sequence_number, payload) in batches {
            let Ok(batch) = decode_batch(sequence_number, &payload) else {
                return Err(missing);
            };
            self.pending.push_back(Arc::new(batch));
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
enum ReplayError {
    #[error(transparent)]
    Zmq(#[from] zeromq::ZmqError),
    #[error("the publisher sent no replay frame within {REPLAY_TIMEOUT:?}")]
    Timeout,
    #[error("malformed replay frame")]
    Malformed,
}

/// The publisher's buffered batches from `start` on, in order. Read in full
/// before any is used, so a slow gRPC client cannot hold up the publisher.
async fn replay(endpoint: &str, start: u64) -> Result<Vec<(u64, Bytes)>, ReplayError> {
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
            .map_err(|_| ReplayError::Timeout)??;
        match published_batch(message, 1) {
            Some((REPLAY_END, _)) => return Ok(batches),
            Some(batch) => batches.push(batch),
            None => return Err(ReplayError::Malformed),
        }
    }
}

/// `(sequence number, payload)` of a `[topic, sequence number, payload]`
/// message that follows `envelope` frames.
fn published_batch(message: ZmqMessage, envelope: usize) -> Option<(u64, Bytes)> {
    let mut frames = message.into_vec();
    if frames.len() != envelope + 3 {
        return None;
    }
    let sequence_number = u64::from_be_bytes(frames[envelope + 1].as_ref().try_into().ok()?);
    Some((sequence_number, frames.swap_remove(envelope + 2)))
}
