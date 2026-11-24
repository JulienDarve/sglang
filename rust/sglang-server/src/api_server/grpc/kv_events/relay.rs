//! Follows the scheduler's `ZmqEventPublisher` over its private PUB and ROUTER
//! sockets (`python/sglang/srt/disaggregation/kv_events.py`) and fans its
//! batches out to the gRPC subscribers. The relay keeps no history: whatever a
//! subscriber or the relay itself missed comes from the publisher's replay
//! buffer.

use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use bytes::Bytes;
use sglang_kv_types::kv::v1 as pb;
use tokio::sync::broadcast;
use zeromq::{DealerSocket, Socket, SocketRecv, SocketSend, SubSocket, ZmqMessage};

use super::wire::decode_batch;
use crate::message::config::KvEventRelayConfig;

/// Live batches a subscriber may fall behind before it is caught up from the
/// publisher's replay buffer. Arbitrary.
pub(super) const LIVE_CAPACITY: usize = 256;
/// How long the publisher may take to send the next frame of a replay. Arbitrary.
const REPLAY_TIMEOUT: Duration = Duration::from_secs(5);
/// `ZmqEventPublisher.END_SEQ`, -1 as a big-endian i64: ends a replay.
const REPLAY_END: u64 = u64::MAX;

pub(crate) struct KvEventRelay {
    pub(super) dp_rank: u32,
    replay_endpoint: String,
    state: Mutex<State>,
}

struct State {
    stream_id: String,
    /// The sequence number of the next batch the relay sends.
    next: u64,
    /// `None` once the relay stopped.
    live: Option<broadcast::Sender<Arc<pb::KvEventBatch>>>,
}

/// Where a new subscriber joins the relay.
pub(super) struct Subscription {
    pub(super) stream_id: String,
    /// The first batch `live` delivers is numbered at least this.
    pub(super) next: u64,
    pub(super) live: broadcast::Receiver<Arc<pb::KvEventBatch>>,
}

#[derive(Debug, thiserror::Error)]
pub(super) enum ReplayError {
    #[error(transparent)]
    Zmq(#[from] zeromq::ZmqError),
    #[error("the publisher sent no replay frame within {REPLAY_TIMEOUT:?}")]
    Timeout,
    #[error("malformed replay frame")]
    Malformed,
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
        let relay = Arc::new(Self {
            dp_rank: config.dp_rank,
            replay_endpoint: config.replay_endpoint.clone(),
            state: Mutex::new(State {
                stream_id: uuid::Uuid::new_v4().to_string(),
                // From the first batch, so the ones published before the
                // subscription took effect are replayed.
                next: 0,
                live: Some(broadcast::channel(live_capacity).0),
            }),
        });
        tokio::spawn(relay.clone().follow(config.endpoint.clone()));
        relay
    }

    /// `None` once the relay stopped.
    pub(super) fn subscribe(&self) -> Option<Subscription> {
        let state = self.state();
        Some(Subscription {
            live: state.live.as_ref()?.subscribe(),
            stream_id: state.stream_id.clone(),
            next: state.next,
        })
    }

    /// The publisher's buffered batches from `start` on, in order. Read in
    /// full before any is used: a reader slower than the publisher would make
    /// its ROUTER socket drop frames.
    pub(super) async fn replay(&self, start: u64) -> Result<Vec<(u64, Bytes)>, ReplayError> {
        let mut socket = DealerSocket::new();
        socket.connect(&self.replay_endpoint).await?;
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

    #[cfg(test)]
    pub(super) fn next(&self) -> u64 {
        self.state().next
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap()
    }

    async fn follow(self: Arc<Self>, endpoint: String) {
        if let Err(error) = self.run(&endpoint).await {
            tracing::error!(%error, endpoint, "KV event relay stopped");
        }
        self.state().live = None;
    }

    async fn run(&self, endpoint: &str) -> Result<(), zeromq::ZmqError> {
        let mut socket = SubSocket::new();
        socket.connect(endpoint).await?;
        socket.subscribe("").await?;
        loop {
            let Some((sequence_number, payload)) = published_batch(socket.recv().await?, 0) else {
                tracing::warn!("dropping a malformed KV event message");
                continue;
            };
            let next = self.state().next;
            if sequence_number > next {
                self.fill(next, sequence_number).await;
            }
            // Lower: already sent from a replay. The publisher lives in this
            // process, so its numbering never restarts.
            if sequence_number == self.state().next {
                self.send(sequence_number, &payload);
            }
        }
    }

    /// Send `[next, until)`, which the PUB socket did not deliver, from the
    /// replay buffer; start a new stream at `until` if they are gone.
    async fn fill(&self, next: u64, until: u64) {
        match self.replay(next).await {
            Ok(batches) => {
                for (sequence_number, payload) in batches {
                    let next = self.state().next;
                    if sequence_number > next {
                        break;
                    }
                    if sequence_number == next {
                        self.send(sequence_number, &payload);
                    }
                }
            }
            Err(error) => tracing::warn!(%error, next, "KV event replay failed"),
        }
        let next = self.state().next;
        if next < until {
            tracing::warn!(next, until, "KV event batches lost; starting a new stream");
            self.restart(until);
        }
    }

    fn send(&self, sequence_number: u64, payload: &[u8]) {
        match decode_batch(sequence_number, payload) {
            Ok(batch) => {
                // Under the lock, so a new subscriber either sees this batch
                // in `live` or starts after it.
                let mut state = self.state();
                state.next = sequence_number + 1;
                if let Some(live) = &state.live {
                    let _ = live.send(Arc::new(batch));
                }
            }
            Err(error) => {
                tracing::warn!(sequence_number, %error, "KV event batch cannot be decoded; starting a new stream");
                self.restart(sequence_number + 1);
            }
        }
    }

    fn restart(&self, next: u64) {
        let mut state = self.state();
        state.stream_id = uuid::Uuid::new_v4().to_string();
        state.next = next;
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
