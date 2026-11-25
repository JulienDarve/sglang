use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use zeromq::{PubSocket, RouterSocket, Socket, SocketRecv, SocketSend};

use super::*;
use crate::message::config::KvEventRelayConfig;

const TIMEOUT: Duration = Duration::from_secs(5);

type Responses = ResponseStream<pb::SubscribeKvEventsResponse>;

/// Stand-in for `ZmqEventPublisher`: its PUB socket and ROUTER replay
/// protocol, with whatever sequence numbers a test asks for.
struct Publisher {
    dir: PathBuf,
    config: KvEventRelayConfig,
    events: PubSocket,
    /// The batches served on the replay socket.
    buffer: Arc<Mutex<BTreeMap<u64, Bytes>>>,
    replays: tokio::task::JoinHandle<()>,
}

impl Publisher {
    async fn bind() -> Self {
        let dir = std::env::temp_dir().join(format!("kv-events-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&dir).unwrap();
        let config = KvEventRelayConfig {
            dp_rank: 0,
            block_size: 16,
            endpoint: format!("ipc://{}/events", dir.display()),
            replay_endpoint: format!("ipc://{}/replay", dir.display()),
            grpc_snapshot: true,
        };
        let mut events = PubSocket::new();
        events.bind(&config.endpoint).await.unwrap();
        let mut replay = RouterSocket::new();
        replay.bind(&config.replay_endpoint).await.unwrap();
        let buffer = Arc::new(Mutex::new(BTreeMap::new()));
        let replays = tokio::spawn(serve_replays(replay, buffer.clone()));
        Self {
            dir,
            config,
            events,
            buffer,
            replays,
        }
    }

    /// Buffer a batch without publishing it, as when the PUB socket drops it.
    fn buffer(&self, sequence_number: u64, payload: Bytes) {
        self.buffer.lock().unwrap().insert(sequence_number, payload);
    }

    async fn publish(&mut self, sequence_number: u64, payload: Bytes) {
        self.buffer(sequence_number, payload.clone());
        let frames = frames(sequence_number, payload);
        self.events.send(frames.try_into().unwrap()).await.unwrap();
    }

    /// Drop the buffered batches before `sequence_number`, as a full buffer does.
    fn forget_before(&self, sequence_number: u64) {
        let mut buffer = self.buffer.lock().unwrap();
        *buffer = buffer.split_off(&sequence_number);
    }
}

impl Drop for Publisher {
    fn drop(&mut self) {
        self.replays.abort();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// `[topic, sequence number, payload]`, as `ZmqEventPublisher` sends a batch.
fn frames(sequence_number: u64, payload: Bytes) -> Vec<Bytes> {
    let sequence_number = Bytes::copy_from_slice(&sequence_number.to_be_bytes());
    vec![Bytes::new(), sequence_number, payload]
}

/// Answer each `[client, "", start]` with the buffered batches from `start`
/// on, then the end marker.
async fn serve_replays(mut socket: RouterSocket, buffer: Arc<Mutex<BTreeMap<u64, Bytes>>>) {
    while let Ok(request) = socket.recv().await {
        let request = request.into_vec();
        let start = u64::from_be_bytes(request[2].as_ref().try_into().unwrap());
        let mut replies: Vec<(u64, Bytes)> = (buffer.lock().unwrap().range(start..))
            .map(|(sequence_number, payload)| (*sequence_number, payload.clone()))
            .collect();
        replies.push((u64::MAX, Bytes::new()));
        for (sequence_number, payload) in replies {
            let mut reply = vec![request[0].clone(), Bytes::new()];
            reply.extend(frames(sequence_number, payload));
            socket.send(reply.try_into().unwrap()).await.unwrap();
        }
    }
}

/// The service over a relay of `publisher`, and the sender that stops its streams.
fn service(publisher: &Publisher) -> (KvEventsService, watch::Sender<bool>) {
    let (stop, stopped) = watch::channel(false);
    let relay = KvEventRelay::start(&publisher.config);
    (KvEventsService::new(Some(relay), stopped), stop)
}

/// Subscribe to rank 0, from `resume = (stream ID, next sequence number)`
/// when given. Returns the stream's ID and its batches.
async fn subscribe(
    service: &KvEventsService,
    resume: Option<(&str, u64)>,
) -> Result<(String, Responses), Status> {
    let request = pb::SubscribeKvEventsRequest {
        data_parallel_rank: 0,
        resume: resume.map(|(stream_id, next_sequence_number)| pb::KvEventCursor {
            stream_id: stream_id.to_owned(),
            next_sequence_number,
        }),
    };
    let response = service.subscribe_kv_events(Request::new(request)).await?;
    let mut stream = response.into_inner();
    match stream.next().await.expect("the start item")?.item {
        Some(Item::Start(start)) => Ok((start.stream_id, stream)),
        other => panic!("expected the start item, got {other:?}"),
    }
}

async fn next_batch(stream: &mut Responses) -> Result<pb::KvEventBatch, Status> {
    let response = tokio::time::timeout(TIMEOUT, stream.next())
        .await
        .expect("no batch in time")
        .expect("the stream ended")?;
    match response.item {
        Some(Item::Batch(batch)) => Ok(batch),
        other => panic!("expected a batch, got {other:?}"),
    }
}

/// `[ts, events, attn_dp_rank]` with no events.
fn empty_batch() -> Bytes {
    batch(&[])
}

/// `[ts, events, attn_dp_rank]`, the events as msgspec encodes them.
fn batch(events: &[serde_json::Value]) -> Bytes {
    rmp_serde::to_vec(&(0.5, events, 0)).unwrap().into()
}

/// A device-tier store of two-token blocks: block `hash` holds `[hash, hash + 100]`.
fn stored(block_hashes: &[i64], parent_block_hash: Option<i64>) -> serde_json::Value {
    let token_ids: Vec<i64> = (block_hashes.iter())
        .flat_map(|hash| [*hash, hash + 100])
        .collect();
    serde_json::json!({
        "type": "BlockStored",
        "block_hashes": block_hashes,
        "parent_block_hash": parent_block_hash,
        "token_ids": token_ids,
        "block_size": 2,
        "lora_id": null,
        "medium": "GPU",
        "session_id": "session",
    })
}

/// Rank 0's snapshot once the blocks have followed the stream up to batch
/// `next`, which they do on their own task: its stores and its end.
async fn snapshot_at(
    service: &KvEventsService,
    next: u64,
) -> Result<(Vec<pb::BlockStored>, pb::KvSnapshotEnd), Status> {
    use pb::get_kv_snapshot_response::Item;
    let followed = async {
        loop {
            let request = Request::new(pb::GetKvSnapshotRequest::default());
            let mut responses = service.get_kv_snapshot(request).await?.into_inner();
            let mut stored = Vec::new();
            while let Some(response) = responses.next().await {
                match response?.item.expect("an item") {
                    Item::Blocks(blocks) => stored.extend(blocks.stored),
                    Item::End(end) if end.resume.as_ref().unwrap().next_sequence_number == next => {
                        assert!(responses.next().await.is_none(), "`end` is last");
                        return Ok((stored, end));
                    }
                    Item::End(_) => tokio::time::sleep(Duration::from_millis(10)).await,
                }
            }
        }
    };
    let followed = tokio::time::timeout(TIMEOUT, followed).await;
    followed.unwrap_or_else(|_| panic!("no snapshot at batch {next} in time"))
}

/// Publish an empty batch until `stream` yields it. A PUB socket drops what
/// it sends before the subscription reaches it, and readers skip the repeats.
async fn publish_until_received(
    publisher: &mut Publisher,
    sequence_number: u64,
    stream: &mut Responses,
) -> pb::KvEventBatch {
    let received = async {
        loop {
            publisher.publish(sequence_number, empty_batch()).await;
            let batch = tokio::time::timeout(Duration::from_millis(20), stream.next());
            if let Ok(Some(Ok(response))) = batch.await {
                return response.item;
            }
        }
    };
    match tokio::time::timeout(TIMEOUT, received).await {
        Ok(Some(Item::Batch(batch))) => batch,
        other => panic!("expected batch {sequence_number}, got {other:?}"),
    }
}

/// A batch with every event type and field: the bytes `ZmqEventPublisher`
/// sends for it (`testdata/kv_event_batch.hex`, which `test_kv_events.py`
/// compares with msgspec's encoding), and the message the stream carries.
fn batch_with_every_field() -> (Bytes, pb::KvEventBatch) {
    use pb::block_token_ids::Ids;
    use pb::kv_cache_event::Event;

    let payload = include_str!("../../../../testdata/kv_event_batch.hex").trim();
    let payload: Vec<u8> = (0..payload.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&payload[i..i + 2], 16).unwrap())
        .collect();

    let hash = |value| pb::BlockHash {
        value: Some(pb::block_hash::Value::SignedValue(value)),
    };
    let token_ids = |ids| Some(pb::BlockTokenIds { ids: Some(ids) });
    let image = pb::MmRoutingSpan {
        start: 0,
        length: 1,
        label: "image".into(),
    };
    let events = vec![
        Event::BlockStored(pb::BlockStored {
            block_hashes: vec![hash(-7), hash(1 << 62)],
            parent_block_hash: Some(hash(-1)),
            token_ids: token_ids(Ids::Single(pb::TokenIdList {
                token_ids: vec![1, 2, 1 << 40, 3],
            })),
            block_size: 2,
            lora_id: Some(5),
            medium: Some("CPU_PINNED".into()),
            lora_name: Some("adapter".into()),
            session_id: Some("session".into()),
            cache_salt: Some("salt".into()),
            mm_routing_spans: vec![
                pb::BlockMmRoutingSpans { spans: vec![image] },
                pb::BlockMmRoutingSpans { spans: vec![] },
            ],
            ..Default::default()
        }),
        // An EAGLE page: `[token, next token]` pairs, and no parent.
        Event::BlockStored(pb::BlockStored {
            block_hashes: vec![hash(11)],
            token_ids: token_ids(Ids::Pairs(pb::TokenIdPairList {
                pairs: [(1, 2), (2, 3)]
                    .map(|(first, second)| pb::TokenIdPair { first, second })
                    .into(),
            })),
            block_size: 2,
            ..Default::default()
        }),
        Event::BlockRemoved(pb::BlockRemoved {
            block_hashes: vec![hash(-7)],
            medium: Some("GPU".into()),
            ..Default::default()
        }),
        Event::AllBlocksCleared(pb::AllBlocksCleared {}),
    ];
    let streamed = pb::KvEventBatch {
        sequence_number: 0,
        ts: 1.5,
        data_parallel_rank: Some(3),
        events: events
            .into_iter()
            .map(|event| pb::KvCacheEvent { event: Some(event) })
            .collect(),
    };
    (payload.into(), streamed)
}

async fn assert_refused(service: &KvEventsService, resume: (&str, u64), code: tonic::Code) {
    let status = subscribe(service, Some(resume))
        .await
        .err()
        .expect("a status");
    assert_eq!(status.code(), code, "{resume:?}: {status}");
}

#[tokio::test]
async fn resume_replays_the_buffer_then_continues_live() {
    let mut publisher = Publisher::bind().await;
    // Published before anyone subscribed.
    let (payload, every_field) = batch_with_every_field();
    publisher.buffer(0, payload);
    let (service, stop) = service(&publisher);

    // Without a cursor, the stream starts live.
    let (stream_id, mut live) = subscribe(&service, None).await.unwrap();
    let first_live = publish_until_received(&mut publisher, 1, &mut live).await;
    assert_eq!(first_live.sequence_number, 1);

    // A cursor replays the publisher's buffer, then continues live.
    let (_, mut resumed) = subscribe(&service, Some((&stream_id, 0))).await.unwrap();
    assert_eq!(next_batch(&mut resumed).await.unwrap(), every_field);
    assert_eq!(next_batch(&mut resumed).await.unwrap(), first_live);
    // Batch 1 again: a batch published during a replay arrives live too.
    publisher.publish(1, empty_batch()).await;
    publisher.publish(2, empty_batch()).await;
    for stream in [&mut resumed, &mut live] {
        assert_eq!(next_batch(stream).await.unwrap().sequence_number, 2);
    }

    // A cursor of another stream, older than the buffer, or with an empty
    // buffer (`buffer_steps: 0`) is refused.
    assert_refused(&service, ("another-stream", 2), tonic::Code::OutOfRange).await;
    publisher.forget_before(1);
    assert_refused(&service, ("", 0), tonic::Code::OutOfRange).await;
    publisher.forget_before(u64::MAX);
    assert_refused(&service, ("", 2), tonic::Code::OutOfRange).await;

    // A shutdown ends the open streams, which it would otherwise wait for.
    stop.send(true).unwrap();
    let status = next_batch(&mut live).await.unwrap_err();
    assert_eq!(status.code(), tonic::Code::Unavailable, "{status}");
}

#[tokio::test]
async fn gap_is_filled_from_the_buffer_or_ends_the_stream() {
    let mut publisher = Publisher::bind().await;
    let (service, _stop) = service(&publisher);
    let (_, mut stream) = subscribe(&service, None).await.unwrap();
    publish_until_received(&mut publisher, 0, &mut stream).await;

    // The PUB socket dropped batch 1, which is still buffered.
    publisher.buffer(1, empty_batch());
    publisher.publish(2, empty_batch()).await;
    for sequence_number in [1, 2] {
        let batch = next_batch(&mut stream).await.unwrap();
        assert_eq!(batch.sequence_number, sequence_number);
    }

    // The publisher skipped batch 3: live and resumed readers both learn of it.
    publisher.publish(4, empty_batch()).await;
    let status = next_batch(&mut stream).await.unwrap_err();
    assert_eq!(status.code(), tonic::Code::DataLoss, "{status}");
    assert_refused(&service, ("", 2), tonic::Code::OutOfRange).await;
}

#[tokio::test]
async fn kv_events_off_is_a_failed_precondition() {
    let (_stop, stopped) = watch::channel(false);
    let service = KvEventsService::new(None, stopped);
    let status = subscribe(&service, None).await.err().expect("a status");
    assert_eq!(status.code(), tonic::Code::FailedPrecondition, "{status}");
}

#[tokio::test]
async fn snapshot_holds_the_device_cache_and_joins_the_stream() {
    let mut publisher = Publisher::bind().await;
    // Two prompts share blocks 1 and 2, which the second stores again; then
    // the second prompt's last block is evicted.
    let prompts = [stored(&[1, 2, 3], None), stored(&[1, 2, 4, 5], None)];
    publisher.buffer(0, batch(&prompts));
    let evicted = serde_json::json!({"type": "BlockRemoved", "block_hashes": [5], "medium": "GPU"});
    publisher.buffer(1, batch(&[evicted]));
    let (service, stop) = service(&publisher);

    let (snapshot, end) = snapshot_at(&service, 2).await.unwrap();
    // The stores that recreate the held blocks, each after its parent, without the session.
    let expected = [
        stored(&[1, 2], None),
        stored(&[3], Some(2)),
        stored(&[4], Some(2)),
    ];
    let expected = wire::decode_batch(0, &batch(&expected)).unwrap().events;
    let expected = expected.into_iter().map(|event| match event.event {
        Some(pb::kv_cache_event::Event::BlockStored(store)) => pb::BlockStored {
            session_id: None,
            ..store
        },
        _ => unreachable!(),
    });
    assert_eq!(snapshot, expected.collect::<Vec<_>>());

    // The stream continues where the snapshot ends.
    let resume = end.resume.unwrap();
    let resume = (resume.stream_id.as_str(), resume.next_sequence_number);
    let (stream_id, mut stream) = subscribe(&service, Some(resume)).await.unwrap();
    assert_eq!(stream_id, resume.0);
    let next = publish_until_received(&mut publisher, 2, &mut stream).await;
    assert_eq!(next.sequence_number, 2);

    // A shutdown ends a snapshot that its client has not read.
    let request = Request::new(pb::GetKvSnapshotRequest::default());
    let mut unread = service.get_kv_snapshot(request).await.unwrap().into_inner();
    stop.send(true).unwrap();
    let status = unread.next().await.expect("a status").unwrap_err();
    assert_eq!(status.code(), tonic::Code::Unavailable, "{status}");
}

#[tokio::test]
async fn snapshot_is_incomplete_after_lost_batches_until_the_cache_is_cleared() {
    let mut publisher = Publisher::bind().await;
    publisher.buffer(0, batch(&[stored(&[1], None)]));
    let (service, _stop) = service(&publisher);
    assert!(snapshot_at(&service, 1).await.unwrap().1.complete);

    // The publisher skipped batch 1, so the blocks cannot be followed to batch 2.
    let (_, mut live) = subscribe(&service, None).await.unwrap();
    publish_until_received(&mut publisher, 2, &mut live).await;
    assert!(!snapshot_at(&service, 3).await.unwrap().1.complete);

    let cleared = serde_json::json!({"type": "AllBlocksCleared"});
    publisher.publish(3, batch(&[cleared])).await;
    let (snapshot, end) = snapshot_at(&service, 4).await.unwrap();
    assert!(snapshot.is_empty() && end.complete);
}

#[tokio::test]
async fn snapshot_can_be_turned_off_and_the_stream_kept() {
    let mut publisher = Publisher::bind().await;
    publisher.config.grpc_snapshot = false;
    let (service, _stop) = service(&publisher);
    let status = snapshot_at(&service, 0).await.expect_err("a status");
    assert_eq!(status.code(), tonic::Code::FailedPrecondition, "{status}");

    let (_, mut stream) = subscribe(&service, None).await.unwrap();
    publish_until_received(&mut publisher, 0, &mut stream).await;
}
