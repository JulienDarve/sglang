use std::collections::VecDeque;
use std::path::PathBuf;
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use tokio::sync::mpsc;
use zeromq::{PubSocket, RouterSocket, Socket, SocketRecv, SocketSend, ZmqMessage};

use super::*;
use crate::message::config::KvEventRelayConfig;

const TIMEOUT: Duration = Duration::from_secs(5);

enum Command {
    /// `live: false` keeps the batch for replay only, as a PUB drop does.
    Publish {
        live: bool,
    },
    HoldReplays,
    ReleaseReplays,
}

/// `ZmqEventPublisher`'s PUB socket, replay buffer and ROUTER replay protocol.
struct Publisher {
    dir: PathBuf,
    commands: mpsc::UnboundedSender<Command>,
    /// Each replay request put on hold.
    held: mpsc::UnboundedReceiver<()>,
    published: u64,
    task: tokio::task::JoinHandle<()>,
}

impl Publisher {
    async fn bind(buffer_steps: usize) -> Self {
        let dir = std::env::temp_dir().join(format!("kv-events-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&dir).unwrap();
        let mut live = PubSocket::new();
        live.bind(&format!("ipc://{}/events", dir.display()))
            .await
            .unwrap();
        let mut replay = RouterSocket::new();
        replay
            .bind(&format!("ipc://{}/replay", dir.display()))
            .await
            .unwrap();
        let (commands, mut command_rx) = mpsc::unbounded_channel();
        let (held_tx, held) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            let mut buffer = VecDeque::<(u64, Bytes)>::new();
            let mut next = 0u64;
            let mut hold = false;
            let mut held_requests = Vec::new();
            loop {
                tokio::select! {
                    command = command_rx.recv() => match command.unwrap() {
                        Command::Publish { live: on_pub } => {
                            let payload = Bytes::from(
                                rmp_serde::to_vec(&(0.5f64, Vec::<()>::new(), Some(0u32))).unwrap(),
                            );
                            let seq = Bytes::copy_from_slice(&next.to_be_bytes());
                            if on_pub {
                                let frames = vec![Bytes::new(), seq, payload.clone()];
                                live.send(ZmqMessage::try_from(frames).unwrap()).await.unwrap();
                            }
                            buffer.push_back((next, payload));
                            if buffer.len() > buffer_steps {
                                buffer.pop_front();
                            }
                            next += 1;
                        }
                        Command::HoldReplays => hold = true,
                        Command::ReleaseReplays => {
                            hold = false;
                            for request in std::mem::take(&mut held_requests) {
                                answer(&mut replay, &buffer, request).await;
                            }
                        }
                    },
                    request = replay.recv() => {
                        let request = request.unwrap();
                        if hold {
                            held_requests.push(request);
                            held_tx.send(()).unwrap();
                        } else {
                            answer(&mut replay, &buffer, request).await;
                        }
                    }
                }
            }
        });
        Self {
            dir,
            commands,
            held,
            published: 0,
            task,
        }
    }

    fn config(&self) -> KvEventRelayConfig {
        KvEventRelayConfig {
            dp_rank: 0,
            block_size: 16,
            endpoint: format!("ipc://{}/events", self.dir.display()),
            replay_endpoint: format!("ipc://{}/replay", self.dir.display()),
        }
    }

    fn publish(&mut self, count: u64) {
        for _ in 0..count {
            self.commands.send(Command::Publish { live: true }).unwrap();
        }
        self.published += count;
    }

    fn drop_on_pub(&mut self) {
        self.commands
            .send(Command::Publish { live: false })
            .unwrap();
        self.published += 1;
    }

    fn hold_replays(&self) {
        self.commands.send(Command::HoldReplays).unwrap();
    }

    fn release_replays(&self) {
        self.commands.send(Command::ReleaseReplays).unwrap();
    }

    /// Publish until the relay has every batch: a PUB socket drops what it
    /// sends before the subscription reaches it.
    async fn warm_up(&mut self, relay: &KvEventRelay) {
        tokio::time::timeout(TIMEOUT, async {
            while relay.forwarded_next() < self.published || self.published == 0 {
                self.publish(1);
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the relay never received a batch");
    }

    async fn wait_relayed(&self, relay: &KvEventRelay) {
        tokio::time::timeout(TIMEOUT, async {
            while relay.forwarded_next() < self.published {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the relay did not receive every batch");
    }
}

impl Drop for Publisher {
    fn drop(&mut self) {
        self.task.abort();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// `[identity, "", start]`: every buffered batch from `start` on, then the end.
async fn answer(replay: &mut RouterSocket, buffer: &VecDeque<(u64, Bytes)>, request: ZmqMessage) {
    let frames = request.into_vec();
    let identity = frames[0].clone();
    let start = u64::from_be_bytes(frames[2].as_ref().try_into().unwrap());
    let reply = |seq: Bytes, payload: Bytes| {
        ZmqMessage::try_from(vec![
            identity.clone(),
            Bytes::new(),
            Bytes::new(),
            seq,
            payload,
        ])
        .unwrap()
    };
    for (seq, payload) in buffer.iter().filter(|(seq, _)| *seq >= start) {
        let seq = Bytes::copy_from_slice(&seq.to_be_bytes());
        replay.send(reply(seq, payload.clone())).await.unwrap();
    }
    let end = Bytes::copy_from_slice(&(-1i64).to_be_bytes());
    replay.send(reply(end, Bytes::new())).await.unwrap();
}

fn service(relay: &Arc<KvEventRelay>) -> (KvEventsService, watch::Sender<bool>) {
    let (stop, stopped) = watch::channel(false);
    (KvEventsService::new(Some(relay.clone()), stopped), stop)
}

async fn subscribe(
    service: &KvEventsService,
    resume: Option<pb::KvEventCursor>,
) -> Result<ResponseStream<pb::SubscribeKvEventsResponse>, Status> {
    let request = pb::SubscribeKvEventsRequest {
        data_parallel_rank: 0,
        resume,
    };
    Ok(service
        .subscribe_kv_events(Request::new(request))
        .await?
        .into_inner())
}

fn cursor(stream_id: &str, next_sequence_number: u64) -> Option<pb::KvEventCursor> {
    Some(pb::KvEventCursor {
        stream_id: stream_id.to_owned(),
        next_sequence_number,
    })
}

async fn next_item(
    stream: &mut ResponseStream<pb::SubscribeKvEventsResponse>,
) -> Result<Item, Status> {
    let response = tokio::time::timeout(TIMEOUT, stream.next())
        .await
        .expect("no response in time")
        .expect("the stream ended")?;
    Ok(response.item.unwrap())
}

async fn start(stream: &mut ResponseStream<pb::SubscribeKvEventsResponse>) -> String {
    match next_item(stream).await.unwrap() {
        Item::Start(start) => start.stream_id,
        Item::Batch(batch) => panic!("batch {} before start", batch.sequence_number),
    }
}

async fn sequence_numbers(
    stream: &mut ResponseStream<pb::SubscribeKvEventsResponse>,
    count: u64,
) -> Vec<u64> {
    let mut received = Vec::new();
    for _ in 0..count {
        match next_item(stream).await.unwrap() {
            Item::Batch(batch) => received.push(batch.sequence_number),
            Item::Start(_) => panic!("a second start"),
        }
    }
    received
}

#[tokio::test]
async fn resume_inside_the_buffer_delivers_each_batch_once() {
    let mut publisher = Publisher::bind(100).await;
    let relay = KvEventRelay::start(&publisher.config());
    let (service, _stop) = service(&relay);
    publisher.warm_up(&relay).await;
    publisher.publish(3);
    publisher.wait_relayed(&relay).await;

    publisher.hold_replays();
    let subscription = tokio::spawn(async move {
        let stream = subscribe(&service, cursor("", 1)).await;
        (service, stream)
    });
    publisher.held.recv().await.unwrap();
    // Published after the subscriber joined and before the replay is
    // answered: both in the replay and live.
    publisher.publish(2);
    publisher.wait_relayed(&relay).await;
    publisher.release_replays();
    let (_service, stream) = subscription.await.unwrap();
    let mut stream = stream.unwrap();
    // Only live.
    publisher.publish(2);

    assert!(!start(&mut stream).await.is_empty());
    let last = publisher.published - 1;
    assert_eq!(
        sequence_numbers(&mut stream, last).await,
        (1..=last).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn cursor_outside_the_buffer_or_stream_is_out_of_range() {
    for buffer_steps in [4, 0] {
        let mut publisher = Publisher::bind(buffer_steps).await;
        let relay = KvEventRelay::start(&publisher.config());
        let (service, _stop) = service(&relay);
        publisher.warm_up(&relay).await;
        publisher.publish(6);
        publisher.wait_relayed(&relay).await;

        let stream_id = relay.stream_id().to_owned();
        let last = publisher.published - 1;
        // An empty buffer holds not even the last batch.
        let old = if buffer_steps == 0 { last } else { 0 };
        for (resume, reason) in [
            (cursor(&stream_id, old), "older than the buffer"),
            (cursor("stale", last), "another stream"),
        ] {
            let status = subscribe(&service, resume).await.err().expect(reason);
            assert_eq!(
                status.code(),
                tonic::Code::OutOfRange,
                "buffer {buffer_steps}, {reason}: {status}"
            );
        }
    }
}

#[tokio::test]
async fn slow_subscriber_is_caught_up_until_the_buffer_no_longer_covers_it() {
    let mut publisher = Publisher::bind(8).await;
    let relay = KvEventRelay::start_with_capacity(&publisher.config(), 2);
    let (service, _stop) = service(&relay);
    publisher.warm_up(&relay).await;
    let mut stream = subscribe(&service, None).await.unwrap();
    start(&mut stream).await;

    // More than the live fan-out keeps, fewer than the replay buffer.
    let first = publisher.published;
    publisher.publish(6);
    publisher.wait_relayed(&relay).await;
    assert_eq!(
        sequence_numbers(&mut stream, 6).await,
        (first..first + 6).collect::<Vec<_>>()
    );

    // More than the replay buffer.
    publisher.publish(12);
    publisher.wait_relayed(&relay).await;
    let status = next_item(&mut stream).await.expect_err("data loss");
    assert_eq!(status.code(), tonic::Code::DataLoss, "{status}");
}

#[tokio::test]
async fn batch_the_pub_socket_dropped_is_filled_from_the_replay_buffer() {
    let mut publisher = Publisher::bind(100).await;
    let relay = KvEventRelay::start(&publisher.config());
    let (service, _stop) = service(&relay);
    publisher.warm_up(&relay).await;
    let mut stream = subscribe(&service, None).await.unwrap();
    start(&mut stream).await;

    let first = publisher.published;
    publisher.drop_on_pub();
    publisher.publish(1);
    assert_eq!(sequence_numbers(&mut stream, 2).await, [first, first + 1]);
}

#[tokio::test]
async fn streams_end_at_shutdown() {
    let mut publisher = Publisher::bind(100).await;
    let relay = KvEventRelay::start(&publisher.config());
    let (service, stop) = service(&relay);
    publisher.warm_up(&relay).await;
    let mut stream = subscribe(&service, None).await.unwrap();
    start(&mut stream).await;

    stop.send(true).unwrap();
    let status = next_item(&mut stream).await.expect_err("shutdown");
    assert_eq!(status.code(), tonic::Code::Unavailable, "{status}");
}
