//! The scheduler's KV event batches as `ZmqEventPublisher` encodes them with
//! msgspec (`python/sglang/srt/disaggregation/kv_events.py`), decoded into
//! `sglang.kv.v1` messages.

use serde::Deserialize;
use sglang_kv_types::kv::v1 as pb;

/// Decode the payload of the batch numbered `sequence_number`.
pub(super) fn decode_batch(
    sequence_number: u64,
    payload: &[u8],
) -> Result<pb::KvEventBatch, rmp_serde::decode::Error> {
    let EventBatch {
        ts,
        events,
        attn_dp_rank,
    } = rmp_serde::from_slice(payload)?;
    Ok(pb::KvEventBatch {
        sequence_number,
        ts,
        attn_dp_rank,
        events: events.into_iter().map(Event::into_proto).collect(),
    })
}

/// `EventBatch`, the positional array `[ts, events, attn_dp_rank]`.
#[derive(Deserialize)]
struct EventBatch {
    ts: f64,
    events: Vec<Event>,
    #[serde(default)]
    attn_dp_rank: Option<u32>,
}

/// `KVCacheEvent`, a map tagged with its Python class name.
#[derive(Deserialize)]
#[serde(tag = "type")]
enum Event {
    BlockStored(BlockStored),
    BlockRemoved(BlockRemoved),
    AllBlocksCleared,
}

/// Fields left at a `None` default are omitted from the map.
#[derive(Deserialize)]
struct BlockStored {
    block_hashes: Vec<i64>,
    parent_block_hash: Option<i64>,
    token_ids: TokenIds,
    block_size: u32,
    lora_id: Option<i64>,
    medium: Option<String>,
    cache_salt: Option<String>,
    session_id: Option<String>,
    lora_name: Option<String>,
    mm_routing_spans: Option<Vec<Option<BlockSpans>>>,
}

/// One block's `[start, length, label]` runs of labeled multimodal tokens.
type BlockSpans = Vec<(u64, u64, String)>;

#[derive(Deserialize)]
struct BlockRemoved {
    block_hashes: Vec<i64>,
    medium: Option<String>,
}

/// Token IDs, or `[token, next token]` pairs on EAGLE pages.
#[derive(Deserialize)]
#[serde(untagged)]
enum TokenIds {
    Single(Vec<u64>),
    Pairs(Vec<(u64, u64)>),
}

impl Event {
    fn into_proto(self) -> pb::KvCacheEvent {
        use pb::kv_cache_event::Event as Proto;
        let event = match self {
            Event::BlockStored(stored) => Proto::BlockStored(stored.into_proto()),
            Event::BlockRemoved(removed) => Proto::BlockRemoved(pb::BlockRemoved {
                block_hashes: removed.block_hashes,
                medium: removed.medium,
            }),
            Event::AllBlocksCleared => Proto::AllBlocksCleared(pb::AllBlocksCleared {}),
        };
        pb::KvCacheEvent { event: Some(event) }
    }
}

impl BlockStored {
    fn into_proto(self) -> pb::BlockStored {
        let token_ids = match self.token_ids {
            TokenIds::Single(token_ids) => {
                pb::block_stored::TokenIds::Single(pb::TokenIdList { token_ids })
            }
            TokenIds::Pairs(pairs) => pb::block_stored::TokenIds::Pairs(pb::TokenIdPairList {
                pairs: pairs
                    .into_iter()
                    .map(|(first, second)| pb::TokenIdPair { first, second })
                    .collect(),
            }),
        };
        let mm_routing_spans = self.mm_routing_spans.unwrap_or_default().into_iter();
        pb::BlockStored {
            block_hashes: self.block_hashes,
            parent_block_hash: self.parent_block_hash,
            token_ids: Some(token_ids),
            block_size: self.block_size,
            lora_id: self.lora_id,
            medium: self.medium,
            cache_salt: self.cache_salt,
            session_id: self.session_id,
            lora_name: self.lora_name,
            mm_routing_spans: mm_routing_spans
                .map(|spans| pb::BlockMmRoutingSpans {
                    spans: spans
                        .unwrap_or_default()
                        .into_iter()
                        .map(|(start, length, label)| pb::MmRoutingSpan {
                            start,
                            length,
                            label,
                        })
                        .collect(),
                })
                .collect(),
        }
    }
}
