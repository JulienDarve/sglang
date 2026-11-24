//! The scheduler's KV event batches as `ZmqEventPublisher` encodes them with
//! msgspec (`python/sglang/srt/disaggregation/kv_events.py`), decoded into
//! `sglang.kv.v1` messages.

use std::fmt;

use serde::Deserialize;
use serde::de::{self, Deserializer, MapAccess, SeqAccess, Visitor};
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
        data_parallel_rank: attn_dp_rank,
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

/// `KVCacheEvent`, a map tagged by its `type` key, which msgspec writes first.
enum Event {
    Stored(BlockStored),
    Removed(BlockRemoved),
    Cleared,
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
enum TokenIds {
    Single(Vec<u64>),
    Pairs(Vec<(u64, u64)>),
}

impl Event {
    fn into_proto(self) -> pb::KvCacheEvent {
        use pb::kv_cache_event::Event as Proto;
        let event = match self {
            Event::Stored(stored) => Proto::BlockStored(stored.into_proto()),
            Event::Removed(BlockRemoved {
                block_hashes,
                medium,
            }) => Proto::BlockRemoved(pb::BlockRemoved {
                block_hashes: block_hashes.into_iter().map(block_hash).collect(),
                medium,
                ..Default::default()
            }),
            Event::Cleared => Proto::AllBlocksCleared(pb::AllBlocksCleared {}),
        };
        pb::KvCacheEvent { event: Some(event) }
    }
}

impl BlockStored {
    fn into_proto(self) -> pb::BlockStored {
        let ids = match self.token_ids {
            TokenIds::Single(token_ids) => {
                pb::block_token_ids::Ids::Single(pb::TokenIdList { token_ids })
            }
            TokenIds::Pairs(pairs) => pb::block_token_ids::Ids::Pairs(pb::TokenIdPairList {
                pairs: pairs
                    .into_iter()
                    .map(|(first, second)| pb::TokenIdPair { first, second })
                    .collect(),
            }),
        };
        let mm_routing_spans = self.mm_routing_spans.unwrap_or_default().into_iter();
        pb::BlockStored {
            block_hashes: self.block_hashes.into_iter().map(block_hash).collect(),
            parent_block_hash: self.parent_block_hash.map(block_hash),
            token_ids: Some(pb::BlockTokenIds { ids: Some(ids) }),
            block_size: self.block_size,
            lora_id: self.lora_id,
            medium: self.medium,
            lora_name: self.lora_name,
            session_id: self.session_id,
            cache_salt: self.cache_salt,
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
            ..Default::default()
        }
    }
}

fn block_hash(value: i64) -> pb::BlockHash {
    pb::BlockHash {
        value: Some(pb::block_hash::Value::SignedValue(value)),
    }
}

impl<'de> Deserialize<'de> for Event {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_map(EventVisitor)
    }
}

struct EventVisitor;

impl<'de> Visitor<'de> for EventVisitor {
    type Value = Event;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a KV cache event map whose first key is `type`")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Event, A::Error> {
        if map.next_key::<String>()?.as_deref() != Some("type") {
            return Err(de::Error::missing_field("type"));
        }
        let tag: String = map.next_value()?;
        let fields = de::value::MapAccessDeserializer::new(map);
        match tag.as_str() {
            "BlockStored" => BlockStored::deserialize(fields).map(Event::Stored),
            "BlockRemoved" => BlockRemoved::deserialize(fields).map(Event::Removed),
            "AllBlocksCleared" => de::IgnoredAny::deserialize(fields).map(|_| Event::Cleared),
            other => Err(de::Error::unknown_variant(
                other,
                &["BlockStored", "BlockRemoved", "AllBlocksCleared"],
            )),
        }
    }
}

impl<'de> Deserialize<'de> for TokenIds {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_seq(TokenIdsVisitor)
    }
}

struct TokenIdsVisitor;

impl<'de> Visitor<'de> for TokenIdsVisitor {
    type Value = TokenIds;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a list of token IDs or of token ID pairs")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<TokenIds, A::Error> {
        let capacity = seq.size_hint().unwrap_or(0);
        // The first element decides the shape of the rest.
        Ok(match seq.next_element::<TokenId>()? {
            None => TokenIds::Single(Vec::new()),
            Some(TokenId::Single(id)) => {
                let mut ids = Vec::with_capacity(capacity);
                ids.push(id);
                while let Some(id) = seq.next_element()? {
                    ids.push(id);
                }
                TokenIds::Single(ids)
            }
            Some(TokenId::Pair(pair)) => {
                let mut pairs = Vec::with_capacity(capacity);
                pairs.push(pair);
                while let Some(pair) = seq.next_element()? {
                    pairs.push(pair);
                }
                TokenIds::Pairs(pairs)
            }
        })
    }
}

enum TokenId {
    Single(u64),
    Pair((u64, u64)),
}

impl<'de> Deserialize<'de> for TokenId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(TokenIdVisitor)
    }
}

struct TokenIdVisitor;

impl<'de> Visitor<'de> for TokenIdVisitor {
    type Value = TokenId;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a token ID or a pair of token IDs")
    }

    fn visit_u64<E: de::Error>(self, id: u64) -> Result<TokenId, E> {
        Ok(TokenId::Single(id))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, seq: A) -> Result<TokenId, A::Error> {
        <(u64, u64)>::deserialize(de::value::SeqAccessDeserializer::new(seq)).map(TokenId::Pair)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hashes(values: &[i64]) -> Vec<pb::BlockHash> {
        values.iter().copied().map(block_hash).collect()
    }

    fn token_ids(ids: pb::block_token_ids::Ids) -> Option<pb::BlockTokenIds> {
        Some(pb::BlockTokenIds { ids: Some(ids) })
    }

    /// Every field of every event, as `ZmqEventPublisher` sends it.
    #[test]
    fn published_batch_maps_every_field() {
        // msgspec.msgpack.encode of KVEventBatch(ts=1.5, attn_dp_rank=3, events=[
        //   BlockStored(block_hashes=[-7, 2**62], parent_block_hash=-1,
        //     token_ids=[1, 2, 2**40, 3], block_size=2, lora_id=5,
        //     medium=StorageMedium.CPU, cache_salt="salt", session_id="session",
        //     lora_name="adapter", mm_routing_spans=[[(0, 1, "image")], []]),
        //   BlockStored(block_hashes=[11], parent_block_hash=None,
        //     token_ids=[(1, 2), (2, 3)], block_size=2, lora_id=None),
        //   BlockRemoved(block_hashes=[-7], medium=StorageMedium.GPU),
        //   AllBlocksCleared()])
        // with python/sglang/srt/disaggregation/kv_events.py.
        let payload = "93cb3ff8000000000000948ba474797065ab426c6f636b53746f726564ac626c6f636b5f68617368657392f9cf4000000000000000b1706172656e745f626c6f636b5f68617368ffa9746f6b656e5f696473940102cf000001000000000003aa626c6f636b5f73697a6502a76c6f72615f696405a66d656469756daa4350555f50494e4e4544aa63616368655f73616c74a473616c74aa73657373696f6e5f6964a773657373696f6ea96c6f72615f6e616d65a761646170746572b06d6d5f726f7574696e675f7370616e739291930001a5696d6167659086a474797065ab426c6f636b53746f726564ac626c6f636b5f686173686573910bb1706172656e745f626c6f636b5f68617368c0a9746f6b656e5f69647392920102920203aa626c6f636b5f73697a6502a76c6f72615f6964c083a474797065ac426c6f636b52656d6f766564ac626c6f636b5f68617368657391f9a66d656469756da347505581a474797065b0416c6c426c6f636b73436c656172656403";
        let payload: Vec<u8> = (0..payload.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&payload[i..i + 2], 16).unwrap())
            .collect();

        use pb::kv_cache_event::Event as E;
        let event = |event| pb::KvCacheEvent { event: Some(event) };
        let expected = pb::KvEventBatch {
            sequence_number: 9,
            ts: 1.5,
            data_parallel_rank: Some(3),
            events: vec![
                event(E::BlockStored(pb::BlockStored {
                    block_hashes: hashes(&[-7, 1 << 62]),
                    parent_block_hash: Some(block_hash(-1)),
                    token_ids: token_ids(pb::block_token_ids::Ids::Single(pb::TokenIdList {
                        token_ids: vec![1, 2, 1 << 40, 3],
                    })),
                    block_size: 2,
                    lora_id: Some(5),
                    medium: Some("CPU_PINNED".into()),
                    lora_name: Some("adapter".into()),
                    session_id: Some("session".into()),
                    cache_salt: Some("salt".into()),
                    mm_routing_spans: vec![
                        pb::BlockMmRoutingSpans {
                            spans: vec![pb::MmRoutingSpan {
                                start: 0,
                                length: 1,
                                label: "image".into(),
                            }],
                        },
                        pb::BlockMmRoutingSpans { spans: vec![] },
                    ],
                    ..Default::default()
                })),
                event(E::BlockStored(pb::BlockStored {
                    block_hashes: hashes(&[11]),
                    token_ids: token_ids(pb::block_token_ids::Ids::Pairs(pb::TokenIdPairList {
                        pairs: vec![
                            pb::TokenIdPair {
                                first: 1,
                                second: 2,
                            },
                            pb::TokenIdPair {
                                first: 2,
                                second: 3,
                            },
                        ],
                    })),
                    block_size: 2,
                    ..Default::default()
                })),
                event(E::BlockRemoved(pb::BlockRemoved {
                    block_hashes: hashes(&[-7]),
                    medium: Some("GPU".into()),
                    ..Default::default()
                })),
                event(E::AllBlocksCleared(pb::AllBlocksCleared {})),
            ],
        };
        assert_eq!(decode_batch(9, &payload).unwrap(), expected);
    }
}
