//! The blocks the scheduler's device cache holds, folded from its KV event batches, and the
//! snapshots of them.

use std::borrow::Cow;
use std::sync::Arc;

use prost::Message as _;
use prost::encoding::{decode_varint, encode_varint};
use rustc_hash::FxHashMap;

use super::ledger::Ledger;
use super::pb;
use super::pb::block_stored::TokenIds as Ids;

/// The medium of the device cache, the only tier tracked.
const DEVICE_MEDIUM: &str = "GPU";
/// Encoded size at which a snapshot response is sent. Arbitrary; gRPC's default limit is 4 MiB.
const RESPONSE_BYTES: usize = 1 << 20;
/// The most blocks and token ids in one store of a snapshot, which keep a store well under
/// `RESPONSE_BYTES`. Arbitrary.
const STORE_BLOCKS: usize = 1024;
const STORE_TOKEN_IDS: usize = 1 << 16;

/// What a store said about one of its blocks.
struct StoredBlock {
    hash: i64,
    token_ids: TokenIds,
    /// Everything else; see [`rest_of`].
    rest: Arc<pb::BlockStored>,
}

/// Token ids, flat (a pair as two ids), as varints: most take two or three bytes.
struct TokenIds(Box<[u8]>);

impl TokenIds {
    fn new(ids: &[u64]) -> Self {
        let mut bytes = Vec::new();
        ids.iter().for_each(|&id| encode_varint(id, &mut bytes));
        Self(bytes.into())
    }

    fn to_vec(&self) -> Vec<u64> {
        let mut bytes = &*self.0;
        std::iter::from_fn(|| decode_varint(&mut bytes).ok()).collect()
    }
}

/// The blocks the device cache holds, as of the batch before `next`.
///
/// A block is held or it is not: a second store of a held page changes nothing and one remove
/// drops it, the rule Dynamo's SGLang path applies.
#[derive(Default)]
pub(super) struct Blocks {
    held: Ledger<Arc<StoredBlock>>,
    rests: Rests,
    next: u64,
    /// Batches were lost since the cache was last empty.
    incomplete: bool,
}

impl Blocks {
    /// Applies the next batch of the stream. A batch before `next` was applied already: after
    /// a loss, the live stream can deliver again what a replay delivered.
    pub fn apply(&mut self, batch: &pb::KvEventBatch) {
        use pb::kv_cache_event::Event;

        if batch.sequence_number < self.next {
            return;
        }
        for event in &batch.events {
            match &event.event {
                Some(Event::BlockStored(stored)) => self.store(stored),
                Some(Event::BlockRemoved(removed)) => self.remove(removed),
                Some(Event::AllBlocksCleared(_)) => {
                    self.held = Ledger::default();
                    self.incomplete = false;
                }
                None => {}
            }
        }
        self.next = batch.sequence_number + 1;
    }

    /// Batches before `next` were lost: until the engine clears its cache, the blocks may lack
    /// some that the cache holds and hold some that it dropped.
    pub fn lost(&mut self, next: u64) {
        self.incomplete = true;
        self.next = self.next.max(next);
    }

    /// The responses of a snapshot of the blocks held now, `end` last. Taking it copies the
    /// ledger and shares the blocks, so it can be done under the lock that `apply` takes;
    /// the responses walk every block, so read them off the API runtime.
    pub fn snapshot(
        &self,
        stream_id: String,
    ) -> impl Iterator<Item = pb::GetKvSnapshotResponse> + Send + use<> {
        use pb::get_kv_snapshot_response::Item;

        let mut left = std::iter::once(self.held.clone()).flat_map(stores);
        let mut end = Some(pb::KvSnapshotEnd {
            resume: Some(pb::KvEventCursor {
                stream_id,
                next_sequence_number: self.next,
            }),
            complete: !self.incomplete,
        });
        std::iter::from_fn(move || {
            let mut stored = Vec::new();
            let mut bytes = 0;
            while bytes < RESPONSE_BYTES
                && let Some(store) = left.next()
            {
                bytes += store.encoded_len();
                stored.push(store);
            }
            let item = if stored.is_empty() {
                Item::End(end.take()?)
            } else {
                Item::Blocks(pb::KvSnapshotBlocks { stored })
            };
            Some(pb::GetKvSnapshotResponse { item: Some(item) })
        })
    }

    fn store(&mut self, stored: &pb::BlockStored) {
        if stored.medium.as_deref() != Some(DEVICE_MEDIUM) {
            return;
        }
        // The ids flat, and the form the store gave them in, without them.
        let (ids, form): (Cow<[u64]>, Ids) = match &stored.token_ids {
            Some(Ids::Single(list)) => ((&list.token_ids).into(), Ids::Single(Default::default())),
            Some(Ids::Pairs(list)) => {
                let ids = list.pairs.iter().flat_map(|pair| [pair.first, pair.second]);
                (ids.collect(), Ids::Pairs(Default::default()))
            }
            None => return,
        };
        let size = stored.block_size as usize * if matches!(form, Ids::Pairs(_)) { 2 } else { 1 };
        // The recorder stores `block_size` tokens per block; otherwise the tokens cannot be
        // told apart per block.
        if size == 0 || ids.len() != stored.block_hashes.len() * size {
            return;
        }

        // The same for every block of the store, unless they have spans of their own.
        let per_block = !stored.mm_routing_spans.is_empty();
        let mut of_store: Option<Arc<pb::BlockStored>> = None;
        let mut parent = stored.parent_block_hash.map(i64::cast_unsigned);
        for (at, &hash) in stored.block_hashes.iter().enumerate() {
            let key = hash.cast_unsigned();
            if self.held.get(key).is_none() {
                let rest = match &of_store {
                    Some(rest) if !per_block => rest.clone(),
                    _ => self.rests.shared(rest_of(stored, at, &form)),
                };
                of_store = Some(rest.clone());
                let block = StoredBlock {
                    hash,
                    token_ids: TokenIds::new(&ids[at * size..][..size]),
                    rest,
                };
                self.held.store(key, Arc::new(block), parent);
            }
            parent = Some(key);
        }
    }

    fn remove(&mut self, removed: &pb::BlockRemoved) {
        if removed.medium.as_deref() != Some(DEVICE_MEDIUM) {
            return;
        }
        for hash in &removed.block_hashes {
            self.held.remove(hash.cast_unsigned());
        }
    }
}

/// What `stored` says about its block `at` besides its hash, parent and tokens: the fields
/// of the whole store, and the block's entry of `mm_routing_spans`. Its `token_ids` are
/// `form`, and it has no `session_id`, which describes the event.
fn rest_of(stored: &pb::BlockStored, at: usize, form: &Ids) -> pb::BlockStored {
    pb::BlockStored {
        block_hashes: Vec::new(),
        parent_block_hash: None,
        token_ids: Some(form.clone()),
        block_size: stored.block_size,
        lora_id: stored.lora_id,
        medium: stored.medium.clone(),
        cache_salt: stored.cache_salt.clone(),
        session_id: None,
        lora_name: stored.lora_name.clone(),
        mm_routing_spans: stored
            .mm_routing_spans
            .get(at)
            .cloned()
            .into_iter()
            .collect(),
    }
}

/// Each [`rest_of`] a held block has, kept once however many blocks have the same: most
/// blocks of a cache do, unless their `mm_routing_spans` differ.
#[derive(Default)]
struct Rests {
    by_encoding: FxHashMap<Vec<u8>, Arc<pb::BlockStored>>,
    /// The size at which the entries no block uses are next dropped.
    sweep_at: usize,
}

impl Rests {
    fn shared(&mut self, rest: pb::BlockStored) -> Arc<pb::BlockStored> {
        if self.by_encoding.len() >= self.sweep_at {
            self.by_encoding
                .retain(|_, rest| Arc::strong_count(rest) > 1);
            // Arbitrary floor; doubling keeps the sweeps amortized.
            self.sweep_at = (2 * self.by_encoding.len()).max(64);
        }
        let rest = self
            .by_encoding
            .entry(rest.encode_to_vec())
            .or_insert_with(|| Arc::new(rest));
        rest.clone()
    }
}

/// The ledger's blocks as stores, each after the store that holds its parent.
fn stores(ledger: Ledger<Arc<StoredBlock>>) -> impl Iterator<Item = pb::BlockStored> + Send {
    let chains = ledger.chains().into_iter();
    chains.flat_map(|chain| chain_stores(chain.parent.map(u64::cast_signed), &chain.blocks))
}

/// One chain as stores: consecutive blocks with the same [`rest_of`] share a store.
fn chain_stores(
    mut parent: Option<i64>,
    blocks: &[(u64, Arc<StoredBlock>)],
) -> Vec<pb::BlockStored> {
    let mut stores: Vec<pb::BlockStored> = Vec::new();
    let mut last: Option<&StoredBlock> = None;
    for (_, block) in blocks {
        let joins = last.is_some_and(|last| Arc::ptr_eq(&last.rest, &block.rest));
        let fits = |store: &pb::BlockStored| {
            let blocks = store.block_hashes.len();
            blocks < STORE_BLOCKS && (blocks + 1) * store.block_size as usize <= STORE_TOKEN_IDS
        };
        match stores.last_mut() {
            Some(store) if joins && fits(store) => append(store, block),
            _ => {
                // The block's own spans are in its `rest`: `append` puts them back.
                let mut store = pb::BlockStored {
                    parent_block_hash: parent,
                    mm_routing_spans: Vec::new(),
                    ..(*block.rest).clone()
                };
                append(&mut store, block);
                stores.push(store);
            }
        }
        parent = Some(block.hash);
        last = Some(block);
    }
    stores
}

/// Adds `block` to the end of a store whose blocks have its `rest`.
fn append(store: &mut pb::BlockStored, block: &StoredBlock) {
    store.block_hashes.push(block.hash);
    let ids = block.token_ids.to_vec();
    match &mut store.token_ids {
        Some(Ids::Single(list)) => list.token_ids.extend(ids),
        Some(Ids::Pairs(list)) => {
            list.pairs
                .extend(ids.chunks_exact(2).map(|pair| pb::TokenIdPair {
                    first: pair[0],
                    second: pair[1],
                }))
        }
        None => {}
    }
    store
        .mm_routing_spans
        .extend(block.rest.mm_routing_spans.iter().cloned());
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::pb::kv_cache_event::Event;
    use super::*;

    struct Seeded(u64);

    impl Seeded {
        fn below(&mut self, bound: u64) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0 % bound
        }

        fn one_of(&mut self, of: &[i64]) -> Option<i64> {
            (!of.is_empty()).then(|| of[self.below(of.len() as u64) as usize])
        }
    }

    /// A store of one to three new blocks after `parent`, with or without each optional part.
    fn new_store(rng: &mut Seeded, last_hash: &mut i64, parent: Option<i64>) -> pb::BlockStored {
        let blocks = 1 + rng.below(3) as usize;
        let size = 1 + rng.below(2) as usize;
        // Some need more than 32 bits.
        let id = |rng: &mut Seeded| rng.below(3) << (30 * rng.below(2));
        let token_ids = if rng.below(8) == 0 {
            let pair = |_| pb::TokenIdPair {
                first: id(rng),
                second: id(rng),
            };
            Ids::Pairs(pb::TokenIdPairList {
                pairs: (0..blocks * size).map(pair).collect(),
            })
        } else {
            Ids::Single(pb::TokenIdList {
                token_ids: (0..blocks * size).map(|_| id(rng)).collect(),
            })
        };
        let labeled = rng.below(3) == 0;
        let block_hashes = (0..blocks).map(|_| {
            *last_hash += 1;
            *last_hash
        });
        pb::BlockStored {
            block_hashes: block_hashes.collect(),
            parent_block_hash: parent,
            token_ids: Some(token_ids),
            block_size: size as u32,
            medium: Some(DEVICE_MEDIUM.to_string()),
            lora_name: (rng.below(3) == 0).then(|| "adapter".to_string()),
            mm_routing_spans: (0..blocks)
                .filter(|_| labeled)
                .map(|_| pb::BlockMmRoutingSpans {
                    spans: vec![pb::MmRoutingSpan {
                        start: 0,
                        length: rng.below(2),
                        label: "image".to_string(),
                    }],
                })
                .collect(),
            ..Default::default()
        }
    }

    /// One store per block of `store`, saying of that block what `store` says, without the
    /// session.
    fn per_block(store: &pb::BlockStored) -> Vec<pb::BlockStored> {
        let size = store.block_size as usize;
        let mut parent = store.parent_block_hash;
        let blocks = store.block_hashes.iter().enumerate().map(|(at, &hash)| {
            let token_ids = match &store.token_ids {
                Some(Ids::Single(list)) => Ids::Single(pb::TokenIdList {
                    token_ids: list.token_ids[at * size..][..size].to_vec(),
                }),
                Some(Ids::Pairs(list)) => Ids::Pairs(pb::TokenIdPairList {
                    pairs: list.pairs[at * size..][..size].to_vec(),
                }),
                None => panic!("a store without token ids: {store:?}"),
            };
            pb::BlockStored {
                block_hashes: vec![hash],
                parent_block_hash: parent.replace(hash),
                token_ids: Some(token_ids),
                session_id: None,
                mm_routing_spans: (store.mm_routing_spans.get(at).cloned().into_iter()).collect(),
                ..store.clone()
            }
        });
        blocks.collect()
    }

    /// The stores of a snapshot of `blocks`, in order. Checks that the stores and the
    /// responses keep to their limits.
    fn snapshot_stores(blocks: &Blocks) -> Vec<pb::BlockStored> {
        use pb::get_kv_snapshot_response::Item;

        let responses = blocks.snapshot(String::new());
        let stores = responses.filter_map(|response| {
            assert!(response.encoded_len() < 2 * RESPONSE_BYTES);
            match response.item {
                Some(Item::Blocks(blocks)) => Some(blocks.stored),
                _ => None,
            }
        });
        let stores: Vec<_> = stores.flatten().collect();
        for store in &stores {
            let most = STORE_BLOCKS.min(STORE_TOKEN_IDS / store.block_size as usize);
            assert!(store.block_hashes.len() <= most);
        }
        stores
    }

    /// What a snapshot of `blocks` says, as one store per block, by hash. Checks that each
    /// block follows its parent.
    fn snapshot(blocks: &Blocks) -> Vec<pb::BlockStored> {
        let mut said: Vec<_> = snapshot_stores(blocks).iter().flat_map(per_block).collect();
        for (at, block) in said.iter().enumerate() {
            let parent = block.parent_block_hash;
            assert!(
                parent.is_none_or(|parent| said[..at].iter().any(|b| b.block_hashes[0] == parent))
            );
        }
        said.sort_by_key(|block| block.block_hashes[0]);
        said
    }

    fn batch(sequence_number: u64, events: impl IntoIterator<Item = Event>) -> pb::KvEventBatch {
        pb::KvEventBatch {
            sequence_number,
            events: (events
                .into_iter()
                .map(|event| pb::KvCacheEvent { event: Some(event) }))
            .collect(),
            ..Default::default()
        }
    }

    /// Over seeded stores, removes and clears, a snapshot says what a plain map of the held
    /// blocks says, and a snapshot followed by the next hundred batches gives the state that
    /// every batch gives. Removes take leaves only: a block whose parent was removed is left
    /// out of a snapshot, so a state loaded from one cannot get it back when the parent
    /// returns.
    #[test]
    fn a_snapshot_then_the_later_batches_equals_every_batch() {
        let mut rng = Seeded(0x9E37_79B9_7F4A_7C15);
        // Each held block as one store.
        let mut held: BTreeMap<i64, pb::BlockStored> = BTreeMap::new();
        let mut blocks = Blocks::default();
        let mut from_snapshot: Option<Blocks> = None;
        // Hashes are signed.
        let mut last_hash = -5;

        for sequence_number in 0..2000 {
            let hashes: Vec<i64> = held.keys().copied().collect();
            let parents: BTreeSet<i64> = (held.values())
                .filter_map(|block| block.parent_block_hash)
                .collect();
            let leaves: Vec<i64> = hashes
                .iter()
                .copied()
                .filter(|hash| !parents.contains(hash))
                .collect();
            let roll = rng.below(20);
            let leaf = rng.one_of(&leaves).filter(|_| roll < 7);
            // Rarely: a clear makes every state equal.
            let event = if rng.below(400) == 0 {
                held.clear();
                Event::AllBlocksCleared(pb::AllBlocksCleared {})
            } else if let Some(leaf) = leaf {
                // A remove on another tier leaves the device's block.
                let medium = if roll == 6 {
                    "CPU_PINNED"
                } else {
                    DEVICE_MEDIUM
                };
                if medium == DEVICE_MEDIUM {
                    held.remove(&leaf);
                }
                Event::BlockRemoved(pb::BlockRemoved {
                    block_hashes: vec![leaf],
                    medium: Some(medium.to_string()),
                })
            } else {
                // A held block again, or new blocks after a held block or none.
                let again = rng.one_of(&hashes).filter(|_| roll == 7);
                let mut store = match again {
                    Some(again) => held[&again].clone(),
                    None => {
                        let parent = rng.one_of(&hashes).filter(|_| rng.below(4) != 0);
                        new_store(&mut rng, &mut last_hash, parent)
                    }
                };
                store.session_id = Some("session".to_string());
                match roll {
                    8 => store.medium = Some("CPU_PINNED".to_string()),
                    // Its tokens no longer divide among its blocks.
                    9 => store.block_size += 1,
                    _ => {
                        for block in per_block(&store) {
                            held.entry(block.block_hashes[0]).or_insert(block);
                        }
                    }
                }
                Event::BlockStored(store)
            };

            let batch = batch(sequence_number, [event]);
            blocks.apply(&batch);
            from_snapshot
                .iter_mut()
                .for_each(|blocks| blocks.apply(&batch));

            if sequence_number % 100 == 99 {
                let expected: Vec<_> = held.values().cloned().collect();
                assert_eq!(snapshot(&blocks), expected);
                if let Some(from_snapshot) = &from_snapshot {
                    assert_eq!(snapshot(from_snapshot), expected);
                }

                let stores = snapshot_stores(&blocks).into_iter().map(Event::BlockStored);
                let loaded = from_snapshot.insert(Blocks::default());
                loaded.apply(&self::batch(sequence_number, stores));
            }
        }
    }

    /// A chain too long for one store is cut into stores, and stores too large for one
    /// response into responses, that still say what was stored.
    #[test]
    fn a_long_chain_is_cut_into_stores_and_responses() {
        for size in [1, 128] {
            let chain = 2 * STORE_BLOCKS + 1;
            let ids = pb::TokenIdList {
                // Ten bytes each.
                token_ids: vec![u64::MAX; chain * size],
            };
            let store = pb::BlockStored {
                block_hashes: (1..=chain as i64).collect(),
                token_ids: Some(Ids::Single(ids)),
                block_size: size as u32,
                medium: Some(DEVICE_MEDIUM.to_string()),
                ..Default::default()
            };
            let mut blocks = Blocks::default();
            blocks.apply(&batch(0, [Event::BlockStored(store.clone())]));
            assert_eq!(snapshot(&blocks), per_block(&store));
        }
    }

    /// After a loss, the live stream can restart before a batch that a replay delivered:
    /// that batch is not applied again and the cursor does not move back.
    #[test]
    fn a_batch_delivered_again_after_a_loss_is_not_applied_again() {
        let mut blocks = Blocks::default();
        let stored = new_store(&mut Seeded(1), &mut 0, None);
        let removed = pb::BlockRemoved {
            block_hashes: stored.block_hashes.clone(),
            medium: stored.medium.clone(),
        };
        let store = batch(0, [Event::BlockStored(stored)]);
        blocks.apply(&store);
        blocks.apply(&batch(1, [Event::BlockRemoved(removed)]));

        blocks.lost(0);
        blocks.apply(&store);
        let end = pb::KvSnapshotEnd {
            resume: Some(pb::KvEventCursor {
                stream_id: String::new(),
                next_sequence_number: 2,
            }),
            complete: false,
        };
        let end = pb::get_kv_snapshot_response::Item::End(end);
        let snapshot: Vec<_> = blocks.snapshot(String::new()).collect();
        assert_eq!(snapshot, [pb::GetKvSnapshotResponse { item: Some(end) }]);
    }
}
