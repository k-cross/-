use std::cmp::Reverse;
use std::collections::{BTreeSet, BinaryHeap, HashMap};

use crate::blob::{BlobId, BlobMeta};
use crate::stream::{Mark, Rank};
use crate::tier::TierSpec;

pub const STEP_BASE_NS: u64 = 7_000_000;
pub const STEP_PER_SEQ_NS: u64 = 40_000;
pub const MAX_BATCH: usize = 64;

const UTILISATION_CAP: f64 = 0.95;

#[derive(Clone, Copy, Debug)]
pub struct Decode {
    pub queue_ns: u64,
    pub exec_ns: u64,
    pub batch: usize,
}

#[derive(Debug)]
pub struct Engine {
    max_batch: usize,

    inflight: BinaryHeap<Reverse<u64>>,
    pub queue_ns: u64,
    pub batch_sum: u64,
    pub admitted: u64,
    pub saturated: u64,
}

impl Engine {
    #[must_use]
    pub fn new(max_batch: usize) -> Self {
        Self {
            max_batch: max_batch.max(1),
            inflight: BinaryHeap::new(),
            queue_ns: 0,
            batch_sum: 0,
            admitted: 0,
            saturated: 0,
        }
    }

    #[must_use]
    pub fn step_ns(batch: usize) -> u64 {
        STEP_BASE_NS + (batch.saturating_sub(1)) as u64 * STEP_PER_SEQ_NS
    }

    fn retire(&mut self, now_ns: u64) {
        while let Some(&Reverse(end)) = self.inflight.peek() {
            if end > now_ns {
                break;
            }
            self.inflight.pop();
        }
    }

    #[must_use]
    pub fn load(&self, now_ns: u64) -> usize {
        self.inflight
            .iter()
            .filter(|Reverse(e)| *e > now_ns)
            .count()
    }

    #[must_use]
    pub fn projected_ns(&self, now_ns: u64, tokens: u64, reserved: usize) -> u64 {
        self.projected_live(now_ns, tokens, self.load(now_ns) + reserved)
    }

    #[must_use]
    pub fn projected_live(&self, now_ns: u64, tokens: u64, live: usize) -> u64 {
        let wait = if live < self.max_batch {
            0
        } else {
            self.inflight
                .peek()
                .map_or(0, |&Reverse(e)| e.saturating_sub(now_ns))
        };
        wait + tokens * Self::step_ns(live.min(self.max_batch - 1) + 1)
    }

    #[must_use]
    pub fn congestion_ns(&self, now_ns: u64, tokens: u64, reserved: usize) -> u64 {
        self.congestion_live(tokens, self.load(now_ns) + reserved)
    }

    #[must_use]
    pub fn congestion_live(&self, tokens: u64, live: usize) -> u64 {
        let u = (live as f64 / self.max_batch as f64).min(UTILISATION_CAP);
        let widening = live as u64 * STEP_PER_SEQ_NS * tokens;
        (widening as f64 / (1.0 - u)) as u64
    }

    pub fn decode(&mut self, arrival_ns: u64, tokens: u64) -> Decode {
        self.retire(arrival_ns);
        let mut start = arrival_ns;
        if self.inflight.len() >= self.max_batch {
            self.saturated += 1;
            if let Some(&Reverse(end)) = self.inflight.peek() {
                start = start.max(end);
            }
            self.retire(start);
        }
        let batch = self.inflight.len() + 1;
        let exec_ns = tokens * Self::step_ns(batch);
        self.inflight.push(Reverse(start + exec_ns));
        let queue_ns = start - arrival_ns;
        self.queue_ns += queue_ns;
        self.batch_sum += batch as u64;
        self.admitted += 1;
        Decode {
            queue_ns,
            exec_ns,
            batch,
        }
    }

    #[must_use]
    pub fn mean_batch(&self) -> f64 {
        if self.admitted == 0 {
            0.0
        } else {
            self.batch_sum as f64 / self.admitted as f64
        }
    }
}

#[derive(Debug)]
pub struct Placed {
    pub evicted: Vec<(BlobId, BlobMeta)>,
    pub resident: bool,
}

#[derive(Clone, Copy, Debug)]
struct Block {
    meta: BlobMeta,
    key: u64,
    children: u32,
    pins: u32,
    mark: Option<Mark>,
}

#[derive(Debug)]
pub struct EngineCache {
    capacity: u64,
    used: u64,
    pinned: u64,
    leaf_first: bool,
    clairvoyant: bool,
    clock: u64,
    blocks: HashMap<BlobId, Block>,
    evictable: BTreeSet<(u64, BlobId)>,
    demoted: BTreeSet<(u64, BlobId)>,
    retained: BTreeSet<(u64, BlobId)>,
    expiries: BinaryHeap<Reverse<(u64, BlobId)>>,
    now: u64,
    staging: Vec<BlobId>,
    inflight: BinaryHeap<Reverse<(u64, u64)>>,
    held: HashMap<u64, Vec<BlobId>>,
    next_seq: u64,
    recovery: Option<TierSpec>,
    last_price: f64,
    pub evictions: u64,
    pub preemptions: u64,
    pub marks_applied: u64,
    pub marks_expired: u64,
    pub pressure_evictions: u64,
}

impl EngineCache {
    #[must_use]
    pub fn new(capacity: u64, leaf_first: bool) -> Self {
        Self {
            capacity,
            used: 0,
            pinned: 0,
            leaf_first,
            clairvoyant: false,
            clock: 0,
            blocks: HashMap::new(),
            evictable: BTreeSet::new(),
            demoted: BTreeSet::new(),
            retained: BTreeSet::new(),
            expiries: BinaryHeap::new(),
            now: 0,
            staging: Vec::new(),
            inflight: BinaryHeap::new(),
            held: HashMap::new(),
            next_seq: 0,
            recovery: None,
            last_price: 0.0,
            evictions: 0,
            preemptions: 0,
            marks_applied: 0,
            marks_expired: 0,
            pressure_evictions: 0,
        }
    }

    #[must_use]
    pub fn with_recovery(mut self, spec: TierSpec) -> Self {
        self.recovery = Some(spec);
        self
    }

    #[must_use]
    pub fn with_clairvoyance(mut self) -> Self {
        self.clairvoyant = true;
        self
    }

    #[must_use]
    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    #[must_use]
    pub fn used(&self) -> u64 {
        self.used
    }

    #[must_use]
    pub fn free(&self) -> u64 {
        self.capacity.saturating_sub(self.used)
    }

    #[must_use]
    pub fn pinned(&self) -> u64 {
        self.pinned
    }

    #[must_use]
    pub fn contains(&self, id: &BlobId) -> bool {
        self.blocks.contains_key(id)
    }

    pub fn ids(&self) -> impl Iterator<Item = BlobId> + '_ {
        self.blocks.keys().copied()
    }

    #[must_use]
    pub fn orphans(&self) -> usize {
        self.blocks
            .values()
            .filter(|b| b.meta.parent.is_some_and(|p| !self.blocks.contains_key(&p)))
            .count()
    }

    fn detach(&mut self, id: BlobId) {
        let Some(b) = self.blocks.get(&id) else {
            return;
        };
        self.evictable.remove(&(b.key, id));
        self.demoted.remove(&(b.key, id));
        if let Some(m) = b.mark {
            self.retained.remove(&(m.until_ns, id));
        }
    }

    fn attach(&mut self, id: BlobId) {
        let Some(b) = self.blocks.get(&id) else {
            return;
        };
        if b.pins > 0 || (self.leaf_first && b.children > 0) {
            return;
        }
        match b.mark {
            None => self.evictable.insert((b.key, id)),
            Some(Mark {
                rank: Rank::EvictFirst,
                ..
            }) => self.demoted.insert((b.key, id)),
            Some(Mark {
                rank: Rank::Retain,
                until_ns,
            }) => self.retained.insert((until_ns, id)),
        };
    }

    fn reslot(&mut self, id: BlobId, f: impl FnOnce(&mut Block)) {
        if !self.blocks.contains_key(&id) {
            return;
        }
        self.detach(id);
        if let Some(b) = self.blocks.get_mut(&id) {
            f(b);
        }
        self.attach(id);
    }

    fn take_victim(&mut self) -> Option<BlobId> {
        if let Some(&entry) = self.demoted.iter().next() {
            self.demoted.remove(&entry);
            return Some(entry.1);
        }
        if let Some(&entry) = self.evictable.iter().next() {
            self.evictable.remove(&entry);
            return Some(entry.1);
        }
        let entry = *self.retained.iter().next()?;
        self.retained.remove(&entry);
        self.pressure_evictions += 1;
        Some(entry.1)
    }

    fn next_victim(&self) -> Option<BlobId> {
        self.demoted
            .iter()
            .next()
            .or_else(|| self.evictable.iter().next())
            .or_else(|| self.retained.iter().next())
            .map(|&(_, id)| id)
    }

    pub fn mark(&mut self, id: BlobId, mark: Mark) -> Option<Mark> {
        if !mark.live_at(self.now) {
            return None;
        }
        let next = match self.blocks.get(&id)?.mark {
            None => mark,
            Some(held) if mark.rank > held.rank => mark,
            Some(held) if mark.rank == held.rank && mark.until_ns > held.until_ns => mark,
            Some(_) => return None,
        };
        self.reslot(id, |b| b.mark = Some(next));
        self.expiries.push(Reverse((next.until_ns, id)));
        self.marks_applied += 1;
        Some(next)
    }

    pub fn expire(&mut self, now: u64) {
        self.now = now;
        while let Some(&Reverse((until, id))) = self.expiries.peek() {
            if until > now {
                break;
            }
            self.expiries.pop();
            let current = self
                .blocks
                .get(&id)
                .is_some_and(|b| b.mark.is_some_and(|m| m.until_ns == until));
            if current {
                self.reslot(id, |b| b.mark = None);
                self.marks_expired += 1;
            }
        }
    }

    #[must_use]
    pub fn meta_of(&self, id: &BlobId) -> Option<BlobMeta> {
        self.blocks.get(id).map(|b| b.meta)
    }

    #[must_use]
    pub fn marked(&self) -> (u64, u64) {
        self.blocks
            .values()
            .filter(|b| b.mark.is_some())
            .fold((0, 0), |(n, bytes), b| (n + 1, bytes + b.meta.bytes))
    }

    #[must_use]
    pub fn live_marks(&self, now: u64) -> Vec<(BlobId, Mark)> {
        let mut live: Vec<(BlobId, Mark)> = self
            .blocks
            .iter()
            .filter_map(|(id, b)| b.mark.filter(|m| m.live_at(now)).map(|m| (*id, m)))
            .collect();
        live.sort_unstable_by_key(|(id, _)| *id);
        live
    }

    #[must_use]
    pub fn mark_of(&self, id: &BlobId) -> Option<Mark> {
        self.blocks.get(id)?.mark
    }

    fn next_key(&mut self) -> u64 {
        self.clock += 1;
        self.clock
    }

    fn pin(&mut self, id: BlobId) {
        let mut newly = 0;
        self.reslot(id, |b| {
            if b.pins == 0 {
                newly = b.meta.bytes;
            }
            b.pins += 1;
        });
        self.pinned += newly;
        self.staging.push(id);
    }

    fn unpin(&mut self, id: BlobId) {
        let mut freed = 0;
        self.reslot(id, |b| {
            if b.pins == 1 {
                freed = b.meta.bytes;
            }
            b.pins = b.pins.saturating_sub(1);
        });
        self.pinned -= freed;
    }

    fn loss_per_byte(&self, meta: &BlobMeta) -> f64 {
        let rebuild = meta.value_per_byte();
        self.recovery.map_or(rebuild, |r| {
            (r.fetch_ns(meta.bytes) as f64 / meta.bytes as f64).min(rebuild)
        })
    }

    pub fn touch(&mut self, id: BlobId, pin: bool) {
        if !self.blocks.contains_key(&id) {
            return;
        }
        if !self.clairvoyant {
            let key = self.next_key();
            self.reslot(id, |b| b.key = key);
        }
        if pin {
            self.pin(id);
        }
    }

    pub fn admit(&mut self, id: BlobId, meta: BlobMeta, pin: bool) -> Placed {
        let mut placed = Placed {
            evicted: Vec::new(),
            resident: true,
        };
        if self.blocks.contains_key(&id) {
            self.touch(id, pin);
            return placed;
        }
        let parent = meta.parent.filter(|p| self.blocks.contains_key(p));
        if let Some(p) = parent {
            self.reslot(p, |b| b.children += 1);
        }
        while self.used + meta.bytes > self.capacity {
            let Some(victim) = self.take_victim() else {
                if let Some(p) = parent {
                    self.reslot(p, |b| b.children = b.children.saturating_sub(1));
                }
                if pin {
                    self.preemptions += 1;
                }
                placed.resident = false;
                return placed;
            };
            let Some(b) = self.blocks.remove(&victim) else {
                continue;
            };
            self.used -= b.meta.bytes;
            self.evictions += 1;
            self.last_price = self.loss_per_byte(&b.meta);
            if let Some(vp) = b.meta.parent {
                self.reslot(vp, |pb| pb.children = pb.children.saturating_sub(1));
            }
            placed.evicted.push((victim, b.meta));
        }
        let key = if self.clairvoyant { 0 } else { self.next_key() };
        self.blocks.insert(
            id,
            Block {
                meta,
                key,
                children: 0,
                pins: 0,
                mark: None,
            },
        );
        self.used += meta.bytes;
        self.evictable.insert((key, id));
        if pin {
            self.pin(id);
        }
        placed
    }

    pub fn remove(&mut self, id: &BlobId) -> Option<BlobMeta> {
        self.detach(*id);
        let b = self.blocks.remove(id)?;
        self.used -= b.meta.bytes;
        if b.pins > 0 {
            self.pinned -= b.meta.bytes;
        }
        if let Some(p) = b.meta.parent {
            self.reslot(p, |pb| pb.children = pb.children.saturating_sub(1));
        }
        Some(b.meta)
    }

    pub fn reprice(&mut self, id: BlobId, next_use: Option<u64>) {
        if !self.clairvoyant {
            return;
        }
        let key = next_use.map_or(0, |pos| u64::MAX - pos);
        self.reslot(id, |b| b.key = key);
    }

    pub fn seal(&mut self, until: Option<u64>) {
        let blocks = std::mem::take(&mut self.staging);
        match until {
            Some(end) => {
                let seq = self.next_seq;
                self.next_seq += 1;
                self.held.insert(seq, blocks);
                self.inflight.push(Reverse((end, seq)));
            }
            None => {
                for id in blocks {
                    self.unpin(id);
                }
            }
        }
    }

    pub fn release(&mut self, now_ns: u64) {
        while let Some(&Reverse((end, seq))) = self.inflight.peek() {
            if end > now_ns {
                break;
            }
            self.inflight.pop();
            for id in self.held.remove(&seq).unwrap_or_default() {
                self.unpin(id);
            }
        }
    }

    pub fn drain(&mut self) {
        self.blocks.clear();
        self.evictable.clear();
        self.demoted.clear();
        self.retained.clear();
        self.expiries.clear();
        self.staging.clear();
        self.inflight.clear();
        self.held.clear();
        self.used = 0;
        self.pinned = 0;
    }

    #[must_use]
    pub fn price_of(&self, meta: &BlobMeta) -> f64 {
        self.loss_per_byte(meta)
    }

    #[must_use]
    pub fn tail_price(&self) -> f64 {
        self.next_victim()
            .and_then(|id| self.blocks.get(&id))
            .map_or(self.last_price, |b| self.loss_per_byte(&b.meta))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blob::BlobKind;

    const BLOCK: u64 = 100;

    fn chain(tag: &str, len: usize) -> Vec<(BlobId, BlobMeta)> {
        let mut out = Vec::with_capacity(len);
        let mut parent: Option<BlobId> = None;
        for i in 0..len {
            let id = parent.map_or_else(
                || BlobId::leaf(format!("{tag}:{i}").as_bytes()),
                |p| BlobId::chain(p, format!("{tag}:{i}").as_bytes()),
            );
            out.push((
                id,
                BlobMeta {
                    kind: BlobKind::KvBlock,
                    bytes: BLOCK,
                    parent,
                    recompute_ns: 1_000,
                },
            ));
            parent = Some(id);
        }
        out
    }

    #[test]
    fn a_sequence_larger_than_the_partition_preempts_and_is_never_refused() {
        let mut cache = EngineCache::new(3 * BLOCK, true);
        let seq = chain("big", 5);
        let mut kept = 0;
        for &(id, meta) in &seq {
            let placed = cache.admit(id, meta, true);
            if !placed.resident {
                break;
            }
            kept += 1;
        }
        assert_eq!(kept, 3, "the partition holds exactly three blocks");
        assert_eq!(cache.preemptions, 1);
        assert_eq!(cache.used(), 3 * BLOCK);
        cache.seal(None);
        assert_eq!(cache.pinned(), 0, "a preempted sequence holds nothing");
    }

    #[test]
    fn eviction_is_lru_over_unpinned_leaves_and_never_opens_a_hole() {
        let mut cache = EngineCache::new(6 * BLOCK, true);
        let a = chain("a", 3);
        let b = chain("b", 3);
        for &(id, meta) in &a {
            assert!(cache.admit(id, meta, true).resident);
        }
        cache.seal(None);
        for &(id, meta) in &b {
            assert!(cache.admit(id, meta, true).resident);
        }
        cache.seal(None);
        let c = chain("c", 4);
        let mut evicted = Vec::new();
        for &(id, meta) in &c {
            let placed = cache.admit(id, meta, true);
            assert!(placed.resident);
            evicted.extend(placed.evicted.into_iter().map(|(id, _)| id));
        }
        assert_eq!(
            evicted,
            vec![a[2].0, a[1].0, a[0].0, b[2].0],
            "oldest chain first, tail before parent"
        );
        assert_eq!(cache.orphans(), 0);
        assert!(cache.contains(&b[0].0) && cache.contains(&b[1].0));
    }

    #[test]
    fn a_running_sequence_is_not_evicted_until_it_is_released() {
        let mut cache = EngineCache::new(4 * BLOCK, true);
        let running = chain("run", 2);
        for &(id, meta) in &running {
            cache.admit(id, meta, true);
        }
        cache.seal(Some(1_000));
        let arriving = chain("new", 3);
        let mut resident = 0;
        for &(id, meta) in &arriving {
            if !cache.admit(id, meta, true).resident {
                break;
            }
            resident += 1;
        }
        assert_eq!(
            resident, 2,
            "only the free half is available while `run` decodes"
        );
        assert_eq!(cache.preemptions, 1);
        assert!(cache.contains(&running[0].0) && cache.contains(&running[1].0));
        cache.seal(None);
        cache.release(1_000);
        assert_eq!(cache.pinned(), 0);
        for &(id, meta) in &arriving {
            assert!(cache.admit(id, meta, true).resident);
        }
        assert_eq!(cache.orphans(), 0);
    }

    #[test]
    fn clairvoyant_evicts_the_block_used_furthest_ahead_and_the_never_again_block_first() {
        let mut cache = EngineCache::new(3 * BLOCK, false).with_clairvoyance();
        let blocks: Vec<_> = (0..4).map(|i| chain(&format!("k{i}"), 1)[0]).collect();
        for &(id, meta) in &blocks[..3] {
            cache.admit(id, meta, false);
        }
        cache.reprice(blocks[0].0, Some(50));
        cache.reprice(blocks[1].0, None);
        cache.reprice(blocks[2].0, Some(10));
        let first = cache.admit(blocks[3].0, blocks[3].1, false);
        assert_eq!(first.evicted[0].0, blocks[1].0, "never again goes first");
        cache.reprice(blocks[3].0, Some(20));
        let fifth = chain("k4", 1)[0];
        let second = cache.admit(fifth.0, fifth.1, false);
        assert_eq!(
            second.evicted[0].0, blocks[0].0,
            "then the furthest next use"
        );
    }

    fn block(tag: &str) -> (BlobId, BlobMeta) {
        chain(tag, 1)[0]
    }

    fn retain(until_ns: u64) -> Mark {
        Mark {
            rank: Rank::Retain,
            until_ns,
        }
    }

    fn demote(until_ns: u64) -> Mark {
        Mark {
            rank: Rank::EvictFirst,
            until_ns,
        }
    }

    fn filled(capacity: usize, tags: &[&str]) -> (EngineCache, Vec<(BlobId, BlobMeta)>) {
        let mut cache = EngineCache::new(capacity as u64 * BLOCK, false);
        let blocks: Vec<_> = tags.iter().map(|t| block(t)).collect();
        for &(id, meta) in &blocks {
            assert!(cache.admit(id, meta, false).resident);
        }
        (cache, blocks)
    }

    #[test]
    fn evict_first_goes_before_unmarked_and_retained_goes_after() {
        let (mut cache, b) = filled(3, &["a", "b", "c"]);
        assert!(cache.mark(b[2].0, demote(1_000)).is_some());
        assert!(cache.mark(b[0].0, retain(1_000)).is_some());
        let mut gone = Vec::new();
        for tag in ["d", "e", "f"] {
            let (id, meta) = block(tag);
            gone.extend(
                cache
                    .admit(id, meta, false)
                    .evicted
                    .into_iter()
                    .map(|(v, _)| v),
            );
        }
        assert_eq!(gone, vec![b[2].0, b[1].0, block("d").0]);
        assert!(
            cache.contains(&b[0].0),
            "the retained block outlives all three"
        );
        assert_eq!(cache.pressure_evictions, 0);
    }

    #[test]
    fn under_pressure_the_soonest_expiring_retained_block_goes_and_is_counted() {
        let (mut cache, b) = filled(2, &["a", "b"]);
        cache.mark(b[0].0, retain(500));
        cache.mark(b[1].0, retain(300));
        let (id, meta) = block("c");
        let placed = cache.admit(id, meta, false);
        assert!(placed.resident, "a mark never causes a preemption");
        assert_eq!(placed.evicted[0].0, b[1].0);
        assert_eq!(cache.pressure_evictions, 1);
        assert_eq!(cache.preemptions, 0);
        assert!(cache.contains(&b[0].0));
    }

    #[test]
    fn an_expired_mark_is_an_unmarked_block_to_every_later_eviction() {
        let (mut cache, b) = filled(2, &["a", "b"]);
        cache.mark(b[0].0, retain(100));
        cache.expire(99);
        assert!(cache.mark_of(&b[0].0).is_some());
        cache.expire(100);
        assert_eq!(cache.mark_of(&b[0].0), None);
        assert_eq!(cache.marks_expired, 1);
        let (id, meta) = block("c");
        let placed = cache.admit(id, meta, false);
        assert_eq!(placed.evicted[0].0, b[0].0, "the oldest unmarked block");
        assert_eq!(cache.pressure_evictions, 0);
    }

    #[test]
    fn a_touch_without_a_directive_leaves_a_live_mark_alone() {
        let (mut cache, b) = filled(2, &["a", "b"]);
        cache.mark(b[0].0, retain(100));
        cache.touch(b[0].0, false);
        assert_eq!(cache.mark_of(&b[0].0), Some(retain(100)));
    }

    #[test]
    fn marks_combine_by_rank_and_then_by_the_latest_deadline() {
        let (mut cache, b) = filled(1, &["a"]);
        let id = b[0].0;
        assert_eq!(cache.mark(id, demote(50)), Some(demote(50)));
        assert_eq!(cache.mark(id, demote(40)), None);
        assert_eq!(cache.mark(id, demote(90)), Some(demote(90)));
        assert_eq!(
            cache.mark(id, retain(60)),
            Some(retain(60)),
            "retain escalates"
        );
        assert_eq!(
            cache.mark(id, demote(500)),
            None,
            "evict-first never demotes a retain"
        );
        assert_eq!(cache.mark(id, retain(80)), Some(retain(80)));
        assert_eq!(cache.mark(id, retain(70)), None);
        assert_eq!(cache.marks_applied, 4);
        cache.expire(60);
        assert_eq!(
            cache.mark_of(&id),
            Some(retain(80)),
            "the stale deadline expires nothing"
        );
        assert_eq!(cache.marks_expired, 0);
    }

    #[test]
    fn a_mark_whose_deadline_is_past_on_receipt_does_nothing() {
        let (mut cache, b) = filled(1, &["a"]);
        cache.expire(50);
        assert_eq!(cache.mark(b[0].0, retain(50)), None);
        assert_eq!(cache.mark(b[0].0, demote(10)), None);
        assert_eq!(cache.marks_applied, 0);
        assert_eq!(cache.mark_of(&b[0].0), None);
    }

    #[test]
    fn a_marked_parent_is_no_candidate_while_it_has_a_resident_child() {
        let mut cache = EngineCache::new(3 * BLOCK, true);
        let seq = chain("p", 2);
        for &(id, meta) in &seq {
            cache.admit(id, meta, false);
        }
        cache.mark(seq[0].0, demote(1_000));
        let other = block("o");
        cache.admit(other.0, other.1, false);
        let extra = block("x");
        let placed = cache.admit(extra.0, extra.1, false);
        assert_eq!(placed.evicted[0].0, seq[1].0, "the child leaf goes first");
        let placed = cache.admit(block("y").0, block("y").1, false);
        assert_eq!(
            placed.evicted[0].0, seq[0].0,
            "then the demoted parent, now a leaf"
        );
    }

    #[test]
    fn an_engine_with_no_marks_evicts_exactly_as_before() {
        let mut plain = EngineCache::new(4 * BLOCK, true);
        let mut marked = EngineCache::new(4 * BLOCK, true);
        marked.expire(10);
        for tag in ["a", "b", "c", "d", "e", "f", "g"] {
            let seq = chain(tag, 2);
            for &(id, meta) in &seq {
                let l = plain.admit(id, meta, false);
                let r = marked.admit(id, meta, false);
                assert_eq!(l.resident, r.resident);
                assert_eq!(
                    l.evicted.iter().map(|(v, _)| *v).collect::<Vec<_>>(),
                    r.evicted.iter().map(|(v, _)| *v).collect::<Vec<_>>()
                );
            }
        }
        assert_eq!(plain.evictions, marked.evictions);
    }

    #[test]
    fn tail_price_is_the_next_victims_loss_and_recovery_caps_it() {
        let seq = chain("price", 1);
        let mut rebuild = EngineCache::new(2 * BLOCK, true);
        rebuild.admit(seq[0].0, seq[0].1, false);
        assert!((rebuild.tail_price() - 10.0).abs() < 1e-9);
        let mut offloadable = EngineCache::new(2 * BLOCK, true).with_recovery(TierSpec {
            capacity: 0,
            fixed_ns: 100,
            ns_per_byte: 0.0,
        });
        offloadable.admit(seq[0].0, seq[0].1, false);
        assert!((offloadable.tail_price() - 1.0).abs() < 1e-9);
    }
}
