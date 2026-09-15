use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashMap};

use crate::blob::{BlobId, BlobKind, BlobMeta};
use crate::flow::FlowHint;
use crate::tier::TierSpec;

const FREQ_CAP: u32 = 16;

// A replica with live connections cannot be evicted at any price; only an idle one is a candidate.
const SERVING_WINDOW: u64 = 600;

const PINNED_SCAN_LIMIT: u32 = 8;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Policy {
    Gdsf,
    #[allow(dead_code, reason = "baseline policy retained for arm comparison")]
    Lru,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Admission {
    Admitted,
    Pending,
}

/// Per-class guarantees. A class at or below its floor is never reclaimed from.
/// `hard` additionally forbids growing past the floor, turning it into a partition.
#[derive(Clone, Copy, Debug)]
pub struct Quota {
    /// Priority band per class: 0 is latency-critical, higher is more sacrificial. Operator
    /// configuration, not a property of the workload kind -- the control plane does not know
    /// which of a user's workloads matters most.
    pub band: [u8; BlobKind::N],
    pub floor: [u64; BlobKind::N],
    /// Soft ceiling: `floor[k] + slack`. A class may grow past it into free space, but may not
    /// *preempt* a more-sacrificial band to get there, so it can never consume another
    /// workload's guaranteed floor.
    pub limit: [u64; BlobKind::N],
    pub hard: bool,
}

impl Quota {
    #[must_use]
    pub fn open(capacity: u64, band: [u8; BlobKind::N]) -> Self {
        Self {
            band,
            floor: [0; BlobKind::N],
            limit: [capacity; BlobKind::N],
            hard: false,
        }
    }

    #[must_use]
    pub fn max_band(&self) -> u8 {
        self.band.iter().copied().max().unwrap_or(0)
    }

    /// `split` may sum to less than 1; the remainder is shared slack every class can grow into.
    #[must_use]
    pub fn from_split(
        capacity: u64,
        split: [f64; BlobKind::N],
        band: [u8; BlobKind::N],
        hard: bool,
    ) -> Self {
        let floor = split.map(|f| (capacity as f64 * f) as u64);
        let reserved: u64 = floor.iter().sum();
        let slack = capacity.saturating_sub(reserved);
        let limit = if hard {
            floor
        } else {
            floor.map(|f| f + slack)
        };
        Self {
            band,
            floor,
            limit,
            hard,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct Entry {
    meta: BlobMeta,
    /// Anticipated near-term accesses announced by a flow, in the same units as `freq`.
    /// Prewarming is therefore a change of *value*, not a separate subsystem.
    expect: f64,
    last_touch: u64,
    freq: u32,
    resident_children: u32,
    priority: f64,
    epoch: u64,
}

#[derive(Clone, Copy, Debug)]
struct Ranked {
    priority: f64,
    epoch: u64,
    id: BlobId,
}

impl PartialEq for Ranked {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for Ranked {}
impl PartialOrd for Ranked {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Ranked {
    fn cmp(&self, other: &Self) -> Ordering {
        self.priority
            .total_cmp(&other.priority)
            .then(self.epoch.cmp(&other.epoch))
            .then(self.id.cmp(&other.id))
    }
}

#[derive(Debug)]
pub struct TierPool {
    spec: TierSpec,
    policy: Policy,
    leaf_first: bool,
    quota: Quota,
    used: u64,
    by_kind: [u64; BlobKind::N],
    clock: u64,
    epoch: u64,
    inflation: f64,
    entries: HashMap<BlobId, Entry>,
    evictable: [BinaryHeap<Reverse<Ranked>>; BlobKind::N],
    pub evicted: [u64; BlobKind::N],
    pub refused: [u64; BlobKind::N],
    pub pinned_skips: u64,
    pub nonleaf_drops: u64,
}

impl TierPool {
    #[must_use]
    pub fn new(spec: TierSpec, policy: Policy, leaf_first: bool, quota: Quota) -> Self {
        Self {
            spec,
            policy,
            leaf_first,
            quota,
            used: 0,
            by_kind: [0; BlobKind::N],
            clock: 0,
            epoch: 0,
            inflation: 0.0,
            entries: HashMap::new(),
            evictable: std::array::from_fn(|_| BinaryHeap::new()),
            evicted: [0; BlobKind::N],
            refused: [0; BlobKind::N],
            pinned_skips: 0,
            nonleaf_drops: 0,
        }
    }

    #[must_use]
    pub fn spec(&self) -> &TierSpec {
        &self.spec
    }

    pub fn resident_ids(&self) -> impl Iterator<Item = BlobId> + '_ {
        self.entries.keys().copied()
    }

    #[must_use]
    pub fn used(&self) -> u64 {
        self.used
    }

    #[must_use]
    pub fn contains(&self, id: &BlobId) -> bool {
        self.entries.contains_key(id)
    }

    #[must_use]
    pub fn resident_bytes(&self, kind: BlobKind) -> u64 {
        self.by_kind[kind.idx()]
    }

    #[must_use]
    pub fn over_capacity(&self) -> bool {
        self.used > self.spec.capacity
    }

    fn score(&self, meta: &BlobMeta, freq: u32, expect: f64) -> f64 {
        match self.policy {
            Policy::Gdsf => {
                self.inflation + (f64::from(freq.min(FREQ_CAP)) + expect) * meta.value_per_byte()
            }
            Policy::Lru => self.clock as f64,
        }
    }

    /// Raise a blob's value because a flow says it is about to be needed. Expressed in the
    /// ledger's own currency, so an anticipated access competes with a real one directly.
    pub fn anticipate(&mut self, id: BlobId, weight: f64) {
        let (inflation, policy, clock) = (self.inflation, self.policy, self.clock);
        let Some(e) = self.entries.get_mut(&id) else {
            return;
        };
        // One announced access is worth at most one access. Accumulating would let a
        // repeatedly-announced blob outrank anything real and never fall back.
        let expect = e.expect.max(weight.min(1.0));
        if (expect - e.expect).abs() < f64::EPSILON {
            return;
        }
        e.expect = expect;
        e.priority = match policy {
            Policy::Gdsf => {
                inflation + (f64::from(e.freq.min(FREQ_CAP)) + expect) * e.meta.value_per_byte()
            }
            Policy::Lru => clock as f64,
        };
        self.reheap(id);
    }

    #[must_use]
    pub fn free_bytes(&self) -> u64 {
        self.spec.capacity.saturating_sub(self.used)
    }

    /// Free space plus burstable bytes that are actually reclaimable. Deliberately
    /// conservative: bytes held by a class whose entries are typically pinned while serving
    /// are not counted, since promising against them is how a task gets admitted and then
    /// stalls.
    #[must_use]
    pub fn reclaimable(&self) -> u64 {
        let burst: u64 = (0..BlobKind::N)
            .filter(|&k| BlobKind::ALL[k] != BlobKind::ServiceHeap)
            .map(|k| self.by_kind[k].saturating_sub(self.quota.floor[k]))
            .sum();
        self.free_bytes() + burst
    }

    fn is_serving(&self, e: &Entry) -> bool {
        e.meta.kind == BlobKind::ServiceHeap
            && self.clock.saturating_sub(e.last_touch) < SERVING_WINDOW
    }

    fn reheap(&mut self, id: BlobId) {
        let Some(e) = self.entries.get_mut(&id) else {
            return;
        };
        self.epoch += 1;
        e.epoch = self.epoch;
        let (k, ranked) = (
            e.meta.kind.idx(),
            Ranked {
                priority: e.priority,
                epoch: e.epoch,
                id,
            },
        );
        self.evictable[k].push(Reverse(ranked));
    }

    pub fn touch(&mut self, id: BlobId) {
        self.clock += 1;
        let Some(e) = self.entries.get_mut(&id) else {
            return;
        };
        e.freq = e.freq.saturating_add(1);
        e.last_touch = self.clock;
        e.expect = 0.0;
        let (meta, freq) = (e.meta, e.freq);
        let p = self.score(&meta, freq, 0.0);
        if let Some(e) = self.entries.get_mut(&id) {
            e.priority = p;
        }
        self.reheap(id);
    }

    fn unlink_parent(&mut self, parent: Option<BlobId>) {
        let Some(p) = parent else { return };
        let Some(e) = self.entries.get_mut(&p) else {
            return;
        };
        e.resident_children = e.resident_children.saturating_sub(1);
        if e.resident_children == 0 {
            self.reheap(p);
        }
    }

    fn link_parent(&mut self, parent: Option<BlobId>) {
        let Some(p) = parent else { return };
        if let Some(e) = self.entries.get_mut(&p) {
            e.resident_children += 1;
        }
    }

    fn clean_top(&mut self, k: usize, parked: &mut Vec<(usize, Ranked)>) -> Option<f64> {
        let mut skipped = 0;
        loop {
            let Reverse(r) = *self.evictable[k].peek()?;
            let stale = self.entries.get(&r.id).is_none_or(|e| e.epoch != r.epoch);
            if stale {
                self.evictable[k].pop();
                continue;
            }
            let e = self.entries[&r.id];
            // A non-leaf is not pinned, just not yet evictable. unlink_parent re-heaps it the
            // moment its last child goes, so drop it rather than paying to carry it.
            if self.leaf_first && e.resident_children > 0 {
                self.evictable[k].pop();
                self.nonleaf_drops += 1;
                continue;
            }
            if self.is_serving(&e) {
                self.pinned_skips += 1;
                self.evictable[k].pop();
                parked.push((k, r));
                skipped += 1;
                // A serving replica must come back. If the class's cheapest bytes are all
                // serving, look elsewhere rather than draining a heap we cannot reclaim from.
                if skipped >= PINNED_SCAN_LIMIT {
                    return None;
                }
                continue;
            }
            return Some(r.priority);
        }
    }

    fn cheapest(
        &mut self,
        parked: &mut Vec<(usize, Ranked)>,
        band: u8,
        above_floor: bool,
    ) -> Option<usize> {
        let mut best: Option<(usize, f64)> = None;
        for k in 0..BlobKind::N {
            if self.quota.band[k] != band {
                continue;
            }
            if above_floor && self.by_kind[k] <= self.quota.floor[k] {
                continue;
            }
            if let Some(p) = self.clean_top(k, parked)
                && best.is_none_or(|(_, bp)| p < bp)
            {
                best = Some((k, p));
            }
        }
        best.map(|(k, _)| k)
    }

    /// Reclaim order is lexicographic by band, then by price.
    ///
    /// Admitting into band `b` may take freely from any more-sacrificial band (floors there
    /// do not protect against a higher-priority admission), then from band `b` itself under
    /// the floor rules -- above floor first, since a floor is a preference and not a barrier.
    /// Bands below `b` are never touched, so best-effort work cannot displace
    /// latency-critical state however valuable its bytes look per byte.
    /// Reclaim takes from classes **above their floor**, most-sacrificial band first, then
    /// cheapest within the band.
    ///
    /// Floors are inviolable, so no class can be pushed below its guarantee by any other
    /// whatever its priority -- that is the anti-starvation property. Band orders only the
    /// burstable bytes above those floors, so a best-effort class gives up its slack long
    /// before a latency-critical one does, but a critical class cannot permanently own slack
    /// it merely reached first.
    fn pick_class(&mut self, want: usize, parked: &mut Vec<(usize, Ranked)>) -> Option<usize> {
        if self.quota.hard {
            return self.clean_top(want, parked).map(|_| want);
        }
        // At or over its soft limit a class may recycle its own bytes and take free space,
        // but may not preempt anyone else -- otherwise one class pushes every other down to
        // its floor and holds there.
        if self.by_kind[want] >= self.quota.limit[want] {
            return self.clean_top(want, parked).map(|_| want);
        }
        // Every class above its floor is a candidate, most-sacrificial band first. Protecting
        // a critical band's *burstable* bytes as well as its floor is what starves everyone
        // else: the guarantee is the floor, and nothing above it is owned.
        for b in (0..=self.quota.max_band()).rev() {
            if let Some(k) = self.cheapest(parked, b, true) {
                return Some(k);
            }
        }
        None
    }

    fn take_victim(&mut self, k: usize) -> Option<(BlobId, Entry)> {
        let Reverse(r) = self.evictable[k].pop()?;
        let e = self.entries.remove(&r.id)?;
        self.used -= e.meta.bytes;
        self.by_kind[k] -= e.meta.bytes;
        self.inflation = r.priority;
        self.evicted[k] += 1;
        self.unlink_parent(e.meta.parent);
        Some((r.id, e))
    }

    pub fn admit(
        &mut self,
        id: BlobId,
        meta: BlobMeta,
        out: &mut Vec<(BlobId, BlobMeta)>,
    ) -> Admission {
        if self.entries.contains_key(&id) {
            self.touch(id);
            return Admission::Admitted;
        }
        let k = meta.kind.idx();
        let ceiling = if self.quota.hard {
            self.quota.floor[k].min(self.spec.capacity)
        } else {
            self.spec.capacity
        };
        if meta.bytes > ceiling {
            self.refused[k] += 1;
            return Admission::Pending;
        }
        self.link_parent(meta.parent);
        let mut parked = Vec::new();
        loop {
            let held = if self.quota.hard {
                self.by_kind[k]
            } else {
                self.used
            };
            if held + meta.bytes <= ceiling {
                break;
            }
            let Some(c) = self.pick_class(k, &mut parked) else {
                self.unlink_parent(meta.parent);
                for (pk, r) in parked {
                    self.evictable[pk].push(Reverse(r));
                }
                self.refused[k] += 1;
                return Admission::Pending;
            };
            if let Some((vid, v)) = self.take_victim(c) {
                out.push((vid, v.meta));
            }
        }
        for (pk, r) in parked {
            self.evictable[pk].push(Reverse(r));
        }
        self.clock += 1;
        self.used += meta.bytes;
        self.by_kind[k] += meta.bytes;
        let priority = self.score(&meta, 1, 0.0);
        self.epoch += 1;
        self.entries.insert(
            id,
            Entry {
                meta,
                expect: 0.0,
                last_touch: self.clock,
                freq: 1,
                resident_children: 0,
                priority,
                epoch: self.epoch,
            },
        );
        self.reheap(id);
        Admission::Admitted
    }

    /// Empty the pool, handing back everything it held. Used when a domain is drained: the
    /// state is migrating, not being discarded, so callers must re-admit it somewhere.
    pub fn drain_all(&mut self) -> Vec<(BlobId, BlobMeta)> {
        let out: Vec<(BlobId, BlobMeta)> =
            self.entries.iter().map(|(id, e)| (*id, e.meta)).collect();
        self.entries.clear();
        self.evictable.iter_mut().for_each(BinaryHeap::clear);
        self.used = 0;
        self.by_kind = [0; BlobKind::N];
        out
    }

    pub fn remove(&mut self, id: &BlobId) -> Option<BlobMeta> {
        let e = self.entries.remove(id)?;
        self.used -= e.meta.bytes;
        self.by_kind[e.meta.kind.idx()] -= e.meta.bytes;
        self.unlink_parent(e.meta.parent);
        Some(e.meta)
    }
}

#[derive(Clone, Copy, Default, Debug)]
pub struct Cost {
    pub transfer_ns: u64,
    pub recompute_ns: u64,
    /// What it cost to *decide*, as distinct from what it cost to do. Zero when the
    /// scheduler and the ledger are the same process; a boundary crossing when they are not.
    pub decide_ns: u64,
    pub bytes_in: u64,
    pub pending: bool,
}

impl Cost {
    #[must_use]
    pub fn total_ns(&self) -> u64 {
        self.transfer_ns + self.recompute_ns + self.decide_ns
    }
}

#[derive(Debug)]
pub struct Hierarchy {
    pub dram: TierPool,
    pub nvme: TierPool,
    pub hits: [u64; BlobKind::N],
    pub nvme_hits: [u64; BlobKind::N],
    pub misses: [u64; BlobKind::N],
    pub prewarmed_bytes: u64,
    pub prewarm_ns: u64,
}

impl Hierarchy {
    #[must_use]
    pub fn new(dram: TierSpec, nvme: TierSpec, policy: Policy, quota: Quota) -> Self {
        Self {
            dram: TierPool::new(dram, policy, true, quota),
            nvme: TierPool::new(nvme, policy, false, Quota::open(nvme.capacity, quota.band)),
            hits: [0; BlobKind::N],
            nvme_hits: [0; BlobKind::N],
            misses: [0; BlobKind::N],
            prewarmed_bytes: 0,
            prewarm_ns: 0,
        }
    }

    fn admit_dram(&mut self, id: BlobId, meta: BlobMeta) -> Admission {
        let mut demoted = Vec::new();
        let a = self.dram.admit(id, meta, &mut demoted);
        for (did, dmeta) in demoted {
            let mut dropped = Vec::new();
            let _ = self.nvme.admit(did, dmeta, &mut dropped);
        }
        a
    }

    /// Value the downstream working set of a task before it is requested, and prewarm any of
    /// it that fits in free space. Prewarming never preempts: speculative work must not evict
    /// state someone is actually using.
    pub fn announce(&mut self, hint: &FlowHint) {
        for &(id, meta) in &hint.downstream {
            if self.dram.contains(&id) {
                self.dram.anticipate(id, hint.probability);
                continue;
            }
            if meta.bytes > self.dram.free_bytes() {
                break;
            }
            let staged = self.nvme.contains(&id);
            if self.admit_dram(id, meta) == Admission::Pending {
                break;
            }
            // Prewarming moves materialization off the critical path; it does not make it
            // free. Charged to a background budget so the two are never conflated.
            if staged {
                self.prewarm_ns += self.nvme.spec().fetch_ns(meta.bytes);
                self.nvme.remove(&id);
            } else {
                self.prewarm_ns += meta.recompute_ns;
            }
            self.dram.anticipate(id, hint.probability);
            self.prewarmed_bytes += meta.bytes;
        }
    }

    /// Could this task's remaining downstream state be made resident? Admitting an upstream
    /// stage whose downstream cannot land burns a warm cell on work that will stall.
    #[must_use]
    pub fn can_satisfy(&self, hint: &FlowHint) -> bool {
        let missing = hint.missing_bytes(|id| self.dram.contains(id));
        missing <= self.dram.reclaimable()
    }

    /// Admit migrated state without charging for it: the bytes already exist, they just live
    /// somewhere else now.
    pub fn reinstate(&mut self, id: BlobId, meta: BlobMeta) {
        let _ = self.admit_dram(id, meta);
    }

    /// Materialise an unordered dependency set. Unlike a chain these have no parent
    /// relation, so each is admitted independently and a refusal does not abort the rest.
    pub fn access_set(&mut self, blobs: &[(BlobId, BlobMeta)]) -> Cost {
        let mut cost = Cost::default();
        for &(id, meta) in blobs {
            let k = meta.kind.idx();
            if self.dram.contains(&id) {
                self.dram.touch(id);
                self.hits[k] += 1;
                continue;
            }
            let staged = self.nvme.contains(&id);
            if self.admit_dram(id, meta) == Admission::Pending {
                cost.pending = true;
                continue;
            }
            if staged {
                cost.transfer_ns += self.nvme.spec().fetch_ns(meta.bytes);
                self.nvme.remove(&id);
                self.nvme_hits[k] += 1;
            } else {
                cost.recompute_ns += meta.recompute_ns;
                self.misses[k] += 1;
            }
            cost.bytes_in += meta.bytes;
        }
        cost
    }

    pub fn access(&mut self, chain: &[(BlobId, BlobMeta)]) -> Cost {
        let mut cost = Cost::default();
        let hit = chain.partition_point(|(id, _)| self.dram.contains(id));
        if hit > 0 {
            let (id, meta) = chain[hit - 1];
            self.dram.touch(id);
            self.hits[meta.kind.idx()] += hit as u64;
        }
        for &(id, meta) in &chain[hit..] {
            let k = meta.kind.idx();
            let staged = self.nvme.contains(&id);
            if self.admit_dram(id, meta) == Admission::Pending {
                cost.pending = true;
                return cost;
            }
            if staged {
                cost.transfer_ns += self.nvme.spec().fetch_ns(meta.bytes);
                self.nvme.remove(&id);
                self.nvme_hits[k] += 1;
            } else {
                cost.recompute_ns += meta.recompute_ns;
                self.misses[k] += 1;
            }
            cost.bytes_in += meta.bytes;
        }
        cost
    }
}
