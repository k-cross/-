use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};

use crate::blob::{BlobId, BlobKind, BlobMeta};
use crate::flow::FlowHint;
use crate::tier::{Tier, TierSpec};

const FREQ_CAP: u32 = 16;

// A replica with live connections cannot be evicted at any price; only an idle one is a candidate.
const SERVING_WINDOW: u64 = 600;

const PINNED_SCAN_LIMIT: u32 = 8;

/// Evicted ids remembered for regret accounting. Bounded so a long run cannot grow it without
/// limit; an eviction whose ghost ages out can no longer register a regret, which biases the
/// measured rate low by at most the fraction of re-requests arriving after this many evictions.
const GHOST_CAP: usize = 1 << 16;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Policy {
    Gdsf,
    #[allow(dead_code, reason = "baseline policy retained for arm comparison")]
    Lru,
    /// Furthest-next-use, `phase-2.md` §1.7, §4.5: a diagnostic baseline, not a candidate for
    /// deployment. It needs the full reference stream ahead of time
    /// (`Hierarchy::set_clairvoyant_index`), so it exists to separate *eviction* quality from
    /// *routing* quality on a single ledger, never to be scored as an arm alongside `Gdsf`.
    Clairvoyant,
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
    ///
    /// These three are private so the per-class accessors are the only way in. `phase-1.md`
    /// §1.6's census can only see reads that go through them, and a `pub` array would make
    /// that a convention rather than an invariant.
    band: [u8; BlobKind::N],
    floor: [u64; BlobKind::N],
    /// Soft ceiling: `floor[k] + slack`. A class may grow past it into free space, but may not
    /// *preempt* a more-sacrificial band to get there, so it can never consume another
    /// workload's guaranteed floor.
    limit: [u64; BlobKind::N],
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

    /// The same quota applied to host memory beside an accelerator, where the accelerator's
    /// classes are only ever *offloaded* copies. Those give up host bytes first: DDR exists
    /// for host workloads, and an offload is a cache of state whose real home is elsewhere.
    #[must_use]
    pub fn offloaded(mut self) -> Self {
        let last = self.max_band();
        for k in BlobKind::ALL {
            if accelerated(k) {
                self.set_band_engine(k, last);
            }
        }
        self
    }

    /// The only place the orchestrator *writes* an engine-allocated class's eviction
    /// priority, rather than reading one. Needs no authority dispatch -- the caller has
    /// already filtered to `accelerated` -- but it is the strongest assumption in the
    /// quota layer and would be invisible to a census that only marked reads.
    #[cfg_attr(
        feature = "census",
        deprecated(note = "assumes allocation authority over engine state (band assignment)")
    )]
    fn set_band_engine(&mut self, kind: BlobKind, band: u8) {
        self.band[kind.idx()] = band;
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

    /// `phase-1.md` §1.6's census target: a per-class floor over an engine-allocated class
    /// stops being representable once Phase 3's engine cache is a class-blind LRU. Splits
    /// on `accelerated` rather than `authority` because `Quota` carries no `Tier` to ask
    /// with; `own::tests::accelerated_is_exactly_engine_allocation_authority` is what keeps
    /// the two answers the same.
    #[must_use]
    pub fn floor_of(&self, kind: BlobKind) -> u64 {
        if accelerated(kind) {
            self.floor_of_engine(kind)
        } else {
            self.floor_of_owned(kind)
        }
    }

    fn floor_of_owned(&self, kind: BlobKind) -> u64 {
        self.floor[kind.idx()]
    }

    #[cfg_attr(
        feature = "census",
        deprecated(
            note = "a per-class floor over an engine-allocated class is unrepresentable \
                     once Phase 3's engine cache has no floors"
        )
    )]
    fn floor_of_engine(&self, kind: BlobKind) -> u64 {
        self.floor[kind.idx()]
    }

    /// A class's soft ceiling. Same split and same reasoning as `floor_of`.
    #[must_use]
    pub fn limit_of(&self, kind: BlobKind) -> u64 {
        if accelerated(kind) {
            self.limit_of_engine(kind)
        } else {
            self.limit_of_owned(kind)
        }
    }

    fn limit_of_owned(&self, kind: BlobKind) -> u64 {
        self.limit[kind.idx()]
    }

    #[cfg_attr(
        feature = "census",
        deprecated(
            note = "a per-class limit over an engine-allocated class is unrepresentable \
                     once Phase 3's engine cache has no limits"
        )
    )]
    fn limit_of_engine(&self, kind: BlobKind) -> u64 {
        self.limit[kind.idx()]
    }

    /// A class's priority band. Same split and same reasoning as `floor_of`: a
    /// class-blind engine cache cannot honour a band either, since bands are what
    /// `pick_class` uses to decide *which* class gives way first.
    #[must_use]
    pub fn band_of(&self, kind: BlobKind) -> u8 {
        if accelerated(kind) {
            self.band_of_engine(kind)
        } else {
            self.band_of_owned(kind)
        }
    }

    fn band_of_owned(&self, kind: BlobKind) -> u8 {
        self.band[kind.idx()]
    }

    #[cfg_attr(
        feature = "census",
        deprecated(
            note = "a per-class band over an engine-allocated class is unrepresentable \
                     once Phase 3's engine cache has no bands"
        )
    )]
    fn band_of_engine(&self, kind: BlobKind) -> u8 {
        self.band[kind.idx()]
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
    /// Recently evicted ids, oldest first: ARC's ghost list, used here for pricing rather than
    /// for admission. A ghost that is requested again is an eviction that was regretted.
    ghosts: VecDeque<BlobId>,
    ghost_set: HashSet<BlobId>,
    pub regrets: [u64; BlobKind::N],
    /// What it costs to bring an evicted blob back from the tier it is demoted to, if there is
    /// one. Eviction from a pool with a tier beneath it is not a loss, it is a move.
    recovery: Option<TierSpec>,
    /// Expected loss per byte of the last blob actually evicted, in the same units as
    /// `marginal_price`. The fallback when nothing is currently reclaimable.
    last_price: f64,
    /// `phase-2.md` §1.8, §4.6: evictions where the class evicted differs from the class being
    /// admitted -- a cross-class trade a siloed, per-class quota could never make, since
    /// `Quota::hard`'s `pick_class` always returns the admitting class itself. Counted here,
    /// unconditionally and at zero cost to the eviction it observes: the comparison is a
    /// byproduct of `pick_class`'s own already-computed answer, not a second simulation.
    pub coupled: u64,
    pub coupled_decisions: u64,
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
            ghosts: VecDeque::new(),
            ghost_set: HashSet::new(),
            regrets: [0; BlobKind::N],
            recovery: None,
            last_price: 0.0,
            coupled: 0,
            coupled_decisions: 0,
        }
    }

    pub fn set_recovery(&mut self, spec: TierSpec) {
        self.recovery = Some(spec);
    }

    /// Per-byte cost of wanting an evicted blob back: rebuilding it, or recovering it from the
    /// tier below if that is cheaper.
    fn loss_per_byte(&self, meta: &BlobMeta) -> f64 {
        let rebuild = meta.value_per_byte();
        self.recovery.map_or(rebuild, |r| {
            (r.fetch_ns(meta.bytes) as f64 / meta.bytes as f64).min(rebuild)
        })
    }

    /// Fraction of this class's evictions that were later wanted back, measured.
    ///
    /// Smoothed with one phantom regret over one phantom eviction, so a pool that has evicted
    /// nothing prices displacement at the full recompute cost -- the conservative answer, and
    /// the one the ledger gave before it measured anything -- and converges on the observed
    /// rate as evidence accumulates.
    #[must_use]
    pub fn regret_rate(&self, k: usize) -> f64 {
        (self.regrets[k] + 1) as f64 / (self.evicted[k] + 1) as f64
    }

    fn remember_eviction(&mut self, id: BlobId) {
        if self.ghost_set.insert(id) {
            self.ghosts.push_back(id);
        }
        while self.ghosts.len() > GHOST_CAP {
            if let Some(old) = self.ghosts.pop_front() {
                self.ghost_set.remove(&old);
            }
        }
    }

    #[must_use]
    pub fn spec(&self) -> &TierSpec {
        &self.spec
    }

    pub fn resident_ids(&self) -> impl Iterator<Item = BlobId> + '_ {
        self.entries.keys().copied()
    }

    pub fn resident_ids_where<'a>(
        &'a self,
        keep: impl Fn(BlobKind) -> bool + 'a,
    ) -> impl Iterator<Item = BlobId> + 'a {
        self.entries
            .iter()
            .filter(move |(_, e)| keep(e.meta.kind))
            .map(|(id, _)| *id)
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

    /// `Policy::Clairvoyant`'s branch is a placeholder, immediately overwritten by
    /// `Hierarchy::clairvoyant_touch`'s `set_priority` the moment this entry's own reference
    /// is processed -- `score` has no reference-stream index to consult, only `Hierarchy`
    /// does, so it cannot compute the real furthest-next-use priority itself.
    fn score(&self, meta: &BlobMeta, freq: u32, expect: f64) -> f64 {
        match self.policy {
            Policy::Gdsf => {
                self.inflation + (f64::from(freq.min(FREQ_CAP)) + expect) * meta.value_per_byte()
            }
            Policy::Lru => self.clock as f64,
            Policy::Clairvoyant => 0.0,
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
            // A speculative bump is not a real reference, so it must not consume from the
            // reference-stream index -- see `Hierarchy::clairvoyant_touch`'s own doc comment.
            // The entry's real furthest-next-use priority is left exactly as it was.
            Policy::Clairvoyant => return,
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
            .map(|k| self.by_kind[k].saturating_sub(self.quota.floor_of(BlobKind::ALL[k])))
            .sum();
        self.free_bytes() + burst
    }

    /// *Expected* recompute cost per byte of the cheapest state this pool would give up,
    /// which is the price of putting something new here. Read-only, so it peeks each
    /// reclaimable class's heap top rather than draining it: a stale or pinned top makes that
    /// class abstain, and if every class abstains the price falls back to the last price
    /// actually paid -- not to `inflation`, which is a GDSF priority, carries a frequency
    /// factor, only ever rises, and so is in the wrong units. An estimate, deliberately -- a faithful dry run would cost as
    /// much as the eviction itself, on every candidate node, on every request.
    ///
    /// Expected, not worst-case. Evicted state only costs anything if it is wanted again, so
    /// each class's price is discounted by how often its evictions have actually been
    /// regretted. And what it costs then is not necessarily a rebuild: state evicted from a
    /// pool with a tier beneath it is demoted, not lost, and comes back at that tier's price.
    /// Accelerator memory is the sharpest case -- a weight shard pushed to host DDR returns
    /// over `PCIe` in tens of milliseconds, where a rebuild is seconds, and pricing it as the
    /// rebuild made every full accelerator look untouchable. Pricing every evicted byte as a certain rebuild made displacement two orders
    /// of magnitude louder than any cost paid with certainty *now* -- queueing, batch
    /// widening -- and a placement score in which one term cannot be outvoted is not weighing
    /// anything.
    #[must_use]
    pub fn marginal_price(&self) -> f64 {
        let mut best: Option<f64> = None;
        for b in (0..=self.quota.max_band()).rev() {
            for k in 0..BlobKind::N {
                if self.quota.band_of(BlobKind::ALL[k]) != b
                    || self.by_kind[k] <= self.quota.floor_of(BlobKind::ALL[k])
                {
                    continue;
                }
                let Some(Reverse(r)) = self.evictable[k].peek() else {
                    continue;
                };
                let Some(e) = self.entries.get(&r.id) else {
                    continue;
                };
                if e.epoch != r.epoch || self.is_serving(e) {
                    continue;
                }
                if self.leaf_first && e.resident_children > 0 {
                    continue;
                }
                let p = self.loss_per_byte(&e.meta) * self.regret_rate(k);
                if best.is_none_or(|bp| p < bp) {
                    best = Some(p);
                }
            }
            if best.is_some() {
                return best.unwrap_or(0.0);
            }
        }
        best.unwrap_or(self.last_price)
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

    /// Override one resident entry's priority directly, for a policy whose ranking is not a
    /// function of `score()`'s inputs -- `Policy::Clairvoyant`'s furthest-next-use, set from
    /// outside by `Hierarchy::clairvoyant_touch` once per real reference. A no-op if the
    /// entry is not resident here, which happens whenever the reference just landed in a
    /// different pool than the one holding the id's earlier occurrence.
    pub fn set_priority(&mut self, id: BlobId, priority: f64) {
        let Some(e) = self.entries.get_mut(&id) else {
            return;
        };
        e.priority = priority;
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
            if self.quota.band_of(BlobKind::ALL[k]) != band {
                continue;
            }
            if above_floor && self.by_kind[k] <= self.quota.floor_of(BlobKind::ALL[k]) {
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
        if self.by_kind[want] >= self.quota.limit_of(BlobKind::ALL[want]) {
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
        self.last_price = self.loss_per_byte(&e.meta) * self.regret_rate(k);
        self.evicted[k] += 1;
        self.remember_eviction(r.id);
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
        // Counted on the attempt, not the success: wanting evicted state back is the regret,
        // whether or not there is now room to readmit it.
        if self.ghost_set.remove(&id) {
            self.regrets[k] += 1;
        }
        let ceiling = if self.quota.hard {
            self.quota.floor_of(meta.kind).min(self.spec.capacity)
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
            // `phase-2.md` §4.6: `c != k` is exactly the trade `Quota::hard`'s own `pick_class`
            // branch can never make -- it always returns `k`, the admitting class itself. So
            // this is the unified arbiter's choice compared against the silo's only possible
            // choice, read off `pick_class`'s answer rather than computed a second time.
            self.coupled_decisions += 1;
            if c != k {
                self.coupled += 1;
            }
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

    /// Admit without recording a refusal. For state the ledger is moving down a tier on its
    /// own initiative: a demotion that does not fit is housekeeping, not a request turned away,
    /// and counting it would bill the refusal rate for the ledger's own eviction policy.
    pub fn offer(
        &mut self,
        id: BlobId,
        meta: BlobMeta,
        out: &mut Vec<(BlobId, BlobMeta)>,
    ) -> Admission {
        let a = self.admit(id, meta, out);
        if a == Admission::Pending {
            let k = meta.kind.idx();
            self.refused[k] = self.refused[k].saturating_sub(1);
        }
        a
    }

    /// Empty the pool, handing back everything it held. Used when a domain is drained: the
    /// state is migrating, not being discarded, so callers must re-admit it somewhere.
    pub fn drain_all(&mut self) -> Vec<(BlobId, BlobMeta)> {
        let out: Vec<(BlobId, BlobMeta)> =
            self.entries.iter().map(|(id, e)| (*id, e.meta)).collect();
        self.ghosts.clear();
        self.ghost_set.clear();
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
    /// Time waiting for a slot rather than for state. A residency policy moves this too:
    /// placing work on a saturated engine is a stall the ledger never sees.
    pub queue_ns: u64,
    /// The hop from wherever placement was settled to the engine that runs the work. Kept out
    /// of `transfer_ns` because that field answers "did the ledger have to move state", which
    /// is what classifies a warm invocation; a control hop the request pays either way would
    /// make every request look cold.
    pub dispatch_ns: u64,
    /// The work itself, once its state is resident: a function body, a decode loop, a request
    /// handler. Without this a warm invocation costs nothing at all and every overhead looks
    /// infinite beside it.
    pub exec_ns: u64,
    pub bytes_in: u64,
    pub pending: bool,
}

impl Cost {
    #[must_use]
    /// Time spent *waiting on state*, which is the only thing a residency policy can move.
    /// Execution is deliberately excluded: adding a fixed 200 ms decode to every arm would
    /// bury the differences under a constant.
    pub fn total_ns(&self) -> u64 {
        self.transfer_ns + self.recompute_ns + self.decide_ns + self.queue_ns + self.dispatch_ns
    }

    /// End-to-end time for the request. This is the denominator an overhead is a fraction of.
    #[must_use]
    pub fn service_ns(&self) -> u64 {
        self.total_ns() + self.exec_ns
    }
}

/// Capacity and policy of one node's memory. `hbm` of zero is unified memory: every class
/// lives in the one host pool, which is what the development machine has and what the
/// datacenter target does not.
#[derive(Clone, Copy, Debug)]
pub struct NodeMemory {
    pub hbm: u64,
    pub ddr: u64,
    pub nvme: u64,
    pub hbm_quota: Quota,
    pub ddr_quota: Quota,
    /// Whether this node has a serving engine at all. `hbm > 0` says a node *has* accelerator
    /// memory; this says whether it can *decode* -- the two usually agree, but a
    /// heterogeneous cluster can have a host-only node with real DDR and no engine, which
    /// `hbm == 0` alone cannot express (that also means "unified memory", where every node
    /// decodes). Defaults belong at the call site: every existing experiment sets this `true`.
    pub can_decode: bool,
}

/// Classes whose hot copy lives on the accelerator when there is one.
#[must_use]
pub fn accelerated(kind: BlobKind) -> bool {
    matches!(kind, BlobKind::KvBlock | BlobKind::WeightShard)
}

/// One node's memory: accelerator HBM, host DDR, and the spill tier under both.
///
/// In a datacenter node these are separate pools with separate budgets. KV blocks and weight
/// shards are usable only in HBM; function cells and service heaps live in DDR. The two meet
/// in exactly one place: state evicted from HBM is **offloaded** to DDR rather than dropped,
/// the way Dynamo's block manager, `LMCache` and host-side weight caches do, because promoting
/// it back over `PCIe` is far cheaper than rebuilding it. That makes offloaded KV and weights
/// a class of *host* state, competing with function cells and service heaps under DDR's
/// quota. It is the only coupling between the pools, and it is priced like every other.
///
/// With no HBM the node is unified memory, and every class competes in DDR directly.
#[derive(Debug)]
pub struct Hierarchy {
    pub hbm: TierPool,
    pub ddr: TierPool,
    pub nvme: TierPool,
    split: bool,
    can_decode: bool,
    link: TierSpec,
    pub hits: [u64; BlobKind::N],
    pub nvme_hits: [u64; BlobKind::N],
    /// Promoted from host DDR back to the accelerator.
    pub offload_hits: [u64; BlobKind::N],
    pub misses: [u64; BlobKind::N],
    /// Blobs materialised from a peer's memory rather than recomputed. Counted apart from
    /// `hits` because they were not free: they cost a link traversal, just less than a rebuild.
    pub remote_hits: [u64; BlobKind::N],
    pub prewarmed_bytes: u64,
    pub prewarm_ns: u64,
    pub engine_ops: EngineOps,
    /// `Policy::Clairvoyant`'s reference-stream index (`phase-2.md` §4.5): for each blob, its
    /// remaining occurrences' absolute trace positions, front-to-back. Empty unless
    /// `set_clairvoyant_index` was called, and every touch site below skips the work when it
    /// is, so a run that never installs one pays nothing for the check.
    clairvoyant: HashMap<BlobId, VecDeque<u64>>,
}

/// The *dynamic* census (`phase-1.md` §4.4). One counter per census-marked entry point,
/// because counting admissions alone would understate it: `touch_engine` runs on every
/// `KvBlock` hit and `demote_engine` is the only one that moves bytes, so an
/// admissions-only figure answers a narrower question than the one Phase 3 has to budget
/// for.
#[derive(Clone, Copy, Default, Debug)]
pub struct EngineOps {
    pub admit: [u64; BlobKind::N],
    pub touch: [u64; BlobKind::N],
    pub anticipate: [u64; BlobKind::N],
    pub demote: [u64; BlobKind::N],
    /// Colder copies dropped after an admission, from `announce` and `supply`.
    pub forget_cold: [u64; BlobKind::N],
    /// Colder copies dropped after a *promotion*, from `materialise`. Kept apart from
    /// `forget_cold` because it is the offload/spill hit path -- far the larger of the two,
    /// and the one that was invisible until `materialise` stopped removing inline.
    pub superseded: [u64; BlobKind::N],
    /// Evicted from DDR by *another* blob's demotion, not its own.
    pub spill: [u64; BlobKind::N],
    pub drain: [u64; BlobKind::N],
}

impl EngineOps {
    #[must_use]
    pub fn total(&self, kind: BlobKind) -> u64 {
        let k = kind.idx();
        self.admit[k]
            + self.touch[k]
            + self.anticipate[k]
            + self.demote[k]
            + self.forget_cold[k]
            + self.superseded[k]
            + self.spill[k]
            + self.drain[k]
    }
}

impl Hierarchy {
    #[must_use]
    pub fn new(mem: NodeMemory, policy: Policy) -> Self {
        let nvme = TierSpec::nvme(mem.nvme);
        let mut hbm = TierPool::new(TierSpec::hbm(mem.hbm), policy, true, mem.hbm_quota);
        let mut ddr = TierPool::new(TierSpec::dram(mem.ddr), policy, true, mem.ddr_quota);
        // Recovery is priced from the first tier an evictee lands in: host DDR under the
        // accelerator, the spill tier under the host.
        hbm.set_recovery(TierSpec::pcie());
        ddr.set_recovery(nvme);
        Self {
            hbm,
            ddr,
            nvme: TierPool::new(
                nvme,
                policy,
                false,
                Quota::open(mem.nvme, mem.ddr_quota.band),
            ),
            split: mem.hbm > 0,
            can_decode: mem.can_decode,
            link: TierSpec::pcie(),
            hits: [0; BlobKind::N],
            nvme_hits: [0; BlobKind::N],
            offload_hits: [0; BlobKind::N],
            misses: [0; BlobKind::N],
            remote_hits: [0; BlobKind::N],
            prewarmed_bytes: 0,
            prewarm_ns: 0,
            engine_ops: EngineOps::default(),
            clairvoyant: HashMap::new(),
        }
    }

    /// Install `Policy::Clairvoyant`'s reference-stream index: for each blob, the absolute
    /// trace position of every occurrence, in order. Built once from the full trace before a
    /// run starts (`arms.rs`), because the whole point of the arm is that it is *not* learned
    /// online. `phase-2.md` §4.5.
    pub fn set_clairvoyant_index(&mut self, index: HashMap<BlobId, VecDeque<u64>>) {
        self.clairvoyant = index;
    }

    /// Pop the occurrence that just happened off `id`'s schedule and, if `Policy::Clairvoyant`
    /// is running, push the resulting furthest-next-use priority into whichever pool now holds
    /// it -- `-inf` when nothing remains, so a blob referenced for the last time is evicted
    /// first. Called once per genuine reference (`access`, `access_set`, `supply`), never from
    /// `announce`'s speculative prewarm (`TierPool::anticipate`'s own `Clairvoyant` arm does
    /// not call this). A no-op, at the cost of one hash lookup, when no index was installed.
    fn clairvoyant_touch(&mut self, id: BlobId, kind: BlobKind) {
        if self.clairvoyant.is_empty() {
            return;
        }
        let Some(q) = self.clairvoyant.get_mut(&id) else {
            return;
        };
        q.pop_front();
        let priority = q.front().map_or(f64::NEG_INFINITY, |&pos| -(pos as f64));
        self.home_mut(kind).set_priority(id, priority);
    }

    #[must_use]
    pub fn split(&self) -> bool {
        self.split
    }

    /// Can this node run a decode step at all? `false` for a host-only node in a heterogeneous
    /// cluster: it can hold and serve `Snapshot`/`ServiceHeap` state, but `KvBlock` and
    /// `WeightShard` state can never be *usable* here, so nothing decode-bearing may be placed
    /// on it. Unlike `split`, this is never inferred from capacity -- a node can have DDR and
    /// still have no engine, which `hbm == 0` alone does not distinguish from unified memory.
    #[must_use]
    pub fn can_decode(&self) -> bool {
        self.can_decode
    }

    /// Which of the two hot pools a class's home lives in on this node. `Nvme` is never
    /// returned here: it is the spill tier reached by demotion, not a class's resting home,
    /// so `own.rs`'s `NVMe` rows are asked about by name rather than resolved through this.
    pub(crate) fn tier_of(&self, kind: BlobKind) -> Tier {
        if self.split && accelerated(kind) {
            Tier::Hbm
        } else {
            Tier::Ddr
        }
    }

    fn on_accelerator(&self, kind: BlobKind) -> bool {
        self.tier_of(kind) == Tier::Hbm
    }

    /// Takes a tier rather than a class, which is what lets a caller reach `Nvme` at all --
    /// `home` cannot, since no class rests there. `pub(crate)` for `tele.rs`.
    pub(crate) fn pool(&self, tier: Tier) -> &TierPool {
        match tier {
            Tier::Hbm => &self.hbm,
            Tier::Ddr => &self.ddr,
            Tier::Nvme => &self.nvme,
        }
    }

    fn pool_mut(&mut self, tier: Tier) -> &mut TierPool {
        match tier {
            Tier::Hbm => &mut self.hbm,
            Tier::Ddr => &mut self.ddr,
            Tier::Nvme => &mut self.nvme,
        }
    }

    /// The pool a class is usable from.
    #[must_use]
    pub fn home(&self, kind: BlobKind) -> &TierPool {
        self.pool(self.tier_of(kind))
    }

    fn home_mut(&mut self, kind: BlobKind) -> &mut TierPool {
        self.pool_mut(self.tier_of(kind))
    }

    /// `own::authority`, with this node's tier resolved so a caller does not have to. Only
    /// ever asks about `Hbm` or `Ddr`, since `tier_of` never resolves to `Nvme` -- an
    /// `Nvme` question is asked directly against `crate::own::authority`.
    #[must_use]
    pub fn authority(&self, kind: BlobKind, q: crate::own::Question) -> crate::own::Authority {
        crate::own::authority(kind, self.tier_of(kind), q)
    }

    /// Usable right now, with no copy.
    #[must_use]
    pub fn is_hot(&self, id: &BlobId, kind: BlobKind) -> bool {
        self.home(kind).contains(id)
    }

    /// Held in memory on this node at all, hot or offloaded. A peer can read either over
    /// RDMA, so either can be a source.
    #[must_use]
    pub fn holds(&self, id: &BlobId, kind: BlobKind) -> bool {
        self.home(kind).contains(id) || (self.on_accelerator(kind) && self.ddr.contains(id))
    }

    pub fn hot_ids(&self) -> impl Iterator<Item = BlobId> + '_ {
        let split = self.split;
        self.hbm.resident_ids().chain(
            self.ddr
                .resident_ids_where(move |k| !(split && accelerated(k))),
        )
    }

    /// What it costs this node to make one missing blob hot without leaving the node: promote
    /// it from host DDR, read it off the spill tier, or rebuild it.
    #[must_use]
    pub fn local_ns(&self, id: &BlobId, meta: &BlobMeta) -> u64 {
        let up = self.on_accelerator(meta.kind);
        if up && self.ddr.contains(id) {
            return self.link.fetch_ns(meta.bytes);
        }
        if self.nvme.contains(id) {
            let lift = if up {
                self.link.fetch_ns(meta.bytes)
            } else {
                0
            };
            return self.nvme.spec().fetch_ns(meta.bytes) + lift;
        }
        meta.recompute_ns
    }

    /// Bytes a set of per-class needs would claim in each pool.
    fn by_pool(&self, need: &[u64; BlobKind::N]) -> (u64, u64) {
        BlobKind::ALL.iter().fold((0, 0), |(h, d), &k| {
            if self.on_accelerator(k) {
                (h + need[k.idx()], d)
            } else {
                (h, d + need[k.idx()])
            }
        })
    }

    /// Would this node take these bytes, on top of `reserved`, without refusing? Read-only,
    /// so a fan-out can be checked across every node it needs before any is committed.
    #[must_use]
    pub fn could_admit(&self, need: &[u64; BlobKind::N], reserved: &[u64; BlobKind::N]) -> bool {
        let (nh, nd) = self.by_pool(need);
        let (rh, rd) = self.by_pool(reserved);
        nh + rh <= self.hbm.reclaimable() && nd + rd <= self.ddr.reclaimable()
    }

    /// Expected recompute this node's other work pays for making room, each pool at its own
    /// price. The pools are separate markets: evicting KV from HBM says nothing about what a
    /// byte of function cell is worth.
    #[must_use]
    pub fn displacement(&self, need: &[u64; BlobKind::N], reserved: &[u64; BlobKind::N]) -> f64 {
        let (nh, nd) = self.by_pool(need);
        let (rh, rd) = self.by_pool(reserved);
        let short = |pool: &TierPool, n: u64, r: u64| {
            n.saturating_sub(pool.free_bytes().saturating_sub(r)) as f64 * pool.marginal_price()
        };
        short(&self.hbm, nh, rh) + short(&self.ddr, nd, rd)
    }

    /// `displacement`, restricted to one pool -- `phase-2.md` §1.8's locality-coupling silo.
    /// An inference router does not know host DDR is under pressure, and a `FaaS` control
    /// plane does not know HBM is; this is what either would price on its own.
    #[must_use]
    pub fn displacement_in(
        &self,
        tier: Tier,
        need: &[u64; BlobKind::N],
        reserved: &[u64; BlobKind::N],
    ) -> f64 {
        let (nh, nd) = self.by_pool(need);
        let (rh, rd) = self.by_pool(reserved);
        let short = |pool: &TierPool, n: u64, r: u64| {
            n.saturating_sub(pool.free_bytes().saturating_sub(r)) as f64 * pool.marginal_price()
        };
        match tier {
            Tier::Hbm => short(&self.hbm, nh, rh),
            Tier::Ddr => short(&self.ddr, nd, rd),
            Tier::Nvme => 0.0,
        }
    }

    #[must_use]
    pub fn used(&self) -> u64 {
        self.hbm.used() + self.ddr.used()
    }

    #[must_use]
    pub fn resident_bytes(&self, kind: BlobKind) -> u64 {
        self.home(kind).resident_bytes(kind)
    }

    #[must_use]
    pub fn refused(&self) -> [u64; BlobKind::N] {
        std::array::from_fn(|k| self.hbm.refused[k] + self.ddr.refused[k])
    }

    /// Includes `nvme`, unlike `refused`. Eviction from HBM or DDR is a demotion -- the
    /// blob moves down a tier and can be promoted back -- while eviction from the spill
    /// tier is the only one that destroys state. A count that left it out would report
    /// every move and no loss, which is the opposite of what Phase 4 validates an
    /// engine-reported eviction metric against.
    #[must_use]
    pub fn evicted(&self) -> [u64; BlobKind::N] {
        std::array::from_fn(|k| self.hbm.evicted[k] + self.ddr.evicted[k] + self.nvme.evicted[k])
    }

    /// Spares a caller from knowing the tier: `TierPool::regret_rate` is keyed by index
    /// within one pool, and which pool that is depends on `split`.
    #[must_use]
    pub fn regret_rate(&self, kind: BlobKind) -> f64 {
        self.home(kind).regret_rate(kind.idx())
    }

    #[must_use]
    pub fn pinned_skips(&self) -> u64 {
        self.hbm.pinned_skips + self.ddr.pinned_skips
    }

    #[must_use]
    pub fn over_capacity(&self) -> bool {
        self.hbm.over_capacity() || self.ddr.over_capacity()
    }

    /// `phase-2.md` §1.8's memory-coupling axis: `(cross-class evictions, evictions)` in host
    /// DDR, the pool the axis is scoped to -- `TierPool::coupled`'s own doc comment says why
    /// HBM's copy of the same counters is not part of this question.
    #[must_use]
    pub fn ddr_memory_coupled(&self) -> (u64, u64) {
        (self.ddr.coupled, self.ddr.coupled_decisions)
    }

    fn spill(&mut self, id: BlobId, meta: BlobMeta) {
        let mut dropped = Vec::new();
        let _ = self.nvme.offer(id, meta, &mut dropped);
    }

    /// Where evicted state goes next. Off the accelerator it is offloaded to host DDR, and
    /// whatever *that* displaces falls to the spill tier; off the host it spills directly.
    /// Demotion is background work and is not charged to the request that caused it.
    ///
    /// Dispatches on the *evicted* blob's own authority, not on whatever admission
    /// triggered it: under unified memory every class shares one pool, so admitting a
    /// `KvBlock` can evict a `ServiceHeap`, and the census has to attribute the demotion
    /// to the victim rather than to whatever caused it (`phase-1.md` §4.4).
    fn demote(&mut self, id: BlobId, meta: BlobMeta) {
        match self.authority(meta.kind, crate::own::Question::Allocation) {
            crate::own::Authority::Engine => self.demote_engine(id, meta),
            crate::own::Authority::Orchestrator => self.demote_owned(id, meta),
        }
    }

    fn demote_owned(&mut self, id: BlobId, meta: BlobMeta) {
        self.demote_body(id, meta);
    }

    /// The allocation this code performs today on the engine's behalf: a `KvBlock` or
    /// `WeightShard` evicted from HBM is offloaded and, if that too is full, spilled --
    /// this ledger's own GDSF policy end to end, the same as an owned class's demotion.
    /// `owned-and-observed.md` §1's disclaimed authority, census-marked per
    /// `phase-1.md` §4.4: Phase 3 replaces this body with an engine cache model that
    /// demotes, if at all, by its own rules instead.
    #[cfg_attr(
        feature = "census",
        deprecated(note = "assumes allocation authority over engine state (demotion)")
    )]
    fn demote_engine(&mut self, id: BlobId, meta: BlobMeta) {
        self.engine_ops.demote[meta.kind.idx()] += 1;
        self.demote_body(id, meta);
    }

    /// The one demotion body both `demote_owned` and `demote_engine` run today -- kept as
    /// a single implementation so Phase 1 cannot drift the two behaviours apart by
    /// accident. Phase 3 is what gives `demote_engine` its own body.
    fn demote_body(&mut self, id: BlobId, meta: BlobMeta) {
        if !self.on_accelerator(meta.kind) {
            self.spill(id, meta);
            return;
        }
        let mut out = Vec::new();
        if self.ddr.offer(id, meta, &mut out) == Admission::Pending {
            self.spill(id, meta);
        }
        for (vid, vmeta) in out {
            self.spill_displaced(vid, vmeta);
        }
    }

    /// A blob evicted from DDR to make room for someone else's demotion. `demote`'s own
    /// rule -- attribute to the victim, not to whatever caused it -- applies here and was
    /// the one place inside `demote` not honouring it: a `KvBlock` pushed to `NVMe` by a
    /// `ServiceHeap` demotion is an engine-state decision the outer dispatch has already
    /// resolved to `Orchestrator`, so it can only be counted here.
    fn spill_displaced(&mut self, id: BlobId, meta: BlobMeta) {
        match self.authority(meta.kind, crate::own::Question::Allocation) {
            crate::own::Authority::Engine => self.spill_displaced_engine(id, meta),
            crate::own::Authority::Orchestrator => self.spill(id, meta),
        }
    }

    #[cfg_attr(
        feature = "census",
        deprecated(note = "assumes allocation authority over engine state (cascade spill)")
    )]
    fn spill_displaced_engine(&mut self, id: BlobId, meta: BlobMeta) {
        self.engine_ops.spill[meta.kind.idx()] += 1;
        self.spill(id, meta);
    }

    fn admit_hot(&mut self, id: BlobId, meta: BlobMeta) -> Admission {
        match self.authority(meta.kind, crate::own::Question::Allocation) {
            crate::own::Authority::Engine => self.admit_engine(id, meta),
            crate::own::Authority::Orchestrator => self.admit_owned(id, meta),
        }
    }

    /// Allocation the orchestrator itself decides -- `Snapshot`, `ServiceHeap` -- admitted
    /// outright into whichever pool `home_mut` resolves. Not census-marked: this
    /// authority is not disclaimed, so there is nothing here for Phase 3 to change.
    fn admit_owned(&mut self, id: BlobId, meta: BlobMeta) -> Admission {
        self.admit_hot_body(id, meta)
    }

    /// The allocation this code performs today on the engine's behalf -- `KvBlock` and
    /// `WeightShard` admission, GDSF-scored and evicted by this ledger rather than by
    /// vLLM's own block manager. `owned-and-observed.md` §1's disclaimed authority,
    /// census-marked per `phase-1.md` §4.4: Phase 3 replaces this body with an engine
    /// cache model.
    #[cfg_attr(
        feature = "census",
        deprecated(note = "assumes allocation authority over engine state (KvBlock/WeightShard)")
    )]
    fn admit_engine(&mut self, id: BlobId, meta: BlobMeta) -> Admission {
        self.engine_ops.admit[meta.kind.idx()] += 1;
        self.admit_hot_body(id, meta)
    }

    /// Shared so Phase 1 cannot drift the owned and engine paths apart by accident; Phase
    /// 3 is what gives `admit_engine` a body of its own.
    fn admit_hot_body(&mut self, id: BlobId, meta: BlobMeta) -> Admission {
        let mut out = Vec::new();
        let a = self.home_mut(meta.kind).admit(id, meta, &mut out);
        for (vid, vmeta) in out {
            self.demote(vid, vmeta);
        }
        a
    }

    /// Make one missing blob hot by the cheapest local route, charging `cost`.
    fn materialise(&mut self, id: BlobId, meta: BlobMeta, cost: &mut Cost) -> Admission {
        let k = meta.kind.idx();
        let up = self.on_accelerator(meta.kind);
        let offloaded = up && self.ddr.contains(&id);
        let staged = self.nvme.contains(&id);
        if self.admit_hot(id, meta) == Admission::Pending {
            return Admission::Pending;
        }
        if offloaded {
            cost.transfer_ns += self.link.fetch_ns(meta.bytes);
            self.drop_superseded(&id, meta.kind, Tier::Ddr);
            self.offload_hits[k] += 1;
        } else if staged {
            let lift = if up {
                self.link.fetch_ns(meta.bytes)
            } else {
                0
            };
            cost.transfer_ns += self.nvme.spec().fetch_ns(meta.bytes) + lift;
            self.drop_superseded(&id, meta.kind, Tier::Nvme);
            self.nvme_hits[k] += 1;
        } else {
            cost.recompute_ns += meta.recompute_ns;
            self.misses[k] += 1;
        }
        cost.bytes_in += meta.bytes;
        Admission::Admitted
    }

    /// The same removal `forget_cold` performs, reached from the other direction: that one
    /// runs after an admission, this one after a promotion has already superseded the
    /// copy. Separate entry point because it targets one named tier rather than every
    /// colder one, and because `materialise` is the hot path -- leaving it uncounted put
    /// every offload and spill hit outside the census, which is most of them.
    fn drop_superseded(&mut self, id: &BlobId, kind: BlobKind, tier: Tier) {
        match self.authority(kind, crate::own::Question::Allocation) {
            crate::own::Authority::Engine => self.drop_superseded_engine(id, kind, tier),
            crate::own::Authority::Orchestrator => self.drop_superseded_owned(id, tier),
        }
    }

    fn drop_superseded_owned(&mut self, id: &BlobId, tier: Tier) {
        self.pool_mut(tier).remove(id);
    }

    #[cfg_attr(
        feature = "census",
        deprecated(
            note = "assumes allocation authority over engine state (superseded-copy removal)"
        )
    )]
    fn drop_superseded_engine(&mut self, id: &BlobId, kind: BlobKind, tier: Tier) {
        self.engine_ops.superseded[kind.idx()] += 1;
        self.pool_mut(tier).remove(id);
    }

    /// Drop any colder copies of a blob that has just become hot, so a node never holds the
    /// same state twice. `phase-1.md` §1.5's "remove": splits the same way `admit_hot` and
    /// `demote` do.
    fn forget_cold(&mut self, id: &BlobId, kind: BlobKind) {
        match self.authority(kind, crate::own::Question::Allocation) {
            crate::own::Authority::Engine => self.forget_cold_engine(id, kind),
            crate::own::Authority::Orchestrator => self.forget_cold_owned(id, kind),
        }
    }

    fn forget_cold_owned(&mut self, id: &BlobId, kind: BlobKind) {
        self.forget_cold_body(id, kind);
    }

    /// Removing a `KvBlock`/`WeightShard`'s colder copy under this ledger's own
    /// bookkeeping -- allocation authority an engine-side connector (`LMCache`, NIXL)
    /// should hold instead. Census-marked per `phase-1.md` §4.4.
    #[cfg_attr(
        feature = "census",
        deprecated(note = "assumes allocation authority over engine state (colder-copy removal)")
    )]
    fn forget_cold_engine(&mut self, id: &BlobId, kind: BlobKind) {
        self.engine_ops.forget_cold[kind.idx()] += 1;
        self.forget_cold_body(id, kind);
    }

    fn forget_cold_body(&mut self, id: &BlobId, kind: BlobKind) {
        if self.on_accelerator(kind) {
            self.ddr.remove(id);
        }
        self.nvme.remove(id);
    }

    /// Raise a blob's value at its home pool because a flow says it is about to be
    /// needed. Dispatches on `kind`'s allocation authority the way `admit_hot` does.
    fn anticipate(&mut self, id: BlobId, kind: BlobKind, weight: f64) {
        match self.authority(kind, crate::own::Question::Allocation) {
            crate::own::Authority::Engine => self.anticipate_engine(id, kind, weight),
            crate::own::Authority::Orchestrator => self.anticipate_owned(id, kind, weight),
        }
    }

    fn anticipate_owned(&mut self, id: BlobId, kind: BlobKind, weight: f64) {
        self.home_mut(kind).anticipate(id, weight);
    }

    /// Prewarming a `KvBlock`/`WeightShard` by raising its priority in this ledger's own
    /// GDSF ranking -- a value judgement over engine-allocated state the engine's own
    /// prefetcher should be making instead. Census-marked per `phase-1.md` §4.4.
    #[cfg_attr(
        feature = "census",
        deprecated(note = "assumes allocation authority over engine state (prewarm priority)")
    )]
    fn anticipate_engine(&mut self, id: BlobId, kind: BlobKind, weight: f64) {
        self.engine_ops.anticipate[kind.idx()] += 1;
        self.home_mut(kind).anticipate(id, weight);
    }

    fn touch(&mut self, id: BlobId, kind: BlobKind) {
        match self.authority(kind, crate::own::Question::Allocation) {
            crate::own::Authority::Engine => self.touch_engine(id, kind),
            crate::own::Authority::Orchestrator => self.touch_owned(id, kind),
        }
    }

    fn touch_owned(&mut self, id: BlobId, kind: BlobKind) {
        self.home_mut(kind).touch(id);
    }

    /// Recording a `KvBlock`/`WeightShard` hit in this ledger's own recency/frequency
    /// bookkeeping -- allocation authority the engine's own block manager should hold.
    /// Census-marked per `phase-1.md` §4.4.
    #[cfg_attr(
        feature = "census",
        deprecated(note = "assumes allocation authority over engine state (hit accounting)")
    )]
    fn touch_engine(&mut self, id: BlobId, kind: BlobKind) {
        self.engine_ops.touch[kind.idx()] += 1;
        self.home_mut(kind).touch(id);
    }

    /// Value the downstream working set of a task before it is requested, and prewarm any of
    /// it that fits in free space. Prewarming never preempts: speculative work must not evict
    /// state someone is actually using.
    pub fn announce(&mut self, hint: &FlowHint) {
        for &(id, meta) in &hint.downstream {
            if self.is_hot(&id, meta.kind) {
                self.anticipate(id, meta.kind, hint.probability);
                continue;
            }
            if meta.bytes > self.home(meta.kind).free_bytes() {
                break;
            }
            let ns = self.local_ns(&id, &meta);
            if self.admit_hot(id, meta) == Admission::Pending {
                break;
            }
            self.forget_cold(&id, meta.kind);
            // Prewarming moves materialization off the critical path; it does not make it
            // free. Charged to a background budget so the two are never conflated.
            self.prewarm_ns += ns;
            self.anticipate(id, meta.kind, hint.probability);
            self.prewarmed_bytes += meta.bytes;
        }
    }

    /// Could this task's remaining downstream state be made resident? Admitting an upstream
    /// stage whose downstream cannot land burns a warm cell on work that will stall.
    #[must_use]
    pub fn can_satisfy(&self, hint: &FlowHint) -> bool {
        let mut need = [0u64; BlobKind::N];
        for (id, meta) in &hint.downstream {
            if !self.is_hot(id, meta.kind) {
                need[meta.kind.idx()] += meta.bytes;
            }
        }
        self.could_admit(&need, &[0; BlobKind::N])
    }

    /// Admit migrated state without charging for it: the bytes already exist, they just live
    /// somewhere else now.
    pub fn reinstate(&mut self, id: BlobId, meta: BlobMeta) {
        let _ = self.admit_hot(id, meta);
    }

    /// Empty the node's memory, handing back everything it held, hot or offloaded.
    ///
    /// Unlike the other entry points this one cannot dispatch on authority *before* acting
    /// -- it drains whole pools, and a pool holds whatever mix of classes the node was
    /// running. So it drains first and attributes afterwards, one census hit per
    /// engine-owned blob it took. This is the largest single assumption of allocation
    /// authority in the simulator: `Machine::retire` relocates an entire engine's KV cache
    /// by orchestrator fiat, which no engine interface in §8 would permit.
    pub fn drain_all(&mut self) -> Vec<(BlobId, BlobMeta)> {
        let mut out = self.hbm.drain_all();
        out.extend(self.ddr.drain_all());
        for &(_, meta) in &out {
            if self.authority(meta.kind, crate::own::Question::Allocation)
                == crate::own::Authority::Engine
            {
                self.drain_engine(meta.kind);
            }
        }
        out
    }

    #[cfg_attr(
        feature = "census",
        deprecated(note = "assumes allocation authority over engine state (bulk drain)")
    )]
    fn drain_engine(&mut self, kind: BlobKind) {
        self.engine_ops.drain[kind.idx()] += 1;
    }

    /// Install state that arrived over a link. The caller has already paid for the traversal,
    /// so the bytes land without a recompute charge -- that is the entire point of fetching
    /// rather than rebuilding. Stops at the first refusal, leaving the rest to be recomputed
    /// by `access`, which is the correct fallback: a node that cannot hold the state cannot
    /// be helped by shipping it.
    pub fn supply(&mut self, chain: &[(BlobId, BlobMeta)]) -> usize {
        for (n, &(id, meta)) in chain.iter().enumerate() {
            if self.is_hot(&id, meta.kind) {
                self.touch(id, meta.kind);
                self.clairvoyant_touch(id, meta.kind);
                continue;
            }
            if self.admit_hot(id, meta) == Admission::Pending {
                return n;
            }
            self.forget_cold(&id, meta.kind);
            self.remote_hits[meta.kind.idx()] += 1;
            self.clairvoyant_touch(id, meta.kind);
        }
        chain.len()
    }

    /// Materialise an unordered dependency set. Unlike a chain these have no parent
    /// relation, so each is admitted independently and a refusal does not abort the rest.
    pub fn access_set(&mut self, blobs: &[(BlobId, BlobMeta)]) -> Cost {
        let mut cost = Cost::default();
        for &(id, meta) in blobs {
            if self.is_hot(&id, meta.kind) {
                self.touch(id, meta.kind);
                self.hits[meta.kind.idx()] += 1;
                self.clairvoyant_touch(id, meta.kind);
                continue;
            }
            if self.materialise(id, meta, &mut cost) == Admission::Pending {
                cost.pending = true;
            } else {
                self.clairvoyant_touch(id, meta.kind);
            }
        }
        cost
    }

    pub fn access(&mut self, chain: &[(BlobId, BlobMeta)]) -> Cost {
        let mut cost = Cost::default();
        let hit = chain.partition_point(|(id, m)| self.is_hot(id, m.kind));
        if hit > 0 {
            let (id, meta) = chain[hit - 1];
            self.touch(id, meta.kind);
            self.hits[meta.kind.idx()] += hit as u64;
        }
        // The clairvoyant schedule advances for every blob this request actually touched,
        // not only the one `touch()` bumped: it was built from every blob in the chain, and
        // leaving the rest of the hit prefix unadvanced would leave their next-use position
        // pointing at an occurrence already in the past.
        for &(id, meta) in &chain[..hit] {
            self.clairvoyant_touch(id, meta.kind);
        }
        for &(id, meta) in &chain[hit..] {
            if self.materialise(id, meta, &mut cost) == Admission::Pending {
                cost.pending = true;
                return cost;
            }
            self.clairvoyant_touch(id, meta.kind);
        }
        cost
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `phase-2.md` §1.7, §4.5: with only two resident entries and a third arriving, the one
    /// with no scheduled future use must go before the one that does, regardless of recency or
    /// frequency -- the property `Policy::Gdsf`/`Policy::Lru` cannot express by construction.
    #[test]
    fn clairvoyant_evicts_the_entry_with_the_furthest_next_use() {
        let bands = [0u8; BlobKind::N];
        let mem = NodeMemory {
            hbm: 0,
            ddr: 200,
            nvme: 0,
            hbm_quota: Quota::open(0, bands),
            ddr_quota: Quota::open(200, bands),
            can_decode: true,
        };
        let mut h = Hierarchy::new(mem, Policy::Clairvoyant);
        // Snapshot, not ServiceHeap: a ServiceHeap entry is pinned while "serving" (unevictable
        // at any price for `SERVING_WINDOW` clock ticks), which this test's few admissions
        // never age out of -- an unrelated mechanism this test must not exercise by accident.
        let meta = || BlobMeta {
            kind: BlobKind::Snapshot,
            bytes: 100,
            parent: None,
            recompute_ns: 1_000,
        };
        let (a, b, c) = (BlobId::leaf(b"a"), BlobId::leaf(b"b"), BlobId::leaf(b"c"));

        // a is referenced again at op 5; b never is; c is referenced once more, later than
        // either. Filling the pool with a and b and then admitting c must evict b -- the one
        // with no future in its own schedule -- and never a, which still has one.
        let mut index = HashMap::new();
        index.insert(a, VecDeque::from([0u64, 5]));
        index.insert(b, VecDeque::from([1u64]));
        index.insert(c, VecDeque::from([2u64, 9]));
        h.set_clairvoyant_index(index);

        assert!(!h.access(&[(a, meta())]).pending);
        assert!(!h.access(&[(b, meta())]).pending);
        assert!(h.is_hot(&a, BlobKind::Snapshot));
        assert!(h.is_hot(&b, BlobKind::Snapshot));

        assert!(!h.access(&[(c, meta())]).pending);
        assert!(
            h.is_hot(&a, BlobKind::Snapshot),
            "a has a scheduled future use and must survive"
        );
        assert!(
            !h.is_hot(&b, BlobKind::Snapshot),
            "b has no future use and must be evicted first"
        );
        assert!(h.is_hot(&c, BlobKind::Snapshot));
    }

    /// `phase-2.md` §1.8, §4.6: admitting a class that must evict a *different* class is
    /// exactly the trade a per-class quota cannot make, so a soft-quota pool with no floors
    /// records it as coupled and a hard-quota pool -- whose `pick_class` never leaves its own
    /// class -- never does, on the same sequence of admissions.
    #[test]
    fn cross_class_eviction_is_coupled_under_soft_quota_and_never_under_hard() {
        let bands = [0u8; BlobKind::N];
        let capacity = 3_000u64;
        let snapshot = |tag: &[u8], bytes: u64| {
            (
                BlobId::leaf(tag),
                BlobMeta {
                    kind: BlobKind::Snapshot,
                    bytes,
                    parent: None,
                    recompute_ns: 1_000,
                },
            )
        };
        let service = |tag: &[u8], bytes: u64| {
            (
                BlobId::leaf(tag),
                BlobMeta {
                    kind: BlobKind::ServiceHeap,
                    bytes,
                    parent: None,
                    recompute_ns: 1_000,
                },
            )
        };

        let mut soft = TierPool::new(
            TierSpec::dram(capacity),
            Policy::Gdsf,
            false,
            Quota::open(capacity, bands),
        );
        let mut out = Vec::new();
        let (id_a, meta_a) = snapshot(b"soft-a", 2_000);
        assert_eq!(soft.admit(id_a, meta_a, &mut out), Admission::Admitted);
        // The pool now holds 2,000 of its 3,000 bytes as Snapshot. Admitting 2,000 bytes of
        // ServiceHeap cannot fit beside it, so the only way to make room is to evict the
        // Snapshot entry -- a cross-class trade a per-class floor would have refused instead.
        let (id_b, meta_b) = service(b"soft-b", 2_000);
        out.clear();
        assert_eq!(soft.admit(id_b, meta_b, &mut out), Admission::Admitted);
        assert_eq!(soft.coupled_decisions, 1);
        assert_eq!(
            soft.coupled, 1,
            "the only evictable byte here is a different class"
        );

        // Floors of 1,500 each, so a 1,000-byte blob is admitted freely but a second one of
        // the same class (2,000 > 1,500) must evict -- from its own class only, since a hard
        // quota's `pick_class` never leaves the class it was asked about.
        let hard = Quota::from_split(capacity, [0.0, 0.5, 0.0, 0.5], bands, true);
        let mut pool = TierPool::new(TierSpec::dram(capacity), Policy::Gdsf, false, hard);
        let mut out = Vec::new();
        for i in 0..3 {
            let (id, meta) = snapshot(format!("hard-s{i}").as_bytes(), 1_000);
            let _ = pool.admit(id, meta, &mut out);
        }
        for i in 0..3 {
            let (id, meta) = service(format!("hard-h{i}").as_bytes(), 1_000);
            let _ = pool.admit(id, meta, &mut out);
        }
        assert!(
            pool.coupled_decisions > 0,
            "the fixture must actually exercise eviction on both sides of the floor"
        );
        assert_eq!(
            pool.coupled, 0,
            "a hard quota's pick_class never leaves the admitting class"
        );
    }

    fn hierarchy(split: bool) -> Hierarchy {
        let bands = [0u8; BlobKind::N];
        let hbm = if split { 4 << 30 } else { 0 };
        let mem = NodeMemory {
            hbm,
            ddr: 8 << 30,
            nvme: 64 << 30,
            hbm_quota: Quota::open(hbm, bands),
            ddr_quota: Quota::open(8 << 30, bands),
            can_decode: true,
        };
        Hierarchy::new(mem, Policy::Gdsf)
    }

    /// `phase-1.md` §5: `tier_of` must agree with the pre-refactor `on_accelerator` body
    /// (`self.split && accelerated(kind)`) on every input. `on_accelerator` now calls
    /// `tier_of` directly, so this pins the *semantics* against an independent
    /// restatement rather than the two functions trivially agreeing by construction.
    #[test]
    fn tier_of_agrees_with_accelerated_and_split_on_every_input() {
        for split in [false, true] {
            let h = hierarchy(split);
            for kind in BlobKind::ALL {
                let expect_hbm = split && accelerated(kind);
                let got = h.tier_of(kind);
                assert_eq!(
                    got == Tier::Hbm,
                    expect_hbm,
                    "kind={kind:?} split={split}: tier_of={got:?}"
                );
                // Nvme is never a class's resting home -- only Hbm or Ddr.
                assert_ne!(got, Tier::Nvme);
            }
        }
    }

    #[test]
    fn hierarchy_authority_resolves_the_same_tier_as_home() {
        for split in [false, true] {
            let h = hierarchy(split);
            for kind in BlobKind::ALL {
                let tier = h.tier_of(kind);
                let direct = crate::own::authority(kind, tier, crate::own::Question::Allocation);
                let via_hierarchy = h.authority(kind, crate::own::Question::Allocation);
                assert_eq!(direct, via_hierarchy, "kind={kind:?} split={split}");
            }
        }
    }
}
