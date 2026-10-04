use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};

use crate::blob::{BlobId, BlobKind, BlobMeta};
use crate::engine::{EngineCache, Placed};
use crate::flow::FlowHint;
use crate::stream::{KvEvent, Mark, Medium};
use crate::tier::{Tier, TierSpec};

const FREQ_CAP: u32 = 16;

const SERVING_WINDOW: u64 = 600;

const PINNED_SCAN_LIMIT: u32 = 8;

const GHOST_CAP: usize = 1 << 16;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Policy {
    Gdsf,
    #[allow(dead_code)]
    Lru,

    Clairvoyant,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Admission {
    Admitted,
    Pending,
}

#[derive(Clone, Copy, Debug)]
pub struct Quota {
    band: [u8; BlobKind::N],
    floor: [u64; BlobKind::N],

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

    expect: f64,
    retain_until: u64,
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

    ghosts: VecDeque<BlobId>,
    ghost_set: HashSet<BlobId>,
    pub regrets: [u64; BlobKind::N],

    recovery: Option<TierSpec>,

    last_price: f64,

    retirements: BinaryHeap<Reverse<(u64, BlobId)>>,

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
            retirements: BinaryHeap::new(),
            coupled: 0,
            coupled_decisions: 0,
        }
    }

    pub fn set_recovery(&mut self, spec: TierSpec) {
        self.recovery = Some(spec);
    }

    fn loss_per_byte(&self, meta: &BlobMeta) -> f64 {
        let rebuild = meta.value_per_byte();
        self.recovery.map_or(rebuild, |r| {
            (r.fetch_ns(meta.bytes) as f64 / meta.bytes as f64).min(rebuild)
        })
    }

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

    fn score(&self, meta: &BlobMeta, freq: u32, expect: f64) -> f64 {
        match self.policy {
            Policy::Gdsf => {
                self.inflation + (f64::from(freq.min(FREQ_CAP)) + expect) * meta.value_per_byte()
            }
            Policy::Lru => self.clock as f64,
            Policy::Clairvoyant => f64::NEG_INFINITY,
        }
    }

    pub fn anticipate(&mut self, id: BlobId, weight: f64) {
        let (inflation, policy, clock) = (self.inflation, self.policy, self.clock);
        let Some(e) = self.entries.get_mut(&id) else {
            return;
        };

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

            Policy::Clairvoyant => return,
        };
        self.reheap(id);
    }

    pub fn retain_until(&mut self, id: BlobId, until: u64, enforced: bool) {
        let Some(e) = self.entries.get_mut(&id) else {
            return;
        };
        if e.expect <= 0.0 {
            return;
        }
        e.retain_until = until;
        if enforced {
            self.retirements.push(Reverse((until, id)));
        }
    }

    pub fn retire(&mut self, now: u64) {
        while let Some(&Reverse((until, id))) = self.retirements.peek() {
            if until > now {
                break;
            }
            self.retirements.pop();
            let Some(e) = self.entries.get_mut(&id) else {
                continue;
            };
            if e.retain_until != until || e.expect <= 0.0 || self.policy != Policy::Gdsf {
                continue;
            }
            e.priority -= e.expect * e.meta.value_per_byte();
            e.expect = 0.0;
            self.reheap(id);
        }
    }

    pub fn evict_first(&mut self, id: BlobId) {
        let inflation = self.inflation;
        let Some(e) = self.entries.get_mut(&id) else {
            return;
        };
        e.priority = inflation;
        self.reheap(id);
    }

    #[must_use]
    pub fn stale_bumps(&self, now: u64) -> usize {
        self.entries
            .values()
            .filter(|e| e.expect > 0.0 && e.retain_until > 0 && e.retain_until <= now)
            .count()
    }

    #[must_use]
    pub fn free_bytes(&self) -> u64 {
        self.spec.capacity.saturating_sub(self.used)
    }

    #[must_use]
    pub fn reclaimable(&self) -> u64 {
        let burst: u64 = (0..BlobKind::N)
            .filter(|&k| BlobKind::ALL[k] != BlobKind::ServiceHeap)
            .map(|k| self.by_kind[k].saturating_sub(self.quota.floor_of(BlobKind::ALL[k])))
            .sum();
        self.free_bytes() + burst
    }

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

    pub(crate) fn set_priority(&mut self, id: BlobId, priority: f64) {
        if self.policy != Policy::Clairvoyant {
            return;
        }
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

    fn pick_class(&mut self, want: usize, parked: &mut Vec<(usize, Ranked)>) -> Option<usize> {
        if self.quota.hard {
            return self.clean_top(want, parked).map(|_| want);
        }

        if self.by_kind[want] >= self.quota.limit_of(BlobKind::ALL[want]) {
            return self.clean_top(want, parked).map(|_| want);
        }

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
        self.admit_body(id, meta, out, true)
    }

    fn admit_body(
        &mut self,
        id: BlobId,
        meta: BlobMeta,
        out: &mut Vec<(BlobId, BlobMeta)>,
        arbitrated: bool,
    ) -> Admission {
        if self.entries.contains_key(&id) {
            self.touch(id);
            return Admission::Admitted;
        }
        let k = meta.kind.idx();

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

            if arbitrated {
                self.coupled_decisions += 1;
                if c != k {
                    self.coupled += 1;
                }
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
                retain_until: 0,
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

    pub fn offer(
        &mut self,
        id: BlobId,
        meta: BlobMeta,
        out: &mut Vec<(BlobId, BlobMeta)>,
    ) -> Admission {
        let a = self.admit_body(id, meta, out, false);
        if a == Admission::Pending {
            let k = meta.kind.idx();
            self.refused[k] = self.refused[k].saturating_sub(1);
        }
        a
    }

    pub fn drain_all(&mut self) -> Vec<(BlobId, BlobMeta)> {
        let mut out: Vec<(BlobId, BlobMeta)> =
            self.entries.iter().map(|(id, e)| (*id, e.meta)).collect();

        out.sort_unstable_by_key(|(id, _)| *id);
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

    pub decide_ns: u64,

    pub queue_ns: u64,

    pub dispatch_ns: u64,

    pub exec_ns: u64,
    pub bytes_in: u64,
    pub pending: bool,
    pub preempted: bool,
}

impl Cost {
    #[must_use]
    pub fn total_ns(&self) -> u64 {
        self.transfer_ns + self.recompute_ns + self.decide_ns + self.queue_ns + self.dispatch_ns
    }

    #[must_use]
    pub fn service_ns(&self) -> u64 {
        self.total_ns() + self.exec_ns
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct KvFit {
    pub need: u64,
    pub free: u64,
    pub capacity: u64,
}

impl KvFit {
    #[must_use]
    pub fn fits(self) -> bool {
        self.need <= self.free
    }

    #[must_use]
    pub fn never(self) -> bool {
        self.need > self.capacity
    }
}

#[derive(Clone, Copy, Debug)]
pub struct NodeMemory {
    pub hbm: u64,
    pub ddr: u64,
    pub nvme: u64,
    pub hbm_quota: Quota,
    pub ddr_quota: Quota,

    pub can_decode: bool,
    pub kv: Option<EngineKv>,
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub struct EngineKv {
    pub partition: u64,
    pub offload: u64,
    pub spill: u64,
    pub clairvoyant: bool,
}

#[derive(Debug)]
struct KvTiers {
    gpu: EngineCache,
    offload: EngineCache,
    spill: EngineCache,
    events: Option<Vec<KvEvent>>,
}

fn record(
    events: &mut Option<Vec<KvEvent>>,
    medium: Medium,
    id: BlobId,
    meta: BlobMeta,
    was_present: bool,
    placed: &Placed,
) {
    let Some(log) = events else {
        return;
    };
    for &(victim, _) in &placed.evicted {
        log.push(KvEvent::Removed { id: victim, medium });
    }
    if placed.resident && !was_present {
        log.push(KvEvent::Stored {
            id,
            bytes: meta.bytes,
            medium,
            mark: None,
        });
    }
}

impl KvTiers {
    fn offload(&mut self, id: BlobId, meta: BlobMeta) {
        let present = self.events.is_some() && self.offload.contains(&id);
        let placed = self.offload.admit(id, meta, false);
        record(&mut self.events, Medium::Cpu, id, meta, present, &placed);
        let mut down = placed.evicted;
        if !placed.resident {
            down.push((id, meta));
        }
        for (vid, vmeta) in down {
            let present = self.events.is_some() && self.spill.contains(&vid);
            let landed = self.spill.admit(vid, vmeta, false);
            record(
                &mut self.events,
                Medium::Storage,
                vid,
                vmeta,
                present,
                &landed,
            );
        }
    }

    fn place(&mut self, id: BlobId, meta: BlobMeta) -> bool {
        let present = self.events.is_some() && self.gpu.contains(&id);
        let placed = self.gpu.admit(id, meta, true);
        record(&mut self.events, Medium::Gpu, id, meta, present, &placed);
        for (vid, vmeta) in placed.evicted {
            self.offload(vid, vmeta);
        }
        placed.resident
    }

    fn mark(&mut self, id: BlobId, mark: Mark) -> bool {
        let Some(applied) = self.gpu.mark(id, mark) else {
            return false;
        };
        if let (Some(log), Some(meta)) = (self.events.as_mut(), self.gpu.meta_of(&id)) {
            log.push(KvEvent::Stored {
                id,
                bytes: meta.bytes,
                medium: Medium::Gpu,
                mark: Some(applied),
            });
        }
        true
    }

    fn forget_cold(&mut self, id: &BlobId) {
        let removed_offload = self.offload.remove(id).is_some();
        let removed_spill = self.spill.remove(id).is_some();
        if let Some(log) = self.events.as_mut() {
            if removed_offload {
                log.push(KvEvent::Removed {
                    id: *id,
                    medium: Medium::Cpu,
                });
            }
            if removed_spill {
                log.push(KvEvent::Removed {
                    id: *id,
                    medium: Medium::Storage,
                });
            }
        }
    }

    fn drain(&mut self) {
        self.gpu.drain();
        self.offload.drain();
        self.spill.drain();
        if let Some(log) = self.events.as_mut() {
            log.push(KvEvent::Cleared);
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum Half {
    #[default]
    Both,
    Retain,
    Prewarm,
    Off,
}

impl Half {
    #[must_use]
    pub fn retains(self) -> bool {
        matches!(self, Self::Both | Self::Retain)
    }

    #[must_use]
    pub fn prewarms(self) -> bool {
        matches!(self, Self::Both | Self::Prewarm)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct AnnounceMix {
    pub kv: Half,
    pub host: Half,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct Ahead {
    pub prefill: bool,
    pub hold: bool,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct DirectiveStats {
    pub applied: u64,
    pub expired: u64,
    pub pressure_evictions: u64,
    pub marked_blocks: u64,
    pub marked_bytes: u64,
}

#[must_use]
pub fn accelerated(kind: BlobKind) -> bool {
    matches!(kind, BlobKind::KvBlock | BlobKind::WeightShard)
}

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

    pub offload_hits: [u64; BlobKind::N],
    pub misses: [u64; BlobKind::N],

    pub remote_hits: [u64; BlobKind::N],
    pub prewarmed_bytes: u64,
    pub prewarm_ns: u64,
    pub engine_ops: EngineOps,

    clairvoyant: HashMap<BlobId, VecDeque<u64>>,
    clairvoyant_op: u64,
    policy: Policy,
    kv: Option<KvTiers>,
    announce: AnnounceMix,
    ahead: Ahead,
    deadline: bool,
    now: u64,
    hbm_total: u64,
    weights: Option<u64>,
    pub prefilled_blocks: u64,
    pub held_blocks: u64,
    seq_hits: u64,
}

#[derive(Clone, Copy, Default, Debug)]
pub struct EngineOps {
    pub admit: [u64; BlobKind::N],
    pub touch: [u64; BlobKind::N],
    pub anticipate: [u64; BlobKind::N],
    pub demote: [u64; BlobKind::N],

    pub forget_cold: [u64; BlobKind::N],

    pub superseded: [u64; BlobKind::N],

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
        let split = mem.hbm > 0;
        let grant = mem.kv.unwrap_or(EngineKv {
            partition: 0,
            offload: 0,
            spill: 0,
            clairvoyant: false,
        });
        assert!(
            split || grant.offload == 0,
            "unified memory has no offload tier beneath its partition"
        );
        let carve = |pool: u64, part: u64, what: &str| {
            pool.checked_sub(part)
                .unwrap_or_else(|| panic!("KV {what} of {part} B exceeds its {pool} B pool"))
        };
        let (hbm_cap, ddr_cap) = if split {
            (
                carve(mem.hbm, grant.partition, "partition"),
                carve(mem.ddr, grant.offload, "offload"),
            )
        } else {
            (0, carve(mem.ddr, grant.partition, "partition"))
        };
        let nvme_cap = carve(mem.nvme, grant.spill, "spill");
        let nvme = TierSpec::nvme(nvme_cap);
        let mut hbm = TierPool::new(TierSpec::hbm(hbm_cap), policy, true, mem.hbm_quota);
        let mut ddr = TierPool::new(TierSpec::dram(ddr_cap), policy, true, mem.ddr_quota);

        hbm.set_recovery(TierSpec::pcie());
        ddr.set_recovery(nvme);
        let kv = mem.kv.map(|k| {
            let recovery = if split { TierSpec::pcie() } else { nvme };
            let gpu = EngineCache::new(k.partition, true).with_recovery(recovery);
            KvTiers {
                gpu: if k.clairvoyant {
                    gpu.with_clairvoyance()
                } else {
                    gpu
                },
                offload: EngineCache::new(k.offload, false),
                spill: EngineCache::new(k.spill, false),
                events: None,
            }
        });
        Self {
            hbm,
            ddr,
            nvme: TierPool::new(
                nvme,
                policy,
                false,
                Quota::open(nvme_cap, mem.ddr_quota.band),
            ),
            split,
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
            clairvoyant_op: 0,
            policy,
            kv,
            announce: AnnounceMix::default(),
            ahead: Ahead::default(),
            deadline: false,
            now: 0,
            hbm_total: mem.hbm,
            weights: None,
            prefilled_blocks: 0,
            held_blocks: 0,
            seq_hits: 0,
        }
    }

    pub fn set_announce(&mut self, mix: AnnounceMix) {
        self.announce = mix;
    }

    pub fn set_ahead(&mut self, ahead: Ahead) {
        self.ahead = ahead;
    }

    pub fn set_deadline(&mut self, on: bool) {
        self.deadline = on;
    }

    pub fn tick(&mut self, now: u64) {
        self.now = now;
        self.expire_marks(now);
        if self.deadline {
            for tier in [Tier::Hbm, Tier::Ddr, Tier::Nvme] {
                self.pool_mut(tier).retire(now);
            }
        }
    }

    #[must_use]
    pub fn stale_bumps(&self) -> usize {
        self.hbm.stale_bumps(self.now)
            + self.ddr.stale_bumps(self.now)
            + self.nvme.stale_bumps(self.now)
    }

    pub fn set_clairvoyant_index(&mut self, index: HashMap<BlobId, VecDeque<u64>>) {
        self.clairvoyant = index;
    }

    pub fn set_clairvoyant_op(&mut self, op: u64) {
        self.clairvoyant_op = op;
    }

    fn clairvoyant_touch(&mut self, id: BlobId, kind: BlobKind) {
        if self.clairvoyant.is_empty() {
            return;
        }
        let op = self.clairvoyant_op;
        let Some(q) = self.clairvoyant.get_mut(&id) else {
            return;
        };
        while q.front().is_some_and(|&pos| pos <= op) {
            q.pop_front();
        }
        let next = q.front().copied();
        if let Some(kv) = self.kv.as_mut().filter(|_| kind == BlobKind::KvBlock) {
            kv.gpu.reprice(id, next);
            return;
        }
        if self.policy != Policy::Clairvoyant {
            return;
        }
        let priority = next.map_or(f64::NEG_INFINITY, |pos| -(pos as f64));
        match self.authority(kind, crate::own::Question::Allocation) {
            crate::own::Authority::Engine => self.reprice_engine(id, kind, priority),
            crate::own::Authority::Orchestrator => self.reprice_owned(id, priority),
        }
    }

    fn reprice_owned(&mut self, id: BlobId, priority: f64) {
        self.reprice_body(id, priority);
    }

    #[cfg_attr(
        feature = "census",
        deprecated(
            note = "assumes allocation authority over engine state (clairvoyant repricing)"
        )
    )]
    fn reprice_engine(&mut self, id: BlobId, kind: BlobKind, priority: f64) {
        self.engine_ops.touch[kind.idx()] += 1;
        self.reprice_body(id, priority);
    }

    fn reprice_body(&mut self, id: BlobId, priority: f64) {
        for tier in [Tier::Hbm, Tier::Ddr, Tier::Nvme] {
            self.pool_mut(tier).set_priority(id, priority);
        }
    }

    #[must_use]
    pub fn split(&self) -> bool {
        self.split
    }

    #[must_use]
    pub fn can_decode(&self) -> bool {
        self.can_decode
    }

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

    #[must_use]
    pub fn lift_ns(&self, kind: BlobKind, bytes: u64) -> u64 {
        if self.on_accelerator(kind) {
            self.link.fetch_ns(bytes)
        } else {
            0
        }
    }

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

    #[must_use]
    pub fn home(&self, kind: BlobKind) -> &TierPool {
        self.pool(self.tier_of(kind))
    }

    fn home_mut(&mut self, kind: BlobKind) -> &mut TierPool {
        self.pool_mut(self.tier_of(kind))
    }

    #[must_use]
    pub fn authority(&self, kind: BlobKind, q: crate::own::Question) -> crate::own::Authority {
        crate::own::authority_in(kind, self.tier_of(kind), q, self.weights.is_some())
    }

    fn assert_fits(&self, partition: u64, weights_bytes: u64) {
        assert!(
            partition + weights_bytes <= self.hbm_total,
            "a partition of {partition} B and weights of {weights_bytes} B exceed {} B of HBM",
            self.hbm_total
        );
    }

    pub fn bind_weights(&mut self, weights_bytes: u64) {
        let partition = self.kv.as_ref().map_or(0, |kv| kv.gpu.capacity());
        self.assert_fits(partition, weights_bytes);
        self.weights = Some(weights_bytes);
    }

    pub fn reload(&mut self, weights_bytes: u64, partition: u64) -> u64 {
        self.assert_fits(partition, weights_bytes);
        let lost = self
            .kv
            .as_ref()
            .map_or(0, |kv| kv.gpu.used() + kv.offload.used() + kv.spill.used());
        if let Some(kv) = self.kv.as_mut() {
            kv.drain();
            kv.gpu.resize(partition);
        }
        self.weights = Some(weights_bytes);
        lost
    }

    #[must_use]
    pub fn weights_bytes(&self) -> Option<u64> {
        self.weights
    }

    #[must_use]
    pub fn hbm_bytes(&self) -> u64 {
        self.hbm_total
    }

    fn engine_kv(&self, kind: BlobKind) -> Option<&KvTiers> {
        self.kv.as_ref().filter(|_| kind == BlobKind::KvBlock)
    }

    #[must_use]
    pub fn engine_cache(&self) -> bool {
        self.kv.is_some()
    }

    #[must_use]
    pub fn is_hot(&self, id: &BlobId, kind: BlobKind) -> bool {
        if let Some(kv) = self.engine_kv(kind) {
            return kv.gpu.contains(id);
        }
        self.home(kind).contains(id)
    }

    #[must_use]
    pub fn holds(&self, id: &BlobId, kind: BlobKind) -> bool {
        if let Some(kv) = self.engine_kv(kind) {
            return kv.gpu.contains(id) || kv.offload.contains(id);
        }
        self.home(kind).contains(id) || (self.on_accelerator(kind) && self.ddr.contains(id))
    }

    pub fn hot_ids(&self) -> impl Iterator<Item = BlobId> + '_ {
        let split = self.split;
        self.hbm
            .resident_ids()
            .chain(
                self.ddr
                    .resident_ids_where(move |k| !(split && accelerated(k))),
            )
            .chain(self.kv.iter().flat_map(|kv| kv.gpu.ids()))
    }

    #[must_use]
    pub fn local_ns(&self, id: &BlobId, meta: &BlobMeta) -> u64 {
        let up = self.on_accelerator(meta.kind);
        if let Some(kv) = self.engine_kv(meta.kind) {
            if kv.offload.contains(id) {
                return self.link.fetch_ns(meta.bytes);
            }
            if kv.spill.contains(id) {
                let lift = if up {
                    self.link.fetch_ns(meta.bytes)
                } else {
                    0
                };
                return self.nvme.spec().fetch_ns(meta.bytes) + lift;
            }
            return meta.recompute_ns;
        }
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

    fn by_pool(&self, need: &[u64; BlobKind::N]) -> (u64, u64) {
        BlobKind::ALL
            .iter()
            .filter(|&&k| self.engine_kv(k).is_none())
            .fold((0, 0), |(h, d), &k| {
                if self.on_accelerator(k) {
                    (h + need[k.idx()], d)
                } else {
                    (h, d + need[k.idx()])
                }
            })
    }

    #[must_use]
    pub fn could_admit(&self, need: &[u64; BlobKind::N], reserved: &[u64; BlobKind::N]) -> bool {
        let (nh, nd) = self.by_pool(need);
        let (rh, rd) = self.by_pool(reserved);
        nh + rh <= self.hbm.reclaimable() && nd + rd <= self.ddr.reclaimable()
    }

    #[must_use]
    pub fn displacement(&self, need: &[u64; BlobKind::N], reserved: &[u64; BlobKind::N]) -> f64 {
        self.displacement_seen(need, reserved, self.kv_view())
    }

    #[must_use]
    pub fn displacement_seen(
        &self,
        need: &[u64; BlobKind::N],
        reserved: &[u64; BlobKind::N],
        kv: Option<(u64, f64)>,
    ) -> f64 {
        let (nh, nd) = self.by_pool(need);
        let (rh, rd) = self.by_pool(reserved);
        let short = |pool: &TierPool, n: u64, r: u64| {
            n.saturating_sub(pool.free_bytes().saturating_sub(r)) as f64 * pool.marginal_price()
        };
        short(&self.hbm, nh, rh) + short(&self.ddr, nd, rd) + Self::kv_shortfall(need, reserved, kv)
    }

    fn kv_view(&self) -> Option<(u64, f64)> {
        self.kv
            .as_ref()
            .map(|kv| (kv.gpu.free(), kv.gpu.tail_price()))
    }

    fn kv_shortfall(
        need: &[u64; BlobKind::N],
        reserved: &[u64; BlobKind::N],
        kv: Option<(u64, f64)>,
    ) -> f64 {
        let Some((free, price)) = kv else {
            return 0.0;
        };
        let k = BlobKind::KvBlock.idx();
        need[k].saturating_sub(free.saturating_sub(reserved[k])) as f64 * price
    }

    #[must_use]
    pub fn displacement_in(
        &self,
        tier: Tier,
        need: &[u64; BlobKind::N],
        reserved: &[u64; BlobKind::N],
    ) -> f64 {
        self.displacement_in_seen(tier, need, reserved, self.kv_view())
    }

    #[must_use]
    pub fn displacement_in_seen(
        &self,
        tier: Tier,
        need: &[u64; BlobKind::N],
        reserved: &[u64; BlobKind::N],
        kv: Option<(u64, f64)>,
    ) -> f64 {
        let (nh, nd) = self.by_pool(need);
        let (rh, rd) = self.by_pool(reserved);
        let short = |pool: &TierPool, n: u64, r: u64| {
            n.saturating_sub(pool.free_bytes().saturating_sub(r)) as f64 * pool.marginal_price()
        };
        let kv_here = self.kv.is_some() && self.tier_of(BlobKind::KvBlock) == tier;
        let kv = if kv_here {
            Self::kv_shortfall(need, reserved, kv)
        } else {
            0.0
        };
        match tier {
            Tier::Hbm => short(&self.hbm, nh, rh) + kv,
            Tier::Ddr => short(&self.ddr, nd, rd) + kv,
            Tier::Nvme => 0.0,
        }
    }

    #[must_use]
    pub fn kv_acquire_ns(&self, believed: Option<Medium>, meta: &BlobMeta) -> u64 {
        match believed {
            Some(Medium::Cpu) => self.link.fetch_ns(meta.bytes),
            Some(Medium::Storage) => {
                let lift = if self.on_accelerator(meta.kind) {
                    self.link.fetch_ns(meta.bytes)
                } else {
                    0
                };
                self.nvme.spec().fetch_ns(meta.bytes) + lift
            }
            _ => meta.recompute_ns,
        }
    }

    #[must_use]
    pub fn kv_unit_price(&self, meta: &BlobMeta) -> f64 {
        self.kv.as_ref().map_or(0.0, |kv| kv.gpu.price_of(meta))
    }

    #[must_use]
    pub fn used(&self) -> u64 {
        let pools = self.hbm.used() + self.ddr.used();
        match &self.kv {
            Some(kv) => pools + kv.gpu.used() + kv.offload.used(),
            None => pools,
        }
    }

    #[must_use]
    pub fn resident_bytes(&self, kind: BlobKind) -> u64 {
        if let Some(kv) = self.engine_kv(kind) {
            return kv.gpu.used();
        }
        self.home(kind).resident_bytes(kind)
    }

    #[must_use]
    pub fn kv_bytes(&self) -> [u64; 3] {
        let k = BlobKind::KvBlock;
        if let Some(kv) = &self.kv {
            return [kv.gpu.used(), kv.offload.used(), kv.spill.used()];
        }
        let offloaded = if self.split {
            self.ddr.resident_bytes(k)
        } else {
            0
        };
        [
            self.home(k).resident_bytes(k),
            offloaded,
            self.nvme.resident_bytes(k),
        ]
    }

    pub fn kv_gpu_ids(&self) -> impl Iterator<Item = BlobId> + '_ {
        self.kv.iter().flat_map(|kv| kv.gpu.ids())
    }

    #[cfg(test)]
    pub fn evict_unrecorded(&mut self, id: &BlobId) -> bool {
        self.kv
            .as_mut()
            .is_some_and(|kv| kv.gpu.remove(id).is_some())
    }

    pub fn record_kv_events(&mut self, on: bool) {
        if let Some(kv) = self.kv.as_mut() {
            kv.events = on.then(Vec::new);
        }
    }

    pub fn take_kv_events(&mut self) -> Vec<KvEvent> {
        self.kv
            .as_mut()
            .and_then(|kv| kv.events.as_mut())
            .map(std::mem::take)
            .unwrap_or_default()
    }

    #[must_use]
    pub fn kv_ids(&self, medium: Medium) -> Vec<BlobId> {
        let Some(kv) = &self.kv else {
            return Vec::new();
        };
        let cache = match medium {
            Medium::Gpu => &kv.gpu,
            Medium::Cpu => &kv.offload,
            Medium::Storage => &kv.spill,
        };
        let mut ids: Vec<BlobId> = cache.ids().collect();
        ids.sort_unstable();
        ids
    }

    #[must_use]
    pub fn kv_partition(&self) -> Option<(u64, u64)> {
        self.kv
            .as_ref()
            .map(|kv| (kv.gpu.capacity(), kv.gpu.pinned()))
    }

    #[must_use]
    pub fn kv_tail_price(&self) -> Option<f64> {
        self.kv.as_ref().map(|kv| kv.gpu.tail_price())
    }

    #[must_use]
    pub fn preemptions(&self) -> u64 {
        self.kv.as_ref().map_or(0, |kv| kv.gpu.preemptions)
    }

    #[must_use]
    pub fn kv_orphans(&self) -> usize {
        self.kv.as_ref().map_or(0, |kv| kv.gpu.orphans())
    }

    pub fn set_owner(&mut self, owner: Option<u32>) {
        if let Some(kv) = self.kv.as_mut() {
            kv.gpu.set_owner(owner);
        }
    }

    pub fn set_tenant_floor(&mut self, bytes: u64) {
        if let Some(kv) = self.kv.as_mut() {
            kv.gpu.set_tenant_floor(bytes);
        }
    }

    #[must_use]
    pub fn kv_owned_by(&self, owner: u32) -> u64 {
        self.kv.as_ref().map_or(0, |kv| kv.gpu.owned_by(owner))
    }

    #[must_use]
    pub fn kv_fit(&self, blocks: &[(BlobId, BlobMeta)]) -> Option<KvFit> {
        let kv = self.kv.as_ref()?;
        let mut seen = HashSet::new();
        let need = blocks
            .iter()
            .filter(|(id, m)| m.kind == BlobKind::KvBlock && seen.insert(*id))
            .filter(|(id, _)| !kv.gpu.is_pinned(id))
            .map(|(_, m)| m.bytes)
            .sum();
        Some(KvFit {
            need,
            free: kv.gpu.capacity().saturating_sub(kv.gpu.pinned()),
            capacity: kv.gpu.capacity(),
        })
    }

    #[must_use]
    pub fn kv_wait_ns(&self, blocks: &[(BlobId, BlobMeta)], now_ns: u64) -> Option<u64> {
        let kv = self.kv.as_ref()?;
        let fit = self.kv_fit(blocks)?;
        if fit.fits() {
            return Some(0);
        }
        if fit.never() {
            return None;
        }
        let mine: HashSet<BlobId> = blocks.iter().map(|(id, _)| *id).collect();
        let mut pins: HashMap<BlobId, u32> = HashMap::new();
        let mut free = fit.free;
        for (end, ids) in kv.gpu.release_schedule() {
            for id in ids {
                let left = pins.entry(id).or_insert_with(|| kv.gpu.pins_of(&id));
                *left = left.saturating_sub(1);
                if *left == 0 && !mine.contains(&id) {
                    free += kv.gpu.meta_of(&id).map_or(0, |m| m.bytes);
                }
            }
            if fit.need <= free {
                return Some(end.saturating_sub(now_ns));
            }
        }
        None
    }

    pub fn seal(&mut self, until: Option<u64>) -> Option<u64> {
        self.kv.as_mut().and_then(|kv| kv.gpu.seal(until))
    }

    pub fn abort_seq(&mut self, seq: u64) -> u64 {
        self.kv.as_mut().map_or(0, |kv| kv.gpu.abort(seq))
    }

    pub fn kv_drop_unpinned(&mut self, ids: &[BlobId]) -> usize {
        let Some(kv) = self.kv.as_mut() else {
            return 0;
        };
        let mut dropped = 0;
        for id in ids.iter().rev() {
            if !kv.gpu.is_pinned(id) && kv.gpu.remove(id).is_some() {
                if let Some(log) = kv.events.as_mut() {
                    log.push(KvEvent::Removed {
                        id: *id,
                        medium: Medium::Gpu,
                    });
                }
                dropped += 1;
            }
        }
        dropped
    }

    pub fn release(&mut self, now_ns: u64) {
        if let Some(kv) = self.kv.as_mut() {
            kv.gpu.release(now_ns);
        }
        self.tick(now_ns);
    }

    pub fn expire_marks(&mut self, now: u64) {
        if let Some(kv) = self.kv.as_mut() {
            kv.gpu.expire(now);
        }
    }

    pub fn mark_kv(&mut self, id: BlobId, mark: Mark) -> bool {
        self.kv.as_mut().is_some_and(|kv| kv.mark(id, mark))
    }

    #[must_use]
    pub fn kv_live_marks(&self, now: u64) -> Vec<(BlobId, Mark)> {
        self.kv
            .as_ref()
            .map_or_else(Vec::new, |kv| kv.gpu.live_marks(now))
    }

    #[must_use]
    pub fn directive_stats(&self) -> DirectiveStats {
        self.kv.as_ref().map_or_else(DirectiveStats::default, |kv| {
            let (marked_blocks, marked_bytes) = kv.gpu.marked();
            DirectiveStats {
                applied: kv.gpu.marks_applied,
                expired: kv.gpu.marks_expired,
                pressure_evictions: kv.gpu.pressure_evictions,
                marked_blocks,
                marked_bytes,
            }
        })
    }

    #[must_use]
    pub fn kv_tier_of(&self, id: &BlobId) -> Option<Medium> {
        let kv = self.kv.as_ref()?;
        if kv.gpu.contains(id) {
            Some(Medium::Gpu)
        } else if kv.offload.contains(id) {
            Some(Medium::Cpu)
        } else if kv.spill.contains(id) {
            Some(Medium::Storage)
        } else {
            None
        }
    }

    pub fn prefill_block(&mut self, id: BlobId, meta: BlobMeta) -> Option<u64> {
        let ns = self.local_ns(&id, &meta);
        let kv = self.kv.as_mut()?;
        if kv.gpu.contains(&id) {
            return Some(0);
        }
        if meta.parent.is_some_and(|p| !kv.gpu.contains(&p)) {
            return None;
        }
        if !kv.place(id, meta) {
            return None;
        }
        kv.forget_cold(&id);
        self.prefilled_blocks += 1;
        self.clairvoyant_touch(id, meta.kind);
        Some(ns)
    }

    #[must_use]
    pub fn refused(&self) -> [u64; BlobKind::N] {
        std::array::from_fn(|k| self.hbm.refused[k] + self.ddr.refused[k])
    }

    #[must_use]
    pub fn evicted(&self) -> [u64; BlobKind::N] {
        let mut out: [u64; BlobKind::N] = std::array::from_fn(|k| {
            self.hbm.evicted[k] + self.ddr.evicted[k] + self.nvme.evicted[k]
        });
        if let Some(kv) = &self.kv {
            out[BlobKind::KvBlock.idx()] +=
                kv.gpu.evictions + kv.offload.evictions + kv.spill.evictions;
        }
        out
    }

    #[must_use]
    pub fn regret_rate(&self, kind: BlobKind) -> f64 {
        if self.engine_kv(kind).is_some() {
            return 1.0;
        }
        self.home(kind).regret_rate(kind.idx())
    }

    #[must_use]
    pub fn pinned_skips(&self) -> u64 {
        self.hbm.pinned_skips + self.ddr.pinned_skips
    }

    #[must_use]
    pub fn over_capacity(&self) -> bool {
        let kv = self
            .kv
            .as_ref()
            .is_some_and(|kv| kv.gpu.used() > kv.gpu.capacity());
        self.hbm.over_capacity() || self.ddr.over_capacity() || kv
    }

    #[must_use]
    pub fn ddr_memory_coupled(&self) -> (u64, u64) {
        (self.ddr.coupled, self.ddr.coupled_decisions)
    }

    fn spill(&mut self, id: BlobId, meta: BlobMeta) {
        let mut dropped = Vec::new();
        let _ = self.nvme.offer(id, meta, &mut dropped);
    }

    fn demote(&mut self, id: BlobId, meta: BlobMeta) {
        match self.authority(meta.kind, crate::own::Question::Allocation) {
            crate::own::Authority::Engine => self.demote_engine(id, meta),
            crate::own::Authority::Orchestrator => self.demote_owned(id, meta),
        }
    }

    fn demote_owned(&mut self, id: BlobId, meta: BlobMeta) {
        self.demote_body(id, meta);
    }

    #[cfg_attr(
        feature = "census",
        deprecated(note = "assumes allocation authority over engine state (demotion)")
    )]
    fn demote_engine(&mut self, id: BlobId, meta: BlobMeta) {
        self.engine_ops.demote[meta.kind.idx()] += 1;
        self.demote_body(id, meta);
    }

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
        debug_assert!(
            self.engine_kv(meta.kind).is_none(),
            "engine-allocated KV reached the ledger's admission path"
        );
        match self.authority(meta.kind, crate::own::Question::Allocation) {
            crate::own::Authority::Engine => self.admit_engine(id, meta),
            crate::own::Authority::Orchestrator => self.admit_owned(id, meta),
        }
    }

    fn admit_owned(&mut self, id: BlobId, meta: BlobMeta) -> Admission {
        self.admit_hot_body(id, meta)
    }

    #[cfg_attr(
        feature = "census",
        deprecated(note = "assumes allocation authority over engine state (KvBlock/WeightShard)")
    )]
    fn admit_engine(&mut self, id: BlobId, meta: BlobMeta) -> Admission {
        self.engine_ops.admit[meta.kind.idx()] += 1;
        self.admit_hot_body(id, meta)
    }

    fn admit_hot_body(&mut self, id: BlobId, meta: BlobMeta) -> Admission {
        let mut out = Vec::new();
        let a = self.home_mut(meta.kind).admit(id, meta, &mut out);
        for (vid, vmeta) in out {
            self.demote(vid, vmeta);
        }
        a
    }

    fn materialise_kv(&mut self, id: BlobId, meta: BlobMeta, cost: &mut Cost) -> bool {
        let k = meta.kind.idx();
        let up = self.split;
        let lift = self.link.fetch_ns(meta.bytes);
        let spill_read = self.nvme.spec().fetch_ns(meta.bytes);
        let Some(kv) = self.kv.as_mut() else {
            return false;
        };
        let offloaded = kv.offload.contains(&id);
        let staged = kv.spill.contains(&id);
        if !kv.place(id, meta) {
            return false;
        }
        kv.forget_cold(&id);
        if offloaded {
            cost.transfer_ns += lift;
            self.offload_hits[k] += 1;
        } else if staged {
            cost.transfer_ns += spill_read + if up { lift } else { 0 };
            self.nvme_hits[k] += 1;
        } else {
            cost.recompute_ns += meta.recompute_ns;
            self.misses[k] += 1;
        }
        cost.bytes_in += meta.bytes;
        true
    }

    fn preempt(&mut self, chain: &[(BlobId, BlobMeta)], from: usize, cost: &mut Cost) {
        let whole: u64 = chain.iter().map(|(_, m)| m.recompute_ns).sum();
        cost.recompute_ns = cost.recompute_ns.max(whole);
        cost.preempted = true;
        if let Some((_, m)) = chain.first() {
            let k = m.kind.idx();
            self.hits[k] = self.hits[k].saturating_sub(self.seq_hits);
            self.misses[k] += self.seq_hits + (chain.len() - from) as u64;
        }
        self.seq_hits = 0;
        self.seal(None);
    }

    pub fn produce(&mut self, blocks: &[(BlobId, BlobMeta)]) -> bool {
        for &(id, meta) in blocks {
            if self.engine_kv(meta.kind).is_some() {
                let placed = self.kv.as_mut().is_some_and(|kv| kv.place(id, meta));
                if !placed {
                    self.seal(None);
                    return false;
                }
            } else if self.admit_hot(id, meta) == Admission::Pending {
                let pool = self.home_mut(meta.kind);
                pool.refused[meta.kind.idx()] = pool.refused[meta.kind.idx()].saturating_sub(1);
                return false;
            }
            self.clairvoyant_touch(id, meta.kind);
        }
        true
    }

    pub fn decode_output(
        &mut self,
        chain: &[(BlobId, BlobMeta)],
        produces: &[(BlobId, BlobMeta)],
        chain_recompute: u64,
        cost: &mut Cost,
    ) {
        if cost.preempted || produces.is_empty() || self.produce(produces) {
            return;
        }
        let whole: u64 = chain.iter().map(|(_, m)| m.recompute_ns).sum();
        cost.recompute_ns += whole.saturating_sub(chain_recompute);
        cost.preempted = true;
        if let Some((_, m)) = chain.first() {
            let k = m.kind.idx();
            self.hits[k] = self.hits[k].saturating_sub(self.seq_hits);
            self.misses[k] += self.seq_hits;
        }
        self.seq_hits = 0;
    }

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

    fn forget_cold(&mut self, id: &BlobId, kind: BlobKind) {
        if let Some(kv) = self.kv.as_mut().filter(|_| kind == BlobKind::KvBlock) {
            kv.forget_cold(id);
            return;
        }
        match self.authority(kind, crate::own::Question::Allocation) {
            crate::own::Authority::Engine => self.forget_cold_engine(id, kind),
            crate::own::Authority::Orchestrator => self.forget_cold_owned(id, kind),
        }
    }

    fn forget_cold_owned(&mut self, id: &BlobId, kind: BlobKind) {
        self.forget_cold_body(id, kind);
    }

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

    fn anticipate(&mut self, id: BlobId, kind: BlobKind, weight: f64) {
        if self.engine_kv(kind).is_some() {
            return;
        }
        match self.authority(kind, crate::own::Question::Allocation) {
            crate::own::Authority::Engine => self.anticipate_engine(id, kind, weight),
            crate::own::Authority::Orchestrator => self.anticipate_owned(id, kind, weight),
        }
    }

    fn anticipate_owned(&mut self, id: BlobId, kind: BlobKind, weight: f64) {
        self.home_mut(kind).anticipate(id, weight);
    }

    #[cfg_attr(
        feature = "census",
        deprecated(note = "assumes allocation authority over engine state (prewarm priority)")
    )]
    fn anticipate_engine(&mut self, id: BlobId, kind: BlobKind, weight: f64) {
        self.engine_ops.anticipate[kind.idx()] += 1;
        self.home_mut(kind).anticipate(id, weight);
    }

    fn touch(&mut self, id: BlobId, kind: BlobKind) {
        if let Some(kv) = self.kv.as_mut().filter(|_| kind == BlobKind::KvBlock) {
            kv.gpu.touch(id, true);
            return;
        }
        match self.authority(kind, crate::own::Question::Allocation) {
            crate::own::Authority::Engine => self.touch_engine(id, kind),
            crate::own::Authority::Orchestrator => self.touch_owned(id, kind),
        }
    }

    fn touch_owned(&mut self, id: BlobId, kind: BlobKind) {
        self.home_mut(kind).touch(id);
    }

    #[cfg_attr(
        feature = "census",
        deprecated(note = "assumes allocation authority over engine state (hit accounting)")
    )]
    fn touch_engine(&mut self, id: BlobId, kind: BlobKind) {
        self.engine_ops.touch[kind.idx()] += 1;
        self.home_mut(kind).touch(id);
    }

    pub fn announce(&mut self, hint: &FlowHint) {
        let until = self.now + u64::from(hint.lead_ops) + 1;
        let deadline = self.deadline;
        let mut prefilling = self.ahead.prefill;
        let mut prefilled = false;
        for &(id, meta) in &hint.downstream {
            if self.engine_kv(meta.kind).is_some() {
                if self.is_hot(&id, meta.kind) {
                    if self.ahead.hold {
                        self.held_blocks += u64::from(self.mark_kv(
                            id,
                            Mark {
                                rank: crate::stream::Rank::Retain,
                                until_ns: until,
                            },
                        ));
                    }
                } else if prefilling {
                    match self.prefill_block(id, meta) {
                        Some(ns) => {
                            self.prewarm_ns += ns;
                            self.prewarmed_bytes += meta.bytes;
                            prefilled = true;
                            if self.ahead.hold {
                                self.held_blocks += u64::from(self.mark_kv(
                                    id,
                                    Mark {
                                        rank: crate::stream::Rank::Retain,
                                        until_ns: until,
                                    },
                                ));
                            }
                        }
                        None => prefilling = false,
                    }
                }
                continue;
            }
            let half = if meta.kind == BlobKind::KvBlock {
                self.announce.kv
            } else {
                self.announce.host
            };
            if half == Half::Off {
                continue;
            }
            if self.is_hot(&id, meta.kind) {
                if half.retains() {
                    self.anticipate(id, meta.kind, hint.probability);
                    self.home_mut(meta.kind).retain_until(id, until, deadline);
                }
                continue;
            }
            if !half.prewarms() {
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

            self.prewarm_ns += ns;
            if half.retains() {
                self.anticipate(id, meta.kind, hint.probability);
                self.home_mut(meta.kind).retain_until(id, until, deadline);
            }
            self.prewarmed_bytes += meta.bytes;
        }
        if prefilled {
            self.seal(None);
        }
    }

    #[must_use]
    pub fn can_satisfy(&self, hint: &FlowHint) -> bool {
        let mut need = [0u64; BlobKind::N];
        for (id, meta) in &hint.downstream {
            if !self.is_hot(id, meta.kind) {
                need[meta.kind.idx()] += meta.bytes;
            }
        }
        let kv_fits = self.kv_partition().is_none_or(|(capacity, _)| {
            let chain: u64 = hint
                .downstream
                .iter()
                .filter(|(_, m)| m.kind == BlobKind::KvBlock)
                .map(|(_, m)| m.bytes)
                .sum();
            chain <= capacity
        });
        kv_fits && self.could_admit(&need, &[0; BlobKind::N])
    }

    pub fn reinstate(&mut self, id: BlobId, meta: BlobMeta) {
        if self.engine_kv(meta.kind).is_some() {
            return;
        }
        let _ = self.admit_hot(id, meta);
    }

    pub fn respill(&mut self, id: BlobId, meta: BlobMeta) {
        if self.engine_kv(meta.kind).is_some() {
            return;
        }
        self.spill(id, meta);
    }

    pub fn drain_all(&mut self, spill: bool) -> (crate::work::Chain, crate::work::Chain) {
        let mut hot = self.hbm.drain_all();
        hot.extend(self.ddr.drain_all());
        let cold = if spill {
            self.nvme.drain_all()
        } else {
            Vec::new()
        };
        if let Some(kv) = self.kv.as_mut() {
            kv.drain();
        }
        for &(_, meta) in hot.iter().chain(&cold) {
            if self.authority(meta.kind, crate::own::Question::Allocation)
                == crate::own::Authority::Engine
            {
                self.drain_engine(meta.kind);
            }
        }
        (hot, cold)
    }

    #[cfg_attr(
        feature = "census",
        deprecated(note = "assumes allocation authority over engine state (bulk drain)")
    )]
    fn drain_engine(&mut self, kind: BlobKind) {
        self.engine_ops.drain[kind.idx()] += 1;
    }

    pub fn supply(&mut self, chain: &[(BlobId, BlobMeta)]) -> usize {
        for (n, &(id, meta)) in chain.iter().enumerate() {
            if self.engine_kv(meta.kind).is_some() && !self.is_hot(&id, meta.kind) {
                let orphan = meta.parent.is_some_and(|p| !self.is_hot(&p, meta.kind));
                if orphan || !self.kv.as_mut().is_some_and(|kv| kv.place(id, meta)) {
                    return n;
                }
                self.forget_cold(&id, meta.kind);
                self.remote_hits[meta.kind.idx()] += 1;
                self.clairvoyant_touch(id, meta.kind);
                continue;
            }
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

    pub fn access_set(&mut self, blobs: &[(BlobId, BlobMeta)]) -> Cost {
        let mut cost = Cost::default();
        for &(id, meta) in blobs {
            if self.is_hot(&id, meta.kind) {
                self.touch(id, meta.kind);
                self.hits[meta.kind.idx()] += 1;
                self.clairvoyant_touch(id, meta.kind);
                continue;
            }
            if self.engine_kv(meta.kind).is_some() {
                if self.materialise_kv(id, meta, &mut cost) {
                    self.clairvoyant_touch(id, meta.kind);
                } else {
                    cost.preempted = true;
                }
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
        if chain
            .first()
            .is_some_and(|(_, m)| self.engine_kv(m.kind).is_some())
        {
            return self.access_kv(chain);
        }
        let mut cost = Cost::default();
        let hit = chain.partition_point(|(id, m)| self.is_hot(id, m.kind));
        self.seq_hits = hit as u64;
        if hit > 0 {
            let (id, meta) = chain[hit - 1];
            self.touch(id, meta.kind);
            self.hits[meta.kind.idx()] += hit as u64;
        }

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

    fn access_kv(&mut self, chain: &[(BlobId, BlobMeta)]) -> Cost {
        let mut cost = Cost::default();
        let hit = chain.partition_point(|(id, m)| self.is_hot(id, m.kind));
        for &(id, meta) in &chain[..hit] {
            self.touch(id, meta.kind);
            self.clairvoyant_touch(id, meta.kind);
        }
        if hit > 0 {
            self.hits[chain[0].1.kind.idx()] += hit as u64;
        }
        self.seq_hits = hit as u64;
        for (i, &(id, meta)) in chain.iter().enumerate().skip(hit) {
            if !self.materialise_kv(id, meta, &mut cost) {
                self.preempt(chain, i, &mut cost);
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
            kv: None,
        };
        let mut h = Hierarchy::new(mem, Policy::Clairvoyant);

        let meta = || BlobMeta {
            kind: BlobKind::Snapshot,
            bytes: 100,
            parent: None,
            recompute_ns: 1_000,
        };
        let (a, b, c) = (BlobId::leaf(b"a"), BlobId::leaf(b"b"), BlobId::leaf(b"c"));

        let mut index = HashMap::new();
        index.insert(a, VecDeque::from([0u64, 5]));
        index.insert(b, VecDeque::from([1u64]));
        index.insert(c, VecDeque::from([2u64, 9]));
        h.set_clairvoyant_index(index);

        h.set_clairvoyant_op(0);
        assert!(!h.access(&[(a, meta())]).pending);
        h.set_clairvoyant_op(1);
        assert!(!h.access(&[(b, meta())]).pending);
        assert!(h.is_hot(&a, BlobKind::Snapshot));
        assert!(h.is_hot(&b, BlobKind::Snapshot));

        h.set_clairvoyant_op(2);
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

    #[test]
    #[allow(clippy::many_single_char_names)]
    fn a_skipped_reference_does_not_desync_the_clairvoyant_schedule() {
        let bands = [0u8; BlobKind::N];

        let mem = NodeMemory {
            hbm: 0,
            ddr: 300,
            nvme: 0,
            hbm_quota: Quota::open(0, bands),
            ddr_quota: Quota::open(300, bands),
            can_decode: true,
            kv: None,
        };
        let mut h = Hierarchy::new(mem, Policy::Clairvoyant);
        let meta = || BlobMeta {
            kind: BlobKind::Snapshot,
            bytes: 100,
            parent: None,
            recompute_ns: 1_000,
        };
        let (a, x, y, z) = (
            BlobId::leaf(b"skip-a"),
            BlobId::leaf(b"skip-x"),
            BlobId::leaf(b"skip-y"),
            BlobId::leaf(b"skip-z"),
        );
        let mut index = HashMap::new();

        index.insert(a, VecDeque::from([0u64, 1, 2, 99]));
        index.insert(x, VecDeque::from([4u64, 10]));
        index.insert(y, VecDeque::from([5u64, 20]));
        index.insert(z, VecDeque::from([6u64, 7]));
        h.set_clairvoyant_index(index);

        h.set_clairvoyant_op(0);
        assert!(!h.access(&[(a, meta())]).pending);
        h.set_clairvoyant_op(3);
        assert!(!h.access(&[(a, meta())]).pending);
        h.set_clairvoyant_op(4);
        assert!(!h.access(&[(x, meta())]).pending);
        h.set_clairvoyant_op(5);
        assert!(!h.access(&[(y, meta())]).pending);

        h.set_clairvoyant_op(6);
        assert!(!h.access(&[(z, meta())]).pending);
        assert!(
            !h.is_hot(&a, BlobKind::Snapshot),
            "a's true next use (99) is the furthest resident, so a goes first"
        );
        assert!(h.is_hot(&x, BlobKind::Snapshot));
        assert!(
            h.is_hot(&y, BlobKind::Snapshot),
            "y is what a desynced schedule would have evicted instead of a"
        );
        assert!(h.is_hot(&z, BlobKind::Snapshot));
    }

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

        let (id_b, meta_b) = service(b"soft-b", 2_000);
        out.clear();
        assert_eq!(soft.admit(id_b, meta_b, &mut out), Admission::Admitted);
        assert_eq!(soft.coupled_decisions, 1);
        assert_eq!(
            soft.coupled, 1,
            "the only evictable byte here is a different class"
        );

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
            kv: None,
        };
        Hierarchy::new(mem, Policy::Gdsf)
    }

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

                assert_ne!(got, Tier::Nvme);
            }
        }
    }

    fn granted(hbm: u64, partition: u64) -> Hierarchy {
        let bands = [0u8; BlobKind::N];
        Hierarchy::new(
            NodeMemory {
                hbm,
                ddr: 8 << 30,
                nvme: 64 << 30,
                hbm_quota: Quota::open(hbm, bands),
                ddr_quota: Quota::open(8 << 30, bands),
                can_decode: true,
                kv: Some(EngineKv {
                    partition,
                    offload: if hbm > 0 { 1 << 30 } else { 0 },
                    spill: 2 << 30,
                    clairvoyant: false,
                }),
            },
            Policy::Gdsf,
        )
    }

    #[test]
    fn the_partition_and_its_pool_sum_to_the_declared_capacity() {
        let split = granted(4 << 30, 1 << 30);
        assert_eq!(split.hbm.spec().capacity + (1 << 30), 4 << 30);
        assert_eq!(split.ddr.spec().capacity + (1 << 30), 8 << 30);
        assert_eq!(split.nvme.spec().capacity + (2 << 30), 64 << 30);
        assert_eq!(split.kv_partition(), Some((1 << 30, 0)));
        let unified = granted(0, 3 << 30);
        assert_eq!(unified.ddr.spec().capacity + (3 << 30), 8 << 30);
    }

    #[test]
    #[should_panic(expected = "exceeds")]
    fn a_partition_larger_than_its_pool_is_refused_at_construction() {
        let _ = granted(0, 16 << 30);
    }

    #[test]
    fn a_sequence_that_cannot_fit_is_preempted_and_charged_its_whole_rebuild() {
        let mut h = granted(4 << 30, 3 * (512 << 10));
        let chain: Vec<(BlobId, BlobMeta)> = {
            let mut out = Vec::new();
            let mut parent = None;
            for i in 0..5u8 {
                let id = parent.map_or_else(|| BlobId::leaf(&[i]), |p| BlobId::chain(p, &[i]));
                out.push((
                    id,
                    BlobMeta {
                        kind: BlobKind::KvBlock,
                        bytes: 512 << 10,
                        parent,
                        recompute_ns: 1_000,
                    },
                ));
                parent = Some(id);
            }
            out
        };
        let first = h.access(&chain[..2]);
        assert!(!first.preempted && !first.pending);
        h.seal(None);
        let second = h.access(&chain);
        assert!(
            second.preempted,
            "five blocks never fit a three-block partition"
        );
        assert!(!second.pending, "preemption is not a refusal");
        assert_eq!(
            second.recompute_ns,
            5 * 1_000,
            "the hit prefix is rebuilt too"
        );
        assert_eq!(h.preemptions(), 1);
        assert_eq!(h.kv_partition().map(|(_, pinned)| pinned), Some(0));
        assert_eq!(h.kv_orphans(), 0);
    }

    const BLOCK: u64 = 512 << 10;

    fn kv_chain(tag: &str, len: usize) -> Vec<(BlobId, BlobMeta)> {
        let mut out = Vec::with_capacity(len);
        let mut parent = None;
        for i in 0..len {
            let content = format!("{tag}:{i}");
            let id = parent.map_or_else(
                || BlobId::leaf(content.as_bytes()),
                |p| BlobId::chain(p, content.as_bytes()),
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

    fn tiers(partition: u64, offload: u64) -> Hierarchy {
        let bands = [0u8; BlobKind::N];
        Hierarchy::new(
            NodeMemory {
                hbm: 4 << 30,
                ddr: 8 << 30,
                nvme: 64 << 30,
                hbm_quota: Quota::open(4 << 30, bands),
                ddr_quota: Quota::open(8 << 30, bands),
                can_decode: true,
                kv: Some(EngineKv {
                    partition,
                    offload,
                    spill: 8 * BLOCK,
                    clairvoyant: false,
                }),
            },
            Policy::Gdsf,
        )
    }

    #[test]
    fn a_promoted_block_leaves_no_copy_in_a_colder_tier() {
        let mut h = tiers(2 * BLOCK, BLOCK);
        let (a, b) = (kv_chain("a", 2), kv_chain("b", 2));
        for chain in [&a, &b, &a] {
            assert!(!h.access(chain).preempted);
            h.seal(None);
        }
        let kv = h.kv.as_ref().expect("the bit is on");
        for (id, _) in &a {
            assert!(kv.gpu.contains(id));
            assert!(!kv.offload.contains(id) && !kv.spill.contains(id));
        }
    }

    #[test]
    fn a_preempted_prefix_counts_as_rebuilt_not_as_hit() {
        let mut h = tiers(3 * BLOCK, BLOCK);
        let chain = kv_chain("p", 5);
        let _ = h.access(&chain[..2]);
        h.seal(None);
        let cost = h.access(&chain);
        assert!(cost.preempted);
        let k = BlobKind::KvBlock.idx();
        assert_eq!(h.hits[k], 0);
        assert_eq!(h.misses[k], 7, "two cold, then the whole second sequence");
    }

    #[test]
    fn output_the_ledger_cannot_hold_is_a_preemption_not_a_refusal() {
        let bands = [0u8; BlobKind::N];
        let quota = Quota::from_split(4 * BLOCK, [0.75, 0.0, 0.0, 0.0], bands, true);
        let mut h = Hierarchy::new(
            NodeMemory {
                hbm: 4 * BLOCK,
                ddr: 8 << 30,
                nvme: 64 << 30,
                hbm_quota: quota,
                ddr_quota: Quota::open(8 << 30, bands),
                can_decode: true,
                kv: None,
            },
            Policy::Gdsf,
        );
        let whole = kv_chain("o", 4);
        let (chain, produces) = whole.split_at(2);
        let mut cost = h.access(chain);
        assert!(!cost.pending);
        let charged = cost.recompute_ns;
        h.decode_output(chain, produces, charged, &mut cost);
        assert!(cost.preempted);
        assert!(!cost.pending);
        assert_eq!(h.refused()[BlobKind::KvBlock.idx()], 0);
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

    fn pool_of(blobs: u64) -> TierPool {
        TierPool::new(
            TierSpec::dram(blobs * 100),
            Policy::Gdsf,
            false,
            Quota::open(blobs * 100, [0; BlobKind::N]),
        )
    }

    fn blob(tag: &str) -> (BlobId, BlobMeta) {
        (
            BlobId::leaf(tag.as_bytes()),
            BlobMeta {
                kind: BlobKind::Snapshot,
                bytes: 100,
                parent: None,
                recompute_ns: 1_000,
            },
        )
    }

    fn victim_of(enforced: bool) -> BlobId {
        let mut pool = pool_of(2);
        let (a, b, c) = (blob("a"), blob("b"), blob("c"));
        let mut out = Vec::new();
        pool.admit(a.0, a.1, &mut out);
        pool.admit(b.0, b.1, &mut out);
        pool.touch(b.0);
        pool.anticipate(a.0, 1.0);
        pool.retain_until(a.0, 10, enforced);
        pool.retire(10);
        pool.admit(c.0, c.1, &mut out);
        out[0].0
    }

    #[test]
    fn a_deadline_withdraws_the_bump_and_an_unbounded_one_never_self_corrects() {
        let (a, b) = (blob("a").0, blob("b").0);
        assert_eq!(
            victim_of(true),
            a,
            "the withdrawn bump no longer shields it"
        );
        assert_eq!(
            victim_of(false),
            b,
            "the unbounded bump outranks a touched blob"
        );
    }

    #[test]
    fn a_bump_past_its_deadline_is_stale_until_withdrawn() {
        for (enforced, stale) in [(false, 1), (true, 0)] {
            let mut pool = pool_of(2);
            let a = blob("a");
            pool.admit(a.0, a.1, &mut Vec::new());
            pool.anticipate(a.0, 1.0);
            pool.retain_until(a.0, 10, enforced);
            assert_eq!(pool.stale_bumps(9), 0);
            pool.retire(9);
            assert_eq!(pool.stale_bumps(10), 1);
            pool.retire(10);
            assert_eq!(pool.stale_bumps(10), stale, "enforced {enforced}");
        }
    }

    #[test]
    fn evict_first_puts_a_blob_at_the_front_of_its_class_until_it_is_touched() {
        let mut pool = pool_of(2);
        let (a, b, c, d) = (blob("a"), blob("b"), blob("c"), blob("d"));
        let mut out = Vec::new();
        pool.admit(a.0, a.1, &mut out);
        pool.admit(b.0, b.1, &mut out);
        for _ in 0..3 {
            pool.touch(a.0);
        }
        pool.evict_first(a.0);
        pool.admit(c.0, c.1, &mut out);
        assert_eq!(out[0].0, a.0, "the hottest blob goes first once demoted");
        pool.touch(b.0);
        pool.evict_first(b.0);
        pool.touch(b.0);
        pool.admit(d.0, d.1, &mut out);
        assert_eq!(out[1].0, c.0, "a touch cancels the demotion");
    }

    #[test]
    fn announce_halves_split_retention_from_prewarm() {
        let downstream: Vec<_> = (0..4).map(|i| blob(&format!("s{i}"))).collect();
        let hint = FlowHint {
            task: 1,
            downstream: downstream.clone(),
            probability: 1.0,
            lead_ops: 6,
            payload_bytes: 0,
        };
        for (half, prewarmed) in [
            (Half::Both, true),
            (Half::Retain, false),
            (Half::Prewarm, true),
            (Half::Off, false),
        ] {
            let mut h = hierarchy(true);
            h.set_announce(AnnounceMix {
                kv: Half::Both,
                host: half,
            });
            h.announce(&hint);
            assert_eq!(h.prewarmed_bytes > 0, prewarmed, "{half:?}");
            assert_eq!(h.is_hot(&downstream[0].0, BlobKind::Snapshot), prewarmed);
        }
        for (deadline, stale) in [(false, 4), (true, 0)] {
            let mut h = hierarchy(true);
            for &(id, meta) in &downstream {
                assert!(!h.access(&[(id, meta)]).pending);
            }
            h.set_deadline(deadline);
            h.set_announce(AnnounceMix {
                kv: Half::Both,
                host: Half::Retain,
            });
            h.announce(&hint);
            h.tick(100);
            assert_eq!(h.stale_bumps(), stale, "deadline {deadline}");
        }
    }

    #[test]
    fn a_prefill_places_a_chain_in_order_and_a_mark_is_echoed_on_the_stream() {
        let mut h = tiers(16 * BLOCK, 0);
        h.record_kv_events(true);
        let chain = kv_chain("p", 3);
        assert_eq!(
            h.prefill_block(chain[2].0, chain[2].1),
            None,
            "no parent yet"
        );
        for &(id, meta) in &chain {
            assert_eq!(h.prefill_block(id, meta), Some(meta.recompute_ns));
        }
        h.seal(None);
        for &(id, meta) in &chain {
            assert!(h.is_hot(&id, meta.kind));
            assert_eq!(h.prefill_block(id, meta), Some(0), "already resident");
        }
        assert_eq!(h.prefilled_blocks, 3);
        let mark = Mark {
            rank: crate::stream::Rank::Retain,
            until_ns: 50,
        };
        let stored = h.take_kv_events().len();
        assert_eq!(stored, 3);
        assert!(h.mark_kv(chain[1].0, mark));
        assert!(!h.mark_kv(BlobId::leaf(b"absent"), mark));
        assert_eq!(
            h.take_kv_events(),
            vec![KvEvent::Stored {
                id: chain[1].0,
                bytes: BLOCK,
                medium: Medium::Gpu,
                mark: Some(mark),
            }]
        );
        assert_eq!(h.kv_live_marks(49), vec![(chain[1].0, mark)]);
        h.tick(50);
        assert!(h.kv_live_marks(50).is_empty());
        assert_eq!(h.directive_stats().expired, 1);
    }
}
