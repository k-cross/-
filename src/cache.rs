use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashMap};

use crate::blob::{BlobId, BlobKind, BlobMeta};
use crate::tier::TierSpec;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Policy {
    Gdsf,
    Lru,
}

#[derive(Clone, Copy, Debug)]
struct Entry {
    meta: BlobMeta,
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
    used: u64,
    clock: u64,
    epoch: u64,
    inflation: f64,
    entries: HashMap<BlobId, Entry>,
    evictable: BinaryHeap<Reverse<Ranked>>,
    pub evicted: [u64; 3],
    pub overcommit: u64,
}

impl TierPool {
    #[must_use]
    pub fn new(spec: TierSpec, policy: Policy, leaf_first: bool) -> Self {
        Self {
            spec,
            policy,
            leaf_first,
            used: 0,
            clock: 0,
            epoch: 0,
            inflation: 0.0,
            entries: HashMap::new(),
            evictable: BinaryHeap::new(),
            evicted: [0; 3],
            overcommit: 0,
        }
    }

    #[must_use]
    pub fn spec(&self) -> &TierSpec {
        &self.spec
    }

    #[must_use]
    pub fn contains(&self, id: &BlobId) -> bool {
        self.entries.contains_key(id)
    }

    #[must_use]
    pub fn resident_bytes(&self, kind: BlobKind) -> u64 {
        self.entries
            .values()
            .filter(|e| e.meta.kind == kind)
            .map(|e| e.meta.bytes)
            .sum()
    }

    fn score(&self, meta: &BlobMeta, freq: u32) -> f64 {
        match self.policy {
            Policy::Gdsf => self.inflation + f64::from(freq) * meta.value_per_byte(),
            Policy::Lru => self.clock as f64,
        }
    }

    fn reheap(&mut self, id: BlobId) {
        let Some(e) = self.entries.get_mut(&id) else {
            return;
        };
        if e.resident_children > 0 && self.leaf_first {
            return;
        }
        self.epoch += 1;
        e.epoch = self.epoch;
        let ranked = Ranked {
            priority: e.priority,
            epoch: e.epoch,
            id,
        };
        self.evictable.push(Reverse(ranked));
    }

    pub fn touch(&mut self, id: BlobId) {
        self.clock += 1;
        let Some(e) = self.entries.get_mut(&id) else {
            return;
        };
        e.freq = e.freq.saturating_add(1);
        let (meta, freq) = (e.meta, e.freq);
        let p = self.score(&meta, freq);
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

    fn pop_victim(&mut self) -> Option<(BlobId, Entry)> {
        while let Some(Reverse(r)) = self.evictable.pop() {
            let Some(e) = self.entries.get(&r.id) else {
                continue;
            };
            if e.epoch != r.epoch {
                continue;
            }
            if self.leaf_first && e.resident_children > 0 {
                continue;
            }
            let e = self.entries.remove(&r.id)?;
            self.used -= e.meta.bytes;
            self.inflation = r.priority;
            self.evicted[e.meta.kind.idx()] += 1;
            self.unlink_parent(e.meta.parent);
            return Some((r.id, e));
        }
        None
    }

    pub fn admit(&mut self, id: BlobId, meta: BlobMeta, out: &mut Vec<(BlobId, BlobMeta)>) {
        if self.entries.contains_key(&id) {
            self.touch(id);
            return;
        }
        self.link_parent(meta.parent);
        while self.used + meta.bytes > self.spec.capacity {
            let Some((vid, v)) = self.pop_victim() else {
                self.overcommit += 1;
                break;
            };
            out.push((vid, v.meta));
        }
        self.clock += 1;
        self.used += meta.bytes;
        let priority = self.score(&meta, 1);
        self.epoch += 1;
        self.entries.insert(
            id,
            Entry {
                meta,
                freq: 1,
                resident_children: 0,
                priority,
                epoch: self.epoch,
            },
        );
        self.reheap(id);
    }

    pub fn remove(&mut self, id: &BlobId) -> Option<BlobMeta> {
        let e = self.entries.remove(id)?;
        self.used -= e.meta.bytes;
        self.unlink_parent(e.meta.parent);
        Some(e.meta)
    }
}

#[derive(Clone, Copy, Default, Debug)]
pub struct Cost {
    pub transfer_ns: u64,
    pub recompute_ns: u64,
    pub bytes_in: u64,
}

impl Cost {
    #[must_use]
    pub fn total_ns(&self) -> u64 {
        self.transfer_ns + self.recompute_ns
    }
}

#[derive(Debug)]
pub struct Hierarchy {
    pub dram: TierPool,
    pub nvme: TierPool,
    pub hits: [u64; 3],
    pub nvme_hits: [u64; 3],
    pub misses: [u64; 3],
}

impl Hierarchy {
    #[must_use]
    pub fn new(dram: TierSpec, nvme: TierSpec, policy: Policy) -> Self {
        Self {
            dram: TierPool::new(dram, policy, true),
            nvme: TierPool::new(nvme, policy, false),
            hits: [0; 3],
            nvme_hits: [0; 3],
            misses: [0; 3],
        }
    }

    fn admit_dram(&mut self, id: BlobId, meta: BlobMeta) {
        let mut demoted = Vec::new();
        self.dram.admit(id, meta, &mut demoted);
        for (did, dmeta) in demoted {
            let mut dropped = Vec::new();
            self.nvme.admit(did, dmeta, &mut dropped);
        }
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
            if self.nvme.contains(&id) {
                cost.transfer_ns += self.nvme.spec().fetch_ns(meta.bytes);
                self.nvme.remove(&id);
                self.nvme_hits[k] += 1;
            } else {
                cost.recompute_ns += meta.recompute_ns;
                self.misses[k] += 1;
            }
            cost.bytes_in += meta.bytes;
            self.admit_dram(id, meta);
        }
        cost
    }
}
