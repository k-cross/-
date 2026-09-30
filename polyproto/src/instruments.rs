use std::collections::{HashMap, HashSet};

use crate::belief::Cause;
use crate::blob::{BlobId, BlobMeta};
use crate::stream::{KvEvent, Medium};
use crate::work::{Origin, Origins};

pub const BINS: usize = 12;
pub const CERTAIN: usize = 11;
pub const FULL: usize = 10;

#[derive(Clone, Copy, Debug, Default)]
pub struct Bin {
    pub n: u64,
    pub resident: u64,
    pub predicted: f64,
}

#[derive(Clone, Debug, Default)]
pub struct Calibration {
    pub bins: [Bin; BINS],
}

impl Calibration {
    pub fn record(&mut self, predicted: f64, unknown: u64, resident: bool) {
        let bin = if unknown == 0 {
            CERTAIN
        } else if predicted >= 1.0 {
            FULL
        } else {
            ((predicted * 10.0) as usize).min(9)
        };
        let b = &mut self.bins[bin];
        b.n += 1;
        b.resident += u64::from(resident);
        b.predicted += predicted;
    }

    #[must_use]
    pub fn label(bin: usize) -> String {
        match bin {
            CERTAIN => "certain".to_string(),
            FULL => "1.0".to_string(),
            b => format!("{:.1}", b as f64 / 10.0),
        }
    }

    #[must_use]
    pub fn total(&self) -> u64 {
        self.bins.iter().map(|b| b.n).sum()
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Share {
    pub inside: u64,
    pub inside_on_node: u64,
    pub outside: u64,
    pub outside_on_node: u64,
}

impl Share {
    #[must_use]
    pub fn inside_share(&self) -> f64 {
        self.inside_on_node as f64 / self.inside.max(1) as f64
    }

    #[must_use]
    pub fn outside_share(&self) -> f64 {
        self.outside_on_node as f64 / self.outside.max(1) as f64
    }
}

#[derive(Clone, Debug, Default)]
pub struct ReuseRow {
    pub accesses: u64,
    pub hit: u64,
    pub cold: u64,
    pub evicted_to_offload: u64,
    pub evicted_to_spill: u64,
    pub evicted_gone: u64,
    pub stores: u64,
    pub gap_hit: Vec<u64>,
    pub gap_evicted: Vec<u64>,
    pub residency: Vec<u64>,
}

#[derive(Clone, Debug)]
pub struct Tenants {
    origins: Origins,
    first: HashMap<BlobId, Option<u32>>,
    requester: Option<u32>,
    pub touches: u64,
    pub cross_touches: u64,
    pub by_origin: Vec<(u64, u64)>,
    pub per_owner: HashMap<Option<u32>, (u64, u64)>,
    pub evictions: u64,
    pub evictions_by_other: u64,
}

impl Tenants {
    #[must_use]
    pub fn new(origins: Origins) -> Self {
        Self {
            origins,
            first: HashMap::new(),
            requester: None,
            touches: 0,
            cross_touches: 0,
            by_origin: vec![(0, 0); Origin::N],
            per_owner: HashMap::new(),
            evictions: 0,
            evictions_by_other: 0,
        }
    }

    pub fn set_requester(&mut self, tenant: Option<u32>) {
        self.requester = tenant;
    }

    pub fn read(&mut self, id: &BlobId, hit: bool) {
        let owner = *self.first.entry(*id).or_insert(self.requester);
        self.touches += 1;
        self.cross_touches += u64::from(owner != self.requester);
        let origin = self.origins.of(id).unwrap_or(Origin::Session);
        let row = &mut self.by_origin[origin.idx()];
        row.0 += 1;
        row.1 += u64::from(hit);
        let per = self.per_owner.entry(self.requester).or_default();
        per.0 += 1;
        per.1 += u64::from(hit);
    }

    pub fn events(&mut self, events: &[KvEvent]) {
        for event in events {
            match *event {
                KvEvent::Stored {
                    id,
                    medium: Medium::Gpu,
                    mark: None,
                    ..
                } => {
                    self.first.entry(id).or_insert(self.requester);
                }
                KvEvent::Removed {
                    id,
                    medium: Medium::Gpu,
                } => {
                    self.evictions += 1;
                    let owner = self.first.get(&id).copied().flatten();
                    self.evictions_by_other += u64::from(owner != self.requester);
                }
                _ => {}
            }
        }
    }

    #[must_use]
    pub fn hit_rate_of(&self, tenants: impl Iterator<Item = u32>) -> f64 {
        let (reads, hits) = tenants
            .filter_map(|t| self.per_owner.get(&Some(t)))
            .fold((0, 0), |(r, h), &(tr, th)| (r + tr, h + th));
        hits as f64 / reads.max(1) as f64
    }

    #[must_use]
    pub fn by_volume(&self, among: u32) -> Vec<u32> {
        let mut ranked: Vec<u32> = (0..among).collect();
        ranked.sort_by_key(|t| {
            std::cmp::Reverse(self.per_owner.get(&Some(*t)).map_or(0, |&(r, _)| r))
        });
        ranked
    }
}

#[derive(Clone, Debug)]
pub struct Reuse {
    origins: Origins,
    last_on: Vec<HashMap<BlobId, u64>>,
    evicted_on: Vec<HashSet<BlobId>>,
    uses: HashMap<BlobId, u32>,
    pub rows: Vec<ReuseRow>,
    pub dispatches: u64,
    pub dispatches_with_evicted_miss: u64,
    pub evicted_miss_ns: u64,
    pub cold_miss_ns: u64,
}

impl Reuse {
    #[must_use]
    pub fn new(origins: Origins, nodes: usize) -> Self {
        Self {
            origins,
            last_on: vec![HashMap::new(); nodes],
            evicted_on: vec![HashSet::new(); nodes],
            uses: HashMap::new(),
            rows: vec![ReuseRow::default(); Origin::N],
            dispatches: 0,
            dispatches_with_evicted_miss: 0,
            evicted_miss_ns: 0,
            cold_miss_ns: 0,
        }
    }

    fn row(&mut self, id: &BlobId) -> &mut ReuseRow {
        let origin = self.origins.of(id).unwrap_or(Origin::Session);
        &mut self.rows[origin.idx()]
    }

    pub fn classify(
        &mut self,
        d: usize,
        now: u64,
        id: &BlobId,
        tier: Option<Medium>,
        cost_ns: u64,
    ) -> bool {
        *self.uses.entry(*id).or_insert(0) += 1;
        let gap = self.last_on[d].get(id).map(|t| now.saturating_sub(*t));
        let evicted = self.evicted_on[d].contains(id);
        let row = self.row(id);
        row.accesses += 1;
        if tier == Some(Medium::Gpu) {
            row.hit += 1;
            row.gap_hit.extend(gap);
            return false;
        }
        if !evicted {
            row.cold += 1;
            self.cold_miss_ns += cost_ns;
            return false;
        }
        match tier {
            Some(Medium::Cpu) => row.evicted_to_offload += 1,
            Some(Medium::Storage) => row.evicted_to_spill += 1,
            _ => row.evicted_gone += 1,
        }
        row.gap_evicted.extend(gap);
        self.evicted_miss_ns += cost_ns;
        true
    }

    pub fn touched(&mut self, d: usize, now: u64, id: BlobId) {
        self.last_on[d].insert(id, now);
    }

    pub fn events(&mut self, d: usize, now: u64, events: &[KvEvent]) {
        for event in events {
            match *event {
                KvEvent::Stored {
                    id,
                    medium: Medium::Gpu,
                    mark: None,
                    ..
                } => {
                    self.row(&id).stores += 1;
                    self.last_on[d].entry(id).or_insert(now);
                    self.uses.entry(id).or_insert(0);
                }
                KvEvent::Removed {
                    id,
                    medium: Medium::Gpu,
                } => {
                    if let Some(t) = self.last_on[d].get(&id).copied() {
                        self.row(&id).residency.push(now.saturating_sub(t));
                    }
                    self.evicted_on[d].insert(id);
                }
                KvEvent::Cleared => {
                    self.last_on[d].clear();
                    self.evicted_on[d].clear();
                }
                _ => {}
            }
        }
    }

    #[must_use]
    pub fn one_shot(&self) -> Vec<(u64, u64)> {
        let mut out = vec![(0u64, 0u64); Origin::N];
        for (id, n) in &self.uses {
            let origin = self.origins.of(id).unwrap_or(Origin::Session);
            out[origin.idx()].0 += 1;
            out[origin.idx()].1 += u64::from(*n <= 1);
        }
        out
    }
}

#[must_use]
pub fn percentile(values: &mut [u64], q: f64) -> Option<u64> {
    if values.is_empty() {
        return None;
    }
    values.sort_unstable();
    let i = ((values.len() as f64 * q) as usize).min(values.len() - 1);
    Some(values[i])
}

#[derive(Clone, Copy, Debug, Default)]
pub struct FlowDownstream {
    pub n: u64,
    pub stall_ns: u64,
}

impl FlowDownstream {
    pub fn record(&mut self, stall_ns: u64) {
        self.n += 1;
        self.stall_ns += stall_ns;
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Prefill {
    pub calls: u64,
    pub blocks: u64,
    pub work_ns: u64,
    pub landings: u64,
    pub landed: u64,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Emitted {
    pub emitted: u64,
    pub honoured: u64,
}

#[derive(Clone, Debug, Default)]
pub struct Instruments {
    pub decisions: u64,
    pub exposed_any: u64,
    pub exposed_chosen: u64,
    pub phantom_depth_blocks: u64,
    pub all: Calibration,
    pub chosen: Calibration,
    pub divergence_samples: u64,
    pub phantom_share: f64,
    pub miss_share: f64,
    pub phantom_blocks: [u64; Cause::N],
    pub miss_blocks: [u64; Cause::N],
    pub phantom_cause_share: [f64; Cause::N],
    pub miss_cause_share: [f64; Cause::N],
    pub reuse: Option<Reuse>,
    pub tenants: Option<Tenants>,
    pub flow: FlowDownstream,
    pub prefill: Prefill,
    pub work_by_origin: [Vec<u64>; Origin::N],
    pub directives: Emitted,
    pub gap_phantom: u64,
    pub gap_miss: u64,
    pub gap_discount: u64,
    pub migrations: u64,
    pub needless_migrations: u64,
    pub churn: u64,
    pub placed: u64,
    pub silence: Share,
    last_node: HashMap<BlobId, usize>,
}

impl Instruments {
    pub fn place(&mut self, chain: &[(BlobId, BlobMeta)], node: usize) {
        self.placed += 1;
        let Some(&(tip, _)) = chain.last() else {
            return;
        };
        if let Some(&before) = chain
            .iter()
            .rev()
            .find_map(|(id, _)| self.last_node.get(id))
        {
            self.churn += u64::from(before != node);
        }
        self.last_node.insert(tip, node);
    }

    #[must_use]
    pub fn gaps(&self) -> u64 {
        self.gap_phantom + self.gap_miss + self.gap_discount
    }
}
