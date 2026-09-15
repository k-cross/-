use crate::blob::BlobKind;
use crate::cache::{Hierarchy, Policy, Quota};
use crate::tier::TierSpec;

#[derive(Clone, Copy, Debug)]
pub struct Trial {
    pub bands: [u8; BlobKind::N],
    pub dram: u64,
    pub nvme: u64,
    pub policy: Policy,
    pub seed: u64,
    pub ops: u64,
    pub vol: f64,
}

#[derive(Debug)]
pub struct Report {
    pub label: String,
    pub total_ns: u64,
    pub transfer_ns: u64,
    pub p99_ns: u64,
    pub hit: [f64; BlobKind::N],
    pub resident: [u64; BlobKind::N],
    pub phase_ns: [u64; crate::work::PHASES],
    pub kind_ns: [u64; BlobKind::N],
    pub kind_ops: [u64; BlobKind::N],
    pub refused: [u64; BlobKind::N],
    pub served: [u64; BlobKind::N],
    pub pinned_skips: u64,
    pub over_capacity: bool,
}

impl Report {
    #[must_use]
    pub fn goodput(&self) -> f64 {
        let served: u64 = self.served.iter().sum();
        let total: u64 = served + self.refused.iter().sum::<u64>();
        if total == 0 {
            0.0
        } else {
            served as f64 / total as f64
        }
    }

    #[must_use]
    pub fn class_goodput(&self, k: usize) -> f64 {
        let total = self.served[k] + self.refused[k];
        if total == 0 {
            0.0
        } else {
            self.served[k] as f64 / total as f64
        }
    }
}

#[must_use]
pub fn run(label: &str, t: Trial, quota: Quota) -> Report {
    let mut h = Hierarchy::new(
        TierSpec::dram(t.dram),
        TierSpec::nvme(t.nvme),
        t.policy,
        quota,
    );
    let mut costs: Vec<u64> = Vec::with_capacity(t.ops as usize);
    let (mut total, mut transfer) = (0u64, 0u64);
    let mut phase_ns = [0u64; crate::work::PHASES];
    let mut kind_ns = [0u64; BlobKind::N];
    let mut kind_ops = [0u64; BlobKind::N];
    let mut served = [0u64; BlobKind::N];

    for req in crate::work::Workload::new(t.seed, t.ops, t.vol) {
        let k = req.chain.first().map_or(0, |(_, m)| m.kind.idx());
        let c = h.access(&req.chain);
        if c.pending {
            continue;
        }
        served[k] += 1;
        total += c.total_ns();
        transfer += c.transfer_ns;
        phase_ns[req.phase] += c.total_ns();
        kind_ns[k] += c.total_ns();
        kind_ops[k] += 1;
        costs.push(c.total_ns());
    }

    costs.sort_unstable();
    let pick = |q: f64| {
        costs
            .get(((costs.len() as f64 * q) as usize).min(costs.len().saturating_sub(1)))
            .copied()
            .unwrap_or(0)
    };
    let mut hit = [0.0; BlobKind::N];
    let mut resident = [0; BlobKind::N];
    for kind in BlobKind::ALL {
        let k = kind.idx();
        let n = h.hits[k] + h.nvme_hits[k] + h.misses[k];
        hit[k] = if n == 0 {
            0.0
        } else {
            h.hits[k] as f64 / n as f64
        };
        resident[k] = h.dram.resident_bytes(kind);
    }
    Report {
        label: label.to_string(),
        total_ns: total,
        transfer_ns: transfer,
        p99_ns: pick(0.99),
        hit,
        resident,
        phase_ns,
        kind_ns,
        kind_ops,
        refused: h.dram.refused,
        served,
        pinned_skips: h.dram.pinned_skips,
        over_capacity: h.dram.over_capacity(),
    }
}

#[must_use]
pub fn mean_ms(ns: u64, ops: u64) -> f64 {
    if ops == 0 {
        0.0
    } else {
        ns as f64 / ops as f64 / 1e6
    }
}
