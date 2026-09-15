use crate::blob::{BlobId, BlobKind, BlobMeta};
use crate::cache::{Cost, Hierarchy, Policy};
use crate::tier::TierSpec;

#[derive(Debug)]
pub enum Cache {
    Unified(Box<Hierarchy>),
    Siloed(Vec<Hierarchy>),
}

impl Cache {
    #[must_use]
    pub fn unified(dram: u64, nvme: u64, policy: Policy) -> Self {
        Cache::Unified(Box::new(Hierarchy::new(
            TierSpec::dram(dram),
            TierSpec::nvme(nvme),
            policy,
        )))
    }

    #[must_use]
    pub fn siloed(dram: u64, nvme: u64, split: [f64; BlobKind::N], policy: Policy) -> Self {
        let pools = split
            .iter()
            .map(|f| {
                Hierarchy::new(
                    TierSpec::dram((dram as f64 * f) as u64),
                    TierSpec::nvme((nvme as f64 * f) as u64),
                    policy,
                )
            })
            .collect();
        Cache::Siloed(pools)
    }

    pub fn access(&mut self, chain: &[(BlobId, BlobMeta)]) -> Cost {
        match self {
            Cache::Unified(h) => h.access(chain),
            Cache::Siloed(pools) => {
                let k = chain.first().map_or(0, |(_, m)| m.kind.idx());
                pools[k].access(chain)
            }
        }
    }

    pub fn pools(&self) -> &[Hierarchy] {
        match self {
            Cache::Unified(h) => std::slice::from_ref(&**h),
            Cache::Siloed(p) => p,
        }
    }

    #[must_use]
    pub fn hit_rate(&self, kind: BlobKind) -> f64 {
        let k = kind.idx();
        let (mut h, mut n) = (0u64, 0u64);
        for p in self.pools() {
            h += p.hits[k];
            n += p.hits[k] + p.nvme_hits[k] + p.misses[k];
        }
        if n == 0 { 0.0 } else { h as f64 / n as f64 }
    }

    #[must_use]
    pub fn resident_bytes(&self, kind: BlobKind) -> u64 {
        self.pools()
            .iter()
            .map(|p| p.dram.resident_bytes(kind))
            .sum()
    }
}

#[derive(Debug)]
pub struct Report {
    pub label: String,
    pub total_ns: u64,
    pub transfer_ns: u64,
    pub p50_ns: u64,
    pub p99_ns: u64,
    pub hit: [f64; BlobKind::N],
    pub resident: [u64; BlobKind::N],
    pub phase_ns: [u64; crate::work::PHASES],
    pub kind_ns: [u64; BlobKind::N],
    pub kind_ops: [u64; BlobKind::N],
    pub overcommit: u64,
    pub pinned_skips: u64,
}

#[must_use]
pub fn run(label: &str, mut cache: Cache, seed: u64, ops: u64, vol: f64) -> Report {
    let mut costs: Vec<u64> = Vec::with_capacity(ops as usize);
    let (mut total, mut transfer) = (0u64, 0u64);
    let mut phase_ns = [0u64; crate::work::PHASES];
    let mut kind_ns = [0u64; BlobKind::N];
    let mut kind_ops = [0u64; BlobKind::N];
    for req in crate::work::Workload::new(seed, ops, vol) {
        let c = cache.access(&req.chain);
        total += c.total_ns();
        transfer += c.transfer_ns;
        phase_ns[req.phase] += c.total_ns();
        let k = req.chain.first().map_or(0, |(_, m)| m.kind.idx());
        kind_ns[k] += c.total_ns();
        kind_ops[k] += 1;
        costs.push(c.total_ns());
    }
    costs.sort_unstable();
    let pick = |q: f64| {
        costs
            .get(((costs.len() as f64 * q) as usize).min(costs.len() - 1))
            .copied()
            .unwrap_or(0)
    };
    let mut hit = [0.0; BlobKind::N];
    let mut resident = [0; BlobKind::N];
    for k in BlobKind::ALL {
        hit[k.idx()] = cache.hit_rate(k);
        resident[k.idx()] = cache.resident_bytes(k);
    }
    Report {
        label: label.to_string(),
        total_ns: total,
        transfer_ns: transfer,
        p50_ns: pick(0.50),
        p99_ns: pick(0.99),
        hit,
        resident,
        phase_ns,
        kind_ns,
        kind_ops,
        overcommit: cache.pools().iter().map(|p| p.dram.overcommit).sum(),
        pinned_skips: cache.pools().iter().map(|p| p.dram.pinned_skips).sum(),
    }
}
