use crate::blob::{BlobId, BlobKind};
use crate::cache::{Hierarchy, NodeMemory, Policy, Quota, accelerated};
use crate::flow::FlowMode;
use std::collections::{HashMap, HashSet, VecDeque};

#[derive(Clone, Copy, Debug)]
pub struct Trial {
    pub bands: [u8; BlobKind::N],
    pub flows: FlowMode,
    /// Accelerator memory; zero models a unified-memory host.
    pub hbm: u64,
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
    pub gated: u64,
    pub flow_started: u64,
    pub flow_attempted: u64,
    pub flow_done: u64,
    pub flow_broken: u64,
    pub flow_e2e_ns: u64,
    pub prewarmed_bytes: u64,
    pub prewarm_ns: u64,
    /// `phase-1.md` §4.4's dynamic census, per census-marked entry point.
    pub engine_ops: crate::cache::EngineOps,
}

impl Report {
    #[must_use]
    pub fn goodput(&self) -> f64 {
        let served: u64 = self.served.iter().sum();
        let total: u64 = served + self.refused.iter().sum::<u64>() + self.gated;
        if total == 0 {
            0.0
        } else {
            served as f64 / total as f64
        }
    }

    /// Fraction of *started* tasks whose downstream stage could not be served. A gated task
    /// never starts, so this measures wasted upstream work, not task success.
    #[must_use]
    pub fn broken_rate(&self) -> f64 {
        if self.flow_started == 0 {
            0.0
        } else {
            self.flow_broken as f64 / self.flow_started as f64
        }
    }

    /// Fraction of *attempted* tasks that completed. Counts gated tasks as failures, so a
    /// gate cannot win by refusing everything.
    #[must_use]
    pub fn task_completion(&self) -> f64 {
        if self.flow_attempted == 0 {
            0.0
        } else {
            self.flow_done as f64 / self.flow_attempted as f64
        }
    }

    #[must_use]
    pub fn flow_e2e_ms(&self) -> f64 {
        mean_ms(self.flow_e2e_ns, self.flow_done)
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
pub fn trace(t: Trial) -> Vec<crate::work::Request> {
    crate::work::Workload::new(t.seed, t.ops, t.vol).collect()
}

/// `Policy::Clairvoyant`'s reference-stream index, built once from the full trace before a run
/// starts: for every blob referenced in a chain or a dependency set, the ops at which it is
/// referenced, in order. Positions are counted the same way `run_on`'s own loop counts --
/// skipping gang requests, which a single ledger never processes -- so they stay aligned with
/// the sequence of `Hierarchy::access`/`access_set` calls the index is consumed by.
#[must_use]
fn clairvoyant_index(trace: &[crate::work::Request]) -> HashMap<BlobId, VecDeque<u64>> {
    let mut index: HashMap<BlobId, VecDeque<u64>> = HashMap::new();
    let mut op = 0u64;
    for req in trace {
        if req.gang.is_some() {
            continue;
        }
        for (id, _) in req.chain.iter().chain(&req.requires) {
            index.entry(*id).or_default().push_back(op);
        }
        op += 1;
    }
    index
}

/// How a trial budgets memory between classes.
#[derive(Clone, Copy, Debug)]
pub enum Budget {
    /// Per-class floors as fractions of a pool, soft or hard.
    Split {
        split: [f64; BlobKind::N],
        hard: bool,
    },
    /// No floors at all.
    Open,
}

impl Budget {
    /// The budget for host memory: the split as given. Beside an accelerator, the classes it
    /// hosts are offloads and give up host bytes first.
    fn host(self, capacity: u64, bands: [u8; BlobKind::N], beside_hbm: bool) -> Quota {
        let q = match self {
            Self::Split { split, hard } => Quota::from_split(capacity, split, bands, hard),
            Self::Open => Quota::open(capacity, bands),
        };
        if beside_hbm { q.offloaded() } else { q }
    }

    /// The budget for accelerator memory: the split's KV-to-weights ratio over the classes
    /// that actually live there, with the same share of the pool reserved as the split
    /// reserves in total. Applying the split verbatim would strand the accelerator budget of
    /// host-only classes, and a hard partition would lose most of the pool to state that can
    /// never occupy it.
    fn accelerator(self, capacity: u64, bands: [u8; BlobKind::N]) -> Quota {
        let Self::Split { split, hard } = self else {
            return Quota::open(capacity, bands);
        };
        let reserved: f64 = split.iter().sum();
        let here: f64 = BlobKind::ALL
            .iter()
            .filter(|k| accelerated(**k))
            .map(|k| split[k.idx()])
            .sum();
        let moved = std::array::from_fn(|i| {
            if accelerated(BlobKind::ALL[i]) && here > 0.0 {
                split[i] / here * reserved
            } else {
                0.0
            }
        });
        Quota::from_split(capacity, moved, bands, hard)
    }
}

fn memory_for(t: Trial, budget: Budget) -> NodeMemory {
    NodeMemory {
        hbm: t.hbm,
        ddr: t.dram,
        nvme: t.nvme,
        hbm_quota: budget.accelerator(t.hbm, t.bands),
        ddr_quota: budget.host(t.dram, t.bands, t.hbm > 0),
        can_decode: true,
    }
}

#[must_use]
pub fn run(label: &str, t: Trial, budget: Budget) -> Report {
    run_on(label, t, budget, &trace(t))
}

/// The request stream is a pure function of (seed, ops, vol), so a sweep over quotas can
/// generate it once instead of rebuilding an identical trace for every candidate.
#[must_use]
///
/// With split memory one budget governs both pools: in HBM it sets KV against weights, in DDR
/// it sets function cells and service heaps against what the accelerator has offloaded.
pub fn run_on(label: &str, t: Trial, budget: Budget, trace: &[crate::work::Request]) -> Report {
    let mut h = Hierarchy::new(memory_for(t, budget), t.policy);
    if t.policy == Policy::Clairvoyant {
        h.set_clairvoyant_index(clairvoyant_index(trace));
    }
    let mut costs: Vec<u64> = Vec::with_capacity(t.ops as usize);
    let (mut total, mut transfer) = (0u64, 0u64);
    let mut phase_ns = [0u64; crate::work::PHASES];
    let mut kind_ns = [0u64; BlobKind::N];
    let mut kind_ops = [0u64; BlobKind::N];
    let mut served = [0u64; BlobKind::N];

    let mut started: HashMap<u64, u64> = HashMap::new();
    let mut gated_tasks: HashSet<u64> = HashSet::new();
    let (mut gated, mut flow_started, mut flow_done, mut flow_broken, mut flow_e2e) =
        (0u64, 0u64, 0u64, 0u64, 0u64);
    let mut flow_attempted = 0u64;

    // Counts exactly what `clairvoyant_index` counted -- non-gang requests, in order -- so
    // the ledger's notion of "already in the past" and the index's positions are the same
    // number by construction rather than by coincidence.
    let mut op = 0u64;
    for req in trace {
        // Gangs need somewhere to be placed across; a single ledger has no second node.
        if req.gang.is_some() {
            continue;
        }
        h.set_clairvoyant_op(op);
        op += 1;
        let k = req.kind_idx();

        if let Some(hint) = &req.hint
            && t.flows == FlowMode::Gate
            && !h.can_satisfy(hint)
        {
            // Refusing the upstream must also cancel the task's downstream stage, or the
            // gate "avoids" work that still runs and the saving is imaginary.
            gated_tasks.insert(hint.task);
            gated += 1;
            flow_attempted += 1;
            continue;
        }
        if let Some(task) = req.completes
            && gated_tasks.remove(&task)
        {
            continue;
        }

        let mut c = h.access(&req.chain);
        if !c.pending && !req.requires.is_empty() {
            let dep = h.access_set(&req.requires);
            c.transfer_ns += dep.transfer_ns;
            c.recompute_ns += dep.recompute_ns;
            c.pending |= dep.pending;
        }
        if !c.pending {
            c.exec_ns = req.exec_ns;
        }

        if let Some(up) = req.completes.and_then(|task| started.remove(&task)) {
            if c.pending {
                flow_broken += 1;
            } else {
                flow_done += 1;
                flow_e2e += up + c.total_ns();
            }
        }
        if c.pending {
            continue;
        }
        if let Some(hint) = &req.hint {
            flow_started += 1;
            flow_attempted += 1;
            started.insert(hint.task, c.total_ns());
            if t.flows != FlowMode::Blind {
                h.announce(hint);
            }
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
    finish(
        label,
        &h,
        &costs,
        &Tally {
            total,
            transfer,
            phase_ns,
            kind_ns,
            kind_ops,
            served,
            gated,
            flow_started,
            flow_attempted,
            flow_done,
            flow_broken,
            flow_e2e,
        },
    )
}

struct Tally {
    total: u64,
    transfer: u64,
    phase_ns: [u64; crate::work::PHASES],
    kind_ns: [u64; BlobKind::N],
    kind_ops: [u64; BlobKind::N],
    served: [u64; BlobKind::N],
    gated: u64,
    flow_started: u64,
    flow_attempted: u64,
    flow_done: u64,
    flow_broken: u64,
    flow_e2e: u64,
}

fn finish(label: &str, h: &Hierarchy, costs: &[u64], t: &Tally) -> Report {
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
        let n = h.hits[k] + h.nvme_hits[k] + h.offload_hits[k] + h.misses[k];
        hit[k] = if n == 0 {
            0.0
        } else {
            h.hits[k] as f64 / n as f64
        };
        resident[k] = h.resident_bytes(kind);
    }
    Report {
        label: label.to_string(),
        total_ns: t.total,
        transfer_ns: t.transfer,
        p99_ns: pick(0.99),
        hit,
        resident,
        phase_ns: t.phase_ns,
        kind_ns: t.kind_ns,
        kind_ops: t.kind_ops,
        refused: h.refused(),
        served: t.served,
        pinned_skips: h.pinned_skips(),
        over_capacity: h.over_capacity(),
        gated: t.gated,
        flow_started: t.flow_started,
        flow_attempted: t.flow_attempted,
        flow_done: t.flow_done,
        flow_broken: t.flow_broken,
        flow_e2e_ns: t.flow_e2e,
        prewarmed_bytes: h.prewarmed_bytes,
        prewarm_ns: h.prewarm_ns,
        engine_ops: h.engine_ops,
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
