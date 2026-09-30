use crate::admit::{Reservations, Reserve};
use crate::blob::{BlobId, BlobKind};
use crate::cache::{
    Ahead, AnnounceMix, Cost, EngineKv, Half, Hierarchy, NodeMemory, Policy, Quota, accelerated,
};
use crate::flow::{FlowHint, FlowMode};
use std::collections::{HashMap, HashSet, VecDeque};

const PHANTOM_SEED: u64 = 0x0BAD_F1A6_5EED;

#[derive(Clone, Copy, Debug)]
pub struct Trial {
    pub bands: [u8; BlobKind::N],
    pub flows: FlowMode,

    pub hbm: u64,
    pub dram: u64,
    pub nvme: u64,
    pub policy: Policy,
    pub seed: u64,
    pub ops: u64,
    pub vol: f64,
    pub fix: Correction,
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub struct Correction {
    pub engine: Option<EngineArm>,
    pub decode_kv: Option<u64>,
    pub reserve: Reserve,
    pub max_token_slack: f64,
    pub announce: AnnounceMix,
    pub ahead: Ahead,
    pub deadline: bool,
    pub false_hints: f64,
}

impl Default for Correction {
    fn default() -> Self {
        Self {
            engine: None,
            decode_kv: None,
            reserve: Reserve::Prompt,
            max_token_slack: crate::work::MAX_TOKEN_SLACK,
            announce: AnnounceMix {
                kv: Half::Both,
                host: Half::Both,
            },
            ahead: Ahead {
                prefill: false,
                hold: false,
            },
            deadline: false,
            false_hints: 0.0,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub struct EngineArm {
    pub scale: f64,
    pub partition: Option<u64>,
    pub clairvoyant: bool,
}

impl Default for EngineArm {
    fn default() -> Self {
        Self {
            scale: 1.0,
            partition: None,
            clairvoyant: false,
        }
    }
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
    pub prefilled_blocks: u64,
    pub held_blocks: u64,
    pub false_hints: u64,
    pub kv_hints: u64,
    pub stale_bumps: usize,

    pub engine_ops: crate::cache::EngineOps,
    pub kv_mean: [u64; 3],
    pub refused_by_router: [u64; BlobKind::N],
    pub preempted: [u64; BlobKind::N],
    pub grant: Option<EngineKv>,
    pub decode_blocks: u64,
    pub decodes: u64,
    pub kv_orphans: usize,
}

impl Report {
    #[must_use]
    pub fn goodput(&self) -> f64 {
        let served: u64 = self.served.iter().sum();
        let total: u64 = served
            + self.refused.iter().sum::<u64>()
            + self.refused_by_router.iter().sum::<u64>()
            + self.gated;
        if total == 0 {
            0.0
        } else {
            served as f64 / total as f64
        }
    }

    #[must_use]
    pub fn broken_rate(&self) -> f64 {
        if self.flow_started == 0 {
            0.0
        } else {
            self.flow_broken as f64 / self.flow_started as f64
        }
    }

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
        let total = self.served[k] + self.refused[k] + self.refused_by_router[k];
        if total == 0 {
            0.0
        } else {
            self.served[k] as f64 / total as f64
        }
    }
}

#[must_use]
pub fn trace(t: Trial) -> Vec<crate::work::Request> {
    let w = crate::work::Workload::new(t.seed, t.ops, t.vol)
        .with_max_token_slack(t.fix.max_token_slack);
    match t.fix.decode_kv {
        Some(per_block) => w.with_decode_kv(per_block).collect(),
        None => w.collect(),
    }
}

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

#[derive(Clone, Copy, Debug)]
pub enum Budget {
    Split {
        split: [f64; BlobKind::N],
        hard: bool,
    },

    Open,
}

impl Budget {
    fn host(self, capacity: u64, bands: [u8; BlobKind::N], beside_hbm: bool) -> Quota {
        let q = match self {
            Self::Split { split, hard } => Quota::from_split(capacity, split, bands, hard),
            Self::Open => Quota::open(capacity, bands),
        };
        if beside_hbm { q.offloaded() } else { q }
    }

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

fn memory_for(t: Trial, budget: Budget, kv: Option<EngineKv>) -> NodeMemory {
    NodeMemory {
        hbm: t.hbm,
        ddr: t.dram,
        nvme: t.nvme,
        hbm_quota: budget.accelerator(t.hbm, t.bands),
        ddr_quota: budget.host(t.dram, t.bands, t.hbm > 0),
        can_decode: true,
        kv,
    }
}

#[must_use]
pub fn grant(t: Trial, budget: Budget, arm: EngineArm, off: &Report) -> EngineKv {
    grant_for(&memory_for(t, budget, None), off.kv_mean, arm)
}

#[must_use]
pub fn grant_for(mem: &NodeMemory, off: [u64; 3], arm: EngineArm) -> EngineKv {
    let split = mem.hbm > 0;
    let floor_or = |q: &Quota, mean: u64| match q.floor_of(BlobKind::KvBlock) {
        0 => mean,
        floor => floor,
    };
    let (partition, pool) = if split {
        (floor_or(&mem.hbm_quota, off[0]), mem.hbm)
    } else {
        (floor_or(&mem.ddr_quota, off[0]), mem.ddr)
    };
    EngineKv {
        partition: arm
            .partition
            .unwrap_or((partition as f64 * arm.scale) as u64)
            .min(pool),
        offload: if split {
            floor_or(&mem.ddr_quota, off[1]).min(mem.ddr)
        } else {
            0
        },
        spill: off[2].min(mem.nvme),
        clairvoyant: arm.clairvoyant,
    }
}

#[must_use]
pub fn run(label: &str, t: Trial, budget: Budget) -> Report {
    run_on(label, t, budget, &trace(t))
}

#[must_use]
pub fn run_on(label: &str, t: Trial, budget: Budget, trace: &[crate::work::Request]) -> Report {
    let Some(arm) = t.fix.engine else {
        return run_with(label, t, budget, trace, None);
    };
    let ledger = Trial {
        fix: Correction {
            engine: None,
            ..t.fix
        },
        ..t
    };
    let off = run_with("", ledger, budget, trace, None);
    run_with(label, t, budget, trace, Some(grant(t, budget, arm, &off)))
}

#[must_use]
#[allow(clippy::too_many_lines)]
pub fn run_with(
    label: &str,
    t: Trial,
    budget: Budget,
    trace: &[crate::work::Request],
    kv: Option<EngineKv>,
) -> Report {
    let mut h = Hierarchy::new(memory_for(t, budget, kv), t.policy);
    h.set_announce(t.fix.announce);
    h.set_ahead(t.fix.ahead);
    h.set_deadline(t.fix.deadline);
    if t.policy == Policy::Clairvoyant || kv.is_some_and(|k| k.clairvoyant) {
        h.set_clairvoyant_index(clairvoyant_index(trace));
    }
    let per_block = t.fix.decode_kv.unwrap_or(crate::work::TOKENS_PER_KV_BLOCK);
    let nothing_in_flight = Reservations::default();
    let mut refused_by_router = [0u64; BlobKind::N];
    let mut preempted = [0u64; BlobKind::N];
    let (mut decode_blocks, mut decodes) = (0u64, 0u64);
    let mut kv_sum = [0u64; 3];
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

    let mut op = 0u64;
    let mut false_hints = 0u64;
    let mut kv_hints = 0u64;
    let mut phantoms = crate::rng::Rng::new(PHANTOM_SEED ^ t.seed);
    let mut phantom_task = u64::MAX;
    for req in trace {
        if req.gang.is_some() {
            continue;
        }
        h.set_clairvoyant_op(op);
        op += 1;
        h.tick(op);
        for (sum, bytes) in kv_sum.iter_mut().zip(h.kv_bytes()) {
            *sum += bytes;
        }
        let k = req.kind_idx();

        if let Some(hint) = &req.hint
            && t.flows == FlowMode::Gate
            && !h.can_satisfy(hint)
        {
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

        let turned_away = h.kv_partition().is_some_and(|(capacity, _)| {
            !nothing_in_flight.admits(capacity, req, t.fix.reserve, per_block, None)
        });
        let mut c = if turned_away {
            refused_by_router[k] += 1;
            Cost {
                pending: true,
                ..Cost::default()
            }
        } else {
            h.access(&req.chain)
        };
        let chain_recompute = c.recompute_ns;
        if !c.pending && !req.requires.is_empty() {
            let dep = h.access_set(&req.requires);
            c.transfer_ns += dep.transfer_ns;
            c.recompute_ns += dep.recompute_ns;
            c.pending |= dep.pending;
            c.preempted |= dep.preempted;
        }
        if !c.pending {
            c.exec_ns = req.exec_ns;
            h.decode_output(&req.chain, &req.produces, chain_recompute, &mut c);
            if req.tokens > 0 {
                decode_blocks += req.produces.len() as u64;
                decodes += 1;
            }
            preempted[k] += u64::from(c.preempted);
        }
        h.seal(None);

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
            kv_hints += u64::from(
                hint.downstream
                    .first()
                    .is_some_and(|(_, m)| m.kind == BlobKind::KvBlock),
            );
            started.insert(hint.task, c.total_ns());
            if t.flows != FlowMode::Blind {
                h.announce(hint);
            }
        }
        if t.flows != FlowMode::Blind
            && t.fix.false_hints > 0.0
            && k == BlobKind::Snapshot.idx()
            && phantoms.chance(t.fix.false_hints)
        {
            let function = phantoms.zipf(crate::work::FLOW_FUNCTIONS, 1.5);
            let call = crate::work::FLOW_CALLS + phantoms.below(crate::work::FLOW_CALLS);
            phantom_task -= 1;
            h.announce(&FlowHint {
                task: phantom_task,
                downstream: crate::work::flow_downstream(function, call),
                probability: 1.0,
                lead_ops: crate::work::FLOW_LEAD_OPS,
                payload_bytes: 0,
            });
            false_hints += 1;
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
    let samples = op.max(1);
    let mut r = finish(
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
    );
    r.false_hints = false_hints;
    r.kv_hints = kv_hints;
    r.kv_mean = kv_sum.map(|b| b / samples);
    r.refused_by_router = refused_by_router;
    r.preempted = preempted;
    r.grant = kv;
    r.decode_blocks = decode_blocks;
    r.decodes = decodes;
    r
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
        prefilled_blocks: h.prefilled_blocks,
        held_blocks: h.held_blocks,
        false_hints: 0,
        kv_hints: 0,
        stale_bumps: h.stale_bumps(),
        engine_ops: h.engine_ops,
        kv_mean: [0; 3],
        refused_by_router: [0; BlobKind::N],
        preempted: [0; BlobKind::N],
        grant: None,
        decode_blocks: 0,
        decodes: 0,
        kv_orphans: h.kv_orphans(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::EngineOps;

    fn trial(ops: u64) -> Trial {
        Trial {
            bands: [0, 1, 2, 1],
            flows: FlowMode::Announce,
            hbm: 4 << 30,
            dram: 8 << 30,
            nvme: 64 << 30,
            policy: Policy::Gdsf,
            seed: 1,
            ops,
            vol: 1.0,
            fix: Correction::default(),
        }
    }

    fn engine(t: Trial) -> Trial {
        Trial {
            fix: Correction {
                engine: Some(EngineArm::default()),
                ..t.fix
            },
            ..t
        }
    }

    fn row(ops: &EngineOps, kind: BlobKind) -> [u64; 8] {
        let k = kind.idx();
        [
            ops.admit[k],
            ops.touch[k],
            ops.anticipate[k],
            ops.demote[k],
            ops.forget_cold[k],
            ops.superseded[k],
            ops.spill[k],
            ops.drain[k],
        ]
    }

    const HARD: Budget = Budget::Split {
        split: [0.25, 0.25, 0.25, 0.125],
        hard: true,
    };

    #[test]
    fn census_kvblock_row_is_zero_with_the_bit_on() {
        for hbm in [4u64 << 30, 0] {
            for budget in [Budget::Open, HARD] {
                let t = engine(Trial {
                    hbm,
                    dram: if hbm == 0 { 12 << 30 } else { 8 << 30 },
                    ..trial(3_000)
                });
                let off = run(
                    "",
                    Trial {
                        fix: Correction::default(),
                        ..t
                    },
                    budget,
                );
                let on = run("", t, budget);
                assert!(
                    row(&off.engine_ops, BlobKind::KvBlock).iter().sum::<u64>() > 0,
                    "the fixture must exercise engine paths with the bit off"
                );
                assert_eq!(
                    row(&on.engine_ops, BlobKind::KvBlock),
                    [0; 8],
                    "hbm={hbm} {budget:?}"
                );
            }
        }
    }

    #[test]
    fn census_weightshard_row_is_unchanged_across_the_bit_under_hard_lru_partitions() {
        let t = engine(Trial {
            flows: FlowMode::Blind,
            policy: Policy::Lru,
            ..trial(3_000)
        });
        let off = run(
            "",
            Trial {
                fix: Correction::default(),
                ..t
            },
            HARD,
        );
        let on = run("", t, HARD);
        let (w_off, w_on) = (
            row(&off.engine_ops, BlobKind::WeightShard),
            row(&on.engine_ops, BlobKind::WeightShard),
        );
        assert!(w_off.iter().sum::<u64>() > 0);
        assert_eq!(w_off, w_on);
    }

    #[test]
    fn the_partition_never_holds_a_block_without_its_parent() {
        for decode_kv in [None, Some(crate::work::TOKENS_PER_KV_BLOCK)] {
            let t = engine(Trial {
                fix: Correction {
                    decode_kv,
                    ..Correction::default()
                },
                ..trial(3_000)
            });
            let r = run("", t, Budget::Open);
            assert!(r.grant.is_some());
            assert_eq!(r.kv_orphans, 0, "decode_kv={decode_kv:?}");
        }
    }

    #[test]
    fn decode_output_holds_the_mean_growth_at_35_and_reproduces_the_table() {
        for (per_block, expect) in [(35u64, 4.015), (32, 4.33), (16, 8.18), (8, 15.88)] {
            let r = run(
                "",
                Trial {
                    fix: Correction {
                        decode_kv: Some(per_block),
                        ..Correction::default()
                    },
                    ..trial(15_000)
                },
                Budget::Open,
            );
            let mean = r.decode_blocks as f64 / r.decodes as f64;
            assert!(
                (mean - expect).abs() / expect < 0.02,
                "{per_block}: {mean:.3} blocks per decode against {expect}"
            );
            if per_block == 35 {
                assert!((mean - 4.0).abs() / 4.0 < 0.01, "{mean:.3}");
            }
        }
    }

    #[test]
    fn the_grant_is_the_budgets_own_kv_floor() {
        let t = engine(trial(1_000));
        let off = run("", trial(1_000), HARD);
        let g = grant(t, HARD, EngineArm::default(), &off);
        let mem = memory_for(t, HARD, None);
        assert_eq!(g.partition, mem.hbm_quota.floor_of(BlobKind::KvBlock));
        assert_eq!(g.offload, mem.ddr_quota.floor_of(BlobKind::KvBlock));
        assert_eq!(g.spill, off.kv_mean[2]);
    }

    fn soft() -> Budget {
        Budget::Split {
            split: [0.12, 0.12, 0.38, 0.25],
            hard: false,
        }
    }

    fn ahead(prefill: bool, hold: bool) -> Correction {
        Correction {
            engine: Some(EngineArm::default()),
            ahead: Ahead { prefill, hold },
            ..Correction::default()
        }
    }

    #[test]
    fn prefill_ahead_is_what_announce_lost_and_a_hold_is_not() {
        let t = |flows, fix| Trial {
            flows,
            fix,
            ..trial(6_000)
        };
        let blind = run("", t(FlowMode::Blind, ahead(false, false)), soft());
        let skipped = run("", t(FlowMode::Announce, ahead(false, false)), soft());
        let held = run("", t(FlowMode::Announce, ahead(false, true)), soft());
        let prefilled = run("", t(FlowMode::Announce, ahead(true, false)), soft());
        let blind_ahead = run("", t(FlowMode::Blind, ahead(true, true)), soft());
        assert_eq!(format!("{blind:?}"), format!("{blind_ahead:?}"));
        let margin = |r: &Report| 1.0 - r.flow_e2e_ms() / blind.flow_e2e_ms();
        assert!(held.held_blocks > 0);
        assert!((margin(&held) - margin(&skipped)).abs() < 0.01);
        assert!(prefilled.prefilled_blocks > 0);
        assert!(margin(&prefilled) > margin(&skipped) + 0.05);
        let net = |r: &Report| r.total_ns + r.prewarm_ns;
        let drift = (net(&prefilled) as f64 - net(&blind) as f64) / net(&blind) as f64;
        assert!(
            drift.abs() < 0.01,
            "moved off the critical path, not added: {drift}"
        );
    }

    #[test]
    fn a_deadline_on_the_bump_removes_the_stale_entries_false_hints_leave_behind() {
        let run_with = |deadline: bool, false_hints: f64| {
            run(
                "",
                Trial {
                    fix: Correction {
                        deadline,
                        false_hints,
                        ..Correction::default()
                    },
                    ..trial(4_000)
                },
                soft(),
            )
        };
        let (kept, bounded) = (run_with(false, 0.5), run_with(true, 0.5));
        assert!(kept.false_hints > 0 && kept.false_hints == bounded.false_hints);
        assert!(kept.stale_bumps > 0);
        assert!(bounded.stale_bumps < kept.stale_bumps);
        let honest = run_with(false, 0.0);
        assert_eq!(honest.false_hints, 0);
        assert!(
            kept.prewarm_ns > honest.prewarm_ns,
            "a false hint costs what it prewarms"
        );
    }
}
