use std::collections::HashMap;

use polyphonic::arms::{Budget, Correction, EngineArm, Report, Trial, run as run_trial};
use polyphonic::belief::Cause;
use polyphonic::cache::{Ahead, AnnounceMix, DirectiveStats, Half, Policy};
use polyphonic::flow::FlowMode;
use polyphonic::instruments::{FULL, Prefill, percentile};
use polyphonic::topo::Distance;
use polyphonic::work::Origin;

use super::belief_cmd::{Env as LabEnv, Lab, Regime, on, scored_fetch};
use super::{
    ArmRun, BeliefArgs, EmitArg, InfluenceArgs, MarksArg, RecoveryArg, ScoringArg, TargetArg,
    best_split, gib, mean_of, ms, quantile,
};

pub struct Env {
    pub nodes: usize,
    pub units_per_node: usize,
    pub hbm: u64,
    pub dram: u64,
    pub nvme: u64,
    pub ops: u64,
    pub seed: u64,
    pub rate: f64,
    pub fanout: f64,
    pub seeds: u64,
    pub sections: String,
}

impl Env {
    fn lab_envs(&self) -> Vec<LabEnv> {
        (self.seed..self.seed + self.seeds)
            .map(|seed| LabEnv {
                nodes: self.nodes,
                units_per_node: self.units_per_node,
                hbm: self.hbm,
                dram: self.dram,
                nvme: self.nvme,
                ops: self.ops,
                seed,
                rate: self.rate,
                fanout: self.fanout,
                sections: String::new(),
            })
            .collect()
    }
}

const TAIL: f64 = 0.99;
const OFF: InfluenceArgs = InfluenceArgs::OFF;

#[derive(Clone, Copy)]
struct Cell {
    service: f64,
    p99: f64,
    stall: f64,
    flow_stall: f64,
    flow_n: u64,
    marks: DirectiveStats,
    emitted: u64,
    prefill: Prefill,
}

fn cell(r: &ArmRun) -> Cell {
    let all: Vec<u64> = r.t.samples.iter().flatten().copied().collect();
    let i = &r.mach.instruments;
    Cell {
        service: mean_of(&all),
        p99: ms(quantile(&all, TAIL)),
        stall: r.total as f64 / r.served.max(1) as f64 / 1e6,
        flow_stall: i.flow.stall_ns as f64 / i.flow.n.max(1) as f64 / 1e6,
        flow_n: i.flow.n,
        marks: r.mach.directive_stats(),
        emitted: i.directives.emitted,
        prefill: i.prefill,
    }
}

#[derive(Default)]
struct Baselines(HashMap<(u64, Regime, usize), Cell>);

fn change(base: f64, x: f64) -> f64 {
    100.0 * (x - base) / base.abs().max(f64::MIN_POSITIVE)
}

fn seeds_of(values: &[f64], unit: &str) -> String {
    let joined: Vec<String> = values.iter().map(|v| format!("{v:+.2}")).collect();
    format!("{}{unit}", joined.join(" / "))
}

fn plain_seeds(values: &[f64], places: usize, unit: &str) -> String {
    let joined: Vec<String> = values.iter().map(|v| format!("{v:.places$}")).collect();
    format!("{}{unit}", joined.join(" / "))
}

fn section(title: &str) {
    println!("\n{title}");
}

fn identical(a: &ArmRun, b: &ArmRun) -> bool {
    a.t.samples == b.t.samples
        && a.total == b.total
        && a.served == b.served
        && a.mach.fetches == b.mach.fetches
        && a.mach.rebuilds == b.mach.rebuilds
        && a.mach.decisions == b.mach.decisions
        && a.mach.instruments.phantom_blocks == b.mach.instruments.phantom_blocks
        && a.mach.instruments.miss_blocks == b.mach.instruments.miss_blocks
        && format!("{:?}", a.mach.spans) == format!("{:?}", b.mach.spans)
}

fn directive(emit: EmitArg, horizon: f64) -> InfluenceArgs {
    InfluenceArgs {
        directives: true,
        emit,
        horizon,
        ..OFF
    }
}

fn gate(labs: &mut [Lab<'_>]) {
    section("1. the gate: ignored directives under an acknowledged belief change nothing");
    let lab = &mut labs[0];
    for regime in Regime::ALL {
        let lossy = BeliefArgs { loss: 0.05, ..on() };
        let off = lab.go_with(Distance::Rack, regime, &scored_fetch(), lossy, OFF, true);
        for (name, emit) in [
            ("declared", EmitArg::Declared),
            ("oracle, 5 s", EmitArg::Oracle),
        ] {
            let ignored = InfluenceArgs {
                ignores: true,
                ..directive(emit, 5.0)
            };
            let r = lab.go_with(
                Distance::Rack,
                regime,
                &scored_fetch(),
                lossy,
                ignored,
                true,
            );
            println!(
                "  {:<36} {:<12} {}: {} marks emitted, {} honoured",
                regime.label(),
                name,
                if identical(&off, &r) {
                    "identical"
                } else {
                    "DIFFERS"
                },
                r.mach.instruments.directives.emitted,
                r.mach.instruments.directives.honoured,
            );
        }
    }
}

fn causes(labs: &mut [Lab<'_>]) {
    section(
        "2. divergence by cause: share of believed blocks that are phantoms, and of held blocks that are misses (%)",
    );
    let lab = &mut labs[0];
    for regime in Regime::ALL {
        println!("\n  {}", regime.label());
        print!("  {:<22} {:>8}", "loss, recovery", "phantom");
        for c in Cause::ALL {
            print!(" {:>13}", c.label());
        }
        println!(" {:>8}", "miss");
        let mut cells: Vec<(String, BeliefArgs)> = Vec::new();
        for (loss, recovery) in [
            (0.0, RecoveryArg::Replay),
            (0.05, RecoveryArg::Replay),
            (0.2, RecoveryArg::Replay),
            (0.05, RecoveryArg::Periodic),
            (0.05, RecoveryArg::None),
            (0.2, RecoveryArg::None),
        ] {
            cells.push((
                format!("{:.0}% {recovery:?}", 100.0 * loss),
                BeliefArgs {
                    loss,
                    recovery,
                    ..on()
                },
            ));
        }
        cells.push((
            "2 s silence, replay".to_string(),
            BeliefArgs {
                silence: 2.0,
                ..on()
            },
        ));
        for (name, belief) in cells {
            let r = lab.go(Distance::Rack, regime, &scored_fetch(), belief, false);
            let i = &r.mach.instruments;
            let n = i.divergence_samples.max(1) as f64;
            print!("  {:<22} {:>7.3}%", name, 100.0 * i.phantom_share / n);
            for c in Cause::ALL {
                print!(" {:>12.3}%", 100.0 * i.phantom_cause_share[c.idx()] / n);
            }
            println!(" {:>7.3}%", 100.0 * i.miss_share / n);
            let missed: Vec<String> = Cause::ALL
                .iter()
                .filter(|c| i.miss_blocks[c.idx()] > 0)
                .map(|c| {
                    format!(
                        "{} {:.4}%",
                        c.label(),
                        100.0 * i.miss_cause_share[c.idx()] / n
                    )
                })
                .collect();
            if !missed.is_empty() {
                println!("  {:<22} misses: {}", "", missed.join(", "));
            }
        }
    }
}

fn announce_run(t: Trial, flows: FlowMode, fix: Correction, budget: Budget) -> Report {
    run_trial("", Trial { flows, fix, ..t }, budget)
}

fn margin(blind: &Report, r: &Report) -> f64 {
    100.0 * (blind.flow_e2e_ms() - r.flow_e2e_ms()) / blind.flow_e2e_ms().max(1e-9)
}

fn work_drift(blind: &Report, r: &Report) -> f64 {
    let net = |x: &Report| (x.total_ns + x.prewarm_ns) as f64;
    100.0 * (net(r) - net(blind)) / net(blind).max(1.0)
}

fn mix(kv: Half, host: Half) -> AnnounceMix {
    AnnounceMix { kv, host }
}

#[allow(clippy::too_many_lines)]
fn flows(env: &Env) {
    section(
        "3. announce split by half, and the engine's hold and prefill-ahead (one node): task-latency margin over blind, by seed",
    );
    for (memory, hbm, dram) in [("split", 4u64 << 30, 8u64 << 30), ("unified", 0, 12 << 30)] {
        let mut rows: Vec<(&str, Vec<f64>, Vec<f64>)> = Vec::new();
        let mut push = |name: &'static str, margins: f64, drift: f64, first: bool| {
            if first {
                rows.push((name, Vec::new(), Vec::new()));
            }
            if let Some(row) = rows.iter_mut().find(|r| r.0 == name) {
                row.1.push(margins);
                row.2.push(drift);
            }
        };
        for (n, seed) in (env.seed..env.seed + env.seeds).enumerate() {
            let first = n == 0;
            let cfg = Trial {
                bands: [0, 1, 2, 1],
                flows: FlowMode::Blind,
                hbm,
                dram,
                nvme: 64 << 30,
                policy: Policy::Gdsf,
                seed,
                ops: 15_000,
                vol: 1.0,
                fix: Correction::default(),
            };
            let (_, split) = best_split(cfg, false, 0.125);
            let budget = Budget::Split { split, hard: false };
            let ledger = |kv, host| Correction {
                announce: mix(kv, host),
                ..Correction::default()
            };
            let blind = announce_run(cfg, FlowMode::Blind, ledger(Half::Both, Half::Both), budget);
            for (name, kv, host) in [
                ("ledger: announce as published", Half::Both, Half::Both),
                ("ledger: KV retention only", Half::Retain, Half::Both),
                ("ledger: KV prewarm only", Half::Prewarm, Half::Both),
                ("ledger: no KV (host-only)", Half::Off, Half::Both),
                ("ledger: host retention only", Half::Off, Half::Retain),
                ("ledger: host prewarm only", Half::Off, Half::Prewarm),
            ] {
                let r = announce_run(cfg, FlowMode::Announce, ledger(kv, host), budget);
                push(name, margin(&blind, &r), work_drift(&blind, &r), first);
            }
            let engine = |prefill, hold| Correction {
                engine: Some(EngineArm::default()),
                ahead: Ahead { prefill, hold },
                ..Correction::default()
            };
            let engine_blind = announce_run(cfg, FlowMode::Blind, engine(false, false), budget);
            for (name, prefill, hold) in [
                ("engine: announce, KV skipped", false, false),
                ("engine: + resident blocks held", false, true),
                ("engine: + missing blocks prefilled", true, false),
                ("engine: + both", true, true),
            ] {
                let r = announce_run(cfg, FlowMode::Announce, engine(prefill, hold), budget);
                push(
                    name,
                    margin(&engine_blind, &r),
                    work_drift(&engine_blind, &r),
                    first,
                );
            }
        }
        println!(
            "\n  {memory} memory, seeds {} .. {}",
            env.seed,
            env.seed + env.seeds - 1
        );
        println!(
            "  {:<40} {:>28} {:>26}",
            "", "margin over blind", "net work vs blind"
        );
        for (name, margins, drift) in rows {
            println!(
                "  {name:<40} {:>28} {:>26}",
                plain_seeds(&margins, 1, "%"),
                seeds_of(&drift, "%")
            );
        }
    }
}

fn versus(
    labs: &mut [Lab<'_>],
    baselines: &mut Baselines,
    dist: Distance,
    regime: Regime,
    arms: &[(&str, InfluenceArgs)],
) {
    println!("\n  {} at {}", regime.label(), dist.label());
    println!(
        "  {:<34} {:>22} {:>22} {:>22} {:>24} {:>9} {:>9} {:>9}",
        "arm",
        "stall vs off",
        "service vs off",
        "p99 vs off",
        "flow stall vs off",
        "marks",
        "pressure",
        "landed"
    );
    let tracking = InfluenceArgs { reuse: true, ..OFF };
    let base: Vec<Cell> = labs
        .iter_mut()
        .enumerate()
        .map(|(seed, lab)| {
            *baselines
                .0
                .entry((dist.one_way_ns(), regime, seed))
                .or_insert_with(|| {
                    cell(&lab.go_with(
                        dist,
                        regime,
                        &scored_fetch(),
                        BeliefArgs::OFF,
                        tracking,
                        false,
                    ))
                })
        })
        .collect();
    for (name, influence) in arms {
        let runs: Vec<Cell> = labs
            .iter_mut()
            .map(|lab| {
                cell(&lab.go_with(
                    dist,
                    regime,
                    &scored_fetch(),
                    BeliefArgs::OFF,
                    InfluenceArgs {
                        reuse: true,
                        ..*influence
                    },
                    false,
                ))
            })
            .collect();
        let over = |f: fn(&Cell) -> f64| -> Vec<f64> {
            base.iter()
                .zip(&runs)
                .map(|(b, r)| change(f(b), f(r)))
                .collect()
        };
        let applied: u64 = runs.iter().map(|c| c.marks.applied).sum();
        let pressure: u64 = runs.iter().map(|c| c.marks.pressure_evictions).sum();
        let landings: u64 = runs.iter().map(|c| c.prefill.landings).sum();
        let landed: u64 = runs.iter().map(|c| c.prefill.landed).sum();
        println!(
            "  {name:<34} {:>22} {:>22} {:>22} {:>24} {applied:>9} {pressure:>9} {:>9}",
            seeds_of(&over(|c| c.stall), "%"),
            seeds_of(&over(|c| c.service), "%"),
            seeds_of(&over(|c| c.p99), "%"),
            seeds_of(&over(|c| c.flow_stall), "%"),
            if landings == 0 {
                "-".to_string()
            } else {
                format!("{:.0}%", 100.0 * landed as f64 / landings as f64)
            },
        );
        if runs.iter().any(|c| c.prefill.calls > 0) {
            let work: f64 = runs.iter().map(|c| c.prefill.work_ns as f64).sum::<f64>() / 1e9;
            let saved: f64 = base
                .iter()
                .zip(&runs)
                .map(|(b, r)| (b.flow_stall - r.flow_stall) * r.flow_n as f64 / 1e3)
                .sum();
            println!(
                "  {:<34} prefill work {work:.1} s for {saved:.1} s of flow stall saved, {} blocks",
                "",
                runs.iter().map(|c| c.prefill.blocks).sum::<u64>()
            );
        }
        if runs.iter().any(|c| c.emitted > 0) {
            println!(
                "  {:<34} {} marks emitted",
                "",
                runs.iter().map(|c| c.emitted).sum::<u64>()
            );
        }
    }
}

fn prefill(labs: &mut [Lab<'_>], baselines: &mut Baselines) {
    section("4. prefill-ahead on the cluster, `scored + fetch`, by seed");
    let ahead = InfluenceArgs {
        prefill_ahead: true,
        ..OFF
    };
    let deepest = InfluenceArgs {
        prefill_target: TargetArg::Deepest,
        ..ahead
    };
    for regime in Regime::ALL {
        versus(
            labs,
            baselines,
            Distance::Rack,
            regime,
            &[
                ("prefill-ahead, scored argmin", ahead),
                ("prefill-ahead, deepest prefix", deepest),
            ],
        );
    }
    println!("\n  by distance, first seed only");
    for dist in [Distance::Zone, Distance::Region] {
        let n = labs.len().min(1);
        versus(
            &mut labs[..n],
            baselines,
            dist,
            Regime::Defaults,
            &[
                ("prefill-ahead, scored argmin", ahead),
                ("prefill-ahead, deepest prefix", deepest),
            ],
        );
    }
}

fn declared(labs: &mut [Lab<'_>], baselines: &mut Baselines) {
    section("5. the declared emitter, honoured and ignored, by seed");
    let arms = [
        ("retain parent chains", directive(EmitArg::Retain, 5.0)),
        (
            "retain parent chains, ignored",
            InfluenceArgs {
                ignores: true,
                ..directive(EmitArg::Retain, 5.0)
            },
        ),
        (
            "evict-first on one-shot scopes",
            directive(EmitArg::EvictFirst, 5.0),
        ),
        ("both", directive(EmitArg::Declared, 5.0)),
    ];
    for regime in Regime::ALL {
        versus(labs, baselines, Distance::Rack, regime, &arms);
    }
}

fn ceilings(labs: &mut [Lab<'_>], baselines: &mut Baselines) {
    section("6. the ceilings: what a perfect gap estimator or block manager could buy, by seed");
    let arms = [
        (
            "clairvoyant block manager",
            InfluenceArgs {
                clairvoyant_kv: true,
                ..OFF
            },
        ),
        ("oracle emitter, 2 s", directive(EmitArg::Oracle, 2.0)),
        ("oracle emitter, 5 s", directive(EmitArg::Oracle, 5.0)),
        ("oracle emitter, 30 s", directive(EmitArg::Oracle, 30.0)),
    ];
    for regime in Regime::ALL {
        versus(labs, baselines, Distance::Rack, regime, &arms);
    }
}

fn top_bin(r: &ArmRun) -> (u64, f64, f64) {
    let bin = &r.mach.instruments.all.bins[FULL];
    let n = bin.n.max(1) as f64;
    (bin.n, bin.predicted / n, bin.resident as f64 / n)
}

fn belief_gap(r: &ArmRun) -> f64 {
    let spans = r.mach.spans.len().max(1) as f64;
    r.mach.spans.iter().map(|s| s.regret.belief).sum::<i64>() as f64 / spans
}

fn marks(labs: &mut [Lab<'_>]) {
    section(
        "7. an ignored directive: the acknowledged belief against one that believes its own requests",
    );
    let rules = [
        ("expected", ScoringArg::Expected),
        ("quantile 0.9", ScoringArg::Quantile),
    ];
    for regime in Regime::ALL {
        println!("\n  {}", regime.label());
        println!(
            "  {:<22} {:<13} {:<8} {:>22} {:>22} {:>30} {:>24}",
            "condition",
            "rule",
            "belief",
            "service vs acked",
            "belief ns/decision",
            "top bin n / predicted / realised",
            "phantom share"
        );
        for (name, loss, recovery) in [
            ("no loss, replay", 0.0, RecoveryArg::Replay),
            ("5% loss, replay", 0.05, RecoveryArg::Replay),
            ("20% loss, no recovery", 0.2, RecoveryArg::None),
        ] {
            for (rule, scoring) in rules {
                let belief = BeliefArgs {
                    loss,
                    recovery,
                    scoring,
                    ..on()
                };
                let mut runs: Vec<(MarksArg, Vec<ArmRun>)> = Vec::new();
                for kind in [MarksArg::Acked, MarksArg::Trusted] {
                    let influence = InfluenceArgs {
                        ignores: true,
                        marks: kind,
                        ..directive(EmitArg::Retain, 5.0)
                    };
                    let set = labs
                        .iter_mut()
                        .map(|lab| {
                            lab.go_with(
                                Distance::Rack,
                                regime,
                                &scored_fetch(),
                                belief,
                                influence,
                                true,
                            )
                        })
                        .collect();
                    runs.push((kind, set));
                }
                let acked: Vec<Cell> = runs[0].1.iter().map(cell).collect();
                for (kind, set) in &runs {
                    let cells: Vec<Cell> = set.iter().map(cell).collect();
                    let service: Vec<f64> = acked
                        .iter()
                        .zip(&cells)
                        .map(|(a, c)| change(a.service, c.service))
                        .collect();
                    let gap: Vec<f64> = set.iter().map(belief_gap).collect();
                    let phantom: Vec<f64> = set
                        .iter()
                        .map(|r| {
                            let i = &r.mach.instruments;
                            100.0 * i.phantom_share / i.divergence_samples.max(1) as f64
                        })
                        .collect();
                    let bins: Vec<(u64, f64, f64)> = set.iter().map(top_bin).collect();
                    let n: u64 = bins.iter().map(|b| b.0).sum();
                    let mean = |f: fn(&(u64, f64, f64)) -> f64| {
                        bins.iter().map(|b| f(b) * b.0 as f64).sum::<f64>() / n.max(1) as f64
                    };
                    println!(
                        "  {name:<22} {rule:<13} {:<8} {:>22} {:>22} {:>14} / {:.3} / {:.3} {:>24}",
                        format!("{kind:?}").to_lowercase(),
                        seeds_of(&service, "%"),
                        plain_seeds(&gap, 0, ""),
                        n,
                        mean(|b| b.1),
                        mean(|b| b.2),
                        plain_seeds(&phantom, 3, "%"),
                    );
                }
            }
        }
    }
}

#[derive(Clone, Copy)]
struct Retained {
    margin: f64,
    drift: f64,
    false_share: f64,
    prewarm_s: f64,
    stale: u64,
}

fn retain(env: &Env) {
    section(
        "8. a deadline in place of the unbounded bump, under hints for flows that never come (one node, split memory)",
    );
    let rates = [0.0, 0.1, 0.45, 1.0];
    let mut rows: Vec<Vec<Retained>> = vec![Vec::new(); rates.len() * 2];
    for seed in env.seed..env.seed + env.seeds {
        let cfg = Trial {
            bands: [0, 1, 2, 1],
            flows: FlowMode::Blind,
            hbm: 4 << 30,
            dram: 8 << 30,
            nvme: 64 << 30,
            policy: Policy::Gdsf,
            seed,
            ops: 15_000,
            vol: 1.0,
            fix: Correction::default(),
        };
        let (_, split) = best_split(cfg, false, 0.125);
        let budget = Budget::Split { split, hard: false };
        let blind = announce_run(cfg, FlowMode::Blind, Correction::default(), budget);
        for (i, &false_hints) in rates.iter().enumerate() {
            for (j, deadline) in [false, true].into_iter().enumerate() {
                let fix = Correction {
                    deadline,
                    false_hints,
                    ..Correction::default()
                };
                let r = announce_run(cfg, FlowMode::Announce, fix, budget);
                let hints = r.kv_hints + r.false_hints;
                rows[i * 2 + j].push(Retained {
                    margin: margin(&blind, &r),
                    drift: work_drift(&blind, &r),
                    false_share: 100.0 * r.false_hints as f64 / hints.max(1) as f64,
                    prewarm_s: r.prewarm_ns as f64 / 1e9,
                    stale: r.stale_bumps as u64,
                });
            }
        }
    }
    println!(
        "\n  {:<10} {:<10} {:>13} {:>28} {:>26} {:>13} {:>16}",
        "hint rate",
        "bump",
        "false share",
        "margin over blind",
        "net work vs blind",
        "prewarm work",
        "stale entries"
    );
    for (i, &false_hints) in rates.iter().enumerate() {
        for (j, name) in ["unbounded", "deadline"].into_iter().enumerate() {
            let cells = &rows[i * 2 + j];
            let col = |f: fn(&Retained) -> f64| -> Vec<f64> { cells.iter().map(f).collect() };
            println!(
                "  {false_hints:<10} {name:<10} {:>13} {:>28} {:>26} {:>13} {:>16}",
                plain_seeds(&col(|c| c.false_share), 0, "%"),
                plain_seeds(&col(|c| c.margin), 1, "%"),
                seeds_of(&col(|c| c.drift), "%"),
                plain_seeds(&col(|c| c.prewarm_s), 1, " s"),
                plain_seeds(&col(|c| c.stale as f64), 0, ""),
            );
        }
    }
}

fn reuse(labs: &mut [Lab<'_>]) {
    section(
        "9. reuse against LRU residency, by origin (`scored + fetch`, rack, exact view, first seed)",
    );
    let tracking = InfluenceArgs { reuse: true, ..OFF };
    let secs = |v: &mut [u64], q: f64| percentile(v, q).map_or(f64::NAN, |ns| ns as f64 / 1e9);
    for regime in Regime::ALL {
        let mut r = labs[0].go_with(
            Distance::Rack,
            regime,
            &scored_fetch(),
            BeliefArgs::OFF,
            tracking,
            false,
        );
        let c = cell(&r);
        let served = r.served.max(1) as f64;
        let Some(reuse) = r.mach.instruments.reuse.as_mut() else {
            continue;
        };
        println!(
            "\n  {}: service {:.3} ms, stall {:.3} ms",
            regime.label(),
            c.service,
            c.stall
        );
        let evicted_ms = reuse.evicted_miss_ns as f64 / served / 1e6;
        println!(
            "  {} KV dispatches, {:.1}% with a miss on a block this node evicted; those misses cost {:.2} ms per served request ({:.1}% of stall, {:.2}% of service); first-touch misses cost {:.2} ms",
            reuse.dispatches,
            100.0 * reuse.dispatches_with_evicted_miss as f64 / reuse.dispatches.max(1) as f64,
            evicted_ms,
            100.0 * evicted_ms / c.stall.max(f64::MIN_POSITIVE),
            100.0 * evicted_ms / c.service.max(f64::MIN_POSITIVE),
            reuse.cold_miss_ns as f64 / served / 1e6,
        );
        let one_shot = reuse.one_shot();
        println!(
            "  {:<12} {:>9} {:>6} {:>6} {:>7} {:>8} {:>8} {:>8} {:>21} {:>21} {:>21} {:>13}",
            "origin",
            "accesses",
            "hit",
            "cold",
            "-> cpu",
            "-> nvme",
            "-> gone",
            "stores",
            "gap hit p10/50/90 s",
            "gap evicted p10/50/90",
            "age at evict p10/50/90",
            "one-shot"
        );
        for origin in Origin::ALL {
            let row = &mut reuse.rows[origin.idx()];
            if row.accesses == 0 && row.stores == 0 {
                continue;
            }
            let n = row.accesses.max(1) as f64;
            let pct = |x: u64| 100.0 * x as f64 / n;
            println!(
                "  {:<12} {:>9} {:>5.1}% {:>5.1}% {:>6.1}% {:>7.1}% {:>7.1}% {:>8} {:>6.2}/{:>5.2}/{:>5.2} {:>6.2}/{:>5.2}/{:>5.2} {:>6.2}/{:>5.2}/{:>5.2} {:>6}/{:<6}",
                origin.label(),
                row.accesses,
                pct(row.hit),
                pct(row.cold),
                pct(row.evicted_to_offload),
                pct(row.evicted_to_spill),
                pct(row.evicted_gone),
                row.stores,
                secs(&mut row.gap_hit, 0.1),
                secs(&mut row.gap_hit, 0.5),
                secs(&mut row.gap_hit, 0.9),
                secs(&mut row.gap_evicted, 0.1),
                secs(&mut row.gap_evicted, 0.5),
                secs(&mut row.gap_evicted, 0.9),
                secs(&mut row.residency, 0.1),
                secs(&mut row.residency, 0.5),
                secs(&mut row.residency, 0.9),
                one_shot[origin.idx()].1,
                one_shot[origin.idx()].0,
            );
        }
    }
}

fn header(env: &Env) {
    println!(
        "what the router can do to memory it does not allocate (phase-5.md \u{a7}4.10)\n\
         cluster: {} nodes, {:.0} GiB HBM + {:.0} GiB DDR, {} req/s, {:.0}% fan-out, ops={}, seeds {} .. {}\n\
         arm: scored + fetch, flow-aware, unified control, no control crossing charged; the engine's \
         grants are sized from the ledger's own run at each regime\n\
         every cell is a difference from the same run with the mechanism off, one value per seed; \
         the one-node sections size soft floors per seed as `flows` does\n",
        env.nodes,
        gib(env.hbm),
        gib(env.dram),
        env.rate,
        100.0 * env.fanout,
        env.ops,
        env.seed,
        env.seed + env.seeds - 1,
    );
}

pub fn run(env: &Env) {
    header(env);
    let envs = env.lab_envs();
    let mut labs: Vec<Lab<'_>> = envs.iter().map(Lab::new).collect();
    let mut baselines = Baselines::default();
    let wanted: Vec<&str> = env.sections.split(',').map(str::trim).collect();
    let on = |name: &str| wanted.contains(&name);
    if on("gate") {
        gate(&mut labs);
    }
    if on("causes") {
        causes(&mut labs);
    }
    if on("flows") {
        flows(env);
    }
    if on("prefill") {
        prefill(&mut labs, &mut baselines);
    }
    if on("declared") {
        declared(&mut labs, &mut baselines);
    }
    if on("ceilings") {
        ceilings(&mut labs, &mut baselines);
    }
    if on("marks") {
        marks(&mut labs);
    }
    if on("retain") {
        retain(env);
    }
    if on("reuse") {
        reuse(&mut labs);
    }
}
