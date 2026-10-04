use polyphonic::topo::Distance;

use super::belief_cmd::{Env as LabEnv, Lab, Regime, scored_fetch};
use super::influence_cmd::{identical, plain_seeds, section};
use super::{
    AdmitArg, ArmRun, BeliefArgs, CancelArg, CancelAtArg, EnforceArgs, EngineWaitArg, FleetArgs,
    InfluenceArgs, PrefillArg, QueueArg, VictimArg, ms, quantile,
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
    pub throughput: f64,
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
const BATCH: f64 = 0.05;
const SCALES: [f64; 4] = [1.0, 0.75, 0.6, 0.5];
const WAITS: [(&str, EngineWaitArg); 4] = [
    ("today's engine", EngineWaitArg::Off),
    ("waits, arrival order", EngineWaitArg::Fifo),
    ("waits, first fit", EngineWaitArg::FirstFit),
    ("waits, by class", EngineWaitArg::Priority),
];

fn fleet(prefill: bool) -> FleetArgs {
    if prefill {
        FleetArgs {
            prefill_time: PrefillArg::Priced,
            ..FleetArgs::OFF
        }
    } else {
        FleetArgs::OFF
    }
}

fn enforce(wait: EngineWaitArg, probe: bool) -> EnforceArgs {
    EnforceArgs {
        engine_wait: wait,
        probe_engine: probe,
        ..EnforceArgs::OFF
    }
}

fn go(env: &Env, lab: &mut Lab<'_>, scale: f64, prefill: bool, args: EnforceArgs) -> ArmRun {
    go_admitting(env, lab, scale, prefill, AdmitArg::None, args)
}

fn go_admitting(
    env: &Env,
    lab: &mut Lab<'_>,
    scale: f64,
    prefill: bool,
    admit: AdmitArg,
    args: EnforceArgs,
) -> ArmRun {
    lab.scale = Some(scale);
    lab.admit = Some(admit);
    lab.enforce = args;
    lab.go_fleet(
        Distance::Rack,
        Regime::Half,
        &scored_fetch(),
        BeliefArgs {
            throughput: if args.batch > 0.0 {
                0.0
            } else {
                env.throughput
            },
            ..BeliefArgs::OFF
        },
        InfluenceArgs::OFF,
        fleet(prefill),
        false,
    )
}

fn header(env: &Env) {
    println!(
        "what enforcement does to an engine that cannot place a sequence (phase-9.md \u{a7}4.12)\n\
         cluster: {} nodes, {:.0} GiB HBM + {:.0} GiB DDR, {} req/s, {:.0}% fan-out, {:.0}% of sessions \
         declare a throughput objective, ops={} seed={}\n\
         arm: scored + fetch, flow-aware, unified control, no control crossing charged, decode output \
         held; the partition is a multiple of the published grant, and the grants are sized from the \
         ledger's own run\n\
         stall is service less decode, the time before the first token; fan-out agents take today's \
         path and are in no class tail\n",
        env.nodes,
        env.hbm as f64 / (1u64 << 30) as f64,
        env.dram as f64 / (1u64 << 30) as f64,
        env.rate,
        100.0 * env.fanout,
        100.0 * env.throughput,
        env.ops,
        env.seed,
    );
}

fn gate_stream(env: &Env, labs: &mut [Lab<'_>]) {
    for scale in [4.0, 0.75] {
        let args = |stream_buffer| EnforceArgs {
            queue: QueueArg::Slo,
            engine_wait: EngineWaitArg::Priority,
            stream_buffer,
            ..EnforceArgs::OFF
        };
        let off: Vec<ArmRun> = labs
            .iter_mut()
            .map(|lab| go_admitting(env, lab, scale, false, AdmitArg::Perfect, args(false)))
            .collect();
        let on: Vec<ArmRun> = labs
            .iter_mut()
            .map(|lab| go_admitting(env, lab, scale, false, AdmitArg::Perfect, args(true)))
            .collect();
        println!(
            "  --stream-buffer at {scale}x the grant: {} against off",
            if off.iter().zip(&on).all(|(a, b)| alike(a, b)) {
                "identical"
            } else {
                "DIFFERS"
            }
        );
    }
}

fn gate_cancel(env: &Env, labs: &mut [Lab<'_>]) {
    let verdict = |same: bool| if same { "identical" } else { "DIFFERS" };
    for (cancel, victim) in [
        (CancelArg::Continue, VictimArg::Recent),
        (CancelArg::Drop, VictimArg::Remaining),
    ] {
        for admit in [AdmitArg::Perfect, AdmitArg::Tiered] {
            let args = |c| EnforceArgs {
                queue: QueueArg::Slo,
                cancel: c,
                victim,
                ..EnforceArgs::OFF
            };
            let off: Vec<ArmRun> = labs
                .iter_mut()
                .map(|lab| go_admitting(env, lab, 4.0, false, admit, args(CancelArg::Off)))
                .collect();
            let on: Vec<ArmRun> = labs
                .iter_mut()
                .map(|lab| go_admitting(env, lab, 4.0, false, admit, args(cancel)))
                .collect();
            let same = off.iter().zip(&on).all(|(a, b)| alike(a, b));
            println!(
                "  --cancel {cancel:?} {victim:?} with --admit {admit:?} at 4x the grant: {} against no cancel, nothing cancelled: {}",
                verdict(same),
                on.iter().all(|r| r.mach.cancel_stats.cancels == 0),
            );
        }
    }
}

fn gate(env: &Env, labs: &mut [Lab<'_>]) {
    section("1. the gate: each bit must change nothing where it has nothing to change");
    let verdict = |same: bool| if same { "identical" } else { "DIFFERS" };
    for prefill in [false, true] {
        let off: Vec<ArmRun> = labs
            .iter_mut()
            .map(|lab| go(env, lab, 4.0, prefill, enforce(EngineWaitArg::Off, false)))
            .collect();
        for wait in [
            EngineWaitArg::Fifo,
            EngineWaitArg::FirstFit,
            EngineWaitArg::Priority,
        ] {
            let on: Vec<ArmRun> = labs
                .iter_mut()
                .map(|lab| go(env, lab, 4.0, prefill, enforce(wait, false)))
                .collect();
            let same = off.iter().zip(&on).all(|(a, b)| identical(a, b));
            println!(
                "  --engine-wait {wait:?} at 4x the grant, prefill {}: {} against off, no sequence queued: {}",
                if prefill {
                    "taking engine time"
                } else {
                    "free"
                },
                verdict(same),
                on.iter().all(|r| r.mach.engine_waits.queued == 0),
            );
        }
    }
    for queue in [QueueArg::Fifo, QueueArg::Slo, QueueArg::Plas] {
        for admit in [AdmitArg::Perfect, AdmitArg::Quantile, AdmitArg::Tiered] {
            let off: Vec<ArmRun> = labs
                .iter_mut()
                .map(|lab| go_admitting(env, lab, 4.0, false, admit, EnforceArgs::OFF))
                .collect();
            let on: Vec<ArmRun> = labs
                .iter_mut()
                .map(|lab| {
                    go_admitting(
                        env,
                        lab,
                        4.0,
                        false,
                        admit,
                        EnforceArgs {
                            queue,
                            ..EnforceArgs::OFF
                        },
                    )
                })
                .collect();
            let same = off.iter().zip(&on).all(|(a, b)| identical(a, b));
            println!(
                "  --queue {queue:?} with --admit {admit:?} at 4x the grant: {} against no queue, nothing queued: {}",
                verdict(same),
                on.iter().all(|r| r.mach.queue_waits.queued == 0),
            );
        }
    }
    gate_cancel(env, labs);
    gate_stream(env, labs);
    for scale in [0.75, 0.5] {
        let plain: Vec<ArmRun> = labs
            .iter_mut()
            .map(|lab| go(env, lab, scale, false, enforce(EngineWaitArg::Off, false)))
            .collect();
        let probed: Vec<ArmRun> = labs
            .iter_mut()
            .map(|lab| go(env, lab, scale, false, enforce(EngineWaitArg::Off, true)))
            .collect();
        let same = plain.iter().zip(&probed).all(|(a, b)| identical(a, b));
        println!(
            "  --probe-engine at {scale}x the grant: {} against off",
            verdict(same)
        );
    }
}

fn tail(values: &[u64]) -> f64 {
    ms(quantile(values, TAIL))
}

fn engine(env: &Env, labs: &mut [Lab<'_>]) {
    section("2. the engine (P1): what today's model gives away, and what waiting costs");
    for prefill in [false, true] {
        for scale in SCALES {
            println!(
                "\n  {scale}x the published grant, prefill {}",
                if prefill {
                    "taking engine time"
                } else {
                    "free"
                }
            );
            println!(
                "  {:<22} {:>24} {:>24} {:>24} {:>36} {:>30}",
                "",
                "interactive service p99",
                "interactive stall p99",
                "throughput stall p99",
                "preempted | queued, % of requests",
                "mean wait ms, int | thr",
            );
            for (label, wait) in WAITS {
                let runs: Vec<ArmRun> = labs
                    .iter_mut()
                    .map(|lab| {
                        go(
                            env,
                            lab,
                            scale,
                            prefill,
                            enforce(wait, wait == EngineWaitArg::Off),
                        )
                    })
                    .collect();
                let col = |f: &dyn Fn(&ArmRun) -> f64| -> Vec<f64> { runs.iter().map(f).collect() };
                let offered = |r: &ArmRun| r.offered.max(1) as f64;
                let mean_wait = |r: &ArmRun, i: usize| {
                    let w = &r.mach.engine_waits;
                    w.waited_ns[i] as f64 / w.waited[i].max(1) as f64 / 1e6
                };
                println!(
                    "  {label:<22} {:>24} {:>24} {:>24} {:>36} {:>30}",
                    plain_seeds(&col(&|r| tail(&r.t.slo_service[0])), 0, ""),
                    plain_seeds(&col(&|r| tail(&r.t.slo_stall[0])), 0, ""),
                    plain_seeds(&col(&|r| tail(&r.t.slo_stall[1])), 0, ""),
                    format!(
                        "{} | {}",
                        plain_seeds(
                            &col(&|r| 100.0 * r.mach.preempted.iter().sum::<u64>() as f64
                                / offered(r)),
                            1,
                            ""
                        ),
                        plain_seeds(
                            &col(&|r| 100.0 * r.mach.engine_waits.queued as f64 / offered(r)),
                            1,
                            ""
                        ),
                    ),
                    format!(
                        "{} | {}",
                        plain_seeds(&col(&|r| mean_wait(r, 0)), 0, ""),
                        plain_seeds(&col(&|r| mean_wait(r, 1)), 0, "")
                    ),
                );
                if wait == EngineWaitArg::Off {
                    let probe = |r: &ArmRun| {
                        let w = &r.mach.engine_waits;
                        let mut v = w.probe_ns.clone();
                        (
                            v.len(),
                            ms(polyphonic::instruments::percentile(&mut v, 0.5).unwrap_or(0)),
                            ms(polyphonic::instruments::percentile(&mut v, TAIL).unwrap_or(0)),
                        )
                    };
                    let ps: Vec<(usize, f64, f64)> = runs.iter().map(probe).collect();
                    println!(
                        "  {:<22} the old engine ran {} sequences it could not place; the wait they \
                         would have had: median {} ms, p99 {} ms",
                        "",
                        ps.iter()
                            .map(|p| p.0.to_string())
                            .collect::<Vec<_>>()
                            .join(" / "),
                        plain_seeds(&ps.iter().map(|p| p.1).collect::<Vec<_>>(), 1, ""),
                        plain_seeds(&ps.iter().map(|p| p.2).collect::<Vec<_>>(), 1, ""),
                    );
                }
            }
        }
    }
}

struct Arm {
    label: &'static str,
    admit: AdmitArg,
    args: EnforceArgs,
}

fn arm(label: &'static str, admit: AdmitArg, wait: EngineWaitArg, queue: QueueArg) -> Arm {
    let engine_wait = match (wait, queue) {
        (EngineWaitArg::Off, QueueArg::Slo) => EngineWaitArg::Priority,
        (EngineWaitArg::Off, _) => EngineWaitArg::Fifo,
        (given, _) => given,
    };
    Arm {
        label,
        admit,
        args: EnforceArgs {
            engine_wait,
            queue,
            ..EnforceArgs::OFF
        },
    }
}

impl Arm {
    fn cancelling(mut self, cancel: CancelArg, victim: VictimArg) -> Self {
        self.args.cancel = cancel;
        self.args.victim = victim;
        self
    }

    fn at(mut self, cancel_at: CancelAtArg) -> Self {
        self.args.cancel_at = cancel_at;
        self
    }

    fn with_batch(mut self) -> Self {
        self.args.batch = BATCH;
        self
    }

    fn pooled(mut self) -> Self {
        self.args.pooled = true;
        self
    }

    fn gated(mut self, share: f64) -> Self {
        self.args.gate = share;
        self
    }
}

fn alike(a: &ArmRun, b: &ArmRun) -> bool {
    let sorted = |r: &ArmRun| {
        r.t.samples
            .iter()
            .map(|v| {
                let mut v = v.clone();
                v.sort_unstable();
                v
            })
            .collect::<Vec<_>>()
    };
    sorted(a) == sorted(b) && a.total == b.total && a.served == b.served
}

fn table(env: &Env, labs: &mut [Lab<'_>], scale: f64, prefill: bool, arms: &[Arm]) {
    println!(
        "\n  {scale}x the published grant, prefill {}",
        if prefill {
            "taking engine time"
        } else {
            "free"
        }
    );
    println!(
        "  {:<40} {:>20} {:>22} {:>22} {:>22} {:>22} {:>20} {:>34} {:>16} {:>16}",
        "",
        "not served, %",
        "interactive stall p99",
        "interactive service p99",
        "throughput stall p99",
        "throughput service p99",
        "queued at router, %",
        "decodes cancelled, %",
        "aborts, by engine",
        "wasted decode, s",
    );
    for a in arms {
        let runs: Vec<ArmRun> = labs
            .iter_mut()
            .map(|lab| go_admitting(env, lab, scale, prefill, a.admit, a.args))
            .collect();
        let col = |f: &dyn Fn(&ArmRun) -> f64| -> Vec<f64> { runs.iter().map(f).collect() };
        let offered = |r: &ArmRun| r.offered.max(1) as f64;
        println!(
            "  {:<40} {:>20} {:>22} {:>22} {:>22} {:>22} {:>20} {:>34} {:>16} {:>16}",
            a.label,
            plain_seeds(
                &col(&|r| 100.0 * (1.0 - r.served as f64 / offered(r))),
                2,
                ""
            ),
            plain_seeds(&col(&|r| tail(&r.t.slo_stall[0])), 0, ""),
            plain_seeds(&col(&|r| tail(&r.t.slo_service[0])), 0, ""),
            plain_seeds(&col(&|r| tail(&r.t.slo_stall[1])), 0, ""),
            plain_seeds(&col(&|r| tail(&r.t.slo_service[1])), 0, ""),
            plain_seeds(
                &col(&|r| 100.0 * r.mach.queue_waits.queued as f64 / offered(r)),
                1,
                ""
            ),
            plain_seeds(
                &col(&|r| {
                    let decodes = r.t.slo_service[0].len() + r.t.slo_service[1].len();
                    100.0 * r.mach.cancelled_requests() as f64 / decodes.max(1) as f64
                }),
                1,
                ""
            ),
            format!(
                "{} , {}",
                plain_seeds(&col(&|r| r.mach.cancel_stats.cancels as f64), 0, ""),
                plain_seeds(&col(&|r| r.mach.cancel_stats.engine_cancels as f64), 0, ""),
            ),
            plain_seeds(
                &col(&|r| r.mach.cancel_stats.wasted_decode_ns as f64 / 1e9),
                0,
                ""
            ),
        );
    }
}

const REGIMES: [(f64, bool); 5] = [
    (0.75, false),
    (0.75, true),
    (0.6, false),
    (0.6, true),
    (0.5, false),
];

fn cancel(env: &Env, labs: &mut [Lab<'_>]) {
    section(
        "5. the cancel (P4, and P9's cancel half): where the overcommit's tail lands once the router can choose the victim",
    );
    let tiered = |label| arm(label, AdmitArg::Tiered, EngineWaitArg::Off, QueueArg::Slo);
    let attained = |label| {
        arm(
            label,
            AdmitArg::Quantile,
            EngineWaitArg::Off,
            QueueArg::Plas,
        )
        .pooled()
    };
    let arms = [
        arm(
            "perfect, refusing",
            AdmitArg::Perfect,
            EngineWaitArg::Off,
            QueueArg::Off,
        ),
        tiered("tiered, by class, no cancel"),
        tiered("tiered, by class, cancel").cancelling(CancelArg::Continue, VictimArg::Recent),
        attained("p90 pooled, by attained, no cancel"),
        attained("p90 pooled, by attained, cancel most attained")
            .cancelling(CancelArg::Continue, VictimArg::Attained),
    ];
    for (scale, prefill) in REGIMES {
        table(env, labs, scale, prefill, &arms);
    }
}

fn claims(env: &Env, labs: &mut [Lab<'_>]) {
    section(
        "6. the claims (P5, on the published workload): one quantile of the pooled lengths against two tiers, under a cancel",
    );
    let by_class = |label, admit| arm(label, admit, EngineWaitArg::Off, QueueArg::Slo);
    let arms = [
        by_class("p90 pooled, by class, cancel", AdmitArg::Quantile)
            .pooled()
            .cancelling(CancelArg::Continue, VictimArg::Recent),
        by_class("p90 own class, by class, cancel", AdmitArg::Quantile)
            .cancelling(CancelArg::Continue, VictimArg::Recent),
        by_class("tiered, by class, cancel", AdmitArg::Tiered)
            .cancelling(CancelArg::Continue, VictimArg::Recent),
        by_class("perfect, by class, no cancel", AdmitArg::Perfect),
    ];
    for (scale, prefill) in [(0.75, true), (0.6, false), (0.6, true), (0.5, false)] {
        table(env, labs, scale, prefill, &arms);
    }
}

fn restart(env: &Env, labs: &mut [Lab<'_>]) {
    section("7. continuation against restart (P7): what the path adds when it relayed the tokens");
    let tiered = |label| arm(label, AdmitArg::Tiered, EngineWaitArg::Off, QueueArg::Slo);
    let arms = [
        tiered("tiered, cancel, continuation").cancelling(CancelArg::Continue, VictimArg::Recent),
        tiered("tiered, cancel, restart").cancelling(CancelArg::Drop, VictimArg::Recent),
        tiered("tiered, cancel, continuation, most left")
            .cancelling(CancelArg::Continue, VictimArg::Remaining),
    ];
    for (scale, prefill) in REGIMES {
        table(env, labs, scale, prefill, &arms);
    }
}

fn llmd(env: &Env, labs: &mut [Lab<'_>]) {
    section(
        "8. the llm-d-shaped arm (P8): a utilisation gate, eviction of the newest lower-class sequence, a restart",
    );
    let gate = |label, share| {
        arm(label, AdmitArg::Gate, EngineWaitArg::Off, QueueArg::Slo)
            .cancelling(CancelArg::Drop, VictimArg::Recent)
            .gated(share)
    };
    let arms = [
        arm(
            "p90 own class, cancel, continuation",
            AdmitArg::Quantile,
            EngineWaitArg::Off,
            QueueArg::Slo,
        )
        .cancelling(CancelArg::Continue, VictimArg::Recent),
        gate("gate 0.95, evict, restart", 0.95),
        gate("gate 0.9, evict, restart", 0.9),
        gate("gate 0.8, evict, restart", 0.8),
    ];
    for (scale, prefill) in [(0.75, false), (0.75, true), (0.6, false), (0.6, true)] {
        table(env, labs, scale, prefill, &arms);
    }
}

fn batch(env: &Env, labs: &mut [Lab<'_>]) {
    section(
        "9. a throughput class with a shape (P5 to P9, the batch halves): every session interactive, 5% of requests a batch request",
    );
    let by_class = |label, admit| arm(label, admit, EngineWaitArg::Off, QueueArg::Slo).with_batch();
    let cancelling = |a: Arm| a.cancelling(CancelArg::Continue, VictimArg::Recent);
    let arms = [
        by_class("perfect, by class, no cancel", AdmitArg::Perfect),
        by_class("p90 pooled, by class, no cancel", AdmitArg::Quantile).pooled(),
        by_class("p90 own class, by class, no cancel", AdmitArg::Quantile),
        by_class("tiered, by class, no cancel", AdmitArg::Tiered),
        cancelling(by_class("p90 own class, cancel", AdmitArg::Quantile)),
        cancelling(by_class(
            "tiered, cancel at the router only",
            AdmitArg::Tiered,
        ))
        .at(CancelAtArg::Router),
        cancelling(by_class("tiered, cancel at both", AdmitArg::Tiered)),
        by_class("tiered, cancel at both, restart", AdmitArg::Tiered)
            .cancelling(CancelArg::Drop, VictimArg::Recent),
        by_class("gate 0.9, evict, restart", AdmitArg::Gate)
            .cancelling(CancelArg::Drop, VictimArg::Recent)
            .gated(0.9),
        arm(
            "p90 own class, arrival order",
            AdmitArg::Quantile,
            EngineWaitArg::Off,
            QueueArg::Fifo,
        )
        .with_batch(),
        arm(
            "p90 own class, by attained service",
            AdmitArg::Quantile,
            EngineWaitArg::Off,
            QueueArg::Plas,
        )
        .with_batch(),
    ];
    for (scale, prefill) in [(1.0, true), (0.75, true), (0.75, false), (0.6, true)] {
        table(env, labs, scale, prefill, &arms);
    }
}

fn departures(env: &Env, labs: &mut [Lab<'_>]) {
    section(
        "10. a client that leaves (P10): the sequence aborted at the next arrival, or left to run",
    );
    for (title, queue, admit, wait, regimes) in [
        (
            "the engine's own wait, nothing reserved",
            QueueArg::Off,
            AdmitArg::None,
            EngineWaitArg::Fifo,
            vec![(0.75, true), (0.6, true), (0.6, false)],
        ),
        (
            "the router queue in class order, a p90 of each class's own lengths",
            QueueArg::Slo,
            AdmitArg::Quantile,
            EngineWaitArg::Priority,
            vec![(0.6, false), (0.6, true)],
        ),
    ] {
        println!("\n  {title}");
        for (scale, prefill) in regimes {
            println!(
                "\n  {scale}x the published grant, prefill {}",
                if prefill {
                    "taking engine time"
                } else {
                    "free"
                }
            );
            println!(
                "  {:<30} {:>20} {:>20} {:>24} {:>24} {:>20} {:>16}",
                "",
                "served, %",
                "departed, %",
                "interactive stall p99",
                "throughput stall p99",
                "leaked decode, s",
                "freed early, s",
            );
            for (label, share, leak) in [
                ("nobody leaves", 0.0, false),
                ("5% leave, aborted", 0.05, false),
                ("5% leave, leaked", 0.05, true),
                ("20% leave, aborted", 0.20, false),
                ("20% leave, leaked", 0.20, true),
            ] {
                let args = EnforceArgs {
                    engine_wait: wait,
                    queue,
                    disconnect: share,
                    leak,
                    ..EnforceArgs::OFF
                };
                let runs: Vec<ArmRun> = labs
                    .iter_mut()
                    .map(|lab| go_admitting(env, lab, scale, prefill, admit, args))
                    .collect();
                let col = |f: &dyn Fn(&ArmRun) -> f64| -> Vec<f64> { runs.iter().map(f).collect() };
                let offered = |r: &ArmRun| r.offered.max(1) as f64;
                println!(
                    "  {label:<30} {:>20} {:>20} {:>24} {:>24} {:>20} {:>16}",
                    plain_seeds(&col(&|r| 100.0 * r.served as f64 / offered(r)), 2, ""),
                    plain_seeds(&col(&|r| 100.0 * r.t.departed as f64 / offered(r)), 2, ""),
                    plain_seeds(&col(&|r| tail(&r.t.slo_stall[0])), 0, ""),
                    plain_seeds(&col(&|r| tail(&r.t.slo_stall[1])), 0, ""),
                    plain_seeds(
                        &col(&|r| r.mach.departure_stats.leaked_ns as f64 / 1e9),
                        0,
                        ""
                    ),
                    plain_seeds(
                        &col(&|r| r.mach.departure_stats.freed_ns as f64 / 1e9),
                        0,
                        ""
                    ),
                );
            }
        }
    }
}

fn buffer(env: &Env, labs: &mut [Lab<'_>]) {
    section(
        "11. the stalled-stream buffer (P11): tokens emitted by decodes in flight, if every stream stalled",
    );
    let node_ddr = env.dram as f64 / env.nodes as f64;
    println!(
        "\n  {:<44} {:>26} {:>26} {:>16} {:>14}",
        "", "peak tokens, busiest node", "mean tokens, a node", "peak MB", "of a node's DDR, %"
    );
    for (label, batch) in [("published workload", 0.0), ("with the batch class", BATCH)] {
        for (scale, prefill) in [(1.0, false), (0.75, true), (0.6, true)] {
            let args = EnforceArgs {
                queue: QueueArg::Slo,
                engine_wait: EngineWaitArg::Priority,
                stream_buffer: true,
                batch,
                ..EnforceArgs::OFF
            };
            let runs: Vec<ArmRun> = labs
                .iter_mut()
                .map(|lab| go_admitting(env, lab, scale, prefill, AdmitArg::Perfect, args))
                .collect();
            let col = |f: &dyn Fn(&ArmRun) -> f64| -> Vec<f64> { runs.iter().map(f).collect() };
            let peak =
                |r: &ArmRun| r.mach.stream.peak_tokens.iter().copied().max().unwrap_or(0) as f64;
            let mean = |r: &ArmRun| {
                let s = &r.mach.stream;
                s.sum_tokens.iter().sum::<u64>() as f64
                    / (s.samples.max(1) as f64 * s.sum_tokens.len().max(1) as f64)
            };
            let bytes = polyphonic::machine::STREAM_BYTES_PER_TOKEN as f64;
            println!(
                "  {:<44} {:>26} {:>26} {:>16} {:>14}",
                format!(
                    "{label}, {scale}x{}",
                    if prefill { ", prefill" } else { "" }
                ),
                plain_seeds(&col(&peak), 0, ""),
                plain_seeds(&col(&mean), 0, ""),
                plain_seeds(&col(&|r| peak(r) * bytes / 1e6), 2, ""),
                plain_seeds(&col(&|r| 100.0 * peak(r) * bytes / node_ddr), 3, ""),
            );
        }
    }
}

fn queue(env: &Env, labs: &mut [Lab<'_>]) {
    section(
        "3. refusal against a queue (P2): what a refusal removes from the tail it is measured on",
    );
    let arms = [
        arm(
            "none, the engine waits",
            AdmitArg::None,
            EngineWaitArg::Fifo,
            QueueArg::Off,
        ),
        arm(
            "perfect, refusing",
            AdmitArg::Perfect,
            EngineWaitArg::Off,
            QueueArg::Off,
        ),
        arm(
            "perfect, queue in arrival order",
            AdmitArg::Perfect,
            EngineWaitArg::Off,
            QueueArg::Fifo,
        ),
    ];
    for (scale, prefill) in [
        (0.75, false),
        (0.75, true),
        (0.6, false),
        (0.6, true),
        (0.5, false),
    ] {
        table(env, labs, scale, prefill, &arms);
    }
}

fn order(env: &Env, labs: &mut [Lab<'_>]) {
    section(
        "4. the order (P3, and P9's order half): the queue's order against the claim it admits by",
    );
    let arms = [
        arm(
            "none, the engine waits, in order",
            AdmitArg::None,
            EngineWaitArg::Fifo,
            QueueArg::Off,
        ),
        arm(
            "none, the engine waits, by class",
            AdmitArg::None,
            EngineWaitArg::Priority,
            QueueArg::Off,
        ),
        arm(
            "perfect, arrival order",
            AdmitArg::Perfect,
            EngineWaitArg::Off,
            QueueArg::Fifo,
        ),
        arm(
            "perfect, by class",
            AdmitArg::Perfect,
            EngineWaitArg::Off,
            QueueArg::Slo,
        ),
        arm(
            "p90 pooled, arrival order",
            AdmitArg::Quantile,
            EngineWaitArg::Off,
            QueueArg::Fifo,
        ),
        arm(
            "p90 pooled, by class",
            AdmitArg::Quantile,
            EngineWaitArg::Off,
            QueueArg::Slo,
        ),
        arm(
            "p90 pooled, by attained service",
            AdmitArg::Quantile,
            EngineWaitArg::Off,
            QueueArg::Plas,
        ),
        arm(
            "tiered, by class",
            AdmitArg::Tiered,
            EngineWaitArg::Off,
            QueueArg::Slo,
        ),
    ];
    for (scale, prefill) in [(0.75, false), (0.6, false), (0.6, true)] {
        table(env, labs, scale, prefill, &arms);
    }
}

pub fn run(env: &Env) {
    header(env);
    let envs = env.lab_envs();
    let mut labs: Vec<Lab<'_>> = envs.iter().map(Lab::new).collect();
    let wanted: Vec<&str> = env.sections.split(',').map(str::trim).collect();
    let on = |name: &str| wanted.contains(&name);
    if on("gate") {
        gate(env, &mut labs);
    }
    if on("engine") {
        engine(env, &mut labs);
    }
    if on("queue") {
        queue(env, &mut labs);
    }
    if on("order") {
        order(env, &mut labs);
    }
    if on("cancel") {
        cancel(env, &mut labs);
    }
    if on("claims") {
        claims(env, &mut labs);
    }
    if on("restart") {
        restart(env, &mut labs);
    }
    if on("llmd") {
        llmd(env, &mut labs);
    }
    if on("batch") {
        batch(env, &mut labs);
    }
    if on("departures") {
        departures(env, &mut labs);
    }
    if on("buffer") {
        buffer(env, &mut labs);
    }
}
