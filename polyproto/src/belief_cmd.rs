use std::collections::HashMap;

use polyphonic::boundary::Cost as Crossing;
use polyphonic::cache::NodeMemory;
use polyphonic::instruments::{BINS, Calibration};
use polyphonic::machine::{Control, Placement};
use polyphonic::topo::{Distance, Topology};

use super::{
    AdmitArg, Arm, ArmRun, BeliefArgs, ClusterBits, Correct, FleetArgs, InfluenceArgs, LoadArg,
    RecoveryArg, Scenario, ScoringArg, TraceKey, arm, correct, distributed_run, mean_of, ms,
    node_memory, quantile,
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
    pub sections: String,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(super) enum Regime {
    Defaults,
    Half,
}

impl Regime {
    pub(super) const ALL: [Self; 2] = [Self::Defaults, Self::Half];

    pub(super) fn label(self) -> &'static str {
        match self {
            Self::Defaults => "published defaults",
            Self::Half => "half partition, decode output held",
        }
    }

    pub(super) fn p3(self) -> Correct {
        match self {
            Self::Defaults => correct(false, AdmitArg::None),
            Self::Half => Correct {
                kv_scale: 0.5,
                ..correct(true, AdmitArg::None)
            },
        }
    }
}

const TAIL: f64 = 0.99;

pub(super) struct Lab<'a> {
    env: &'a Env,
    memory: NodeMemory,
    grants: HashMap<(u64, Regime, TraceKey), [u64; 3]>,
}

pub(super) fn scored_fetch() -> Arm {
    arm(
        "scored + fetch",
        Placement::Scored,
        true,
        Control::Unified,
        true,
    )
}

fn gossiped_scored() -> Arm {
    arm(
        "scored + fetch, gossiped",
        Placement::Scored,
        true,
        Control::Gossip { period: 200 },
        true,
    )
}

fn gossiped_greedy() -> Arm {
    arm(
        "both, gossiped",
        Placement::Aware,
        true,
        Control::Gossip { period: 200 },
        false,
    )
}

pub(super) fn on() -> BeliefArgs {
    BeliefArgs {
        belief: true,
        ..BeliefArgs::OFF
    }
}

fn exact() -> BeliefArgs {
    BeliefArgs {
        exact: true,
        ..on()
    }
}

impl<'a> Lab<'a> {
    pub(super) fn new(env: &'a Env) -> Self {
        let n = env.nodes as u64;
        Self {
            env,
            memory: node_memory(env.hbm / n, env.dram / n, env.nvme / n, [0, 1, 2, 1], false),
            grants: HashMap::new(),
        }
    }

    fn topo(&self, dist: Distance) -> Topology {
        let per_node = self.env.dram / self.env.nodes as u64;
        Topology::cluster(
            self.env.nodes,
            self.env.units_per_node,
            per_node,
            dist,
            Crossing::default(),
        )
    }

    fn scenario(
        &self,
        dist: Distance,
        p3: Correct,
        b: BeliefArgs,
        influence: InfluenceArgs,
        fleet: FleetArgs,
        regret: bool,
    ) -> Scenario {
        Scenario {
            cost: Crossing::default(),
            rate: self.env.rate,
            fanout: self.env.fanout,
            seed: self.env.seed,
            ops: self.env.ops,
            flow_payload: None,
            regret,
            p3,
            bits: ClusterBits {
                shared_l2: None,
                no_displacement: false,
            },
            belief: b,
            influence,
            fleet,
            lag_ns: dist.one_way_ns(),
        }
    }

    pub(super) fn go(
        &mut self,
        dist: Distance,
        regime: Regime,
        a: &Arm,
        b: BeliefArgs,
        regret: bool,
    ) -> ArmRun {
        self.go_with(dist, regime, a, b, InfluenceArgs::OFF, regret)
    }

    pub(super) fn go_with(
        &mut self,
        dist: Distance,
        regime: Regime,
        a: &Arm,
        b: BeliefArgs,
        influence: InfluenceArgs,
        regret: bool,
    ) -> ArmRun {
        self.go_fleet(dist, regime, a, b, influence, FleetArgs::OFF, regret)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn go_fleet(
        &mut self,
        dist: Distance,
        regime: Regime,
        a: &Arm,
        b: BeliefArgs,
        influence: InfluenceArgs,
        fleet: FleetArgs,
        regret: bool,
    ) -> ArmRun {
        let topo = self.topo(dist);
        let p3 = regime.p3();
        let trace = fleet.trace_only();
        let key = (dist.one_way_ns(), regime, trace.trace_key());
        if !self.grants.contains_key(&key) {
            let ledger = Correct {
                engine_cache: false,
                ..p3
            };
            let sc = self.scenario(
                dist,
                ledger,
                BeliefArgs::OFF,
                InfluenceArgs::OFF,
                trace,
                false,
            );
            let run = distributed_run(&scored_fetch(), &topo, self.memory, &sc);
            self.grants.insert(key.clone(), run.mach.kv_mean());
        }
        let mean = self.grants[&key];
        let sc = self.scenario(dist, p3, b, influence, fleet, regret);
        distributed_run(a, &topo, p3.engine_memory(self.memory, mean), &sc)
    }
}

struct Digest {
    service: f64,
    p99: f64,
    exec: f64,
    belief: f64,
    model: f64,
    exposed: f64,
    phantom: f64,
    miss: f64,
    gaps: [f64; 3],
    migrations: (u64, u64),
    churn: f64,
    silence: (f64, f64),
}

fn digest(r: &ArmRun) -> Digest {
    let all: Vec<u64> = r.t.samples.iter().flatten().copied().collect();
    let spans = r.mach.spans.len().max(1) as f64;
    let mean = |f: fn(&polyphonic::oracle::Regret) -> i64| {
        r.mach.spans.iter().map(|s| f(&s.regret)).sum::<i64>() as f64 / spans
    };
    let i = &r.mach.instruments;
    let gaps = i.gaps().max(1) as f64;
    let samples = i.divergence_samples.max(1) as f64;
    Digest {
        service: mean_of(&all),
        p99: ms(quantile(&all, TAIL)),
        exec: mean(|g| g.execution),
        belief: mean(|g| g.belief),
        model: mean(|g| g.model),
        exposed: 100.0 * i.exposed_any as f64 / i.decisions.max(1) as f64,
        phantom: 100.0 * i.phantom_share / samples,
        miss: 100.0 * i.miss_share / samples,
        gaps: [
            100.0 * i.gap_phantom as f64 / gaps,
            100.0 * i.gap_miss as f64 / gaps,
            100.0 * i.gap_discount as f64 / gaps,
        ],
        migrations: (i.needless_migrations, i.migrations),
        churn: 100.0 * i.churn as f64 / i.placed.max(1) as f64,
        silence: (
            100.0 * i.silence.inside_share(),
            100.0 * i.silence.outside_share(),
        ),
    }
}

fn delta(base: f64, x: f64) -> f64 {
    100.0 * (x - base) / base.abs().max(f64::MIN_POSITIVE)
}

fn header(env: &Env) {
    println!(
        "what routing costs when residency is a lossy belief (phase-4.md \u{a7}4.10)\n\
         cluster: {} nodes, {:.0} GiB HBM + {:.0} GiB DDR, {} req/s, {:.0}% fan-out, ops={} seed={}\n\
         arm: scored + fetch, flow-aware, unified control, no control crossing charged; the engine's \
         grants are sized from the ledger's own run at each distance and regime\n\
         cadence is the engine's step; lag is the one-way hop of the distance; service and p99 are over \
         every served request, ms\n",
        env.nodes,
        env.hbm as f64 / (1u64 << 30) as f64,
        env.dram as f64 / (1u64 << 30) as f64,
        env.rate,
        100.0 * env.fanout,
        env.ops,
        env.seed,
    );
}

fn section(title: &str) {
    println!("\n{title}");
}

fn gate(lab: &mut Lab<'_>) {
    section("1. the gate: an exact channel must change nothing");
    for regime in Regime::ALL {
        let plain = lab.go(
            Distance::Rack,
            regime,
            &scored_fetch(),
            BeliefArgs::OFF,
            false,
        );
        let seen = lab.go(Distance::Rack, regime, &scored_fetch(), exact(), false);
        let same = plain.total == seen.total
            && plain.served == seen.served
            && plain.t.service == seen.t.service
            && plain.t.stall == seen.t.stall
            && plain.t.warm == seen.t.warm
            && plain.mach.fetches == seen.mach.fetches
            && plain.mach.rebuilds == seen.mach.rebuilds;
        let i = &seen.mach.instruments;
        println!(
            "  {:<36} {}: {} requests served, {} KV decisions, exposure {}, divergence {:.3}%",
            regime.label(),
            if same { "PASS" } else { "FAIL" },
            seen.served,
            i.decisions,
            i.exposed_any,
            100.0 * (i.phantom_share + i.miss_share) / i.divergence_samples.max(1) as f64,
        );
    }
}

fn lag(lab: &mut Lab<'_>) {
    section("2. cadence and lag at loss zero (P1): the belief against the exact view");
    println!(
        "  {:<9} {:<36} {:>10} {:>9} {:>9} {:>14} {:>14} {:>9}",
        "distance",
        "regime",
        "service",
        "vs exact",
        "p99 vs",
        "belief ns/dec",
        "execution",
        "exposed"
    );
    for dist in [Distance::Rack, Distance::Zone, Distance::Region] {
        for regime in Regime::ALL {
            let base = digest(&lab.go(dist, regime, &scored_fetch(), BeliefArgs::OFF, true));
            let d = digest(&lab.go(dist, regime, &scored_fetch(), on(), true));
            println!(
                "  {:<9} {:<36} {:>8.3}ms {:>+8.3}% {:>+8.3}% {:>14.0} {:>14.0} {:>8.2}%",
                dist.label(),
                regime.label(),
                d.service,
                delta(base.service, d.service),
                delta(base.p99, d.p99),
                d.belief,
                d.exec - base.exec,
                d.exposed,
            );
        }
    }
}

fn rules() -> Vec<(String, ScoringArg, f64)> {
    vec![
        ("face-value".into(), ScoringArg::FaceValue, 0.9),
        ("expected".into(), ScoringArg::Expected, 0.9),
        ("quantile 0.5".into(), ScoringArg::Quantile, 0.5),
        ("quantile 0.9".into(), ScoringArg::Quantile, 0.9),
        ("quantile 0.99".into(), ScoringArg::Quantile, 0.99),
    ]
}

fn loss(lab: &mut Lab<'_>) {
    section("3. loss, recovery and the scoring rule (P2, P3, P9), rack");
    for regime in Regime::ALL {
        let base = digest(&lab.go(
            Distance::Rack,
            regime,
            &scored_fetch(),
            BeliefArgs::OFF,
            false,
        ));
        println!(
            "\n  {} (exact-view reference {:.3} ms, p99 {:.1} ms)",
            regime.label(),
            base.service,
            base.p99
        );
        println!(
            "  {:<32} {:>9} {:>9} {:>8} {:>8} {:>7} {:>16} {:>14} {:>7}",
            "loss, recovery, rule",
            "vs exact",
            "p99 vs",
            "exposed",
            "phantom",
            "miss",
            "gap P/M/D %",
            "needless migr",
            "churn"
        );
        let mut cells: Vec<(f64, RecoveryArg)> = vec![(0.0, RecoveryArg::Replay)];
        for l in [0.01, 0.05, 0.2] {
            for r in [
                RecoveryArg::Replay,
                RecoveryArg::Periodic,
                RecoveryArg::None,
            ] {
                cells.push((l, r));
            }
        }
        for (l, recovery) in cells {
            for (name, scoring, quantile) in rules() {
                let b = BeliefArgs {
                    loss: l,
                    recovery,
                    scoring,
                    quantile,
                    ..on()
                };
                let d = digest(&lab.go(Distance::Rack, regime, &scored_fetch(), b, true));
                println!(
                    "  {:<32} {:>+8.3}% {:>+8.3}% {:>7.2}% {:>7.3}% {:>6.3}% {:>5.0}/{:>4.0}/{:>4.0} {:>7}/{:<6} {:>6.1}%",
                    format!("{:.0}% {:?} {name}", 100.0 * l, recovery),
                    delta(base.service, d.service),
                    delta(base.p99, d.p99),
                    d.exposed,
                    d.phantom,
                    d.miss,
                    d.gaps[0],
                    d.gaps[1],
                    d.gaps[2],
                    d.migrations.0,
                    d.migrations.1,
                    d.churn,
                );
            }
        }
    }
}

fn silence(lab: &mut Lab<'_>) {
    section("4. a node going quiet (P4), rack, published defaults");
    let regime = Regime::Defaults;
    let loads = [
        ("the path", LoadArg::Path),
        ("the stream", LoadArg::Stream),
        ("stream + dispatches", LoadArg::StreamPlusDispatch),
    ];
    let rules = [
        ("face-value", ScoringArg::FaceValue),
        ("expected", ScoringArg::Expected),
        ("quantile 0.9", ScoringArg::Quantile),
    ];
    let quiet: Vec<Vec<Digest>> = loads
        .iter()
        .map(|&(_, load)| {
            rules
                .iter()
                .map(|&(_, scoring)| {
                    let b = BeliefArgs {
                        scoring,
                        load,
                        ..on()
                    };
                    digest(&lab.go(Distance::Rack, regime, &scored_fetch(), b, false))
                })
                .collect()
        })
        .collect();
    println!(
        "  reference, no silence, path and face-value: {:.3} ms, p99 {:.1} ms. each row is against \
         the same load source and rule with no silence. share is the silent node's share of KV \
         decisions: inside its episode / outside it\n",
        quiet[0][0].service, quiet[0][0].p99
    );
    println!(
        "  {:<12} {:<22} {:<14} {:>10} {:>9} {:>17}",
        "silence", "load from", "rule", "vs quiet", "p99 vs", "share in / out"
    );
    for seconds in [2.0, 8.0] {
        for (li, &(name, load)) in loads.iter().enumerate() {
            for (ri, &(rule, scoring)) in rules.iter().enumerate() {
                let b = BeliefArgs {
                    silence: seconds,
                    load,
                    scoring,
                    ..on()
                };
                let d = digest(&lab.go(Distance::Rack, regime, &scored_fetch(), b, false));
                let base = &quiet[li][ri];
                println!(
                    "  {:<12} {:<22} {:<14} {:>+9.3}% {:>+8.3}% {:>7.1}% / {:>5.1}%",
                    format!("{seconds:.0} s"),
                    name,
                    rule,
                    delta(base.service, d.service),
                    delta(base.p99, d.p99),
                    d.silence.0,
                    d.silence.1,
                );
            }
        }
    }
}

fn curve(title: &str, r: &ArmRun) {
    let i = &r.mach.instruments;
    println!(
        "\n  {title}: {} candidate predictions, {} at the chosen node",
        i.all.total(),
        i.chosen.total()
    );
    println!(
        "  {:<8} {:>9} {:>9} {:>9}   {:>9} {:>9} {:>9}",
        "bin", "n (all)", "predicted", "realised", "n (chosen)", "predicted", "realised"
    );
    for bin in 0..BINS {
        let (a, c) = (i.all.bins[bin], i.chosen.bins[bin]);
        if a.n == 0 && c.n == 0 {
            continue;
        }
        let cell = |b: polyphonic::instruments::Bin| {
            (
                b.predicted / b.n.max(1) as f64,
                b.resident as f64 / b.n.max(1) as f64,
            )
        };
        let (ap, ar) = cell(a);
        let (cp, cr) = cell(c);
        println!(
            "  {:<8} {:>9} {:>9.3} {:>9.3}   {:>10} {:>9.3} {:>9.3}",
            Calibration::label(bin),
            a.n,
            ap,
            ar,
            c.n,
            cp,
            cr
        );
    }
}

fn calibration(lab: &mut Lab<'_>) {
    section("5. P(resident) against realised residency (P3, P8), rack");
    for regime in Regime::ALL {
        println!("\n  {}", regime.label());
        for (name, l, recovery) in [
            ("5% loss, no recovery, face-value", 0.05, RecoveryArg::None),
            ("no loss, replay, face-value", 0.0, RecoveryArg::Replay),
        ] {
            let b = BeliefArgs {
                loss: l,
                recovery,
                ..on()
            };
            curve(
                name,
                &lab.go(Distance::Rack, regime, &scored_fetch(), b, false),
            );
        }
    }
}

fn gossip(lab: &mut Lab<'_>) {
    section(
        "6. the gossiped arms: engine state from the snapshot against from the channel (P5), rack",
    );
    println!(
        "  {:<28} {:<36} {:>11} {:>11} {:>9}",
        "arm", "regime", "snapshot", "channel", "change"
    );
    for a in [gossiped_scored(), gossiped_greedy()] {
        for regime in Regime::ALL {
            let snapshot = digest(&lab.go(Distance::Rack, regime, &a, BeliefArgs::OFF, false));
            let channel = digest(&lab.go(Distance::Rack, regime, &a, on(), false));
            println!(
                "  {:<28} {:<36} {:>9.3}ms {:>9.3}ms {:>+8.3}%",
                a.label,
                regime.label(),
                snapshot.service,
                channel.service,
                delta(snapshot.service, channel.service),
            );
        }
    }
}

fn observables(lab: &mut Lab<'_>) {
    section("7. RequestView: the score sees the observed mean output length (P6), rack");
    println!(
        "  {:<36} {:<20} {:>9} {:>9} {:>14} {:>14}",
        "regime", "belief", "service", "p99", "belief ns/dec", "model ns/dec"
    );
    for regime in Regime::ALL {
        for (name, base) in [
            ("off", BeliefArgs::OFF),
            ("5% loss, replay", BeliefArgs { loss: 0.05, ..on() }),
        ] {
            let mut rows = Vec::new();
            for seen in [false, true] {
                let b = BeliefArgs {
                    observables: seen,
                    ..base
                };
                rows.push(digest(&lab.go(
                    Distance::Rack,
                    regime,
                    &scored_fetch(),
                    b,
                    true,
                )));
            }
            for (i, d) in rows.iter().enumerate() {
                println!(
                    "  {:<36} {:<20} {:>7.3}ms {:>7.1}ms {:>14.0} {:>14.0}   {}",
                    regime.label(),
                    name,
                    d.service,
                    d.p99,
                    d.belief,
                    d.model,
                    if i == 0 {
                        "exact output length".to_string()
                    } else {
                        format!(
                            "observed mean ({:+.3}% mean, {:+.3}% p99)",
                            delta(rows[0].service, d.service),
                            delta(rows[0].p99, d.p99)
                        )
                    },
                );
            }
        }
    }
}

fn slo(lab: &mut Lab<'_>) {
    section("8. a declared SLO (P7), rack, published defaults, 30% of sessions throughput-bearing");
    println!(
        "  {:<26} {:<14} {:>22} {:>22} {:>22} {:>22}",
        "condition",
        "rule",
        "interactive service",
        "throughput service",
        "interactive stall",
        "throughput stall"
    );
    println!(
        "  {:<26} {:<14} {:>22} {:>22} {:>22} {:>22}",
        "", "", "mean / p99 ms", "mean / p99 ms", "mean / p99 ms", "mean / p99 ms"
    );
    let cell = |v: &[u64]| format!("{:>8.1} / {:>8.1}", mean_of(v), ms(quantile(v, TAIL)));
    for (name, l, recovery, seconds) in [
        ("5% loss, no recovery", 0.05, RecoveryArg::None, 0.0),
        ("2 s silence", 0.0, RecoveryArg::Replay, 2.0),
    ] {
        for (rule, scoring) in [
            ("expected", ScoringArg::Expected),
            ("quantile 0.9", ScoringArg::Quantile),
            ("slo", ScoringArg::Slo),
        ] {
            let b = BeliefArgs {
                loss: l,
                recovery,
                silence: seconds,
                scoring,
                throughput: 0.3,
                ..on()
            };
            let r = lab.go(Distance::Rack, Regime::Defaults, &scored_fetch(), b, false);
            println!(
                "  {:<26} {:<14} {:>22} {:>22} {:>22} {:>22}",
                name,
                rule,
                cell(&r.t.slo_service[0]),
                cell(&r.t.slo_service[1]),
                cell(&r.t.slo_stall[0]),
                cell(&r.t.slo_stall[1]),
            );
        }
    }
}

type Step = fn(&mut Lab<'_>);

pub fn run(env: &Env) {
    let mut lab = Lab::new(env);
    header(env);
    let wanted: Vec<&str> = env.sections.split(',').map(str::trim).collect();
    let steps: [(&str, Step); 8] = [
        ("gate", gate),
        ("lag", lag),
        ("loss", loss),
        ("silence", silence),
        ("calibration", calibration),
        ("gossip", gossip),
        ("observables", observables),
        ("slo", slo),
    ];
    for (name, step) in steps {
        if wanted.contains(&name) {
            step(&mut lab);
        }
    }
}
