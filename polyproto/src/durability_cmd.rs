use std::path::PathBuf;

use polyphonic::durable::{self, Commit};
use polyphonic::fault::{
    Client, Degrade, EngineCrash, Estimators, Fate, Fault, NodeLoss, Rebuild, Restart, Retry,
    Spill, Subscriber,
};
use polyphonic::programs::{Config, LogCause, Preset, ShapeConfig, Suspend, run};
use polyphonic::topo::Distance;
use polyphonic::work::{FAAS_EXEC_MIN_NS, FAAS_EXEC_SPAN_NS};
use polyphonic::writes::{KV_EVENT_LABELS, LEASE_RENEW_S, Writes, liveness_writes};

use super::belief_cmd::{Env as LabEnv, Lab, Regime, scored_fetch};
use super::influence_cmd::{plain_seeds, section};
use super::{
    AdmitArg, ArmRun, BeliefArgs, CancelArg, Detail, EnforceArgs, EngineWaitArg, FaultPlan,
    FleetArgs, InfluenceArgs, ObserveArg, PrefillArg, QueueArg, VictimArg,
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
    pub scales: Vec<f64>,
    pub fleet_nodes: u64,
    pub lease_renew: f64,
    pub reps: usize,
    pub dir: PathBuf,
    pub sections: String,
}

impl Env {
    fn lab_envs(&self) -> Vec<LabEnv> {
        self.lab_envs_of(self.seeds)
    }

    fn lab_envs_of(&self, seeds: u64) -> Vec<LabEnv> {
        (self.seed..self.seed + seeds)
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

const FDB_WRITES_PER_CORE: f64 = 20_000.0;
const FDB_CLUSTER_OPS: f64 = 8_200_000.0;
const FDB_CLUSTER_CORES: f64 = 384.0;
const FDB_CLUSTER_WRITE_SHARE: f64 = 0.10;
const FDB_COMMIT_MS: (f64, f64) = (1.5, 2.5);
const PLP_FSYNC_US: (f64, f64) = (1.6, 12.4);
const CONSUMER_FSYNC_US: (f64, f64) = (891.1, 2974.2);
const AGENT_TURN_NS: f64 = 1e9;

fn enforcing(count: bool) -> EnforceArgs {
    EnforceArgs {
        engine_wait: EngineWaitArg::Priority,
        queue: QueueArg::Slo,
        cancel: CancelArg::Continue,
        victim: VictimArg::Recent,
        count_writes: count,
        ..EnforceArgs::OFF
    }
}

fn published(lab: &mut Lab<'_>, count: bool) -> ArmRun {
    lab.scale = None;
    lab.admit = None;
    lab.enforce = EnforceArgs {
        count_writes: count,
        ..EnforceArgs::OFF
    };
    lab.go_fleet(
        Distance::Rack,
        Regime::Defaults,
        &scored_fetch(),
        BeliefArgs::OFF,
        InfluenceArgs::OFF,
        FleetArgs::OFF,
        false,
    )
}

fn enforced(lab: &mut Lab<'_>, env: &Env, scale: f64, count: bool) -> ArmRun {
    enforced_with(lab, env, scale, AdmitArg::Quantile, enforcing(count))
}

fn enforced_with(
    lab: &mut Lab<'_>,
    env: &Env,
    scale: f64,
    admit: AdmitArg,
    enforce: EnforceArgs,
) -> ArmRun {
    enforced_in(lab, env, scale, false, admit, enforce)
}

fn enforced_in(
    lab: &mut Lab<'_>,
    env: &Env,
    scale: f64,
    prefill: bool,
    admit: AdmitArg,
    enforce: EnforceArgs,
) -> ArmRun {
    lab.scale = Some(scale);
    lab.admit = Some(admit);
    lab.enforce = enforce;
    lab.go_fleet(
        Distance::Rack,
        Regime::Half,
        &scored_fetch(),
        BeliefArgs {
            throughput: env.throughput,
            ..BeliefArgs::OFF
        },
        InfluenceArgs::OFF,
        if prefill {
            FleetArgs {
                prefill_time: PrefillArg::Priced,
                ..FleetArgs::OFF
            }
        } else {
            FleetArgs::OFF
        },
        false,
    )
}

fn alike(a: &ArmRun, b: &ArmRun) -> bool {
    a.total == b.total
        && a.served == b.served
        && a.t.service == b.t.service
        && a.t.stall == b.t.stall
        && a.mach.writes().owned() == b.mach.writes().owned()
}

fn same_costs(a: &ArmRun, b: &ArmRun) -> bool {
    a.total == b.total
        && a.served == b.served
        && a.t.service == b.t.service
        && a.t.stall == b.t.stall
}

fn verdict(same: bool) -> &'static str {
    if same { "identical" } else { "DIFFERS" }
}

fn gate(env: &Env, labs: &mut [Lab<'_>]) {
    section("1. the gate: counting writes must change nothing it does not count");
    for scale in [Some(1.0), Some(0.75), None] {
        let mut off = Vec::new();
        let mut on = Vec::new();
        for lab in labs.iter_mut() {
            if let Some(s) = scale {
                off.push(enforced(lab, env, s, false));
                on.push(enforced(lab, env, s, true));
            } else {
                off.push(published(lab, false));
                on.push(published(lab, true));
            }
        }
        let same = off.iter().zip(&on).all(|(a, b)| alike(a, b));
        let silent = off.iter().all(|r| r.mach.writes().kv_total() == 0);
        let counted = on.iter().all(|r| r.mach.writes().kv_total() > 0);
        println!(
            "  --count-writes {}: {} against off; KV events counted only when on: {}",
            scale.map_or_else(
                || "on the published ledger run".to_string(),
                |s| format!("at {s}x of the grant")
            ),
            verdict(same),
            silent && counted,
        );
    }
    gate_faults(env, labs);
}

type Row = fn(&Writes) -> u64;

fn gate_faults(env: &Env, labs: &mut [Lab<'_>]) {
    let armed = EnforceArgs {
        track_flights: true,
        ..enforcing(false)
    };
    let past_the_end = Some(FaultPlan {
        at_request: usize::MAX,
        fault: Fault::Estimators,
        retry: Retry::Burst,
    });
    for scale in [1.0, 0.75] {
        let plain: Vec<ArmRun> = labs
            .iter_mut()
            .map(|l| enforced_with(l, env, scale, AdmitArg::Quantile, enforcing(false)))
            .collect();
        let tracked: Vec<ArmRun> = labs
            .iter_mut()
            .map(|l| enforced_with(l, env, scale, AdmitArg::Quantile, armed))
            .collect();
        println!(
            "  --track-flights at {scale}x of the grant: {} against off",
            verdict(plain.iter().zip(&tracked).all(|(a, b)| same_costs(a, b)))
        );
        let late: Vec<ArmRun> = labs
            .iter_mut()
            .map(|l| {
                l.fault = past_the_end;
                let run = enforced_with(l, env, scale, AdmitArg::Quantile, armed);
                l.fault = None;
                run
            })
            .collect();
        println!(
            "  a fault past the trace's end at {scale}x of the grant: {} against no fault",
            verdict(tracked.iter().zip(&late).all(|(a, b)| alike(a, b)))
        );
    }
    for scale in [1.0, 0.75] {
        let tracked: Vec<ArmRun> = labs
            .iter_mut()
            .map(|l| enforced_with(l, env, scale, AdmitArg::Quantile, armed))
            .collect();
        let checking = EnforceArgs {
            node_check: true,
            snapshot_estimators: 10.0,
            copy_durable: true,
            ..armed
        };
        let with_checks: Vec<ArmRun> = labs
            .iter_mut()
            .map(|l| enforced_with(l, env, scale, AdmitArg::Quantile, checking))
            .collect();
        println!(
            "  --node-check, --snapshot-estimators 10 and --copy-durable with no fault at {scale}x of the grant: {} against off; snapshots written: {}",
            verdict(
                tracked
                    .iter()
                    .zip(&with_checks)
                    .all(|(a, b)| same_costs(a, b))
            ),
            with_checks.iter().all(|r| r.mach.writes().snapshots > 0),
        );
    }
    let completion = EnforceArgs {
        observe: ObserveArg::Completion,
        ..enforcing(false)
    };
    let dispatch: Vec<ArmRun> = labs
        .iter_mut()
        .map(|l| enforced_with(l, env, 1.0, AdmitArg::Perfect, enforcing(false)))
        .collect();
    let at_completion: Vec<ArmRun> = labs
        .iter_mut()
        .map(|l| enforced_with(l, env, 1.0, AdmitArg::Perfect, completion))
        .collect();
    println!(
        "  --observe completion where no claim reads a length, at 1x of the grant: {} against dispatch",
        verdict(
            dispatch
                .iter()
                .zip(&at_completion)
                .all(|(a, b)| alike(a, b))
        )
    );
}

struct Measured {
    label: String,
    runs: Vec<(Writes, f64, u64)>,
}

impl Measured {
    fn per_second(&self, f: impl Fn(&Writes) -> u64) -> Vec<f64> {
        self.runs.iter().map(|(w, s, _)| f(w) as f64 / s).collect()
    }

    fn per_request(&self, f: impl Fn(&Writes) -> u64) -> Vec<f64> {
        self.runs
            .iter()
            .map(|(w, _, offered)| f(w) as f64 / (*offered).max(1) as f64)
            .collect()
    }

    fn mean_per_second(&self, f: impl Fn(&Writes) -> u64) -> f64 {
        let v = self.per_second(f);
        v.iter().sum::<f64>() / v.len().max(1) as f64
    }
}

fn measure(run: &ArmRun) -> (Writes, f64, u64) {
    (
        run.mach.writes(),
        run.mach.now_ns() as f64 / 1e9,
        run.offered,
    )
}

fn measured(env: &Env, labs: &mut [Lab<'_>]) -> Vec<Measured> {
    let mut out = vec![Measured {
        label: "engine allocating, nothing enforced".to_string(),
        runs: labs
            .iter_mut()
            .map(|l| measure(&published(l, true)))
            .collect(),
    }];
    for &scale in &env.scales {
        out.push(Measured {
            label: format!("Phase 9's integrated arm at {scale}x of the grant"),
            runs: labs
                .iter_mut()
                .map(|l| measure(&enforced(l, env, scale, true)))
                .collect(),
        });
    }
    out
}

fn count(env: &Env, measured: &[Measured]) {
    section("2. the count: owned changes by owner, and the inferred stream, per simulated second");
    let node_seconds = env.nodes as f64;
    for m in measured {
        println!(
            "\n  {}: per second | per offered request | per node per second",
            m.label
        );
        let rows: [(&str, Row); 16] = [
            ("decisions", |w| w.decisions),
            ("dispatches", |w| w.dispatches),
            ("reservations committed", |w| w.reservations_committed),
            ("reservations released", |w| w.reservations_released),
            ("flights opened", |w| w.flights_opened),
            ("flights closed", |w| w.flights_closed),
            ("router queue enqueued", |w| w.queue_enqueued),
            ("router queue served", |w| w.queue_served),
            ("engine queue enqueued", |w| w.engine_enqueued),
            ("engine queue started", |w| w.engine_started),
            ("cancels", |w| w.cancels),
            ("router refusals", |w| w.refusals),
            ("flow-graph writes", |w| w.flow_graph),
            ("fan-outs staged", |w| w.fanouts_staged),
            ("host-class admissions", |w| w.host_admissions),
            ("host-class evictions", |w| w.host_evictions),
        ];
        for (name, f) in rows {
            println!(
                "    {name:<26} {:>24} | {:>22} | {:>20}",
                plain_seeds(&m.per_second(f), 1, ""),
                plain_seeds(&m.per_request(f), 3, ""),
                plain_seeds(
                    &m.per_second(f)
                        .iter()
                        .map(|v| v / node_seconds)
                        .collect::<Vec<_>>(),
                    1,
                    ""
                ),
            );
        }
        println!(
            "    {:<26} {:>24} | {:>22} | {:>20}",
            "OWNED CHANGES",
            plain_seeds(&m.per_second(Writes::owned), 1, ""),
            plain_seeds(&m.per_request(Writes::owned), 2, ""),
            plain_seeds(
                &m.per_second(Writes::owned)
                    .iter()
                    .map(|v| v / node_seconds)
                    .collect::<Vec<_>>(),
                1,
                ""
            ),
        );
        println!(
            "    {:<26} {:>24} | {:>22} | {:>20}",
            "KV events (inferred)",
            plain_seeds(&m.per_second(Writes::kv_total), 0, ""),
            plain_seeds(&m.per_request(Writes::kv_total), 1, ""),
            plain_seeds(
                &m.per_second(Writes::kv_total)
                    .iter()
                    .map(|v| v / node_seconds)
                    .collect::<Vec<_>>(),
                0,
                ""
            ),
        );
        for (i, label) in KV_EVENT_LABELS.iter().enumerate() {
            println!(
                "      {label:<24} {:>24}",
                plain_seeds(&m.per_second(|w| w.kv_events[i]), 1, "")
            );
        }
        println!(
            "    {:<26} {:>24} | removals from GPU and host offload, {:.1}-{:.1} KB/s of 8-byte hashes an engine",
            "of them, removals",
            plain_seeds(&m.per_second(Writes::kv_removals), 0, ""),
            lowest(&m.per_second(Writes::kv_removals)) / node_seconds * 8.0 / 1e3,
            highest(&m.per_second(Writes::kv_removals)) / node_seconds * 8.0 / 1e3,
        );
        println!(
            "    {:<26} {:>24} | {:>22}",
            "length observations (inferred)",
            plain_seeds(&m.per_second(|w| w.length_observations), 1, ""),
            plain_seeds(&m.per_request(|w| w.length_observations), 3, ""),
        );
    }
    liveness(env);
}

fn lowest(v: &[f64]) -> f64 {
    v.iter().copied().fold(f64::INFINITY, f64::min)
}

fn highest(v: &[f64]) -> f64 {
    v.iter().copied().fold(f64::NEG_INFINITY, f64::max)
}

fn liveness(env: &Env) {
    let planner = 0.046;
    let per_second = liveness_writes(env.nodes, 1.0, env.lease_renew);
    let eight = liveness_writes(8, 1.0, env.lease_renew);
    println!(
        "\n  the record tier: liveness at a {:.0} s renewal is {per_second:.2} writes a second on {} nodes and {eight:.2} on 8, \
         against the planner's {planner} (Phase 6, eight nodes): {:.0}x of it",
        env.lease_renew,
        env.nodes,
        eight / planner
    );
}

fn logged_config(preset: Preset, seed: u64) -> Config {
    let long = preset == Preset::LongRunning;
    Config {
        seed,
        programs: if long { 200 } else { 450 },
        rate: if long { 2.0 } else { 6.0 },
        mix: vec![(preset, 1.0)],
        shape: ShapeConfig::default(),
        suspend: if long { Suspend::Joint } else { Suspend::Never },
        ddr_mib: if long {
            2048
        } else {
            Config::default().ddr_mib
        },
        ..Config::default()
    }
}

fn logged_shares(env: &Env) -> Vec<(Preset, f64, f64)> {
    [
        Preset::Pipeline,
        Preset::Agentic,
        Preset::MultiAgent,
        Preset::LongRunning,
    ]
    .into_iter()
    .map(|preset| {
        let outs: Vec<_> = (env.seed..env.seed + env.seeds)
            .map(|seed| run(&logged_config(preset, seed)))
            .collect();
        let seconds: f64 = outs.iter().map(|o| o.span_ns as f64 / 1e9).sum();
        let decisions = outs.iter().map(|o| o.decisions).sum::<u64>() as f64 / seconds;
        let writes: u64 = outs
            .iter()
            .map(|o| LogCause::ALL.iter().map(|&c| o.logged_of(c)).sum::<u64>())
            .sum();
        (preset, decisions, writes as f64 / seconds)
    })
    .collect()
}

fn print_logged(shares: &[(Preset, f64, f64)]) {
    section(
        "3. the logged tier's writes against the soft tier's decisions (Phase 7's count, per preset)",
    );
    for &(preset, decisions, writes) in shares {
        println!(
            "  {:<14} decisions {decisions:>6.1} a second, logged writes {writes:>6.2} a second: {:>4.0}% of decisions",
            preset.name(),
            100.0 * writes / decisions
        );
    }
}

fn rung(env: &Env) {
    section("4. the durable-append rung: what a commit costs before any quorum is added");
    let appends = match durable::measure(&env.dir, env.reps) {
        Ok(appends) => appends,
        Err(e) => {
            println!("  cannot measure in {}: {e}", env.dir.display());
            return;
        }
    };
    println!(
        "  {}-byte appends to one file in {}, the lowest median and p99 of {} runs, timer {:.0} ns subtracted",
        durable::PAYLOAD_BYTES,
        appends.dir.display(),
        env.reps,
        appends.timer_ns
    );
    for commit in Commit::ALL {
        let t = appends.of(commit);
        println!(
            "    {:<24} median {:>10.1} us   p99 {:>10.1} us",
            commit.label(),
            t.median_ns as f64 / 1e3,
            t.p99_ns as f64 / 1e3
        );
    }
    let full = appends.of(Commit::FullFlush).median_ns as f64;
    let faas_ns = FAAS_EXEC_MIN_NS as f64 + FAAS_EXEC_SPAN_NS as f64 / 2.0;
    let rack_rtt = 2.0 * Distance::Rack.one_way_ns() as f64;
    let zone_rtt = 2.0 * Distance::Zone.one_way_ns() as f64;
    println!(
        "\n  published: datacenter drives with power-loss protection fsync in {:.1}-{:.1} us, consumer drives in {:.0}-{:.0} us \
         (Callaghan, 2026-01-07); FoundationDB commits in {:.1}-{:.1} ms below 75% load",
        PLP_FSYNC_US.0,
        PLP_FSYNC_US.1,
        CONSUMER_FSYNC_US.0,
        CONSUMER_FSYNC_US.1,
        FDB_COMMIT_MS.0,
        FDB_COMMIT_MS.1
    );
    println!(
        "  a commit per decision, against a warm FaaS invocation of {:.0} us (chosen constants):",
        faas_ns / 1e3
    );
    let rows = [
        (
            "protected drive, a rack's quorum",
            PLP_FSYNC_US.1 * 1e3 + rack_rtt,
        ),
        (
            "protected drive, a zone's quorum",
            PLP_FSYNC_US.1 * 1e3 + zone_rtt,
        ),
        ("this host's full flush, a rack's quorum", full + rack_rtt),
        ("this host's full flush, a zone's quorum", full + zone_rtt),
        ("FoundationDB, low", FDB_COMMIT_MS.0 * 1e6),
        ("FoundationDB, high", FDB_COMMIT_MS.1 * 1e6),
    ];
    for (label, ns) in rows {
        println!(
            "    {label:<42} {:>9.1} us  {:>6.2}x a warm FaaS invocation, {:.3}% of a one-second agent turn",
            ns / 1e3,
            ns / faas_ns,
            100.0 * ns / AGENT_TURN_NS
        );
    }
}

fn cores(rate: f64) -> (f64, f64) {
    let cluster_per_core = FDB_CLUSTER_OPS * FDB_CLUSTER_WRITE_SHARE / FDB_CLUSTER_CORES;
    (rate / FDB_WRITES_PER_CORE, rate / cluster_per_core)
}

fn fleet(env: &Env, measured: &[Measured], shares: &[(Preset, f64, f64)]) {
    section(
        "5. the count at fleet scale (arithmetic on the count above and FoundationDB's published figures)",
    );
    let Some(arm) = measured.last() else {
        println!("  needs a count");
        return;
    };
    let nodes = env.nodes as f64;
    let scale = env.fleet_nodes as f64 / nodes;
    println!(
        "  inputs: {} measured, per node; FoundationDB {FDB_WRITES_PER_CORE:.0} writes a second on one core (SSD engine), and \
         {:.0} writes a second on {FDB_CLUSTER_CORES:.0} cores ({:.0} a core) at {:.0}% writes; {} nodes",
        arm.label,
        FDB_CLUSTER_OPS * FDB_CLUSTER_WRITE_SHARE,
        FDB_CLUSTER_OPS * FDB_CLUSTER_WRITE_SHARE / FDB_CLUSTER_CORES,
        100.0 * FDB_CLUSTER_WRITE_SHARE,
        env.fleet_nodes
    );
    let owned = arm.mean_per_second(Writes::owned) * scale;
    let (single, cluster) = cores(owned);
    println!(
        "    owned changes (soft)        {owned:>12.0} a second: {single:>7.1} cores at the single-core rate, {cluster:>7.1} at the cluster rate; \
         per node {:.4} cores at the cluster rate",
        cluster / env.fleet_nodes as f64
    );
    let decisions_per_node = arm.mean_per_second(|w| w.decisions) / nodes;
    for &(preset, decisions, writes) in shares {
        let rate = decisions_per_node * env.fleet_nodes as f64 * writes / decisions;
        let (single, cluster) = cores(rate);
        println!(
            "    logged, {:<16}  {rate:>12.0} a second: {single:>7.1} cores at the single-core rate, {cluster:>7.1} at the cluster rate{}",
            preset.name(),
            if rate < FDB_CLUSTER_OPS * FDB_CLUSTER_WRITE_SHARE {
                ""
            } else {
                "  OVER THE PUBLISHED CLUSTER"
            }
        );
    }
    let record = liveness_writes(env.fleet_nodes as usize, 1.0, env.lease_renew)
        + 0.046 / 8.0 * env.fleet_nodes as f64;
    println!(
        "    record (liveness + planner) {record:>12.0} a second: {:.0}% of it liveness",
        100.0 * liveness_writes(env.fleet_nodes as usize, 1.0, env.lease_renew) / record
    );
}

const FAULT_AT: f64 = 0.4;
const WINDOW_S: f64 = 30.0;
const SEC: u64 = 1_000_000_000;

struct Faulted {
    run: ArmRun,
    details: Vec<Detail>,
}

fn faulted(lab: &mut Lab<'_>, env: &Env, scale: f64, plan: Option<Fault>, retry: Retry) -> Faulted {
    let args = EnforceArgs {
        track_flights: true,
        ..enforcing(false)
    };
    faulted_in(lab, env, (scale, false), args, plan, retry)
}

fn faulted_in(
    lab: &mut Lab<'_>,
    env: &Env,
    (scale, prefill): (f64, bool),
    args: EnforceArgs,
    plan: Option<Fault>,
    retry: Retry,
) -> Faulted {
    lab.fault = plan.map(|fault| FaultPlan {
        at_request: (FAULT_AT * env.ops as f64) as usize,
        fault,
        retry,
    });
    let run = enforced_in(lab, env, scale, prefill, AdmitArg::Quantile, args);
    lab.fault = None;
    let details = run.t.details.clone();
    Faulted { run, details }
}

struct Digest {
    total_s: f64,
    unserved: u64,
    window_mean_ms: f64,
    window_ttft_p99_ms: f64,
}

fn digest(details: &[Detail], env: &Env) -> Digest {
    let lo = (FAULT_AT * env.ops as f64) as u64;
    let hi = lo + (WINDOW_S * env.rate) as u64;
    let served: Vec<&Detail> = details.iter().filter(|d| d.served).collect();
    let window: Vec<&&Detail> = served
        .iter()
        .filter(|d| d.position >= lo && d.position < hi)
        .collect();
    let mut ttft: Vec<u64> = window
        .iter()
        .filter(|d| d.decodes && d.slo == 0 && d.class == 0)
        .map(|d| d.stall_ns)
        .collect();
    Digest {
        total_s: served.iter().map(|d| d.service_ns as f64).sum::<f64>() / 1e9,
        unserved: (details.len() - served.len()) as u64,
        window_mean_ms: window.iter().map(|d| d.service_ns as f64).sum::<f64>()
            / window.len().max(1) as f64
            / 1e6,
        window_ttft_p99_ms: polyphonic::instruments::percentile(&mut ttft, 0.99)
            .map_or(0.0, |ns| ns as f64 / 1e6),
    }
}

fn restart(
    fate: Fate,
    client: Client,
    outage_s: f64,
    subscriber: Subscriber,
    estimators: Estimators,
) -> Fault {
    Fault::Scheduler(Restart {
        fate,
        client,
        outage_ns: (outage_s * SEC as f64) as u64,
        subscriber,
        estimators,
        ledger: Rebuild::Now,
    })
}

fn blind(outage_s: f64, ledger: Rebuild) -> Fault {
    Fault::Scheduler(Restart {
        fate: Fate::Held,
        client: Client::Restart,
        outage_ns: (outage_s * SEC as f64) as u64,
        subscriber: Subscriber::Warm,
        estimators: Estimators::Kept,
        ledger,
    })
}

fn fault_row(label: &str, base: &[Faulted], arm: &[Faulted], env: &Env) {
    let ds: Vec<Digest> = arm.iter().map(|f| digest(&f.details, env)).collect();
    let bs: Vec<Digest> = base.iter().map(|f| digest(&f.details, env)).collect();
    let col = |f: &dyn Fn(&Digest, &Digest) -> f64, digits: usize| {
        plain_seeds(
            &ds.iter().zip(&bs).map(|(d, b)| f(d, b)).collect::<Vec<_>>(),
            digits,
            "",
        )
    };
    let stat = |f: &dyn Fn(&polyphonic::fault::FaultStats) -> f64| {
        plain_seeds(
            &arm.iter()
                .map(|r| f(&r.run.mach.fault_stats))
                .collect::<Vec<_>>(),
            0,
            "",
        )
    };
    println!(
        "  {label:<54} cost {} req-s | unserved {} | window mean {} ms, int ttft p99 {} ms | aborted {} gangs {} lost decode {} s held {} router held {} unknown reads {} | blind {} over {} refused at node {} ({} attempts)",
        col(&|d, b| d.total_s - b.total_s, 1),
        col(&|d, b| d.unserved as f64 - b.unserved as f64, 0),
        col(&|d, _| d.window_mean_ms, 1),
        col(&|d, _| d.window_ttft_p99_ms, 0),
        stat(&|f| f.streams_aborted as f64),
        stat(&|f| f.gangs_aborted as f64),
        stat(&|f| f.lost_decode_ns as f64 / 1e9),
        stat(&|f| f.held_through as f64),
        stat(&|f| f.router_held as f64),
        plain_seeds(
            &arm.iter()
                .map(|r| r.run.mach.unknown_reads() as f64)
                .collect::<Vec<_>>(),
            0,
            ""
        ),
        stat(&|f| f.blind_admissions as f64),
        stat(&|f| f.over_admissions as f64),
        stat(&|f| f.refused_at_node as f64),
        stat(&|f| f.refusal_attempts as f64),
    );
}

fn held_arms(snapshot: Subscriber) -> Vec<(String, Fault, Retry)> {
    vec![
        (
            "routing state only: belief cold, no outage".into(),
            restart(
                Fate::Held,
                Client::Restart,
                0.0,
                Subscriber::Cold,
                Estimators::Kept,
            ),
            Retry::Burst,
        ),
        (
            "routing state only: belief snapshot after 1 s".into(),
            restart(Fate::Held, Client::Restart, 0.0, snapshot, Estimators::Kept),
            Retry::Burst,
        ),
        ("estimators only".into(), Fault::Estimators, Retry::Burst),
        (
            "streams held, 0.1 s outage".into(),
            restart(Fate::Held, Client::Restart, 0.1, snapshot, Estimators::Lost),
            Retry::Burst,
        ),
        (
            "streams held, 1 s outage".into(),
            restart(Fate::Held, Client::Restart, 1.0, snapshot, Estimators::Lost),
            Retry::Burst,
        ),
        (
            "streams held, 1 s outage, nothing lost but the outage".into(),
            restart(
                Fate::Held,
                Client::Restart,
                1.0,
                Subscriber::Warm,
                Estimators::Kept,
            ),
            Retry::Burst,
        ),
        (
            "streams held, 15 s outage, burst".into(),
            restart(
                Fate::Held,
                Client::Restart,
                15.0,
                snapshot,
                Estimators::Lost,
            ),
            Retry::Burst,
        ),
        (
            "streams held, 15 s outage, backoff".into(),
            restart(
                Fate::Held,
                Client::Restart,
                15.0,
                snapshot,
                Estimators::Lost,
            ),
            Retry::Backoff,
        ),
    ]
}

fn shared_arms(snapshot: Subscriber) -> Vec<(String, Fault, Retry)> {
    vec![
        (
            "streams die, 0.1 s outage, client restarts".into(),
            restart(
                Fate::Shared,
                Client::Restart,
                0.1,
                snapshot,
                Estimators::Lost,
            ),
            Retry::Burst,
        ),
        (
            "streams die, 1 s outage, client restarts".into(),
            restart(
                Fate::Shared,
                Client::Restart,
                1.0,
                snapshot,
                Estimators::Lost,
            ),
            Retry::Burst,
        ),
        (
            "streams die, 1 s outage, client restarts, backoff".into(),
            restart(
                Fate::Shared,
                Client::Restart,
                1.0,
                snapshot,
                Estimators::Lost,
            ),
            Retry::Backoff,
        ),
        (
            "streams die, 1 s outage, client continues".into(),
            restart(
                Fate::Shared,
                Client::Continue,
                1.0,
                snapshot,
                Estimators::Lost,
            ),
            Retry::Burst,
        ),
        (
            "streams die, 1 s outage, belief and estimators kept".into(),
            restart(
                Fate::Shared,
                Client::Restart,
                1.0,
                Subscriber::Warm,
                Estimators::Kept,
            ),
            Retry::Burst,
        ),
        (
            "streams die, 15 s outage, client restarts, burst".into(),
            restart(
                Fate::Shared,
                Client::Restart,
                15.0,
                snapshot,
                Estimators::Lost,
            ),
            Retry::Burst,
        ),
        (
            "streams die, 15 s outage, client restarts, backoff".into(),
            restart(
                Fate::Shared,
                Client::Restart,
                15.0,
                snapshot,
                Estimators::Lost,
            ),
            Retry::Backoff,
        ),
    ]
}

fn restart_arms() -> Vec<(String, Fault, Retry)> {
    let snapshot = Subscriber::Snapshot { after_ns: SEC };
    let mut arms = held_arms(snapshot);
    arms.extend(shared_arms(snapshot));
    arms
}

fn restarts(env: &Env, labs: &mut [Lab<'_>]) {
    section(&format!(
        "6. a scheduler restart at {:.0}% of the trace: cost is the request-seconds of service added to the same trace with no fault; the window is {WINDOW_S:.0} s of arrivals from the fault",
        100.0 * FAULT_AT
    ));
    for &scale in &env.scales {
        println!("\n  {scale}x of the grant, prefill free");
        let base: Vec<Faulted> = labs
            .iter_mut()
            .map(|l| faulted(l, env, scale, None, Retry::Burst))
            .collect();
        base_row("no fault", &base, env);
        let arms = restart_arms();
        for (label, fault, retry) in arms {
            let runs: Vec<Faulted> = labs
                .iter_mut()
                .map(|l| faulted(l, env, scale, Some(fault), retry))
                .collect();
            fault_row(&label, &base, &runs, env);
        }
    }
}

const HEAVY_SEEDS: u64 = 5;
const OVERLOADS: [(f64, bool); 3] = [(0.6, false), (0.6, true), (0.5, false)];
const LOADS: [(f64, bool); 2] = [(1.0, false), (0.75, false)];

fn base_row(label: &str, base: &[Faulted], env: &Env) {
    let ds: Vec<Digest> = base.iter().map(|f| digest(&f.details, env)).collect();
    println!(
        "  {label:<54} sum of service {} s, unserved {}, window mean {} ms, int ttft p99 {} ms",
        plain_seeds(&ds.iter().map(|d| d.total_s).collect::<Vec<_>>(), 1, ""),
        plain_seeds(
            &ds.iter().map(|d| d.unserved as f64).collect::<Vec<_>>(),
            0,
            ""
        ),
        plain_seeds(
            &ds.iter().map(|d| d.window_mean_ms).collect::<Vec<_>>(),
            1,
            ""
        ),
        plain_seeds(
            &ds.iter().map(|d| d.window_ttft_p99_ms).collect::<Vec<_>>(),
            0,
            ""
        ),
    );
}

fn regime_label((scale, prefill): (f64, bool)) -> String {
    format!(
        "{scale}x of the grant, prefill {}",
        if prefill {
            "taking engine time"
        } else {
            "free"
        }
    )
}

fn routing<'a>(env: &Env, labs: &mut [Lab<'a>], heavy: &mut [Lab<'a>]) {
    section(
        "7. the restarted scheduler's ledger: rebuilt from node agents at once, after a delay, or never, with and without node agents checking their own partitions",
    );
    let checked = EnforceArgs {
        node_check: true,
        track_flights: true,
        ..enforcing(false)
    };
    let plain = EnforceArgs {
        track_flights: true,
        ..enforcing(false)
    };
    for (regimes, set) in [(&LOADS[..], &mut *labs), (&OVERLOADS[..], &mut *heavy)] {
        for &regime in regimes {
            println!("\n  {}", regime_label(regime));
            let base: Vec<Faulted> = set
                .iter_mut()
                .map(|l| faulted_in(l, env, regime, plain, None, Retry::Burst))
                .collect();
            base_row("no fault", &base, env);
            let after = Rebuild::After { after_ns: SEC };
            let arms: [(&str, Fault, EnforceArgs); 8] = [
                (
                    "0.1 s outage, ledger rebuilt at once",
                    blind(0.1, Rebuild::Now),
                    plain,
                ),
                ("0.1 s outage, rebuilt after 1 s", blind(0.1, after), plain),
                (
                    "0.1 s outage, never rebuilt",
                    blind(0.1, Rebuild::Never),
                    plain,
                ),
                (
                    "0.1 s outage, never rebuilt, node agents check",
                    blind(0.1, Rebuild::Never),
                    checked,
                ),
                (
                    "1 s outage, ledger rebuilt at once",
                    blind(1.0, Rebuild::Now),
                    plain,
                ),
                ("1 s outage, rebuilt after 1 s", blind(1.0, after), plain),
                (
                    "1 s outage, never rebuilt",
                    blind(1.0, Rebuild::Never),
                    plain,
                ),
                (
                    "1 s outage, never rebuilt, node agents check",
                    blind(1.0, Rebuild::Never),
                    checked,
                ),
            ];
            for (label, fault, args) in arms {
                let runs: Vec<Faulted> = set
                    .iter_mut()
                    .map(|l| faulted_in(l, env, regime, args, Some(fault), Retry::Burst))
                    .collect();
                fault_row(label, &base, &runs, env);
                println!(
                    "      engine waits {} against {}, cancels {} against {}",
                    plain_seeds(
                        &runs
                            .iter()
                            .map(|r| r.run.mach.engine_waits.queued as f64)
                            .collect::<Vec<_>>(),
                        0,
                        ""
                    ),
                    plain_seeds(
                        &base
                            .iter()
                            .map(|r| r.run.mach.engine_waits.queued as f64)
                            .collect::<Vec<_>>(),
                        0,
                        ""
                    ),
                    plain_seeds(
                        &runs
                            .iter()
                            .map(|r| r.run.mach.cancel_stats.cancels as f64)
                            .collect::<Vec<_>>(),
                        0,
                        ""
                    ),
                    plain_seeds(
                        &base
                            .iter()
                            .map(|r| r.run.mach.cancel_stats.cancels as f64)
                            .collect::<Vec<_>>(),
                        0,
                        ""
                    ),
                );
            }
        }
    }
}

fn snapshot_row(base: &[Faulted], runs: &[Faulted]) {
    let col = |set: &[Faulted], f: &dyn Fn(&ArmRun) -> f64, digits: usize| {
        plain_seeds(
            &set.iter().map(|r| f(&r.run)).collect::<Vec<_>>(),
            digits,
            "",
        )
    };
    println!(
        "      queued at the router {} against {}, cancels {} against {}, snapshots {} ({} KiB), restored at an age of {} s",
        col(runs, &|r| r.mach.queue_waits.queued as f64, 0),
        col(base, &|r| r.mach.queue_waits.queued as f64, 0),
        col(runs, &|r| r.mach.cancel_stats.cancels as f64, 0),
        col(base, &|r| r.mach.cancel_stats.cancels as f64, 0),
        col(runs, &|r| r.mach.writes().snapshots as f64, 0),
        col(runs, &|r| r.mach.writes().snapshot_bytes as f64 / 1024.0, 0),
        col(
            runs,
            &|r| r.mach.fault_stats.snapshot_age_ns as f64 / 1e9,
            1
        ),
    );
}

fn estimators<'a>(env: &Env, labs: &mut [Lab<'a>], heavy: &mut [Lab<'a>]) {
    section(
        "8. the estimators, observed when a decode ends: lost, restored from a snapshot at most 10 s old, and kept",
    );
    let args = EnforceArgs {
        track_flights: true,
        observe: ObserveArg::Completion,
        snapshot_estimators: 10.0,
        ..enforcing(false)
    };
    let regimes: [(&[(f64, bool)], bool); 2] = [
        (&[(1.0, false), (0.75, false), (0.75, true)], false),
        (&[(0.6, true)], true),
    ];
    for (list, is_heavy) in regimes {
        let set: &mut [Lab<'a>] = if is_heavy { &mut *heavy } else { &mut *labs };
        for &regime in list {
            println!("\n  {}", regime_label(regime));
            let base: Vec<Faulted> = set
                .iter_mut()
                .map(|l| faulted_in(l, env, regime, args, None, Retry::Burst))
                .collect();
            base_row("no fault", &base, env);
            let arms = [
                ("estimators lost", Estimators::Lost),
                ("estimators restored from a snapshot", Estimators::Snapshot),
                ("estimators kept", Estimators::Kept),
            ];
            for (label, estimators) in arms {
                let fault = restart(
                    Fate::Held,
                    Client::Restart,
                    0.0,
                    Subscriber::Warm,
                    estimators,
                );
                let runs: Vec<Faulted> = set
                    .iter_mut()
                    .map(|l| faulted_in(l, env, regime, args, Some(fault), Retry::Burst))
                    .collect();
                fault_row(label, &base, &runs, env);
                snapshot_row(&base, &runs);
            }
        }
    }
}

fn loss_row(base: &[Faulted], runs: &[Faulted]) {
    let col = |set: &[Faulted], f: &dyn Fn(&ArmRun) -> f64, digits: usize| {
        plain_seeds(
            &set.iter().map(|r| f(&r.run)).collect::<Vec<_>>(),
            digits,
            "",
        )
    };
    println!(
        "      kv lost {}, host lost {}, durable lost {} saved {}; fan-outs refused {} against {}, router refusals {} against {}; moved from the engine {}, parked {} for {} s",
        col(runs, &|r| r.mach.fault_stats.kv_lost as f64, 0),
        col(runs, &|r| r.mach.fault_stats.host_lost as f64, 0),
        col(
            runs,
            &|r| r.mach.fault_stats.durable_lost_with_node as f64,
            0
        ),
        col(runs, &|r| r.mach.fault_stats.durable_saved as f64, 0),
        col(runs, &|r| r.mach.fanouts_refused as f64, 0),
        col(base, &|r| r.mach.fanouts_refused as f64, 0),
        col(
            runs,
            &|r| r.mach.refused_by_router.iter().sum::<u64>() as f64,
            0
        ),
        col(
            base,
            &|r| r.mach.refused_by_router.iter().sum::<u64>() as f64,
            0
        ),
        col(runs, &|r| r.mach.fault_stats.moved_from_engine as f64, 0),
        col(runs, &|r| r.mach.fault_stats.limbo_requests as f64, 0),
        col(runs, &|r| r.mach.fault_stats.limbo_wait_ns as f64 / 1e9, 0),
    );
}

fn engines(env: &Env, labs: &mut [Lab<'_>]) {
    section(&format!(
        "9. an engine crash on node 0 at {:.0}% of the trace: its streams failed, its KV dropped, the node out of the decode pool for its restart while its host work runs on",
        100.0 * FAULT_AT
    ));
    let crash = |restart_s: f64, spill: Spill, client: Client| {
        Fault::Engine(EngineCrash {
            node: 0,
            restart_ns: (restart_s * SEC as f64) as u64,
            spill,
            client,
        })
    };
    let arms: [(&str, Fault); 7] = [
        (
            "restart 2 s, spill kept, continued",
            crash(2.0, Spill::Kept, Client::Continue),
        ),
        (
            "restart 8 s, spill kept, continued",
            crash(8.0, Spill::Kept, Client::Continue),
        ),
        (
            "restart 30 s, spill kept, continued",
            crash(30.0, Spill::Kept, Client::Continue),
        ),
        (
            "restart 30 s, spill kept, restarted",
            crash(30.0, Spill::Kept, Client::Restart),
        ),
        (
            "restart 30 s, spill lost, continued",
            crash(30.0, Spill::Lost, Client::Continue),
        ),
        (
            "restart 120 s, spill kept, continued",
            crash(120.0, Spill::Kept, Client::Continue),
        ),
        (
            "restart 120 s, spill lost, restarted",
            crash(120.0, Spill::Lost, Client::Restart),
        ),
    ];
    for regime in LOADS {
        println!("\n  {}", regime_label(regime));
        let args = EnforceArgs {
            track_flights: true,
            ..enforcing(false)
        };
        let base: Vec<Faulted> = labs
            .iter_mut()
            .map(|l| faulted_in(l, env, regime, args, None, Retry::Burst))
            .collect();
        base_row("no fault", &base, env);
        for (label, fault) in arms {
            let runs: Vec<Faulted> = labs
                .iter_mut()
                .map(|l| faulted_in(l, env, regime, args, Some(fault), Retry::Burst))
                .collect();
            fault_row(label, &base, &runs, env);
            loss_row(&base, &runs);
        }
    }
}

fn nodes(env: &Env, labs: &mut [Lab<'_>]) {
    section(&format!(
        "10. node 0 lost at {:.0}% of the trace and never replaced: declared at once, or only when its lease expires, with requests placed on it parked until then",
        100.0 * FAULT_AT
    ));
    let lose = |declare_s: f64, client: Client| {
        Fault::Node(NodeLoss {
            node: 0,
            declare_ns: (declare_s * SEC as f64) as u64,
            client,
        })
    };
    let arms: [(&str, Fault); 5] = [
        ("declared at once, continued", lose(0.0, Client::Continue)),
        ("declared at once, restarted", lose(0.0, Client::Restart)),
        (
            "declared after 2 s (a silence), continued",
            lose(2.0, Client::Continue),
        ),
        (
            "declared after 10 s (a short lease), continued",
            lose(10.0, Client::Continue),
        ),
        (
            "declared after 40 s (Kubernetes' grace), continued",
            lose(40.0, Client::Continue),
        ),
    ];
    for regime in LOADS {
        println!("\n  {}", regime_label(regime));
        let args = EnforceArgs {
            track_flights: true,
            ..enforcing(false)
        };
        let base: Vec<Faulted> = labs
            .iter_mut()
            .map(|l| faulted_in(l, env, regime, args, None, Retry::Burst))
            .collect();
        base_row("no fault", &base, env);
        for (label, fault) in arms {
            let runs: Vec<Faulted> = labs
                .iter_mut()
                .map(|l| faulted_in(l, env, regime, args, Some(fault), Retry::Burst))
                .collect();
            fault_row(label, &base, &runs, env);
            loss_row(&base, &runs);
        }
    }
}

const SIDECAR_TAX_US: (f64, f64) = (43.97, 74.97);

fn crossover_arms() -> Vec<(String, Fault)> {
    let snapshot = Subscriber::Snapshot { after_ns: SEC };
    let crash = Fault::Engine(EngineCrash {
        node: 0,
        restart_ns: 30 * SEC,
        spill: Spill::Kept,
        client: Client::Continue,
    });
    vec![
        (
            "streams held, 0.1 s takeover".into(),
            restart(Fate::Held, Client::Restart, 0.1, snapshot, Estimators::Lost),
        ),
        (
            "streams die, 0.1 s takeover, client restarts".into(),
            restart(
                Fate::Shared,
                Client::Restart,
                0.1,
                snapshot,
                Estimators::Lost,
            ),
        ),
        (
            "streams held, 1 s takeover".into(),
            restart(Fate::Held, Client::Restart, 1.0, snapshot, Estimators::Lost),
        ),
        (
            "streams die, 1 s takeover, client continues".into(),
            restart(
                Fate::Shared,
                Client::Continue,
                1.0,
                snapshot,
                Estimators::Lost,
            ),
        ),
        (
            "streams die, 1 s takeover, client restarts".into(),
            restart(
                Fate::Shared,
                Client::Restart,
                1.0,
                snapshot,
                Estimators::Lost,
            ),
        ),
        (
            "a 15 s lease, streams held".into(),
            restart(
                Fate::Held,
                Client::Restart,
                15.0,
                snapshot,
                Estimators::Lost,
            ),
        ),
        ("one engine of four down for 30 s".into(), crash),
        (
            "the sidecar's fail-open window: 15 s of hash-only routing, streams intact".into(),
            Fault::Degrade(Degrade { for_ns: 15 * SEC }),
        ),
    ]
}

fn hours(request_seconds: f64, saved_per_s: f64) -> String {
    let secs = request_seconds / saved_per_s;
    if secs < 120.0 {
        format!("{secs:.0} s")
    } else if secs < 172_800.0 {
        format!("{:.1} h", secs / 3600.0)
    } else {
        format!("{:.1} weeks", secs / 604_800.0)
    }
}

fn crossover(env: &Env, labs: &mut [Lab<'_>]) {
    section(&format!(
        "11. the crossover: how long the integrated path's saving takes to pay for one fault (Phase 8's published tax of {:.2}-{:.2} us a request, at {:.0} req/s)",
        SIDECAR_TAX_US.0, SIDECAR_TAX_US.1, env.rate
    ));
    let saved = (
        env.rate * SIDECAR_TAX_US.0 / 1e6,
        env.rate * SIDECAR_TAX_US.1 / 1e6,
    );
    let args = EnforceArgs {
        track_flights: true,
        ..enforcing(false)
    };
    for &scale in &env.scales {
        println!("\n  {scale}x of the grant, prefill free");
        let regime = (scale, false);
        let base: Vec<Faulted> = labs
            .iter_mut()
            .map(|l| faulted_in(l, env, regime, args, None, Retry::Burst))
            .collect();
        for (label, fault) in crossover_arms() {
            let runs: Vec<Faulted> = labs
                .iter_mut()
                .map(|l| faulted_in(l, env, regime, args, Some(fault), Retry::Burst))
                .collect();
            let costs: Vec<f64> = runs
                .iter()
                .zip(&base)
                .map(|(r, b)| digest(&r.details, env).total_s - digest(&b.details, env).total_s)
                .collect();
            let mean = costs.iter().sum::<f64>() / costs.len().max(1) as f64;
            println!(
                "  {label:<76} {} req-s | pays for {} to {} of the tax",
                plain_seeds(&costs, 1, ""),
                hours(mean.max(0.0), saved.1),
                hours(mean.max(0.0), saved.0),
            );
        }
    }
}

fn durable_cells(env: &Env) {
    section(
        "12. durable sandboxes under node loss: long-running programs, node 0 lost half way through the arrivals",
    );
    let link_bytes_per_s = 1e9 / Distance::Zone.ns_per_byte();
    for copy in [false, true] {
        println!(
            "\n  {}",
            if copy {
                "each durable cell copied off its node when it is marked"
            } else {
                "no copy"
            }
        );
        for seed in env.seed..env.seed + env.seeds {
            let mut base = logged_config(Preset::LongRunning, seed);
            base.copy_durable = copy;
            let half = (base.programs as f64 / base.rate / 2.0 * 1e9) as u64;
            let quiet = run(&base);
            let lost = run(&Config {
                fault: Some((
                    half,
                    Fault::Node(NodeLoss {
                        node: 0,
                        declare_ns: 0,
                        client: Client::Restart,
                    }),
                )),
                ..base.clone()
            });
            let seconds = lost.span_ns as f64 / 1e9;
            let rate = lost.fault.durable_copied_bytes as f64 / seconds / base.nodes as f64;
            let turn = |o: &polyphonic::programs::Outcome| {
                polyphonic::programs::mean(o.turns_of(polyphonic::work::Pattern::LongRunning)) / 1e6
            };
            println!(
                "    seed {seed}: durable cells lost {} saved {}, programs left with lost state {}, host blobs lost {}, copied {:.0} MiB = {:.2} MiB/s a node ({:.3}% of a zone link); turn mean {:.0} ms against {:.0} ms with no loss",
                lost.fault.durable_lost_with_node,
                lost.fault.durable_saved,
                lost.state_lost,
                lost.fault.host_lost,
                lost.fault.durable_copied_bytes as f64 / (1u64 << 20) as f64,
                rate / (1u64 << 20) as f64,
                100.0 * rate / link_bytes_per_s,
                turn(&lost),
                turn(&quiet),
            );
        }
    }
}

pub fn run_all(env: &Env) {
    println!(
        "durability (phase-10.md §4.14): what each tier writes and what a crash costs; \
         {} nodes, {} req/s, {:.0}% fan-out, {:.0}% of sessions declare a throughput objective, ops={}, seeds {}..{}\n\
         the lease renews every {:.0} s (Kubernetes' default is {LEASE_RENEW_S:.0} s)",
        env.nodes,
        env.rate,
        100.0 * env.fanout,
        100.0 * env.throughput,
        env.ops,
        env.seed,
        env.seed + env.seeds - 1,
        env.lease_renew,
    );
    let envs = env.lab_envs();
    let mut labs: Vec<Lab<'_>> = envs.iter().map(Lab::new).collect();
    let big_envs = env.lab_envs_of(HEAVY_SEEDS);
    let mut heavy: Vec<Lab<'_>> = big_envs.iter().map(Lab::new).collect();
    let wanted: Vec<&str> = env.sections.split(',').map(str::trim).collect();
    let on = |name: &str| wanted.contains(&name);
    if on("gate") {
        gate(env, &mut labs);
    }
    let counts = (on("count") || on("fleet")).then(|| measured(env, &mut labs));
    if on("count")
        && let Some(counts) = &counts
    {
        count(env, counts);
    }
    let shares = (on("logged") || on("fleet")).then(|| logged_shares(env));
    if on("logged")
        && let Some(shares) = &shares
    {
        print_logged(shares);
    }
    if on("restart") {
        restarts(env, &mut labs);
    }
    if on("routing") {
        routing(env, &mut labs, &mut heavy);
    }
    if on("estimators") {
        estimators(env, &mut labs, &mut heavy);
    }
    if on("engine") {
        engines(env, &mut labs);
    }
    if on("node") {
        nodes(env, &mut labs);
    }
    if on("crossover") {
        crossover(env, &mut labs);
    }
    if on("durable") {
        durable_cells(env);
    }
    if on("rung") {
        rung(env);
    }
    if on("fleet")
        && let (Some(counts), Some(shares)) = (&counts, &shares)
    {
        fleet(env, counts, shares);
    }
}
