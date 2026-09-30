use std::collections::HashMap;
use std::fmt::Write;

use polyphonic::blob::BlobKind;
use polyphonic::engine::MODEL_COUNT;
use polyphonic::fleet::{Costs, GIB, phase_demand};
use polyphonic::instruments::percentile;
use polyphonic::machine::{Control, Placement};
use polyphonic::topo::Distance;
use polyphonic::work::{Origin, PHASES, Request, Workload, model_of};

use super::belief_cmd::{Env as LabEnv, Lab, Regime, scored_fetch};
use super::influence_cmd::{change, identical, plain_seeds, section, seeds_of};
use super::{
    Arm, ArmRun, BeliefArgs, FleetArgs, InfluenceArgs, ModelBatchesArg, PairingArg, PlannerArg,
    PrefillArg, arm, mean_of, ms, quantile,
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
    fn lab_envs(&self, fanout: f64) -> Vec<LabEnv> {
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
                fanout,
                sections: String::new(),
            })
            .collect()
    }
}

const TAIL: f64 = 0.99;
const FLEET: FleetArgs = FleetArgs::OFF;
const OFF: InfluenceArgs = InfluenceArgs::OFF;
const TRACKED: InfluenceArgs = InfluenceArgs {
    reuse: true,
    ..InfluenceArgs::OFF
};
const HEAVY_PREFILL_NS: u64 = 10_000_000;
const TENANT_COUNT: u32 = polyphonic::work::TENANTS as u32;
const QUIET_FROM: usize = TENANT_COUNT as usize / 2;

struct Cell {
    in_window: f64,
    out_window: f64,
    in_p99: f64,
    neighbour: f64,
    neighbour_refused: f64,
    others_refused: f64,
    others_run: f64,
    prefillers: usize,
    role_moves: u64,
    paired: f64,
    coupled: f64,
    duplicated: f64,
    wait_ms: f64,
    phase: [f64; PHASES],
    counts: [usize; MODEL_COUNT],
    moves: u64,
    full: f64,
    warm: f64,
    unplaced: u64,
    service: f64,
    p99: f64,
    stall: f64,
    flow_stall: f64,
    landed: f64,
    preempted: f64,
    models: [f64; MODEL_COUNT + 1],
    work_share: f64,
    engine_share: f64,
    stretch: f64,
}

fn cell(r: &ArmRun, rate: f64) -> Cell {
    let all: Vec<u64> = r.t.samples.iter().flatten().copied().collect();
    let m = &r.mach;
    let decodes: u64 = m.models_in_flight().iter().sum();
    let models = m
        .models_in_flight()
        .map(|n| 100.0 * n as f64 / decodes.max(1) as f64);
    let wall_ns = r.offered as f64 / rate * 1e9 * m.nodes() as f64;
    let work = m.prefill_work_ns() as f64;
    let i = &m.instruments;
    let snapshots = BlobKind::Snapshot.idx();
    let ps = m.pair_stats;
    let window = tenant_window(r);
    Cell {
        in_window: window[0],
        out_window: window[1],
        in_p99: window[2],
        neighbour: window[3],
        neighbour_refused: window[4],
        others_refused: window[5],
        others_run: window[6],
        prefillers: m.fleet().map_or(0, |f| f.prefiller_counts()[0]),
        role_moves: m.fleet().map_or(0, |f| f.stats.role_moves),
        paired: 100.0 * ps.paired as f64 / ps.decisions.max(1) as f64,
        coupled: 100.0 * ps.coupled as f64 / ps.decisions.max(1) as f64,
        duplicated: ps.work_ns as f64 / ps.avoided_ns.max(1) as f64,
        wait_ms: ps.wait_ns as f64 / ps.paired.max(1) as f64 / 1e6,
        phase: std::array::from_fn(|p| r.t.phase[p].0 as f64 / r.t.phase[p].1.max(1) as f64 / 1e6),
        counts: m
            .fleet()
            .map_or([0; MODEL_COUNT], polyphonic::fleet::Fleet::counts),
        moves: m.fleet().map_or(0, |f| f.stats.loads),
        full: 100.0 * m.saturated() as f64 / m.decodes().max(1) as f64,
        warm: 100.0 * r.t.warm[snapshots] as f64 / r.t.ops[snapshots].max(1) as f64,
        unplaced: m.fleet().map_or(0, |f| f.stats.unplaced),
        service: mean_of(&all),
        p99: ms(quantile(&all, TAIL)),
        stall: r.total as f64 / r.served.max(1) as f64 / 1e6,
        flow_stall: i.flow.stall_ns as f64 / i.flow.n.max(1) as f64 / 1e6,
        landed: 100.0 * i.prefill.landed as f64 / i.prefill.landings.max(1) as f64,
        preempted: 100.0 * m.preempted.iter().sum::<u64>() as f64 / r.offered.max(1) as f64,
        models,
        work_share: 100.0 * work / wall_ns,
        engine_share: 100.0 * work / (work + m.decode_share_ns() as f64).max(1.0),
        stretch: 100.0 * m.stretch_ns() as f64 / m.decode_ns().max(1) as f64,
    }
}

fn runs(
    labs: &mut [Lab<'_>],
    regime: Regime,
    fleet: FleetArgs,
    influence: InfluenceArgs,
) -> Vec<ArmRun> {
    labs.iter_mut()
        .map(|lab| {
            lab.go_fleet(
                Distance::Rack,
                regime,
                &scored_fetch(),
                BeliefArgs::OFF,
                influence,
                fleet,
                false,
            )
        })
        .collect()
}

fn cells(rs: &[ArmRun], rate: f64) -> Vec<Cell> {
    rs.iter().map(|r| cell(r, rate)).collect()
}

fn col(cs: &[Cell], f: impl Fn(&Cell) -> f64) -> Vec<f64> {
    cs.iter().map(f).collect()
}

fn against(base: &[Cell], cs: &[Cell], f: impl Fn(&Cell) -> f64) -> Vec<f64> {
    base.iter()
        .zip(cs)
        .map(|(b, c)| change(f(b), f(c)))
        .collect()
}

fn verdict(same: bool) -> &'static str {
    if same { "identical" } else { "DIFFERS" }
}

fn one(lab: &mut Lab<'_>, regime: Regime, fleet: FleetArgs) -> ArmRun {
    lab.go_fleet(
        Distance::Rack,
        regime,
        &scored_fetch(),
        BeliefArgs::OFF,
        OFF,
        fleet,
        false,
    )
}

fn gate(env: &Env, labs: &mut [Lab<'_>]) {
    section("1. the gate: each bit must change nothing where it has nothing to change");
    let no_agents_envs = env.lab_envs(0.0);
    let mut no_agents = Lab::new(&no_agents_envs[0]);
    for regime in Regime::ALL {
        println!("\n  {}", regime.label());
        gate_engine(regime, &mut labs[0]);
        gate_keyed(regime, &mut no_agents);
        gate_planner(regime, &mut labs[0]);
        gate_tenants(regime, &mut labs[0]);
    }
}

fn gate_tenants(regime: Regime, lab: &mut Lab<'_>) {
    let base = prefill_arm(FLEET, 1.0, 0.0);
    let off = one(lab, regime, base);
    for (name, fleet) in [
        (
            "the tenants instrument",
            FleetArgs {
                tenants: true,
                ..base
            },
        ),
        (
            "a prefill quota and a slot limit nothing reaches",
            FleetArgs {
                tenant_prefill: 1e6,
                tenant_slots: 1_000_000,
                ..base
            },
        ),
        (
            "a tenant floor of nothing",
            FleetArgs {
                tenant_floor: Some(0),
                ..base
            },
        ),
    ] {
        let r = one(lab, regime, fleet);
        println!("    {name}, against off: {}", verdict(identical(&off, &r)));
    }
}

fn gate_engine(regime: Regime, lab: &mut Lab<'_>) {
    let one_model = FleetArgs {
        one_model: true,
        ..FLEET
    };
    let shared = one(lab, regime, one_model);
    for (name, batches) in [
        ("blind", ModelBatchesArg::Blind),
        ("priced", ModelBatchesArg::Priced),
    ] {
        let r = one(
            lab,
            regime,
            FleetArgs {
                model_batches: batches,
                ..one_model
            },
        );
        println!(
            "    one model, a batch per model, score {name}, against one batch per node: {}",
            verdict(identical(&shared, &r))
        );
    }
    let base = one(lab, regime, FLEET);
    for (name, mode) in [("blind", PrefillArg::Blind), ("priced", PrefillArg::Priced)] {
        let free = one(
            lab,
            regime,
            FleetArgs {
                prefill_time: mode,
                prefill_free: 1_000.0,
                ..FLEET
            },
        );
        println!(
            "    prefill time {name} with 1 s a step free, against prefill off: {}",
            verdict(identical(&base, &free))
        );
    }
}

fn gate_keyed(regime: Regime, no_agents: &mut Lab<'_>) {
    let flat = one(no_agents, regime, FLEET);
    let keyed = one(
        no_agents,
        regime,
        FleetArgs {
            model_keyed: true,
            ..FLEET
        },
    );
    println!(
        "    model-keyed with no fan-outs, against unkeyed: {}",
        verdict(identical(&flat, &keyed))
    );
}

fn gate_planner(regime: Regime, lab: &mut Lab<'_>) {
    for (name, planner) in [("follow", PlannerArg::Follow), ("eager", PlannerArg::Eager)] {
        let r = one(
            lab,
            regime,
            FleetArgs {
                planner,
                model_mix: Some([0.25; 4]),
                ..placed_fleet(None, false)
            },
        );
        let moved = r.mach.fleet().is_some_and(|f| f.stats.loads > 0);
        println!(
            "    planner {name} on a stationary mix, {} ticks: {}",
            r.mach.planner_ticks(),
            if moved { "MOVED" } else { "no replica moved" }
        );
    }
}

fn batches(env: &Env, labs: &mut [Lab<'_>]) {
    section("2. a batch per model: what one batch per node was not charging (P1)");
    let arms = [
        ("one batch per node, as published", FLEET),
        (
            "a batch per model, the score as it is",
            FleetArgs {
                model_batches: ModelBatchesArg::Blind,
                ..FLEET
            },
        ),
        (
            "a batch per model, the score pricing it",
            FleetArgs {
                model_batches: ModelBatchesArg::Priced,
                ..FLEET
            },
        ),
    ];
    for regime in Regime::ALL {
        println!("\n  {}", regime.label());
        let mut base: Vec<Cell> = Vec::new();
        for (name, fleet) in arms {
            let cs = cells(&runs(labs, regime, fleet, OFF), env.rate);
            println!("    {name}");
            println!(
                "      mean service {}, p99 {}",
                plain_seeds(&col(&cs, |c| c.service), 1, " ms"),
                plain_seeds(&col(&cs, |c| c.p99), 0, " ms"),
            );
            if base.is_empty() {
                base = cs;
                println!(
                    "      models in flight at a decode's admission: three {}, four {}",
                    plain_seeds(&col(&base, |c| c.models[3]), 0, "%"),
                    plain_seeds(&col(&base, |c| c.models[4]), 0, "%"),
                );
                continue;
            }
            println!(
                "      against one batch per node: service {}, p99 {}",
                seeds_of(&against(&base, &cs, |c| c.service), "%"),
                seeds_of(&against(&base, &cs, |c| c.p99), "%"),
            );
            println!(
                "      models in flight: three {}, four {}",
                plain_seeds(&col(&cs, |c| c.models[3]), 0, "%"),
                plain_seeds(&col(&cs, |c| c.models[4]), 0, "%"),
            );
        }
    }
}

fn prefill_arm(fleet: FleetArgs, window: f64, free: f64) -> FleetArgs {
    FleetArgs {
        prefill_time: PrefillArg::Priced,
        prefill_window: window,
        prefill_free: free,
        ..fleet
    }
}

fn blind_prefill(fleet: FleetArgs) -> FleetArgs {
    FleetArgs {
        prefill_time: PrefillArg::Blind,
        ..fleet
    }
}

fn origin_table(r: &ArmRun) {
    let m = &r.mach.instruments;
    let total: u64 = m.work_by_origin.iter().flatten().sum();
    println!(
        "      prefill work per KV dispatch, by the origin of the chain's last block (seed 1):"
    );
    for o in Origin::ALL {
        let mut v = m.work_by_origin[o.idx()].clone();
        if v.is_empty() {
            continue;
        }
        let sum: u64 = v.iter().sum();
        let n = v.len();
        let p50 = ms(percentile(&mut v, 0.5).unwrap_or(0));
        let p90 = ms(percentile(&mut v, 0.9).unwrap_or(0));
        let p99 = ms(percentile(&mut v, 0.99).unwrap_or(0));
        println!(
            "        {:<12} n {n:>6}  mean {:>6.2} ms  p50 {p50:>6.2}  p90 {p90:>6.2}  p99 {p99:>6.2}  {:>5.1}% of the work",
            o.label(),
            sum as f64 / n as f64 / 1e6,
            100.0 * sum as f64 / total.max(1) as f64,
        );
    }
    let all: Vec<u64> = m.work_by_origin.iter().flatten().copied().collect();
    let (n, w) = all
        .iter()
        .filter(|w| **w >= HEAVY_PREFILL_NS)
        .fold((0u64, 0u64), |(n, w), x| (n + 1, w + x));
    println!(
        "      dispatches with 10 ms or more of prefill: {:.1}% of KV dispatches, {:.1}% of the work",
        100.0 * n as f64 / all.len().max(1) as f64,
        100.0 * w as f64 / total.max(1) as f64,
    );
}

fn prefill(env: &Env, labs: &mut [Lab<'_>]) {
    section("6. prefill as engine work (P5)");
    for regime in Regime::ALL {
        println!("\n  {}", regime.label());
        let free_runs = runs(labs, regime, FLEET, TRACKED);
        let base = cells(&free_runs, env.rate);
        println!(
            "    prefill work per node, as a share of wall time: {}",
            plain_seeds(&col(&base, |c| c.work_share), 1, "%"),
        );
        println!(
            "    prefill's share of engine time, decode at its batch share: {}",
            plain_seeds(&col(&base, |c| c.engine_share), 1, "%"),
        );
        origin_table(&free_runs[0]);
        println!(
            "    prefill free: mean service {}, preempted {}",
            plain_seeds(&col(&base, |c| c.service), 1, " ms"),
            plain_seeds(&col(&base, |c| c.preempted), 1, "%"),
        );
        let arms = [
            ("takes engine time, score blind to it", blind_prefill(FLEET)),
            (
                "takes engine time, 1 s window",
                prefill_arm(FLEET, 1.0, 0.0),
            ),
            (
                "takes engine time, 250 ms window",
                prefill_arm(FLEET, 0.25, 0.0),
            ),
            (
                "takes engine time, 4 s window",
                prefill_arm(FLEET, 4.0, 0.0),
            ),
            ("0.5 ms a step free", prefill_arm(FLEET, 1.0, 0.5)),
            ("1 ms a step free", prefill_arm(FLEET, 1.0, 1.0)),
        ];
        for (name, fleet) in arms {
            let cs = cells(&runs(labs, regime, fleet, TRACKED), env.rate);
            println!(
                "    prefill {name}: service {} against free, stretch {} of decode time, preempted {}",
                seeds_of(&against(&base, &cs, |c| c.service), "%"),
                plain_seeds(&col(&cs, |c| c.stretch), 1, "%"),
                plain_seeds(&col(&cs, |c| c.preempted), 1, "%"),
            );
        }
        for (name, fleet) in [
            ("free", FLEET),
            ("takes engine time", prefill_arm(FLEET, 1.0, 0.0)),
        ] {
            let without = cells(&runs(labs, regime, fleet, TRACKED), env.rate);
            let ahead = InfluenceArgs {
                prefill_ahead: true,
                ..TRACKED
            };
            let with = cells(&runs(labs, regime, fleet, ahead), env.rate);
            println!(
                "    prefill-ahead, prefill {name}: service {}, the flow downstream's stall {}, landed {}",
                seeds_of(&against(&without, &with, |c| c.service), "%"),
                seeds_of(&against(&without, &with, |c| c.flow_stall), "%"),
                plain_seeds(&col(&with, |c| c.landed), 0, "%"),
            );
        }
    }
    prefill_on_the_fleet(env);
}

fn prefill_on_the_fleet(env: &Env) {
    let (nodes, rate) = (8, 500.0);
    println!(
        "\n  on the fleet: {nodes} nodes, {rate:.0} req/s, {} ops, two replicas a model",
        env.ops * 2
    );
    let envs = scaled_envs(env, nodes, rate, 2, env.dram / env.nodes as u64);
    let mut labs: Vec<Lab<'_>> = envs.iter().map(Lab::new).collect();
    let placed = placed_fleet(Some([2, 2, 2, 2]), false);
    let free = cells(
        &runs_with(&mut labs, Regime::Defaults, &scored_fetch(), placed),
        rate,
    );
    println!(
        "    prefill free: mean service {}",
        plain_seeds(&col(&free, |c| c.service), 1, " ms"),
    );
    for (name, fleet) in [
        ("score blind to it", blind_prefill(placed)),
        ("1 s window", prefill_arm(placed, 1.0, 0.0)),
    ] {
        let cs = cells(
            &runs_with(&mut labs, Regime::Defaults, &scored_fetch(), fleet),
            rate,
        );
        println!(
            "    prefill takes engine time, {name}: service {} against free, stretch {} of decode time, full batch {}",
            seeds_of(&against(&free, &cs, |c| c.service), "%"),
            plain_seeds(&col(&cs, |c| c.stretch), 1, "%"),
            plain_seeds(&col(&cs, |c| c.full), 1, "%"),
        );
    }
    let unkeyed = FleetArgs {
        model_keyed: false,
        ..placed
    };
    let unkeyed_free = cells(
        &runs_with(&mut labs, Regime::Defaults, &scored_fetch(), unkeyed),
        rate,
    );
    let unkeyed_blind = cells(
        &runs_with(
            &mut labs,
            Regime::Defaults,
            &scored_fetch(),
            blind_prefill(unkeyed),
        ),
        rate,
    );
    println!(
        "    unkeyed, as the pre-measurement ran it and --fleet refuses: prefill free {}, prefill \
         taking engine time with the score blind to it {} against free",
        plain_seeds(&col(&unkeyed_free, |c| c.service), 1, " ms"),
        seeds_of(&against(&unkeyed_free, &unkeyed_blind, |c| c.service), "%"),
    );
}

fn foreign_agents(seed: u64, ops: u64, fanout: f64) -> (u64, u64) {
    let trace: Vec<Request> = Workload::with_fanout(seed, ops, 1.0, fanout).collect();
    let resumed: HashMap<u64, Option<u8>> = trace
        .iter()
        .filter(|r| r.gang.is_none())
        .filter_map(|r| r.completes.map(|t| (t, model_of(&r.requires))))
        .collect();
    let (mut foreign, mut agents) = (0, 0);
    for req in &trace {
        let (Some(gang), Some(hint)) = (&req.gang, &req.hint) else {
            continue;
        };
        let Some(&parent) = resumed.get(&hint.task) else {
            continue;
        };
        for a in &gang.agents {
            agents += 1;
            foreign += u64::from(model_of(&a.requires) != parent);
        }
    }
    (foreign, agents)
}

fn keyed(env: &Env, labs: &mut [Lab<'_>]) {
    section(
        "7. KV keyed by model: a fan-out that crosses models re-encodes its parent's context (P6)",
    );
    let shares: Vec<f64> = (env.seed..env.seed + env.seeds)
        .map(|seed| {
            let (foreign, agents) = foreign_agents(seed, env.ops, env.fanout);
            100.0 * foreign as f64 / agents.max(1) as f64
        })
        .collect();
    println!(
        "\n  fan-out agents on a model other than their parent's: {}",
        plain_seeds(&shares, 1, "%")
    );
    let with_keys = |fleet: FleetArgs| FleetArgs {
        model_keyed: true,
        ..fleet
    };
    for regime in Regime::ALL {
        println!("\n  {}", regime.label());
        for (name, fleet) in [
            ("free", FLEET),
            ("takes engine time", prefill_arm(FLEET, 1.0, 0.0)),
        ] {
            let plain_runs = runs(labs, regime, fleet, OFF);
            let keyed_runs = runs(labs, regime, with_keys(fleet), OFF);
            let plain = cells(&plain_runs, env.rate);
            let keyed = cells(&keyed_runs, env.rate);
            let work: Vec<f64> = plain_runs
                .iter()
                .zip(&keyed_runs)
                .map(|(p, k)| {
                    change(
                        p.mach.prefill_work_ns() as f64,
                        k.mach.prefill_work_ns() as f64,
                    )
                })
                .collect();
            println!(
                "    prefill {name}: service {}, stall {}, prefill work {}",
                seeds_of(&against(&plain, &keyed, |c| c.service), "%"),
                seeds_of(&against(&plain, &keyed, |c| c.stall), "%"),
                seeds_of(&work, "%"),
            );
        }
    }
}

fn scaled_envs(
    env: &Env,
    nodes: usize,
    rate: f64,
    ops_mult: u64,
    dram_per_node: u64,
) -> Vec<LabEnv> {
    let per = |total: u64| total / env.nodes as u64 * nodes as u64;
    (env.seed..env.seed + env.seeds)
        .map(|seed| LabEnv {
            nodes,
            units_per_node: env.units_per_node,
            hbm: per(env.hbm),
            dram: dram_per_node * nodes as u64,
            nvme: per(env.nvme),
            ops: env.ops * ops_mult,
            seed,
            rate,
            fanout: env.fanout,
            sections: String::new(),
        })
        .collect()
}

fn runs_with(labs: &mut [Lab<'_>], regime: Regime, a: &Arm, fleet: FleetArgs) -> Vec<ArmRun> {
    labs.iter_mut()
        .map(|lab| {
            lab.go_fleet(
                Distance::Rack,
                regime,
                a,
                BeliefArgs::OFF,
                OFF,
                fleet,
                false,
            )
        })
        .collect()
}

fn placed_fleet(replicas: Option<[u8; 4]>, one_model: bool) -> FleetArgs {
    FleetArgs {
        fleet: true,
        model_keyed: true,
        model_batches: ModelBatchesArg::Priced,
        replicas,
        one_model,
        ..FLEET
    }
}

fn tenant_window(r: &ArmRun) -> [f64; 7] {
    let lo = (r.t.window.0 * r.t.base as f64) as u64;
    let hi = (r.t.window.1 * r.t.base as f64) as u64;
    let mut inside = Vec::new();
    let (mut outside, mut neighbour) = (Vec::new(), Vec::new());
    for &(at, tenant, service) in &r.t.tenant_samples {
        if tenant == polyphonic::work::NEIGHBOUR_TENANT {
            neighbour.push(service);
        } else if (lo..hi).contains(&at) {
            inside.push(service);
        } else {
            outside.push(service);
        }
    }
    let refused = r
        .mach
        .tenant_refused
        .get(&polyphonic::work::NEIGHBOUR_TENANT)
        .copied()
        .unwrap_or(0) as f64;
    let others_refused: f64 = r
        .mach
        .tenant_refused
        .iter()
        .filter(|(t, _)| **t != polyphonic::work::NEIGHBOUR_TENANT)
        .map(|(_, n)| *n as f64)
        .fold(0.0, |a, n| a + n);
    let served_others = inside.len() + outside.len();
    let whole: Vec<u64> = inside.iter().chain(&outside).copied().collect();
    [
        mean_of(&inside),
        mean_of(&outside),
        ms(quantile(&inside, 0.99)),
        mean_of(&neighbour),
        100.0 * refused / (refused + neighbour.len() as f64).max(1.0),
        100.0 * others_refused / (others_refused + served_others as f64).max(1.0),
        mean_of(&whole),
    ]
}

fn group_service(r: &ArmRun) -> [f64; 3] {
    let mut by_tenant: HashMap<u32, Vec<u64>> = HashMap::new();
    for &(_, tenant, service) in &r.t.tenant_samples {
        if tenant < TENANT_COUNT {
            by_tenant.entry(tenant).or_default().push(service);
        }
    }
    let mut ranked: Vec<u32> = by_tenant.keys().copied().collect();
    ranked.sort_by_key(|t| (std::cmp::Reverse(by_tenant[t].len()), *t));
    let mean_over = |tenants: &[u32]| {
        let all: Vec<u64> = tenants
            .iter()
            .flat_map(|t| by_tenant[t].iter().copied())
            .collect();
        mean_of(&all)
    };
    let half = ranked.len() / 2;
    [
        mean_over(&ranked[..1.min(ranked.len())]),
        mean_over(&ranked[ranked.len() - half..]),
        mean_over(&ranked),
    ]
}

fn pairing_fleet(prefillers: usize, rule: PairingArg, over_ms: f64, fresh: f64) -> FleetArgs {
    FleetArgs {
        fleet: true,
        model_keyed: true,
        one_model: true,
        model_batches: ModelBatchesArg::Priced,
        prefill_time: PrefillArg::Priced,
        replicas: Some([8, 0, 0, 0]),
        prefillers,
        pairing: rule,
        pair_over: over_ms,
        fresh,
        ..FLEET
    }
}

fn pairing_rows(
    labs: &mut [Lab<'_>],
    rate: f64,
    aggregated: &[Cell],
    label: &str,
    fleet: FleetArgs,
) {
    let cs = cells(
        &runs_with(labs, Regime::Defaults, &scored_fetch(), fleet),
        rate,
    );
    println!(
        "      {label}: service {}, paired {}, coupled {}, prefiller work {} of what it replaced, \
         wait {}",
        seeds_of(&against(aggregated, &cs, |c| c.service), "%"),
        plain_seeds(&col(&cs, |c| c.paired), 0, "%"),
        plain_seeds(&col(&cs, |c| c.coupled), 0, "%"),
        plain_seeds(&col(&cs, |c| c.duplicated), 2, "x"),
        plain_seeds(&col(&cs, |c| c.wait_ms), 1, " ms"),
    );
}

fn pairing(env: &Env) {
    section("8. prefill and decode: three rules, the ratio, two mixes, two rates (P7)");
    for rate in [300.0, 500.0] {
        let envs = scaled_envs(env, 8, rate, 2, env.dram / env.nodes as u64);
        let mut labs: Vec<Lab<'_>> = envs.iter().map(Lab::new).collect();
        for (mix, fresh) in [
            ("the published mix", 0.0),
            ("plus fresh 64-block prompts, 0.15 a request", 0.15),
        ] {
            println!(
                "\n  8 replicas of one model, {rate:.0} req/s, {} ops, {mix}",
                env.ops * 2
            );
            pairing_cell(&mut labs, rate, fresh);
        }
    }
}

fn pairing_cell(labs: &mut [Lab<'_>], rate: f64, fresh: f64) {
    let nodes = 8;
    let aggregated = cells(
        &runs_with(
            labs,
            Regime::Defaults,
            &scored_fetch(),
            pairing_fleet(0, PairingArg::Off, 0.0, fresh),
        ),
        rate,
    );
    println!(
        "    eight aggregated: mean service {}, decode stretched by {}, full batch {}",
        plain_seeds(&col(&aggregated, |c| c.service), 1, " ms"),
        plain_seeds(&col(&aggregated, |c| c.stretch), 1, "%"),
        plain_seeds(&col(&aggregated, |c| c.full), 1, "%"),
    );
    let fetching = |p| FleetArgs {
        prefill_fetch: true,
        ..pairing_fleet(p, PairingArg::Joint, 0.0, fresh)
    };
    let arms: Vec<(String, FleetArgs)> = (1..=4)
        .map(|p| {
            (
                format!("joint, {p} prefill : {} decode", nodes - p),
                pairing_fleet(p, PairingArg::Joint, 0.0, fresh),
            )
        })
        .chain((1..=3).map(|p| {
            (
                format!(
                    "joint, the prefiller may fetch the prefix, {p} : {}",
                    nodes - p
                ),
                fetching(p),
            )
        }))
        .chain((1..=3).map(|p| {
            (
                format!("independent, over 10 ms, {p} : {}", nodes - p),
                pairing_fleet(p, PairingArg::Independent, 10.0, fresh),
            )
        }))
        .chain((1..=3).map(|p| {
            (
                format!("list, every prefill, {p} : {}", nodes - p),
                pairing_fleet(p, PairingArg::List, 0.0, fresh),
            )
        }))
        .chain([(
            "list, over 10 ms, 3 : 5".to_string(),
            pairing_fleet(3, PairingArg::List, 10.0, fresh),
        )])
        .collect();
    for (label, fleet) in arms {
        pairing_rows(labs, rate, &aggregated, &label, fleet);
    }
    for (name, planner) in [("follow", PlannerArg::Follow), ("eager", PlannerArg::Eager)] {
        let fleet = FleetArgs {
            planner,
            ..pairing_fleet(0, PairingArg::Joint, 0.0, fresh)
        };
        let cs = cells(
            &runs_with(labs, Regime::Defaults, &scored_fetch(), fleet),
            rate,
        );
        println!(
            "      joint, planner {name} chooses the ratio: service {}, prefillers at the end {}, \
             role moves {}, paired {}",
            seeds_of(&against(&aggregated, &cs, |c| c.service), "%"),
            cs.iter()
                .map(|c| c.prefillers.to_string())
                .collect::<Vec<_>>()
                .join(" / "),
            cs.iter()
                .map(|c| c.role_moves.to_string())
                .collect::<Vec<_>>()
                .join(" / "),
            plain_seeds(&col(&cs, |c| c.paired), 0, "%"),
        );
    }
}

fn routing(env: &Env) {
    section("3. routing on the fleet: the score's lead by replicas per model (P2)");
    let hash = arm(
        "hash only",
        Placement::Sticky,
        false,
        Control::Unified,
        false,
    );
    let scored = scored_fetch();
    let per_node = env.dram / env.nodes as u64;
    let shapes = [
        (
            "any node, any model, one batch per node (published engine)",
            FLEET,
            1,
        ),
        (
            "one model, eight replicas",
            placed_fleet(Some([8, 0, 0, 0]), true),
            1,
        ),
        (
            "four models, two replicas each",
            placed_fleet(Some([2, 2, 2, 2]), false),
            8,
        ),
    ];
    for rate in [500.0, 700.0] {
        println!("\n  8 nodes, {rate:.0} req/s, {} ops", env.ops * 2);
        let envs = scaled_envs(env, 8, rate, 2, per_node);
        let mut labs: Vec<Lab<'_>> = envs.iter().map(Lab::new).collect();
        for (name, fleet, layouts) in shapes {
            println!("    {name}");
            let mut leads = Vec::new();
            for rotate in 0..layouts {
                let fleet = FleetArgs { rotate, ..fleet };
                let h = cells(&runs_with(&mut labs, Regime::Defaults, &hash, fleet), rate);
                let s = cells(
                    &runs_with(&mut labs, Regime::Defaults, &scored, fleet),
                    rate,
                );
                let tag = if layouts > 1 {
                    format!(" (layout {rotate})")
                } else {
                    String::new()
                };
                println!(
                    "      mean service{tag}: hash only {}, scored + fetch {}",
                    plain_seeds(&col(&h, |c| c.service), 1, " ms"),
                    plain_seeds(&col(&s, |c| c.service), 1, " ms"),
                );
                let lead = against(&h, &s, |c| c.service);
                println!(
                    "      scored + fetch against hash only{tag}: service {}, stall {}",
                    seeds_of(&lead, "%"),
                    seeds_of(&against(&h, &s, |c| c.stall), "%"),
                );
                leads.push(lead);
            }
            if layouts > 1 {
                let all: Vec<f64> = leads.iter().flatten().copied().collect();
                let lo = all.iter().copied().fold(f64::MAX, f64::min);
                let hi = all.iter().copied().fold(f64::MIN, f64::max);
                println!("      the lead over the layouts and seeds: {lo:+.1}% to {hi:+.1}%");
            }
        }
    }
}

fn placed(env: &Env, labs: &mut [Lab<'_>]) {
    section("5. the partition: one model per node, and what its size adds (P1, P4)");
    for regime in Regime::ALL {
        println!("\n  {}", regime.label());
        let grant = match regime {
            Regime::Defaults => GIB,
            Regime::Half => GIB / 2,
        };
        let base = cells(&runs(labs, regime, FLEET, OFF), env.rate);
        let lazy = cells(
            &runs(
                labs,
                regime,
                FleetArgs {
                    model_batches: ModelBatchesArg::Priced,
                    ..FLEET
                },
                OFF,
            ),
            env.rate,
        );
        let arms = [
            (
                "one model per node, the partition the weights leave",
                placed_fleet(None, false),
            ),
            (
                "one model per node, at the published grant",
                FleetArgs {
                    fleet_partition: Some(grant),
                    ..placed_fleet(None, false)
                },
            ),
        ];
        println!(
            "    the published engine: mean service {}, preempted {}",
            plain_seeds(&col(&base, |c| c.service), 1, " ms"),
            plain_seeds(&col(&base, |c| c.preempted), 1, "%"),
        );
        println!(
            "    a batch per model, lazy weights: {}, against the published engine {}",
            plain_seeds(&col(&lazy, |c| c.service), 1, " ms"),
            seeds_of(&against(&base, &lazy, |c| c.service), "%"),
        );
        for (name, fleet) in arms {
            let cs = cells(&runs(labs, regime, fleet, OFF), env.rate);
            println!(
                "    {name}: {}, against the published engine {}, against lazy weights {}",
                plain_seeds(&col(&cs, |c| c.service), 1, " ms"),
                seeds_of(&against(&base, &cs, |c| c.service), "%"),
                seeds_of(&against(&lazy, &cs, |c| c.service), "%"),
            );
            println!(
                "      preempted {}, unplaced {}",
                plain_seeds(&col(&cs, |c| c.preempted), 1, "%"),
                cs.iter()
                    .map(|c| c.unplaced.to_string())
                    .collect::<Vec<_>>()
                    .join(" / "),
            );
        }
    }
}

fn offload(env: &Env) {
    section("5, continued. the connector's host-DDR offload grant (P4)");
    for dram_gib in [8u64, 4] {
        println!("\n  {dram_gib} GiB of DDR per node");
        let envs = scaled_envs(env, env.nodes, env.rate, 1, dram_gib * GIB);
        let mut labs: Vec<Lab<'_>> = envs.iter().map(Lab::new).collect();
        let mut base: Vec<Cell> = Vec::new();
        for (name, mib) in [
            ("as sized", None),
            ("no offload", Some(0u64)),
            ("128 MiB", Some(128)),
            ("256 MiB", Some(256)),
            ("410 MiB", Some(410)),
            ("819 MiB", Some(819)),
            ("1.6 GiB", Some(1638)),
        ] {
            let fleet = FleetArgs {
                kv_offload: mib.map(|m| m << 20),
                ..FLEET
            };
            let cs = cells(&runs(&mut labs, Regime::Defaults, fleet, OFF), env.rate);
            let delta = if base.is_empty() {
                String::new()
            } else {
                format!(
                    ", service {}",
                    seeds_of(&against(&base, &cs, |c| c.service), "%")
                )
            };
            println!(
                "    offload {name}: service {}, stall {}, function warm {}{delta}",
                plain_seeds(&col(&cs, |c| c.service), 1, " ms"),
                plain_seeds(&col(&cs, |c| c.stall), 2, " ms"),
                plain_seeds(&col(&cs, |c| c.warm), 0, "%"),
            );
            if base.is_empty() {
                base = cs;
            }
        }
    }
}

type Arms = Vec<(String, Box<dyn Fn(u64) -> FleetArgs>)>;

const HOT_SHARES: [f64; 4] = [0.55, 0.25, 0.12, 0.08];

fn placed_mix(
    planner: PlannerArg,
    start: f64,
    interval: f64,
    late: f64,
    counts: Option<[u8; 4]>,
) -> FleetArgs {
    FleetArgs {
        fleet: true,
        model_keyed: true,
        model_batches: ModelBatchesArg::Priced,
        model_mix: Some(HOT_SHARES),
        planner,
        start,
        interval,
        late,
        replicas: counts,
        ..FLEET
    }
}

const SIZES: [f64; 4] = [0.5, 1.0, 1.0, 2.0];

fn sized(shares: [f64; 4], planner: PlannerArg, counts: Option<[u8; 4]>) -> FleetArgs {
    FleetArgs {
        sizes: Some(SIZES),
        model_mix: Some(shares),
        ..placed_mix(planner, 8.0, 5.0, 0.0, counts)
    }
}

fn sizes(env: &Env) {
    section(
        "5, continued. three model sizes: the planner's allocation, and the rotating mix again (P3)",
    );
    let (nodes, rate, ops) = (8, env.rate * 2.0, env.ops * 8);
    let envs = scaled_envs(env, nodes, rate, 8, env.dram / env.nodes as u64);
    let mut labs: Vec<Lab<'_>> = envs.iter().map(Lab::new).collect();
    println!(
        "\n  {nodes} nodes, {rate:.0} req/s, {ops} ops; models of {SIZES:?} GiB, each node's step base \
         scaled by its model's size and its partition what the weights leave"
    );
    let even = Some([2, 2, 2, 2]);
    let arithmetic = Some([1, 2, 2, 3]);
    let equal = [0.25; 4];
    let arms: [(&str, FleetArgs); 7] = [
        (
            "equal demand, two replicas a model",
            sized(equal, PlannerArg::None, even),
        ),
        (
            "equal demand, placed at the arithmetic's 1 / 2 / 2 / 3",
            sized(equal, PlannerArg::None, arithmetic),
        ),
        (
            "equal demand, from two a model, planner eager",
            sized(equal, PlannerArg::Eager, even),
        ),
        (
            "equal demand, from two a model, planner follow",
            sized(equal, PlannerArg::Follow, even),
        ),
        (
            "rotating mix, oracle",
            sized(HOT_SHARES, PlannerArg::Oracle, None),
        ),
        (
            "rotating mix, planner follow",
            sized(HOT_SHARES, PlannerArg::Follow, None),
        ),
        (
            "rotating mix, planner once",
            sized(HOT_SHARES, PlannerArg::Once, None),
        ),
    ];
    let mut reference: Vec<Cell> = Vec::new();
    for (name, fleet) in arms {
        let cs = cells(
            &runs_with(&mut labs, Regime::Defaults, &scored_fetch(), fleet),
            rate,
        );
        println!("\n    {name}");
        println!(
            "      mean service {}, p99 {}, decodes arriving at a full batch {}",
            plain_seeds(&col(&cs, |c| c.service), 1, " ms"),
            plain_seeds(&col(&cs, |c| c.p99), 0, " ms"),
            plain_seeds(&col(&cs, |c| c.full), 1, "%"),
        );
        println!(
            "      replicas at the end {}, replica loads {}, unplaced {}",
            cs.iter()
                .map(|c| format!("{:?}", c.counts))
                .collect::<Vec<_>>()
                .join(" / "),
            cs.iter()
                .map(|c| c.moves.to_string())
                .collect::<Vec<_>>()
                .join(" / "),
            cs.iter()
                .map(|c| c.unplaced.to_string())
                .collect::<Vec<_>>()
                .join(" / "),
        );
        if name.starts_with("equal demand, two") || name.ends_with("oracle") {
            reference = cs;
            continue;
        }
        println!(
            "      against {}: service {}",
            if name.starts_with("equal") {
                "two replicas a model"
            } else {
                "the oracle"
            },
            seeds_of(&against(&reference, &cs, |c| c.service), "%"),
        );
    }
}

fn clock_arms(fanout: f64, nodes: usize, rate: f64, ops: u64) -> Arms {
    let mix = Some(HOT_SHARES);
    let costs = Costs::published(5_000_000_000);
    let counts = move |seed: u64, first: bool, fanout: f64| -> [u8; 4] {
        let trace: Vec<Request> = placed_mix(PlannerArg::None, 8.0, 5.0, 0.0, None)
            .workload(Workload::with_fanout(seed, ops, 0.0, fanout))
            .collect();
        let d = phase_demand(&trace, (1e9 / rate) as u64);
        costs
            .best_counts(if first { &d.per_phase[0] } else { &d.overall }, nodes)
            .map(|n| n as u8)
    };
    let mut arms: Arms = vec![
        (
            "any node, any model, one batch per node (published engine)".into(),
            Box::new(move |_| FleetArgs {
                model_mix: mix,
                ..FLEET
            }),
        ),
        (
            "lazy weights, a batch per model".into(),
            Box::new(move |_| FleetArgs {
                model_mix: mix,
                model_batches: ModelBatchesArg::Priced,
                ..FLEET
            }),
        ),
        (
            "placed once, for the first phase".into(),
            Box::new(move |seed| {
                placed_mix(
                    PlannerArg::None,
                    8.0,
                    5.0,
                    0.0,
                    Some(counts(seed, true, fanout)),
                )
            }),
        ),
        (
            "placed once, for the run's mean mix".into(),
            Box::new(move |seed| {
                placed_mix(
                    PlannerArg::None,
                    8.0,
                    5.0,
                    0.0,
                    Some(counts(seed, false, fanout)),
                )
            }),
        ),
        (
            "planner once, after the first interval".into(),
            Box::new(move |_| placed_mix(PlannerArg::Once, 8.0, 5.0, 0.0, None)),
        ),
    ];
    for start in [0.0, 2.0, 8.0, 30.0] {
        arms.push((
            format!("oracle, {start:.0} s to start"),
            Box::new(move |_| placed_mix(PlannerArg::Oracle, start, 5.0, 0.0, None)),
        ));
    }
    for late in [10.0, 30.0] {
        arms.push((
            format!("oracle, {late:.0} s late, 8 s to start"),
            Box::new(move |_| placed_mix(PlannerArg::Oracle, 8.0, 5.0, late, None)),
        ));
    }
    arms.push((
        "planner follow, 5 s interval, 8 s to start".into(),
        Box::new(move |_| placed_mix(PlannerArg::Follow, 8.0, 5.0, 0.0, None)),
    ));
    arms.push((
        "planner eager, 5 s interval, 8 s to start".into(),
        Box::new(move |_| placed_mix(PlannerArg::Eager, 8.0, 5.0, 0.0, None)),
    ));
    for interval in [1.0, 15.0] {
        arms.push((
            format!("planner follow, {interval:.0} s interval, 8 s to start"),
            Box::new(move |_| placed_mix(PlannerArg::Follow, 8.0, interval, 0.0, None)),
        ));
    }
    for start in [2.0, 30.0] {
        arms.push((
            format!("planner follow, 5 s interval, {start:.0} s to start"),
            Box::new(move |_| placed_mix(PlannerArg::Follow, start, 5.0, 0.0, None)),
        ));
    }
    arms
}

fn clock(env: &Env) {
    section(
        "4. the clock: a rotating model mix, and what a placement costs when it is late (P3, P9)",
    );
    let nodes = 8;
    let rate = env.rate * 2.0;
    let ops = env.ops * 8;
    let envs = scaled_envs(env, nodes, rate, 8, env.dram / env.nodes as u64);
    let mut labs: Vec<Lab<'_>> = envs.iter().map(Lab::new).collect();
    println!(
        "\n  {nodes} nodes, {rate:.0} req/s, {ops} ops ({:.0} s); demand by model {HOT_SHARES:?}, the hot \
         model rotating one place each phase, the class mix held flat; a replica changing model \
         serves nothing for its start time and the copy",
        ops as f64 / rate
    );
    let arms = clock_arms(env.fanout, nodes, rate, ops);
    let mut published: Vec<Cell> = Vec::new();
    let mut oracle: HashMap<u64, Vec<Cell>> = HashMap::new();
    for (name, args) in &arms {
        let shapes: Vec<FleetArgs> = (0..labs.len()).map(|i| args(env.seed + i as u64)).collect();
        let rs: Vec<ArmRun> = labs
            .iter_mut()
            .zip(&shapes)
            .map(|(lab, &shape)| {
                lab.go_fleet(
                    Distance::Rack,
                    Regime::Defaults,
                    &scored_fetch(),
                    BeliefArgs::OFF,
                    OFF,
                    shape,
                    false,
                )
            })
            .collect();
        let cs = cells(&rs, rate);
        println!("\n    {name}");
        println!(
            "      mean service {}, p99 {}",
            plain_seeds(&col(&cs, |c| c.service), 1, " ms"),
            plain_seeds(&col(&cs, |c| c.p99), 0, " ms"),
        );
        if published.is_empty() {
            published = cs;
            continue;
        }
        let by_phase: Vec<String> = (0..PHASES)
            .map(|p| {
                format!(
                    "{:.1}",
                    cs.iter().map(|c| c.phase[p]).sum::<f64>() / cs.len() as f64
                )
            })
            .collect();
        println!(
            "      against the published engine: service {}; by phase (mean over seeds) {} ms",
            seeds_of(&against(&published, &cs, |c| c.service), "%"),
            by_phase.join(" / "),
        );
        println!(
            "      decodes arriving at a full batch {}, replica loads {}, unplaced {}",
            plain_seeds(&col(&cs, |c| c.full), 1, "%"),
            cs.iter()
                .map(|c| c.moves.to_string())
                .collect::<Vec<_>>()
                .join(" / "),
            cs.iter()
                .map(|c| c.unplaced.to_string())
                .collect::<Vec<_>>()
                .join(" / "),
        );
        let shape = shapes[0];
        let start = shape.start.to_bits();
        if shape.planner == PlannerArg::Oracle && shape.late <= 0.0 {
            oracle.insert(start, cs);
            continue;
        }
        if let Some(reference) = oracle.get(&start) {
            println!(
                "      against the oracle with the same start: service {}",
                seeds_of(&against(reference, &cs, |c| c.service), "%"),
            );
        }
    }
}

fn header(env: &Env) {
    println!(
        "the engine's corrections and the fleet's decisions (phase-6.md \u{a7}4.14)\n\
         cluster: {} nodes, {:.0} GiB HBM + {:.0} GiB DDR, {} req/s, {:.0}% fan-out, ops={} seeds {}..{}\n\
         arm: scored + fetch, flow-aware, unified control, no control crossing charged, exact view; \
         the engine's grants are sized from the ledger's own run of the same trace at each regime\n\
         every figure is one value per seed; a change is against the same seed with the bit off\n",
        env.nodes,
        env.hbm as f64 / (1u64 << 30) as f64,
        env.dram as f64 / (1u64 << 30) as f64,
        env.rate,
        100.0 * env.fanout,
        env.ops,
        env.seed,
        env.seed + env.seeds - 1,
    );
}

pub fn run(env: &Env) {
    header(env);
    let envs = env.lab_envs(env.fanout);
    let mut labs: Vec<Lab<'_>> = envs.iter().map(Lab::new).collect();
    let wanted: Vec<&str> = env.sections.split(',').map(str::trim).collect();
    let on = |name: &str| wanted.contains(&name);
    if on("gate") {
        gate(env, &mut labs);
    }
    if on("batches") {
        batches(env, &mut labs);
    }
    if on("routing") {
        routing(env);
    }
    if on("clock") {
        clock(env);
    }
    if on("placed") {
        placed(env, &mut labs);
    }
    if on("offload") {
        offload(env);
    }
    if on("sizes") {
        sizes(env);
    }
    if on("prefill") {
        prefill(env, &mut labs);
    }
    if on("keyed") {
        keyed(env, &mut labs);
    }
    if on("pairing") {
        pairing(env);
    }
    if on("tenants") {
        tenants(env, &mut labs);
    }
    if on("duty") {
        duty(env);
    }
}

fn tenants(env: &Env, labs: &mut [Lab<'_>]) {
    section(
        "9. tenants: what is shared, what a neighbour costs, and what isolation costs (P8, P9)",
    );
    sharing(env, labs);
    neighbour(env);
    ceiling(labs);
    writes(env, labs);
}

fn sharing(env: &Env, labs: &mut [Lab<'_>]) {
    let tracked = FleetArgs {
        tenants: true,
        ..FLEET
    };
    for regime in Regime::ALL {
        println!("\n  {}", regime.label());
        for (name, shared) in [
            ("the published workload", false),
            ("a prefix per model under every tenant's", true),
        ] {
            let fleet = FleetArgs {
                shared_prefix: shared,
                ..tracked
            };
            let rs = runs(labs, regime, fleet, OFF);
            let rows: Vec<String> = rs
                .iter()
                .map(|r| {
                    let t = r.mach.instruments.tenants.as_ref().expect("tenants on");
                    let hits: u64 = t.by_origin.iter().map(|o| o.1).sum();
                    let own = t.by_origin[Origin::Tenant.idx()];
                    let ranked = t.by_volume(TENANT_COUNT);
                    format!(
                        "cross {} of {} touches; prefix {:.0}% of reads, hit {:.0}%, {:.0}% of hits; \
                         evictions by another owner {:.1}%; busiest tenant hit {:.0}%, quietest twelve {:.0}%",
                        t.cross_touches,
                        t.touches,
                        100.0 * own.0 as f64 / t.touches.max(1) as f64,
                        100.0 * own.1 as f64 / own.0.max(1) as f64,
                        100.0 * own.1 as f64 / hits.max(1) as f64,
                        100.0 * t.evictions_by_other as f64 / t.evictions.max(1) as f64,
                        100.0 * t.hit_rate_of(ranked[..1].iter().copied()),
                        100.0 * t.hit_rate_of(ranked[QUIET_FROM..].iter().copied()),
                    )
                })
                .collect();
            println!("    {name}");
            for (i, row) in rows.iter().enumerate() {
                println!("      seed {}: {row}", env.seed + i as u64);
            }
            let cs = cells(&rs, env.rate);
            println!(
                "      mean service {}",
                plain_seeds(&col(&cs, |c| c.service), 1, " ms")
            );
        }
    }
}

fn tenant_fleet(priced: bool, neighbour: f64) -> FleetArgs {
    FleetArgs {
        prefill_time: if priced {
            PrefillArg::Priced
        } else {
            PrefillArg::Off
        },
        neighbour,
        ..pairing_fleet(0, PairingArg::Off, 0.0, 0.0)
    }
}

fn neighbour(env: &Env) {
    for rate in [500.0, 300.0] {
        neighbour_at(env, rate);
    }
}

fn neighbour_at(env: &Env, rate: f64) {
    let nodes = 8;
    let envs = scaled_envs(env, nodes, rate, 2, env.dram / env.nodes as u64);
    let mut labs: Vec<Lab<'_>> = envs.iter().map(Lab::new).collect();
    for priced in [false, true] {
        println!(
            "\n  the neighbour, 8 replicas of one model, {rate:.0} req/s, {} ops, prefill {}; \
             the others' mean service inside the burst against the shared fleet with no burst",
            env.ops * 2,
            if priced {
                "takes engine time"
            } else {
                "is free"
            }
        );
        let base = cells(
            &runs_with(
                &mut labs,
                Regime::Defaults,
                &scored_fetch(),
                tenant_fleet(priced, 0.0),
            ),
            rate,
        );
        println!(
            "    no burst, shared: inside {}, outside {}, p99 inside {}",
            plain_seeds(&col(&base, |c| c.in_window), 1, " ms"),
            plain_seeds(&col(&base, |c| c.out_window), 1, " ms"),
            plain_seeds(&col(&base, |c| c.in_p99), 0, " ms"),
        );
        for burst in [0.1, 0.2] {
            let mut arms: Vec<(String, FleetArgs)> =
                vec![("shared".to_string(), tenant_fleet(priced, burst))];
            for n in [1, 2] {
                arms.push((
                    format!("{n} of 8 replicas the neighbour's"),
                    FleetArgs {
                        tenant_set: n,
                        ..tenant_fleet(priced, burst)
                    },
                ));
            }
            if priced {
                for q in [0.25, 0.5, 1.0] {
                    arms.push((
                        format!("quota {q} engine-seconds a second"),
                        FleetArgs {
                            tenant_prefill: q,
                            ..tenant_fleet(priced, burst)
                        },
                    ));
                }
            }
            for n in [1, 4] {
                arms.push((
                    format!("{n} sequences in flight a tenant"),
                    FleetArgs {
                        tenant_slots: n,
                        ..tenant_fleet(priced, burst)
                    },
                ));
            }
            println!("    burst at {burst}");
            for (name, fleet) in arms {
                let cs = cells(
                    &runs_with(&mut labs, Regime::Defaults, &scored_fetch(), fleet),
                    rate,
                );
                println!(
                    "      {name}: others inside {}, p99 inside {}, outside {}, others refused {}; the neighbour's own {}, refused {}",
                    seeds_of(&against(&base, &cs, |c| c.in_window), "%"),
                    seeds_of(&against(&base, &cs, |c| c.in_p99), "%"),
                    seeds_of(&against(&base, &cs, |c| c.out_window), "%"),
                    plain_seeds(&col(&cs, |c| c.others_refused), 1, "%"),
                    plain_seeds(&col(&cs, |c| c.neighbour), 0, " ms"),
                    plain_seeds(&col(&cs, |c| c.neighbour_refused), 0, "%"),
                );
            }
        }
    }
}

fn ceiling(labs: &mut [Lab<'_>]) {
    println!(
        "\n  the ceiling: each tenant's KV kept from other tenants' evictions, per tenant per node"
    );
    for regime in Regime::ALL {
        println!("\n  {}", regime.label());
        for (name, fleet) in [
            ("prefill free", FLEET),
            ("prefill takes engine time", prefill_arm(FLEET, 1.0, 0.0)),
        ] {
            let tracked = FleetArgs {
                tenants: true,
                ..fleet
            };
            let base_runs = runs(labs, regime, tracked, OFF);
            let quiet_hit = |rs: &[ArmRun]| -> Vec<f64> {
                rs.iter()
                    .map(|r| {
                        let t = r.mach.instruments.tenants.as_ref().expect("tenants on");
                        100.0
                            * t.hit_rate_of(t.by_volume(TENANT_COUNT)[QUIET_FROM..].iter().copied())
                    })
                    .collect()
            };
            println!(
                "    {name}: quietest twelve's hit rate without a floor {}",
                plain_seeds(&quiet_hit(&base_runs), 1, "%")
            );
            let partition = base_runs[0].mach.kv_partition_bytes();
            let base: Vec<[f64; 3]> = base_runs.iter().map(group_service).collect();
            println!(
                "    {name}, partition {:.2} GiB",
                partition as f64 / GIB as f64
            );
            for share in [24u64, 12, 6] {
                let floor = partition / share;
                let rs = runs(
                    labs,
                    regime,
                    FleetArgs {
                        tenant_floor: Some(floor),
                        ..tracked
                    },
                    OFF,
                );
                let groups: Vec<[f64; 3]> = rs.iter().map(group_service).collect();
                let changes = |g: usize| -> Vec<f64> {
                    base.iter()
                        .zip(&groups)
                        .map(|(b, f)| change(b[g], f[g]))
                        .collect()
                };
                println!(
                    "      a {}th of the partition each: busiest tenant {}, quietest twelve {}, all {}; quietest twelve hit {}",
                    share,
                    seeds_of(&changes(0), "%"),
                    seeds_of(&changes(1), "%"),
                    seeds_of(&changes(2), "%"),
                    plain_seeds(&quiet_hit(&rs), 1, "%"),
                );
            }
        }
    }
}

fn writes(env: &Env, labs: &mut [Lab<'_>]) {
    println!("\n  the tiers' write rates (P9)");
    let rs = runs(labs, Regime::Defaults, FLEET, OFF);
    let w = BlobKind::WeightShard.idx();
    for (i, r) in rs.iter().enumerate() {
        let seconds = r.offered as f64 / env.rate;
        println!(
            "    seed {}: lazy weight loads {:.1} a second, {:.0} scored decisions a second, over {seconds:.0} simulated seconds",
            env.seed + i as u64,
            r.mach.engine_ops().admit[w] as f64 / seconds,
            r.mach.decisions as f64 / seconds,
        );
    }
}

fn duty(env: &Env) {
    section(
        "9, continued. the neighbour's duty cycle: where the quota and a replica set cross (P8)",
    );
    let nodes = 8;
    for rate in [500.0, 300.0] {
        let envs = scaled_envs(env, nodes, rate, 2, env.dram / env.nodes as u64);
        let mut labs: Vec<Lab<'_>> = envs.iter().map(Lab::new).collect();
        let base = cells(
            &runs_with(
                &mut labs,
                Regime::Defaults,
                &scored_fetch(),
                tenant_fleet(true, 0.0),
            ),
            rate,
        );
        println!(
            "\n  8 replicas of one model, {rate:.0} req/s, {} ops, prefill takes engine time; \
             no burst: the others' mean service {}",
            env.ops * 2,
            plain_seeds(&col(&base, |c| c.others_run), 1, " ms"),
        );
        let burst_rates: &[f64] = if rate > 400.0 { &[0.1, 0.2] } else { &[0.2] };
        for &burst in burst_rates {
            println!(
                "    burst at {burst} of the request rate: the others over the whole run, against no burst"
            );
            for duty in [0.05, 0.1, 0.2, 0.4, 0.6, 1.0] {
                let arms: [(&str, FleetArgs); 5] = [
                    (
                        "shared",
                        FleetArgs {
                            neighbour_duty: duty,
                            ..tenant_fleet(true, burst)
                        },
                    ),
                    (
                        "1 of 8 replicas",
                        FleetArgs {
                            neighbour_duty: duty,
                            tenant_set: 1,
                            ..tenant_fleet(true, burst)
                        },
                    ),
                    (
                        "quota 1.0",
                        FleetArgs {
                            neighbour_duty: duty,
                            tenant_prefill: 1.0,
                            ..tenant_fleet(true, burst)
                        },
                    ),
                    (
                        "quota 0.5",
                        FleetArgs {
                            neighbour_duty: duty,
                            tenant_prefill: 0.5,
                            ..tenant_fleet(true, burst)
                        },
                    ),
                    (
                        "quota 0.25",
                        FleetArgs {
                            neighbour_duty: duty,
                            tenant_prefill: 0.25,
                            ..tenant_fleet(true, burst)
                        },
                    ),
                ];
                let mut row = format!("      bursting {:>3.0}% of the run:", 100.0 * duty);
                for (name, fleet) in arms {
                    let cs = cells(
                        &runs_with(&mut labs, Regime::Defaults, &scored_fetch(), fleet),
                        rate,
                    );
                    let _ = write!(
                        row,
                        " {name} {} (inside {}, neighbour refused {});",
                        seeds_of(&against(&base, &cs, |c| c.others_run), "%"),
                        seeds_of(&against(&base, &cs, |c| c.in_window), "%"),
                        plain_seeds(&col(&cs, |c| c.neighbour_refused), 0, "%"),
                    );
                }
                println!("{row}");
            }
        }
    }
}
