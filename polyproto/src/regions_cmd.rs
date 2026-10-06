use polyphonic::blob::BlobKind;
use polyphonic::boundary::Cost as Crossing;
use polyphonic::cache::{EngineKv, NodeMemory, Policy};
use polyphonic::engine::Batching;
use polyphonic::fleet::{Catalogue, Costs, Fleet, PUBLISHED_START_NS};
use polyphonic::machine::{
    BudgetRule, Budgets, Control, Machine, Overflow, Placement, RegionMode, Regions, TenantShares,
};
use polyphonic::topo::{Distance, Topology};
use polyphonic::work::{RegionDemand, RegionShape, Workload};
use std::collections::HashMap;

use super::{AdmitArg, ClassTally, ClusterBits, Correct, Detail, correct, drive_with, node_memory};

#[derive(Clone)]
pub struct Env {
    pub regions: usize,
    pub per_region: usize,
    pub slots: usize,
    pub units_per_node: usize,
    pub hbm: u64,
    pub dram: u64,
    pub nvme: u64,
    pub rate: f64,
    pub seconds: f64,
    pub seed: u64,
    pub seeds: u64,
    pub fanout: f64,
    pub rtt: String,
    pub volatility: f64,
    pub decode_kv: bool,
    pub sections: String,
}

#[derive(Clone, Copy, Debug)]
enum Budget {
    Static,
    Clairvoyant { load_s: f64, late_s: f64 },
    Follow { load_s: f64 },
}

#[derive(Clone, Debug)]
struct Cfg {
    label: String,
    mode: Option<RegionMode>,
    price_reach: bool,
    shape: RegionShape,
    overflow: Overflow,
    summary_s: f64,
    own_forwards: bool,
    table_s: Option<f64>,
    budget: Budget,
    placement: Option<Vec<Option<u8>>>,
    schedulers: (usize, f64),
    shares: Option<Shares>,
    residency: f64,
}

#[derive(Clone, Copy, Debug)]
struct Shares {
    headroom: f64,
    lease_s: Option<f64>,
}

impl Cfg {
    fn new(label: &str, mode: RegionMode, price_reach: bool, shape: &RegionShape) -> Self {
        Self {
            label: label.to_string(),
            mode: Some(mode),
            price_reach,
            shape: shape.clone(),
            overflow: Overflow::Off,
            summary_s: 0.0,
            own_forwards: false,
            table_s: None,
            budget: Budget::Static,
            placement: None,
            schedulers: (1, 0.0),
            shares: None,
            residency: 0.0,
        }
    }

    fn overflowing(
        label: &str,
        shape: &RegionShape,
        overflow: Overflow,
        summary_s: f64,
        own_forwards: bool,
    ) -> Self {
        Self {
            overflow,
            summary_s,
            own_forwards,
            ..Self::new(label, RegionMode::Regional, true, shape)
        }
    }
}

struct Out {
    t: ClassTally,
    mach: Machine,
}

const AZURE_ONE_WAY_NS: [[u64; 3]; 3] = [
    [0, 42_000_000, 81_000_000],
    [42_000_000, 0, 116_000_000],
    [81_000_000, 116_000_000, 0],
];

fn one_way(name: &str, regions: usize) -> Vec<Vec<u64>> {
    (0..regions)
        .map(|a| {
            (0..regions)
                .map(|b| match name {
                    _ if a == b => 0,
                    "azure" if regions == AZURE_ONE_WAY_NS.len() => AZURE_ONE_WAY_NS[a][b],
                    "near" => 15_000_000,
                    "far" => 75_000_000,
                    _ => Distance::Region.one_way_ns(),
                })
                .collect()
        })
        .collect()
}

fn p3(env: &Env) -> Correct {
    correct(env.decode_kv, AdmitArg::None)
}

fn total_rate(env: &Env) -> f64 {
    env.rate * env.regions as f64
}

fn ops(env: &Env) -> u64 {
    (total_rate(env) * env.seconds) as u64
}

const MODEL_SHARES: [f64; 4] = [0.55, 0.25, 0.12, 0.08];

const MODEL_DECODE_SHARE: f64 = 0.36;

const MODEL_TOKENS: f64 = 124.0;

const MODEL_OVERLOAD_NS: u64 = 5_000_000_000;

const TENANT_DEPTH_S: f64 = 2.0;

const BUDGET_STEP_S: f64 = 5.0;

const BUDGET_SECONDS: f64 = 240.0;

const BUDGET_SPARE_SLOTS: usize = 2;

fn slots(env: &Env) -> usize {
    env.slots.max(env.per_region)
}

fn workload(env: &Env, seed: u64, shape: &RegionShape, regions: bool, fleet: bool) -> Workload {
    let w = Workload::with_fanout(seed, ops(env), env.volatility, env.fanout);
    let w = if fleet {
        w.with_model_keyed(true)
            .with_model_mix([MODEL_SHARES; polyphonic::work::PHASES])
    } else {
        w
    };
    let w = if regions {
        w.with_regions(Some(RegionDemand {
            count: env.regions,
            shape: shape.clone(),
        }))
    } else {
        w
    };
    p3(env).workload(w)
}

fn topology(env: &Env) -> Topology {
    Topology::regions(
        slots(env),
        env.units_per_node,
        env.dram,
        Distance::Rack,
        &one_way(&env.rtt, env.regions),
        Crossing::default(),
    )
}

fn memory(env: &Env) -> NodeMemory {
    node_memory(env.hbm, env.dram, env.nvme, [0, 1, 2, 1], false)
}

fn proportional_counts(shares: &[f64], total: usize, cap: usize) -> Vec<usize> {
    let mut counts = vec![1usize; shares.len()];
    for _ in shares.len()..total {
        let Some(best) = (0..shares.len())
            .filter(|&r| counts[r] < cap)
            .max_by(|&a, &b| {
                (shares[a] / counts[a] as f64)
                    .total_cmp(&(shares[b] / counts[b] as f64))
                    .then(b.cmp(&a))
            })
        else {
            break;
        };
        counts[best] += 1;
    }
    counts
}

fn clairvoyant_plan(env: &Env, shape: &RegionShape, late_s: f64) -> Vec<(u64, Vec<usize>)> {
    let demand = RegionDemand {
        count: env.regions,
        shape: shape.clone(),
    };
    let total = env.per_region * env.regions;
    let mut current = vec![env.per_region; env.regions];
    let mut plan = Vec::new();
    for i in 0..(env.seconds / BUDGET_STEP_S).ceil() as usize {
        let at = i as f64 * BUDGET_STEP_S;
        let mid = ((at + BUDGET_STEP_S / 2.0) / env.seconds).min(1.0);
        let want = proportional_counts(&demand.shares(mid), total, slots(env));
        if want != current {
            plan.push((((at + late_s) * 1e9) as u64, want.clone()));
            current = want;
        }
    }
    plan
}

fn budgets(env: &Env, cfg: &Cfg) -> Option<Budgets> {
    let (rule, load_s) = match cfg.budget {
        Budget::Static => (BudgetRule::Static, 0.0),
        Budget::Clairvoyant { load_s, late_s } => (
            BudgetRule::Planned(clairvoyant_plan(env, &cfg.shape, late_s)),
            load_s,
        ),
        Budget::Follow { load_s } => (
            BudgetRule::Follow {
                interval_ns: (BUDGET_STEP_S * 1e9) as u64,
            },
            load_s,
        ),
    };
    let moves = !matches!(cfg.budget, Budget::Static);
    (moves || slots(env) > env.per_region).then(|| {
        Budgets::new(
            rule,
            env.regions,
            slots(env),
            env.per_region,
            (load_s * 1e9) as u64,
        )
    })
}

fn tenant_quotas(env: &Env, seed: u64, cfg: &Cfg, headroom: f64) -> HashMap<u32, f64> {
    let mut quotas: HashMap<u32, f64> = HashMap::new();
    for req in workload(env, seed, &cfg.shape, true, cfg.placement.is_some()) {
        if let (true, Some(tenant)) = (req.client_facing() && req.tokens > 0, req.tenant) {
            *quotas.entry(tenant).or_insert(0.0) += req.tokens as f64 / env.seconds;
        }
    }
    for quota in quotas.values_mut() {
        *quota *= 1.0 + headroom;
    }
    quotas
}

fn machine(env: &Env, topo: Topology, mem: NodeMemory, cfg: &Cfg, seed: u64) -> Machine {
    let fleet = cfg
        .placement
        .as_ref()
        .map(|p| Fleet::new(Catalogue::published(PUBLISHED_START_NS), p));
    let per_node: Vec<NodeMemory> = (0..topo.domains.len())
        .map(|d| {
            match fleet
                .as_ref()
                .and_then(|f| f.model_on(d).map(|m| f.partition_bytes(m, mem.hbm)))
            {
                Some(partition) => NodeMemory {
                    kv: mem.kv.map(|kv| EngineKv { partition, ..kv }),
                    ..mem
                },
                None => mem,
            }
        })
        .collect();
    let mut mach = Machine::new(topo, move |d| per_node[d], Policy::Gdsf, Placement::Scored);
    if let Some(fleet) = fleet {
        mach.set_fleet(fleet, None);
        mach.set_model_batches(Batching::PerModel, true);
    }
    mach.set_flow_aware(true);
    mach.set_control(Control::Unified, Crossing::default());
    mach.set_state_transfer(true);
    mach.set_arrival_rate(total_rate(env));
    mach.set_fanout_atomic(true);
    p3(env).setup(
        &mut mach,
        ClusterBits {
            shared_l2: None,
            no_displacement: false,
        },
    );
    if let Some(mode) = cfg.mode {
        let mut regions = Regions::new(slots(env), one_way(&env.rtt, env.regions), mode);
        regions.price_reach = cfg.price_reach;
        regions.overflow = cfg.overflow;
        regions.summary_ns = (cfg.summary_s * 1e9) as u64;
        regions.own_forwards = cfg.own_forwards;
        regions.set_table(cfg.table_s.map(|s| (s * 1e9) as u64));
        mach.set_regions(Some(regions));
        mach.set_budgets(budgets(env, cfg));
        mach.set_residency(cfg.residency);
        if let Some(shares) = cfg.shares {
            mach.set_tenant_shares(Some(TenantShares::new(
                tenant_quotas(env, seed, cfg, shares.headroom),
                env.regions,
                TENANT_DEPTH_S,
                shares.lease_s.map(|s| (s * 1e9) as u64),
            )));
        }
    }
    mach.set_shards(cfg.schedulers.0, (cfg.schedulers.1 * 1e9) as u64);
    mach
}

fn grant(env: &Env, seed: u64) -> [u64; 3] {
    let cfg = Cfg::new("ledger", RegionMode::Regional, true, &RegionShape::Even);
    let mut mach = machine(env, topology(env), memory(env), &cfg, seed);
    let w = workload(env, seed, &RegionShape::Even, true, false);
    let _ = drive_with(&mut mach, total_rate(env), w, None);
    mach.kv_mean()
}

fn run(env: &Env, cfg: &Cfg, seed: u64, kv: [u64; 3]) -> Out {
    let mem = p3(env).engine_memory(memory(env), kv);
    let mut mach = machine(env, topology(env), mem, cfg, seed);
    let (t, _, _, _) = drive_with(
        &mut mach,
        total_rate(env),
        workload(
            env,
            seed,
            &cfg.shape,
            cfg.mode.is_some(),
            cfg.placement.is_some(),
        ),
        None,
    );
    Out { t, mach }
}

fn percentile(v: &mut [u64], q: f64) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v.sort_unstable();
    v[((v.len() as f64 - 1.0) * q).round() as usize] as f64 / 1e6
}

fn mean(v: &[u64]) -> f64 {
    polyphonic::programs::mean(v) / 1e6
}

struct Digest {
    service: f64,
    p99: f64,
    turns: f64,
    turn_p99: f64,
    turn_stall: f64,
    window_turns: f64,
    worst_region_p99: f64,
    away_facing: f64,
    forced: f64,
    faas: f64,
    saturated: f64,
    unserved: u64,
    unserved_pct: f64,
    moves: u64,
    changes: u64,
}

fn digest(o: &Out, window: (f64, f64)) -> Digest {
    let details: &[Detail] = &o.t.details;
    let total = o.t.base.max(1) as f64;
    let served: Vec<&Detail> = details.iter().filter(|x| x.served).collect();
    let mut all: Vec<u64> = served.iter().map(|x| x.service_ns).collect();
    let turns: Vec<&&Detail> = served
        .iter()
        .filter(|x| x.class == BlobKind::KvBlock.idx() && x.decodes && x.facing)
        .collect();
    let mut turn_ns: Vec<u64> = turns.iter().map(|x| x.service_ns).collect();
    let stall: Vec<u64> = turns.iter().map(|x| x.stall_ns).collect();
    let inside: Vec<u64> = turns
        .iter()
        .filter(|x| {
            let at = x.position as f64 / total;
            at >= window.0 && at < window.1
        })
        .map(|x| x.service_ns)
        .collect();
    let regions = o
        .mach
        .regions()
        .map_or(1, polyphonic::machine::Regions::count);
    let worst = (0..regions)
        .map(|r| {
            let mut v: Vec<u64> = turns
                .iter()
                .filter(|x| usize::from(x.region) == r)
                .map(|x| x.service_ns)
                .collect();
            percentile(&mut v, 0.99)
        })
        .fold(0.0, f64::max);
    let facing: Vec<&&Detail> = served.iter().filter(|x| x.facing).collect();
    let away = facing
        .iter()
        .filter(|x| x.served_in.is_some_and(|s| s != usize::from(x.region)))
        .count();
    let faas: Vec<u64> = served
        .iter()
        .filter(|x| x.class == BlobKind::Snapshot.idx() && x.facing)
        .map(|x| x.service_ns)
        .collect();
    let forced = o.mach.regions().map_or(0, |r| r.stats.forced);
    Digest {
        service: mean(&all),
        p99: percentile(&mut all, 0.99),
        turns: mean(&turn_ns),
        turn_p99: percentile(&mut turn_ns, 0.99),
        turn_stall: mean(&stall),
        window_turns: mean(&inside),
        worst_region_p99: worst,
        away_facing: 100.0 * away as f64 / facing.len().max(1) as f64,
        forced: 100.0 * forced as f64 / facing.len().max(1) as f64,
        faas: mean(&faas),
        saturated: 100.0 * o.mach.saturated() as f64 / o.mach.decodes().max(1) as f64,
        unserved: details.iter().filter(|x| !x.served).count() as u64,
        unserved_pct: 100.0 * details.iter().filter(|x| !x.served).count() as f64
            / details.len().max(1) as f64,
        moves: o.mach.budgets().map_or(0, |b| b.moves),
        changes: o.mach.regions().map_or(0, |r| r.table_stats.changes),
    }
}

fn joined(g: &[Digest], f: impl Fn(&Digest) -> f64) -> String {
    g.iter()
        .map(|x| format!("{:.1}", f(x)))
        .collect::<Vec<_>>()
        .join(" / ")
}

fn row(label: &str, g: &[Digest], reference: &[Digest]) {
    let against: Vec<String> = g
        .iter()
        .zip(reference)
        .map(|(x, y)| format!("{:+.1}", 100.0 * (x.service - y.service) / y.service))
        .collect();
    println!(
        "  {label:<46} service {} ({}%)  p99 {}  turns {}  turn p99 {}",
        joined(g, |x| x.service),
        against.join(" / "),
        joined(g, |x| x.p99),
        joined(g, |x| x.turns),
        joined(g, |x| x.turn_p99),
    );
    println!(
        "  {:<46} window turns {}  worst region turn p99 {}",
        "",
        joined(g, |x| x.window_turns),
        joined(g, |x| x.worst_region_p99),
    );
    println!(
        "  {:<46} served away {}%  forced {}%  turn stall {}  faas {} ms  full batch {}%  unserved {}",
        "",
        joined(g, |x| x.away_facing),
        joined(g, |x| x.forced),
        joined(g, |x| x.turn_stall),
        joined(g, |x| x.faas),
        joined(g, |x| x.saturated),
        g.iter()
            .map(|x| format!("{} ({:.1}%)", x.unserved, x.unserved_pct))
            .collect::<Vec<_>>()
            .join(" / "),
    );
    if g.iter().any(|x| x.moves > 0 || x.changes > 0) {
        println!(
            "  {:<46} budget moves {}  table changes {}",
            "",
            g.iter()
                .map(|x| x.moves.to_string())
                .collect::<Vec<_>>()
                .join(" / "),
            g.iter()
                .map(|x| x.changes.to_string())
                .collect::<Vec<_>>()
                .join(" / "),
        );
    }
}

fn cells(env: &Env, cfgs: &[Cfg], window: (f64, f64)) {
    let seeds: Vec<u64> = (env.seed..env.seed + env.seeds).collect();
    let grants: Vec<[u64; 3]> = seeds.iter().map(|&s| grant(env, s)).collect();
    let even = Cfg::new("regional", RegionMode::Regional, true, &RegionShape::Even);
    let reference: Vec<Digest> = seeds
        .iter()
        .zip(&grants)
        .map(|(&s, &kv)| digest(&run(env, &even, s, kv), window))
        .collect();
    for cfg in cfgs {
        let g: Vec<Digest> = seeds
            .iter()
            .zip(&grants)
            .map(|(&s, &kv)| digest(&run(env, cfg, s, kv), window))
            .collect();
        row(&cfg.label, &g, &reference);
    }
}

fn gate(env: &Env) {
    println!("1. the gate: one region, every region bit on, must change nothing");
    let one = Env {
        regions: 1,
        per_region: env.per_region * env.regions,
        rtt: env.rtt.clone(),
        sections: String::new(),
        ..*env
    };
    let shape = RegionShape::Even;
    let kv = grant(&one, one.seed);
    let off = Cfg {
        mode: None,
        ..Cfg::new("off", RegionMode::Global, true, &shape)
    };
    let reference = digest(&run(&one, &off, one.seed, kv), (0.0, 1.0));
    for (label, cfg) in [
        (
            "global, priced",
            Cfg::new("", RegionMode::Global, true, &shape),
        ),
        (
            "global, unpriced",
            Cfg::new("", RegionMode::Global, false, &shape),
        ),
        ("regional", Cfg::new("", RegionMode::Regional, true, &shape)),
        (
            "node price, 1 s",
            Cfg::overflowing("", &shape, Overflow::Node, 1.0, true),
        ),
        (
            "region mean, 1 s",
            Cfg::overflowing("", &shape, Overflow::RegionMean, 1.0, true),
        ),
        ("table, 1 s", tabled("", &shape, 1.0, true)),
        (
            "rent-or-buy budget",
            Cfg {
                budget: Budget::Follow { load_s: 8.0 },
                ..Cfg::new("", RegionMode::Regional, true, &shape)
            },
        ),
        (
            "threshold 0.5, 1 s",
            Cfg::overflowing(
                "",
                &shape,
                Overflow::Threshold { utilisation: 0.5 },
                1.0,
                false,
            ),
        ),
        (
            "two schedulers, exact reports",
            Cfg {
                schedulers: (2, 0.0),
                ..Cfg::new("", RegionMode::Regional, true, &shape)
            },
        ),
        (
            "all tenants restricted, region mean",
            Cfg {
                residency: 1.0,
                ..Cfg::overflowing("", &shape, Overflow::RegionMean, 1.0, true)
            },
        ),
        (
            "leased tenant shares, a quota never reached",
            Cfg {
                shares: Some(Shares {
                    headroom: 1_000.0,
                    lease_s: Some(0.5),
                }),
                ..Cfg::new("", RegionMode::Regional, true, &shape)
            },
        ),
    ] {
        let d = digest(&run(&one, &cfg, one.seed, kv), (0.0, 1.0));
        let same = [
            (d.service, reference.service),
            (d.p99, reference.p99),
            (d.turns, reference.turns),
            (d.turn_stall, reference.turn_stall),
        ]
        .iter()
        .all(|(a, b)| a.to_bits() == b.to_bits())
            && d.unserved == reference.unserved;
        println!(
            "  {label:<18} service {:.3} ms, p99 {:.3} ms: {}",
            d.service,
            d.p99,
            if same {
                "identical to off"
            } else {
                "DIFFERS from off"
            }
        );
    }
}

fn overflow_arms(shape: &RegionShape) -> Vec<Cfg> {
    let o = |label: &str, overflow, summary_s, own| {
        Cfg::overflowing(label, shape, overflow, summary_s, own)
    };
    vec![
        Cfg::new("regional", RegionMode::Regional, true, shape),
        Cfg::new(
            "global argmin, round trip priced",
            RegionMode::Global,
            true,
            shape,
        ),
        o("node price, exact view", Overflow::Node, 0.0, false),
        o("node price, 1 s summary", Overflow::Node, 1.0, false),
        o(
            "node price, 1 s summary + own forwards",
            Overflow::Node,
            1.0,
            true,
        ),
        o(
            "node price, 5 s summary + own forwards",
            Overflow::Node,
            5.0,
            true,
        ),
        o(
            "region mean, 1 s summary + own forwards",
            Overflow::RegionMean,
            1.0,
            true,
        ),
        o(
            "region mean, 5 s summary + own forwards",
            Overflow::RegionMean,
            5.0,
            true,
        ),
        o(
            "threshold 0.5, 1 s summary",
            Overflow::Threshold { utilisation: 0.5 },
            1.0,
            false,
        ),
        o(
            "threshold 0.7, 1 s summary",
            Overflow::Threshold { utilisation: 0.7 },
            1.0,
            false,
        ),
    ]
}

fn day_peaks() -> Vec<f64> {
    vec![0.79, 0.54, 0.21]
}

fn even(env: &Env) {
    println!("\n2. clients in regions at equal demand: service ms by seed, against regional");
    let shape = RegionShape::Even;
    let mut cfgs = vec![Cfg::new(
        "global argmin, round trip unpriced",
        RegionMode::Global,
        false,
        &shape,
    )];
    cfgs.extend(overflow_arms(&shape));
    cells(env, &cfgs, (0.0, 1.0));
}

fn burst(env: &Env) {
    for share in [0.6, 0.75] {
        println!(
            "\n3. a burst: region 0 takes {share} of arrivals from 0.3 to 0.6 of the run, against regional at equal demand"
        );
        let shape = RegionShape::Skew {
            region: 0,
            share,
            from: 0.3,
            to: 0.6,
        };
        cells(env, &overflow_arms(&shape), (0.3, 0.6));
    }
}

fn day(env: &Env) {
    for amplitude in [0.5, 0.75] {
        println!(
            "\n4. follow the sun: amplitude {amplitude}, one day over the run, against regional at equal demand"
        );
        let shape = RegionShape::Sun {
            amplitude,
            peaks: day_peaks(),
            days: 1.0,
        };
        cells(env, &overflow_arms(&shape), (0.0, 1.0));
    }
}

fn cases() -> Vec<(String, RegionShape, (f64, f64))> {
    vec![
        ("equal demand".to_string(), RegionShape::Even, (0.0, 1.0)),
        (
            "a burst: region 0 takes 0.75 of arrivals from 0.3 to 0.6 of the run".to_string(),
            RegionShape::Skew {
                region: 0,
                share: 0.75,
                from: 0.3,
                to: 0.6,
            },
            (0.3, 0.6),
        ),
        (
            "follow the sun: amplitude 0.75, one day over the run".to_string(),
            RegionShape::Sun {
                amplitude: 0.75,
                peaks: day_peaks(),
                days: 1.0,
            },
            (0.0, 1.0),
        ),
    ]
}

fn tabled(label: &str, shape: &RegionShape, epoch_s: f64, mean_overflow: bool) -> Cfg {
    let base = if mean_overflow {
        Cfg::overflowing(label, shape, Overflow::RegionMean, 1.0, true)
    } else {
        Cfg::new(label, RegionMode::Regional, true, shape)
    };
    Cfg {
        table_s: Some(epoch_s),
        ..base
    }
}

fn table(env: &Env) {
    for (title, shape, window) in cases() {
        println!("\n5. the routing table: {title}, against regional at equal demand");
        let cfgs = [
            Cfg::new("regional", RegionMode::Regional, true, &shape),
            tabled("table, 1 s epoch", &shape, 1.0, false),
            tabled("table, 5 s epoch", &shape, 5.0, false),
            tabled("table, 1 s epoch + region mean", &shape, 1.0, true),
            tabled("table, 5 s epoch + region mean", &shape, 5.0, true),
        ];
        cells(env, &cfgs, window);
    }
}

fn budget(env: &Env) {
    let moving = Env {
        seconds: BUDGET_SECONDS,
        slots: env.per_region + BUDGET_SPARE_SLOTS,
        ..env.clone()
    };
    for amplitude in [0.5, 0.75] {
        println!(
            "\n6. node budgets that follow the day: amplitude {amplitude}, {} s, {} slots a region, \
             against regional at equal demand over {} s",
            moving.seconds,
            slots(&moving),
            moving.seconds,
        );
        let shape = RegionShape::Sun {
            amplitude,
            peaks: day_peaks(),
            days: 1.0,
        };
        let bases = [
            ("regional", Cfg::new("", RegionMode::Regional, true, &shape)),
            (
                "threshold 0.7",
                Cfg::overflowing(
                    "",
                    &shape,
                    Overflow::Threshold { utilisation: 0.7 },
                    1.0,
                    false,
                ),
            ),
            ("table 5 s", tabled("", &shape, 5.0, false)),
        ];
        let mut cfgs = Vec::new();
        for (name, base) in &bases {
            let with = |label: String, budget| Cfg {
                label,
                budget,
                ..base.clone()
            };
            cfgs.push(with(format!("{name}, static budget"), Budget::Static));
            for (load_s, late_s) in [(8.0, 0.0), (8.0, 10.0), (8.0, 30.0), (30.0, 0.0)] {
                cfgs.push(with(
                    format!("{name}, clairvoyant, load {load_s} s, {late_s} s late"),
                    Budget::Clairvoyant { load_s, late_s },
                ));
            }
            cfgs.push(with(
                format!("{name}, rent-or-buy, load 8 s"),
                Budget::Follow { load_s: 8.0 },
            ));
        }
        cells(&moving, &cfgs, (0.0, 1.0));
    }
}

fn layouts(env: &Env) -> Vec<(&'static str, Vec<Option<u8>>)> {
    let nodes = slots(env) * env.regions;
    let tokens = total_rate(env) * MODEL_DECODE_SHARE * MODEL_TOKENS;
    let demand: [f64; 4] = std::array::from_fn(|m| tokens * MODEL_SHARES[m]);
    let counts = Costs::published(MODEL_OVERLOAD_NS).best_counts(&demand, nodes);
    println!("  the fleet's counts for {nodes} nodes: {counts:?}");
    let pool: Vec<u8> = counts
        .iter()
        .enumerate()
        .flat_map(|(m, &n)| std::iter::repeat_n(m as u8, n))
        .collect();
    let place = |regions: &[Vec<u8>]| -> Vec<Option<u8>> {
        let mut v = vec![None; nodes];
        for (r, models) in regions.iter().enumerate() {
            for (i, &m) in models.iter().enumerate() {
                v[r * slots(env) + i] = Some(m);
            }
        }
        v
    };
    let everywhere: Vec<Vec<u8>> = (0..env.regions)
        .map(|_| (0..slots(env)).map(|i| (i % 4) as u8).collect())
        .collect();
    let mut spread = vec![Vec::new(); env.regions];
    for (i, &m) in pool.iter().enumerate() {
        spread[i % env.regions].push(m);
    }
    let concentrated: Vec<Vec<u8>> = pool.chunks(slots(env)).map(<[u8]>::to_vec).collect();
    vec![
        ("every model in every region", place(&everywhere)),
        ("the fleet's counts, spread", place(&spread)),
        ("the fleet's counts, concentrated", place(&concentrated)),
    ]
}

fn models(env: &Env) {
    println!(
        "\n7. models placed by region: four models at 55 / 25 / 12 / 8% of demand, against regional \
         with every model everywhere on the published engine"
    );
    let shape = RegionShape::Even;
    let mut cfgs = Vec::new();
    for (name, placement) in layouts(env) {
        for (label, base) in [
            ("regional", Cfg::new("", RegionMode::Regional, true, &shape)),
            (
                "global argmin",
                Cfg::new("", RegionMode::Global, true, &shape),
            ),
            (
                "threshold 0.7",
                Cfg::overflowing(
                    "",
                    &shape,
                    Overflow::Threshold { utilisation: 0.7 },
                    1.0,
                    false,
                ),
            ),
        ] {
            cfgs.push(Cfg {
                label: format!("{name}: {label}"),
                placement: Some(placement.clone()),
                ..base
            });
        }
    }
    cells(env, &cfgs, (0.0, 1.0));
}

fn shards(env: &Env) {
    println!("\n9. active-active schedulers in each region at equal demand, against one scheduler");
    let shape = RegionShape::Even;
    let mut cfgs = vec![Cfg::new(
        "one scheduler",
        RegionMode::Regional,
        true,
        &shape,
    )];
    for k in [2, 4] {
        for report_s in [0.0, 0.025, 0.25, 1.0, 5.0] {
            cfgs.push(Cfg {
                label: format!("{k} schedulers, reports every {report_s} s"),
                schedulers: (k, report_s),
                ..Cfg::new("", RegionMode::Regional, true, &shape)
            });
        }
    }
    cells(env, &cfgs, (0.0, 1.0));
}

fn tenants(env: &Env) {
    let day = RegionShape::Sun {
        amplitude: 0.75,
        peaks: day_peaks(),
        days: 1.0,
    };
    for headroom in [0.1, 0.5] {
        println!(
            "\n8. tenants' regional shares: a quota {:.0}% above each tenant's mean decode rate, the \
             day at amplitude 0.75 against equal demand, refused requests are unserved",
            100.0 * headroom
        );
        let mut cfgs = Vec::new();
        for (shape, name) in [(&RegionShape::Even, "equal demand"), (&day, "the day")] {
            let with = |label: String, lease_s| Cfg {
                label,
                shares: Some(Shares { headroom, lease_s }),
                ..Cfg::new("", RegionMode::Regional, true, shape)
            };
            cfgs.push(with(format!("{name}: static split"), None));
            for lease_s in [0.1, 0.5, 2.0] {
                cfgs.push(with(
                    format!("{name}: lease, refreshed every {lease_s} s"),
                    Some(lease_s),
                ));
            }
        }
        cells(env, &cfgs, (0.0, 1.0));
    }
    println!(
        "\n8. residency: a share of tenants never leave their region, the burst at 0.75, region-mean \
         overflow on a 1 s summary with own forwards, against regional at equal demand"
    );
    let burst = RegionShape::Skew {
        region: 0,
        share: 0.75,
        from: 0.3,
        to: 0.6,
    };
    let mut cfgs = vec![Cfg::new("regional", RegionMode::Regional, true, &burst)];
    for residency in [0.0, 0.25, 0.5, 0.75, 1.0] {
        cfgs.push(Cfg {
            residency,
            ..Cfg::overflowing(
                &format!("overflow, {:.0}% of tenants restricted", 100.0 * residency),
                &burst,
                Overflow::RegionMean,
                1.0,
                true,
            )
        });
    }
    cells(env, &cfgs, (0.3, 0.6));
}

fn round_trip_table(env: &Env) -> Vec<Vec<f64>> {
    let regions = Regions::new(1, one_way(&env.rtt, env.regions), RegionMode::Regional);
    (0..env.regions)
        .map(|a| {
            (0..env.regions)
                .map(|b| regions.round_trip_ns(a, b) as f64)
                .collect()
        })
        .collect()
}

const SIDECAR_TAX_US: [f64; 2] = [43.97, 74.97];

const FAAS_US: f64 = 120.0;

const FLEET_NODES: f64 = 10_000.0;

const LIVENESS_PER_NODE_S: f64 = 0.1;

const TABLE_EPOCH_S: f64 = 300.0;

const DAY_S: f64 = 86_400.0;

fn path(env: &Env) {
    println!("\n10. a global scheduler on the request path, at equal demand (arithmetic)");
    let trips = round_trip_table(env);
    for (at, row) in trips.iter().enumerate() {
        let mean_ms = row.iter().sum::<f64>() / row.len() as f64 / 1e6;
        println!(
            "  scheduler in region {at}: a mean of {mean_ms:.1} ms a request -- {:.0}-{:.0} times the \
             sidecar tax, {:.1}% of a one-second turn, {:.0} times a warm FaaS call",
            mean_ms * 1e3 / SIDECAR_TAX_US[1],
            mean_ms * 1e3 / SIDECAR_TAX_US[0],
            mean_ms / 10.0,
            mean_ms * 1e3 / FAAS_US,
        );
    }
    let cut_s = 60.0;
    println!(
        "  a region of {:.0} req/s cut off from its scheduler for {cut_s:.0} s loses {:.0} request-seconds (lambda D^2 / 2)",
        env.rate,
        env.rate * cut_s * cut_s / 2.0,
    );
}

fn share_arithmetic(amplitude: f64, regions: usize) -> (Vec<f64>, Vec<Vec<f64>>) {
    let demand = RegionDemand {
        count: regions,
        shape: RegionShape::Sun {
            amplitude,
            peaks: day_peaks(),
            days: 1.0,
        },
    };
    let steps: u32 = 1_440;
    let series: Vec<Vec<f64>> = (0..steps)
        .map(|i| demand.shares(f64::from(i) / f64::from(steps)))
        .collect();
    let mean: Vec<f64> = (0..regions)
        .map(|r| series.iter().map(|s| s[r]).sum::<f64>() / f64::from(steps))
        .collect();
    (mean, series)
}

fn tenants_arithmetic(env: &Env) {
    println!(
        "\n11. a tenant's quota split by its mean regional share, on the generator's day (arithmetic)"
    );
    for amplitude in [0.5, 0.75] {
        let (mean, series) = share_arithmetic(amplitude, env.regions);
        for headroom in [0.0, 0.25, 0.5] {
            let refused: f64 = series
                .iter()
                .map(|s| {
                    s.iter()
                        .zip(&mean)
                        .map(|(share, m)| (share - m * (1.0 + headroom)).max(0.0))
                        .sum::<f64>()
                })
                .sum::<f64>()
                / series.len() as f64;
            println!(
                "  amplitude {amplitude}, {:.0}% headroom: a static split refuses {:.1}% of the demand",
                100.0 * headroom,
                100.0 * refused
            );
        }
        for lag_s in [16.0, 60.0, 300.0] {
            let demand = RegionDemand {
                count: env.regions,
                shape: RegionShape::Sun {
                    amplitude,
                    peaks: day_peaks(),
                    days: 1.0,
                },
            };
            let steps = series.len();
            let moved = (0..steps)
                .flat_map(|i| {
                    let f = i as f64 / steps as f64;
                    let (a, b) = (demand.shares(f), demand.shares(f + lag_s / DAY_S));
                    a.into_iter()
                        .zip(b)
                        .map(|(x, y)| (x - y).abs())
                        .collect::<Vec<_>>()
                })
                .fold(0.0, f64::max);
            println!(
                "  amplitude {amplitude}: a region's share moves by at most {:.3} points in {lag_s:.0} s of a real day",
                100.0 * moved
            );
        }
    }
}

fn acquisitions(plan: &[(u64, Vec<usize>)], start: &[usize]) -> usize {
    let mut have = start.to_vec();
    let mut moves = 0;
    for (_, want) in plan {
        moves += have
            .iter()
            .zip(want)
            .map(|(&h, &w)| w.saturating_sub(h))
            .sum::<usize>();
        have.clone_from(want);
    }
    moves
}

fn records(env: &Env) {
    println!(
        "\n12. what each record writes at {FLEET_NODES:.0} nodes in {} regions (arithmetic)",
        env.regions
    );
    let per_region = FLEET_NODES / env.regions as f64 * LIVENESS_PER_NODE_S;
    println!(
        "  each region's record: liveness {per_region:.0} writes a second ({LIVENESS_PER_NODE_S} a node)"
    );
    let table = (env.regions * (env.regions - 1)) as f64 / TABLE_EPOCH_S;
    println!(
        "  the global record: the table's {} fractions every {TABLE_EPOCH_S:.0} s, {table:.3} a second",
        env.regions * (env.regions - 1)
    );
    let moving = Env {
        seconds: BUDGET_SECONDS,
        slots: env.per_region + BUDGET_SPARE_SLOTS,
        ..env.clone()
    };
    let nodes = (env.per_region * env.regions) as f64;
    for amplitude in [0.5, 0.75] {
        let shape = RegionShape::Sun {
            amplitude,
            peaks: day_peaks(),
            days: 1.0,
        };
        let plan = clairvoyant_plan(&moving, &shape, 0.0);
        let moves = acquisitions(&plan, &vec![env.per_region; env.regions]);
        let per_second = moves as f64 / DAY_S * FLEET_NODES / nodes;
        println!(
            "  amplitude {amplitude}: {moves} node moves a day on {nodes:.0} nodes, {per_second:.2} a second at {FLEET_NODES:.0}; \
             the global record in all {:.2} a second, {:.0} times fewer writes than one region's liveness",
            table + per_second,
            per_region / (table + per_second),
        );
    }
}

fn arithmetic(env: &Env) {
    path(env);
    tenants_arithmetic(env);
    records(env);
}

pub fn run_all(env: &Env) {
    println!(
        "regions: {} x {} nodes, {:.0} req/s a region, {:.0} s, fan-out {:.0}%, round trips {}, \
         class mix volatility {}, decode kv {}",
        env.regions,
        env.per_region,
        env.rate,
        env.seconds,
        100.0 * env.fanout,
        env.rtt,
        env.volatility,
        env.decode_kv,
    );
    for section in env.sections.split(',') {
        match section.trim() {
            "gate" => gate(env),
            "even" => even(env),
            "burst" => burst(env),
            "day" => day(env),
            "table" => table(env),
            "budget" => budget(env),
            "models" => models(env),
            "shards" => shards(env),
            "tenants" => tenants(env),
            "arithmetic" => arithmetic(env),
            other => println!("unknown section {other}"),
        }
    }
}
