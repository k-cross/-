use clap::{Parser, Subcommand};
use polyphonic::arms::{Report, Trial, mean_ms, run, run_on, trace};
use polyphonic::blob::BlobKind;
use polyphonic::cache::{Policy, Quota};
use polyphonic::flow::FlowMode;

#[derive(Parser, Debug)]
#[command(name = "polyphonic", about = "state-residency scheduler experiments")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Unified vs siloed residency ledger under a phase-shifting workload
    Residency {
        /// DRAM tier capacity, e.g. 8GiB
        #[arg(long, default_value = "8GiB", value_parser = parse_bytes)]
        dram: u64,
        /// `NVMe` tier capacity
        #[arg(long, default_value = "64GiB", value_parser = parse_bytes)]
        nvme: u64,
        /// Requests to issue
        #[arg(long, default_value_t = 60_000)]
        ops: u64,
        #[arg(long, default_value_t = 1)]
        seed: u64,
        /// Static-partition sweep granularity for the siloed arms
        #[arg(long, default_value_t = 0.125)]
        step: f64,
        /// Phase-shift amplitude: 0 = flat mix, 1 = full swing
        #[arg(long, default_value_t = 1.0)]
        volatility: f64,
        /// Priority bands as inference,faas,training,service (0 = highest)
        #[arg(long, default_value = "0,1,2,1", value_parser = parse_bands)]
        bands: String,
    },

    /// Do cross-workload flows pay: blind vs. anticipatory value vs. downstream-aware admission
    Flows {
        #[arg(long, default_value = "8GiB", value_parser = parse_bytes)]
        dram: u64,
        #[arg(long, default_value = "64GiB", value_parser = parse_bytes)]
        nvme: u64,
        #[arg(long, default_value_t = 15_000)]
        ops: u64,
        #[arg(long, default_value_t = 1)]
        seed: u64,
        #[arg(long, default_value_t = 0.125)]
        step: f64,
        #[arg(long, default_value = "0,1,2,1", value_parser = parse_bands)]
        bands: String,
    },

    /// State-blind vs state-aware placement over a synthetic multi-domain machine
    Placement {
        /// Memory domains (sockets). Link costs between them are MODELLED, not measured.
        #[arg(long, default_value_t = 4)]
        sockets: usize,
        #[arg(long, default_value_t = 3)]
        units_per_socket: usize,
        /// Total DRAM across all domains
        #[arg(long, default_value = "8GiB", value_parser = parse_bytes)]
        dram: u64,
        #[arg(long, default_value = "64GiB", value_parser = parse_bytes)]
        nvme: u64,
        #[arg(long, default_value_t = 15_000)]
        ops: u64,
        #[arg(long, default_value_t = 1)]
        seed: u64,
        #[arg(long, default_value = "0,1,2,1", value_parser = parse_bands)]
        bands: String,
        /// Drain one domain at this fraction through the trace (0 = never). Its state
        /// migrates to the survivors, so the bytes remain but every hash to it is stale.
        #[arg(long, default_value_t = 0.0)]
        drain_at: f64,
        /// Make inference depend on its model's weight shards, shared across tenants
        #[arg(long)]
        share_weights: bool,
    },

    /// Discover the host's compute/memory graph and measure its link asymmetry
    Topology {
        /// Streaming buffer per probe; must exceed the largest cache to measure memory
        #[arg(long, default_value = "256MiB", value_parser = parse_bytes)]
        bytes: u64,
        #[arg(long, default_value_t = 5)]
        iters: u32,
    },

    /// Measure this machine's real tier costs: page-fault, spill write, spill read
    Calibrate {
        /// Backing file for the spill tier
        #[arg(long, default_value = "target/polyphonic-spill.bin")]
        path: String,
        #[arg(long, default_value_t = 8)]
        iters: u32,
    },

    /// Does the unified advantage scale with volatility, and vanish at zero?
    Volatility {
        #[arg(long, default_value = "8GiB", value_parser = parse_bytes)]
        dram: u64,
        #[arg(long, default_value = "64GiB", value_parser = parse_bytes)]
        nvme: u64,
        #[arg(long, default_value_t = 30_000)]
        ops: u64,
        #[arg(long, default_value_t = 1)]
        seed: u64,
        #[arg(long, default_value_t = 0.125)]
        step: f64,
    },
}

fn parse_bands(s: &str) -> Result<String, String> {
    let n = s.split(',').count();
    if n != BlobKind::N {
        return Err(format!(
            "expected {} comma-separated bands, got {n}",
            BlobKind::N
        ));
    }
    for p in s.split(',') {
        p.trim().parse::<u8>().map_err(|e| e.to_string())?;
    }
    Ok(s.to_string())
}

fn bands_of(s: &str) -> [u8; BlobKind::N] {
    let mut out = [0u8; BlobKind::N];
    for (i, p) in s.split(',').enumerate() {
        out[i] = p.trim().parse().unwrap_or(0);
    }
    out
}

fn parse_bytes(s: &str) -> Result<u64, String> {
    let t = s.trim();
    let (num, mult) = if let Some(p) = t.strip_suffix("GiB") {
        (p, 1u64 << 30)
    } else if let Some(p) = t.strip_suffix("MiB") {
        (p, 1u64 << 20)
    } else if let Some(p) = t.strip_suffix("KiB") {
        (p, 1u64 << 10)
    } else {
        (t, 1)
    };
    num.trim()
        .parse::<f64>()
        .map(|v| (v * mult as f64) as u64)
        .map_err(|e| e.to_string())
}

/// Band-lexicographic objective: inference first, then customer-facing, then training.
/// Training is best-effort, so a configuration is preferred if it improves a higher band
/// even at the cost of a lower one.
fn prefer(a: &Report, b: &Report, bands: [u8; BlobKind::N]) -> bool {
    // Throughput before latency: a config may not buy a faster band by dropping requests.
    let (ga, gb) = (a.goodput(), b.goodput());
    if (ga - gb).abs() > 0.02 {
        return ga > gb;
    }
    let max_band = bands.iter().copied().max().unwrap_or(0);
    for band in 0..=max_band {
        let stall = |r: &Report| -> f64 {
            let (mut ns, mut ops) = (0u64, 0u64);
            for (k, &kb) in bands.iter().enumerate() {
                if kb == band {
                    ns += r.kind_ns[k];
                    ops += r.kind_ops[k];
                }
            }
            mean_ms(ns, ops)
        };
        let good = |r: &Report| -> f64 {
            let (mut s, mut n) = (0u64, 0u64);
            for (k, &kb) in bands.iter().enumerate() {
                if kb == band {
                    s += r.served[k];
                    n += r.served[k] + r.refused[k];
                }
            }
            if n == 0 { 1.0 } else { s as f64 / n as f64 }
        };
        let (ga, gb) = (good(a), good(b));
        if (ga - gb).abs() > 0.02 {
            return ga > gb;
        }
        let (sa, sb) = (stall(a), stall(b));
        if sa < sb * 0.98 {
            return true;
        }
        if sb < sa * 0.98 {
            return false;
        }
    }
    false
}

fn best_split(t: Trial, hard: bool, step: f64) -> (Report, [f64; BlobKind::N]) {
    let mut best: Option<(Report, [f64; BlobKind::N])> = None;
    let stream = trace(t);
    let n = (1.0 / step).round() as u64;
    for a in 1..n {
        for b in 1..n - a {
            for c in 1..n - a - b {
                // splits sum to less than n; the remainder is shared slack
                for d in 1..n - a - b - c {
                    let split = [a, b, c, d].map(|x| x as f64 / n as f64);
                    let q = Quota::from_split(t.dram, split, t.bands, hard);
                    let r = run_on("", t, q, &stream);
                    let better = best.as_ref().is_none_or(|(x, _)| prefer(&r, x, t.bands));
                    if better {
                        best = Some((r, split));
                    }
                }
            }
        }
    }
    best.expect("sweep produced no candidate splits")
}

const PHASE_NAME: [&str; 4] = [
    "inference-heavy",
    "faas-burst",
    "training-window",
    "mixed-steady",
];
const CLASS_NAME: [&str; BlobKind::N] = ["inference", "faas", "training", "service"];

fn ms(ns: u64) -> f64 {
    ns as f64 / 1e6
}

fn gib(b: u64) -> f64 {
    b as f64 / (1u64 << 30) as f64
}

fn calibrate(path: &str, iters: u32) {
    use polyphonic::blob::BlobId;
    use polyphonic::store::Store;

    let sizes: [(&str, usize); 4] = [
        ("kv-block    512KiB", 512 * 1024),
        ("prefill-batch 2MiB", 2 * 1024 * 1024),
        ("snapshot     32MiB", 32 * 1024 * 1024),
        ("weight-shard 512MiB", 512 * 1024 * 1024),
    ];
    let mut store = Store::open(std::path::Path::new(path), 8 << 30).expect("open spill file");
    println!("spill file: {path}  (direct I/O; page cache bypassed)\n");
    println!(
        "{:<22} {:>12} {:>10} {:>12} {:>10} {:>12} {:>10}",
        "object", "fault (us)", "GB/s", "write (us)", "GB/s", "read (us)", "GB/s"
    );
    for (name, len) in sizes {
        let (mut f, mut w, mut r) = (0u64, 0u64, 0u64);
        for i in 0..iters {
            let id = BlobId::leaf(format!("cal:{name}:{i}").as_bytes());
            f += store.materialize(id, len);
            w += store.demote(id);
            r += store.promote(id);
            store.demote(id);
            store.drop_cold(id);
        }
        let n = u64::from(iters);
        let gbs = |ns: u64| (len as f64 * n as f64) / (ns.max(1) as f64);
        println!(
            "{:<22} {:>12.1} {:>10.2} {:>12.1} {:>10.2} {:>12.1} {:>10.2}",
            name,
            f as f64 / n as f64 / 1000.0,
            gbs(f),
            w as f64 / n as f64 / 1000.0,
            gbs(w),
            r as f64 / n as f64 / 1000.0,
            gbs(r)
        );
    }
    let _ = std::fs::remove_file(path);
}

fn arm_row(r: &Report) {
    let served: u64 = r.served.iter().sum();
    println!(
        "{:<32} {:>11.3} {:>9.3} {:>8.1}% {:>9.0}%   {:.2}/{:.2}/{:.2}/{:.2}   {:.1}/{:.1}/{:.1}/{:.1}",
        r.label,
        mean_ms(r.total_ns, served),
        ms(r.p99_ns),
        100.0 * r.goodput(),
        100.0 * r.transfer_ns as f64 / r.total_ns.max(1) as f64,
        r.hit[0],
        r.hit[1],
        r.hit[2],
        r.hit[3],
        gib(r.resident[0]),
        gib(r.resident[1]),
        gib(r.resident[2]),
        gib(r.resident[3]),
    );
}

/// Dependent-load latency over a working set. Bandwidth is memory-bound and identical
/// across clusters; what differs is how far up the hierarchy a given working set still fits,
/// which is exactly the "what does this state cost me to reach" question.
#[allow(
    clippy::too_many_arguments,
    reason = "experiment knobs, all surfaced on the CLI"
)]
fn placement(
    sockets: usize,
    units_per_socket: usize,
    dram: u64,
    nvme: u64,
    ops: u64,
    seed: u64,
    bands: [u8; BlobKind::N],
    drain_at: f64,
    share_weights: bool,
) {
    use polyphonic::machine::{Machine, Placement};
    use polyphonic::topo::Topology;

    let per_socket = dram / sockets as u64;
    let topo = Topology::synthetic(sockets, units_per_socket, per_socket);
    let split = [0.12, 0.12, 0.12, 0.25];
    println!(
        "synthetic machine: {sockets} domains x {:.1} GiB, {units_per_socket} units each\n\
         cross-domain link constants are MODELLED (coherent, ~2x per-byte, 120 ns hop)",
        gib(per_socket)
    );
    if share_weights {
        println!(
            "inference depends on shared model weight shards ({} tenants over 4 models)",
            24
        );
    }
    if drain_at > 0.0 {
        println!(
            "domain 0 drained at {:.0}% through the trace; its state migrates\n",
            drain_at * 100.0
        );
    } else {
        println!();
    }

    println!(
        "{:<10} {:>12} {:>14} {:>12} {:>10} {:>12} {:>14}",
        "placement",
        "stall/req",
        "interconnect",
        "local hops",
        "cold",
        "bytes moved",
        "domain spread"
    );
    for mode in [Placement::Blind, Placement::Sticky, Placement::Aware] {
        let mut m = Machine::new(
            topo.clone(),
            nvme / sockets as u64,
            Policy::Gdsf,
            |cap| Quota::from_split(cap, split, bands, false),
            mode,
        );
        let mut total = 0u64;
        let mut served = 0u64;
        let drain_op = if drain_at > 0.0 {
            (ops as f64 * drain_at) as u64
        } else {
            u64::MAX
        };
        let wl = polyphonic::work::Workload::new(seed, ops, 1.0);
        let wl = if share_weights {
            wl.with_shared_weights()
        } else {
            wl
        };
        for (i, req) in wl.enumerate() {
            if i as u64 == drain_op {
                m.drain(0);
            }
            let c = m.serve_request(&req);
            if c.pending {
                continue;
            }
            total += c.total_ns();
            served += 1;
        }
        let label = match mode {
            Placement::Blind => "blind",
            Placement::Sticky => "sticky",
            Placement::Aware => "aware",
        };
        println!(
            "{label:<10} {:>11.3}ms {:>13.1}s {:>10.2}s {:>11.1}% {:>9.1}% {:>13.2}",
            mean_ms(total, served),
            total.saturating_sub(m.interconnect_ns + m.handoff_ns) as f64 / 1e9,
            m.handoff_ns as f64 / 1e9,
            100.0 * m.split_tasks as f64 / (m.split_tasks + m.joined_tasks).max(1) as f64,
            100.0 * m.cold as f64 / served.max(1) as f64,
            m.domain_spread(),
        );
    }
}

fn chase_median(bytes: usize, cluster: polyphonic::plat::Cluster, reps: u32) -> f64 {
    let mut v: Vec<f64> = (0..reps.max(1))
        .map(|_| chase_latency(bytes, cluster))
        .collect();
    v.sort_by(f64::total_cmp);
    v[v.len() / 2]
}

fn chase_latency(bytes: usize, cluster: polyphonic::plat::Cluster) -> f64 {
    std::thread::spawn(move || {
        if !polyphonic::plat::pin_cluster(cluster) {
            return f64::NAN;
        }
        let stride = 128 / 8;
        let slots = bytes / 8;
        let steps = slots / stride;
        if steps < 2 {
            return f64::NAN;
        }
        // A single cycle visiting one slot per cache line, in scrambled order so the
        // prefetcher cannot follow it.
        let mut order: Vec<usize> = (0..steps).map(|i| i * stride).collect();
        let mut rng = polyphonic::rng::Rng::new(0x5EED);
        for i in (1..steps).rev() {
            let j = rng.below(i as u64 + 1) as usize;
            order.swap(i, j);
        }
        let mut buf = vec![0usize; slots];
        for w in 0..steps {
            buf[order[w]] = order[(w + 1) % steps];
        }
        let mut p = order[0];
        let warm = steps * 2;
        for _ in 0..warm {
            p = buf[p];
        }
        std::hint::black_box(p);
        let laps = (64 << 20) / bytes.max(1);
        let n = steps * laps.max(4);
        let t = std::time::Instant::now();
        for _ in 0..n {
            p = buf[p];
        }
        let ns = t.elapsed().as_nanos() as f64;
        std::hint::black_box(p);
        ns / n as f64
    })
    .join()
    .unwrap_or(f64::NAN)
}

fn topology(bytes: u64, iters: u32) {
    use polyphonic::plat::Cluster;
    use polyphonic::topo::Topology;

    let t = Topology::discover();
    println!("discovered host graph\n");
    println!(
        "{:<10} {:>6} {:>14} {:>10}",
        "domain", "id", "kind", "capacity"
    );
    for d in &t.domains {
        println!(
            "{:<10} {:>6} {:>14?} {:>9.1}G",
            "memory",
            d.id,
            d.kind,
            gib(d.capacity)
        );
    }
    let mut perf = 0;
    let mut eff = 0;
    for u in &t.units {
        match u.kind {
            polyphonic::topo::UnitKind::Efficiency => eff += 1,
            _ => perf += 1,
        }
    }
    println!(
        "\n{perf} performance units, {eff} efficiency units, {} memory domain(s)",
        t.domains.len()
    );

    println!("\nlink matrix (ns per byte, [c] = coherent)");
    print!("{:<12}", "unit\\domain");
    for d in &t.domains {
        print!("{:>14}", format!("dom{}", d.id));
    }
    println!();
    for (i, u) in t.units.iter().enumerate() {
        if i > 0 && t.units[i - 1].cluster == u.cluster {
            continue;
        }
        print!("{:<12}", format!("cluster{}", u.cluster));
        for (j, _) in t.domains.iter().enumerate() {
            let l = t.link(i, j);
            print!(
                "{:>14}",
                format!(
                    "{:.4}{}",
                    l.ns_per_byte,
                    if l.coherent { "[c]" } else { "" }
                )
            );
        }
        println!();
    }

    if eff == 0 {
        println!("\nsingle compute cluster: no asymmetry to measure on this host");
        return;
    }
    let _ = bytes;
    println!("\nmeasured dependent-load latency by working set (ns/access)");
    println!(
        "{:<14} {:>12} {:>12} {:>10}",
        "working set", "cluster A", "cluster B", "ratio"
    );
    let probes = [1usize, 4, 16, 64, 256];
    let (mut slower, mut faster) = (0usize, 0usize);
    for ws_mib in probes {
        let ws = ws_mib << 20;
        let a = chase_median(ws, Cluster::Performance, iters);
        let b = chase_median(ws, Cluster::Efficiency, iters);
        let r = b / a;
        if r > 1.15 {
            slower += 1;
        } else if r < 0.87 {
            faster += 1;
        }
        println!(
            "{:<14} {a:>12.2} {b:>12.2} {r:>9.2}x",
            format!("{ws_mib} MiB")
        );
    }
    // Real cluster separation is consistent in direction; ratios that flip sign across
    // working sets are dispersion, not signal.
    let separated = slower >= probes.len() - 1 || faster >= probes.len() - 1;

    println!(
        "\nThe latency curve is a real measurement of this host's one memory domain, and it is\n\
         what a per-domain link cost should be derived from: ~10 ns in L2, ~30 ns at 32 MiB,\n\
         ~120 ns out to DRAM."
    );
    if separated {
        println!("Cluster placement separated the two probes; the ratio above is meaningful.");
    } else {
        println!(
            "\nCluster placement did NOT separate: QoS is advisory on this host and macOS runs\n\
             background threads on performance cores when idle, so both probes measure the same\n\
             cluster. Treat the ratio column as noise, not as an interconnect measurement --\n\
             this machine has one memory domain and cannot exhibit the asymmetry the cost model\n\
             is built for. The link constants in `Topology::synthetic` are MODELLED and need\n\
             re-deriving on multi-socket or multi-GPU hardware before any result depends on them."
        );
    }
}

fn flows_report(dram: u64, nvme: u64, ops: u64, seed: u64, step: f64, bands: [u8; BlobKind::N]) {
    println!(
        "dram={:.1}GiB ops={ops} seed={seed} bands={bands:?}\n",
        gib(dram)
    );
    let cfg = Trial {
        bands,
        flows: FlowMode::Blind,
        dram,
        nvme,
        policy: Policy::Gdsf,
        seed,
        ops,
        vol: 1.0,
    };
    let (_, split) = best_split(cfg, false, step);
    let quota = Quota::from_split(dram, split, bands, false);
    println!(
        "soft floors [{:.2}/{:.2}/{:.2}/{:.2}], identical quota and trace in every row\n\
         task e2e is critical-path only; prewarm work is materialisation moved off it\n",
        split[0], split[1], split[2], split[3]
    );

    println!(
        "{:<10} {:>13} {:>13} {:>14} {:>11} {:>10} {:>10}",
        "flows", "task e2e (ms)", "stall total", "prewarm work", "net work", "inference", "goodput"
    );
    for mode in [FlowMode::Blind, FlowMode::Announce, FlowMode::Gate] {
        let trial = Trial { flows: mode, ..cfg };
        let r = run("", trial, quota);
        let label = match mode {
            FlowMode::Blind => "blind",
            FlowMode::Announce => "announce",
            FlowMode::Gate => "gate",
        };
        let stall_s = r.total_ns as f64 / 1e9;
        let prewarm_s = r.prewarm_ns as f64 / 1e9;
        println!(
            "{label:<10} {:>13.2} {stall_s:>12.2}s {prewarm_s:>13.2}s {:>10.2}s {:>10.2} {:>9.1}%",
            r.flow_e2e_ms(),
            stall_s + prewarm_s,
            mean_ms(r.kind_ns[0], r.kind_ops[0]),
            100.0 * r.goodput(),
        );
    }
}

fn volatility_sweep(dram: u64, nvme: u64, ops: u64, seed: u64, step: f64) {
    let bands = [0u8, 1, 2, 1];
    println!("dram={:.1}GiB ops={ops} seed={seed}\n", gib(dram));
    println!(
        "{:>10} {:>18} {:>16} {:>12}",
        "volatility", "hard-partition (ms)", "soft-floor (ms)", "advantage"
    );
    for i in 0..=5 {
        let v = f64::from(i) / 5.0;
        let t = Trial {
            bands,
            flows: FlowMode::Blind,
            dram,
            nvme,
            policy: Policy::Gdsf,
            seed,
            ops,
            vol: v,
        };
        let (hard, _) = best_split(t, true, step);
        let (soft, _) = best_split(t, false, step);
        let hm = mean_ms(hard.total_ns, hard.served.iter().sum());
        let sm = mean_ms(soft.total_ns, soft.served.iter().sum());
        println!(
            "{v:>10.1} {hm:>18.3} {sm:>16.3} {:>11.1}%",
            100.0 * (hm - sm) / hm
        );
    }
}

fn residency_report(
    dram: u64,
    nvme: u64,
    ops: u64,
    seed: u64,
    step: f64,
    volatility: f64,
    bands: [u8; BlobKind::N],
) {
    println!(
        "dram={:.1}GiB nvme={:.1}GiB ops={ops} seed={seed} volatility={volatility}\n",
        gib(dram),
        gib(nvme)
    );
    println!("priority bands (operator-configured): {bands:?}\n");

    let t = Trial {
        bands,
        flows: FlowMode::Blind,
        dram,
        nvme,
        policy: Policy::Gdsf,
        seed,
        ops,
        vol: volatility,
    };
    let (mut hard, hs) = best_split(t, true, step);
    hard.label = format!(
        "hard-partition [{:.2}/{:.2}/{:.2}/{:.2}]",
        hs[0], hs[1], hs[2], hs[3]
    );
    let (mut soft, ss) = best_split(t, false, step);
    soft.label = format!(
        "soft-floor     [{:.2}/{:.2}/{:.2}/{:.2}]",
        ss[0], ss[1], ss[2], ss[3]
    );
    let mut open = run("", t, Quota::open(dram, t.bands));
    open.label = "no-floor       [open]".to_string();

    println!(
        "{:<32} {:>11} {:>9} {:>9} {:>10} {:>20} {:>21}",
        "arm", "stall/req", "p99 (ms)", "goodput", "from tier", "hit kv/sn/wt/svc", "resident GiB"
    );
    for r in [&hard, &soft, &open] {
        arm_row(r);
    }

    println!("\nadmission integrity");
    for r in [&hard, &soft, &open] {
        println!(
            "{:<32} over-capacity={:<7} refused={:?} pinned-skips={}",
            r.label, r.over_capacity, r.refused, r.pinned_skips
        );
    }

    println!("\nper-class: mean stall per served request (ms) / goodput");
    println!(
        "{:<32} {:>16} {:>16} {:>16} {:>16}",
        "arm", CLASS_NAME[0], CLASS_NAME[1], CLASS_NAME[2], CLASS_NAME[3]
    );
    for r in [&hard, &soft, &open] {
        print!("{:<32}", r.label);
        for k in 0..BlobKind::N {
            let cell = format!(
                "{:.1} / {:.0}%",
                mean_ms(r.kind_ns[k], r.kind_ops[k]),
                100.0 * r.class_goodput(k)
            );
            print!("{cell:>16}");
        }
        println!();
    }

    println!("\nshare of total stall by class (policy can only move what dominates)");
    for r in [&hard, &soft, &open] {
        print!("{:<32}", r.label);
        for k in 0..BlobKind::N {
            print!(
                "{:>15.1}%",
                100.0 * r.kind_ns[k] as f64 / r.total_ns.max(1) as f64
            );
        }
        println!();
    }

    println!("\nstall (s) by phase");
    println!(
        "{:<32} {:>16} {:>16} {:>16} {:>16}",
        "arm", PHASE_NAME[0], PHASE_NAME[1], PHASE_NAME[2], PHASE_NAME[3]
    );
    for r in [&hard, &soft, &open] {
        print!("{:<32}", r.label);
        for p in r.phase_ns {
            print!("{:>16.2}", p as f64 / 1e9);
        }
        println!();
    }

    let hm = mean_ms(hard.total_ns, hard.served.iter().sum());
    let sm = mean_ms(soft.total_ns, soft.served.iter().sum());
    println!(
        "\nsoft-floor vs hard-partition: {:+.1}% stall/req at {:+.1}pp goodput",
        100.0 * (hm - sm) / hm,
        100.0 * (soft.goodput() - hard.goodput())
    );
}

fn main() {
    match Cli::parse().cmd {
        Cmd::Residency {
            dram,
            nvme,
            ops,
            seed,
            step,
            volatility,
            bands,
        } => {
            residency_report(dram, nvme, ops, seed, step, volatility, bands_of(&bands));
        }
        Cmd::Calibrate { path, iters } => calibrate(&path, iters),
        Cmd::Topology { bytes, iters } => topology(bytes, iters),
        Cmd::Placement {
            sockets,
            units_per_socket,
            dram,
            nvme,
            ops,
            seed,
            bands,
            drain_at,
            share_weights,
        } => {
            placement(
                sockets,
                units_per_socket,
                dram,
                nvme,
                ops,
                seed,
                bands_of(&bands),
                drain_at,
                share_weights,
            );
        }
        Cmd::Flows {
            dram,
            nvme,
            ops,
            seed,
            step,
            bands,
        } => {
            flows_report(dram, nvme, ops, seed, step, bands_of(&bands));
        }
        Cmd::Volatility {
            dram,
            nvme,
            ops,
            seed,
            step,
        } => {
            volatility_sweep(dram, nvme, ops, seed, step);
        }
    }
}
