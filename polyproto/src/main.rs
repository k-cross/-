use clap::{Parser, Subcommand};
use polyphonic::arms::{Budget, Report, Trial, mean_ms, run, run_on, trace};
use polyphonic::blob::BlobKind;
use polyphonic::cache::{NodeMemory, Policy, Quota};
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
        /// Accelerator HBM holding KV and weights, e.g. 4GiB. 0 models unified memory, where
        /// every class shares the DRAM pool
        #[arg(long, default_value = "4GiB", value_parser = parse_bytes)]
        hbm: u64,
        /// Host DDR capacity, e.g. 8GiB
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
        /// Priority bands as inference,faas,weights,service (0 = highest)
        #[arg(long, default_value = "0,1,2,1", value_parser = parse_bands)]
        bands: String,
    },

    /// Do cross-workload flows pay: blind vs. anticipatory value vs. downstream-aware admission
    Flows {
        /// Accelerator HBM holding KV and weights, e.g. 4GiB. 0 models unified memory, where
        /// every class shares the DRAM pool
        #[arg(long, default_value = "4GiB", value_parser = parse_bytes)]
        hbm: u64,
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
        /// Total accelerator HBM across all domains, holding KV and weights. 0 models unified
        /// memory; compare against it with the HBM added to --dram
        #[arg(long, default_value = "16GiB", value_parser = parse_bytes)]
        hbm: u64,
        /// Total host DDR across all domains
        #[arg(long, default_value = "32GiB", value_parser = parse_bytes)]
        dram: u64,
        #[arg(long, default_value = "64GiB", value_parser = parse_bytes)]
        nvme: u64,
        #[arg(long, default_value_t = 15_000)]
        ops: u64,
        #[arg(long, default_value_t = 1)]
        seed: u64,
        #[arg(long, default_value = "0,1,2,1", value_parser = parse_bands)]
        bands: String,
        /// Arrival rate in requests/sec, driving the engine model. 0 charges a flat
        /// per-token decode cost instead
        #[arg(long, default_value_t = 350.0)]
        rate: f64,
        /// Drain one domain at this fraction through the trace (0 = never). Its state
        /// migrates to the survivors, so the bytes remain but every hash to it is stale.
        #[arg(long, default_value_t = 0.0)]
        drain_at: f64,
    },

    /// Residency-aware placement across a cluster, with the control plane's own cost charged
    Distributed {
        #[arg(long, default_value_t = 4)]
        nodes: usize,
        #[arg(long, default_value_t = 3)]
        units_per_node: usize,
        /// Total accelerator HBM across all nodes, holding KV and weights. 0 models unified
        /// memory; compare against it with the HBM added to --dram
        #[arg(long, default_value = "16GiB", value_parser = parse_bytes)]
        hbm: u64,
        /// Total host DDR across all nodes
        #[arg(long, default_value = "32GiB", value_parser = parse_bytes)]
        dram: u64,
        #[arg(long, default_value = "64GiB", value_parser = parse_bytes)]
        nvme: u64,
        #[arg(long, default_value_t = 15_000)]
        ops: u64,
        #[arg(long, default_value_t = 1)]
        seed: u64,
        #[arg(long, default_value = "0,1,2,1", value_parser = parse_bands)]
        bands: String,
        /// Node distances to sweep
        #[arg(long, default_value = "rack,zone,region")]
        distances: String,
        /// Transport the control plane crosses on: native|ring|syscall|pipe|unix|tcp|grpc
        #[arg(long, default_value = "grpc")]
        crossing: String,
        /// Requests between gossip refreshes for the stale-view arm
        #[arg(long, default_value_t = 200)]
        gossip_period: u64,
        /// Arrival rate in requests/sec. Drives the engine model: decode cost depends on
        /// batch occupancy, which depends on load. 0 charges a flat per-token cost instead
        #[arg(long, default_value_t = 250.0)]
        rate: f64,
        /// Fraction of agent turns that fan out to sub-agents (each making function tool calls);
        /// 0 leaves fan-outs out of the stream
        #[arg(long, default_value_t = 0.10)]
        fanout: f64,
        /// Boundary-ladder repetitions
        #[arg(long, default_value_t = 3)]
        repeat: usize,
    },

    /// One model host and one agent-framework host, swept from same-socket to cross-region.
    /// The agent host has no accelerator: it can never decode, only run tool calls and hold
    /// its own bookkeeping. Every reasoning step pays the round trip to the model host; tool
    /// calls default to the agent host and ship only when that is actually cheaper.
    CodeReview {
        /// Model host's accelerator memory, holding KV and weight shards
        #[arg(long, default_value = "24GiB", value_parser = parse_bytes)]
        hbm: u64,
        /// Model host's DDR: the offload target under HBM
        #[arg(long, default_value = "16GiB", value_parser = parse_bytes)]
        model_ddr: u64,
        /// Agent host's DDR: tool-call (`FaaS`) cells and orchestration bookkeeping. This
        /// node has no HBM and no decode engine
        #[arg(long, default_value = "32GiB", value_parser = parse_bytes)]
        agent_ddr: u64,
        #[arg(long, default_value = "64GiB", value_parser = parse_bytes)]
        nvme: u64,
        #[arg(long, default_value_t = 3)]
        units_per_node: usize,
        #[arg(long, default_value_t = 15_000)]
        ops: u64,
        #[arg(long, default_value_t = 1)]
        seed: u64,
        #[arg(long, default_value = "0,1,2,1", value_parser = parse_bands)]
        bands: String,
        /// Distances to sweep, local to cross-region
        #[arg(long, default_value = "socket,rack,zone,region")]
        distances: String,
        /// Transport the control plane crosses on: native|ring|syscall|pipe|unix|tcp|grpc
        #[arg(long, default_value = "grpc")]
        crossing: String,
        #[arg(long, default_value_t = 200)]
        gossip_period: u64,
        /// Arrival rate in requests/sec. One engine serves the whole cluster here, so the
        /// saturation knee sits near half the symmetric-cluster rate -- about 130/s at these
        /// defaults. 110 keeps the sweep below it, where placement still decides something
        #[arg(long, default_value_t = 110.0)]
        rate: f64,
        /// Fraction of agent turns that call a tool (read a file, grep, run tests) -- a code
        /// review agent reaches for one far more often than a chat agent does
        #[arg(long, default_value_t = 0.70)]
        tool_fraction: f64,
        /// Tool-call payload: file-sized, not a small function argument
        #[arg(long, default_value = "512KiB", value_parser = parse_bytes)]
        tool_payload: u64,
        /// Give every class a fixed, non-borrowable slice, the way separate orchestrators
        /// owning separate budgets would. Off means one ledger arbitrates all of them
        #[arg(long)]
        hard_pools: bool,
        #[arg(long, default_value_t = 3)]
        repeat: usize,
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

    /// Measure what it costs to cross a boundary on this host: native call, shared ring,
    /// syscall, pipe, unix socket, TCP loopback
    Boundary {
        /// Repetitions; the best observation of each rung is kept and the spread reported
        #[arg(long, default_value_t = 5)]
        repeat: usize,
    },

    /// Does the unified advantage scale with volatility, and vanish at zero?
    Volatility {
        /// Accelerator HBM holding KV and weights, e.g. 4GiB. 0 models unified memory, where
        /// every class shares the DRAM pool
        #[arg(long, default_value = "4GiB", value_parser = parse_bytes)]
        hbm: u64,
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

/// Band-lexicographic objective: the most latency-critical band first, the most sacrificial
/// last, so a configuration is preferred if it improves a higher band even at the cost of a
/// lower one.
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
                    let r = run_on("", t, Budget::Split { split, hard }, &stream);
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

const PHASE_NAME: [&str; 4] = ["agent-heavy", "faas-burst", "service-steady", "mixed"];
const CLASS_NAME: [&str; BlobKind::N] = ["inference-kv", "faas", "weights", "service"];

fn memory_label(hbm: u64, dram: u64) -> String {
    if hbm == 0 {
        format!("unified memory: dram={:.1}GiB", gib(dram))
    } else {
        format!("hbm={:.1}GiB ddr={:.1}GiB", gib(hbm), gib(dram))
    }
}

/// One node's memory for the multi-node experiments.
///
/// Split memory budgets each pool for what lives in it. HBM carries KV against weights; the
/// weights floor holds two whole models, since one model is two 512 MiB shards and a floor
/// under that thrashes on something no policy can repair. DDR carries function cells and
/// service heaps, with modest floors for what the accelerator offloads. Unified memory keeps
/// the one-pool split the earlier rounds used, so the comparison is against what was there.
/// `hard` partitions every class at its floor: each class owns a fixed slice and may not
/// borrow from another's. That is what separate orchestrators owning separate budgets looks
/// like -- an inference gateway with a fixed KV allocation beside a `FaaS` control plane with
/// a fixed warm pool, neither able to see or lend to the other.
fn node_memory(hbm: u64, ddr: u64, nvme: u64, bands: [u8; BlobKind::N], hard: bool) -> NodeMemory {
    if hbm == 0 {
        let q = Quota::from_split(ddr, [0.10, 0.12, 0.50, 0.26], bands, hard);
        return NodeMemory {
            hbm: 0,
            ddr,
            nvme,
            hbm_quota: Quota::open(0, bands),
            ddr_quota: q,
            can_decode: true,
        };
    }
    NodeMemory {
        hbm,
        ddr,
        nvme,
        hbm_quota: Quota::from_split(hbm, [0.25, 0.0, 0.50, 0.0], bands, hard),
        ddr_quota: Quota::from_split(ddr, [0.10, 0.15, 0.15, 0.35], bands, hard).offloaded(),
        can_decode: true,
    }
}

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
    hbm: u64,
    dram: u64,
    nvme: u64,
    ops: u64,
    seed: u64,
    bands: [u8; BlobKind::N],
    rate: f64,
    drain_at: f64,
) {
    use polyphonic::machine::{Machine, Placement};
    use polyphonic::topo::Topology;

    let per_socket = dram / sockets as u64;
    let topo = Topology::synthetic(sockets, units_per_socket, per_socket);
    let memory = node_memory(
        hbm / sockets as u64,
        per_socket,
        nvme / sockets as u64,
        bands,
        false,
    );
    println!(
        "synthetic machine: {sockets} domains x {:.1} GiB, {units_per_socket} units each\n\
         cross-domain link constants are MODELLED (coherent, ~2x per-byte, 120 ns hop)",
        gib(per_socket)
    );
    if drain_at > 0.0 {
        println!(
            "domain 0 drained at {:.0}% through the trace; its state migrates\n",
            drain_at * 100.0
        );
    } else {
        println!();
    }

    println!(
        "{:<13} {:>12} {:>14} {:>12} {:>10} {:>12} {:>14}",
        "placement",
        "stall/req",
        "interconnect",
        "local hops",
        "cold",
        "bytes moved",
        "domain spread"
    );
    // Cross-socket links are coherent and cheap, so this is the topology where shipping
    // state should beat rebuilding it almost always. Whether it does is the point of the row.
    let modes = [
        (Placement::Blind, false),
        (Placement::Sticky, false),
        (Placement::Aware, false),
        (Placement::Aware, true),
        (Placement::Scored, false),
        (Placement::Scored, true),
    ];
    for (mode, transfer) in modes {
        let mut m = Machine::new(topo.clone(), |_| memory, Policy::Gdsf, mode);
        m.set_state_transfer(transfer);
        m.set_arrival_rate(rate);
        let mut total = 0u64;
        let mut served = 0u64;
        let drain_op = if drain_at > 0.0 {
            (ops as f64 * drain_at) as u64
        } else {
            u64::MAX
        };
        for (i, req) in polyphonic::work::Workload::new(seed, ops, 1.0).enumerate() {
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
        let label = match (mode, transfer) {
            (Placement::Blind, _) => "blind",
            (Placement::Sticky, _) => "sticky",
            (Placement::Aware, false) => "aware",
            (Placement::Aware, true) => "aware+fetch",
            (Placement::Scored, false) => "scored",
            (Placement::Scored, true) => "scored+fetch",
        };
        println!(
            "{label:<13} {:>11.3}ms {:>13.1}s {:>10.2}s {:>11.1}% {:>9.1}% {:>13.2}",
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

fn flows_report(
    hbm: u64,
    dram: u64,
    nvme: u64,
    ops: u64,
    seed: u64,
    step: f64,
    bands: [u8; BlobKind::N],
) {
    println!(
        "{} ops={ops} seed={seed} bands={bands:?}\n",
        memory_label(hbm, dram)
    );
    let cfg = Trial {
        bands,
        flows: FlowMode::Blind,
        hbm,
        dram,
        nvme,
        policy: Policy::Gdsf,
        seed,
        ops,
        vol: 1.0,
    };
    let (_, split) = best_split(cfg, false, step);
    let budget = Budget::Split { split, hard: false };
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
        let r = run("", trial, budget);
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

fn volatility_sweep(hbm: u64, dram: u64, nvme: u64, ops: u64, seed: u64, step: f64) {
    let bands = [0u8, 1, 2, 1];
    println!("{} ops={ops} seed={seed}\n", memory_label(hbm, dram));
    println!(
        "{:>10} {:>18} {:>16} {:>12}",
        "volatility", "hard-partition (ms)", "soft-floor (ms)", "advantage"
    );
    for i in 0..=5 {
        let v = f64::from(i) / 5.0;
        let t = Trial {
            bands,
            flows: FlowMode::Blind,
            hbm,
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

#[allow(
    clippy::too_many_arguments,
    reason = "experiment knobs, all independent"
)]
fn residency_report(
    hbm: u64,
    dram: u64,
    nvme: u64,
    ops: u64,
    seed: u64,
    step: f64,
    volatility: f64,
    bands: [u8; BlobKind::N],
) {
    println!(
        "{} nvme={:.1}GiB ops={ops} seed={seed} volatility={volatility}\n",
        memory_label(hbm, dram),
        gib(nvme)
    );
    println!("priority bands (operator-configured): {bands:?}\n");

    let t = Trial {
        bands,
        flows: FlowMode::Blind,
        hbm,
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
    let mut open = run("", t, Budget::Open);
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

#[allow(
    clippy::too_many_lines,
    reason = "one arm per subcommand, nothing else"
)]
fn main() {
    match Cli::parse().cmd {
        Cmd::Residency {
            hbm,
            dram,
            nvme,
            ops,
            seed,
            step,
            volatility,
            bands,
        } => {
            residency_report(
                hbm,
                dram,
                nvme,
                ops,
                seed,
                step,
                volatility,
                bands_of(&bands),
            );
        }
        Cmd::Calibrate { path, iters } => calibrate(&path, iters),
        Cmd::Boundary { repeat } => boundary(repeat),
        Cmd::Distributed {
            nodes,
            units_per_node,
            hbm,
            dram,
            nvme,
            ops,
            seed,
            bands,
            distances,
            crossing,
            gossip_period,
            rate,
            fanout,
            repeat,
        } => distributed(
            nodes,
            units_per_node,
            hbm,
            dram,
            nvme,
            ops,
            seed,
            bands_of(&bands),
            &distances,
            &crossing,
            gossip_period,
            rate,
            fanout,
            repeat,
        ),
        Cmd::CodeReview {
            hbm,
            model_ddr,
            agent_ddr,
            nvme,
            units_per_node,
            ops,
            seed,
            bands,
            distances,
            crossing,
            gossip_period,
            rate,
            tool_fraction,
            tool_payload,
            hard_pools,
            repeat,
        } => code_review(
            hbm,
            model_ddr,
            agent_ddr,
            nvme,
            units_per_node,
            ops,
            seed,
            bands_of(&bands),
            &distances,
            &crossing,
            gossip_period,
            rate,
            tool_fraction,
            tool_payload,
            hard_pools,
            repeat,
        ),
        Cmd::Topology { bytes, iters } => topology(bytes, iters),
        Cmd::Placement {
            sockets,
            units_per_socket,
            hbm,
            dram,
            nvme,
            ops,
            seed,
            bands,
            rate,
            drain_at,
        } => {
            placement(
                sockets,
                units_per_socket,
                hbm,
                dram,
                nvme,
                ops,
                seed,
                bands_of(&bands),
                rate,
                drain_at,
            );
        }
        Cmd::Flows {
            hbm,
            dram,
            nvme,
            ops,
            seed,
            step,
            bands,
        } => {
            flows_report(hbm, dram, nvme, ops, seed, step, bands_of(&bands));
        }
        Cmd::Volatility {
            hbm,
            dram,
            nvme,
            ops,
            seed,
            step,
        } => {
            volatility_sweep(hbm, dram, nvme, ops, seed, step);
        }
    }
}

fn boundary(repeat: usize) {
    use polyphonic::boundary::{Boundary, measure};

    let l = measure(repeat);
    println!(
        "boundary ladder (p50 ns per operation, measured on this host)\ntimer overhead {:.1} ns/call -- rungs near it are batch-timed and have no tail\n",
        l.timer_ns
    );
    print!("{:<22}", "boundary");
    for n in polyphonic::boundary::SIZES {
        print!("{:>12}", format!("{n} B"));
    }
    println!(
        "{:>12}{:>14}{:>12}{:>10}",
        "fixed ns", "ns/byte", "p99 ns", "spread"
    );

    for r in &l.rungs {
        print!("{:<22}", r.boundary.label());
        for n in polyphonic::boundary::SIZES {
            match r.by_size.iter().find(|s| s.0 == n) {
                Some((_, ns)) => print!("{ns:>12}"),
                None => print!("{:>12}", "-"),
            }
        }
        println!(
            "{:>12.0}{:>14.3}{:>12}{:>9.1}x",
            r.cost.fixed_ns,
            r.cost.ns_per_byte,
            if r.p99_ns == 0 {
                "-".to_string()
            } else {
                r.p99_ns.to_string()
            },
            r.spread
        );
    }

    let mid = polyphonic::boundary::SIZES[1];
    let at = |b: Boundary| -> Option<u64> {
        l.rungs
            .iter()
            .find(|r| r.boundary == b)
            .and_then(|r| r.by_size.iter().find(|s| s.0 == mid))
            .map(|s| s.1)
    };

    println!("\nwhat each step adds, at {mid} B");
    let steps = [
        (
            Boundary::Native,
            Boundary::Ring,
            "cross-core cache line + spin detect",
        ),
        (Boundary::Ring, Boundary::Syscall, "ring transition"),
        (
            Boundary::Syscall,
            Boundary::Pipe,
            "kernel buffer copy + second syscall",
        ),
        (
            Boundary::Pipe,
            Boundary::UnixSocket,
            "waking a blocked thread",
        ),
        (
            Boundary::UnixSocket,
            Boundary::TcpLoopback,
            "loopback network stack",
        ),
        (
            Boundary::TcpLoopback,
            Boundary::Grpc,
            "HTTP/2 framing + protobuf",
        ),
    ];
    for (lo, hi, what) in steps {
        let (Some(a), Some(b)) = (at(lo), at(hi)) else {
            continue;
        };
        println!(
            "  {what:<38}{:>10.2} us   {:>6.1}x",
            b.saturating_sub(a) as f64 / 1000.0,
            b as f64 / a.max(1) as f64
        );
    }

    if let (Some(total), Some(ring)) = (at(Boundary::Grpc), at(Boundary::Ring)) {
        let wake = at(Boundary::UnixSocket).unwrap_or(0) - at(Boundary::Pipe).unwrap_or(0);
        let frame = total - at(Boundary::TcpLoopback).unwrap_or(0);
        println!(
            "\nof a {:.1} us gRPC round trip: {:.0}% is HTTP/2 + protobuf, {:.0}% is one thread wakeup,\nand {:.2} us is what the same exchange costs through shared memory",
            total as f64 / 1000.0,
            100.0 * frame as f64 / total as f64,
            100.0 * wake as f64 / total as f64,
            ring as f64 / 1000.0
        );
    }
}

#[derive(Default, Clone)]
struct ClassTally {
    stall: [u64; BlobKind::N],
    service: [u64; BlobKind::N],
    decide: [u64; BlobKind::N],
    ops: [u64; BlobKind::N],
    warm: [u64; BlobKind::N],
    /// Service time of warm requests only. A warm invocation is the regime where an overhead
    /// measured in tens of microseconds stops being a rounding error.
    warm_ns: [u64; BlobKind::N],
}

type ClassRow<'a> = (&'a str, ClassTally);
type ClassRows<'a> = [ClassRow<'a>];

/// Service time is the denominator that matters: an overhead is only ever a fraction of the
/// work it decorates, and a warm invocation has almost no work.
fn class_table(rows: &ClassRows<'_>) {
    println!("\n  per class: mean service time (ms) / share spent deciding / warm rate");
    print!("  {:<18}", "arm");
    for name in CLASS_NAME {
        print!("{name:>26}");
    }
    println!();
    for (label, t) in rows {
        print!("  {label:<18}");
        for k in BlobKind::ALL {
            let i = k.idx();
            print!(
                "{:>14.3} {:>5.2}% {:>4.0}%",
                mean_ms(t.service[i], t.ops[i]),
                100.0 * t.decide[i] as f64 / t.service[i].max(1) as f64,
                100.0 * t.warm[i] as f64 / t.ops[i].max(1) as f64,
            );
        }
        println!();
    }
    println!();
}

use polyphonic::machine::{Control, Placement};

struct Arm {
    label: &'static str,
    placement: Placement,
    flow: bool,
    control: Control,
    /// May a node pull missing state off a peer instead of rebuilding it? Held apart from
    /// placement so the two can be attributed separately: one decides where work runs, the
    /// other decides how its state gets there once that is settled.
    transfer: bool,
}

fn arm(
    label: &'static str,
    placement: Placement,
    flow: bool,
    control: Control,
    transfer: bool,
) -> Arm {
    Arm {
        label,
        placement,
        flow,
        control,
        transfer,
    }
}

fn distributed_arms(gossip_period: u64) -> Vec<Arm> {
    use Control::{Gossip, Query, Unified};
    use Placement::{Aware, Scored, Sticky};
    vec![
        arm("hash only", Sticky, false, Unified, false),
        arm("residency only", Aware, false, Unified, false),
        arm("flow only", Sticky, true, Unified, false),
        arm("both, unified", Aware, true, Unified, false),
        arm("both, rpc query", Aware, true, Query, false),
        arm(
            "both, gossiped",
            Aware,
            true,
            Gossip {
                period: gossip_period,
            },
            false,
        ),
        arm("residency + fetch", Aware, false, Unified, true),
        arm("scored, no flows", Scored, false, Unified, false),
        arm("scored", Scored, true, Unified, false),
        arm("scored + fetch", Scored, true, Unified, true),
        arm(
            "scored + fetch, gossiped",
            Scored,
            true,
            Gossip {
                period: gossip_period,
            },
            true,
        ),
    ]
}

/// How the three acquisition routes actually split, what the engine did with the load, and
/// how fan-outs and their tool calls were placed. Printed for every arm because an arm that never
/// fetches and an arm that cannot fetch produce the same stall number for opposite reasons.
fn state_terms(mach: &polyphonic::machine::Machine, served: u64) {
    let pct = |n: u64| 100.0 * n as f64 / served.max(1) as f64;
    // Shares of materialisations, not of requests: a request whose chain was already
    // resident acquired nothing, and counting it would hide the split this line is for.
    let acts = (mach.fetches + mach.rebuilds).max(1) as f64;
    print!(
        "{:<22} acquired: {:.1}% fetched ({:.1} GiB), {:.1}% rebuilt, {:.1}% stale",
        "",
        100.0 * mach.fetches as f64 / acts,
        gib(mach.fetched_bytes),
        100.0 * mach.rebuilds as f64 / acts,
        pct(mach.stale_fetches),
    );
    if mach.mean_batch() > 0.0 {
        // Per decode rather than as a share of stall: a fan-out's stall is its slowest
        // agent's, but every agent queued, so the share stops meaning anything once agents run.
        print!(
            "; batch {:.1}, queue {:.2} ms/decode, {:.1}% of decodes arrived saturated",
            mach.mean_batch(),
            mean_ms(mach.queue_ns(), mach.decodes()),
            100.0 * mach.saturated() as f64 / mach.decodes().max(1) as f64,
        );
    }
    println!();
    let fanouts = mach.fanouts_admitted + mach.fanouts_refused;
    if fanouts > 0 {
        println!(
            "{:<22} fan-outs {}/{} ran, {:.1} ms each; agents co-located {:.1}%; \
             {} tool calls, {:.1}% beside their agent, {:.2} ms each",
            "",
            mach.fanouts_admitted,
            fanouts,
            mean_ms(mach.fanout_service_ns, mach.fanouts_admitted),
            100.0 * mach.agents_colocated as f64 / mach.agents_run.max(1) as f64,
            mach.tool_calls,
            100.0 * mach.tool_coplaced as f64 / mach.tool_calls.max(1) as f64,
            mean_ms(mach.tool_ns, mach.tool_calls),
        );
    }
}

/// Mean spread of each term across candidate nodes. An argmin is decided by spread alone, so
/// this is what says whether a term can ever outvote another one.
fn term_spread(mach: &polyphonic::machine::Machine) {
    use polyphonic::machine::TERM_LABELS;
    let n = mach.scored_decisions.max(1) as f64;
    print!("{:<22} term spread (mean, ms):", "");
    for (label, total) in TERM_LABELS.iter().zip(mach.term_spread) {
        print!(" {label} {:.2}", total / n / 1e6);
    }
    println!();
}

fn score_terms(mach: &polyphonic::machine::Machine, served: u64) {
    let pct = |n: u64| 100.0 * n as f64 / served.max(1) as f64;
    println!(
        "{:<22} moved by displacement {:.1}%, by flow {:.1}%, by load {:.1}%, by congestion \
         {:.1}%, held at affinity {:.1}%; {:.1}% of {} flows co-placed; engine spread {:.2}",
        "",
        pct(mach.moved_by_displacement),
        pct(mach.moved_by_flow),
        pct(mach.moved_by_load),
        pct(mach.moved_by_congestion),
        pct(mach.held_by_affinity),
        100.0 * mach.flow_coplaced as f64 / mach.flow_requests.max(1) as f64,
        mach.flow_requests,
        mach.engine_spread(),
    );
}

fn cluster_header(
    nodes: usize,
    units_per_node: usize,
    memory: &NodeMemory,
    crossing: &str,
    cost: polyphonic::boundary::Cost,
) {
    println!(
        "cluster: {nodes} nodes, {} per node, {units_per_node} units each\n\
         control crossing: {crossing} = {:.1} us + {:.3} ns/byte (MEASURED on this host)\n\
         node link latency, bandwidth and PCIe are MODELLED\n",
        memory_label(memory.hbm, memory.ddr),
        cost.fixed_ns / 1000.0,
        cost.ns_per_byte,
    );
}

fn crossing_of(l: &polyphonic::boundary::Ladder, name: &str) -> Option<polyphonic::boundary::Cost> {
    use polyphonic::boundary::Boundary;
    let b = match name {
        "native" => Boundary::Native,
        "ring" => Boundary::Ring,
        "syscall" => Boundary::Syscall,
        "pipe" => Boundary::Pipe,
        "unix" => Boundary::UnixSocket,
        "tcp" => Boundary::TcpLoopback,
        _ => Boundary::Grpc,
    };
    l.get(b).or_else(|| l.get(Boundary::TcpLoopback))
}

#[allow(
    clippy::too_many_arguments,
    reason = "experiment knobs, all independent"
)]
fn distributed(
    nodes: usize,
    units_per_node: usize,
    hbm: u64,
    dram: u64,
    nvme: u64,
    ops: u64,
    seed: u64,
    bands: [u8; BlobKind::N],
    distances: &str,
    crossing: &str,
    gossip_period: u64,
    rate: f64,
    fanout: f64,
    repeat: usize,
) {
    use polyphonic::machine::Machine;
    use polyphonic::topo::{Distance, Topology};

    let ladder = polyphonic::boundary::measure(repeat);
    let Some(cost) = crossing_of(&ladder, crossing) else {
        println!("no boundary rung available");
        return;
    };
    let per_node = dram / nodes as u64;
    let memory = node_memory(
        hbm / nodes as u64,
        per_node,
        nvme / nodes as u64,
        bands,
        false,
    );

    cluster_header(nodes, units_per_node, &memory, crossing, cost);

    // Residency routing and flow co-placement are separate mechanisms that were previously
    // bundled into one arm. Split so the win can be attributed to one of them.
    let mut warm_seen = [(0u64, 0u64); BlobKind::N];
    let arms = distributed_arms(gossip_period);

    for name in distances.split(',') {
        let Ok(dist) = name.trim().parse::<Distance>() else {
            println!("skipping unknown distance {name}");
            continue;
        };
        let topo = Topology::cluster(nodes, units_per_node, per_node, dist, cost);
        println!(
            "== {} : {:.0} us hop, {:.2} ns/byte ==",
            dist.label(),
            dist.one_way_ns() as f64 / 1000.0,
            dist.ns_per_byte()
        );
        // Service time leads: once decode cost depends on the batch a request joins, placement
        // moves execution as well as waiting, and stall alone cannot see the difference.
        println!(
            "{:<22} {:>13} {:>12} {:>9} {:>10} {:>11} {:>9} {:>9} {:>9} {:>11}",
            "arm",
            "service/req",
            "stall/req",
            "served",
            "fan-outs",
            "deciding",
            "of stall",
            "split",
            "handoff",
            "spread"
        );
        let mut per_class: Vec<ClassRow<'_>> = Vec::new();
        for a in &arms {
            let label = a.label;
            let mut mach = Machine::new(topo.clone(), |_| memory, Policy::Gdsf, a.placement);
            mach.set_flow_aware(a.flow);
            mach.set_control(a.control, cost);
            mach.set_state_transfer(a.transfer);
            mach.set_arrival_rate(rate);
            mach.set_fanout_atomic(true);
            let (t, total, served, offered) = drive(
                &mut mach,
                rate,
                polyphonic::work::Workload::with_fanout(seed, ops, 1.0, fanout),
            );
            let stall = mean_ms(total, served);
            let service = mean_ms(t.service.iter().sum(), served);
            println!(
                "{label:<22} {service:>11.3}ms {stall:>10.3}ms {:>8.1}% {:>9.1}% {:>9.3}ms \
                 {:>8.2}% {:>8.1}% {:>8.2}s {:>11.2}",
                100.0 * served as f64 / offered.max(1) as f64,
                100.0 * mach.fanouts_admitted as f64
                    / (mach.fanouts_admitted + mach.fanouts_refused).max(1) as f64,
                mean_ms(mach.decide_ns, served),
                100.0 * mach.decide_ns as f64 / total.max(1) as f64,
                100.0 * mach.split_tasks as f64
                    / (mach.split_tasks + mach.joined_tasks).max(1) as f64,
                mach.handoff_ns as f64 / 1e9,
                mach.domain_spread(),
            );
            state_terms(&mach, served);
            if a.placement == Placement::Scored {
                score_terms(&mach, served);
                term_spread(&mach);
            }
            for (seen, (ns, n)) in warm_seen.iter_mut().zip(t.warm_ns.iter().zip(&t.warm)) {
                seen.0 += ns;
                seen.1 += n;
            }
            per_class.push((label, t));
        }

        class_table(&per_class);
        fanout_admission(&topo, memory, seed, ops, rate, fanout);
    }
    crossover(&ladder, cost, &warm_seen);
}

/// One model host and one agent-framework host, swept from same-socket to cross-region.
///
/// The two nodes are deliberately unequal. Node 0 has the accelerator and is the only place a
/// decode can run. Node 1 has host memory and no engine at all, which is what an agent
/// framework actually runs on: it holds the orchestrator process and its tool-call cells, and
/// every reasoning step it wants has to cross the link to node 0 and come back.
///
/// `set_tool_anchor(1)` pins the recorded origin of each tool call to the agent host rather
/// than to wherever the model ran the turn that asked for it. Without it, a tool call's
/// flow-affinity would pull it toward the accelerator, which is only right when the
/// orchestrator and the engine share a host -- exactly what this topology says they do not.
/// Tool calls still *may* ship to node 0 when the score says a warm cell there beats a local
/// restore; the anchor makes staying home the default, not the only option.
#[allow(
    clippy::too_many_arguments,
    reason = "experiment knobs, all independent"
)]
#[allow(clippy::too_many_lines, reason = "one scenario, printed in full")]
fn code_review(
    hbm: u64,
    model_ddr: u64,
    agent_ddr: u64,
    nvme: u64,
    units_per_node: usize,
    ops: u64,
    seed: u64,
    bands: [u8; BlobKind::N],
    distances: &str,
    crossing: &str,
    gossip_period: u64,
    rate: f64,
    tool_fraction: f64,
    tool_payload: u64,
    hard_pools: bool,
    repeat: usize,
) {
    use polyphonic::machine::Machine;
    use polyphonic::topo::{Distance, Topology};

    const MODEL: usize = 0;
    const AGENT: usize = 1;
    const NODES: usize = 2;

    let ladder = polyphonic::boundary::measure(repeat);
    let Some(cost) = crossing_of(&ladder, crossing) else {
        println!("no boundary rung available");
        return;
    };

    let model_mem = node_memory(hbm, model_ddr, nvme / 2, bands, hard_pools);
    // No accelerator and no engine: `FaaS` cells and service heaps only. Its DDR budget is the
    // host split with the accelerator classes' floors left open, since neither can land here.
    let agent_mem = NodeMemory {
        hbm: 0,
        ddr: agent_ddr,
        nvme: nvme / 2,
        hbm_quota: Quota::open(0, bands),
        ddr_quota: Quota::from_split(agent_ddr, [0.0, 0.35, 0.0, 0.35], bands, hard_pools),
        can_decode: false,
    };
    let memory_at = move |d: usize| if d == MODEL { model_mem } else { agent_mem };

    println!(
        "model host:  hbm={:.1}GiB ddr={:.1}GiB, decode engine\n\
         agent host:  ddr={:.1}GiB, no accelerator -- cannot decode, runs tool calls\n\
         control crossing: {crossing} = {:.1} us + {:.3} ns/byte (MEASURED on this host)\n\
         node link, PCIe and every workload constant are MODELLED\n\
         memory: {}\n\
         tool calls: {:.0}% of turns, {:.0} KiB each way, anchored to the agent host\n",
        gib(hbm),
        gib(model_ddr),
        gib(agent_ddr),
        cost.fixed_ns / 1000.0,
        cost.ns_per_byte,
        if hard_pools {
            "hard partitions -- each class owns a fixed slice, no borrowing"
        } else {
            "one ledger, soft floors"
        },
        100.0 * tool_fraction,
        tool_payload as f64 / 1024.0,
    );

    let arms = distributed_arms(gossip_period);
    let mut warm_seen = [(0u64, 0u64); BlobKind::N];

    for name in distances.split(',') {
        let Ok(dist) = name.trim().parse::<Distance>() else {
            println!("skipping unknown distance {name}");
            continue;
        };
        // `dram_per_node` only sizes the topology's domain records; the ledger's real budgets
        // come from `memory_at`, which differs per node.
        let topo = Topology::cluster(NODES, units_per_node, model_ddr, dist, cost);
        println!(
            "== {} : {:.0} us hop, {:.2} ns/byte ==",
            dist.label(),
            dist.one_way_ns() as f64 / 1000.0,
            dist.ns_per_byte()
        );
        println!(
            "{:<22} {:>13} {:>12} {:>9} {:>11} {:>10} {:>10} {:>11}",
            "arm",
            "service/req",
            "stall/req",
            "served",
            "round trip",
            "tools home",
            "handoff",
            "on model"
        );
        let mut per_class: Vec<ClassRow<'_>> = Vec::new();
        for a in &arms {
            let label = a.label;
            let mut mach = Machine::new(topo.clone(), memory_at, Policy::Gdsf, a.placement);
            mach.set_flow_aware(a.flow);
            mach.set_control(a.control, cost);
            mach.set_state_transfer(a.transfer);
            mach.set_fanout_atomic(true);
            mach.set_tool_anchor(Some(AGENT));
            // Every reasoning request starts at the agent host and its answer returns there.
            // The context delta going in is dominated by the last tool result the agent
            // gathered, so that is what sizes the trip.
            mach.set_origin(Some((AGENT, tool_payload)));
            let (t, total, served, offered) = drive(
                &mut mach,
                rate,
                polyphonic::work::Workload::new(seed, ops, 1.0)
                    .with_tool_profile(tool_fraction, tool_payload),
            );
            let stall = mean_ms(total, served);
            let service = mean_ms(t.service.iter().sum(), served);
            // A task is "split" when its downstream stage did not run where its upstream did.
            // Here that is the agent host keeping its own tool call, so the complement is the
            // share of tool calls that stayed home.
            let stages = (mach.split_tasks + mach.joined_tasks).max(1);
            println!(
                "{label:<22} {service:>11.3}ms {stall:>10.3}ms {:>8.1}% {:>9.3}ms {:>9.1}% \
                 {:>9.2}s {:>10.1}%",
                100.0 * served as f64 / offered.max(1) as f64,
                mean_ms(mach.origin_ns, mach.origin_hops),
                100.0 * mach.joined_tasks as f64 / stages as f64,
                mach.handoff_ns as f64 / 1e9,
                100.0 * mach.decodes_on(MODEL) as f64 / mach.decodes().max(1) as f64,
            );
            state_terms(&mach, served);
            if a.placement == Placement::Scored {
                score_terms(&mach, served);
            }
            for (seen, (ns, n)) in warm_seen.iter_mut().zip(t.warm_ns.iter().zip(&t.warm)) {
                seen.0 += ns;
                seen.1 += n;
            }
            per_class.push((label, t));
        }
        class_table(&per_class);
    }
    crossover(&ladder, cost, &warm_seen);
}

/// Run one configured machine over any request stream, tallying per class. Shared by every
/// experiment so a comparison can never accidentally be between two different accounting
/// rules; callers build whatever `Workload` shape the scenario calls for.
fn drive(
    mach: &mut polyphonic::machine::Machine,
    rate: f64,
    workload: polyphonic::work::Workload,
) -> (ClassTally, u64, u64, u64) {
    mach.set_arrival_rate(rate);
    let mut t = ClassTally::default();
    let (mut total, mut served, mut offered) = (0u64, 0u64, 0u64);
    for req in workload {
        let k = req.kind_idx();
        offered += 1;
        let c = mach.serve_request(&req);
        if c.pending {
            continue;
        }
        total += c.total_ns();
        t.stall[k] += c.total_ns();
        t.service[k] += c.service_ns();
        t.decide[k] += c.decide_ns;
        t.ops[k] += 1;
        // Warm means the ledger had everything: no fetch, no recompute, just the work.
        if c.transfer_ns == 0 && c.recompute_ns == 0 {
            t.warm[k] += 1;
            t.warm_ns[k] += c.service_ns();
        }
        served += 1;
    }
    (t, total, served, offered)
}

/// What all-or-nothing fan-out admission is worth, measured rather than argued.
///
/// The baseline is the same agents admitted one at a time, which is what a per-request
/// scheduler does when nobody told it the requests belong together. An orchestrator missing
/// one agent cannot resume, so every agent that did run was work for nothing -- and it ran on
/// engines and memory that other requests needed.
fn fanout_admission(
    topo: &polyphonic::topo::Topology,
    memory: NodeMemory,
    seed: u64,
    ops: u64,
    rate: f64,
    fanout: f64,
) {
    use polyphonic::machine::{Machine, Placement};
    if fanout <= 0.0 {
        return;
    }
    println!("\n  fan-out admission (scored + fetch, unified control)");
    println!(
        "  {:<16} {:>11} {:>13} {:>12} {:>12} {:>16} {:>9}",
        "admission", "fan-outs", "wasted work", "fan-out", "stall/req", "inference stall", "served"
    );
    for atomic in [false, true] {
        let mut mach = Machine::new(topo.clone(), |_| memory, Policy::Gdsf, Placement::Scored);
        mach.set_flow_aware(true);
        mach.set_state_transfer(true);
        mach.set_fanout_atomic(atomic);
        let (t, total, served, offered) = drive(
            &mut mach,
            rate,
            polyphonic::work::Workload::with_fanout(seed, ops, 1.0, fanout),
        );
        println!(
            "  {:<16} {:>6}/{:<4} {:>11.2}s {:>10.1}ms {:>10.3}ms {:>14.3}ms {:>8.1}%",
            if atomic {
                "all-or-nothing"
            } else {
                "per agent"
            },
            mach.fanouts_admitted,
            mach.fanouts_admitted + mach.fanouts_refused,
            mach.fanout_wasted_ns as f64 / 1e9,
            mean_ms(mach.fanout_service_ns, mach.fanouts_admitted),
            mean_ms(total, served),
            mean_ms(t.stall[0], t.ops[0]),
            100.0 * served as f64 / offered.max(1) as f64,
        );
    }
}

/// The boundary tax is not a fixed overhead, it is a fraction -- and the fraction depends
/// entirely on how long the work being scheduled takes. The ladder is measured; this only
/// divides it by service times spanning a warm `FaaS` invocation to a full prefill.
fn crossover(
    ladder: &polyphonic::boundary::Ladder,
    grpc: polyphonic::boundary::Cost,
    warm: &[(u64, u64); BlobKind::N],
) {
    use polyphonic::boundary::Boundary;
    const RPCS: f64 = 4.0;
    let Some(ring) = ladder.get(Boundary::Ring) else {
        return;
    };
    let q = 1024;
    let (g, r) = (RPCS * grpc.ns(q) as f64, RPCS * ring.ns(q) as f64);
    println!(
        "control-plane tax as a share of one request, at {RPCS:.0} decisions/request\n\
         (gRPC {:.1} us and shared ring {:.2} us per decision, both measured)\n",
        grpc.ns(q) as f64 / 1000.0,
        ring.ns(q) as f64 / 1000.0
    );
    println!(
        "{:<40} {:>12} {:>12}",
        "work being scheduled", "over gRPC", "over a ring"
    );
    let row = |name: &str, ns: f64| {
        println!(
            "{name:<40} {:>11.1}% {:>11.2}%",
            100.0 * g / (g + ns),
            100.0 * r / (r + ns)
        );
    };
    for (k, name) in CLASS_NAME.iter().enumerate() {
        let (ns, n) = warm[k];
        if n == 0 {
            continue;
        }
        let mean = ns as f64 / n as f64;
        row(
            &format!("warm {name} ({n} seen, {:.0} us)", mean / 1000.0),
            mean,
        );
    }
    println!();
    for (name, ns) in [
        ("hypothetical: 10 us of work", 10_000.0),
        ("hypothetical: 1 ms of work", 1_000_000.0),
        ("hypothetical: 100 ms of work", 100_000_000.0),
    ] {
        row(name, ns);
    }
}
