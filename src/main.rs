mod arms;
mod blob;
mod cache;
mod plat;
mod rng;
mod store;
mod tier;
mod work;

use arms::{Cache, Report, run};
use blob::BlobKind;
use cache::Policy;
use clap::{Parser, Subcommand};

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

fn best_static(
    dram: u64,
    nvme: u64,
    policy: Policy,
    seed: u64,
    ops: u64,
    step: f64,
    vol: f64,
) -> (Report, [f64; BlobKind::N]) {
    let mut best: Option<(Report, [f64; BlobKind::N])> = None;
    let n = (1.0 / step).round() as u64;
    for a in 1..n {
        for b in 1..n - a {
            for c in 1..n - a - b {
                let split = [a, b, c, n - a - b - c].map(|x| x as f64 / n as f64);
                let r = run("", Cache::siloed(dram, nvme, split, policy), seed, ops, vol);
                if best.as_ref().is_none_or(|(x, _)| r.total_ns < x.total_ns) {
                    best = Some((r, split));
                }
            }
        }
    }
    best.expect("sweep produced no candidate partitions")
}

const PHASE_NAME: [&str; 4] = [
    "inference-heavy",
    "faas-burst",
    "training-window",
    "mixed-steady",
];

fn ms(ns: u64) -> f64 {
    ns as f64 / 1e6
}

fn gib(b: u64) -> f64 {
    b as f64 / (1u64 << 30) as f64
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
        } => {
            residency_report(dram, nvme, ops, seed, step, volatility);
        }
        Cmd::Calibrate { path, iters } => calibrate(&path, iters),
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

fn calibrate(path: &str, iters: u32) {
    use crate::blob::BlobId;
    use crate::store::Store;

    let sizes: [(&str, usize); 4] = [
        ("kv-block   512KiB", 512 * 1024),
        ("prefill-batch 2MiB", 2 * 1024 * 1024),
        ("snapshot    32MiB", 32 * 1024 * 1024),
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

fn volatility_sweep(dram: u64, nvme: u64, ops: u64, seed: u64, step: f64) {
    println!("dram={:.1}GiB ops={ops} seed={seed}\n", gib(dram));
    println!(
        "{:>10} {:>16} {:>16} {:>12}",
        "volatility", "siloed-gdsf (s)", "unified (s)", "advantage"
    );
    for i in 0..=5 {
        let v = f64::from(i) / 5.0;
        let (gd, _) = best_static(dram, nvme, Policy::Gdsf, seed, ops, step, v);
        let uni = run("", Cache::unified(dram, nvme, Policy::Gdsf), seed, ops, v);
        let adv = 100.0 * (gd.total_ns as f64 - uni.total_ns as f64) / gd.total_ns as f64;
        println!(
            "{v:>10.1} {:>16.2} {:>16.2} {adv:>11.1}%",
            gd.total_ns as f64 / 1e9,
            uni.total_ns as f64 / 1e9
        );
    }
}

fn arm_row(r: &Report) {
    println!(
        "{:<34} {:>11.2} {:>9.0}% {:>9.3} {:>9.3}   {:.2}/{:.2}/{:.2}/{:.2}   {:.1}/{:.1}/{:.1}/{:.1}",
        r.label,
        r.total_ns as f64 / 1e9,
        100.0 * r.transfer_ns as f64 / r.total_ns.max(1) as f64,
        ms(r.p50_ns),
        ms(r.p99_ns),
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

fn residency_report(dram: u64, nvme: u64, ops: u64, seed: u64, step: f64, volatility: f64) {
    println!(
        "dram={:.1}GiB nvme={:.1}GiB ops={ops} seed={seed} volatility={volatility}\n",
        gib(dram),
        gib(nvme)
    );

    let (mut lru, ls) = best_static(dram, nvme, Policy::Lru, seed, ops, step, volatility);
    lru.label = format!(
        "siloed-lru   [{:.2}/{:.2}/{:.2}/{:.2}]",
        ls[0], ls[1], ls[2], ls[3]
    );
    let (mut gd, gs) = best_static(dram, nvme, Policy::Gdsf, seed, ops, step, volatility);
    gd.label = format!(
        "siloed-gdsf  [{:.2}/{:.2}/{:.2}/{:.2}]",
        gs[0], gs[1], gs[2], gs[3]
    );
    let uni = run(
        "unified-gdsf",
        Cache::unified(dram, nvme, Policy::Gdsf),
        seed,
        ops,
        volatility,
    );

    println!(
        "{:<34} {:>11} {:>10} {:>9} {:>9} {:>20} {:>21}",
        "arm", "stall (s)", "from tier", "p50 (ms)", "p99 (ms)", "hit kv/sn/wt/svc", "resident GiB"
    );
    for r in [&lru, &gd, &uni] {
        arm_row(r);
    }
    println!("\nadmission integrity (nonzero overcommit invalidates the row above)");
    for r in [&lru, &gd, &uni] {
        println!(
            "{:<34} overcommit={:<10} pinned-skips={}",
            r.label, r.overcommit, r.pinned_skips
        );
    }

    println!("\nstall (s) by phase");
    println!(
        "{:<34} {:>16} {:>16} {:>16} {:>16}",
        "arm", PHASE_NAME[0], PHASE_NAME[1], PHASE_NAME[2], PHASE_NAME[3]
    );
    for r in [&lru, &gd, &uni] {
        print!("{:<34}", r.label);
        for p in r.phase_ns {
            print!("{:>16.2}", p as f64 / 1e9);
        }
        println!();
    }
    print!("{:<34}", "unified advantage");
    for i in 0..PHASE_NAME.len() {
        let b = gd.phase_ns[i] as f64;
        print!(
            "{:>15.1}%",
            100.0 * (b - uni.phase_ns[i] as f64) / b.max(1.0)
        );
    }
    println!();

    println!("\nmean stall per request (ms), by workload class");
    println!(
        "{:<34} {:>13} {:>13} {:>13} {:>13}",
        "arm", "inference", "faas", "training", "service"
    );
    for r in [&gd, &uni] {
        print!("{:<34}", r.label);
        for k in 0..BlobKind::N {
            print!("{:>13.2}", ms(r.kind_ns[k] / r.kind_ops[k].max(1)));
        }
        println!();
    }
    print!("{:<34}", "unified advantage");
    for k in 0..BlobKind::N {
        let b = (gd.kind_ns[k] / gd.kind_ops[k].max(1)) as f64;
        let u = (uni.kind_ns[k] / uni.kind_ops[k].max(1)) as f64;
        print!("{:>12.1}%", 100.0 * (b - u) / b.max(1.0));
    }
    println!();

    let win =
        |base: &Report| 100.0 * (base.total_ns as f64 - uni.total_ns as f64) / base.total_ns as f64;
    println!(
        "\nunified vs best-static-lru:  {:+.1}% stall\nunified vs best-static-gdsf: {:+.1}% stall  <- attributable to unification alone",
        win(&lru),
        win(&gd)
    );
}
