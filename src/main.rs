mod arms;
mod blob;
mod cache;
mod rng;
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
        #[arg(long, default_value_t = 0.1)]
        step: f64,
        /// Phase-shift amplitude: 0 = flat mix, 1 = full swing
        #[arg(long, default_value_t = 1.0)]
        volatility: f64,
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
        #[arg(long, default_value_t = 0.1)]
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
) -> (Report, [f64; 3]) {
    let mut best: Option<(Report, [f64; 3])> = None;
    let n = (1.0 / step) as u64;
    for a in 1..n {
        for b in 1..n - a {
            let split = [
                a as f64 * step,
                b as f64 * step,
                1.0 - (a + b) as f64 * step,
            ];
            if split[2] < step - 1e-9 {
                continue;
            }
            let r = run("", Cache::siloed(dram, nvme, split, policy), seed, ops, vol);
            if best.as_ref().is_none_or(|(x, _)| r.total_ns < x.total_ns) {
                best = Some((r, split));
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
        "{:<28} {:>12.2} {:>10.0}% {:>10.3} {:>10.3} {:>6.2}/{:.2}/{:.2} {:>10.2}/{:.2}/{:.2}",
        r.label,
        r.total_ns as f64 / 1e9,
        100.0 * r.transfer_ns as f64 / r.total_ns.max(1) as f64,
        ms(r.p50_ns),
        ms(r.p99_ns),
        r.hit[BlobKind::KvBlock.idx()],
        r.hit[BlobKind::Snapshot.idx()],
        r.hit[BlobKind::WeightShard.idx()],
        gib(r.resident[BlobKind::KvBlock.idx()]),
        gib(r.resident[BlobKind::Snapshot.idx()]),
        gib(r.resident[BlobKind::WeightShard.idx()]),
    );
}

fn residency_report(dram: u64, nvme: u64, ops: u64, seed: u64, step: f64, volatility: f64) {
    println!(
        "dram={:.1}GiB nvme={:.1}GiB ops={ops} seed={seed} volatility={volatility}\n",
        gib(dram),
        gib(nvme)
    );

    let (mut lru, ls) = best_static(dram, nvme, Policy::Lru, seed, ops, step, volatility);
    lru.label = format!("siloed-lru   [{:.2}/{:.2}/{:.2}]", ls[0], ls[1], ls[2]);
    let (mut gd, gs) = best_static(dram, nvme, Policy::Gdsf, seed, ops, step, volatility);
    gd.label = format!("siloed-gdsf  [{:.2}/{:.2}/{:.2}]", gs[0], gs[1], gs[2]);
    let uni = run(
        "unified-gdsf",
        Cache::unified(dram, nvme, Policy::Gdsf),
        seed,
        ops,
        volatility,
    );

    println!(
        "{:<28} {:>12} {:>11} {:>10} {:>10} {:>18} {:>22}",
        "arm", "stall (s)", "from tier", "p50 (ms)", "p99 (ms)", "hit kv/snap/wt", "resident GiB"
    );
    for r in [&lru, &gd, &uni] {
        arm_row(r);
    }

    println!("\nstall (s) by phase");
    println!(
        "{:<28} {:>16} {:>16} {:>16} {:>16}",
        "arm", PHASE_NAME[0], PHASE_NAME[1], PHASE_NAME[2], PHASE_NAME[3]
    );
    for r in [&lru, &gd, &uni] {
        print!("{:<28}", r.label);
        for p in r.phase_ns {
            print!("{:>16.2}", p as f64 / 1e9);
        }
        println!();
    }
    print!("{:<28}", "unified advantage");
    for i in 0..PHASE_NAME.len() {
        let b = gd.phase_ns[i] as f64;
        print!(
            "{:>15.1}%",
            100.0 * (b - uni.phase_ns[i] as f64) / b.max(1.0)
        );
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
