mod arms;
mod blob;
mod cache;
mod plat;
mod rng;
mod store;
mod tier;
mod work;

use arms::{Report, Trial, mean_ms, run};
use blob::BlobKind;
use cache::{Policy, Quota};
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
        /// Priority bands as inference,faas,training,service (0 = highest)
        #[arg(long, default_value = "0,1,2,1", value_parser = parse_bands)]
        bands: String,
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
    let n = (1.0 / step).round() as u64;
    for a in 1..n {
        for b in 1..n - a {
            for c in 1..n - a - b {
                // splits sum to less than n; the remainder is shared slack
                for d in 1..n - a - b - c {
                    let split = [a, b, c, d].map(|x| x as f64 / n as f64);
                    let q = Quota::from_split(t.dram, split, t.bands, hard);
                    let r = run("", t, q);
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
    use crate::blob::BlobId;
    use crate::store::Store;

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
