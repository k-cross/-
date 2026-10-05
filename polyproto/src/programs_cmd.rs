use std::thread;

use polyphonic::machine::{Claim, ClaimKey, EngineWait, HintGrade};
use polyphonic::programs::{
    AnnotationMode, ClaimBy, ClassMode, Config, Hints, LogCause, LoopMode, Outcome, Preset,
    ROLE_NAMES, ReplayConfig, ReplayOutcome, Shape, ShapeConfig, Speculate, Suspend, ToolSet, mean,
    quantile, rag_reference, replay, replay_matches_submit, role_root, run,
};
use polyphonic::work::Pattern;

use super::influence_cmd::{change, plain_seeds, section, seeds_of};

pub struct Env {
    pub seeds: u64,
    pub seed: u64,
    pub programs: usize,
    pub rate: f64,
    pub compress: f64,
    pub ops: u64,
    pub sections: String,
}

fn base(env: &Env, seed: u64) -> Config {
    Config {
        seed,
        programs: env.programs,
        rate: env.rate,
        shape: ShapeConfig {
            compress: env.compress,
            ..ShapeConfig::default()
        },
        ..Config::default()
    }
}

fn each<T: Send>(env: &Env, f: impl Fn(&mut Config) -> T + Sync) -> Vec<T> {
    thread::scope(|scope| {
        let handles: Vec<_> = (0..env.seeds)
            .map(|i| {
                let f = &f;
                scope.spawn(move || {
                    let mut cfg = base(env, env.seed + i);
                    f(&mut cfg)
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().expect("a seed's run completes"))
            .collect()
    })
}

fn runs(env: &Env, f: impl Fn(&mut Config) + Sync) -> Vec<Outcome> {
    each(env, |cfg| {
        f(cfg);
        run(cfg)
    })
}

fn replays(env: &Env, rc: ReplayConfig, f: impl Fn(&mut Config) + Sync) -> Vec<ReplayOutcome> {
    each(env, |cfg| {
        f(cfg);
        replay(cfg, &rc)
    })
}

fn ms(ns: f64) -> f64 {
    ns / 1e6
}

fn turn_ms(out: &Outcome, pattern: Pattern) -> f64 {
    ms(mean(out.turns_of(pattern)))
}

fn session_ms(out: &Outcome, pattern: Pattern) -> f64 {
    ms(mean(&out.by_pattern[pattern.idx()].latency))
}

fn fingerprint(out: &Outcome) -> String {
    let stats: Vec<_> = out
        .by_pattern
        .iter()
        .map(|s| (&s.turns, &s.latency, s.calls, s.tools))
        .collect();
    format!("{stats:?}|{:?}", out.logged)
}

fn identical(a: &[Outcome], b: &[Outcome]) -> bool {
    a.iter()
        .zip(b)
        .all(|(x, y)| fingerprint(x) == fingerprint(y))
}

fn check(label: &str, ok: bool) {
    println!("  {label:<92} {}", if ok { "identical" } else { "DIFFERS" });
}

fn agentic(cfg: &mut Config) {
    cfg.mix = vec![(Preset::Agentic, 1.0)];
}

fn only(preset: Preset) -> impl Fn(&mut Config) + Sync {
    move |cfg: &mut Config| cfg.mix = vec![(preset, 1.0)]
}

#[allow(clippy::too_many_lines)]
fn gate(env: &Env) {
    section("1. the gate: every new bit off, or in a case where it must change nothing");
    let cfg = base(env, env.seed);
    check(
        "the trace replayed at its arrival instants against the trace submitted in order",
        replay_matches_submit(&cfg, 3_000),
    );
    let reference = runs(env, agentic);
    check(
        "--tool-slots 10000 against unbounded executors",
        identical(
            &reference,
            &runs(env, |c| {
                agentic(c);
                c.tool_slots = Some(10_000);
            }),
        ),
    );
    check(
        "--hints declared on a preset with no declared graph",
        identical(
            &reference,
            &runs(env, |c| {
                agentic(c);
                c.hints = Hints::Declared;
            }),
        ),
    );
    let chat = runs(env, only(Preset::Conversational));
    check(
        "--speculate run on a preset that calls no tools",
        identical(
            &chat,
            &runs(env, |c| {
                only(Preset::Conversational)(c);
                c.speculate = Speculate::Run;
            }),
        ),
    );
    let oneshot = runs(env, only(Preset::OneShot));
    check(
        "--suspend joint on a preset with no idle gap",
        identical(
            &oneshot,
            &runs(env, |c| {
                only(Preset::OneShot)(c);
                c.suspend = Suspend::Joint;
            }),
        ),
    );
    check(
        "--claim-key root under a static claim",
        identical(
            &reference,
            &runs(env, |c| {
                agentic(c);
                c.claim_key = ClaimKey::Root;
            }),
        ),
    );
    check(
        "--roles off on a preset with no fan-out",
        identical(
            &reference,
            &runs(env, |c| {
                agentic(c);
                c.shape.roles = false;
            }),
        ),
    );
    check("a prefill-ahead replay run twice", {
        let rc = ReplayConfig {
            ops: env.ops.min(3_000),
            fanout: 0.1,
            causal: false,
            prefill_ahead: true,
            grade: HintGrade::Declared,
            gate: 0.0,
            decode_held: false,
        };
        let a = replays(env, rc, |_| {});
        let b = replays(env, rc, |_| {});
        a.iter().zip(&b).all(|(x, y)| {
            x.flow_stall_ns == y.flow_stall_ns && x.prefill_blocks == y.prefill_blocks
        })
    });
    let silent = [
        Preset::OneShot,
        Preset::Extraction,
        Preset::Conversational,
        Preset::Batch,
    ];
    let quiet = silent.iter().all(|&preset| {
        runs(env, only(preset))
            .iter()
            .all(|out| out.logged_total() == 0)
    });
    println!(
        "  {:<92} {}",
        "one-shot, extraction, conversational and batch write nothing to the logged tier",
        if quiet { "yes" } else { "NO" }
    );
}

fn causal_header(label: &str) {
    println!(
        "  {label:<42} {:>24} {:>30} {:>16}",
        "arrives before upstream ends", "by a median (ms)", "lead (ms)"
    );
}

fn causal_rows(rs: &[ReplayOutcome]) {
    for kind in polyphonic::programs::FlowKind::ALL {
        let flows: Vec<_> = rs
            .iter()
            .map(|r| &r.causality.flows[kind as usize])
            .collect();
        let before: Vec<f64> = flows.iter().map(|f| 100.0 * f.before_share()).collect();
        let early: Vec<f64> = flows.iter().map(|f| ms(f.early_quantile(0.5))).collect();
        let lead: Vec<f64> = flows
            .iter()
            .map(|f| ms(quantile(&f.lead_ns, 0.5)))
            .collect();
        println!(
            "  {:<42} {:>24} {:>30} {:>16}",
            kind.label(),
            plain_seeds(&before, 1, "%"),
            plain_seeds(&early, 1, ""),
            plain_seeds(&lead, 1, "")
        );
    }
}

fn causal(env: &Env) {
    section("2. causality: does a downstream wait for its upstream (P1)");
    let rc = |causal| ReplayConfig {
        ops: env.ops,
        fanout: 0.1,
        causal,
        prefill_ahead: false,
        grade: HintGrade::Declared,
        gate: 0.0,
        decode_held: false,
    };
    println!("\n  the published trace, submitted in its order");
    causal_header("flow");
    let open = replays(env, rc(false), |_| {});
    causal_rows(&open);
    let overlap: Vec<f64> = open
        .iter()
        .map(|r| 100.0 * r.causality.turn_overlaps as f64 / r.causality.turn_pairs.max(1) as f64)
        .collect();
    println!(
        "  consecutive turns of a session that overlap: {}",
        plain_seeds(&overlap, 1, "%")
    );
    println!("\n  the same trace, each downstream released when its upstream finishes");
    causal_header("flow");
    let closed = replays(env, rc(true), |_| {});
    causal_rows(&closed);
    let service = |rs: &[ReplayOutcome]| -> Vec<f64> {
        rs.iter()
            .map(|r| ms(r.service_ns as f64 / r.served.max(1) as f64))
            .collect()
    };
    println!(
        "  mean service per request: published order {} ms, causal {} ms",
        plain_seeds(&service(&open), 1, ""),
        plain_seeds(&service(&closed), 1, "")
    );
    println!(
        "\n  agent programs at {} a second, compress {}: closed loop against the same scripts open loop",
        env.rate, env.compress
    );
    println!(
        "  {:<26} {:>30} {:>30} {:>30}",
        "", "turn, mean (ms)", "turn, p50 (ms)", "session, mean (s)"
    );
    let loop_row = |label: &str, mode: LoopMode| {
        let outs = runs(env, |c| {
            agentic(c);
            c.loop_mode = mode;
        });
        let turn: Vec<f64> = outs.iter().map(|o| turn_ms(o, Pattern::Agentic)).collect();
        let p50: Vec<f64> = outs
            .iter()
            .map(|o| ms(quantile(o.turns_of(Pattern::Agentic), 0.5)))
            .collect();
        let session: Vec<f64> = outs
            .iter()
            .map(|o| session_ms(o, Pattern::Agentic) / 1e3)
            .collect();
        println!(
            "  {label:<26} {:>30} {:>30} {:>30}",
            plain_seeds(&turn, 0, ""),
            plain_seeds(&p50, 0, ""),
            plain_seeds(&session, 1, "")
        );
        turn
    };
    let c = loop_row("closed loop", LoopMode::Closed);
    let o = loop_row(
        "open loop, 28 ms lead",
        LoopMode::Open {
            lead_ns: 28_000_000,
        },
    );
    let ratio: Vec<f64> = c.iter().zip(&o).map(|(c, o)| c / o).collect();
    println!(
        "  closed over open, turn mean: {}",
        plain_seeds(&ratio, 2, "x")
    );
}

fn hints(env: &Env) {
    section("3. hints by grade (P2, P3)");
    for (label, partition, held) in [
        ("published defaults", 1024u64, false),
        ("half partition, decode output held", 512, true),
    ] {
        for causal in [false, true] {
            println!(
                "\n  prefill-ahead on the base trace, {label}, {}; flow downstream's stall against prefill-ahead off",
                if causal {
                    "each downstream released when its upstream finishes"
                } else {
                    "published order"
                }
            );
            println!(
                "  {:<34} {:>34} {:>30} {:>26} {:>10}",
                "hint",
                "flow stall vs off",
                "total stall vs off",
                "prefill work / saved (s)",
                "template"
            );
            let rc = |ahead, grade, gate| ReplayConfig {
                ops: env.ops,
                fanout: 0.1,
                causal,
                prefill_ahead: ahead,
                grade,
                gate,
                decode_held: held,
            };
            let off = replays(env, rc(false, HintGrade::Declared, 0.0), |c| {
                c.kv_partition_mib = partition;
            });
            let arms = [
                ("the declared downstream", HintGrade::Declared, 0.0),
                ("the declared template only", HintGrade::Template, 0.0),
                ("a template learned per function", HintGrade::Learned, 0.0),
                ("learned, gated at P(flow) 0.5", HintGrade::Learned, 0.5),
            ];
            for (name, grade, gate) in arms {
                let on = replays(env, rc(true, grade, gate), |c| {
                    c.kv_partition_mib = partition;
                });
                let flow: Vec<f64> = off
                    .iter()
                    .zip(&on)
                    .map(|(a, b)| change(a.flow_stall_mean_ns(), b.flow_stall_mean_ns()))
                    .collect();
                let total: Vec<f64> = off
                    .iter()
                    .zip(&on)
                    .map(|(a, b)| change(a.stall_ns as f64, b.stall_ns as f64))
                    .collect();
                let work: f64 = on.iter().map(|r| r.prefill_work_ns as f64).sum::<f64>() / 1e9;
                let saved: f64 = off
                    .iter()
                    .zip(&on)
                    .map(|(a, b)| (a.flow_stall_ns as f64 - b.flow_stall_ns as f64) / 1e9)
                    .sum();
                let known: u64 = on.iter().map(|r| r.learn.known).sum();
                let flows: u64 = on.iter().map(|r| r.learn.flows).sum();
                let template = if flows > 0 {
                    format!("{:.0}%", 100.0 * known as f64 / flows as f64)
                } else {
                    "-".to_string()
                };
                println!(
                    "  {name:<34} {:>34} {:>30} {:>26} {template:>10}",
                    seeds_of(&flow, "%"),
                    seeds_of(&total, "%"),
                    format!("{work:.1} for {saved:.1}"),
                );
            }
        }
    }
    programs_hints(env);
}

fn programs_hints(env: &Env) {
    println!(
        "\n  warm-ups before a tool, long-running agents whose idle sandboxes are suspended by timers: turn latency against no hint"
    );
    println!(
        "  {:<34} {:>34} {:>12} {:>10} {:>22}",
        "hint", "turn, mean vs none", "warms", "skipped", "first tool's restore"
    );
    let lr = |c: &mut Config| {
        c.mix = vec![(Preset::LongRunning, 1.0)];
        c.programs = env.programs / 3;
        c.rate = (env.rate / 6.0).max(1.0);
        c.suspend = Suspend::Timers;
        c.ddr_mib = 4096;
    };
    let none = runs(env, lr);
    let arms: Vec<(String, Hints, f64)> = vec![
        ("none".to_string(), Hints::None, 0.3),
        ("declared".to_string(), Hints::Declared, 0.3),
        ("predicted".to_string(), Hints::Predicted, 0.3),
        (
            "stream, name after 80% of decode".to_string(),
            Hints::Stream,
            0.2,
        ),
        (
            "stream, name after 30% of decode".to_string(),
            Hints::Stream,
            0.7,
        ),
        (
            "stream, name after 2% of decode".to_string(),
            Hints::Stream,
            0.98,
        ),
    ];
    for (name, hint, share) in arms {
        let outs = runs(env, |c| {
            lr(c);
            c.hints = hint;
            c.tool_share = share;
        });
        let vs: Vec<f64> = none
            .iter()
            .zip(&outs)
            .map(|(a, b)| {
                change(
                    turn_ms(a, Pattern::LongRunning),
                    turn_ms(b, Pattern::LongRunning),
                )
            })
            .collect();
        let warms: u64 = outs.iter().map(|o| o.hints.warms).sum();
        let skipped: u64 = outs
            .iter()
            .map(|o| o.hints.skipped_hot + o.hints.skipped_price)
            .sum();
        let restore: f64 = outs
            .iter()
            .map(|o| o.hints.first_tool_restore_ns as f64)
            .sum::<f64>()
            / outs.iter().map(|o| o.hints.first_tools).sum::<u64>().max(1) as f64;
        println!(
            "  {name:<34} {:>34} {warms:>12} {skipped:>10} {:>20.2} ms",
            seeds_of(&vs, "%"),
            ms(restore)
        );
    }
    let pipeline = |c: &mut Config| {
        c.mix = vec![(Preset::Pipeline, 1.0)];
        c.ddr_mib = 2048;
        c.programs = env.programs;
    };
    let none = runs(env, pipeline);
    let declared = runs(env, |c| {
        pipeline(c);
        c.hints = Hints::Declared;
    });
    let vs: Vec<f64> = none
        .iter()
        .zip(&declared)
        .map(|(a, b)| change(turn_ms(a, Pattern::Pipeline), turn_ms(b, Pattern::Pipeline)))
        .collect();
    println!(
        "  a declared fixed pipeline against none, DDR 2 GiB a node: turn mean {}",
        seeds_of(&vs, "%")
    );
}

fn speculation(env: &Env) {
    section("4. speculation on read tools (P4)");
    println!(
        "\n  turn latency against no speculation; accuracy is the estimator's measured top-1 for the next tool"
    );
    println!(
        "  {:<34} {:>8} {:>30} {:>10} {:>14} {:>16}",
        "tools, speculation", "top-1", "turn vs none", "hit/try", "saved (s)", "wasted (s)"
    );
    let cases: [(&str, ToolSet, bool, f64); 4] = [
        ("coding", ToolSet::Coding, false, 0.25),
        ("coding, habit 0.6", ToolSet::Coding, false, 0.6),
        (
            "research, open world refused",
            ToolSet::Research,
            false,
            0.25,
        ),
        (
            "research, open world allowed",
            ToolSet::Research,
            true,
            0.25,
        ),
    ];
    for (name, tools, open, det) in cases {
        let shape = |c: &mut Config| {
            agentic(c);
            c.shape.tools = tools;
            c.shape.determinism = det;
            c.open_world = open;
        };
        let none = runs(env, shape);
        for (label, mode) in [
            ("priced", Speculate::Run),
            ("toolspec, unpriced", Speculate::Always),
            ("oracle ceiling", Speculate::Oracle),
        ] {
            let outs = runs(env, |c| {
                shape(c);
                c.speculate = mode;
            });
            let vs: Vec<f64> = none
                .iter()
                .zip(&outs)
                .map(|(a, b)| change(turn_ms(a, Pattern::Agentic), turn_ms(b, Pattern::Agentic)))
                .collect();
            let top1: f64 = outs.iter().map(|o| o.est.top1 as f64).sum::<f64>()
                / outs.iter().map(|o| o.est.predictions).sum::<u64>().max(1) as f64;
            let tries: u64 = outs.iter().map(|o| o.spec.tries).sum();
            let hits: u64 = outs.iter().map(|o| o.spec.hits).sum();
            let saved: f64 = outs.iter().map(|o| o.spec.saved_ns as f64).sum::<f64>() / 1e9;
            let wasted: f64 = outs.iter().map(|o| o.spec.wasted_ns as f64).sum::<f64>() / 1e9;
            println!(
                "  {:<34} {:>7.0}% {:>30} {:>10} {saved:>14.1} {wasted:>16.1}",
                format!("{name}, {label}"),
                100.0 * top1,
                seeds_of(&vs, "%"),
                format!("{hits}/{tries}"),
            );
        }
    }
    println!(
        "\n  executors with capacity, coding tools: other sessions' tool waits against what speculation saves"
    );
    println!(
        "  {:<12} {:<22} {:>28} {:>16} {:>14}",
        "slots/node", "speculation", "turn mean (ms)", "tool wait (s)", "utilisation"
    );
    for slots in [2usize, 3, 4, 6] {
        for (label, mode) in [
            ("off", Speculate::Off),
            ("toolspec, unpriced", Speculate::Always),
            ("priced", Speculate::Run),
        ] {
            let outs = runs(env, |c| {
                agentic(c);
                c.tool_slots = Some(slots);
                c.speculate = mode;
            });
            let turn: Vec<f64> = outs.iter().map(|o| turn_ms(o, Pattern::Agentic)).collect();
            let wait: f64 = outs.iter().map(|o| o.tool_wait_ns as f64).sum::<f64>() / 1e9;
            let busy: f64 = outs
                .iter()
                .map(|o| {
                    (o.tool_exec_ns + o.spec.exec_ns) as f64
                        / (Config::default().nodes as f64 * slots as f64 * o.span_ns.max(1) as f64)
                })
                .sum::<f64>()
                / outs.len() as f64;
            println!(
                "  {slots:<12} {label:<22} {:>28} {wait:>16.1} {:>13.0}%",
                plain_seeds(&turn, 0, ""),
                100.0 * busy
            );
        }
    }
}

fn leases(env: &Env) {
    section("5. leases and drafts (P5)");
    println!(
        "\n  {:<34} {:>10} {:>10} {:>16} {:>12} {:>10} {:>30}",
        "annotations, DDR a node",
        "leased",
        "broken",
        "peak / node DDR",
        "durable lost",
        "abandoned",
        "turn mean (ms)"
    );
    for ddr in [8192u64, 4096] {
        for (name, mode) in [
            ("declared", AnnotationMode::Declared),
            ("pessimistic", AnnotationMode::Pessimistic),
        ] {
            let outs = runs(env, |c| {
                agentic(c);
                c.ddr_mib = ddr;
                c.shape.annotations = mode;
            });
            let taken: u64 = outs.iter().map(|o| o.leases.0).sum();
            let broken: u64 = outs.iter().map(|o| o.leases.1).sum();
            let peak = outs.iter().map(|o| o.leases.2).max().unwrap_or(0);
            let lost: u64 = outs.iter().map(|o| o.durable_lost).sum();
            let abandoned: u64 = outs.iter().map(|o| o.abandoned).sum();
            let turn: Vec<f64> = outs.iter().map(|o| turn_ms(o, Pattern::Agentic)).collect();
            println!(
                "  {:<34} {taken:>10} {broken:>10} {:>15.2}% {lost:>12} {abandoned:>10} {:>30}",
                format!("{name}, {} GiB", ddr / 1024),
                100.0 * peak as f64 / (ddr << 20) as f64,
                plain_seeds(&turn, 0, "")
            );
        }
    }
    println!(
        "\n  reclaiming draft cells (edit tools declared closed-world): turn latency against not reclaiming"
    );
    for ddr in [8192u64, 4096, 2048] {
        let keep = runs(env, |c| {
            agentic(c);
            c.ddr_mib = ddr;
            c.reclaim_drafts = false;
        });
        let reclaim = runs(env, |c| {
            agentic(c);
            c.ddr_mib = ddr;
        });
        let vs: Vec<f64> = keep
            .iter()
            .zip(&reclaim)
            .map(|(a, b)| change(turn_ms(a, Pattern::Agentic), turn_ms(b, Pattern::Agentic)))
            .collect();
        let drafts: u64 = reclaim.iter().map(|o| o.drafts).sum();
        let kept: u64 = keep.iter().map(|o| o.abandoned).sum();
        let reclaimed: u64 = reclaim.iter().map(|o| o.abandoned).sum();
        println!(
            "  DDR {} GiB a node: {} ({drafts} drafts; programs abandoned {kept} keeping, {reclaimed} reclaiming)",
            ddr / 1024,
            seeds_of(&vs, "%")
        );
    }
}

fn gap_table(outs: &[Outcome]) {
    let buckets = [
        (0u64, 100_000_000u64, "under 0.1 s"),
        (100_000_000, 1_000_000_000, "0.1-1 s"),
        (1_000_000_000, 5_000_000_000, "1-5 s"),
        (5_000_000_000, 30_000_000_000, "5-30 s"),
        (30_000_000_000, u64::MAX, "over 30 s"),
    ];
    for (lo, hi, label) in buckets {
        let samples: Vec<_> = outs
            .iter()
            .flat_map(|o| o.gaps.iter())
            .filter(|g| g.gap_ns >= lo && g.gap_ns < hi)
            .collect();
        if samples.is_empty() {
            continue;
        }
        let n = samples.len() as f64;
        let missed = samples.iter().filter(|g| g.excess_ns > 0).count() as f64;
        let fetched = samples.iter().filter(|g| g.fetched).count() as f64;
        let excess: f64 = samples.iter().map(|g| g.excess_ns as f64).sum::<f64>() / n;
        println!(
            "    gap {label:<12} {:>7} calls, {:>5.1}% rebuilt beyond their new blocks, {:>5.1}% fetched, excess {:.2} ms a call",
            samples.len(),
            100.0 * missed / n,
            100.0 * fetched / n,
            ms(excess)
        );
    }
}

fn retention(env: &Env) {
    section("6. retention through a tool call (P6)");
    println!(
        "\n  {:<40} {:>10} {:>30} {:>30} {:>14}",
        "load", "mean batch", "excess rebuild a call (ms)", "turn vs no TTL", "marks/honoured"
    );
    let loads: [(&str, f64, u64, EngineWait); 5] = [
        ("6 a second", 6.0, 1024, EngineWait::Off),
        ("12 a second", 12.0, 1024, EngineWait::Off),
        ("18 a second", 18.0, 1024, EngineWait::Off),
        (
            "12 a second, half the partition",
            12.0,
            512,
            EngineWait::Off,
        ),
        (
            "12 a second, the engine that waits",
            12.0,
            512,
            EngineWait::Fifo,
        ),
    ];
    for (name, rate, partition, wait) in loads {
        let shape = |c: &mut Config| {
            agentic(c);
            c.rate = rate;
            c.kv_partition_mib = partition;
            c.engine_wait = wait;
        };
        let off = runs(env, shape);
        let on = runs(env, |c| {
            shape(c);
            c.ttl = true;
        });
        let excess: Vec<f64> = off
            .iter()
            .map(|o| {
                ms(o.gaps.iter().map(|g| g.excess_ns as f64).sum::<f64>()
                    / o.gaps.len().max(1) as f64)
            })
            .collect();
        let vs: Vec<f64> = off
            .iter()
            .zip(&on)
            .map(|(a, b)| change(turn_ms(a, Pattern::Agentic), turn_ms(b, Pattern::Agentic)))
            .collect();
        let batch: f64 = off.iter().map(|o| o.mean_batch).sum::<f64>() / off.len() as f64;
        let marks: u64 = on.iter().map(|o| o.marks.0).sum();
        let honoured: u64 = on.iter().map(|o| o.marks.1).sum();
        println!(
            "  {name:<40} {batch:>10.1} {:>30} {:>30} {:>14}",
            plain_seeds(&excess, 2, ""),
            seeds_of(&vs, "%"),
            format!("{marks}/{honoured}")
        );
        if name.starts_with("18") || partition == 512 && wait == EngineWait::Off {
            gap_table(&off);
        }
    }
}

fn lifecycle(env: &Env) {
    section("7. one suspend decision for the KV and the sandbox, against two timers (P7)");
    println!(
        "\n  {:<26} {:<10} {:>9} {:>10} {:>12} {:>12} {:>14} {:>16} {:>26}",
        "DDR a node",
        "suspend",
        "idles",
        "abandoned",
        "pair differs",
        "joint differs",
        "idle freed",
        "resume (ms)",
        "turn mean (ms)"
    );
    for ddr in [8192u64, 2048] {
        let shape = |c: &mut Config| {
            c.mix = vec![(Preset::LongRunning, 1.0)];
            c.programs = env.programs / 2;
            c.rate = (env.rate / 4.0).max(1.0);
            c.ddr_mib = ddr;
        };
        let never = runs(env, |c| {
            shape(c);
            c.suspend = Suspend::Never;
        });
        for (name, mode) in [
            ("never", Suspend::Never),
            ("timers", Suspend::Timers),
            ("joint", Suspend::Joint),
        ] {
            let outs = runs(env, |c| {
                shape(c);
                c.suspend = mode;
            });
            let idles: u64 = outs.iter().map(|o| o.life.idles).sum();
            let abandoned: u64 = outs.iter().map(|o| o.abandoned).sum();
            let pair: u64 = outs.iter().map(|o| o.life.pair_disagree).sum();
            let joint: u64 = outs.iter().map(|o| o.life.joint_differs).sum();
            let freed: f64 = outs.iter().map(|o| o.life.freed_ns as f64).sum::<f64>()
                / outs
                    .iter()
                    .map(|o| o.life.idle_ns as f64)
                    .sum::<f64>()
                    .max(1.0);
            let suspended: u64 = outs.iter().map(|o| o.life.suspended_idles).sum();
            let resume =
                outs.iter().map(|o| o.life.resume_ns as f64).sum::<f64>() / suspended.max(1) as f64;
            let turn: Vec<f64> = outs
                .iter()
                .map(|o| turn_ms(o, Pattern::LongRunning))
                .collect();
            let vs: Vec<f64> = never
                .iter()
                .zip(&outs)
                .map(|(a, b)| {
                    change(
                        turn_ms(a, Pattern::LongRunning),
                        turn_ms(b, Pattern::LongRunning),
                    )
                })
                .collect();
            println!(
                "  {:<26} {name:<10} {idles:>9} {abandoned:>10} {:>11.1}% {:>11.1}% {:>13.1}% {:>16.2} {:>26}",
                format!("{} GiB", ddr / 1024),
                100.0 * pair as f64 / idles.max(1) as f64,
                100.0 * joint as f64 / idles.max(1) as f64,
                100.0 * freed,
                ms(resume),
                format!("{} ({})", plain_seeds(&turn, 0, ""), seeds_of(&vs, "%"))
            );
        }
    }
}

fn logged(env: &Env) {
    section("8. the logged tier's writes against the soft tier's decisions (P8)");
    println!(
        "\n  {:<34} {:>9} {:>11} {:>10} {:>9} {:>10} {:>9} {:>10} {:>12}",
        "preset",
        "decisions",
        "intent+out",
        "approval",
        "suspend",
        "resume",
        "task",
        "total",
        "of decisions"
    );
    let row = |label: &str, outs: &[Outcome]| {
        let secs: f64 = outs.iter().map(|o| o.span_ns as f64 / 1e9).sum();
        let decisions = outs.iter().map(|o| o.decisions).sum::<u64>() as f64 / secs;
        let cause = |c: LogCause| outs.iter().map(|o| o.logged_of(c)).sum::<u64>() as f64 / secs;
        let writes = cause(LogCause::Intent)
            + cause(LogCause::Outcome)
            + cause(LogCause::Approval)
            + cause(LogCause::Suspend)
            + cause(LogCause::Resume)
            + cause(LogCause::TaskState);
        println!(
            "  {label:<34} {decisions:>9.1} {:>11.2} {:>10.2} {:>9.2} {:>10.2} {:>9.2} {:>10.2} {:>11.0}%",
            cause(LogCause::Intent) + cause(LogCause::Outcome),
            cause(LogCause::Approval),
            cause(LogCause::Suspend),
            cause(LogCause::Resume),
            cause(LogCause::TaskState),
            writes,
            100.0 * writes / decisions
        );
    };
    for preset in Preset::ALL {
        let outs = runs(env, |c| {
            only(preset)(c);
            c.rate = if preset == Preset::LongRunning {
                2.0
            } else {
                6.0
            };
            c.programs = if preset == Preset::LongRunning {
                200
            } else {
                env.programs / 2
            };
            if preset == Preset::LongRunning {
                c.suspend = Suspend::Joint;
                c.ddr_mib = 2048;
            }
        });
        row(preset.name(), &outs);
    }
    let pessimistic = runs(env, |c| {
        agentic(c);
        c.rate = 6.0;
        c.programs = env.programs / 2;
        c.shape.annotations = AnnotationMode::Pessimistic;
    });
    row("agentic, MCP defaults for edits", &pessimistic);
    println!(
        "\n  the record tier's one writer so far, Phase 6's planner: 0.046 writes a simulated second"
    );
}

fn retrieval(env: &Env) {
    section("9. retrieval: the chunks a call finds already in the partition (P9)");
    println!(
        "\n  {:<34} {:>26} {:>26} {:>22} {:>22}",
        "corpus, k, order",
        "prefix reuse, partition",
        "position-independent",
        "prefix, infinite",
        "position-ind., infinite"
    );
    for corpus in [10_000u64, 100_000] {
        for k in [3usize, 5, 10] {
            for canonical in [true, false] {
                let shape_cfg = ShapeConfig {
                    compress: env.compress,
                    rag_docs: corpus,
                    rag_k: k,
                    rag_canonical: canonical,
                    ..ShapeConfig::default()
                };
                let outs = runs(env, |c| {
                    only(Preset::Rag)(c);
                    c.shape = shape_cfg;
                    c.programs = env.programs * 3;
                    c.rate = env.rate * 2.0;
                });
                let share = |f: fn(&Outcome) -> u64| -> Vec<f64> {
                    outs.iter()
                        .map(|o| 100.0 * f(o) as f64 / o.rag.chunk_blocks.max(1) as f64)
                        .collect()
                };
                let prefix = share(|o| o.rag.prefix_blocks);
                let any = share(|o| o.rag.any_blocks);
                let (ref_prefix, ref_any) = rag_reference(&Shape::new(shape_cfg), 50_000, env.seed);
                println!(
                    "  {:<34} {:>26} {:>26} {:>21.1}% {:>21.1}%",
                    format!(
                        "{corpus}, {k}, {}",
                        if canonical { "canonical" } else { "relevance" }
                    ),
                    plain_seeds(&prefix, 1, "%"),
                    plain_seeds(&any, 1, "%"),
                    100.0 * ref_prefix,
                    100.0 * ref_any
                );
            }
        }
    }
    println!(
        "\n  position-independent reuse recomputes about 15% of the tokens it finds (CacheBlend); its net is 0.85 of the column above"
    );
}

fn roles(env: &Env) {
    section("10. lengths by role: a pooled quantile against a role's own (P10)");
    for partition in [256u64, 160] {
        roles_at(env, partition);
    }
}

fn roles_at(env: &Env, partition: u64) {
    let shape = |c: &mut Config| {
        only(Preset::MultiAgent)(c);
        c.programs = env.programs / 2;
        c.rate = 4.0;
        c.kv_partition_mib = partition;
    };
    println!("\n  KV partition {partition} MiB a node");
    println!(
        "  {:<28} {:<12} {:>9} {:>10} {:>16} {:>12} {:>16}",
        "claim", "role", "agents", "overran", "reserved/agent", "fan-outs", "fan-out (ms)"
    );
    let arms: [(&str, Claim, ClaimKey, bool); 3] = [
        (
            "pooled p90",
            Claim::Quantile {
                q: 0.9,
                pooled: true,
            },
            ClaimKey::Slo,
            false,
        ),
        (
            "per-role p90",
            Claim::Quantile {
                q: 0.9,
                pooled: false,
            },
            ClaimKey::Root,
            false,
        ),
        (
            "per-role p90, score by role",
            Claim::Quantile {
                q: 0.9,
                pooled: false,
            },
            ClaimKey::Root,
            true,
        ),
    ];
    let mut service = Vec::new();
    for (name, claim, key, observables) in arms {
        let outs = runs(env, |c| {
            shape(c);
            c.claim = claim;
            c.claim_key = key;
            c.observables = observables;
        });
        let admitted: u64 = outs.iter().map(|o| o.fanouts.0).sum();
        let refused: u64 = outs.iter().map(|o| o.fanouts.1).sum();
        let fan_ms =
            ms(outs.iter().map(|o| o.fanout_service_ns as f64).sum::<f64>()
                / admitted.max(1) as f64);
        let mut reserved_all = 0;
        let mut agents_all = 0;
        for (role, role_name) in ROLE_NAMES.iter().enumerate() {
            let mut n = 0;
            let mut over = 0;
            let mut reserved = 0;
            for o in &outs {
                if let Some(s) = o.claims.get(&role_root(role)) {
                    n += s.n;
                    over += s.overruns;
                    reserved += s.reserved_tokens;
                }
            }
            reserved_all += reserved;
            agents_all += n;
            println!(
                "  {name:<28} {role_name:<12} {n:>9} {:>9.1}% {:>16.0} {:>12} {:>16}",
                100.0 * over as f64 / n.max(1) as f64,
                reserved as f64 / n.max(1) as f64,
                if role == 0 {
                    format!("{admitted}/{}", admitted + refused)
                } else {
                    String::new()
                },
                if role == 0 {
                    format!("{fan_ms:.1}")
                } else {
                    String::new()
                }
            );
        }
        println!(
            "  {name:<28} {:<12} {agents_all:>9} {:>10} {:>16.0}",
            "all roles",
            "",
            reserved_all as f64 / agents_all.max(1) as f64
        );
        service.push(fan_ms);
    }
    println!(
        "  fan-out service, per-role claims against pooled: {:+.1}%; the score reading per-role means against the pooled mean: {:+.1}%",
        change(service[0], service[1]),
        change(service[1], service[2])
    );
}

type Consumer = (&'static str, fn(&mut Config), fn(&Outcome) -> f64);

#[allow(clippy::too_many_lines)]
fn classify(env: &Env) {
    section("11. the workload class from observables (P11)");
    let mix = vec![
        (Preset::OneShot, 0.15),
        (Preset::Extraction, 0.05),
        (Preset::Conversational, 0.15),
        (Preset::Rag, 0.10),
        (Preset::Pipeline, 0.10),
        (Preset::Agentic, 0.25),
        (Preset::LongRunning, 0.05),
        (Preset::MultiAgent, 0.10),
        (Preset::Batch, 0.05),
    ];
    let shape = |c: &mut Config| {
        c.mix.clone_from(&mix);
        c.programs = env.programs;
        c.rate = env.rate / 2.0;
    };
    let outs = runs(env, shape);
    for (title, pick) in [
        (
            "at a request's first call",
            (|c: &polyphonic::programs::ClassStats| c.first)
                as fn(&polyphonic::programs::ClassStats) -> [[u64; Pattern::N]; Pattern::N],
        ),
        (
            "at a request's last call, with its history",
            |c: &polyphonic::programs::ClassStats| c.last,
        ),
        ("over every call", |c: &polyphonic::programs::ClassStats| {
            c.confusion
        }),
    ] {
        let mut total = [[0u64; Pattern::N]; Pattern::N];
        for o in &outs {
            for (t, row) in pick(&o.class).iter().enumerate() {
                for (i, c) in row.iter().enumerate() {
                    total[t][i] += c;
                }
            }
        }
        println!("\n  inferred class by true class, {title}, all seeds");
        for pattern in Pattern::ALL {
            let row = &total[pattern.idx()];
            let n: u64 = row.iter().sum();
            if n == 0 {
                continue;
            }
            let right = row[pattern.idx()];
            let wrong = Pattern::ALL
                .iter()
                .filter(|p| **p != pattern)
                .max_by_key(|p| row[p.idx()])
                .copied()
                .unwrap_or(pattern);
            println!(
                "  {:<22} {n:>7} requests, {:>5.1}% right, most often taken for {} ({:.1}%)",
                pattern.label(),
                100.0 * right as f64 / n as f64,
                wrong.label(),
                100.0 * row[wrong.idx()] as f64 / n as f64
            );
        }
        let (n, correct) = polyphonic::programs::ClassStats::totals_of(&total);
        println!(
            "  overall {:.1}% of {n}",
            100.0 * correct as f64 / n.max(1) as f64
        );
    }
    println!("\n  consumers, with the class from the generator, from observables, and pooled");
    println!(
        "  {:<34} {:<10} {:>28} {:>30}",
        "consumer", "class", "metric", "against the generator's"
    );
    let consumers: [Consumer; 3] = [
        (
            "joint suspension (idle freed)",
            |c| {
                c.suspend = Suspend::Joint;
                c.ddr_mib = 2048;
            },
            |o| 100.0 * o.life.freed_ns as f64 / (o.life.idle_ns as f64).max(1.0),
        ),
        (
            "speculation (turn mean, ms)",
            |c| {
                c.speculate = Speculate::Run;
            },
            |o| ms(mean(o.turns_of(Pattern::Agentic))),
        ),
        (
            "claim by class (overrun %)",
            |c| {
                c.claim = Claim::Quantile {
                    q: 0.9,
                    pooled: false,
                };
                c.claim_key = ClaimKey::Root;
                c.claim_by = ClaimBy::Class;
            },
            |o| {
                let (n, over) = o
                    .claims
                    .values()
                    .fold((0, 0), |a, s| (a.0 + s.n, a.1 + s.overruns));
                100.0 * over as f64 / n.max(1) as f64
            },
        ),
    ];
    for (name, apply, metric) in consumers {
        let mut truth: Vec<f64> = Vec::new();
        for (label, mode) in [
            ("generator", ClassMode::Truth),
            ("inferred", ClassMode::Inferred),
            ("pooled", ClassMode::Pooled),
        ] {
            let outs = runs(env, |c| {
                shape(c);
                apply(c);
                c.class_mode = mode;
            });
            let values: Vec<f64> = outs.iter().map(metric).collect();
            let against = if truth.is_empty() {
                truth.clone_from(&values);
                "-".to_string()
            } else {
                let diff: Vec<f64> = truth
                    .iter()
                    .zip(&values)
                    .map(|(t, v)| change(*t, *v))
                    .collect();
                seeds_of(&diff, "%")
            };
            println!(
                "  {name:<34} {label:<10} {:>28} {against:>30}",
                plain_seeds(&values, 2, "")
            );
        }
    }
}

#[allow(clippy::too_many_lines)]
fn table(env: &Env) {
    section("12. where a unified orchestrator can help at all (P12)");
    println!(
        "\n  coupled %, locality = scored decisions a silo's argmin would have made differently, memory = host DDR evictions across classes"
    );
    println!(
        "  {:<22} {:>26} {:>26} {:>26}",
        "pattern", "locality, 8 GiB DDR", "memory, 8 GiB DDR", "memory, 2 GiB DDR"
    );
    for preset in Preset::ALL {
        let shape = |c: &mut Config, ddr: u64| {
            only(preset)(c);
            c.regret = true;
            c.ddr_mib = ddr;
            c.kv_offload_mib = ddr * 820 / 8192;
            c.programs = if preset == Preset::LongRunning {
                200
            } else {
                env.programs / 2
            };
            c.rate = if preset == Preset::LongRunning {
                2.0
            } else {
                6.0
            };
        };
        let wide = runs(env, |c| shape(c, 8192));
        let tight = runs(env, |c| shape(c, 2048));
        let pct = |outs: &[Outcome], f: fn(&Outcome, usize) -> (u64, u64)| -> String {
            let v: Vec<f64> = outs
                .iter()
                .map(|o| {
                    let (a, b) = f(o, preset.pattern().idx());
                    100.0 * a as f64 / b.max(1) as f64
                })
                .collect();
            let n: u64 = outs.iter().map(|o| f(o, preset.pattern().idx()).1).sum();
            format!("{} of {n}", plain_seeds(&v, 1, "%"))
        };
        println!(
            "  {:<22} {:>26} {:>26} {:>26}",
            preset.pattern().label(),
            pct(&wide, |o, i| o.locality[i]),
            pct(&wide, |o, i| o.memory[i]),
            pct(&tight, |o, i| o.memory[i])
        );
    }
    let mixed = |outs: &[Outcome], f: fn(&Outcome) -> (u64, u64)| -> String {
        let v: Vec<f64> = outs
            .iter()
            .map(|o| {
                let (a, b) = f(o);
                100.0 * a as f64 / b.max(1) as f64
            })
            .collect();
        let n: u64 = outs.iter().map(|o| f(o).1).sum();
        format!("{} of {n}", plain_seeds(&v, 1, "%"))
    };
    let all_mix = |c: &mut Config, ddr: u64| {
        c.mix = vec![
            (Preset::Rag, 0.2),
            (Preset::Agentic, 0.5),
            (Preset::MultiAgent, 0.2),
            (Preset::Pipeline, 0.1),
        ];
        c.regret = true;
        c.ddr_mib = ddr;
        c.kv_offload_mib = ddr * 820 / 8192;
        c.programs = env.programs / 2;
        c.rate = 6.0;
    };
    let wide = runs(env, |c| all_mix(c, 8192));
    let tight = runs(env, |c| all_mix(c, 2048));
    let locality = |o: &Outcome| {
        o.locality
            .iter()
            .fold((0, 0), |a, b| (a.0 + b.0, a.1 + b.1))
    };
    let memory = |o: &Outcome| o.memory.iter().fold((0, 0), |a, b| (a.0 + b.0, a.1 + b.1));
    println!(
        "  {:<22} {:>26} {:>26} {:>26}",
        "all four together",
        mixed(&wide, locality),
        mixed(&wide, memory),
        mixed(&tight, memory)
    );
    println!(
        "\n  the published trace by pattern, the engine allocating, DDR a node: services, function cells and offloaded weights share host DDR here"
    );
    println!(
        "  {:<22} {:>26} {:>26} {:>26}",
        "pattern", "locality, 8 GiB DDR", "memory, 8 GiB DDR", "memory, 2 GiB DDR"
    );
    let rc = ReplayConfig {
        ops: env.ops,
        fanout: 0.1,
        causal: false,
        prefill_ahead: false,
        grade: HintGrade::Declared,
        gate: 0.0,
        decode_held: false,
    };
    let trace_wide = replays(env, rc, |c| c.regret = true);
    let trace_tight = replays(env, rc, |c| {
        c.regret = true;
        c.ddr_mib = 2048;
        c.kv_offload_mib = 205;
    });
    for pattern in [
        Pattern::Plain,
        Pattern::Conversational,
        Pattern::Pipeline,
        Pattern::Agentic,
        Pattern::MultiAgent,
    ] {
        let show = |rs: &[ReplayOutcome], f: fn(&ReplayOutcome, usize) -> (u64, u64)| {
            let v: Vec<f64> = rs
                .iter()
                .map(|r| {
                    let (a, b) = f(r, pattern.idx());
                    100.0 * a as f64 / b.max(1) as f64
                })
                .collect();
            let n: u64 = rs.iter().map(|r| f(r, pattern.idx()).1).sum();
            format!("{} of {n}", plain_seeds(&v, 1, "%"))
        };
        println!(
            "  {:<22} {:>26} {:>26} {:>26}",
            pattern.label(),
            show(&trace_wide, |r, i| r.locality[i]),
            show(&trace_wide, |r, i| r.memory[i]),
            show(&trace_tight, |r, i| r.memory[i])
        );
    }
    println!("\n  the joint decisions this phase adds, each against its stated silo");
    let lr = runs(env, |c| {
        only(Preset::LongRunning)(c);
        c.programs = env.programs / 2;
        c.rate = (env.rate / 4.0).max(1.0);
        c.ddr_mib = 2048;
        c.suspend = Suspend::Joint;
    });
    let idles: u64 = lr.iter().map(|o| o.life.idles).sum();
    let differs: u64 = lr.iter().map(|o| o.life.joint_differs).sum();
    println!(
        "  joint suspension against an engine's 5-minute retention and a sandbox's 15-minute timeout: differs on {:.1}% of {idles} idle gaps",
        100.0 * differs as f64 / idles.max(1) as f64
    );
    let spec = runs(env, |c| {
        agentic(c);
        c.shape.tools = ToolSet::Research;
        c.speculate = Speculate::Always;
        c.open_world = true;
    });
    let tries: u64 = spec.iter().map(|o| o.spec.tries).sum();
    let off: u64 = spec.iter().map(|o| o.spec.off_anchor).sum();
    println!(
        "  a speculative tool placed away from its agent's node, against a framework speculating in its own sandbox: {:.1}% of {tries} runs",
        100.0 * off as f64 / tries.max(1) as f64
    );
    for (label, atomic) in [("all-or-nothing", true), ("per agent", false)] {
        let outs = runs(env, |c| {
            only(Preset::MultiAgent)(c);
            c.programs = env.programs / 2;
            c.rate = 6.0;
            c.kv_partition_mib = 160;
            c.atomic = atomic;
        });
        let admitted: u64 = outs.iter().map(|o| o.fanouts.0).sum();
        let total: u64 = outs.iter().map(|o| o.fanouts.0 + o.fanouts.1).sum();
        println!(
            "  multi-agent admission, {label}: {admitted} of {total} fan-outs completed at a 160 MiB KV partition a node"
        );
    }
}

pub fn run_all(env: &Env) {
    println!(
        "programs (phase-7.md §4.18): agent programs submitted step by step on the belief cluster's shape, no control crossing charged\n\
         {} seeds from {}; {} programs a run at {} a second, compress {}; every figure is one value per seed or a range",
        env.seeds, env.seed, env.programs, env.rate, env.compress
    );
    let wanted: Vec<&str> = env.sections.split(',').map(str::trim).collect();
    let on = |name: &str| wanted.contains(&name);
    if on("gate") {
        gate(env);
    }
    if on("causal") {
        causal(env);
    }
    if on("hints") {
        hints(env);
    }
    if on("speculation") {
        speculation(env);
    }
    if on("leases") {
        leases(env);
    }
    if on("retention") {
        retention(env);
    }
    if on("lifecycle") {
        lifecycle(env);
    }
    if on("logged") {
        logged(env);
    }
    if on("retrieval") {
        retrieval(env);
    }
    if on("roles") {
        roles(env);
    }
    if on("classify") {
        classify(env);
    }
    if on("table") {
        table(env);
    }
}
