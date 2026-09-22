# Phase 8 -- The data path as an arm

Implementation plan for Phase 8 of [`owned-and-observed.md`](owned-and-observed.md): turn the
sidecar-versus-integrated question into a `data_path` arm, charge [`phase-0.md`](phase-0.md)'s
measured seam costs per request, and publish the service time at which control-plane overhead
passes 5% and 1% of a request.

**Status: planned.** Phase 0 is implemented and measured, which is the only thing this phase
depends on. Nothing in the memory chain (Phases 1-6) gates it and it gates nothing, so it can run
alongside them.

**Four findings from reading Phase 0's output against §2, before any prediction below is read.**
Each one changes what the arm should charge or what the result can claim, and three of them shrink
or re-aim the headline rather than growing it. They are §1; the predictions that depend on them are
§2.

Phase 8 touches `machine.rs` (one enum, one setter, two charge sites), `main.rs` (arms and the
report), and nothing else. No ledger change, no scheduler change, no new dependency.

---

## 1. What has to be settled before the arm is built

### 1.1 The crossover is arithmetic, and the simulator cannot move it

§2.7 asks for "the crossover -- the service time at which control-plane overhead passes 5% and 1%
of a request -- swept across the denominator", and calls it "a property of the measured ladder
alone". That is exactly right, and it is stronger than it sounds: for a per-request tax `T` and a
service time `S`, the overhead share is `T / (T + S)`, so the threshold `f` is crossed at

```
S* = T * (1/f - 1)
```

`S*(5%) = 19T` and `S*(1%) = 99T`. **There is no simulator in that.** A run that charges `T` per
request and divides by `S` returns `19T` and `99T` to within its own rounding, and reporting it as
a simulated result would be reporting the arithmetic with extra steps.

So the phase needs a clear answer to *what the arm adds*, or it is theatre. Three things it adds,
in descending order of worth:

1. **The decisions-per-request multiplier `d`.** The tax is not paid once per request. `decide()`
   is called from three sites -- `serve_request`, `serve_gang`, `run_tool` -- so a fan-out is one
   decision covering N agents, each tool call is another, and each stage of a flow is a request of
   its own. The realised tax is `T * d`, and `d` is a property of the **workload mix**, which the
   simulator knows and the arithmetic does not. It moves the crossover linearly.
2. **The fleet-size ceiling** (§1.4 below, and P3). Per-candidate hook cost makes scheduler work
   **quadratic in fleet size**, and the fleet at which a single scheduler saturates is computable
   from the measured crossing and a simulated `d`. This is a feasibility statement, not a
   percentage, and it is the strongest form §2.2's claim can take.
3. **Where each class actually sits on the curve**, weighted by its share of the stream rather than
   listed as four illustrative rows.

And one thing it **cannot** add, which the phase must not claim. `serve_request` advances the
arrival clock with `self.arrival_ns += self.interval_ns`, independent of what the request costs, so
arrivals are open-loop. A uniform per-request tax is then a constant offset on every arrival and
changes nothing in steady state **by construction** -- no batch composition shift, no queueing
amplification, no goodput effect. There is no second-order effect to find here, and looking for one
would find a bug rather than a result. Getting one would need a closed-loop arrival model, which is
a different and much larger change (§7).

### 1.2 The ~7 us parse term has no provenance

§2.3 builds the tax from "the callout, the loopback hop at 15 us and its parse, less the ~6 us the
integrated path still pays to reach the engine", and lands on **52 us** with stream reuse and
**79 us** without. Reconstructing it from the published ladder:

| term | ns at 1 KiB | rung |
|---|---|---|
| `ext_proc` callout, stream open | 36052 | `Boundary::ExtProc` |
| `ext_proc` callout, stream per request | 63066 | `Ladder::extra` |
| loopback hop to the sidecar | 15525 | `Boundary::TcpLoopback` |
| less: integrated dispatch over UDS | -5524 | `Boundary::UnixSocket` |
| **and a parse term of** | **~6900** | **no rung** |

The first four are measured. The fifth is inferred: it is the residue that makes 36052 + 15525 -
5524 reach 52 us and 63066 + 15525 - 5524 reach 79 us, and it appears in both, so it is one
constant, not rounding. It is also **9-13% of the tax and therefore 9-13% of the crossover**, which
is the whole deliverable.

§7's rule is that provenance is part of the number, and this number has none. Worse, it is very
likely too large: parsing a header block is conventionally sub-microsecond, so ~6.9 us is plausibly
an order of magnitude high. **Drop it.** The tax becomes

| shape | tax | 5% crossover | 1% crossover |
|---|---|---|---|
| `ext_proc` stream reuse | **46.1 us** | **0.88 ms** | **4.56 ms** |
| `ext_proc` stream per request (Envoy's default) | **73.1 us** | **1.39 ms** | **7.23 ms** |

against §2.3's published 52-79 us, 1.0-1.6 ms and 5.2-7.9 ms. Dropping the term moves the crossover
**down by 7-11%**, entirely from measured rungs. That is a correction, not a concession: it is the
same direction §2.7's risk points, and it arrives before the measurement rather than after.

The alternative -- adding an HTTP/1.1 parse rung to the ladder -- is a Phase 0-shaped item, and
§6's "a real Envoy filter, a real proxy integration" exclusion is the reason it was not built. If
the parse term is ever wanted, it belongs on the ladder with its own row, not in a subtraction.

### 1.3 "Per placement" is ambiguous, and one reading is a straw man

§9 says the sidecar arm pays "an `ext_proc` callout per placement", and §2.7 lists "a per-candidate
hook rather than per-placement" among the missing mechanisms. Those are two different multipliers,
and conflating them overstates the sidecar's cost by the candidate count.

- **llm-d as deployed pays one callout per request.** Envoy calls the Endpoint Picker once; the EPP
  runs its own argmin **in-process** and returns one `x-gateway-destination-endpoint`. It does not
  pay 4 x 36 us to score 4 nodes.
- **§2.2's 142 us per placement is about extensibility**, not about llm-d's bill. It is what a
  *pluggable* scoring term costs when the thing being plugged in is out of process: `best_scored`
  is an argmin, so the hook runs once per candidate.

Charging the sidecar arm `N x 36 us` would model a system nobody deploys and inflate the crossover
by 4x at the default fleet. So the phase builds **three arms**, not two, and keeps the two
multipliers apart:

| arm | hook | dispatch | models |
|---|---|---|---|
| `Integrated` | in-process, 0 ns | UDS, 5.5 us | scheduler and data plane in one address space |
| `Sidecar` | one `ext_proc` callout per placement | loopback, 15.5 us | llm-d as deployed |
| `SidecarPluggable` | one `ext_proc` callout **per candidate** | loopback, 15.5 us | what extending the sidecar's policy costs |

`Sidecar` against `Integrated` is §2.3's tax and produces the crossover. `SidecarPluggable` against
`Sidecar` is §2.2's expressiveness premium and produces the fleet ceiling. They are reported
separately and never summed.

### 1.4 The per-candidate hook makes scheduler work quadratic in fleet size

This falls out of Phase 0's ceiling table and is worth stating as a result rather than leaving
implicit in it. At `N` nodes each offering `lambda` requests per second, with `d` decisions per
request and `c` nanoseconds per crossing, a single unsharded scheduler scoring every candidate does

```
W(N) = N * lambda * d * N * c   nanoseconds of hook work per second
```

which saturates one thread at `N_max = sqrt(1e9 / (lambda * d * c))`. **Quadratic in N**: one factor
because a bigger fleet offers more requests, one because each decision scores more candidates. At
the `distributed` defaults (`--rate 250 --nodes 4`, so `lambda` = 62.5 req/s/node) and a placeholder
`d` = 1.2, using the 64 B column of Phase 0's hook table:

| hook boundary | ns/crossing (64 B) | fleet where one scheduler saturates |
|---|---|---|
| `Native` | 0 | unbounded |
| `Wasm`, warm instance | 13 | ~1010 nodes |
| `Ring` | 70 | ~440 nodes |
| `ExtProc`, open stream | ~35575 (extrapolated) | **~19 nodes** |
| `Grpc` unary | 47649 | **~17 nodes** |

Cross-checked against the ledger's own row: at 32 nodes `ext_proc` gives 878 decisions/s and the
workload offers 32 x 62.5 x 1.2 = 2400, so 32 is past the knee; at 19 the two meet at ~1450. The
sweep should produce this curve rather than this single table, since `lambda` and `d` are both
arguable and `N_max` moves as their square root.

**Two assumptions, named because a real system would attack both.** One scheduler thread, and every
active node scored. Sharding the scheduler divides `W` by the shard count; pruning candidates with
a cheap filter before the expensive hook divides it by the prune ratio. Neither is free, and the
honest form of the claim is that **an out-of-process hook forces sharding or pruning at a fleet
size where an in-process one does not** -- roughly 19 nodes against roughly 1000. That is an
architectural consequence with a number on it, which is what §5's emergent-properties argument has
been missing.

---

## 2. Predictions, stated first

§7's rule: name the conclusions that would flip before the measurement, not after.

**P1 -- the realised overhead share matches `T*d / (T*d + S)` to within a few percent.** The
simulator charges the tax on the critical path and divides by service time; §1.1 says there is no
mechanism for it to do anything else.

- *If right:* the crossover is confirmed as arithmetic, §2.7 should say the number needed no
  simulator, and the arm's reportable contribution is `d`, the per-class distribution, and the
  fleet ceiling.
- *If wrong* (realised share off by more than ~10%): something charges the tax where the arithmetic
  does not model it. Find it before publishing -- it is a bug in the charge sites far more likely
  than a real effect, and publishing an unexplained gap would be publishing a mistake.

**P2 -- `d` is between 1.0 and 1.5 on the `distributed` mix, and materially higher on
`code-review`.** A gang is one `decide()` covering every agent, which pushes `d` down; each tool
call is another `decide()`, which pushes it up. At `--fanout 0.10` the first dominates and `d`
should sit just above 1; at `code-review --tool-fraction 0.70` the second does.

- *If `d` > 1:* the crossover moves **up** by `d` -- the data path binds at *longer* service times
  than §2.3's single-crossing arithmetic implies, because a request pays more than one crossing.
  This is the one direction §2.3 did not consider, and it strengthens §2 rather than weakening it.
- *If `d` ~ 1.0:* the multiplier is a non-result, §2.3's crossover stands as computed, and the
  arm's contribution narrows to the ceiling and the class distribution. Say so plainly.
- *If `d` varies by more than ~2x between the two scenarios:* the crossover is a **workload**
  property, not a fleet one, and §2.3's "answerable from published FaaS duration distributions"
  needs a decisions-per-request distribution too.

**P3 -- the per-candidate arm breaches its own decision-rate ceiling between 16 and 32 nodes, and
the in-process arms do not breach it at any fleet size the prototype can express.** §1.4's
arithmetic, with `d` measured rather than assumed.

- *If right:* §2.2's expressiveness argument gets its sharpest form -- an out-of-process policy
  hook caps an unsharded scheduler at tens of nodes while a sandboxed in-process one caps it at
  hundreds -- from measured constants and one simulated multiplier.
- *If wrong* (the ceiling sits above any plausible fleet): the quadratic is real but does not bind,
  §2.2 rests on per-request latency alone, and the `SidecarPluggable` arm should be reported as a
  null result rather than dropped.

**No prediction is offered for the per-class rows**, and that is deliberate. §2.7 already rules
them out as the result: `FAAS_EXEC_MIN_NS` and `SERVICE_EXEC_NS` are chosen constants, so
predicting "~30% for warm FaaS" would be predicting the constant. They are printed to show where
each class sits on a curve whose shape is measured, labelled **modelled** at the point of printing.

---

## 3. What the arms must and must not do

The ladder's discipline was that every rung asks the same question so the difference is the
boundary. The arm's equivalent: **every arm serves the same trace through the same placement policy
at the same control model, so the difference is the data path and nothing else.**

1. **One `Control` for all three arms: `Control::Unified`.** `Control::Query` already charges a
   crossing per placement for the *residency question*; `DataPath::Sidecar` charges one for the
   *routing callout*. In llm-d the EPP is both -- it holds its own view and makes the decision --
   so running `Sidecar` over `Query` double-charges a single crossing and inflates the tax by up to
   100%. Fix the control model at `Unified` across the data-path sweep and say so in the header.
   The existing `distributed_arms` control sweep stays where it is and answers a different question.
2. **One placement policy: `Scored`.** The data path is orthogonal to placement quality, and
   sweeping both at once produces a table where neither factor can be attributed.
3. **The dispatch hop is charged only on a request that actually runs.** `run_here` already knows
   `cost.pending`; a refused request was never dispatched, and charging it would put the tax on
   traffic that consumed nothing -- the same asymmetry §1 of the design doc flags for admission.
4. **Charge at a named payload, and say the slope is flat.** UDS measures 0.000 ns/byte and TCP
   loopback 0.048, so the dispatch payload barely moves the number. State that rather than leaving
   a reader to wonder whether 1 KiB was chosen to produce an answer.
5. **Report the tax from the ladder, not from the run.** The run's job is `d` and the class
   distribution. `T` comes from `Ladder::get` so a reader can substitute a datacenter-Linux `T`
   without re-running anything.

---

## 4. Work items

### 4.1 `DataPath` in `machine.rs`

An enum beside `Control`, a setter beside `set_control`, and the crossings it resolves to held as
`Cost` values from the ladder rather than as constants:

```rust
pub enum DataPath {
    Integrated,
    Sidecar,
    SidecarPluggable,
}
```

`Machine` gains `data_path: DataPath`, `hook: Crossing` and `dispatch: Crossing`, defaulting to
`Integrated` with zero costs so every existing experiment is byte-identical. One setter takes all
three, following `set_control`'s shape:

```rust
pub fn set_data_path(&mut self, path: DataPath, hook: Crossing, dispatch: Crossing)
```

### 4.2 The hook charge, and the per-candidate multiplier

`decide()` is the single place a placement decision is priced, and all three call sites route
through it. It gains the candidate count and adds the hook term:

```rust
fn decide(&mut self, chain_len: usize, candidates: usize) -> u64
```

with the data-path term added to whatever `Control` already returns:

| `DataPath` | hook charge |
|---|---|
| `Integrated` | 0 |
| `Sidecar` | `hook.ns(HOOK_BYTES)` |
| `SidecarPluggable` | `candidates as u64 * hook.ns(HOOK_BYTES)` |

One ordering fix is needed: `serve_request` calls `decide()` at line 824 but computes `candidates`
at 827. Move the `decide()` call below the candidate selection. It has no semantic effect --
`decide_ns` is not read until line 890 -- and it is required for the multiplier to see a real count
rather than `self.active.len()`. `serve_gang` and `run_tool` both already have their candidate set
in hand at the point they decide.

`HOOK_BYTES` sits beside `QUERY_BYTES_PER_BLOB` and is 64: an argmin's per-candidate payload is
small, and it is the column Phase 0's hook table is denominated in. Note in passing that `ext_proc`
has no 64 B sample -- its floor is a 367 B header map -- so the `Sidecar` arms are charged at a fit
**extrapolated down**, exactly as the published hook table is, and the report must repeat the
caveat rather than let it live only in `residency-ledger.md`.

### 4.3 The dispatch hop

The seam §2.2 calls irreducible: reaching the engine. Charged once per request that runs, in
`run_here` alongside the existing acquisition terms, guarded on `!cost.pending`:

| `DataPath` | dispatch charge | rung |
|---|---|---|
| `Integrated` | `dispatch.ns(DISPATCH_BYTES)` | `Boundary::UnixSocket` |
| `Sidecar`, `SidecarPluggable` | `dispatch.ns(DISPATCH_BYTES)` | `Boundary::TcpLoopback` |

It lands in `Cost::transfer_ns`, not `decide_ns`: `decide_ns` is documented as "what it cost to
*decide*, as distinct from what it cost to do", and a dispatch is doing. That keeps the existing
`deciding` and `of stall` columns meaning what they say, and it means the tax shows up in
`service_ns()` either way, which is the denominator the crossover divides by.

`DISPATCH_BYTES` is 1 KiB, and the report states that both rungs are effectively flat in payload
(0.000 and 0.048 ns/byte) so the choice moves the tax by under 1%.

### 4.4 Counting `d`

One counter, `pub decisions: u64`, incremented in `decide()` -- which is already the single
chokepoint, and already increments `self.ops` there. `d` is `decisions / served`, reported per arm
and per class. The per-class split needs the class index at the decision site; `serve_request` and
`run_tool` both have their `Request` in hand, and `serve_gang` attributes to the gang's own kind.

This also retires a chosen constant hiding in plain sight: `crossover()` currently hardcodes
`const RPCS: f64 = 4.0` with no provenance and no label. Replacing it with the measured `d` is the
§7 correction the function has been waiting for.

### 4.5 The sweep and the report

`crossover()` is rewritten around the closed form rather than around four hypothetical rows. It
takes the ladder, the three arms' realised `d`, and prints:

```
control-plane tax, from the measured ladder (parse term dropped -- see phase-8.md §1.2)

arm                         hook        dispatch      tax    d     tax/req
integrated                  0.00 us      5.52 us     5.52 us  1.xx   x.xx us
sidecar (stream reuse)     36.05 us     15.53 us    51.58 us  1.xx  xx.xx us
sidecar (stream/request)   63.07 us     15.53 us    78.60 us  1.xx  xx.xx us
sidecar, pluggable policy  4 x 35.58    15.53 us   157.83 us  1.xx  xxx.xx us

crossover against the integrated path

arm                          5% of a request    1% of a request
sidecar (stream reuse)              0.88 ms            4.56 ms
sidecar (stream/request)            1.39 ms            7.23 ms
```

Three rules for it:

- **The crossover row is `(T_sidecar - T_integrated) * d * (1/f - 1)`**, computed from the ladder.
  The simulated share is printed **beside** it as a check, not instead of it. P1 is the assertion
  that they agree; printing only one of them would make P1 unfalsifiable.
- **Sweep `S` on a log grid** from 10 us to 1 s and print the share at each decade, so the shape is
  visible and a reader can place a workload the prototype does not model.
- **Per-class rows are printed last and labelled `MODELLED`** at the point of printing, following
  `cluster_header`'s existing habit of naming provenance inline.

A `--tax` override that substitutes a hand-supplied `T` in microseconds makes the datacenter-Linux
question answerable without a Linux host, which is the concrete form of §7's "publish the range
over which it holds".

### 4.6 The fleet ceiling

A second table from §1.4's closed form, taking `lambda` from `--rate / --nodes` and `d` from the
run:

```
fleet size at which one unsharded scheduler saturates
(lambda = 62.5 req/s/node, d = 1.xx measured, every active node scored)

hook boundary        ns/crossing    N_max
native                        0    unbounded
wasm (warm)                  13      ~1010
shared ring                  70       ~440
ext_proc (open stream)    35575        ~19
gRPC unary                47649        ~17
```

with the two assumptions printed under it, because a reader who does not see "unsharded, every
candidate scored" will read a ceiling where there is a design choice.

### 4.7 CLI and publish

One new subcommand rather than more flags on `Distributed`, whose argument list is already at
fourteen:

```
polyphonic data-path --nodes 4 --rate 250 --fanout 0.10 --repeat 5 [--tax <us>]
```

sharing `drive`, `node_memory`, `class_table` and `crossing_of` with the existing commands. It runs
the three arms at `Placement::Scored` and `Control::Unified`, prints the class table, the tax table,
the crossover table and the fleet ceiling.

| target | change |
|---|---|
| `residency-ledger.md` | a *Data path* section: the tax table, the crossover, the fleet ceiling, `d` per scenario |
| `residency-ledger.md`, *Standing* | a row for the data-path claim with its verdict |
| §2.3 | the 52-79 us tax becomes 46-73 us, with §1.2's reason; the crossover becomes 0.88-1.39 / 4.6-7.2 ms |
| §2.2 | the dispatch row's "UDS today, 6.0-6.6 us" against the published ladder's 5.2-5.7 us (§5) |
| §2.7 | replace the prediction with the result, and say whether the crossover needed a simulator |
| §5 | the fleet ceiling, if P3 holds -- it is an emergent-property claim with a number on it |
| §9, Phase 8 | status, and a pointer to this file |

---

## 5. Verification

- `cargo fmt --check` and `cargo clippy --all-targets --all-features` clean. `pedantic` at `warn`,
  `unsafe_op_in_unsafe_fn` / `unused_must_use` at `deny`.
- **Every pre-existing experiment is byte-identical.** `DataPath::Integrated` with zero costs is the
  default, and the only behavioural change outside the new arm is moving one `decide()` call below
  candidate selection, which cannot alter a result. Diff `distributed` and `code-review` output
  before and after; any difference is a bug, not a finding.
- `cargo build` with no features still works. Without `grpc`, `Boundary::ExtProc` is absent and
  `crossing_of` already substitutes TCP loopback under a loud label -- the `Sidecar` arms must
  degrade the same way rather than silently charging a cheaper boundary.
- The simulated overhead share and the closed form agree within 10% (P1), asserted rather than
  eyeballed. A test with a synthetic single-class trace at a known service time is the cheap version.
- `d >= 1.0` for every arm, and identical across the three -- the data path changes what a decision
  costs, never how many are made. If `d` differs between arms, a charge site is miscounting.
- The `ext_proc` 64 B extrapolation caveat appears in the printed report, not only in the ledger.
- Spread: unchanged from Phase 0. The tax is best-of-`repeat` off the ladder, and on this host
  (darwin/arm64) the **ordering is the result**; no crossover here should be quoted to two digits.

---

## 6. Risks

1. **The crossover lands far below a millisecond.** §2.7's named risk and the one that matters: if
   almost nothing real sits below it, the data-path case rests entirely on §2.2's expressiveness
   argument and §2 should say so. §1.2 has already moved the prediction 7-11% in that direction
   before any measurement, which is the honest direction for a correction to arrive from.
2. **`d` turns out to be 1.0 and the arm's distinctive contribution evaporates.** Then P2 is a null
   result and the phase's deliverable is the fleet ceiling plus a confirmation that §2.3's
   arithmetic was right. That is a smaller result, not a failed one, and it should be reported at
   its real size rather than padded with per-class rows.
3. **Double-charging `Control` and `DataPath`.** Mitigated by fixing `Control::Unified` across the
   sweep (§3.1) and by the byte-identical check on the existing experiments.
4. **The ceiling is the most quotable number here and the easiest to over-claim.** It assumes one
   scheduler thread and every candidate scored. Both are printed with it; neither should be dropped
   when the number is quoted elsewhere.
5. **The UDS subtrahend is the ladder's least stable rung.** `residency-ledger.md` flags it as
   non-monotone in payload -- 5733/5524/5233 on the published run, 5983/7441/6649 on an earlier one
   -- and the tax is a difference that includes it. Worth about 1 us of a 46-73 us tax, so ~2% of
   the crossover, but it should be quoted as a band and it is the reason §2.2's "6.0-6.6 us" and
   the published table's 5.2-5.7 us disagree. Reconcile that line while publishing.
6. **Host noise and host shape.** Every seam cost is darwin/arm64. `--tax` exists so the crossover
   can be recomputed for a host this repository has never run on, which is the only honest answer
   to "would this hold in a datacenter".

---

## 7. Out of scope

- **A real proxy.** No `pingora`, no `hyper`/`tower`, no Envoy, no HTTP parser. §2.6 settles the
  library-versus-written argument on structure, and this phase prices a data path rather than
  building one.
- **§2.6 item 1 -- cancellation, backpressure, retry.** Named there as the honest residual and in
  §8 as the largest gap between the design being right and being shipped. They are stream semantics,
  not seam costs, and pricing them needs a mechanism this phase does not add. In particular the
  stalled-stream buffer that §2.6 calls "an occupant of the pool §5 prices" is a real missing ledger
  term, and adding it is a memory-chain change.
- **Closed-loop arrivals.** §1.1's reason: the current model cannot show queueing amplification and
  should not pretend to. Making arrivals depend on completions is a different simulator.
- **P:D pairing and the replica ratio** (§2.5) -- a macro-tier capacity decision, Phase 6.
- **Everything in the memory chain.** No `cache.rs`, no ownership types, no telemetry.
- **A WASM `data_path` arm.** `Boundary::Wasm` appears in the fleet-ceiling table because that table
  is arithmetic over the ladder, but a sandboxed hook is an *extension* mechanism, not a data path,
  and giving it an arm would imply a sandbox sits between the scheduler and the engine. It does not.
