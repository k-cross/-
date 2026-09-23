# Phase 2 -- Oracle, regret, coupling

Implementation plan for Phase 2 of [`owned-and-observed.md`](owned-and-observed.md): a routing
oracle, per-request regret decomposed into the things that cause it, a clairvoyant eviction
baseline, the wait regime (§3.5), one span per request (§3.11), and coupled % on both axes §3.4
defines.

**Status: planned.** Nothing below is built. §2's predictions are stated before the run, per §7.

Phase 2 changes no policy and no result. What it changes is **what a result is**: today every
published number is a delta between two arms this repository wrote, so the comparison is only as
honest as the weaker arm. After Phase 2 each arm carries a distance from a reference it did not
author. That is the whole deliverable, and it has to land before Phase 3, because Phase 3 is the
phase that can return a negative verdict and it must not also be the phase that invents the
scoring it is judged by.

The measurement apparatus is *not* free of design decisions, and most of them are traps. §1 settles
ten of them; four changed the design while this was written, and one of those deletes the obvious
form of the headline metric entirely.

---

## 1. What has to be settled before the oracle is written

### 1.1 "Oracle" names three different things, and the obvious one measures nothing

§3.4 asks for `policy_cost - oracle_cost` computed on the policy's own state. It does not say what
`oracle_cost` is, and there are three candidates that differ by what the oracle is allowed to know:

| oracle | knows | measures |
|---|---|---|
| **information** | the *model's* cost, over **true** state instead of the policy's belief | belief error |
| **realized** | the cost the simulator would **actually charge** at each candidate, now | belief error **and** cost-model error |
| **clairvoyant** | the whole future trace | everything, including what a decision does to later decisions |

**The information oracle is the obvious one and it measures nothing here.** `Placement::Scored`
under `Control::Unified` *is* an argmin over the model's cost with exact state. An oracle defined
the same way is the policy, and its regret is identically zero -- not "small", zero, on every
request, for the arm the results are about. A metric whose headline arm cannot move it is not a
metric.

**The clairvoyant oracle is the one worth having and it is not buildable.** Optimal placement over a
trace is an offline assignment problem where every decision changes the residency the next decision
faces; there is no tractable exact answer, and the approximations that exist are heuristics that
would need their own defence.

**Chosen: the realized oracle**, myopic in time, truthful about state. At each decision it computes,
read-only, what the simulator would charge to serve this request at each candidate node, and takes
the minimum. It is strictly stronger than the policy on two axes (it sees truth, and it sees the
charge rather than the estimate of the charge) and strictly weaker on one (it cannot see the next
request).

**What that costs, stated now rather than discovered later.** A myopic oracle cannot see a policy
whose value is the residency it creates -- which is not a hypothetical, it is a result this
repository has already published. [`residency-ledger.md`](residency-ledger.md)'s *falsification test
that fails* found that `flow only` beats the score at a 512 MiB handoff **while being myopically
wrong on every individual request**, because unconditional co-placement makes state converge. A
realized-cost oracle will agree with the score there and score `flow only` as the worse policy. So:

> **Myopic regret is a lower bound on what a better policy could win, and the gap between it and the
> truth is exactly the class of effect the published falsification found.**

That is a limitation to publish beside the number, not a reason to skip the number -- and P6 turns
it into an experiment rather than a caveat.

The clairvoyant cell is not left empty, it is moved: §1.7's eviction baseline **is** clairvoyant, on
the axis where the future reference stream is knowable. So the pair covers *truthful-but-myopic
routing* and *clairvoyant-but-single-node eviction*, and the missing cell -- clairvoyant routing --
is named rather than approximated.

### 1.2 Regret decomposes into four gaps, and the decomposition is the deliverable

A single regret number says an arm lost without saying why, which is the same failure as a blended
coupling figure (§9). Four quantities are computable per decision, three of them by the same
machinery:

```
charged(p)  what the simulator actually billed for serving at the policy's node p
R(p)        realized cost at p, planned against truth
R(m_b)      realized cost at the model's argmin over the policy's belief
R(m_t)      realized cost at the model's argmin over truth
R(o)        realized cost at the realized argmin -- the oracle
```

They telescope:

| gap | definition | what it is | zero today when |
|---|---|---|---|
| **execution** | `charged(p) - R(p)` | a plan made on a stale view, executed against truth: `stale_fetches` in nanoseconds | `Unified`, `Query` |
| **heuristic** | `R(p) - R(m_b)` | the policy is not an argmin (hash, residency-greedy, flow, and the affinity tie-break) | never -- see P1 |
| **belief** | `R(m_b) - R(m_t)` | the argmin was taken over a stale view | `Unified`, `Query` |
| **model** | `R(m_t) - R(o)` | the score's cost function is not the charge | never |

Sum: `charged(p) - R(o)`, total regret, by construction rather than by fitting.

Two properties make this worth the machinery. **The components can individually be negative** -- a
heuristic can luck into a node the model-argmin ranked second -- and a decomposition whose parts are
signed is more informative than one that is clipped, so they are not clipped, and P2 says so in
advance. And **two of the four are identically zero today and become Phase 4's entire subject**:
lossy telemetry is what makes the execution and belief gaps nonzero under every control mode, so
Phase 2 builds the instrument with two slots that are provably empty now, and Phase 4 fills them
without touching the instrument.

### 1.3 Realized cost has to be computable without mutating anything

The oracle runs inside the decision loop, on every candidate. If it mutates, it is no longer an
observer; if it clones, it is unaffordable. `Hierarchy` and `TierPool` derive `Debug` and not
`Clone` today, and **Phase 2 must not make them `Clone`** -- the derive would be the first step
toward a counterfactual-execution oracle whose cost is a ledger copy per candidate per request, and
the enum below shows one is not needed.

Every term of a served request's charge is already computable read-only:

| term | read-only source | exact? |
|---|---|---|
| acquire (transfer + recompute) | `Machine::plan`, with residency read from truth | **exact**, and §1.3's whole reason the oracle is affordable |
| queue + exec | `Engine::projected_ns` | **exact below saturation**; overstates by at most one step's width at and above it |
| handoff | `Topology::fetch_ns` over the recorded upstream sources | exact |
| origin round trip | `Machine::reach`'s arithmetic | exact |
| decide, dispatch | `hook_ns`, `dispatch.ns` | exact, and equal across candidates, so they set the level and never the argmin |
| **displacement** | -- | **not a realized cost at all** |

**Displacement is excluded, and the exclusion is the interesting half.** `Terms::displaced` never
enters `Cost`; it is a claim about what the *next* request will pay for bytes this one evicts. A
realized-cost oracle therefore cannot price it, so the oracle is systematically friendly to arms
that ignore eviction debt. Since both quantities are computed anyway, the oracle is reported
**twice** -- with and without a displacement term -- and the difference is the published sensitivity
of the whole metric to the one term that prices the future. Reporting one of them silently would be
choosing the answer.

`plan` gains a `View { Belief, Truth }` parameter rather than a second copy: `Belief` reproduces
today's body exactly, `Truth` substitutes `is_hot`/`holds` for `believes_resident`/`believes_held`.
That is the second legitimate consumer of `phase-1.md` §1.4's ground-truth accessors, and it is why
Phase 1 kept them separable instead of folding them into `Telemetry`.

### 1.4 Regret is denominated in service nanoseconds, not stall

`Cost::total_ns()` excludes execution; `service_ns()` includes it. §6's transferable warning, learned
here the hard way, is that once decode cost depends on batch occupancy **placement moves execution
and not just waiting**, and the published headline already moved from stall to service time for
exactly that reason. A regret denominated in stall would reintroduce the mistake inside the metric
built to retire the caveats.

Consequence worth naming: the engine term dominates service time, so regret will be a small
difference of large numbers on decode-bearing classes. Report it per class as well as in aggregate,
since a mean over a mix where decode is ~385 ms will round every routing effect to nothing.

### 1.5 Refusal has no price, so it gets a count and never a number

A refused request costs the simulator zero and its requester everything. Two bad options: compute
regret only over served requests, which rewards an arm for refusing the expensive work -- §6's named
trap, and the reason `prefer()` compares goodput before stall -- or invent a refusal penalty, which
is the first dimensionless constant in a document whose §3.9 rule is that there are none.

Chosen: **feasibility regret is a count, not a duration.** Decisions where the policy's node refused
and at least one candidate would have served are counted and reported beside regret, never folded
into it. Regret itself is computed over served requests and is quoted only alongside goodput, the
same pairing the ledger already requires.

### 1.6 A gang's oracle is a joint assignment, and the independent one is an upper bound on its regret

`place_agent` runs a separate argmin per agent against staged reservations, so the fan-out's optimum
is a joint assignment over agents and nodes, not a product of per-agent optima. An oracle that
minimises each agent independently ignores the contention siblings create, so its cost is **at most**
the joint optimum's -- which makes gang regret an **upper** bound rather than a measurement.

Chosen: compute it anyway, report it on its own row, never blended into the single-request figure,
and label it a bound. Blending would let a bound contaminate a measurement, and the fan-out rate is
a knob, so the contamination would be tunable.

### 1.7 The clairvoyant baseline is not optimal, and calling it optimal would be the third wrong label

Belady's MIN is optimal for **uniform size and uniform cost**. This ledger has neither: a `Snapshot`
is 32 MiB restoring in ~9 ms, a `KvBlock` is 512 KiB recomputing in 400 us, a `WeightShard` is
512 MiB reloading in seconds. Offline caching with variable sizes is NP-hard, so a furthest-next-use
rule here is a **clairvoyant heuristic**, not a bound.

§7's third lesson is that a wrong label is worse than a wrong number, because a label exempts a
figure from the test it needed. So:

- It is named `clairvoyant`, never `optimal`, `MIN` or `belady`, wherever it is printed.
- **Regret against it may be negative**, and P4 predicts the sign in advance. A negative number here
  is a finding about cost-weighting, not a bug.
- The rule implemented is pure furthest-next-use over the recorded reference stream. A cost-weighted
  variant is a second experiment and not this one: two clairvoyant rules would need their own
  comparison, and the point of the baseline is to separate *ledger* quality from *placement*
  quality, not to search policy space.

**It is a single-node result.** The future reference stream at a node in a cluster is a function of
the routing policy, so a per-node clairvoyant baseline is clairvoyant about a trace that the arm
under test produced -- a different object per arm, and not a common reference. On one ledger the
stream is the trace, policy-independent, and the separation §9 asks for is clean. The distributed
two-pass replay is named in §7 as deferred, with that as the reason.

### 1.8 Coupled % needs the silo's information set written down, or it is a term ablation in new clothes

§3.4 defines coupled % as the fraction of decisions that change when evaluated globally rather than
in silos. `machine.rs` already reports `moved_by_displacement`, `moved_by_flow`, `moved_by_load` and
`moved_by_congestion`, which are **nested ablations of one score's own terms** in a fixed order.
They are not the same object, and the difference decides whether Phase 2 publishes a new number or a
renamed one:

- A term ablation asks *what would this score do without this term*, with the order of removal
  deciding the attribution.
- Coupled % asks *what would a different system, holding less state, decide* -- and a silo does not
  merely lose a term, it loses a **view**.

So each axis needs its silo's information set spelled out, and each must be a *decision* a silo
actually makes:

**Memory coupling (host DDR).** The decision is an admission into the DDR pool and the eviction it
causes. The siloed counterfactual is already implemented: `Quota::hard` restricts `pick_class` to the
admitting class's own bytes, which is precisely "separate orchestrators owning separate budgets"
(`node_memory`'s own doc comment says so). Coupled when the unified arbiter's **victim class**
differs from the siloed one, or when one admits and the other refuses. No new policy, no tuning, and
the outcome space is finite.

**Locality coupling (topology).** The decision is a placement. The siloed information set is the
score minus every cross-workload input: no handoff term (an inference router does not know where the
caller's tool ran, and a FaaS control plane does not know where the inference context lives) and
displacement restricted to the deciding class's own pool. Coupled when the argmin differs.

Two properties have to be stated with the numbers or they will be over-read.

**It is only meaningful in a binding regime.** The ledger's own regime-selection rules say a
configuration where nothing binds makes every arm identical; coupled % in that regime is a zero
meaning "no pressure", not "no coupling". It is published against the same `rho`, `lambda`, `P/C`
triple as everything else, and at the `distributed` defaults -- where the published table already
reports no memory pressure at all -- it is expected to be near zero and must be read that way.

**It bounds the advantage from above only per-decision, on one trajectory.** Where the two decisions
agree, the unified system provably cannot do better *on that decision*; it can still do better
across a trace, because agreeing decisions made from different histories reach different states.
§3.4's "bounds the unified advantage from above" is true in the per-decision reading and false in
the trajectory reading, and only the per-decision one is measured.

### 1.9 "Regret" is already a word in this codebase

`TierPool::regret_rate` is the fraction of evictions later wanted back -- an *eviction* regret,
learned from the ghost list, and it is already load-bearing inside `marginal_price`. Phase 2's
regret is a *routing* quantity in nanoseconds. Two meanings for one word is how §6's tier collision
happened, and that one at least had two documents to separate it; this one would have two fields on
adjacent lines.

Chosen: the new quantity is `routing_regret` everywhere -- field, printed label, and doc comment --
and `regret_rate` keeps its name with a doc comment pointing at the distinction. Renaming the
existing field instead would touch `marginal_price`, which is on the hot path of every published
memory result, for a naming preference.

### 1.10 The regime split needs a rule, and the obvious rule double-counts

§3.5 wants `sched_lm`'s wait / transfer / recompute split. `machine.rs` counts `fetches`,
`rebuilds` and `stale_fetches`, which are **per materialisation**, not per request: a request whose
chain needs three blobs can fetch two and rebuild one and is currently counted in both columns.
Adding a `waits` counter alongside them would produce a fourth column of a different denominator and
a table that does not sum.

Chosen: keep the materialisation counters exactly as they are -- they answer "how did the ledger
acquire state", which is a different question -- and add a **per-request regime** as a separate,
exhaustive classification: `resident` when nothing was acquired and nothing queued, otherwise the
argmax of `(queue_ns, transfer_ns, recompute_ns)`. Four exclusive buckets over served requests,
summing to 100%, reported on their own line and never mixed with the acquisition split.

---

## 2. Predictions, stated first

§7's rule. Six predictions, each attached to a claim it would rewrite.

**P1 -- the scored arm's heuristic gap is small but not zero, and every nanosecond of it is the
affinity tie-break.** `best_scored` does not return its own argmin: `full = if cost(top) <
cost(affinity) { top } else { affinity }` prefers content affinity on ties, and `held_by_affinity`
already counts how often. So the heuristic gap for `Scored` under `Unified` should be nonzero,
confined to exactly the decisions `held_by_affinity` counts, and near zero in nanoseconds because it
fires where the costs are equal.

- *If right:* the decomposition is validated against a mechanism that is already counted, and the
  scored arm's regret is then **all model gap** -- the distance between the score and the charge,
  which is the quantity Phase 3 will move and the one worth publishing.
- *If wrong* on the count (heuristic-gap decisions ≠ `held_by_affinity` decisions): the
  decomposition or the oracle disagrees with the policy about what the policy did, and that is an
  instrument bug to fix before anything is published.
- *If wrong* on the size (the tie-break is worth real nanoseconds): the fallback is not firing on
  ties at all, it is overriding genuine wins, and a line written to stop cold requests herding is
  quietly deciding placements. That would be a finding about the score, found by the instrument, and
  it goes in the ledger's *Standing* table rather than being repaired here.

**P2 -- regret preserves the arms' ranking but compresses their gaps, and `hash only` is the
exception that carries the information.** The score is an argmin over a cost model, so it should
show the least regret; residency-greedy picks saturated nodes and should show the most. But `hash
only` performs well end to end while consulting nothing, so its *per-decision* regret should be
large relative to its end-to-end deficit.

- *If right:* that signature -- large regret, small deficit -- is the fingerprint of a policy whose
  value is in the trajectory rather than the decision, and it is the same shape as the published
  handoff falsification. It would mean myopic regret systematically under-credits spreading
  policies, and every regret figure has to be quoted with that direction attached.
- *If wrong* (regret ranks the arms exactly as service time does, gaps and all): regret is a
  restatement of the existing table rather than a new instrument. Say so plainly -- it still retires
  the fairness caveat, because a restatement against a reference is what the caveat was about, but
  the phase's contribution is smaller than §9 implies.

**P3 -- the oracle's cost at the node the policy chose equals the cost the simulator charged, to the
nanosecond, on every single non-gang request that was served and did not saturate.** There is no
mechanism for it to be otherwise: both walk the same plan against the same truth, and displacement
is not billed to any request. This is the phase's `sidecar_tax_matches_closed_form` -- an exact
arithmetic identity that fails loudly if the oracle drifts from the simulator.

- *If right:* `R` is the charge function and not a second model of it, which is what makes the four
  gaps a decomposition rather than an estimate.
- *If wrong:* find the term before publishing anything. A mismatch means the oracle is pricing a
  different system than the one being measured, and every number in the phase is then a comparison
  between two cost models, which is the thing Phase 2 exists to stop.

**P4 -- the clairvoyant ledger beats GDSF on hit rate and may lose on cost.** Furthest-next-use is
optimal for what it optimises, which is not what the ledger optimises: GDSF prices recovery in
ns/byte discounted by measured regret, so it will keep a `WeightShard` a distance rule evicts. The
`hit` column should favour clairvoyance; the stall column may not.

- *If it loses on cost:* the baseline has done its job as a diagnostic rather than a bound -- it
  isolates that the ledger's value is in **cost weighting**, not in recency prediction, which is a
  direct statement about what Phase 3 gives up when a class-blind LRU replaces it. Publish it as a
  signed difference, not as regret, per §1.7.
- *If it wins on cost by a wide margin* (say more than the 32% soft-floor-over-hard-partition gap):
  eviction quality is a bigger lever than budget policy, the headline memory result has been
  measuring the smaller of two effects, and Phase 3's LRU is cheaper than feared for the wrong
  reason -- because the thing it replaces was not very good either.

**P5 -- memory coupling is double digits where memory binds and near zero at the published
defaults; locality coupling is single digits everywhere.** The soft-floor result is 32% on the
tightened configuration, so classes there genuinely contend; the `distributed` defaults report no
pressure at all. Locality coupling should sit near the `moved_by_flow` + `moved_by_displacement`
scale already published at rack (3.1% and 2.2%), because that is roughly the information the silo is
missing.

- *If right:* §3.4's claim that coupled % answers "does unified beat siloed" survives with a sharp
  qualifier -- it answers it **per regime**, and a single published figure would be a statement
  about a capacity choice as much as about an architecture.
- *If locality coupling is much higher than the term ablations:* the silo loses more from missing a
  *view* than from missing a *term*, which is §1.8's distinction turning out to be load-bearing, and
  it is the first number that separates the two.
- *If both are near zero in every regime:* the unified thesis is narrower than §5 claims, and
  `owned-and-observed.md` should say so in §5 rather than waiting for Phase 7's per-pattern table.

**P6 -- the oracle cannot see the falsification that already failed, and running it there proves
it.** Re-run the flow-payload sweep (512x, region distance) with the instrument attached. Prediction:
`flow only` shows **higher** regret than `scored` at 512 MiB while showing **lower** service time,
and co-placement stays flat near 48% as it did before.

- *If right:* the phase publishes its own headline metric's blind spot, measured, in the one place
  the repository already knows it exists. That is worth more than the metric, because it says
  precisely which future conclusions a low regret number may not support -- and it pre-registers the
  limit before Phase 3 quotes regret in anger.
- *If wrong* (regret ranks `flow only` correctly at 512 MiB): the realized-cost oracle is picking up
  the convergence effect through the acquire term, the myopia is milder than §1.1 argues, and the
  falsification deserves re-examination under the current engine model rather than its retracted
  one.

---

## 3. What the apparatus must and must not do

Five rules, so the phase does not become Phase 3 while nobody is looking.

1. **It measures; it never repairs.** If regret shows the handoff term is decorative -- which is
   already published -- Phase 2 reports it. Changing the score to respond is a policy change, it
   would land without the very instrument that justified it having been validated, and it belongs to
   whichever later phase owns that term.
2. **The oracle never becomes the policy.** It reads ground truth and realized charges, so routing
   by it is a cheat that no deployment can run. The type keeps them apart: the oracle's output is a
   number attached to a decision already made, and it is never returned from a placement function.
3. **Truth is read through the named accessors, not through a widened `Telemetry`.** `Telemetry` is
   the scheduler's view and Phase 4 is what widens it. The oracle is not the scheduler and must not
   borrow its boundary; it uses `plan(View::Truth)`, `ground_truth_holds` and `truly_resident`.
4. **No existing number moves.** Every new output is behind a flag, default off, and an unflagged
   run stays byte-identical to `HEAD` on the reproducible set -- the same check Phase 1 ran, with
   the same incremental discipline and the same three commands excluded for the same reason
   (`phase-1.md` §5).
5. **Nanoseconds or counts, nothing else.** §3.9's rule applies to the apparatus too. There is no
   refusal penalty (§1.5), no risk multiplier, no weighting between the four gaps. The one
   non-nanosecond quantity is a percentage of decisions, which is a count over a count.

---

## 4. Work items

Ordered so each lands compiling and independently checkable, and so the identity in P3 is provable
before anything is built on top of it.

### 4.1 `View` on `plan`

```rust
enum View { Belief, Truth }
fn plan(&self, d: usize, req: &Request, view: View) -> Plan
```

`Belief` is today's body verbatim; `Truth` substitutes `is_hot`/`holds` at the two predicates.
Every existing call site passes `Belief`, so this item alone is a no-op and is checked as one before
the oracle exists to blame.

### 4.2 `oracle.rs` -- the realized cost and the four gaps

```rust
pub struct Regret { total: i64, execution: i64, heuristic: i64, belief: i64, model: i64 }
```

Signed, per §1.2. One read-only function computing `R(d)` from §1.3's table, and one that takes the
four picks and returns the decomposition. Both `&self` on `Machine`; the oracle allocates nothing
per candidate beyond the `Plan` the policy already builds.

Two entry points, because the two are charged differently: single requests via `serve_request`, and
gang agents via `place_agent`, tallied separately per §1.6.

**The identity test is written here, not in §5**, because it is the reason to believe the rest:
`R` at the chosen node must equal `Cost::service_ns()` for a served, non-gang, non-saturated
request. That is P3, and it is the single most sensitive test in the phase.

### 4.3 The regime classifier

Four exclusive buckets per served request (§1.10), tallied in `drive()` where the `Cost` is already
in hand and the warm/cold classification already happens. No change to `fetches`/`rebuilds`.

### 4.4 `span.rs` -- one record per request

§3.11 permits one span per request and forbids one per candidate, and that is not a compromise, it
is the same rate rule that rejected `ext_proc`. The span is also the natural carrier for everything
above rather than a sixth feature:

```rust
pub struct Span {
    op: u64, class: BlobKind, node: usize, oracle_node: usize,
    regret: Regret, regime: Regime, decided_by: Option<usize>, service_ns: u64,
}
```

`decided_by` is the index into `TERM_LABELS` of the last term in the nested ladder whose addition
changed the argmin -- the per-request form of the `moved_by_*` counters, so the aggregates are
reductions over spans and cannot disagree with them. That is deliberate: two ways to compute the
same percentage is how two published numbers drift.

Naming: `span.rs`, not `trace.rs`, because `arms::trace` already names the request vector.

Spans are priced at **zero**. The simulator charges modelled seam costs for control-plane work on
the request path; a span is instrumentation of the simulator, not of the modelled system, and
charging it would put an observability cost into results that are about something else. If a later
phase wants to model the cost of tracing, Phase 0's ladder already has the rung. Say so at the type,
because a reader who finds free instrumentation in a repository that prices syscalls deserves the
reason.

### 4.5 `Policy::Clairvoyant` and the next-use index

A third `Policy` variant whose `score` returns `-(next_use as f64)`, with never-referenced-again as
`-inf` so it is evicted first. The index is built in one pass over the collected trace:
`HashMap<BlobId, VecDeque<u64>>` of op positions, popped as the reference is consumed. On the
15k-op default that is a few hundred thousand entries, built once and shared across arms.

Single-ledger commands only (`residency`, `flows`, `volatility`), per §1.7. It appears as an arm
labelled `clairvoyant`, alongside `soft-floor` and `hard-partition`, and its column is a **signed
difference**, not a regret.

### 4.6 Coupled % on both axes

- **Memory:** at each DDR admission that evicts, compare the unified victim's class against the
  class `pick_class` would choose under `Quota::hard` semantics, and compare admit-versus-refuse.
  **`clean_top` mutates** -- it pops stale heap entries and parks pinned ones -- so the probe cannot
  simply call it twice. Factor the peek from the cleaning, or run the probe immediately after the
  real decision when the heaps are already clean; the first is better and the second is the fallback
  if the borrow shape resists. Getting this wrong does not produce a wrong percentage, it produces a
  *different eviction order*, which would move published memory results -- so this item is the one
  where rule 4's byte-identity check is load-bearing rather than ceremonial.
- **Locality:** a second argmin over §1.8's reduced information set, computed from the `Terms`
  vector `best_scored` already builds. Free in the scored arms; for unscored arms the silo
  comparison is against that arm's own rule applied to the reduced view.

Both are counts of decisions, reported as percentages with their denominators printed.

### 4.7 The flow-payload knob, and the re-run of the falsification

`Workload::with_flow_payload(bytes)`, chainable like `with_tool_profile`, replacing the constant
`FLOW_PAYLOAD_BYTES` at its single use site. Default unchanged, so every existing trace is
byte-identical. A `--flow-payload` argument on `distributed` sweeps it, and P6's re-run is that
sweep with `--regret` on.

This is the one workload change in the phase. It is justified because the sweep it enables is the
only available test of the headline metric's known blind spot, and because the published table it
re-runs is currently marked as measured on a model that no longer exists.

### 4.8 Report and publish

| target | change |
|---|---|
| `distributed`, `code-review`, `placement` | `--regret`: a regret line per arm (total and four gaps, per class and aggregate), the regime line, feasibility-regret count, coupled % on both axes |
| `residency`, `flows`, `volatility` | `--clairvoyant`: the clairvoyant arm, and memory coupled % |
| `owned-and-observed.md` §9 Phase 2 | a **Status** line, as Phases 0, 1 and 8 carry |
| `owned-and-observed.md` §3.4 | a pointer to `oracle.rs` as the definition's executable form, and §1.1's three-oracle table, since §3.4 currently names one oracle and needs three |
| `residency-ledger.md` | a new *Regret* section: the decomposition per arm, the two coupled figures with their regime, the clairvoyant comparison, and the P6 re-run |
| `residency-ledger.md` *Method* | the fairness caveats each result now carries a reference for, or the note that it does not |
| `residency-ledger.md` *Standing* | `scored placement` restated as regret; the falsification row updated with P6's outcome |

No new subcommand. Phase 1 added one because the ownership table had no run to attach to; every
Phase 2 quantity is a property of an arm that already runs, and a `polyphonic regret` command would
have to re-specify the cluster to say anything.

---

## 5. Verification

- **Byte-identity, unflagged.** `residency`, `flows`, `placement` (split and unified), `volatility`
  at reduced `--ops` and a second seed, diffed with zero tolerance against `HEAD` after each of
  §4's items individually -- not once at the end, per `phase-1.md` §2's P4. `distributed`,
  `code-review` and `data-path` get the smoke run instead, for the reason `phase-1.md` §5 records:
  they print live host timings and cannot be byte-identical with themselves.
- **P3's identity** as a test, on a fan-out-free fixture at a rate low enough that no decode
  saturates, asserting exact equality between `R` at the chosen node and the charged `service_ns()`.
  The saturation and gang exclusions are stated in the fixture rather than relied on, the way
  `sidecar_tax_matches_closed_form_without_fanout` states its own precondition.
- **The decomposition sums.** `execution + heuristic + belief + model == total`, asserted per
  decision, not per run -- a per-run assertion passes on cancelling errors.
- **The two zero slots are zero.** Under `Unified` and `Query`, execution and belief gaps are
  identically zero across a full run. Under `Gossip` at least one is not. This is the test that
  Phase 4 will delete, which is what makes it worth writing now.
- **Spans reduce to the existing counters.** `decided_by` aggregated over spans equals
  `moved_by_displacement`, `moved_by_flow`, `moved_by_load`, `moved_by_congestion` exactly.
- **The clairvoyant policy is clairvoyant.** On a fixture whose reference stream is known, it evicts
  the entry with the furthest next use, including the never-again case.
- `cargo fmt --check`, `cargo clippy --all-targets --all-features`, `cargo test` clean, and
  `cargo build --features census` still compiles with its warning count unchanged at 12 -- Phase 2
  adds no allocation entry point, and a moved count would mean it did.

---

## 6. Risks

1. **The oracle is quadratic in fleet size and runs per decision.** `plan` already scans peers, so
   evaluating it at every candidate is `O(N^2)` per decision in nodes. At four nodes this is
   affordable; at the fleet sizes `phase-8.md` §4.6 reasons about it is not. Mitigations, in order:
   the flag is off by default; deterministic sampling (every k-th decision, k printed with the
   result) if wall time binds. **Random sampling is forbidden** -- it would put an RNG in the
   measurement path of a repository whose results are seed-reproducible.
2. **Scope creep into Phase 3 and Phase 4.** Two temptations, both one commit away. The oracle reads
   truth, so `P(resident)` looks like a natural next field -- it is Phase 4's first edit and the one
   that ends byte-identity. And a regret number invites tuning the score against it -- which is
   fitting the policy to a myopic reference, and it would bake §1.1's blind spot into the policy
   rather than merely into the metric. Rules 1 and 2 exist to make both refusable by citation.
3. **§4.6's memory probe changes eviction order.** `clean_top`'s mutation is a real hazard and the
   only item in the phase that can move a published number. It is checked by rule 4's byte-identity
   run, and the probe lands last so a failure has one candidate cause.
4. **Regret on decode-bearing classes is a small difference of large numbers.** With ~385 ms of
   decode in the denominator, an aggregate mean will round routing effects to zero and look like a
   null result. Per-class reporting is not a nicety here; it is the only presentation in which the
   number exists.
5. **The clairvoyant baseline may be weaker than the policy** (P4), which reads as a broken oracle
   to anyone skimming. That is why it is never called an oracle, never called optimal, and always
   printed as a signed difference.
6. **Coupled % is a per-decision divergence quoted as an architectural verdict.** §1.8 states both
   readings; the risk is that only the headline travels. Mitigation: it is never printed without its
   regime (`rho`, `lambda`, `P/C`) on the same line, so a zero cannot be quoted without the capacity
   that produced it.
7. **P6 re-runs a test whose published numbers predate the engine model, the Firecracker constants
   and the corrected score.** The old table is not a baseline for the new one, and the comparison
   is between the new run's arms, not against the retracted figures. Stated here because the obvious
   presentation -- old table beside new -- is the wrong one.

---

## 7. Out of scope

- **`P(resident)`, lossy telemetry, sequence gaps, divergence, the ZMQ channel.** Phase 4. The
  execution and belief gaps are the slots they fill; leaving them provably zero is the deliverable,
  not an omission.
- **The engine cache, the partition split, the admission sweep.** Phase 3. Phase 2 exists to make
  Phase 3 measurable and must not pre-empt what it measures.
- **The `RequestView` half of §3.1** -- making `req.tokens` unavailable to the score. That is a
  workload ground-truth leak, it changes the placement score, and it is Phase 4's, per
  `phase-1.md` §7. The oracle reading `req.tokens` is *correct* by construction: an oracle is
  allowed ground truth, which is the one place in the codebase where reading it is not a cheat.
- **Retention directives, `retain_until`, `evict_first`.** Phase 5.
- **Predicted flows, the tool-gap estimator, taxonomy presets, per-pattern coupling.** Phase 7.
  Phase 2 publishes coupled % on two axes for the whole mix; §4.6's per-pattern table needs the
  taxonomy fields that do not exist yet.
- **A clairvoyant *routing* oracle**, and the distributed two-pass clairvoyant ledger replay. §1.1
  and §1.7 give the reasons -- intractable in the first case, and not a common reference across arms
  in the second. Both are named so the missing cell is visible; neither is approximated, because an
  approximate oracle is a heuristic with an authoritative name.
- **Changing any policy in response to what the instrument finds.** Rule 1.
