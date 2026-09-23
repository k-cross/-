# Phase 3 -- The engine allocates; the orchestrator sizes the partition

Implementation plan for Phase 3 of [`owned-and-observed.md`](owned-and-observed.md): the HBM pool
splits along §1's macro/micro seam, the engine takes allocation authority over `KvBlock`, the
orchestrator keeps capacity authority over the partition it provisions, and every result §1 marks
contaminated is re-run against the expectation tabulated there.

**Status: planned.** Nothing below is built. §2's predictions are stated before the run, per
`owned-and-observed.md` §7.

This is the phase that can return a negative verdict on the project's central claim, and the three
before it exist to make that verdict trustworthy. Phase 1 named the boundary in a type so the split
is expressible in the two places it cuts *within* a pool. Phase 2 replaced every hand-built baseline
with a distance from a reference this repository did not author. What is left is to move the
authority and read the instruments.

Three things make this phase structurally unlike the four that precede it.

- **There is no byte-identity to check.** Phases 0, 1, 2 and 8 were additive and their discipline
  was that nothing moved. Here everything moves, deliberately. §3's rule 1 replaces it: the
  correction lands behind one bit, both bodies stay live for the whole phase, and every published
  number is an A/B on that bit rather than a diff across a commit.
- **The deliverable is a price, not a capability.** "The engine allocates" is not a feature and
  nobody can use it. What Phase 3 produces is a difference between two arms, per class and at p99.
- **It is allowed to fail.** §8's fifth item is explicit: if the arbitration effects vanish once the
  engine allocates its own memory, what remains is a router with good cost accounting, and the
  honest report is that the ledger's value was an artifact of assumed authority. §2 names in advance
  which measurements would say that, so the answer cannot be graded on a curve after it arrives.

§1 settles twelve decisions. Four of them changed this plan while it was written, and two are
findings about the existing documents rather than about the work ahead: §1.8 names a contaminated
result that `owned-and-observed.md` §1's table does not list, and §1.10 reports that the published
census count is stale.

---

## 1. What has to be settled before the boundary is moved

### 1.1 "The HBM pool splits in two" is a split of the allocator, not of the pool

§9 says the HBM pool "splits in two rather than changing hands". Read as two `TierPool`s it is
wrong in a way that would be discovered late: a second `TierPool` still has a `Quota`, still returns
`Admission::Pending`, still exposes `anticipate` and a per-class `by_kind`, and every one of those is
a thing the orchestrator is giving up. The split is between **allocators**, and only one of the two
is still a `TierPool`.

What makes this worth a subsection is that the engine's side is very nearly a `TierPool` already,
and that near-miss is the trap. `TierPool::new(spec, Policy::Lru, leaf_first = true, Quota::open(..))`
is LRU over a chain-structured keyspace that evicts leaves before parents -- which is what vLLM's
block manager does, refcounting shared prefix blocks and freeing a sequence tail-first. The
differences are exactly four, and each one is load-bearing:

| `TierPool` today | the engine cache |
|---|---|
| `admit` returns `Admitted \| Pending` | never refuses; evicts until it fits, or preempts (§1.2) |
| bands, floors, limits, `pick_class` | none -- one class, no operator policy inside the partition |
| `expect` / `anticipate`, the ghost list, `regret_rate` | none -- the orchestrator cannot insert or reprice (§1.8) |
| `marginal_price` derived from a GDSF score it chose | a turnover figure it reports (§1.7) |

So the correction is *`TierPool` minus four mechanisms, plus preemption*, which is a smaller change
than §8's "large" suggests and a much more dangerous one to implement by configuration. A pool
constructed with an open quota and an LRU policy would compile at every call site that Phase 1
marked, and the entire point of the phase is that those call sites stop compiling.

**Chosen: a distinct type, `EngineCache`, in `engine.rs`.** `owned-and-observed.md` §8's feasibility
table already puts it there -- "gains the cache alongside the batch model -- same component" -- and
that is right for a second reason it does not give: the batch model and the block cache are bounded
by the same physical thing and Phase 6 resizes both in one act. Its API is the one §8 says model
agnosticism makes checkable, and nothing else: `admit`, `touch`, `contains`, `holds`, capacity, and
the counters. No `Quota` parameter, no `Admission` in any return type, no `anticipate`.

### 1.2 An engine that never refuses needs somewhere for refusal to go

`Admission::Pending` on a `KvBlock` stops being reachable, and three mechanisms are built on it
being reachable: `access` aborting a chain at the first refusal, `Cost::pending` becoming goodput
loss, and `Report::goodput`'s denominator. Deleting the refusal without re-homing it would make
every arm's goodput rise for a reason that is not an improvement.

Two things replace it, and they are different objects that must not be summed:

- **A router refusal.** The orchestrator sized the partition, so a byte- or token-depth check
  against it is authoritative in the sense §1 defends. It happens before dispatch, costs nothing,
  and is a *policy* with a threshold -- which is what the §1.4 sweep sweeps.
- **An engine preemption.** When a sequence's blocks do not fit, vLLM evicts and recomputes rather
  than declining. In this model that means the request still runs, its prefix hit is lost, and it
  pays a rebuild it would not have paid under an admitting ledger. The cost lands in
  `Cost::recompute_ns`, which is already the right bucket.

Consequence to state with every number: **goodput before and after this phase is not the same
quantity.** Refusals that used to be a ledger fact become a router policy plus an engine
externality, and an arm that refuses less is not thereby serving more. `Report` gets three
separately named counters (§4.7) and the aggregate `goodput()` is quoted only beside them.

### 1.3 There is nothing to admit against: decode does not consume partition bytes today

§9's deliverable is an admission sweep -- "refuse on `max_tokens`, on an output-length estimate, or
on neither". All three reserve against KV the decode is about to produce, and the simulator does not
model any. A request's `chain` is its *prompt*; `tokens` is its output length and is charged through
`Engine::step_ns` as time, never as bytes. Sweeping a reservation against a quantity that never
occupies anything measures the reservation's cost and none of its benefit, and the arm admitting
against nothing could not overcommit even in principle -- which is precisely the mechanism §1's
asymmetry argument is about.

**So decode-produced KV has to be modelled, and it is the largest single addition in the phase.**

The workload already contains half of it, disconnected. `Workload::agent_turn` grows a session's
chain by a fixed **4 blocks (2 MiB) per turn**, and `Request::tokens` is drawn uniformly from
`[24, 224)`. These are two unrelated statements about the same decode: the blocks appear
retroactively in the *next* turn's chain, never during the decode that produced them, and their
count does not vary with the output length it represents.

Tying them is the work, and the tie should be calibrated to preserve today's *level* so that what
the change introduces is variance -- which is the only property the sweep needs, and which is the
difference between one correction landing in this measurement and two.

The calibration is arithmetic, not a fit. A sequence's last block is partially filled and still
allocated, so blocks per turn is `tokens.div_ceil(TOKENS_PER_KV_BLOCK)` over `tokens` uniform on
`[24, 223]`:

| tokens per block | mean blocks per turn | against today's fixed 4 |
|---|---|---|
| 8 | 15.88 | **+297%** |
| 16 | 8.18 | **+105%** |
| 32 | 4.33 | +8.3% |
| **35** | **4.015** | **+0.4%** |

So **35 is the level-preserving value** and 32 is the nearest vLLM-idiomatic one. Chosen: 35 as the
default, named for what it is -- a calibration constant whose only justification is that it holds
today's mean growth fixed -- with 8/16/32 swept (§4.11) and published, per `owned-and-observed.md`
§7's rule. The table above is itself a sensitivity result worth printing: block size is a
deployment config knob, and at 16 it doubles the KV a decode produces, so any conclusion about when
the partition binds is worth only the gap between 16 and 35.

### 1.4 `max_tokens` does not exist, and the estimator that would replace it is two phases away

§1's two options are a declared bound and an estimate. `Request` has neither: it carries the exact
output length, which `machine.rs:696-704` reads and §1 lists as a cheat. A real predictive
distribution over remaining tokens is §3.2's, calibrated, and it needs the observables view Phase 4
introduces -- so Phase 3 cannot have one, and inventing a worse one here would put a fitted
constant inside the measurement that justifies it.

**Chosen: the sweep brackets rather than estimates.** Three arms:

| arm | reserves against | what it is |
|---|---|---|
| `bound` | `max_tokens`, a new declared field | the authoritative check, at its real cost |
| `perfect` | `req.tokens` | the cheat, used honestly: an **upper bound** on what any estimator could buy |
| `none` | prompt bytes only | the optimistic admission, and the only arm that can overcommit |

`perfect` is the item that makes this work rather than a gap. An estimator cannot beat the exact
value, so `bound` and `perfect` bracket every estimator Phase 7 could build, and the width of that
bracket is the honest statement of how much an output-length predictor is worth *before* anyone
builds one. It is also the number Phase 7 gets graded against, which is the posture §6 approves of
for the shared L2 tier and for the request-path ring.

`max_tokens` itself is a modelled constant and a bad one to guess. Clients set it from a template,
not from the request, so it is close to a per-class constant far above the mean: the field is
generated as a fixed per-class ceiling with a `--max-token-slack` multiplier over the workload's own
`TOKENS_MIN + TOKENS_SPAN`, defaulting to 4x. The default is arbitrary within a plausible range and
the sweep over it is part of the deliverable, per §7's rule.

### 1.5 Gang feasibility and per-request admission stop being two mechanisms

Today they are genuinely separate: `Hierarchy::could_admit` is a read-only reclaimability test over
`TierPool::reclaimable`, and `admit` returning `Pending` is the real decision made later. After the
correction the second one is gone for `KvBlock` (§1.2), and what remains -- a check against the
partition's capacity and what siblings have already reserved -- is the *same object* the §1.4 sweep
configures. `can_satisfy`, hence `FlowMode::Gate`, resolves to it as well.

Two consequences worth naming before the code makes them implicit:

- **The gang's +22% result is now a result about the admission policy**, not about the ledger. It
  has to be re-earned against whichever of §1.4's three arms is in force, and quoting it without
  naming the arm would be quoting a different experiment.
- **Engine slots become load-bearing for feasibility.** §1 lists "the granted partition budget,
  engine slots, queue depth, host memory" as the coarsened test. `Engine::MAX_BATCH` and
  `Engine::load` exist and `place_agent` already stages `staged_seqs`, but nothing today *refuses*
  on slot exhaustion -- `projected_ns` prices the wait instead. Whether a full batch is a refusal or
  a price is a policy decision that did not have to be made while KV refusal existed to make it
  moot. Chosen: it stays a price, and the admission check is memory-only, because turning a wait
  into a refusal changes the goodput denominator a second time in the same phase. Recorded as a
  deferral rather than an omission.

### 1.6 The offload tier is the second cut, and it is where the arbitration thesis actually lives

§9's third scope line says host DDR is not a synonym for orchestrator-owned: `cache.rs` models HBM
eviction as offload into DDR's quota, so an engine-owned class sits inside the pool the DDR shadow
price arbitrates. That is the boundary's second cut and it is easy to under-read as a detail.

It is not a detail. The dynamic census (`residency-ledger.md`, *Ownership*) says `KvBlock` accounts
for 86,753 demotions and 86,753 cascade spills on a 15k-op single-node trace against 13,759 and
11,140 for weights, and zero for the host classes. **DDR traffic is overwhelmingly KV offload.**
So when the connector takes allocation authority over the offload tier, most of what the DDR arbiter
arbitrates today stops being arbitrable by it.

That is the mechanism behind §8's risk 5, and stating it now is what makes P2 a prediction instead
of a post-hoc explanation. The shape of the replacement is fixed by the ownership table rather than
chosen: the connector's bytes become **one occupant of DDR whose size the orchestrator sets and
whose contents the connector picks**. `pick_class` can no longer name a `KvBlock` victim; it can
only be given a smaller sub-budget, on a slower clock, which is Phase 6's decision and Phase 3's
fixed input.

**What survives, and why it is not nothing.** The orchestrator still decides how much DDR the
connector gets against what `Snapshot` and `ServiceHeap` get, and that is a cross-class trade no
siloed stack makes. What it loses is the *per-eviction* form of the trade, which is the form
`phase-2.md` §4.6's coupled % counts. So the number will fall, and the honest reading is that it
falls to the rate at which the remaining classes contend, not that coupling was imaginary. Both
readings get published; P2 says which one the measurement supports.

### 1.7 Phase 3 moves authority, not observability, and the line has to be drawn at `marginal_price`

The temptation is to make the orchestrator's view of the engine cache lossy at the same time, since
both are "the engine owns it". That would merge Phase 3 and Phase 4 and destroy the one property
that makes either measurable: `phase-2.md`'s four-gap decomposition separates a plan invalidated
before it ran (`execution`) from an argmin taken over a stale view (`belief`), and Phase 4 is
defined as the phase that fills the `belief` slot. If Phase 3 also makes the view lossy, no
measurement can say which correction cost what.

**Rule: every read stays exact in Phase 3.** The orchestrator reads the engine cache's true contents
in-process. What it cannot do is decide them.

The line bites in exactly one non-obvious place. `TierPool::marginal_price` is a GDSF price over a
heap the orchestrator's own policy built, and the placement score's `displaced` term consumes it.
After the correction there is no GDSF heap for `KvBlock`, but there is still an LRU tail, and its
recompute value is exactly computable read-only. So:

- `displacement` for the KV partition stays **exact** and becomes *the value of what an LRU would
  evict* rather than *the value of what the arbiter would choose*. Same units, different quantity,
  and the difference between them is a measurable part of the boundary's price.
- The turnover-based estimator, `P(resident)`, the eviction-rate extrapolation across a sequence gap
  -- all Phase 4. §8's third item ("displacement becomes an externality... noisier and lagged") is
  describing the end state after Phase 4, not this phase.

A `--no-displacement` control arm is included anyway (§4.8), because if the term's value collapses
once it prices an LRU tail instead of a chosen victim, that is worth knowing before Phase 4 builds
an estimator for it.

### 1.8 `announce` over `KvBlock` is a contaminated result, and §1's table does not list it

`owned-and-observed.md` §1's contamination table has seven rows and says of itself that it covers
one invalidation and is not the complete list. This is the row it is missing.

`Hierarchy::announce` prewarms a flow's downstream state: for state already hot it calls
`anticipate` to bump `expect`, and for state that is not it **admits the blob outright** and then
bumps. Over a `KvBlock` both halves are the disclaimed authority -- inserting a block into the
engine's cache, and repricing one that is there. The census counts 13,100 `KvBlock` anticipates on
the default trace, and `Quota::set_band_engine` is flagged for the same reason at the quota layer.

After the correction the orchestrator can cause a KV block to exist only by dispatching work that
produces it, and can express a retention preference only as an advisory directive, which is Phase 5.
So `announce`'s KV half becomes a no-op in Phase 3 and the published flows result -- announce buys
11-18% task latency -- loses whatever share of its margin came from KV rather than from `Snapshot`
cells.

Two things follow. The flows result gets re-run and **decomposed by class**, which it never has
been. And Phase 5's deliverable ("what a directive is worth") acquires a real hole to be measured
against instead of a hypothetical one, which is a strictly better experiment than the one §9
currently describes.

### 1.9 The clairvoyant baseline survives the correction with a different meaning, and it is the number §3.8 said would be needed

`Policy::Clairvoyant` is a `TierPool` policy, and after the correction `KvBlock` is not in a
`TierPool`. The arm could be dropped as collateral. It should not be, because its meaning improves.

Before: furthest-next-use against GDSF separates *eviction* quality from *routing* quality on one
ledger, and Phase 2 measured GDSF giving up 22.8pp of hit rate and still winning on cost.

After: the engine's eviction rule is LRU by construction and is not the orchestrator's to change. A
clairvoyant *engine cache* therefore measures something else entirely -- **what a better block
manager would be worth**, which is exactly the residual §3.8 says must be measured before asking
vLLM for a tenant-aware or cost-aware eviction floor, under §6's rule that you measure the residual
regret first and ask second. It is the difference between a promotion-tier-2 request with a number
attached and one without.

So `EngineCache` carries a `clairvoyant: bool` alongside its LRU, same index and same
`clairvoyant_op` discipline as today (including the pop-by-position rule, which exists because
several paths reference a blob without completing, and the correction adds another: a preempted
sequence). It is printed as a signed difference and never called optimal, for the reasons
`phase-2.md` §1.7 gives and which the variable-size argument makes no weaker here.

### 1.10 The dynamic census is the progress bar; the static one is not -- and the published count is stale

Phase 1's static census was the feasibility answer: 12 call sites assuming allocation authority,
all in `cache.rs`. It is the wrong instrument for *this* phase, for a mechanical reason. The
`_engine` entry points are split by path, not by class -- `admit_engine` serves `KvBlock` and
`WeightShard` alike -- and Phase 3 scopes to `KvBlock` only. So the static count barely moves while
most of the behaviour does, and a plan budgeted against it would report almost no progress.

**The dynamic census is the right instrument and it gives Phase 3 the invariant that replaces
byte-identity:**

> With the correction on, `EngineOps`' `KvBlock` row reads **zero on every counter**, and its
> `WeightShard` row is identical to the correction-off run **op for op**.

A nonzero `KvBlock` cell names precisely which path was missed, which matters because that failure
has already happened twice in this repository and neither instance was visible from inside the
census (`residency-ledger.md` records `materialise` dropping superseded copies inline and
`demote_body` spilling past its own dispatcher). A `WeightShard` row that moves means the correction
leaked out of its scope.

**The published static count is 13, not 12.** `cargo build --release --features census` emits
thirteen deprecation warnings on `HEAD`. The thirteenth is `Hierarchy::reprice_engine`, added by
Phase 2's clairvoyant arm, which writes an eviction priority into an engine-allocated entry -- a
correct marking of a genuine assumption of authority. It means `phase-2.md` §5's verification line
("still compiles with its warning count unchanged at 12 -- Phase 2 adds no allocation entry point,
and a moved count would mean it did") is not satisfied as written, and three documents
(`phase-1.md` §2, `owned-and-observed.md` §9, `residency-ledger.md` *Ownership*) quote 12. Phase 3
corrects all four in its own documentation pass (§4.12) rather than leaving the discrepancy to be
found by whoever next budgets against the number.

### 1.11 The partition's default decides whether the A/B measures ownership or sizing, and unified memory has no partition at all

§9 makes the partition a fixed input, sized from config. That leaves the default unspecified, and
the default chooses the answer. Carve the KV partition out of today's `--hbm` and the weights pool
shrinks, so the correction's A/B reports an ownership change confounded with a capacity change --
and weights are the one class this phase is not supposed to touch.

**Chosen: the default partition is the KV floor the correction-off arm's own budget already
implies.** Under `Budget::Split` that is `floor[KvBlock]` in HBM, which the oracle sweep selects per
arm, so the A/B holds capacity fixed and varies only who allocates within it. Under `Budget::Open`
there is no floor to read, so the default is the mean `KvBlock` HBM occupancy of the
correction-off run at the same seed -- computed, printed with the result, and not a constant.
`--kv-partition` overrides it, and the sweep over it is a deliverable in its own right (§2's P7),
since "how sensitive is the price of the boundary to a sizing decision Phase 6 will later make" is
the question Phase 6 inherits.

**Unified memory (`--hbm 0`) is not a skip.** `Hierarchy::tier_of` puts `KvBlock` in DDR, and
`own::authority(KvBlock, Ddr, Allocation)` is already `Engine`, so the correction applies: the
engine cache is a sub-budget of DDR sitting beside `Snapshot` and `ServiceHeap`, which is §1.6's
offload case with no HBM above it. Every published unified-memory comparison moves, including the
one the ledger uses to separate hardware effects from policy effects. Skipping it would leave the
project's one control for "is this a property of the hardware" unavailable exactly where it is most
needed.

### 1.12 `drain` loses its premise, and the name it is published under does not exist

`Hierarchy::drain_all` is described by `residency-ledger.md` as "the largest single assumption of
the disclaimed authority in the simulator": it relocates an entire engine's KV cache by
orchestrator fiat. After the correction it cannot. A drained node's KV partition dies with its
engine; the state is gone and has to be rebuilt wherever the work lands. `Snapshot` and
`ServiceHeap` still migrate.

This changes `placement --drain-at` results by more than a little, and it is a genuine architectural
consequence rather than a modelling shortcut -- live migration of a VM's memory is §1's thesis for
`Snapshot`, and it is exactly what an opaque engine allocator forbids for KV.

Two smaller items ride along, both recorded rather than fixed silently:

- **The `NVMe` defect.** `drain_all` empties `hbm` and `ddr` and never touches `nvme`, so spilled
  state is neither returned nor migrated and no `holds()` will report it again. `residency-ledger.md`
  assigned this to Phase 2; Phase 2 did not take it. Phase 3 fixes it, because Phase 3 is already
  rewriting what a drain means and leaving a known-wrong baseline under a changed mechanism would
  make the delta uninterpretable. The fix is separated into its own bit so its effect on
  `--drain-at` is attributable (§3, rule 5).
- **`Machine::retire` does not exist.** `residency-ledger.md` names it twice; the method is
  `Machine::drain`. `Engine::retire` is a different thing -- retiring completed sequences from a
  batch -- which is the collision shape §6 made a naming rule about. Corrected in the docs pass.

---

## 2. Predictions, stated first

`owned-and-observed.md` §7's rule. Eight predictions, each attached to a claim it would rewrite.
Several are predictions that a published result *survives*, which are as falsifiable as the others
and are the ones §1's contamination table exists to make checkable.

**P1 -- Soft floors still beat hard partitions by roughly 32%, because the mechanism the ledger
publishes for that result is weights, and weights are out of scope.**

§1's table predicts "likely survives" and gives a reason that is mostly about KV offload. The
ledger's own account is different: weight residency rises 0.51 -> 0.66, and `WeightShard` keeps
today's semantics this phase. So the prediction is stronger than §1's -- not "survives", but
"survives at close to its published size".

- *If right:* §1's table had the right answer with the wrong mechanism, the result belongs to quota
  policy rather than to memory arbitration (the reframing §3.8 already proposes), and it is Phase 6
  that puts it at risk rather than Phase 3.
- *If wrong* (the gap collapses): the published mechanism is misattributed, the result was carried
  by KV offload after all, and §8's risk 5 has arrived on the very first re-run.

**P2 -- DDR coupled % falls sharply in the binding regime and does not reach zero.**

Phase 2 measured 93.8-96.4% of 16k-24k evictions at 4 GiB DDR per node, and 0.0% at the
`distributed` defaults where nothing binds. §1.6's census split says KV is the overwhelming
majority of DDR traffic, so once the connector's bytes are one opaque occupant, most cross-class
evictions stop being expressible. Predicted: **under 25% in the tightened regime**, still 0.0% at
the defaults, with the residue being genuine `Snapshot`-versus-`ServiceHeap` contention.

- *If right:* the per-eviction form of cross-class arbitration was mostly an artifact of assumed
  authority, and what survives is capacity arbitration on Phase 6's clock. That is a partial
  negative verdict and it should be published as one.
- *If wrong* (coupling holds above 50%): the host classes contend with each other far more than the
  census's zero rows suggest, and the arbitration thesis is not resting on the disclaimed authority.
- *Either way*, the figure is never printed without its `rho`, `lambda`, `P/C` triple, per
  `phase-2.md` §6's sixth risk.

**P3 -- The `execution` gap grows and the `belief` gap stays exactly zero, and that pair is the
signature of the boundary.**

`phase-2.md` found `execution` nonzero wherever DDR eviction pressure exists, from same-request
ordering, with `belief` at exactly zero beside it. After the correction, `Machine::plan` prices
against a cache an allocator it does not control can change between plan and run -- a second,
architectural source of the same gap. Predicted: `execution` rises on every arm with partition
pressure, by more than the existing residual; `belief` remains identically zero on `Unified` and
`Query`.

This is the prediction that makes Phase 2 worth having built. The four-gap decomposition separates
*lost authority* from *lost observability*, and Phase 3 should move exactly one of them.

- *If wrong* in the `belief` direction: Phase 3 leaked into Phase 4 and the leak is located by the
  arm that moved.
- *If wrong* in the `execution` direction (it does not rise): either the partition never binds in
  the published regimes -- checkable against the eviction counts -- or the orchestrator's plan was
  already so weakly coupled to KV residency that losing authority over it costs nothing, which is
  itself most of the answer Phase 3 exists to produce.

**P4 -- The admission bracket is wide, and optimistic admission moves cost onto a class that did not
cause it.**

Predicted ordering: `none` wins mean service time and loses at p99; `bound` loses mean service time
and goodput (it strands the partition against a 4x ceiling) and wins p99 on the latency-bearing
class; `perfect` is within a few percent of `none` on mean while retaining most of `bound`'s p99.

The falsifiable half is the asymmetry, not the ordering: **the p99 loss under `none` lands on a
different class than the one that overran**, because LRU is priority-blind and takes whatever is
coldest. If it lands on the same class, §1's entire argument for a per-class mix of the two rules is
unnecessary, and the two-tier admission §1 proposes collapses back into a scalar.

**P5 -- Gang all-or-nothing survives on the coarsened test, and the contended band moves to larger
capacities rather than closing.**

§1's table predicts survival with a coarser test. The addition is directional: with decode-produced
KV modelled (§1.3), the partition binds at capacities where it did not before, so the band in which
admission binds at all -- 6+12 GiB in the published sweep, narrow on both sides -- shifts upward.
Predicted: all-or-nothing still wins every column where it binds; the band's lower edge rises; the
6.6 s of wasted work at 5+10 GiB changes size because it is a different conservative estimate.

- *If wrong* (the advantage disappears): the +22% was a property of per-block feasibility rather
  than of gang semantics, and `owned-and-observed.md` §5's emergent-properties argument loses a
  member.

**P6 -- `announce` keeps most of its win, because the flows result is carried by `Snapshot` cells.**

§1.8's newly-named contamination. The published mechanism -- 90% of tool calls shipped to idle
model-host DDR, tool placement flipping at the region boundary -- is about warm FaaS cells, not
about KV. Predicted: the KV half of announce's 11-18% is the smaller half, under a third of the
margin.

- *If wrong* (KV is the larger half): the flows result was substantially an artifact of writing into
  the engine's cache, Phase 5 moves from incremental to load-bearing, and the coupling-tier-1 claim
  needs restating before Phase 5 rather than after.

**P7 -- The price of the boundary is more sensitive to the partition's size than to the eviction
rule inside it.**

The `--kv-partition` sweep against the clairvoyant engine-cache arm (§1.9). Phase 2 already found
that GDSF surrenders 22.8pp of hit rate to a perfect recency oracle and still wins on cost, which
says eviction-rule quality is a small lever on this workload. Predicted: the correction's A/B delta
varies by more across a 2x partition sweep than the clairvoyant-versus-LRU delta does at any fixed
size.

- *If right:* the number to take to a serving-stack maintainer is a partition-sizing interface, not
  an eviction-policy change, which inverts §3.8's suggested promotion-tier-2 ask.
- *If wrong:* a tenant-aware or cost-aware block manager is worth asking for, and §3.8's residual
  has its number.

**P8 -- The shared L2 term changes almost no `KvBlock` placement and fires mostly on weights.**

§3.10's crossover, priced with the existing constants: a 512 KiB `KvBlock` reads off local NVMe in
~180 us against a 400 us rebuild, so the local spill tier already beats recompute and a shared pool
has to beat *that* across a network hop. A 512 MiB `WeightShard` against a 4 s reload is the
opposite case by four orders of magnitude. Predicted: the term's band is weights on cold replicas,
which is Phase 6's subject, and its `KvBlock` effect at the published topology is under 1% of
placements.

- *If wrong* (it fires materially on KV): the term is load-bearing in the score permanently, and the
  infrastructure question §3.10 says to ask has its answer earlier than expected.

---

## 3. What the correction must and must not do

Six rules. The first replaces byte-identity, which this phase cannot have.

1. **One bit, both bodies, for the whole phase.** `--engine-cache` (off by default until §4.12)
   selects between today's ledger and the correction. Off must stay byte-identical to `HEAD` on the
   reproducible set after every work item, by the discipline `phase-1.md` §5 established and with
   the same three commands excluded for the same reason. Every published Phase 3 number is a delta
   on that bit at a fixed everything-else. The seam already exists: Phase 1 split twelve
   census-marked entry points into `_owned`/`_engine` bodies -- and Phase 2 added a thirteenth
   under the same discipline (§1.10) -- precisely so Phase 3 could give the `_engine` body a
   different implementation. This rule is that sentence made operational.
2. **Authority moves; observability does not.** Every read stays exact (§1.7). The `belief` gap must
   be identically zero with the correction on, and a test asserts it. Anything that makes the view
   lossy -- `P(resident)`, sequence gaps, the ZMQ channel, turnover extrapolation -- is Phase 4 and
   is refusable by citation.
3. **The engine cache never acquires a policy.** LRU, leaf-first, preemption when a sequence does
   not fit, and nothing else -- no bands, no floors, no cost weighting, no anticipation -- even if
   the measurement makes LRU look bad. Making it smarter is modelling a block manager nobody ships,
   and §1.9's clairvoyant arm exists to price that temptation rather than to indulge it.
4. **`KvBlock` only.** `WeightShard` keeps today's semantics, and the dynamic census's `WeightShard`
   row is identical op for op across the bit (§1.10). Two corrections in one measurement is the
   failure §9 scoped this phase to avoid.
5. **Every mechanism that can move a number independently gets its own bit.** Decode-produced KV
   (§1.3), the `NVMe` drain fix (§1.12), the shared L2 term (§4.10) and the admission arm (§1.4) are
   separately selectable from the ownership bit. A result attributable to two changes is not a
   result.
6. **Nanoseconds or counts.** `phase-2.md` rule 5, carried forward, with one addition: a router
   refusal is counted and never priced. There is no refusal penalty, and the admission threshold is
   a byte or token quantity, never a tuned score.

---

## 4. Work items

Ordered so each lands compiling, each is independently checkable against rule 1, and the items that
can move a published number arrive after the ones that cannot.

### 4.1 `EngineCache` in `engine.rs`

```rust
pub struct EngineCache {
    capacity: u64,
    used: u64,
    blocks: HashMap<BlobId, Block>,
    lru: /* leaf-first ordering over resident_children */,
    pub evictions: u64,
    pub preemptions: u64,
    pub hits: u64,
    pub misses: u64,
}
```

`admit(&mut self, id, meta) -> Admitted` -- no `Admission` in the signature at all, which is the
type-level form of rule 3. It evicts leaves in LRU order until the block fits; if the arriving
sequence alone exceeds the partition it records a preemption and the caller pays a rebuild (§1.2).
`touch`, `contains`, `holds`, `free`, `total`, and the counters. No `Quota` parameter, no
`anticipate`, no `marginal_price`.

Leaf-first is the prefix invariant, and it is not decoration: `residency-ledger.md`'s *Monotone
residency invariant* forbids a hole in a chain, and §3.7's per-prefix Bernoulli argument depends on
a sequence's blocks being freed tail-first. `TierPool` gets this from `leaf_first` plus
`resident_children`; `EngineCache` reimplements it because it is the one piece of `TierPool`'s
bookkeeping that survives the four deletions in §1.1's table.

Lands with the correction bit unwired, so this item alone is a no-op and is checked as one.

### 4.2 The partition in `Hierarchy`

`NodeMemory` gains `kv_partition: u64`, defaulted per §1.11 and overridable. `Hierarchy` gains
`kv: EngineCache` beside `hbm/ddr/nvme`, and `tier_of`/`home`/`is_hot`/`holds` route `KvBlock`
there when the bit is on.

`TierSpec::hbm(capacity)` for the weights pool becomes `hbm - kv_partition`. The invariant to assert
rather than assume: the two sum to the node's declared HBM, checked at construction, because a
partition silently carved from nothing is how a capacity change gets read as an ownership result.

### 4.3 The thirteen seams

Each `_engine` body from Phase 1 branches on the bit: `admit`, `touch`, `anticipate`, `demote`,
`forget_cold`, `drop_superseded`, `spill_displaced`, `drain`, `reprice`, and `Quota`'s
`floor_of/limit_of/band_of/set_band_engine`. With the bit on, the `KvBlock` path reaches
`EngineCache` and the census counter is **not** incremented -- that is the whole point, and it is
what makes §5's zero-row check meaningful. With `WeightShard` the body is unchanged and the counter
still fires.

`anticipate` and `reprice` are the two that become no-ops rather than redirections (§1.8, §1.9):
the orchestrator cannot reprice what it does not allocate.

### 4.4 Decode-produced KV

`TOKENS_PER_KV_BLOCK = 35` in `work.rs` (§1.3); `agent_turn`'s fixed 4-block growth becomes
`tokens.div_ceil(TOKENS_PER_KV_BLOCK)`, and the same relation applies to agent and flow turns.
`Machine::execute` materialises the produced blocks into the partition as the decode runs, so the
bytes occupy something during the decode rather than appearing in the next turn's chain.

Its own bit (rule 5). Off, chain growth is today's fixed 4; on, the mean is held at 4.015 by the
calibration and the variance is new. The check that this landed correctly is that measured mean
blocks per turn across a full trace is within 1% of 4 -- which the arithmetic predicts and the run
confirms, or the draw is not the distribution §1.3 assumed.

### 4.5 `max_tokens` and the admission check

`Request::max_tokens`, generated per §1.4 with `--max-token-slack`. One router-side check, shared by
the three consumers §1.5 collapses into one: single-request admission, `could_admit` for gang
feasibility, and `can_satisfy` for the gate. `--admit {bound,perfect,none}` selects the reservation.

The check is against partition capacity and staged reservations only -- memory, not slots (§1.5).

### 4.6 The offload sub-budget

The connector's DDR allocation: a sub-budget of the DDR pool whose size the orchestrator sets and
whose contents `EngineCache`'s own LRU picks. `TierPool::pick_class` can no longer name a `KvBlock`
victim; `Quota` for DDR loses its `KvBlock` class and gains an opaque reservation.

This is the item that moves P2's number, and it lands after 4.5 so a coupling change has one
candidate cause.

### 4.7 The refusal taxonomy

`Report` gains `refused_by_router`, `preempted`, and keeps `refused` for the orchestrator-owned
classes. `goodput()` keeps its definition and is printed beside all three, with a line stating that
the quantity is not comparable across the bit (§1.2).

### 4.8 Displacement over the engine cache

`Telemetry::marginal_price(Tier::Hbm)` resolves to the LRU tail's recompute value per byte when the
bit is on (§1.7) -- exact, not estimated. `displacement_in` and the locality-coupling silo follow.
A `--no-displacement` control arm zeroes the term so its post-correction value is measurable rather
than assumed.

### 4.9 Drain

`drain_all` stops returning `KvBlock` when the bit is on; the partition dies with the node (§1.12).
The `NVMe` fix lands as its own bit so `--drain-at` moves for one reason at a time.

### 4.10 The shared L2 term

§3.10's fifth option in the acquire argmin: a cross-node NVMe pool, `min(resident, peer, local
spill, shared pool, rebuild)`. Its constants are a network hop plus NVMe, both already in
`topo.rs`/`tier.rs`, so nothing new is fitted. Own flag, default off.

**This is the one item that can be cut without weakening the deliverable**, and the reason it is
here at all is that Phase 3 rewrites the acquire path anyway. If it slips, it slips to Phase 6,
where P8 predicts its value lives.

### 4.11 The sweeps

| sweep | axis | answers |
|---|---|---|
| the bit | correction off/on | the price of the boundary -- the headline |
| `--admit` | bound / perfect / none | §1.4's bracket, per class and at p99 |
| `--kv-partition` | 0.5x to 2x the default | P7's sensitivity, and Phase 6's inherited question |
| `--max-token-slack` | 1x to 8x | how much of `bound`'s cost is the ceiling's fault |
| `TOKENS_PER_KV_BLOCK` | 8 / 16 / 32 against the level-preserving 35 | §1.3's constant, per §7's rule |
| `--clairvoyant` | LRU vs furthest-next-use in the engine cache | §1.9's promotion-tier-2 number |

Every one is reported per class, and the admission sweep additionally at p99 -- §9's requirement,
and §1's reason for it is that a mean cannot see a cost that lands on someone else.

### 4.12 Report and publish

| target | change |
|---|---|
| `residency`, `flows`, `volatility`, `placement`, `distributed`, `code-review` | the correction bit, and every sweep above that applies |
| `ownership` | the dynamic census's `KvBlock` row on both sides of the bit -- the phase's own progress bar |
| `owned-and-observed.md` §1's contamination table | a **measured** column against each of the seven rows' stated expectations, plus the `announce` row §1.8 adds |
| `owned-and-observed.md` §9 Phase 3 | a **Status** line, as Phases 0, 1, 2 and 8 carry |
| `residency-ledger.md` | every re-run result, each marked with which side of the bit produced it; the goodput incomparability note; `Machine::retire` corrected to `Machine::drain` |
| `phase-1.md` §2, `owned-and-observed.md` §9, `residency-ledger.md` *Ownership*, `phase-2.md` §5 | the static census count corrected from 12 to 13, with `reprice_engine` named (§1.10) |

Whether the bit's default flips to on at the end of the phase is a decision taken *after* the
numbers, not before, and the plan deliberately does not pre-commit: if P2 and P3 come back saying
the arbitration results were carried by the disclaimed authority, the honest repository keeps both
arms and says so.

---

## 5. Verification

- **Rule 1's byte-identity, with the bit off.** `residency`, `flows`, `placement` (split and
  unified), `volatility` at reduced `--ops` and a second seed, diffed with zero tolerance against
  `HEAD` after each of §4's items individually. `distributed`, `code-review` and `data-path` get
  the structural smoke run instead, for `phase-1.md` §5's reason.
- **The census zero-row.** With the bit on, `EngineOps`' `KvBlock` row is zero on all eight
  counters; its `WeightShard` row matches the bit-off run op for op. This is the phase's single
  most informative check and it replaces byte-identity as the thing that fails loudly.
- **`EngineCache` cannot refuse.** Enforced by the type -- no `Admission` in any signature -- and
  asserted behaviourally on a fixture whose sequence exceeds the whole partition: it preempts,
  the counter fires, the request still runs.
- **The prefix invariant holds under preemption.** After any eviction, every resident chain is
  still a prefix of itself: no hole in the middle, on a fixture that forces eviction mid-chain.
- **`belief` stays exactly zero with the bit on** (rule 2), under `Unified` and `Query`, across a
  full run. This is the test that would catch Phase 4 leaking in.
- **`execution` is permitted to move, and by how much is recorded rather than asserted.** P3 makes
  it a measurement; a test that pinned it would pin the result.
- **The decomposition still sums** per decision, and `decided_by` still reduces to the `moved_by_*`
  counters, both with the bit on -- `phase-2.md`'s instrument must survive its own subject changing.
- **Decode-KV preserves the level.** Measured mean blocks per turn across a full trace is within
  1% of 4 at the default `TOKENS_PER_KV_BLOCK` (§4.4), and the sweep's 8/16/32 rows reproduce
  §1.3's table -- a disagreement there means `tokens` is not drawn as that arithmetic assumes.
- **Partition arithmetic.** `kv_partition + hbm_pool == declared HBM`, asserted at construction.
- **The clairvoyant engine cache is clairvoyant**, on a fixture with a known reference stream,
  including the never-again case and the new skip path (a preempted sequence).
- `cargo fmt --check`, `cargo clippy --all-targets --all-features`, `cargo test` clean, and the
  census build's warning count stated explicitly in the commit rather than assumed unchanged --
  §1.10 is why.

---

## 6. Risks

1. **The negative verdict, arriving as a plausible story instead of a number.** If P2 comes back at
   5% coupling, the temptation is to find a regime where it is higher and publish that one.
   `phase-2.md` §1.8 and the ledger's own *Regime selection* rules already forbid it; the specific
   mitigation here is that P2 states the threshold in advance and both readings of a low number are
   written down before the run.
2. **Two corrections in one measurement.** Decode-produced KV changes memory pressure everywhere and
   is landing in the same phase as the ownership change. Rule 5's separate bit is the mitigation,
   and §1.3's level-preserving calibration is what makes the bit cheap to hold. This is the single
   most likely way Phase 3 produces an uninterpretable headline.
3. **The partition default choosing the answer** (§1.11). Mitigated by deriving it from the
   correction-off arm's own budget and printing it with every result, and by P7's sweep -- but a
   reader who quotes the headline without the partition size is quoting a capacity decision.
4. **Scope creep in three directions**, each one commit away. Into Phase 4: the engine cache is
   opaque, so making the *view* of it lossy feels like the same change and is not (rule 2). Into
   Phase 6: the partition is a fixed input and making it a decision is one line (§1.11). Into an
   inference engine: `EngineCache` will look bad on some measurement and improving it is modelling a
   block manager nobody ships (rule 3).
5. **The engine cache is nearly a `TierPool`** (§1.1), so the cheapest implementation is a
   configuration rather than a type -- and it would compile at every call site the phase exists to
   break. The distinct type is the enforcement, the same argument `own.rs` made in Phase 1.
6. **Goodput is not comparable across the bit** (§1.2) and it is the most quotable number in the
   report. Mitigated by the three-counter taxonomy and a printed note, neither of which travels with
   a screenshot.
7. **`phase-2.md`'s instrument may need repair mid-phase.** The oracle prices candidates through
   `plan(View::Truth)`, which now reads the engine cache; P3 predicts the `execution` gap grows for
   an architectural reason, and distinguishing that growth from an instrument defect requires the
   bit-off arm to keep reproducing Phase 2's published decomposition exactly. If it does not, the
   correction is not the first thing to suspect.
8. **`--drain-at` moves for three reasons at once** -- lost KV migration, the `NVMe` fix, and the
   partition -- and it is a small, easily-overlooked result. Three bits, checked pairwise, or the
   number is not published this phase.

---

## 7. Out of scope

- **`WeightShard`.** Also HBM-resident and engine-owned once loaded, but its interesting decision is
  placement on a slow timescale. Phase 6. Rule 4 is what keeps it out, and the `WeightShard` census
  row is how that is checked rather than asserted.
- **Partition sizing as a decision**, the P:D replica ratio, the heterogeneous model mix, and
  tenancy's second `Quota` axis. Phase 6. §1.11 makes the sizing an input and P7 measures how much
  that input is worth, which is the handoff.
- **Lossy telemetry, `P(resident)`, sequence gaps, divergence, the ZMQ channel, and the per-class
  scoring quantile.** Phase 4. Rule 2 draws the line and the `belief`-gap test enforces it. §1's
  two-tier admission reserves latency-bearing work at a high quantile of predicted output length;
  Phase 3 brackets that with `bound` and `perfect` (§1.4) and leaves the quantile itself to the
  phase that has a calibrated distribution to take it from.
- **Retention directives, `retain_until`, `evict_first`.** Phase 5, and §1.8 is what gives Phase 5 a
  real hole to measure against rather than a hypothetical one.
- **Predicted flows, a tool-gap estimator, the real output-length estimator, taxonomy presets,
  per-pattern coupling.** Phase 7. `perfect` is the upper bound Phase 7's estimator gets graded
  against.
- **Slot-exhaustion refusal** (§1.5): a full batch stays a price, not a refusal. Changing that would
  move the goodput denominator a second time in one phase.
- **A tenant-aware or cost-aware engine cache.** §3.8's promotion-tier-2 ask. Phase 3 produces the
  number with which to ask (§1.9) and does not implement the answer.
- **Changing the score in response to what the correction finds.** `phase-2.md`'s rule 1, which did
  not stop applying when the instrument did its job. If displacement turns out to be worth nothing
  once it prices an LRU tail, Phase 3 reports that and Phase 4 owns the term.
