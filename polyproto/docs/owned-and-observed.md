# Owned, Inferred, Observed

Design notes for the next step: a telemetry boundary around the inference engine, a decision about
what carries a request across it, the workload taxonomy as a scheduler input, and a statement of
which advantages are emergent rather than assumed.

Two boundaries, resolving in opposite directions. §1 hands the engine's memory back to the engine.
§2 refuses to put a process boundary on the request path -- which is a statement about where the
code runs, not about who wrote it; the HTTP itself is a linked library's (§2.6). **Cede the bytes,
keep the path** -- and the second is only defensible because of the first, since routing and
cancellation are what is left to decide with once allocation is gone.

Status: design only. Nothing here is built.

**On the numbers.** Most figures come from the runs in [`residency-ledger.md`](residency-ledger.md)
and are not uniformly trustworthy. Four grades, worth keeping apart:

- **Measured on the host.** The boundary ladder -- syscall, pipe, socket, ring, wasm, `ext_proc`,
  gRPC -- times real crossings, best-of-10, timer overhead subtracted. It is the firmest evidence in
  the repository and §2 leans on it deliberately. Caveat: Apple silicon, up to ~2-3x spread on the
  cheap rungs (`Ring` was 4.2x before Phase 0 re-timed it) and a unix-socket rung that is not even
  monotone in payload, so the **ordering** is the result and no constant survives being quoted to
  two digits.
- **Simulated on modelled constants.** Every residency, placement and arbitration result. These run
  on a calibrated model rather than moving bytes; the ledger lists link latency, PCIe, HBM/DDR
  capacities and the workload's `exec_ns` as modelled, "most worth replacing with real traces".
- **Simulated on assumptions since found invalid.** A subset of the above. The ledger allocating KV
  (§1); one memory pool where the target has two; gangs shaped as training jobs rather than
  multi-agent fan-outs; a cold-container snapshot cost that inverted a headline when replaced (§7).
  The ledger's own *Standing* table already marks several claims retracted or failing.
- **Not from the runs at all.** A few figures are arithmetic on published capacities -- §3.8's HBM
  budget is a subtraction over declared model and accelerator sizes. Reproducible without the
  simulator, worth exactly what its inputs are, and labelled inline wherever used.

So a number here is a reason to run an experiment, not a result to build on. §1's contamination
table covers **one** invalidation -- ownership -- and is not the complete list.

Two inputs shape this document. [`taxo.md`](taxo.md) is the workload taxonomy it has to serve.
[`k-cross/sched_lm`](https://github.com/k-cross/sched_lm) is a sibling prototype reaching the same
central decision from the llm-d/vLLM side, read here as **evidence, not direction**.

---

## 1. The boundary

### Three categories, not two

The obvious split is *what the orchestrator tracks* against *what stays external telemetry*. One
category is missing from that, and it is where every observability-driven behaviour lives:

| | authority | freshness | if it is wrong |
|---|---|---|---|
| **Owned** | the orchestrator decided it; nothing else can be the source of truth | exact, per-request | the system is **invalid** -- overcommitted, double-admitted, a gang half-placed |
| **Inferred** | an estimate the orchestrator maintains in-process from observation | fresh, explicitly uncertain | a decision is **worse**, and self-corrects if it carries a deadline |
| **Observed** | another system owns the fact | streamed or sampled | a placement is **suboptimal**, diagnosis is blurrier; no invariant breaks |

`ToolGapIndex` in `sched_lm` is the middle category exactly: not ground truth, not external, but
orchestrator-resident and uncertain. Collapsing it into either neighbour produces the two failure
modes to avoid -- treating an estimate as authoritative (a stale residency view forcing an illegal
placement), or treating one as merely advisory (a learned re-arrival gap that never changes an
eviction).

### Tests for ownership

Something must be **owned** if any of these hold:

1. **Authority.** The orchestrator decided it, so nothing external can report it back. Admissions,
   evictions, placements, retention directives and gang membership are in this class by construction.
2. **Correctness dependency.** A wrong value makes an outcome *invalid* rather than slow. "Is this
   gang fully admitted", "is this replica still serving", "does durable state still have a home" are
   correctness questions. "Which node is hottest" is not.
3. **Hot-path rate.** Consulted per request, orders of magnitude above any export interval. Nothing
   read per request can come from a 15-second scrape.

Something stays **observed** if all hold: another component owns the fact, it is an aggregate or
derivable statistic, and losing it degrades explanation rather than decisions.

Anything else is **inferred**, and inferred state carries three obligations: a confidence, a decay,
and a deadline on any action it drives. §3.7 is where the first two stop being adornments and reach
a decision -- as one object, since a residency belief's confidence *is* its decay -- which today
they do not. The third is §3.3.

### Where polyproto's state falls

| state | category | where it is now |
|---|---|---|
| per-blob residency, host classes (`Snapshot`, `ServiceHeap`) | **owned** | `TierPool.entries` |
| per-blob residency, engine classes (`KvBlock`, `WeightShard`, offloaded KV) | **inferred** | **wrong** -- `TierPool.entries` claims it as owned |
| quotas: floors, bands, limits | **owned** | `Quota`, operator config |
| admission outcome, refusals | **owned** | `Admission`, `TierPool.refused` |
| gang membership, staged reservations | **owned** | `staged_*`, `cancelled` |
| retention directives it issued | **owned** | `Entry.expect` today; needs `retain_until` |
| placement decisions, flow graph | **owned** | `upstream`, `tool_anchor`, `origin` |
| shadow price per pool | **inferred** | `TierPool::marginal_price` (already an estimate) |
| regret rate per class | **inferred** | `TierPool::regret_rate` (already learned, ghost list) |
| per-tool re-arrival gap | **inferred** | **missing** -- `sched_lm`'s `ToolGapIndex` |
| P(turn calls a tool), which tool, payload | **inferred** | **cheated** -- `FlowHint.probability` is `1.0` |
| output length of a decode | **inferred** | **cheated** -- score reads exact `req.tokens` |
| peer residency | **inferred** | **cheated** -- `Gossip` gives a stale *exact* set |
| tenant identity and per-tenant quota | **owned** | **missing** -- §3.8 |
| workload class | **inferred** | **missing** -- §4 |
| realised TTFT / ITL, batch occupancy | **observed** | modelled internally instead |
| engine prefix-cache hit rate | **observed** | **conflated** with owned residency |
| node health, utilisation, power | **observed** | absent |

Two entries are already inferred, for good reasons. `marginal_price` is a deliberate estimate
because a faithful dry run costs as much as the eviction it prices. `regret_rate` is measured from
a bounded ghost list rather than assumed. Both are precedents: the pattern works and the engine has
somewhere to put this kind of quantity.

Three are cheats, and they are load-bearing. `FlowHint.probability` is hardcoded `1.0`
(`work.rs:638`), so every cross-workload flow result rests on the scheduler being *told* the future
with certainty. The placement score reads `req.tokens` (`machine.rs:696-704`) -- the exact output
length, before decoding -- and both engine terms scale with it. `Control::Gossip` hands over a
stale but **exact** peer residency set, which is not a thing telemetry can produce at all; that one
gets its own subsection below.

### The correction: the orchestrator does not allocate the KV cache

`README.md` is explicit: *"AI inference (llm-d style but model-type agnostic for routing/scheduling
but does not replace inference engines like vLLM)."* **Model agnosticism requires not controlling
the engine.** An orchestrator that owns KV block allocation must know block layout, attention
scheme, quantisation and paging behaviour -- it becomes an inference engine, for one family of
models, and the central goal is lost.

So the engine **allocates** within memory the orchestrator **provisions**, and reports what it did;
the orchestrator tracks that report. At the wrong grain this becomes "the engine owns its memory",
which is false in a way that matters: the orchestrator sizes the partition and that capacity stays
an owned fact. What moves is authority over *which blocks occupy it*.

**What the code does today is the old model.** `TierPool` *is* the KV cache: it admits, evicts by
its own GDSF priority, refuses when full, and is the single source of truth. Every
memory-arbitration result rests on an authority the architecture has disclaimed.

### Ownership is per class

The boundary runs between workload classes. `accelerated(kind)` in `cache.rs` already separates HBM
from DDR and comes close to drawing it -- but it is a **tier** predicate, not an **ownership** one,
and the table below cuts across it twice:

| class / resource | who owns the bytes | what the orchestrator does |
|---|---|---|
| **HBM partition** | **the orchestrator** -- it provisions the engine | decide how much HBM a replica gets, scale replicas, partition the hardware |
| `KvBlock` | **the engine** (vLLM's block manager) | observe an approximate index; influence by directive; **route** |
| `WeightShard` | **the engine**, once loaded | decide which models load where, and when to unload -- slow, coarse, genuinely orchestration (Phase 6) |
| `Snapshot` | **the orchestrator** -- it starts and stops microVMs | own outright: admit, evict, refuse |
| `ServiceHeap` | **the orchestrator** -- it scales replicas | own outright |
| offloaded KV in host DDR | the engine's KV connector (`LMCache`, NIXL) | observe; ownership follows the connector, so assume the engine's |

The two exceptions are the ones that matter: the orchestrator sizes an HBM partition the engine
allocates *within*, and an engine-owned offload tier sits *inside* host DDR's quota. So "HBM" is
not a synonym for engine-owned and "host" is not a synonym for orchestrator-owned. §8 returns to
what that costs.

This table has an executable form: `own::authority(kind, tier, question)` in `own.rs`, Phase 1's
type (`phase-1.md`), total over all twelve `(BlobKind, Tier)` cells and both the capacity and
allocation questions -- the shape §9's Phase 1 found this table needed, since a class alone cannot
say which side answers "how big is the pool" against "which bytes occupy it right now".

This is a **two-tier control system**: macro authority over the hardware (provisioning, partition
sizing, model loading) stays with the orchestrator; micro, per-request authority (KV block
eviction) goes to the engine.

**The loss: no *per-block* admission control over inference state.** The orchestrator cannot refuse
a specific KV admission, choose an eviction victim, or hold a block against the engine's will. An
engine under pressure evicts, preempts and recomputes; it does not reject for lack of KV. So
refusal moves out of the ledger and into the router -- where it can still be authoritative, because
the orchestrator granted the partition and knows its capacity. A byte- or token-depth threshold
against that partition is a real check, not a naive queue-depth guess.

**That check has a hole worth stating.** It is authoritative only if its inputs are. Prompt bytes
are known at admission; output length is not, and the table above lists it as cheated. Two options,
neither clean:

- **Admit against `max_tokens`.** A declared bound, so the check stays authoritative -- but
  `max_tokens` is routinely set far above actual output, so reserving against it strands most of the
  partition and refuses work the node could have served.
- **Admit against an estimate.** Usable, and **not authoritative**: an estimator wrong in the
  optimistic direction admits what the partition cannot hold, and the engine resolves the
  overcommit by evicting someone else's warm blocks.

Per-block authority is not replaced by partition authority, then. It is replaced by a
**bound-versus-utilisation tradeoff** -- and the tradeoff is not a scalar, because an overcommit's
cost does not land on the request that caused it. The engine resolves it by evicting whatever is
coldest, and a tenant- and priority-blind LRU (§3.8) is as likely to take an interactive turn's warm
prefix as the agent loop that overran. An optimistic admission therefore converts a *mean*
utilisation gain into a *tail* loss on someone else's class, and a sweep reporting mean service
time prices that at approximately zero.

The asymmetry is also where the fix lives -- not as a third option but as a **per-class mix of the
two**, on an axis §4 turns out to need:

- **Reserve conservatively for latency-bearing work** -- whatever a user or a blocked agent is
  waiting on. Admit it against a high quantile of the predicted output length rather than its mean,
  so the check stays near-authoritative for the class whose tail *is* the product.
- **Let throughput-bearing and `DraftOnly` work borrow the remainder** against the mean, as the
  designated victim when the conservative class expands into the slack.

§3.7 arrives at the same quantile from the routing side, which is the reason to believe the axis is
real rather than convenient.

**The catch is that the orchestrator cannot cash that understanding inside the engine.** Ceding
eviction ceded the **choice of victim**: the engine still preempts and recomputes under pressure,
but on its own LRU order, and nothing the orchestrator can say makes it drop a draft to spare a
chat turn.
The only preemption primitive left never crossed into the allocator -- **cancel the request on the
path it arrived on** (§2.3). Two-tier admission without that is a reservation policy with no
enforcement arm, which is worth knowing before it is built rather than after.

Where a deployment sits on the curve is a policy choice with a measurable cost. Phase 3 sweeps it
rather than assuming the middle option works -- per class, and reporting p99 beside the mean, since
a mean cannot see the effect this passage is about.

**The save: macro-orchestration, dataflow placement, and targeted host DDR arbitration.** HBM and
DDR are physically separate pools, so pricing an HBM KV block against a host DDR microVM cell in one
eviction shadow price is a category error: GPU prefill recompute (compute-bound, non-linear) and
microVM restore (I/O-bound, flat) do not compete for the same bus. Nor do expensive multi-GPU hosts
run arbitrary background services -- their DDR is provisioned for PCIe bounce buffers, NUMA-pinned
staging and offload connectors, and co-locating CPU compute there risks throttling the accelerator.

What remains is three real mechanisms:

1. **Dataflow locality.** The relationship between GPU inference and tool execution is *spatial*,
   not memory contention: place the dependent tool workload near the active GPU context (same node,
   rack, or zone) to cut serialisation, link latency and congestion tolls.
2. **Macro-scale orchestration.** What engines cannot see: multi-node gang admission, model weight
   loading across heterogeneous hardware, partition sizing, and the tenancy policy that follows from
   it (§3.8).
3. **Targeted intra-host DDR arbitration.** Where tool execution is co-located with inference,
   shadow pricing binds between **host-side offload connectors** and **ephemeral tool cells**
   (`Snapshot`). Soft floors beat hard partitions by balancing local tool working sets against
   offloaded inference state without starving the PCIe pipeline.

So the unified advantage is macro capacity and gang orchestration, joint dataflow placement across
topological boundaries, and targeted host DDR multiplexing -- not cross-hardware arbitration of HBM
bytes. Whether that beats siloed specialists is the empirical question.

### Which existing results this contaminates

Stated with expected direction, so the re-measurement cannot be quietly graded on a curve:

| result | why it is affected | expected |
|---|---|---|
| soft floors beat hard partitions, 32% oracle-tuned | its mechanism is entirely host DDR: a soft floor lets HBM-evicted state grow into idle DDR and promote back over PCIe (weight residency 0.51 -> 0.66). The **budget** stays orchestrator-owned; only the offload demand becomes engine-driven | **likely survives.** Still needs re-running: the ledger chose the HBM victims that generated the offload |
| fixed budgets cost 4.0% service, 2.7pp goodput | measures what non-borrowable per-class budgets cost. The DDR half (`Snapshot` / `ServiceHeap` / offload) stays orchestrator-owned; the HBM half (`KvBlock` / `WeightShard`) becomes the engine's | **shrinks** by roughly the HBM half's share |
| 90% of tool calls shipped to idle model-host DDR | the option exists because the ledger controls that DDR; an engine KV connector may own it | **uncertain**, probably unchanged for `Snapshot` cells |
| all-or-nothing fan-out admission: +22% fan-outs | rests on `could_admit` as a *per-block* HBM feasibility test, gone at that granularity | **survives, test coarsens** to the granted partition budget, engine slots, queue depth, host memory |
| tool-placement inversion under memory pressure | driven by host DDR contention, which the orchestrator owns | **likely survives** |
| acquisition crossover (KV ships within a rack, rebuilt across a zone) | a cost comparison, not an allocation decision | **survives**, and becomes advice to the engine rather than an action |
| boundary-cost ladder, origin round trip, congestion toll | independent of memory ownership | **unaffected** |

The pattern: results about **costs** survive; results about **per-block authority over inference
memory** do not; results about **budgets** survive in whichever pool the orchestrator sizes. Those
do not divide along HBM/host lines, which is why "host" is not usable as shorthand for "survives".

### Observability is not a stale exact view

`Control::Gossip` hands the scheduler a stale but **exact per-blob** residency set. No observability
system provides that -- you cannot scrape "does node 3 hold prefix X". Real telemetry offers llm-d's
precise-versus-approximate split:

- **`Metrics { interval }`** -- exact aggregates, no per-blob detail: hit rate, queue depth, pinned
  usage, batch occupancy.
- **`Events { loss }`** -- an approximate per-blob index from a KV event stream: fresh, lossy,
  probabilistic.

Replacing `Gossip` with these two is a correctness fix, not a refinement.

#### Step-aligned ingestion over ZMQ IPC

Out-of-band monitoring (Prometheus/OTel at 5-15s) is far too slow for hot-path decisions.
Per-block RPCs at request time flood the host with interrupts. In-band response trailers arrive
only after a multi-second generation finishes. So the orchestrator ingests telemetry the engine
emits directly, batched at the engine's own step boundary:

1. **Step-aligned cadence (40-100 Hz).** Engines execute discrete forward iterations: prefill
   chunks ~10-50 ms, decode steps ~10-25 ms, with allocations, preemptions and evictions happening
   at step boundaries. Flushing at the end of each iteration rate-limits event volume to the step
   frequency and keeps belief fresh to within ~15 ms.
2. **Transport: ZMQ over Unix domain sockets** (`ipc:///tmp/poly_engine_{id}.ipc`), giving lock-free
   queueing and clean framing between Python workers (`pyzmq`) and the Rust orchestrator.
   **Chosen for adoptability, not speed.** At 40-100 Hz and ~400 bytes a batch, one engine costs
   ~0.6 ms of wakeups per second and 6 us of crossing is 0.04% of the 15 ms freshness the cadence
   already sets -- every boundary on the ladder is free at this rate. What decides it is that
   **vLLM already publishes KV events over ZMQ** (`kv_events`, what llm-d's KV-aware routing
   consumes; confirm the event set against the release targeted), making this promotion tier 0/1
   with no serving-stack change. Ring-grade transport belongs on the request path, where cost is
   paid per request and per decision (§2.2).
3. **Asymmetric, eviction-centric wire format.** The router knows which prefixes it dispatched, so
   allocations can be tracked optimistically. What it cannot guess is which blocks the engine
   **evicted** under pressure and where capacity stands. A 32-byte `TelemetryBatchHeader`
   (`engine_id`, `epoch`, `seq`, `free_kv_blocks`, `total_kv_blocks`, `queued`, `running`, `flags`
   for normal/yellow >80%/red >95%, eviction and allocation counts) followed by truncated 64-bit
   blake3 hashes. 100-400 bytes typical, under 15 KB/s per engine.
4. **Drop detection.** The router tracks `seq`; a gap means the ZMQ high-water mark dropped a
   message, so the engine's reported eviction count becomes a **lower bound** rather than a fact
   (§3.7 is what turns that into a decision, by extrapolating the missing evictions at the last
   observed rate instead of assuming none happened). Every 1-2 seconds, or immediately on a gap,
   the engine pushes a **sync batch** -- a prefix-tree snapshot or block bloom filter -- to
   reconcile drift.

#### Two control loops, two clocks

Step-aligned ingestion closes the loop on **routing and admission**: the orchestrator learns of
memory pressure within 15 ms and diverts incoming prefill before the engine descends into
preemption cascades. It cannot close the loop on **provisioning**, which is physical: slicing new
partitions, pulling 140 GB of weights, initialising contexts takes 5-30+ seconds.

That gap is the point. Near-instant micro-actions (diversion, backpressure, cancellation) buy the
time that slow macro-actions (provisioning, weight swapping) need, which is what makes the two-tier
model viable.

A measured aside on staleness, sweeping the gossip period at rack distance:

| refresh every | `both, gossiped` service |
|---|---|
| 200 requests | 693.3 ms |
| 1000 | 613.2 |
| 3750 (~15 s scrape at 250 req/s) | 499.8 |
| 7500 | 481.3 |

The residency-greedy arm gets **better** as its view goes stale, because staleness is the only
thing stopping it concentrating work on whichever node looks hottest. Realistic telemetry lag is
not a handicap for that policy; it is a crutch. So "how fresh is your view" is the wrong question
to organise the experiment around. The right one is "does the score price congestion" -- and a
score that does needs no fresh view to avoid concentrating.

---

## 2. The data path

§1 gave the engine its memory back. This section keeps the other half.

The two are load-bearing on each other. Once the orchestrator cannot refuse a per-block KV
admission or pick an eviction victim, routing and cancellation are the entire remaining lever. A
design that gives the memory away *and* puts an out-of-process proxy between the belief and the
decision has kept nothing.

### 2.1 The sidecar is a second scheduler

llm-d's request path, as deployed:

```
client -> Envoy gateway -> ext_proc gRPC callout to the Endpoint Picker -> back
       -> network hop to the chosen node
       -> node-local routing proxy sidecar
       -> localhost HTTP -> vLLM
```

The sidecar mutates headers, injects `x-prefiller-host-port` and `remote_kv_source`, and runs the
disaggregated handshake: fire a prefill with `max_tokens=1`, capture the `KVTransferParams`, hand
them to the local decode engine.

**Every one of those is a scheduling decision.** Choosing a prefiller is placement. Deciding to
disaggregate is admission. Injecting a KV source is issuing a directive -- the object §3.3 prices.
So the pattern is not "a proxy with an extension point". It is **a second scheduler, smuggled into
the data path, running on a partial view**, and it exists because the first scheduler is a network
hop away while decisions must be made at engine speed.

Removing it is what "integrate deeper" means concretely: **the component holding the freshest
belief and the component acting on it are the same component.** That is also the only way §3.6's
divergence metric has a single referent -- with a sidecar there are two beliefs, and "how far has
the router drifted from the engine" stops being well-formed.

Neither predecessor had to answer this. Kubernetes is not on the request path: its decisions are
per-pod-lifecycle, so a 50 us admission webhook is free, and where it does need per-candidate work
-- scheduler `Score` plugins -- it already runs them in-process. Envoy is on the request path and
has no scheduler, so its extension points can afford to be coarse. Polyproto proposes to be both,
which is why the boundary question is load-bearing here and was not there.

### 2.2 Put the boundary where the rate is low

The ladder is measured and its costs are fixed per crossing, so what decides a transport is never
its cost alone; it is **cost x rate**, against the service time of the work being scheduled. One
rule covers every seam:

| rate | seam | llm-d | polyproto | affordable boundaries |
|---|---|---|---|---|
| per connection | TLS, ALPN, protocol normalisation | Envoy listener | commodity edge or own listener | anything |
| per step, 40-100 Hz | engine telemetry | KV events, scraped metrics | same, step-aligned (§1) | anything; choose for adoptability |
| per request | dispatch to the engine | sidecar -> localhost HTTP, 15.4 us + parse | direct, UDS: **5.2-7.5 us** (the ladder's least stable rung -- non-monotone in payload, see `residency-ledger.md`'s *Boundary costs*) | sockets; a ring for short-request classes |
| **per decision, in an argmin** | **routing and policy hooks** | **`ext_proc` callout, open stream: 36 us (measured)** | **in-process, 0 ns / wasm 13-25 ns / ring 70-180 ns** | **`Native`, `Wasm` and `Ring`** |

The dispatch row is the seam that cannot be removed -- removing it means implementing the engine,
which §1 forbids. It can only be made cheaper, and a shared ring would make it two orders of
magnitude cheaper. That is a serving-stack change (promotion tier 2), and §6 is explicit about how
to ask for one: measure the regret that justifies it first.

**The last row sets the architecture, and `machine.rs` already argues it.** `decide()` charges
`Control::Unified` nothing and `Control::Query` one crossing per placement, incrementing
`control_rpcs` by `self.active.len()` under a comment stating the rule: a fan-out query costs "one
crossing of *latency* ... but N crossings of *work*, **which is what caps the decision rate**". §2
extends that from the query seam to the extension seam, where the multiplier is identical and the
rate is higher.

`best_scored` is an argmin over candidates, so a pluggable scoring term runs once per candidate.
Phase 0 ([`phase-0.md`](phase-0.md)) measured this row instead of borrowing gRPC unary's number for
it: an `ext_proc`-shaped callout on a stream the hook service gets to keep open costs **36 us**, not
49 us. At the 4 nodes the current runs use that is **142 us per placement** -- the margin over a
warm FaaS invocation (~129 us) has fallen from 69 us to about **13 us**, one config change from
crossing under it. At a fleet of 32 it is **1.1 ms**, and the scheduler's decision rate becomes the
cluster's throughput ceiling. The same fleet over a ring costs 2.1 us; over WASM, 0.4 us.

That is not a performance difference, it is an **expressiveness** difference. At 36 us a hook runs
once, at the gateway, with the policy pre-collapsed into a single score. At 13-25 ns it runs per
candidate, per eviction candidate, per telemetry batch. `Boundary::Wasm` and `Boundary::Ring` are
now measured separately -- `phase-0.md`'s P1 predicted WASM would land at or below the ring rather
than beside it, and it did, so this is where the README's zero-cost-extension claim cashes out for
a sandboxed extension specifically, not just an in-process one.

### 2.3 The honest accounting

Removing `ext_proc` and the sidecar saves a tax that depends on how the sidecar's processor stream
is configured -- `phase-0.md` measured both shapes rather than assuming one, and
[`phase-8.md`](phase-8.md) turned the estimate into an arm: `data_path: { Integrated, Sidecar,
SidecarPluggable }`, charged on the same trace through the same scored placement policy, so only
the data path varies. Envoy's documented default opens a new `ext_proc` stream per HTTP request,
measured at 63 us; a processor that gets to keep its stream open instead measures at 36 us. The
first version of this section adjusted a hand-built estimate the two ways and added a ~6.9 us parse
term with no provenance behind it; `phase-8.md` §1.2 dropped that term for exactly that reason, and
the arm now reports what the simulator's own `Cost::total_ns()` carries rather than an estimate
built by hand: **43.97-74.97 us per request** across the two deployment shapes, at `--fanout 0.10`
(the `distributed` default) with `d = 1.23` decisions per request, *measured, not assumed* -- a
gang's agents share one decision but each tool call is another, so this multiplier rises with
agentic traffic rather than staying fixed at the "4 decisions" earlier sections used. Both figures
are from the measured ladder and are the solid half. The denominator is not:

| work being scheduled | provenance | what the sidecar path adds |
|---|---|---|
| agent turn, ~1 s of decode | modelled | **0.007%** |
| 30 ms classification or extraction | illustrative | 0.2% |
| 1 ms of work | illustrative | 6.5% |
| warm service request, 250 us | `SERVICE_EXEC_NS`, a **chosen constant** | +26% |
| warm FaaS invocation, ~129 us | `FAAS_EXEC_MIN_NS` 40 us + `U[0, 160 us]`, a **chosen constant** | +50% |

The bottom two are workload constants, not measurements -- `residency-ledger.md` labels them
"measured" in one table and lists `exec_ns` as **modelled** four hundred lines later, and the code
is `FAAS_EXEC_MIN_NS + rng.below(FAAS_EXEC_SPAN_NS)`.

**So argue from the crossover, which needs no denominator.** `S* = T x (1/f - 1)` for the realized
tax `T`, needing no sweep and no simulator beyond producing `T` and `d` -- `phase-8.md` §1.1. Across
four runs on this host, the sidecar path costs more than 5% of a request below **0.83-0.90 ms**
with the stream kept open and **1.38-1.43 ms** on Envoy's per-request default, and less than 1%
above **4.34-4.71 ms** and **7.18-7.43 ms** respectively -- against this section's earlier borrowed
prediction of ~1.0-1.6 ms / ~5.2-7.9 ms, confirming that dropping the unexplained parse term moved
the crossover down, as §1.2 predicted it would. The claim that survives any constant here, and
either shape: *the data-path choice binds for sub-millisecond work and dissolves an order of
magnitude above it.* Whether that matters is then a question about the **request mix**, answerable
from published FaaS duration distributions rather than polyproto's `exec_ns`.

**A pluggable sidecar policy is a different, larger number, and conflating the two overstates the
deployed tax.** `SidecarPluggable` charges the hook once per *candidate* rather than once per
*placement* -- what extending the sidecar's own scoring logic out of process would cost, not what
llm-d's Endpoint Picker (which scores in-process and returns one decision) actually pays. At the
same defaults it measures **148.14 us/request**, crossing at **2.81 ms / 14.67 ms** -- roughly
3.4x the deployed sidecar's tax, tracking the 4-node candidate count almost exactly. `phase-8.md`
§1.3 names this the difference between §2's `data_path` arm and §2.2's expressiveness argument, and
keeps the two arms separate so neither number is quoted for the other's question.

**That figure is a lower bound, and the direction matters.** A fan-out pays one `decide()` for the
whole gang while `place_agent` runs a separate argmin per agent, so a hook that really ran per
candidate would be consulted `agents x candidates` times rather than once times candidates. The
undercount flatters the sidecar, which is the arm this section is least inclined to flatter, so it
is stated rather than corrected mid-publish.

**For inference alone, killing the sidecar is not worth doing on latency grounds, and this document
should not claim it is.** A decode-bound turn sits three orders of magnitude above the crossover;
no plausible constant moves it. Four things make it worth doing anyway, and only the first is
about speed.

1. **One data path serves both denominators.** An inference-only stack can buy a proxy hop out of
   the decode budget; a FaaS control plane cannot, which is why none of them have one on the invoke
   path. Polyproto serves both from one path, so choosing Envoy makes the FaaS and service classes
   pay the inference class's overhead budget. No silo can make this argument, because no silo has
   both denominators.
2. **Expressiveness in the argmin** (§2.2). Hook cost decides whether a policy can be a function of
   the candidate or must be a constant attached to the request.
3. **One belief, one actor.** Residency is a belief maintained at ~15 ms freshness. If the decision
   point is a hop from the belief, the belief is stale again when used, and divergence has two
   referents instead of one.
4. **Cancellation is the only preemption left.** The engine still preempts; §1 ceded the choice of
   victim, so none of it runs in the orchestrator's priority order. What remains is to stop
   sending, and to stop a stream already in flight, since an abort propagated to the engine frees
   its blocks at the next step boundary. That makes the request path the
   **enforcement arm for every priority policy in this document** -- two-tier admission (§1),
   `DraftOnly` preemption (§4) and the tenancy trade (§3.8) are reservation policies whose only
   teeth are a cancel. A sidecar can carry a cancel; it carries it one hop from the component that
   decided to issue it, and when the client simply disappears it decides on its own partial view
   whether that was a preemption or a retry.

### 2.4 The split moves; it does not vanish

Polyproto still has two processes, a global scheduler and a node agent. What changes is where the
seam falls, and it falls where §1 puts the memory seam:

| | §1: who owns the bytes | §2: who owns the decision |
|---|---|---|
| **macro / global** | HBM partition sizing, model placement | cross-node routing, P/D pairing, gang admission |
| **micro / local** | KV block allocation and eviction (engine) | dispatch, backpressure, policy hooks (node agent) |

A decision goes to the tier holding the state it needs, and the tier boundary is crossed at the
rate of the **coarser** tier. The sidecar pattern inverts this: it cuts the path at a seam every
request must cross, rather than at one only globally-informed decisions cross.

### 2.5 Prefill/decode: pair the request, size the fleet

The sidecar's most substantial job is one polyproto already has machinery for, at one of its two
timescales.

**Per request**, choosing a prefiller and a decoder is one placement decision over a pair, not two
independent ones: the transfer between them is a link cost `Topology` already prices, and its
magnitude depends on both endpoints. It is also all-or-nothing -- a prefill placed without a decoder
to receive its KV is wasted GPU work -- which is the primitive behind the +22% fan-out result,
applied to a gang of two with a direction.

**Per fleet**, prefill is FLOPs-bound and decode is memory-bandwidth-bound, so the right **P:D
replica ratio** shifts with traffic shape: long prompts and short outputs want more prefill
capacity; agent turns with short prompts and long generations want less. That is a slow capacity
decision, so it sits in §2.4's macro tier and in Phase 6. Per-request pairing without it only
distributes the imbalance evenly.

The obvious objection: §1 removed the ability to refuse a per-block KV admission, so how can a pair
be refused? Because this is a **placement** refusal made before dispatch -- against the granted
partition budget, engine slots and queue depth -- not an eviction decision inside the engine.

The transfer itself stays out of the data path: NIXL or Mooncake move bytes GPU-to-GPU over RDMA,
and the orchestrator owns the **handshake, not the bytes**. Same discipline as §1 -- the value is in
deciding the pair, not carrying their traffic.

**"Owns the handshake" means owns the pairing, not the transition.** Worth stating, because the
other reading puts a control-plane round trip in the middle of TTFT: prefill finishes, the engine
reports up, the orchestrator then dispatches the decode. Both endpoints are chosen *before*
dispatch and both are told then, so prefill completion signals its paired decoder directly, peer to
peer alongside the KV transfer, and the orchestrator learns of it on the telemetry clock like
everything else. Relaying it instead would cost one intra-cluster round trip -- hundreds of
microseconds within a rack, past a millisecond across zones -- on a prefill of tens of milliseconds.
That is single-digit percent rather than a catastrophe, but it is pure loss, avoidable by
construction, and the kind of thing an architecture settles once rather than measures later.

An orchestrator picks the pair and sets the ratio; a sidecar picks a prefiller from a list. That
difference is a **coupling-tier-2** joint decision, and coupled % (§3.4) sizes it.

### 2.6 What this gives up

First, what it does not. **"Integrated" is a claim about the process boundary, not about
authorship.** Everything §2.2 argues -- a policy hook inside the argmin at 0 ns, one component
holding the belief and acting on it, a cancel issued by the component that decided to issue it --
follows from the scheduler and the data plane sharing an address space. None of it requires writing
HTTP, and nothing here proposes to. A library-grade proxy core linked into the scheduler process
satisfies the argument whole: `pingora` already exposes connection pooling, HTTP/1.1 and HTTP/2
with flow control, and an `upstream_peer` hook called in-process to choose the upstream -- which is
this document's argmin, at exactly the boundary §2.2 requires. `hyper`/`h2` under `tower` is the
same trade one layer lower, with more assembly and more control. Either is linked, not written.

So the item that usually opens a list like this one is mostly not ours: framing, parsing, TLS and
protocol normalisation belong to a dependency maintained by people who do it full time. Not zero --
a linked CVE is still a redeploy, and picking the dependency is a real decision -- but it is the
exposure every Envoy deployment already carries, not a new one.

What remains, worst first:

1. **Stream semantics are ours whoever wrote the framing.** A library supplies flow control; it
   cannot decide what to do when a stream stalls. Three decisions stay:
   - **Cancellation.** A client that disconnects mid-generation has to become an engine abort, or
     the request decodes into a socket nobody is reading and holds its KV blocks until
     `max_tokens`. Engines have leaked here historically. It is also the primitive §2.3 leans on
     for preemption, so it is load-bearing twice and a bug in it is a correctness bug in the
     priority model, not just a wasted GPU.
   - **Backpressure, and where it lands.** A slow client stalls its HTTP/2 receive window and the
     tokens already generated have to go somewhere: buffered in host DDR, or pushed back into the
     decode loop as head-of-line blocking that looks exactly like engine slowness. Neither is free,
     and the first is **an occupant of the pool §5 prices** -- a few hundred stalled streams are a
     memory-arbitration event and not only a latency one. That term does not exist in the ledger
     today.
   - **Retry, timeout and hedge against a stateful backend.** Re-issuing a partly-decoded request
     is not idempotent and throws away a warm prefix; hedging one duplicates prefill. These are
     scheduling decisions wearing transport clothes, which is an argument for holding them here,
     but they still have to be made.
2. **In-process extensions trade isolation for the 0 ns.** `ext_proc`'s 36-63 us (measured;
   depends on whether its stream stays open) buys a separate address space. A first-party ABI
   extension can corrupt the scheduler, and a segfault takes the node's control plane with it.
3. **Ecosystem.** SPIFFE/mTLS wiring, the WASM filter catalogue, observability that assumes an
   Envoy in the path.

Item 2 is a scope decision and resolves below. **Item 1 does not**, and it is the honest residual:
these are semantics no library chooses for us, they are east-west by definition, and §8 lists them
as the largest gap between this design being right and being shipped.

**North-south stays commodity; east-west is ours.** Directly from §2.2: a proxy is acceptable where
its cost amortises per connection and unacceptable where it is paid per decision. TLS termination,
HTTP/3, WAF and DDoS handling are per-connection concerns at the internet edge and can stay behind
a commodity proxy without touching anything claimed here. What gets replaced is the per-decision
path from the trust boundary inward -- also the only part llm-d puts a sidecar on.

**Isolation becomes a costed choice**, which is `README.md`'s ring 0 / ring 3 split made measurable.
`phase-0.md` replaced the borrowed `Ring` figure below with a real WASM measurement, and split it
into two rows rather than one: a warm sandbox shared across calls, and a fresh instance per call.
The two answer different trust questions and differ by ~400x, which the single borrowed number hid:

| extension | isolation | boundary | cost | where it may run |
|---|---|---|---|---|
| first-party policy: scoring terms, admission rules | none | `Native` | 0 ns | inside the argmin, per candidate |
| policy trusted per tenant: a shared warm WASM instance | WASM sandbox | `Wasm` | 13-25 ns | inside the argmin, per candidate |
| policy untrusted per call: a fresh WASM instance | WASM sandbox, no cross-call state | `Wasm` (fresh instance) | ~9.5 us | once per call, off the argmin |
| a cross-core hop to another thread, over shared memory | **none measured** (see below) | `Ring` | 70-180 ns | per request, per decision |
| foreign runtime: a Python classifier, a small model | separate process | `UnixSocket` / `Grpc` | 6-48 us | once per request, off the argmin |

**The `Ring` row prices a crossing, not an isolation boundary, and the distinction is load-bearing
in a table whose whole point is isolation.** `ring()` spins two threads over one address space, so
70-180 ns buys a cross-core cache line and a spin detect -- nothing that would contain a hostile
extension. A ring between genuinely separate processes pays shared-page mapping and a second
scheduler domain on top, and this repository has never measured that. The row is here because it
bounds what an in-process ABI extension costs, not because it is an isolation option.

"Can this extension be trusted" then has an answer in nanoseconds, priced by the ladder rather than
settled by an architecture review -- and now a different answer depending on whether the trust
boundary is per-tenant (share the instance, pay 13-25 ns) or per-call (pay ~9.5 us for isolation
that survives a hostile input). `phase-0.md`'s P3 predicted the second row would cost 10-100x the
first; measured, it is **400-750x**, so per-call WASM isolation is real but far pricier than the
prediction expected, and it leaves the argmin entirely -- at that cost it belongs off to the side
with the foreign-runtime row, not inside a per-candidate score.

### 2.7 What to measure

Two numbers. Both are done, and neither needed the sweep this section originally asked for.

**Extend the ladder. Done in `phase-0.md`.** `boundary.rs` used to measure gRPC unary standing in
for both an `ext_proc` callout with header-mutation semantics and a WASM sandbox it had no rung for
at all -- so the old comparison was gRPC-unary-versus-native, which is nobody's actual choice. It
now has a warm-instance WASM rung, an `ext_proc` rung in both deployment shapes (stream kept open,
and a stream per request), and a re-timed `Ring` rung, and publishes the per-decision cost of a
policy hook at each isolation level (§2.2, §2.6).

**Then an arm. Done in `phase-8.md`.** `data_path: { Integrated, Sidecar, SidecarPluggable }`,
charging measured seam costs per request across the workload mix -- three arms rather than two,
because charging the pluggable-policy multiplier to the deployed sidecar overstates its tax by the
candidate count (§2.3 above). `Machine::decide` charges the hook and `run_here` charges the
dispatch hop; both are counted exactly (`decisions`, `dispatches`, `candidates_seen`), not assumed.

**A curve turned out not to be the deliverable -- the crossover is a closed form.** This section
originally asked for a sweep of the service-time denominator because a run charging measured seam
costs against modelled `exec_ns` "returns whatever those constants imply". That is still true, and
it is exactly why the crossover needs no sweep to compute: `S* = T x (1/f - 1)` for the realized
per-request tax `T`, a straight consequence of `overhead share = T / (T + S)`, so a simulator that
charges `T` on the critical path and divides by service time can only reproduce this arithmetic
(`phase-8.md` §1.1). The simulated share is still printed beside the closed form as a check, and it
agrees -- exactly on a fan-out-free trace, and within the amount a gang's worst-agent-wins cost
aggregation predicts once fan-out is added (§2.3). What the simulator adds that the arithmetic alone
cannot is `d` -- the measured decisions-per-request multiplier, workload-dependent rather than
fixed -- and the fleet-size ceiling below.

Measured, now from the simulator's own totals rather than a borrowed or hand-adjusted estimate:
**0.83-0.90 ms and 4.34-4.71 ms** with the `ext_proc` stream kept open, **1.38-1.43 ms and
7.18-7.43 ms** on Envoy's per-request default -- against this section's earlier prediction of
~1.0-1.6 ms / ~5.2-7.9 ms, confirming that dropping the unexplained ~6.9 us parse term
(`phase-8.md` §1.2) moved the crossover down as predicted, not that the prediction was wrong to
make. The crossover did not land an order of magnitude lower, so this is not the branch where the
data-path case would have had to rest on §2.2's expressiveness argument alone -- though that
argument gets its own, sharper number too: an unsharded scheduler scoring `ext_proc` callouts
saturates between 19 and 20 nodes at this workload's decision rate, against ~1000 for a warm WASM
hook (§5).

---

## 3. What changes in the engine

Ordered by what makes the rest trustworthy: the boundary first (3.1), then the estimates that stop
being cheats (3.2-3.3), then the apparatus that makes any of it measurable (3.4-3.6), then two
gaps the design had and did not notice (3.7-3.8) and three smaller additions (3.9-3.11).

### 3.1 A `Telemetry` boundary

One type mediates everything a policy may read. Owned state stays directly accessible to the
ledger; the *scheduler* reaches residency, costs and load only through it. Inferred quantities live
behind it with their uncertainty attached in the shape the score consumes -- for residency that is
`P(resident)` (§3.7), not a flag; observed quantities arrive sampled.

This is `sched_lm`'s `RequestView` discipline -- "body observables, never the workload's
ground-truth class" -- applied to the whole engine rather than one policy signature. Without it,
`req.tokens` and exact `recompute_ns` stay in the score and every subsequent result is contaminated
by information no deployment has.

The migration is mechanical and the compiler finds the work: make `Request`'s ground-truth fields
private to the workload and the ledger, and give the scheduler an observables view (prompt tokens,
message count, whether the last message was a tool result, chain-root hash) plus estimator handles.

### 3.2 Predicted flows and predicted lengths replace declared ones

Replace `FlowHint { probability: 1.0, lead_ops, payload_bytes }` with an estimator over observed
history, one per (tool, session-class): P(this turn calls a tool); which tool, as a distribution;
re-arrival gap as EWMA mean **and variance**, so confidence is available rather than invented; and
payload size.

`Hierarchy::anticipate(id, weight)` already takes a probability and already caps an announced
access at one real access. It has been fed `1.0` since it was written. Feed it the predicted
probability and the mechanism becomes honest with no change to the ledger.

This unlocks the falsification the prewarm and gate results need: **how much of the
coupling-tier-1 win survives when the hint is an estimate?** Announce currently buys 11-18% task
latency against a perfect oracle. Against an EWMA with real variance it buys less, and the amount
it loses is the honest value of the mechanism.

**Output length is the second cheat here, and a mean will not close it.** The score reads exact
`req.tokens` today; both consumers want more than its average. §1's admission reserves
latency-bearing classes against a **high quantile** of remaining output, and §3.7 scores each class
at the quantile its SLO names -- so what this estimator publishes is a *predictive distribution*
over remaining tokens, conditioned on the observables §3.1 permits (prompt length, message count,
whether the last message was a tool result, inferred workload class). A mean and a variance is the
cheapest form that serves both. It is also the form that has to be **calibrated** rather than
merely accurate, since a quantile drawn from a miscalibrated distribution is a number with a
decimal point and no meaning -- the same obligation §3.7 puts on `P(resident)`, for the same
reason.

### 3.3 Retention directives in the ledger

Add to `Entry`: `retain_until: u64`, a soft pin with a deadline, and `evict_first: bool`, the
one-shot / cache-pollution mark.

This is RFC-0001's `50; ttl=<window>; scope=<session>` and `-1`, and it is strictly better than the
current unbounded `expect` bump: a priority inflation with no deadline never self-corrects when the
prediction was wrong, while a TTL does. It is also live in `sched_lm`'s forked simulator, so
modelling it means modelling something that exists.

The unified angle: a directive priced in the same ns/byte as everything else can be weighed against
what it displaces. In a siloed stack a retention hint is advisory and unpriced.

### 3.4 Oracle, regret, coupling

**Status: implemented and measured** (`phase-2.md`, `src/oracle.rs`, `Machine::oracle_pick` /
`finish_regret` in `src/machine.rs`). "Oracle" names three different things, only one of which is
buildable: an *information* oracle (the model's own cost over true state) is identical to the scored
policy by construction and its regret is trivially zero; a *clairvoyant* oracle over the whole future
trace is intractable, since a decision changes the residency the next decision faces. What is built is
the **realized-cost oracle** -- at each decision, read-only, price what the simulator would actually
charge to serve this request at each candidate, against truth rather than belief, and take the
minimum. It is a lower bound on what a better policy could win, not an upper one: it is myopic, so it
cannot see a policy whose value is the residency it creates, which `residency-ledger.md`'s
*falsification test that fails* already demonstrated (§9's P6 re-ran that test through this exact
instrument and reproduced the blind spot: at a 512 MiB flow payload, `flow only` shows far larger
heuristic regret than `scored` while achieving *lower* service time).

The methodological gap, and the reason every number so far carries a fairness caveat. Import three
metrics from `sched_lm`:

- **Routing regret** -- `policy_cost - oracle_cost` computed on *the policy's own state*, which
  separates decision quality from state quality.
- **A clairvoyant eviction baseline** (`oracle-belady`), distinct from the routing oracle, so ledger
  quality and placement quality are separable too.
- **Coupled %** -- the fraction of decisions that change when evaluated globally rather than in
  silos, reported on two orthogonal axes so isolated hardware domains are not conflated:
  - **Memory coupling (host DDR):** how often optimal retention, eviction or allocation of a FaaS
    snapshot changes once co-located service heaps and offload tiers are accounted for.
  - **Locality coupling (topology):** how often optimal tool placement changes based on which node
    or rack holds the dependent inference context.

**Coupled % is the best available answer to "does unified beat siloed."** It measures how much a
decision depends on state a silo would not have, per request, with no baseline to tune and no arm
to handicap. It also **bounds the unified advantage from above**: where coupling is low, a unified
view provably cannot help much, and we should say so rather than hunt for a configuration where it
does.

### 3.5 Regime mix, including wait

`sched_lm` reports the share of requests resolved by wait / transfer / recompute. Polyproto reports
fetched / rebuilt and has **no wait regime**, though the congestion toll prices queueing
implicitly. Adding the third makes the acquisition decision legible and comparable across the two
prototypes.

### 3.6 Divergence as a first-class signal

Once cache state is observed rather than owned, the gap between what the ledger believes is
resident and what the engine reports is itself the metric -- the only honest measure of how well a
directive-plus-belief architecture tracks reality. Report it, do not smooth it:

```
Divergence(e, t) = |Belief(e) \ Actual(e)| / |Belief(e)|
```

It spikes on three things: eviction cascades the engine runs between batches; telemetry drops (a
`seq` gap, after which the router's eviction count is a lower bound); and ignored retention
directives. Tracking it isolates whether a routing mistake came from a bad cost model or from a
belief that drifted.

### 3.7 Confidence has to reach the argmin

§1 requires inferred quantities to carry a confidence. §1's telemetry detects a sequence gap. §3.6
publishes divergence. **None of it changes a placement.** `Machine::plan` takes an argmin over
expected cost, and an argmin over means is blind to spread: a node whose belief just went stale
keeps whatever mean it last had and keeps winning on it.

This bites hardest in exactly the situation the telemetry exists for. A node under memory pressure
bursts evictions, overruns the ZMQ high-water mark and drops batches -- so to a mean-only score it
becomes indistinguishable from a quiet node. The failure mode is not "the router learns slowly". It
is **"the router herds onto whichever node has stopped reporting"**, because silence reads as calm.

**The obvious fix is the wrong shape**, and the reasons are worth recording rather than
rediscovering. A risk penalty -- `cost = E[cost] + lambda * sigma[cost]`, with sigma widened by
belief age, gap count and divergence -- fails twice. First, the distribution is not one a mean and
a standard deviation describe: a prefix is resident or it is not, the acquire term is ~0 ns or a
full prefill recompute, and nothing lives between them. Sigma on a bimodal variable is largest
exactly where the mean is least informative, so a penalty scaled by it moves for the right reason
by an arbitrary amount. Second, lambda is dimensionless, which is §3.9's objection to
`alpha * overlap - beta * load` reappearing inside the fix: a knob with no exchange rate, swept per
deployment, is a policy wearing a constant's clothes.

**The bimodality is the structure, so price it.** What is uncertain is a binary fact -- does node
`e` still hold prefix `p` -- and both branches already have costs the model computes:

```
E[acquire] = P(resident) * cost_hit + (1 - P(resident)) * cost_rebuild
```

The only new quantity is `P(resident)`, and §1's telemetry was designed to supply it without
knowing that was the use. Under LRU over block hashes a block survives until the pool turns over
past its stack depth, so the estimator is a **turnover count**, not a fitted curve: with `V` blocks
evicted since the belief was last confirmed and a partition of `B` blocks,
`P(resident) ~ max(0, 1 - V/B)` under a uniform-rank assumption, sharpened by how recently the
router last dispatched that prefix. The `TelemetryBatchHeader` carries eviction and allocation
counts and free/total blocks for precisely this.

Three properties make it better than the penalty it replaces.

- **A sequence gap acquires a meaning rather than a magnitude.** A gap does not widen a variance; it
  makes `V` a **lower bound**, and the honest move is to extrapolate at the last observed rate. A
  node that goes quiet has its eviction count estimated from the pressure that preceded the silence,
  so its `P(resident)` decays on its own. Silence stops reading as calm with nothing tuned, and
  staleness becomes self-limiting because the belief is *used* as a probability rather than
  *penalised* as a risk.
- **A prefix is one Bernoulli, not a product of them.** Blocks of a prefix are touched by the same
  request and share a last-use time, so they age and evict together -- which is what makes a single
  `P(resident)` per prefix defensible instead of a per-block product that would drive every long
  prefix to zero. The simplification's error is in the *length* of the surviving prefix, not in
  whether one survives, since a sequence's blocks are freed tail-first and a truncated prefix is
  still a shorter hit.
- **Everything stays in nanoseconds.** No new constant enters the score.

**Risk aversion does not vanish; it moves to where it has units.** One genuine convexity survives
the mixture: if a node went quiet because it is in a preemption cascade, the miss branch is not a
clean recompute but a recompute behind a queue, and an expectation over a heavy tail underweights
the tail a latency SLO is about. The answer is not a dimensionless multiplier but a statement of
**which quantile of the predictive distribution a class is scored on** -- itself in nanoseconds,
and a field §4 turns out to need and not have:

| class | scored on | behaviour |
|---|---|---|
| latency-bearing | p90 of predicted cost | conservative; pays for certainty |
| throughput-bearing | the mean | utilisation-seeking; absorbs the tail |

For a two-point mixture this collapses to something with no free parameter at all: the p90 of
`{cost_hit w.p. p, cost_rebuild w.p. 1-p}` **is** `cost_hit` when `p >= 0.9` and `cost_rebuild`
otherwise. Scoring latency-bearing traffic at p90 therefore means *assume the prefix is gone unless
belief is at least 90% confident*, and the quantile the SLO names is the entire input. It is also
the same object §1's two-tier admission reserves against, so one statement per class governs both
the routing score and the admission bound -- which is the test of whether this is a real axis or
two knobs sharing a name.

Two things to watch, stated in advance so they count as predictions. A threshold rule can **flap**:
a node oscillating around `p = 0.9` alternates between two very different scores, and whether the
continuous congestion term damps that or hysteresis is needed is a measurement, not an assertion.
And the turnover estimator is crude -- uniform stack rank is a convenient lie -- so Phase 4
publishes `P(resident)` against realised hit rate as a calibration curve, the cheapest available
test of whether the belief means anything at all.

This also turns an accident into a principle. §1 measured that residency-greedy gets *better* as its
view goes stale, because staleness happens to stop it concentrating. Pricing the belief as a
probability is that effect on purpose -- and unlike a lambda, it is the effect with a unit.

### 3.8 Tenancy is soft, and the engine is tenant-blind

The target is Kubernetes-shaped **soft** multi-tenancy: teams inside one company, separated by
quota and policy, not mutually hostile. §1's cession has a consequence for that which is not
obvious.

**Ceding eviction cedes tenant fairness on that pool.** vLLM's block manager evicts LRU over block
hashes and has no tenant concept. So when one team's agent loop floods a shared engine with unique
prefixes it evicts another team's warm blocks, and the orchestrator cannot choose otherwise:
directives are advisory and the engine may ignore them. Under the old model this *was* expressible
-- `TierPool` evicted by a GDSF priority the orchestrator controlled, and a tenant term could have
gone into it.

**That blindness is contingent, not structural, and the difference decides how to ask.** A tenant id
on a block group and a per-tenant eviction floor is bookkeeping over opaque hashes: it needs nothing
about block layout, attention scheme or quantisation, so it is *not* the model-specific dependency
§1 refuses. What it is, is a change to somebody else's scheduler -- promotion tier 2, under §6's
rule that you measure the residual regret first and ask second. The design must therefore assume a
tenant-blind engine while being able to say what a tenant-aware one would have been worth. Calling
it impossible would be wrong; assuming it available would be worse.

**What remains is the partition**, which lands tenancy back on a decision the orchestrator owns:

| | one shared partition | a partition per tenant |
|---|---|---|
| cross-tenant prefix sharing | **yes**, the highest-value hit in this workload -- teams share system prompts and company context, which is why `work.rs` gives each tenant a shared prefix | no |
| batch occupancy | one wide batch, `step_ns` amortised across tenants | fragmented; each tenant pays the weight-read floor |
| noisy-neighbour isolation | **none**; LRU is tenant-blind | enforced by construction |

**But "partition" has to name something physical, and the options are coarse.** An engine instance
has one global block allocator and no internal quota, so a partition is not a slice of an engine's
KV pool -- it is an engine:

| partition = | isolation | what it costs |
|---|---|---|
| a separate engine instance | real; separate allocators | **a second copy of the weights**, out of the same HBM the KV wanted |
| a MIG slice on a shared GPU | real, hardware-enforced | weight duplication again, plus fixed slice sizes and no NVLink-width tensor parallelism inside a slice |
| a LoRA adapter over a shared base | **none on KV** | nothing -- this is the *sharing* case wearing a tenancy word |

The last row is the one to get right, because it reads like isolation and is not. Multi-adapter
serving keeps one base model and one block allocator, so LoRA is how you avoid duplicating
*weights*, not how you separate *memory*; it belongs in the left-hand column of the table above,
and a tenancy story resting on it has bought nothing.

The first row prices the whole question, and it is layout-independent: however the GPUs are sliced,
a node's KV pool is `HBM - tenants x weights`. On declared capacities rather than any measurement
here -- a 70B model at fp16 is ~140 GB, an 8xH100 node holds ~640 GB -- that is ~500 GB of KV at one
partition, ~360 GB at two, ~80 GB at four and infeasible at five. **The first split costs 140 GB,
more than a quarter of the KV pool, and the curve steepens from there.**

So fairness on KV is purchasable only in units of partition, at three named prices: duplicated
weights, lost cross-tenant prefix sharing, and narrower batches. There is no hint-shaped
workaround. And because the quantum is that large, the realistic unit below a handful of tenants
per model is a **replica**, which makes tenancy a question of *which tenants share a replica set* --
a routing and capacity decision, in §2.4's macro tier, and the reason Phase 6 settles isolation and
sizing in one act rather than two.

**The quota axis is missing.** `Quota` is per *class*: `band`, `floor` and `limit` are all
`[_; BlobKind::N]`. Soft tenancy needs a second axis per tenant, and the two interact the standard
way -- when a tenant is over quota and an under-floor class wants its bytes, one has to yield. The
workload already carries tenants; the ledger does not. Same generator-side / scheduler-side split
§4 describes for the taxonomy, same fix.

One reframing falls out. The soft-floors-beat-hard-partitions result **is** this argument in
miniature: a soft floor lets a class borrow idle capacity where a hard partition strands it, which
is Kubernetes' requests-versus-limits trade one axis over. Read that way it is less a claim about
memory arbitration than about **quota policy**, and it survives §1's correction better in that
form.

### 3.9 What the score already does

Two objections arrive reliably enough to answer here rather than in review.

**"Routing on prefix overlap causes herding."** Correct, and already priced. `Machine::plan` scores
five named terms -- `acquire`, `displaced`, `handoff`, `engine`, `congestion` -- and
`Engine::congestion_ns` charges what joining a batch does to *every sequence already in it*. In the
recorded runs congestion alone changes 4.7% of placements and load 13.6%. §1 then measured the
failure directly from the other side: residency-greedy improves as its view goes stale, because
staleness is the only thing stopping it concentrating, while the scored arm shows no such effect.

**"Use `alpha * PrefixOverlap - beta * TokenLoad`."** The same idea, weaker. A weighted sum needs
alpha and beta tuned per deployment and they are not commensurable -- a unit of overlap and a unit
of load have no exchange rate, so the tuning *is* the policy. The cost model denominates every term
in **nanoseconds**, which have an exchange rate by construction. Nothing is tuned because nothing
needs converting, which is also why a term can be added without re-tuning the others.

That rule is why §3.7 turns down the risk penalty it was reaching for: `lambda * sigma` would have
been the first dimensionless constant in the score, and the shape of the uncertainty -- one binary
fact with two already-priced branches -- made it unnecessary. The single input there that is not a
nanosecond is the quantile a class is scored on, and a quantile is a **declared SLO, not a fitted
constant**: it comes from the workload, means something before any sweep, and is the same number
§1's admission reserves against.

### 3.10 A shared L2 tier, priced before it is built

The acquire argmin is `min(resident, fetch from a peer, this node's NVMe spill, rebuild)`.
Production stacks add a fifth option: a **cross-node NVMe pool** shared by every replica (SageMaker
HyperPod mounts Curvine over FUSE for this), turning a cold replica's miss into a read rather than a
recompute.

It belongs in the cost model whether or not it is built, because its value is a crossover. A shared
read is slower than a rack-local P2P pull and faster than recomputing a long prefill, so it wins in
a band -- long contexts, cold replicas, peers that do not hold the prefix -- and loses outside it.
Adding the term costs nothing; building the tier is infrastructure, and §6 names the way to ask:
measure the residual regret that would justify it. Without the term, that regret is invisible and
the question cannot be put.

### 3.11 Tracing at the rate the ladder allows

Standardised spans across the decision path (llm-d defines contracts like
`gen_ai.latency.time_to_first_token` and `llm_d.kv_cache.lookup.cache_hit`) are worth adopting, and
§2.2's rule decides where they go rather than taste.

**One span per request**, recording the winning node and which of the five terms decided it: yes. A
request costs hundreds of microseconds at minimum, so a span is lost in it, and this is the
`observed` category doing its job. **One span per candidate inside the argmin**: no. That is the
per-decision rate, where a bare syscall (97 ns) already exceeds an entire ring round trip.
Instrumentation costing more than the decision it describes has stopped being instrumentation.

Not a compromise -- the same rule that rejected `ext_proc`. An orchestrator that gets this wrong for
telemetry has reintroduced as observability precisely the overhead §2 removed as architecture.

---

## 4. The taxonomy as a scheduler input

[`taxo.md`](taxo.md) is ten patterns across four dimensions. That is right as analysis and wrong as
an engine input: the scheduler needs a handful of fields it can act on, not a pattern name.

### Four dimensions, five fields

| dimension | scheduler field | status |
|---|---|---|
| Control flow | `flow: None \| Declared \| Predicted(dist) \| Fanout(n)` | 3 of 4 built; **Predicted** is §3.2, as is the output-length distribution it needs |
| Knowledge grounding | which blob classes, and their sharing shape | KV / snapshot / weights built; **RAG missing** |
| State and time horizon | `retention: evict_first \| until(deadline) \| durable` | **missing**; §3.3 covers the first two |
| Authority to act | `authority: ReadOnly \| DraftOnly \| SideEffecting` + `pause_tolerance` | **missing**; drives speculation, and sets which preemption primitive applies (§2.3) |
| *no dimension -- see below* | `slo: Interactive \| Deadline(t) \| Throughput` | **missing**; sets the admission bound (§1) and the scoring quantile (§3.7) |

**The fifth field has no dimension behind it, which is why it went unnoticed.** §1's admission and
§3.7's score both need to know how much of the cost distribution a request is priced against, and
that is a property of the **latency objective**, not of authority: a `ReadOnly` search can be the
thing a user is blocked on, and a `SideEffecting` write can be the last step of an overnight job.
Authority correlates with it and is not it. `taxo.md`'s patterns *imply* the axis -- a conversational
assistant is interactive, batch inference is not -- but none of its four dimensions expresses it, so
the scheduler needs a field the taxonomy does not supply. Two independent mechanisms arriving at
the same missing number is the reason to think it is real rather than a knob.

Unlike the other four it is **declared, not inferred**: a caller states a latency objective the way
it states `max_tokens`, so this field needs none of the estimator machinery below and is available
to any phase that wants it. That is also what keeps §3.7 free of a tuned constant -- the quantile
arrives with the request instead of being swept into existence.

Each of the ten patterns becomes a named preset over those fields, the way `sched_lm` takes
`--mix tool=0.5,rag=0.3,oneshot=0.2`. Three consequences:

**Control flow maps onto machinery that exists.** Single-call is a plain request; fixed multi-step
is the declared flow; parallel/delegated is the fan-out; dynamic multi-step -- the defining agentic
case -- is definitionally the one that cannot be declared and must be predicted. That is why
declared hints felt natural: they are the *fixed*-pipeline case, and the prototype has been testing
the easy half of the dimension.

**RAG-grounded is genuinely missing.** Retrieved chunks are shared across *sessions* with Zipf
popularity, not chain-structured like a KV prefix, so they evict differently from anything modelled
and contend with KV for the same pool. `sched_lm` models this (`--rag-docs`, `--rag-zipf`);
polyproto has no equivalent. Cheapest high-value addition, and the one grounding mode that changes
the ledger's contention shape.

**Durable memory breaks an invariant.** Every class in the ledger is evictable at a priced cost.
Durable state must never be *lost*, only demoted -- a correctness constraint, not a cost tradeoff.
That exists today only as `ServiceHeap`'s serving pin, and generalising it means a class of state
whose eviction is forbidden rather than expensive.

### Authority drives speculation, preemption, and idempotency

Authority is not an audit label. It sets what the scheduler may speculatively execute, branch,
preempt or checkpoint:

1. **`ReadOnly`** (search, reads, analysis, summarisation) -> **speculative dispatch and parallel
   pre-warming.** When turn N predicts a tool call with probability P, the orchestrator can pre-warm
   the tool microVM or dispatch the query concurrently with the final decode tokens. If the model
   veers away, the branch aborts with zero rollback.
2. **`DraftOnly`** (drafts, staged patches, proposed invites) -> **burstable scheduling with
   zero-compensation preemption.** These can occupy burstable slack and be reclaimed the moment
   high-priority work arrives, with no saga. The reclaim primitive differs by pool, and §1 is the
   reason: in host DDR the orchestrator still evicts, so a draft's `Snapshot` cell is taken
   directly; inside an engine it does not, so the only lever is to **cancel the request** and
   requeue it, which frees its blocks at the next step boundary. Same policy, two mechanisms, and
   the second is why §2 keeps the path.
3. **`SideEffecting`** (transactions, mutations, webhooks, deployments) -> **strictly
   non-speculative.** Durable checkpoint before dispatch, non-revocable leases so execution cannot
   be torn down mid-flight.
4. **Human-approved** (unbounded pauses) -> demote the whole execution context out of HBM and host
   DDR into cold storage until the approval callback arrives.

### Generator-side truth, scheduler-side inference

**The taxonomy exists twice, and telemetry is the only bridge.** The workload generator uses the
full taxonomy as ground truth to synthesise traces. The scheduler never sees the class; it infers a
profile from observables. The one deliberate exception is `slo`, which is declared rather than
inferred -- and marking it as such is the point, since a field the caller supplies is not evidence
that inference works. Then classification accuracy, and the cost of getting it wrong, become
measurable -- what `class_aware` plus `ToolGapIndex` do in `sched_lm`, and what polyproto cannot do
while its `Request` carries the truth. This is what turns `taxo.md` into the experiment's
independent variable. The same split applies to tenancy (§3.8).

### Per-pattern coupling is the falsifier

Run coupled % per taxonomy cell on both axes. The output is a two-column table saying, for each
pattern, whether a unified orchestrator can help at all.

Expected shape, stated in advance so it can be wrong: batch inference and one-shot generation show
near-zero coupling on both axes (independent requests, nothing to co-decide); multi-agent and
long-running agents show high locality coupling (shared context, cross-node dataflow, atomic
admission) and moderate-to-high memory coupling on the host. If that fails, the thesis is narrower
than claimed and this document should say so.

---

## 5. Emergent properties

An advantage is *emergent* if no silo can produce it independently and it is not merely a hint
away. Five are simulated; two are now measured end to end (`phase-8.md`); five are proposed.

**On the first five:** simulated, not measured, on a model since found wrong in more than one way.
Treat each as what is expected to survive re-running rather than what has been re-run, and as
directional rather than sized. The properties worth most are the ones whose *existence* does not
depend on a constant.

**Within host DDR (orchestrator-budgeted memory).** Budgeted, not owned: the orchestrator sizes the
pool and sets its floors, but one occupant it arbitrates -- the engine's offload tier -- is
engine-owned. That is why these survive the correction. The decision measured is a **budget**, and
the budget stays the orchestrator's whoever fills it.

1. **A cross-class DDR shadow price.** One `marginal_price` balances microVM warm pools against
   offload tiers and local services. Fixed budgets cost 4.0% service and 2.7pp goodput; soft floors
   beat hard cgroup partitions by 32%. Multi-GPU hosts rarely co-locate heavy non-AI services, but
   soft floors excel where tool cells compete directly with host-side KV offload.
2. **Placement options static partitioning forecloses.** With soft floors the score ships 90% of
   tool calls to idle host DDR for a 53% warm rate; with hard pools that slice is capped however
   much memory sits free beside it and the warm rate halves to 19%. This concerns `Snapshot` cells
   -- orchestrator-owned -- so it does not depend on HBM authority.

**Across topology.**

3. **Congestion and residency in one argmin.** Neither an inference router nor a FaaS control plane
   can price "place the tool call near the active GPU context unless the link is congested or local
   memory is full". Measured consequence: the tool-placement decision **inverts** between
   unpressured and memory-bound regimes, and again at region distance.
4. **Cross-workload atomic admission.** All-or-nothing placement of a fan-out across nodes is not
   expressible per request. Where it binds: +22% fan-outs completed, inference stall -10%.
5. **One currency for host hints.** A prewarm, a retention directive and an eviction priced in the
   same host DDR units can be traded against each other. A siloed hint is advisory and unpriced.

**Measured end to end (§2, `phase-8.md`).**

6. **One data path serving two denominators.** Control-plane overhead is a fraction set by the work
   being scheduled; the **crossover** is its durable form (§2.3), now run as an arm rather than
   computed by hand: 0.83-0.90 ms / 4.34-4.71 ms with the sidecar's stream kept open, 1.38-1.43 ms /
   7.18-7.43 ms on Envoy's per-request default. An inference-only stack buys a proxy out of the
   decode budget; a FaaS control plane cannot. **Only a unified orchestrator is forced to pick one
   path for both**, which makes "integrate, do not proxy" a consequence of unification rather than a
   preference.
7. **Extension cost as an expressiveness bound, and a fleet-size ceiling with a number on it.** A
   hook at 36-63 us (measured, `ext_proc`) must be a constant attached to the request; at 0-180 ns
   (native through ring) it can be a function of each candidate inside the argmin. The same ladder
   prices trust. Run as an arm, this bound turns out to be quadratic in fleet size for a
   per-candidate hook -- `phase-8.md` §1.4 -- and the ceiling is measured, not asserted: at this
   workload's decision rate (`d = 1.23`, measured), one unsharded scheduler scoring `ext_proc`
   callouts saturates between **19 and 20 nodes**; scoring a warm `Wasm` hook, **~1000**. No silo
   needs this ordering, because no silo is simultaneously a scheduler and a data plane.

**Proposed, and the reason to do §3 and §4.**

8. **Learned cross-class retention.** "This agent returns to this tool in ~800 ms, confidence 0.7"
   driving a FaaS warm-cell retention decision priced against what holding it displaces. A silo can
   receive that as a hint; it cannot weigh it.
9. **Authority-driven speculative scheduling.** Pre-executing `ReadOnly` tool calls concurrently
   with decode, and scheduling `DraftOnly` work into burstable capacity with zero-compensation
   preemption -- reclaimed by eviction where the orchestrator still owns the pool and by
   cancellation where the engine does (§4). Both require knowing the authority class, which is a
   property of the *workload*, not of any one runtime.
10. **Joint prefill/decode pairing and ratio.** Disaggregation as a two-member gang with a
    direction, plus the fleet ratio behind it (§2.5). A sidecar picks a prefiller from a list.
11. **Tenant fairness across a tenant-blind engine.** An engine evicts LRU and cannot see tenants,
    and a partition is physically an engine -- so isolation is paid in a duplicated copy of the
    weights as well as in lost prefix sharing and batch width (§3.8). That makes it a *capacity*
    decision, not a policy toggle, and only a component that both sizes partitions and routes into
    them can make the trade deliberately: shared where prefixes overlap and load shapes are
    compatible, separated where one tenant is bursty enough to evict the others and the HBM exists
    to pay for it. A siloed router can only pick one side in advance.
12. **Two-dimensional coupling as a published quantity.** Not an advantage but the measure of one.

Plainly: the largest defensible effects are **topological dataflow co-placement**,
**macro-orchestration of weights, partitions and gangs**, and **targeted host DDR multiplexing** --
not cross-hardware arbitration of HBM bytes. Once we stop pretending the orchestrator allocates KV
blocks, what remains is a system solving three problems existing stacks fail at: joint dataflow
placement across network boundaries, macro capacity and gang coordination, and authority-aware
speculative execution. Phases 3 and 7 test whether that holds.

---

## 6. Reading `sched_lm`

### Worth importing

- **The argmin *is* the decision.** `cost.py` computes `wait + transfer + prefill(missing)` and
  takes the minimum; the policy is the cost model, not a heuristic layered over one. Polyproto
  arrived at the same shape independently in `Machine::plan`.
- **Regret on the policy's own state.** Separating routing quality from cache-state quality is why
  its comparisons need no tuned baseline.
- **`RequestView`.** Refusing the policy access to ground truth, by type.
- **Coupled %.** The best single metric for the unified question.
- **Learning from timing metadata alone.** `ToolGapIndex` needs no new instrumentation, which is
  what makes it adoptable.
- **Deadline-scoped directives.** `ttl` and `scope` rather than an unbounded priority bump.
- **Using residual regret to justify infrastructure before it is built.** The transfer regime is not
  expressible in stock llm-d, so the sim's job is to show the regret that would justify a KV
  connector tier. That is the correct use of a simulator and the posture this prototype should copy
  -- for the shared L2 tier (§3.10) and for a request-path ring (§2.2).
- **A promotion path.** Promotion tier 0 config / tier 1 plugin / tier 2 serving-stack change, so a
  finding has somewhere to go.

### Where a unified system needs more

- **It is inference-only.** One workload class in the cost model: no warm pool, no service replicas,
  no cross-class contention, so the effects dominating every result here are invisible to it.
- **One pool per node.** No HBM/DDR split and no offload tier priced as a recovery path, so eviction
  cannot be priced as `min(rebuild, promote, fetch)`. That mispricing was worth two orders of
  magnitude here before it was fixed.
- **No refusal in the argmin.** Load shed is tallied in the live stack; the offline cost model never
  declines a request, so admission is not part of the decision.
- **Unpriced retention.** A directive is per-request and advisory, with no global shadow price to
  weigh a pin against what it evicts.
- **No atomic multi-worker admission.** Nothing expresses a fan-out that must be placed whole.
- **TTFT-only objective.** The transferable warning, learned the hard way here: once decode cost
  depends on batch occupancy, **placement moves execution and not just waiting**, and a wait-only
  metric misranks policies. Polyproto's headline had to move from stall to end-to-end service time
  for exactly this reason, and stall is TTFT's analogue. Adding goodput alongside catches the
  related trap where an arm looks fast because it refused the expensive work.

### The naming collision

`sched_lm` uses Tier 0/1/2 for *promotion effort into production llm-d*; polyproto uses Tier 1/2 for
*information-sharing versus joint decisions*. Same words, orthogonal axes. **This document qualifies
every use**: *promotion tier* for effort-to-ship, *coupling tier* for how tightly a decision is
joined. The two meet inside §2 alone -- a shared ring is promotion tier 2, P/D pairing is coupling
tier 2 -- which is what makes the qualification a rule rather than a suggestion.

---

## 7. Constants are a method, not a debt

This prototype is for understanding architecture and tradeoffs, so modelled constants are
legitimate. What makes them legitimate is **sensitivity, not precision**:

> For every constant, know which conclusions are robust to it across its plausible range, and which
> flip. Publish the ones that flip.

This has bitten three times.

**A wrong value.** The FaaS snapshot cost was a cold-container constant (200 ms / 32 MiB, linear);
replacing it with a Firecracker lazy-restore model (~9 ms, roughly flat in image size) **inverted
the ns/byte ordering** and reversed a headline about which bytes are cheapest. The conclusion
depended on it entirely.

**The same constant, still untested.** The restore model is `4 ms + bytes * 0.15 * 1.0 ns/byte` for
UFFD page-in -- but `UFFDIO_COPY` (a major fault that copies) and `UFFDIO_CONTINUE` (a minor fault
mapping a page already in the page cache) differ by enough to move that slope several-fold, and work
restoring snapshots through hardware decompression or overlay VMAs reports 3.7-4.3x cold-start
improvements over the naive path. Since §5's second property is an *ordering* claim, and orderings
are worth only the gap between the constants producing them, any result ranking snapshot bytes
against KV bytes must be published across that range.

**A wrong label, which is worse.** `exec_ns` is chosen -- `FAAS_EXEC_MIN_NS + rng.below(SPAN)` --
and `residency-ledger.md` says so under *Modelled, not measured*. Its denominator table nonetheless
prints "warm FaaS invocation (**measured**: 129 us)", and §2 built an argument on that label before
checking. A wrong constant gets sensitivity-tested; one wearing the word *measured* is exempted
from the test by its own label. Hence: **provenance is part of the number.** Any figure quoted here
names where it came from, or it is not quoted.

So the rule for this document's proposals: a result may rest on constants, but it must come with the
range over which it holds. Where a crossover is the finding -- KV ships within a rack and is rebuilt
across a zone; the data path binds below a millisecond -- the crossover's *position* moves with the
constants while its *existence* does not, and the existence is the claim. Where an ordering is the
finding, it is worth only the gap between the constants producing it.

---

## 8. Feasibility

### Is the correction tractable?

Yes, more cheaply than the size of the claim suggests -- though not because the ownership boundary
"coincides with a partition the code already has". It does not. **`accelerated()` is a tier
predicate, not an ownership one**, and §1's table cuts across it twice.

The useful half survives: `accelerated()` still *locates* nearly every site that has to change,
because a site asking which pool a class lives in is about to assume something about who controls
it. The two exceptions are narrow and named, and naming them is what Phase 1's type is for -- the
compiler still generates the work list, it just needs a two-axis predicate to generate it correctly.

| area | what changes | scale |
|---|---|---|
| `cache.rs` | HBM `TierPool` splits into an orchestrator-sized partition and an engine-cache *model* inside it; `Snapshot` and `ServiceHeap` keep current semantics, offloaded KV does not | **measured: 12/12 census-marked allocation entry points** (`phase-1.md`). Still the whole of it -- and the dynamic split says **admission is under a third** of what the ledger does on the engine's behalf, with demotion, cascade spill and superseded-copy removal making up the rest, so the offload and promote paths are the bulk of the rewrite rather than a detail of it |
| `machine.rs` | residency reads go through a belief; `could_admit` for KV coarsens; displacement for KV becomes an estimate; the acquire term becomes an expectation over `P(resident)` scored at a per-class quantile (§3.7) | **measured: 0/8** -- every read here is a query (`Telemetry`, `ground_truth_holds`), not an allocation decision, so Phase 1's census found nothing to mark. "Moderate, mechanical" still describes the *edits* Phase 3/4 make to these query sites; it no longer describes a share of a disclaimed *authority*, since this file never held any |
| `main.rs` | new arms (engine honours / ignores directives; precise / approximate index; data path) | additive |
| `work.rs` | unaffected by ownership; changes for §3.2 and §4 | none for this correction |
| `engine.rs` | gains the cache alongside the batch model -- same component | small |
| `boundary.rs` | two new rungs, an `ext_proc`-shaped callout and a WASM invocation (§2.7) | small, independent of everything else |

### What gets harder

Worst first.

1. **Per-block admission control over inference state disappears.** An engine evicts and recomputes;
   it does not refuse for lack of KV. `Admission::Pending` stops being reachable for an individual
   `KvBlock` or `WeightShard`. What it does *not* lose is the partition: the orchestrator sized it,
   so its capacity stays an owned fact and a byte- or token-depth check against it stays
   authoritative. The loss is **granularity, not authority**. Three things lean on the granularity:
   - `Hierarchy::could_admit`, hence **gang feasibility**. A fan-out can no longer be refused block
     by block, only against the partition budget, host memory and engine slots -- so the primitive
     survives on a coarser, weaker test, and the +22% needs re-earning on it.
   - `can_satisfy`, hence the **downstream-aware gate**, which becomes a partition-budget,
     queue-depth and host-memory judgement.
   - **Goodput as an outcome.** Refusals for inference move to the router, so the numbers change
     shape even where they do not change size.
2. **Tenant fairness on KV goes with it** (§3.8). LRU is tenant-blind, so a noisy neighbour on a
   shared partition is unpreventable, and isolation costs a partition -- which is an engine, which
   is a second copy of the weights. The only cheaper answer is an upstream one: per-tenant eviction
   floors are bookkeeping over block hashes, not model internals, so they are askable at promotion
   tier 2 once there is a regret number to ask with. This consequence was invisible until the
   tenancy model was stated.
3. **Displacement becomes an externality, not a decision.** Routing still *causes* the engine to
   evict; the orchestrator does not choose what. The term stays in the score as an estimate from
   observed eviction pressure -- noisier and lagged. §3.7 covers that only partly: the quantile a
   class is scored at applies to the whole predictive cost, but the turnover estimator behind
   `P(resident)` prices *this* node's belief going stale, not what dispatching here does to someone
   else's warm prefix. Displacement's uncertainty has no estimator yet, and saying so beats implying
   §3.7 absorbs it.
4. **Two sources of truth, permanently.** What the engine holds and what the router believes drift.
   Not a defect to engineer away; it is the architecture, and the drift is a metric (§3.6).
5. **The cross-class arbitration thesis may not survive.** If the arbitration effects vanish once
   the engine allocates its own memory, what remains is a router with good cost accounting, and the
   honest report is that the ledger's value was an artifact of assumed authority. Phase 3 can return
   that answer. (Called *that*, not "the unified memory thesis": in this project "unified memory"
   means the **hardware** -- Apple silicon's single pool against the datacenter's split -- and two
   meanings for one phrase is how a hardware caveat and an architectural thesis get read as each
   other.)
6. **Owning the path means owning stream semantics -- not HTTP.** The framing, parsing and TLS are
   a linked library's (§2.6), which removes the CVE-surface half of this item and most of the
   engineering. What does not delegate is what to *do* with a stream: turn a client cancel into an
   engine abort, decide where a stalled stream's tokens accumulate, and choose retry and hedge
   against a backend holding warm state. It threatens no result here -- the simulator charges seam
   costs from a measured ladder and never parses a byte of HTTP -- but it remains the largest gap
   between this design being right and being shipped, and one piece of it has a modelling
   consequence: **a stalled stream's buffer is an occupant of the host DDR pool §5's shadow price
   arbitrates**, and that term does not exist in the ledger.

### What gets easier

- **The engine cache is simpler than a `TierPool`.** LRU over block hashes: no quotas, bands,
  floors or refusal. Modelling vLLM means modelling *less* policy, not more.
- **Model agnosticism becomes checkable.** Once the orchestrator cannot see inside the engine, the
  interface it consumes is small enough to write down: capacity, hit/miss/eviction counts, queue
  depth, load and unload cost per model, declared context window, a directive channel, and
  **request cancellation**. Anything beyond that list is a model-specific dependency, and the
  compiler enforces the list.

  Cancellation is listed apart from the directive channel because it is the one entry that is not
  advisory. A retention directive the engine ignores costs a worse placement, and §3.6 counts it; an
  abort the engine ignores costs the priority model its only enforcement (§2.3). Both are
  model-agnostic -- neither needs to know what a block contains -- but an engine that cannot be
  asked to stop is one this design cannot schedule priorities on.

  Context window earns its place because it is *declared metadata*, like size and load time, and
  Phase 6's heterogeneous fleet needs it: dispatching a 200k-token prompt to a 32k model produces a
  failure and a retry costing more than the placement saved. It also marks where the list stops.
  Routing by **model accuracy** needs per-model, per-workload evaluation, which is model-specific by
  definition and is what §1 gave up KV ownership to avoid. A capability *gate* is in scope; a
  quality *ranking* is not, and the test is whether the engine can state the fact about itself.
- **Weights become orchestration rather than caching** (Phase 6) -- more realistic, and a capability
  no arm has today.

### Security: what the architecture answers, what a prototype defers

This is a prototype, so production hardening is out of scope. What is worth recording is which
threats are **architectural** -- they would change a decision here -- and which are operational.
Reviewing the standard Kubernetes vulnerability classes, most are architectural and already
answered, which is evidence for decisions taken on other grounds.

| threat class | k8s precedent | status here |
|---|---|---|
| annotation / header injection into a proxy config | Ingress-NGINX RCE via unsanitised annotations templated into `nginx.conf` | **structurally absent.** The vector is *config templating*; §2 has no templating step, only typed in-process values |
| admission-webhook SSRF and redirect following | api-server following redirects into internal networks | **not applicable.** The webhook callout is the `ext_proc` shape §2 removes |
| untrusted workload escaping its sandbox | `runc` fd leaks, overlayfs and page-cache bugs | **already the thesis.** `README.md` makes microVMs the native abstraction because containers isolate poorly; §2.6 tiers extensions by trust |
| unauthenticated peer data channels | endpoint and ExternalIP hijacking | **deferred**, and the only one |

One class is new since §2 chose to link a proxy core rather than write one: the dependency's CVE
stream. It is **operational, not architectural** -- patching is a redeploy rather than a config
change, it is the same exposure an Envoy deployment already carries, and it changes no decision
here, which is why it is a sentence and not a row.

One accidental benefit, from a decision made on other grounds: §1 chose **ZMQ over Unix domain
sockets**. A UDS has no network surface -- it is scoped by filesystem permissions and unreachable
off-node -- so the telemetry channel needs no transport authentication to be un-spoofable
remotely. `tcp://` would have needed mTLS to reach the same place.

The tenancy model is **soft** multi-tenancy (§3.8), which moves the question from confidentiality to
**fairness** -- a scheduling problem, answered in §3.8, not here. Two residuals tracked rather than
fixed:

- **Cross-node transport is unauthenticated.** NIXL/RDMA peer pulls and cross-node control traffic
  assume a trusted network. Under soft tenancy that is defensible for a prototype -- the threat is a
  compromised node, not a hostile tenant -- but it is the boundary that needs workload-bound
  identity first, and adding it changes a measured quantity: a handshake and per-message
  authentication land on the P2P path §2.5 keeps out of the data plane precisely because it is
  cheap.
- **Prefix sharing is a timing channel between tenants.** A cache hit is observable as faster TTFT,
  so one team can in principle detect that another sent a given prefix. Under soft tenancy that is
  an accepted and favourable trade -- cross-tenant sharing of system prompts is the highest-value
  hit in this workload. It stops being acceptable for a regulated subset, which wants an opt-out
  rather than a global policy: a cache scope on the request, of the shape RFC-0001's
  `scope=<session>` already has (§3.3). Cheap, and worth adding before a result depends on sharing
  being universal.

---

## 9. Phases

Each phase compiles, runs, and ends with a number. The measurement apparatus comes before the thing
it measures, so the phase that may invalidate the thesis is not also the phase that invents its own
scoring.

### Phase 0 -- Price the seams

Implementation plan: [`phase-0.md`](phase-0.md), which states its predictions before the run.

Extend `boundary.rs` with the two rungs the ladder is missing: an **`ext_proc`-shaped callout**
(gRPC with header-mutation semantics, not bare unary) and a **WASM invocation** through a real
sandbox. Publish the per-decision cost of a policy hook at each isolation level.

- **Deliverable:** the price of an extension boundary, measured rather than asserted -- and the
  first honest statement of the README's zero-cost-extension claim, which today's ladder supports
  only by comparing gRPC unary against a native call.
- **Risk:** none. It touches nothing else and can return "the gap is smaller than claimed", which is
  a result.
- **Size:** small. The cheapest number here and the only one available before any correction lands.

### Phase 1 -- Name the memory boundary in types

Implementation plan: [`phase-1.md`](phase-1.md), which states its predictions before the run.

The *memory* boundary; §2's path boundary is Phases 0 and 8, and they are independent.

Introduce an ownership predicate, route every ledger access through it, and preserve today's
semantics exactly. Engine-owned state becomes reachable only through one narrow interface -- the
`Telemetry` type of §3.1.

The shape matters, and the obvious shape is wrong. `Ownership { Orchestrator, Engine }` keyed on
`BlobKind` cannot express §1's table: the orchestrator sizes an HBM partition while the engine
allocates the `KvBlock`s inside it, and an engine-owned offload tier sits inside host DDR's quota.
Ownership is a function of **(kind, tier)** and of **which question is asked** -- *capacity* or
*allocation* authority, §1's macro and micro tiers named as something an enum can hold. Getting it
wrong here is what would leave Phase 3 ambiguous about what changes hands.

- **Deliverable:** results byte-identical to today, plus a count of call sites assuming *allocation*
  authority over engine state. That count *is* the feasibility answer, generated by the compiler.
  **Measured** (`phase-1.md` §2, §5): byte-identity confirmed on every reproducible command; the
  static census is **12**, all in `cache.rs`, none in `machine.rs` -- the scheduler turned out to
  hold no allocation authority to disclaim in the first place, only residency and cost *queries*,
  which is a stronger form of "moderate, mechanical" than §8 asserted. The dynamic census (§4.4) is
  four to five orders of magnitude larger, and its split by entry point says **admission is under a
  third** of what the ledger does on the engine's behalf -- so Phase 3's engine-cache model has to
  cover the offload and promote paths, not just admission.
- **Risk:** none to results; a no-op by construction. **Size:** small.

### Phase 2 -- Oracle, regret, coupling

Implementation plan: [`phase-2.md`](phase-2.md), which states its predictions before the run.

The routing oracle, a clairvoyant eviction baseline, per-request regret on the policy's own state,
the wait regime (§3.5), per-request tracing spans (§3.11), and coupled % **on both axes §3.4
defines**. No architectural change.

A single blended coupling figure would average an orchestrator-owned pool against an engine-owned
one and hide which is carrying the result -- which is exactly what Phase 3 changes.

- **Deliverable:** every existing comparison restated as regret against an oracle instead of a delta
  against a hand-built baseline, retiring the fairness caveat on every number so far.
- **Risk:** the oracle may reveal the scored policy's margin is mostly baseline weakness. Better
  found here than after Phase 3 muddies it. **Size:** medium.

**Status: implemented and measured.** `phase-2.md` §2's six predictions, checked against `distributed
--regret` and `residency`/`flows`/`volatility --clairvoyant`:

- **P1** (scored's heuristic gap is the affinity tie-break, confined and small): **confirmed.** The
  `scored` arm's `execution`, `heuristic` and `belief` gaps are all exactly zero; `scored, no flows`
  -- which never falls back to affinity -- carries a tiny nonzero heuristic gap instead. The scored
  arm's entire regret is model gap, as predicted.
- **P2** (myopic regret under-credits a policy whose value is the trajectory): **confirmed.**
  `hash only`, `residency only` and `flow only` all carry heuristic regret in the tens of millions of
  ns/decision while matching or beating `scored` on end-to-end service time -- the large-regret,
  small-deficit signature P2 named in advance.
- **P3** (`R(p) == charged(p)` exactly, non-gang, non-saturated): **confirmed under ample capacity,
  with a scoped exception.** Exact on the fixture every regret test uses. Under a deliberately tight
  pool a bounded residual appears (under 20% of spans in the adversarial fixture, isolated to
  `execution` alone -- the other three gaps stay exactly zero wherever it fires): `plan` prices a
  request's chain and its dependencies independently against one snapshot, while `run_here`
  materialises them in sequence, so real eviction pressure lets one side's admission invalidate the
  other's price. Not belief staleness -- both reads see the same, current, true state -- and left
  alone per rule 1 rather than patched mid-phase; `machine.rs`'s test pins the boundary.
- **P4** (clairvoyant may lose on cost): **confirmed, the losing branch.** At `residency`'s defaults,
  clairvoyant eviction buys +16.2pp `KvBlock` hit rate over soft-floor and costs +96.1% stall/req.
  The diagnostic did its job: eviction quality is not what limits GDSF here, cost-weighting is.
- **P5** (coupled % is low at the published defaults, higher where memory binds): **half-confirmed,
  and the surprising half is the finding.** Locality coupling is low (1-2% of scored decisions) as
  predicted. Memory coupling is **not** near zero at `distributed`'s defaults -- 44-55% of host-DDR
  evictions are cross-class for several arms, because real DDR pressure exists there that the
  `residency-ledger.md` tables (measured on an older engine model) did not show. The prediction was
  wrong about the regime, not about the mechanism: coupled % tracks pressure exactly as designed, and
  `distributed`'s defaults turn out to have more of it than assumed.
- **P6** (the oracle cannot see the falsification that already failed): **confirmed.** Re-running the
  flow-payload sweep (§4.7's knob, region distance) through the instrument: at 512 MiB, `flow only`'s
  heuristic regret is far larger than `scored`'s (which stays exactly zero throughout, per P1) while
  `flow only`'s service time is *lower* -- 534.5 ms against `scored`'s 537.3 ms. The metric's own
  blind spot, reproduced on demand rather than argued from the retracted numbers.

### Phase 3 -- The engine allocates; the orchestrator sizes the partition

The HBM pool splits in two rather than changing hands, along §1's macro/micro seam. The
orchestrator keeps **capacity authority** -- how many bytes a replica gets -- and hands
**allocation authority** to the engine: LRU over block hashes, bounded by the partition, **never
refuses**, reports metrics. The orchestrator routes and observes.

Three scope lines, because "the engine owns its cache" is too coarse to build from:

- **`KvBlock` only.** `WeightShard` is also HBM-resident and engine-owned once loaded, but its
  interesting decision is *placement*, on a slow timescale, and that is Phase 6. Weights keep
  today's semantics so two corrections do not land in one measurement.
- **The partition is a fixed input**, sized from config. Making it a decision is Phase 6.
- **Host DDR is not a synonym for orchestrator-owned.** `Snapshot` and `ServiceHeap` keep every
  current mechanism; **offloaded KV does not**, since `cache.rs` already models HBM eviction as
  offload into DDR's quota. An engine-owned class sits inside the pool the DDR shadow price
  arbitrates.

Also lands the shared L2 term in the acquire argmin (§3.10), since this phase already rewrites that
path.

- **Deliverable:** the price of the boundary. Re-run each contaminated result from §1 against the
  expectations tabulated there. Include the admission sweep §1 calls for -- refuse on `max_tokens`,
  on an output-length estimate, or on neither -- since that is what tests the claim that
  partition-level admission stays authoritative. Report it **per class and at p99**: §1's asymmetry
  is that an optimistic admission moves cost onto traffic that did not cause it, and a mean cannot
  see that.
- **Risk:** the headline. Gang feasibility and the gate lose their per-block test and must be
  re-grounded. **Size:** large -- the core of the correction.

### Phase 4 -- Belief, not truth: lossy telemetry and risk-adjusted scoring

Two halves, in order.

**First, the belief becomes a probability** (§3.7): `E[acquire] = P(resident) * cost_hit +
(1 - P(resident)) * cost_rebuild`, with `P(resident)` estimated from reported turnover and
extrapolated forward across sequence gaps, and each class scored on the quantile its SLO names.
Without it the second half measures nothing -- a loss sweep against a score that cannot react to
loss returns the same answer at every rate, and today's mean over an exact belief is the control arm
rather than the design.

**Then, belief replaces truth.** Split what the engine holds from what the router thinks it holds.
Ingest step-aligned micro-batches over ZMQ IPC at forward-step cadence. Retire `Control::Gossip`,
which models a stale *exact* view no telemetry provides. Add sequence-gap detection, reconciliation
sync batches, and divergence reporting (§3.6).

What the simulator models here is **cadence and loss, not transport cost** -- §1 settled that the
crossing is free at 40-100 Hz whichever boundary carries it.

- **Deliverable:** what routing quality costs when residency is a lossy belief. Sweep loss (0/1/5%
  dropped batches) **against the scoring quantile**, publish `P(resident)` against realised hit rate
  as a calibration curve, and report whether a probabilistic score recovers what the lossy belief
  costs -- including the failure mode §3.7 names, where a mean over an exact belief herds onto the
  node that went quiet. Watch for flapping at the quantile threshold.
- **Risk:** low; contained, and replaces a mechanism known to be unphysical. **Size:** medium.

### Phase 5 -- Influence: retention directives

Emit RFC-0001-shaped directives on the request path, with two engine arms: honours, and ignores.
Directives are advisory by construction -- §1 removed any ability to hold a block against the
engine's will -- so the `ignores` arm is not a pessimistic sweep, it is §3.6's third divergence
source turned into an experiment.

- **Deliverable:** what a directive is worth, and what routing alone achieves when the serving stack
  does not cooperate. The second number is the one that matters for planning, and the first
  coupling-tier-1 result earned by influence rather than assumed by declaration.
- **Risk:** low. **Size:** small to medium.

### Phase 6 -- Macro authority: placement, partitions, tenancy

The slow, coarse, orchestrator-owned decisions -- §2.4's macro tier, and between them everything
Phase 3 froze.

Weight shards stop being per-request cache entries: the orchestrator decides which models load
where on a slow timescale, with load and unload costs, through a model-agnostic interface (size,
load time, context window, no internals). **HBM partition sizing stops being Phase 3's fixed
input**, since a node's partition budget and its resident model set are one allocation made twice.
The **P:D replica ratio** (§2.5) is the third member, bound by different resources at each end, so
the ratio serving a long-prompt mix starves an agent mix.

**Tenancy constrains all three** (§3.8). Because engine eviction is tenant-blind, how many
partitions exist and who shares one *is* the fairness policy -- and because a partition is
physically an engine, each extra one spends HBM on another copy of the weights. That is why
isolation and capacity are decided in one act here rather than as separate knobs. It also adds the
missing per-tenant axis to `Quota`, which is today per-class only.

- **Deliverable:** a result no arm can produce today -- a heterogeneous fleet serving several model
  types under a shifting request mix -- plus a tenancy arm: shared partitions against per-tenant
  partitions under a bursty neighbour, reporting isolation's cost on all three prices §3.8 names --
  cross-tenant prefix hits, batch width, and the HBM spent duplicating the weights. The third is
  what makes this a capacity result rather than a policy toggle.
- **Risk:** needs a workload with a realistic model mix, which `taxo.md` supplies. **Size:** medium.

### Phase 7 -- Learned flows, speculative authority, and the taxonomy

Predicted flows replacing declared ones (§3.2), a tool-gap estimator, taxonomy presets, the RAG
class, durable retention, and authority-driven speculative scheduling (`ReadOnly` pre-execution,
`DraftOnly` burst preemption, non-preemptible `SideEffecting` leases), then per-pattern coupled %.

- **Deliverable:** what the coupling-tier-1 win is worth against estimates rather than oracles; the
  latency and goodput delta from speculative scheduling; and a table saying for which workload
  patterns a unified orchestrator can help at all.
- **Risk:** the coupling table may show the advantage confined to a few cells. That is a result.
  **Size:** large, separable into increments.

### Phase 8 -- The data path as an arm

Implementation plan: [`phase-8.md`](phase-8.md), which states its predictions before the run.

`data_path: { Sidecar, Integrated }`, charging Phase 0's seam costs per request across the workload
mix, with the sidecar arm paying an `ext_proc` callout per placement and a loopback hop per
dispatch. The plan splits the sidecar in two -- one callout per placement, which is llm-d as
deployed, and one per candidate, which is what extending its policy would cost -- because charging
the second to the first overstates the deployed tax by the candidate count.

- **Deliverable:** the **crossover** -- the service time at which control-plane overhead passes 5%
  and 1% of a request -- computed in closed form from the arm's realized tax rather than read off
  `exec_ns` (§2.7). Measured: 0.83-0.90 ms / 4.34-4.71 ms with the sidecar's stream kept open,
  1.38-1.43 ms / 7.18-7.43 ms on Envoy's per-request default. Per-class figures illustrate where
  each class sits on the curve; they are not the result, because those service times are chosen
  constants.
- **Risk:** falsifiable in the direction that matters, and did not fall the falsifying way: the
  crossover sits within a factor of 2 of a millisecond rather than an order of magnitude below it,
  so it is not the case that almost nothing real is beneath it.
- **Size:** small given Phase 0, confirmed in the build: `Machine::decide` and `run_here` already
  charged a configurable boundary and scaled by candidate count, so the decision and dispatch seams
  were new arms, not new mechanism -- `machine.rs`, `main.rs`, and one test.

### Ordering

Two independent chains.

**The memory chain: 1 -> 2 -> 3 -> 4 -> 5 -> 6.** Phase 1 makes the boundary visible and
specifically gates Phase 3 -- a one-axis ownership type makes the split unbuildable in the two
places the boundary cuts *within* a pool. Phase 2 makes measurement fair before Phase 3 changes what
is measured. Phase 3 is the correction and the decision point. Phases 4 and 5 are two of the three
channels of the engine interface, observe then influence; the third is cancellation, which is
authoritative rather than advisory (§8) and arrives with the data path rather than with the memory
chain. Phase 6 adds the capability the corrected architecture makes central and unfreezes what
Phase 3 held fixed, which is why it follows rather than precedes.

**The path chain: 0 -> 8.** Phase 0 needs no simulator change, blocks nothing, and is the only phase
that could be finished today. Phase 8 depends on it and nothing else. So the data-path question is
answerable in parallel with the ownership correction rather than queued behind it.

Phase 7 is orthogonal to both and can run alongside 4 through 6.

Phases 0, 1, 2, 4, 5 and 8 are low-risk and additive. **Phase 3 is the one that can return a
negative verdict on the project's central claim**, which is why it comes early and why Phase 2
precedes it.
