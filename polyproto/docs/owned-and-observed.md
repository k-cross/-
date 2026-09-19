# Owned, Inferred, Observed

Design notes for the next step: giving the orchestrator a telemetry boundary, making the
workload taxonomy a scheduler input, and stating which of its advantages are emergent rather
than assumed.

Status: design only. Nothing here is built. Numbers cited as measured come from the runs
recorded in [`residency-ledger.md`](residency-ledger.md); everything else is a proposal.

Two inputs shape this document. [`taxo.md`](taxo.md) is the workload taxonomy it has to serve.
[`k-cross/sched_lm`](https://github.com/k-cross/sched_lm) is a sibling prototype that reaches the
same central decision from the llm-d/vLLM side; it is read here as **inspiration and evidence,
not direction** -- what it got right, where a unified system needs more, and which of its
methods are worth importing wholesale.

---

## 1. The boundary

### Three categories, not two

The obvious split is *what the orchestrator tracks* against *what stays external telemetry*.
One category is missing from that, and it is the one where every observability-driven behaviour
actually lives:

| | authority | freshness | if it is wrong |
|---|---|---|---|
| **Owned** | the orchestrator decided it; nothing else can be the source of truth | exact, per-request | the system is **invalid** -- overcommitted, double-admitted, a gang half-placed |
| **Inferred** | an estimate the orchestrator maintains in-process from observation | fresh, explicitly uncertain | a decision is **worse**, and the error is self-correcting if it carries a deadline |
| **Observed** | another system owns the fact | fresh (in-band/streamed) or sampled (metrics) | a placement is **suboptimal** or diagnosis is blurrier; no safety invariant breaks |

`ToolGapIndex` in `sched_lm` is the middle category exactly: not ground truth, not external, but
orchestrator-resident and uncertain. Collapsing it into either neighbour is what produces the
two failure modes to avoid -- treating an estimate as authoritative (a stale residency view that
forces an illegal placement), or treating an estimate as merely advisory (a learned re-arrival
gap that never changes an eviction).

### Tests for ownership

Something must be **owned** if any of these hold:

1. **Authority.** The orchestrator decided it, so no external system can report it back. Every
   admission, eviction, placement, retention directive, and gang membership is in this class by
   construction.
2. **Correctness dependency.** A wrong value makes an outcome *invalid* rather than slow.
   "Is this gang fully admitted", "is this replica still serving", "does durable state still
   have a home" are correctness questions. "Which node is hottest" is not.
3. **Hot-path rate.** It is consulted per request, orders of magnitude above any plausible
   export interval. Nothing read per request can come from a 15-second scrape.

Something should stay **observed** if all of these hold: another component owns the fact, it is
an aggregate or a derivable statistic, and losing it degrades explanation rather than decisions.

Anything else is **inferred**, and inferred state carries three obligations: a confidence, a
decay, and a deadline on any action it drives.

### Where polyproto's state falls

| state | category | where it is now |
|---|---|---|
| per-blob residency per pool | **owned** | `TierPool.entries` |
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
| workload class | **inferred** | **missing** -- see §3 |
| realised TTFT / ITL, batch occupancy | **observed** | modelled internally instead |
| engine prefix-cache hit rate | **observed** | **conflated** with owned residency |
| node health, utilisation, power | **observed** | absent |

Two entries in that table are already inferred and were built that way for good reasons.
`marginal_price` is a deliberate estimate because a faithful dry run costs as much as the
eviction it is pricing. `regret_rate` is measured from a bounded ghost list rather than assumed.
Both are precedents: the pattern works, and the engine already has somewhere to put this kind of
quantity.

Four entries are cheats, and they are load-bearing. `FlowHint.probability` is hardcoded `1.0`
(`work.rs:638`), so every cross-workload flow result rests on the scheduler being *told* the
future with certainty. The placement score reads `req.tokens` (`machine.rs:696-704`) -- the exact
output length, before decoding -- and both engine terms scale with it.

### The correction: the orchestrator does not own the KV cache

This is the most consequential section in the document, and an earlier draft of it was wrong.

`README.md` is explicit: *"AI inference (llm-d style but model-type agnostic for
routing/scheduling but does not replace inference engines like vLLM)."* **Model agnosticism
requires not controlling the engine.** An orchestrator that owns KV block allocation has to know
block layout, attention scheme, quantisation and paging behaviour -- it becomes an inference
engine, for one family of models, and the central goal is lost.

So there is no fork. The engine owns its memory and reports on it; the orchestrator **tracks**
it. An earlier draft framed this as a choice between owning the cache and owning a belief. Only
the second was ever available.

**What the code does today is the first, and that is the mistake to correct.** `TierPool` *is*
the KV cache: it admits, evicts by its own GDSF priority, refuses when full, and is the single
source of truth. Every memory-arbitration result rests on an authority the architecture has
disclaimed.

### Ownership is per class, and the seam already exists

The boundary is not global. It runs between workload classes, and `accelerated(kind)` -- already
in `cache.rs` to separate HBM from DDR -- draws it exactly:

| class / resource | who owns the bytes | what the orchestrator does |
|---|---|---|
| **HBM Partitions** | **the orchestrator** -- it provisions the engine | decide *how much HBM* an engine replica gets, scale replicas, and partition the hardware |
| `KvBlock` | **the engine** (vLLM's block manager) | observe an approximate index; influence via retention directives; **route** |
| `WeightShard` | **the engine**, once loaded | decide *which models load on which nodes*, and when to unload -- slow, coarse, and genuinely orchestration (Phase 6) |
| `Snapshot` | **the orchestrator** -- it starts and stops microVMs | own outright: admit, evict, refuse |
| `ServiceHeap` | **the orchestrator** -- it scales replicas | own outright |
| offloaded KV in host DDR | the engine's KV connector (`LMCache`, NIXL) | observe; ownership depends on the connector, so assume the engine's |

This establishes a **two-tiered control system**. The orchestrator retains macro-level authority over the hardware (provisioning, HBM partitioning, and model loading), but cedes micro-level, per-request authority (KV block eviction) to the engine.

Two consequences worth stating separately, because one is a loss and one is a save.

**The loss: no admission control over inference state.** The orchestrator cannot refuse a specific KV
admission, cannot choose an eviction victim, and cannot hold a block against the engine's will.
An engine under pressure evicts or preempts and recomputes; it does not reject a request for lack
of KV. So refusal, for inference, moves out of the ledger and into the router. However, because the
orchestrator provisions the HBM and knows the engine's capacity, this refusal does not have to be a
naive queue-depth threshold. It can be a **byte-depth or token-depth** threshold (derived from prompt
sizes and max tokens) evaluated against the HBM partition the orchestrator granted. This preserves
a form of authoritative admission control, just shifted to the router.

**The save: macro-orchestration, topological dataflow, and targeted host DDR arbitration.**
HBM and DDR are fundamentally two different classes of memory operating in physical isolation.
Attempting to compare an accelerator HBM KV block directly against a host DDR microVM cell in the
same eviction shadow price is an architectural category error: GPU prefill recompute (compute-bound,
non-linear) and microVM restore (I/O-bound, flat) do not compete for the same physical memory bus.

Furthermore, in production datacenter topology, expensive multi-GPU instances ($30–$40+/hr) do not
treat host DDR as a municipal dump for arbitrary background services (`ServiceHeap`). Host DDR on GPU
hosts is tightly provisioned for PCIe bounce buffers, NUMA-pinned staging, and accelerator offload
connectors (`LMCache`, NIXL). Co-locating arbitrary CPU compute on GPU nodes risks memory bus
saturation and NUMA interference that throttles accelerator throughput.

Instead, the orchestrator's value proposition splits across three distinct, realistic mechanisms:

1. **Topological Placement & Dataflow Locality (GPU Compute $\leftrightarrow$ CPU Compute):** The
   primary relationship between GPU inference and tool/agent execution is not memory contention, but
   **spatial dataflow placement**. The orchestrator's goal is to co-locate or proximity-schedule the
   dependent tool workload as close as possible to the active GPU context (same-node, same-rack, or
   same-zone) to minimize serialization overheads, link latency, and network congestion tolls.
2. **Macro-Scale Orchestration & Gang Slicing:** The orchestrator maintains global authority over
   what engines cannot see: multi-node gang flow admission (all-or-nothing sub-agent placement),
   dynamic model weight loading/swapping across heterogeneous hardware, and coarse HBM partition sizing.
3. **Targeted Intra-Host DDR Arbitration:** On nodes where tool execution is co-located with inference,
   shadow pricing binds specifically between **Host-side Offload/KV connectors** and **Ephemeral Tool
   Warm Cells (`Snapshot`)**. Here, soft floors beat hard static partitions by dynamically balancing
   local tool working sets against offloaded inference state without starving the accelerator's PCIe
   pipeline.

That reframes the thesis cleanly: the unified advantage is **macro-scale capacity and gang orchestration**,
**joint dataflow placement across topological boundaries**, and **targeted host DDR multiplexing** where
co-location is physically sound. Whether this architectural coordination outperforms siloed specialists
is the core empirical question to measure.

### Which existing results this contaminates

Stated up front, with the expected direction, so nobody builds on them and so the re-measurement
cannot be quietly graded on a curve:

| result | why it is affected | expected |
|---|---|---|
| soft floors beat hard partitions, 32% oracle-tuned (single node) | both arms assume one authoritative allocator across all four classes | **shrinks** -- the KV/weights half becomes advisory |
| fixed budgets cost 4.0% service, 2.7pp goodput | same | **shrinks** |
| 90% of tool calls shipped to idle model-host DDR | the option exists because the ledger controls that DDR; an engine KV connector may own it | **uncertain**, possibly unchanged for `Snapshot` cells |
| all-or-nothing fan-out admission: +22% fan-outs | rests on `could_admit` as a hard HBM feasibility test, which no longer exists | **survives, but the binding constraint moves** to engine slots, queue depth, and host memory |
| tool-placement inversion under memory pressure | driven by host DDR contention, which the orchestrator owns | **likely survives** |
| the acquisition crossover (KV ships within a rack, rebuilt across a zone) | a cost comparison, not an allocation decision | **survives** -- and becomes advice to the engine rather than an action |
| boundary-cost ladder, origin round trip, congestion toll | independent of memory ownership | **unaffected** |

The pattern: results about *host* classes and about *costs* survive; results about *authority
over inference memory* do not.

### Observability is not a stale exact view

`Control::Gossip` hands the scheduler a stale but **exact per-blob** residency set. No
observability system can provide that: you cannot scrape "does node 3 hold prefix X". Real
telemetry offers one of two things, which is precisely llm-d's precise-vs-approximate split:

- **`Metrics { interval }`** -- exact aggregates, no per-blob detail. Hit rate, queue depth,
  pinned usage, batch occupancy.
- **`Events { loss }`** -- an approximate per-blob index maintained from a KV event stream:
  fresh, but lossy and probabilistic.

Replacing `Gossip` with these two is a correctness fix to the experiment, not a refinement.

#### Direct Ingestion: Step-Aligned Micro-Batching over ZMQ IPC

Relying on a traditional out-of-band monitoring pipeline (e.g. Prometheus / OTel with 5–15s scrape
intervals) is an artificial handicap for hot-path decisions. Conversely, streaming per-block RPCs
at request time floods the host CPU with tens of thousands of interrupts per second, while in-band
response trailers arrive far too late (only after multi-second decode generations finish).

Instead, the orchestrator directly ingests telemetry emitted by the inference engine via
**step-aligned micro-batching over ZeroMQ (ZMQ) IPC**:

1. **Step-Aligned Cadence (40–100 Hz, ~10–25 ms):**
   Inference engines (vLLM, SGLang) execute in discrete forward iterations (`step()`): prefill chunks
   take ~10–50 ms, decode steps take ~10–25 ms. Block allocations, preemptions, and evictions occur
   strictly during these step boundaries. Flusing telemetry *at the end of each iteration* naturally
   rate-limits event volume to the engine's step frequency (40–100 Hz), completely eliminating
   socket churn while keeping belief state fresh to within ~15 ms.
2. **Transport via ZMQ IPC (`PUSH/PULL`):**
   Transport runs over Unix Domain Sockets via ZeroMQ (`ipc:///tmp/poly_engine_{id}.ipc`). ZMQ provides
   lock-free queueing, kernel-assisted batching, and clean multi-language framing between Python engine
   workers (`pyzmq`) and the Rust orchestrator (`zeromq-rs`) at $< 5\,\mu\text{s}$ crossing cost,
   bypassing the HTTP/2 framing overhead of gRPC without incurring custom shared-memory ring ABI debt.
3. **Asymmetric, Eviction-Centric Wire Format:**
   The router already knows which prompt prefixes it dispatched, so allocations can be tracked
   optimistically. What the router cannot guess without telemetry is **which blocks the engine evicted
   under pressure** and **current capacity watermarks**. Telemetry is packed into a compact binary struct:
   - `TelemetryBatchHeader` (32 bytes): `engine_id: u32`, `epoch: u32`, `seq: u64` (monotonic step counter),
     `free_kv_blocks: u32`, `total_kv_blocks: u32`, `queued_requests: u16`, `running_requests: u16`,
     `flags: u16` (Bit 0: Normal, Bit 1: Yellow watermark $>80\%$, Bit 2: Red/Preempting $>95\%$),
     `num_evictions: u16`, `num_allocations: u16`.
   - Payload: array of truncated 64-bit blake3 hashes for evicted blocks, followed by newly committed prefix nodes.
   - Total payload is typically 100–400 bytes, consuming $< 15\text{ KB/s}$ of bandwidth per GPU engine.
4. **Drop Detection and Periodic Reconciliation:**
   - **Gap detection:** The router tracks `seq`. If $seq_n \neq seq_{n-1} + 1$, a message was dropped
     by the ZMQ high-water mark. The router immediately tags that engine's belief state as degraded
     (widening the uncertainty interval on displacement cost).
   - **Periodic Sync:** Every 1–2 seconds (or immediately upon a sequence gap), the engine pushes a
     compact **Sync Batch** (a full prefix tree snapshot or block bloom filter) to reconcile drift.

#### The Dual Time-Scale Control Loop

Step-aligned micro-batching closes the loop on **routing and admission control**: the orchestrator
learns of memory pressure and evictions within 15 ms, diverting incoming prefill traffic *before* the
engine descends into severe preemption cascades.

However, micro-batching cannot eliminate the physical latency of **macro-provisioning**:
- **Detection & diversion are fast (10–25 ms):** The router catches watermark interrupts on the next step
  and halts new prefill admissions to that node.
- **Remediation is physical (5–30+ seconds):** Slicing new HBM partitions, pulling 140 GB of model
  weights over PCIe/network, and initializing engine contexts takes seconds.

Step-aligned ingestion is therefore what makes the two-tiered model viable: it buys the necessary time
for slow macro-actions (provisioning and weight swapping) by executing near-instantaneous micro-actions
(traffic diversion and backpressure admission).

A measured aside on why staleness matters: sweeping the gossip period at rack distance,

| refresh every | `both, gossiped` service |
|---|---|
| 200 requests | 693.3 ms |
| 1000 | 613.2 |
| 3750 (≈15 s scrape at 250 req/s) | 499.8 |
| 7500 | 481.3 |

The residency-greedy arm gets **better** as its view gets staler, because staleness stops it
concentrating work on whichever node looks hottest. Realistic telemetry lag is not a handicap
for that policy; it is a crutch. Which means "how fresh is your view" is the wrong question to
organise the control-plane experiment around. The right one is "does the score price
congestion" -- a score that does needs no fresh view to avoid concentrating, and the scored arm
does not show this effect.

---

## 2. What changes in the engine

Ordered by what makes the rest trustworthy.

### 2.1 A `Telemetry` boundary

One type mediates everything a policy may read. Owned state stays directly accessible to the
ledger; the *scheduler* reaches residency, costs, and load only through it. Inferred quantities
live behind it with confidences attached. Observed quantities arrive sampled.

This is `sched_lm`'s `RequestView` discipline -- "body observables, never the workload's
ground-truth class" -- applied to the whole engine rather than to one policy signature. Without
it, `req.tokens` and exact `recompute_ns` stay in the score and every subsequent result is
contaminated by information no deployment has.

The migration is mechanical and the compiler finds the work: make `Request`'s ground-truth
fields private to the workload and the ledger, and give the scheduler an observables view
(prompt tokens, message count, whether the last message was a tool result, chain-root hash) plus
estimator handles.

### 2.2 Predicted flows replace declared flows

Replace `FlowHint { probability: 1.0, lead_ops, payload_bytes }` with an estimator over observed
history, one per (tool, session-class):

- P(this turn calls a tool)
- which tool, as a distribution
- re-arrival gap: EWMA mean **and variance**, so confidence is available and not invented
- payload size: EWMA

`Hierarchy::anticipate(id, weight)` already takes a probability and is already capped at "one
announced access is worth at most one access". It has been fed `1.0` since it was written. Feed
it the predicted probability and the mechanism becomes honest with no change to the ledger.

The experiment this unlocks is the falsification the prewarm and gate results need: **how much
of the Tier-1 win survives when the hint is an estimate?** Announce currently buys 11-18% task
latency at slightly worse net work, measured against a perfect oracle. Against an EWMA with real
variance it will buy less, and the amount it loses is the honest value of the mechanism.

### 2.3 Retention directives in the ledger

Add to `Entry`:

- `retain_until: u64` -- a soft pin with a deadline
- `evict_first: bool` -- the one-shot / cache-pollution mark

This is RFC-0001's `50; ttl=<window>; scope=<session>` and `-1`, and it is strictly better than
the current unbounded `expect` bump, which was already flagged as a weakness: a priority
inflation with no deadline never self-corrects when the prediction was wrong, while a TTL does.
It is also the mechanism already live in `sched_lm`'s forked simulator with pinned-cache gauges,
so modelling it means modelling something that exists.

The unified angle: a directive priced in the same ns/byte as everything else can be *weighed
against what it displaces*. In a siloed stack a retention hint is advisory and unpriced -- the
engine can honour it or not, and cannot compare it to its own eviction candidates.

### 2.4 Oracle, regret, coupling

The methodological gap, and the reason every number handed over so far has carried a fairness
caveat. Import three metrics from `sched_lm`:

- **Routing regret** -- `policy_cost − oracle_cost` computed on *the policy's own state*. This
  separates decision quality from state quality, which arm-vs-arm comparison cannot.
- **A clairvoyant eviction baseline** (`oracle-belady`), distinct from the routing oracle, so
  ledger quality and placement quality are separable too.
- **Coupled %** -- the fraction of decisions that change when evaluated globally rather than in silos.
  To avoid conflating isolated physical hardware domains, coupling is reported along two orthogonal axes:
  - **Memory Coupling % (DDR: Traditional Compute vs. FaaS):** How often the optimal retention,
    eviction, or allocation of a FaaS snapshot changes when accounting for co-located service heaps
    in host DDR. This directly tests whether soft floors beat hard cgroups for host memory.
  - **Locality Coupling % (GPU Inference $\leftrightarrow$ CPU Tool Placement):** How often the
    optimal placement of a tool execution changes based on which node/rack hosts the dependent
    inference context and the network congestion ladder. This directly tests the value of
    co-scheduling agent dataflow.

**Coupled % is the best available answer to "does unified beat siloed."** It measures how much a
decision depends on state a silo would not have, directly and per request, with no baseline to
tune and no arm to handicap. It also *bounds the unified advantage from above*: where coupling
is low, a unified view provably cannot help much, and we should say so rather than hunt for a
configuration where it does.

### 2.5 Regime mix, including wait

`sched_lm` reports the share of requests resolved by wait / transfer / recompute. Polyproto
reports fetched / rebuilt and has **no wait regime at all**, even though the congestion toll
prices queueing implicitly. Adding the third regime makes the acquisition decision legible and
comparable across the two prototypes.

### 2.6 Divergence as a first-class signal

Once the engine's cache state is observed rather than owned (§1), the difference between what the
ledger believes is resident and what the engine reports is itself a metric -- and the only
honest measure of how well a directive-plus-belief architecture tracks reality. It should be
reported, not smoothed away.

Formally, residency divergence at time $t$ for engine $e$ is published as:
$$\text{Divergence}(e, t) = \frac{|\text{Belief Resident Blocks}(e) \setminus \text{Actual Resident Blocks}(e)|}{|\text{Belief Resident Blocks}(e)|}$$

Divergence spikes during three conditions:
1. **Unobserved eviction cascades:** The engine drops blocks under burst decode pressure between micro-batches.
2. **Telemetry drops:** A gap in the ZMQ `seq` counter indicates lost batches, immediately widening the
   router's displacement cost uncertainty band.
3. **Ignored retention directives:** The router issued a soft pin (`retain_until`), but the engine
   evicted anyway under strict LRU pressure.

Tracking divergence allows Polyproto to isolate whether routing mistakes were caused by a bad cost
scoring model or by an engine belief that drifted from physical reality.

---

## 3. The taxonomy as a scheduler input

[`taxo.md`](taxo.md) is ten patterns across four dimensions. That is right as analysis and wrong
as an engine input: the scheduler needs a handful of fields it can act on, not a pattern name.

### Four dimensions, four fields

| dimension | scheduler field | status |
|---|---|---|
| Control flow | `flow: None \| Declared \| Predicted(dist) \| Fanout(n)` | 3 of 4 built; **Predicted** is §2.2 |
| Knowledge grounding | which blob classes, and their sharing shape | KV / snapshot / weights built; **RAG missing** |
| State and time horizon | `retention: evict_first \| until(deadline) \| durable` | **missing**; §2.3 covers the first two |
| Authority to act | `authority: ReadOnly \| DraftOnly \| SideEffecting` + `pause_tolerance` | **missing**; drives speculative execution & preemption |

Each of the ten patterns becomes a named preset over those fields, the way `sched_lm` takes
`--mix tool=0.5,rag=0.3,oneshot=0.2`. Three consequences:

**Control flow maps one-to-one onto machinery that already exists.** Single-call is a plain
request; fixed multi-step is the declared flow polyproto has; parallel/delegated is the
fan-out; dynamic multi-step -- the defining agentic case -- is definitionally the one that cannot
be declared and must be predicted. That explains why declared hints felt natural: they are the
*fixed*-pipeline case, and the prototype has been testing the easy half of the dimension.

**RAG-grounded is a genuinely missing class.** Retrieved chunks are shared across *sessions*
with Zipf popularity, not chain-structured like a KV prefix, so they evict differently from
anything currently modelled and they contend with KV for the same pool. `sched_lm` models this
(`--rag-docs`, `--rag-zipf`); polyproto has no equivalent. It is the cheapest high-value addition
to the workload, and it is the one grounding mode that changes the ledger's contention shape.

**Durable memory breaks an invariant.** Every class in the ledger is evictable at a priced cost.
Durable state must never be *lost*, only demoted -- a correctness constraint, not a cost
tradeoff. That exists today only as `ServiceHeap`'s serving pin, and generalising it means the
ledger needs a class of state whose eviction is forbidden rather than expensive.

### Authority to act drives speculative scheduling, preemption, and idempotency

Dismissing authority as an audit or governance label leaves a major orchestrator capability
untapped. In agent workflows, **authority directly sets what the scheduler may speculatively
execute, branch, preempt, or checkpoint**:

1. **`ReadOnly` (Idempotent / Informational):**
   - *Workload shape:* Search queries, database reads, code analysis, document summarization.
   - *Scheduling action:* **Speculative dispatch and parallel pre-warming.** When turn $N$ predicts
     a tool call with probability $P$, the orchestrator can speculatively pre-warm the tool microVM
     or even dispatch the query concurrently with the final decode tokens. If the model veers away
     or cancels, the speculative branch is aborted with zero rollback penalty.
2. **`DraftOnly` (Soft / Staged Output):**
   - *Workload shape:* Draft responses, staged code patches, proposed calendar invites.
   - *Scheduling action:* **Burstable scheduling with zero-compensation preemption.** These tasks
     can safely occupy burstable slack in host DDR or low-priority engine slots. If high-priority
     work arrives, they can be immediately preempted, evicted, or rescheduled without distributed
     transaction sagas.
3. **`SideEffecting` (High Authority / Material Action):**
   - *Workload shape:* Financial transactions, database mutations, external webhooks, deployments.
   - *Scheduling action:* **Strictly non-speculative, atomic gang reservation, and non-preemptibility.**
     Speculative dispatch is strictly forbidden. The orchestrator must enforce synchronous durable
     checkpointing *prior* to dispatch, and issue non-revocable resource leases so the execution cannot
     be torn down mid-flight.
4. **Human-approved execution (unbounded pauses):**
   - A task awaiting human approval holds state for minutes to days. Once an action hits a human gate,
     the orchestrator demotes the entire execution context out of expensive HBM and host DDR into
     cold storage (NVMe/object store), releasing active memory until the external approval callback arrives.

### Generator-side truth, scheduler-side inference

The structural recommendation: **the taxonomy exists twice, and telemetry is the only bridge.**

- The **workload generator** uses the full taxonomy as ground truth to synthesise traces.
- The **scheduler never sees the class.** It infers a profile from observables.

Then classification accuracy, and the cost of getting it wrong, become measurable -- which is
what `class_aware` plus `ToolGapIndex` do in `sched_lm`, and what polyproto cannot do today
because its `Request` carries the truth. This is what turns `taxo.md` from a document into the
experiment's independent variable.

### Per-pattern coupling is the falsifier

Run coupled % per taxonomy cell across both dimensions:
- **Memory Coupling (DDR):** tests how much traditional compute and FaaS gain from sharing host RAM.
- **Locality Coupling (Topology):** tests how much tool and agent placement gains from knowing where
  the GPU inference engine and KV context reside.

The output is a two-column table saying, for each workload pattern, whether a unified orchestrator
can help at all. Expected shape, stated in advance so it can be wrong: batch inference and one-shot
generation should show near-zero coupling on both axes (independent requests, nothing to co-decide);
multi-agent and long-running agents should show high locality coupling (shared context, cross-node
dataflow, atomic admission) and moderate-to-high memory coupling on the host. If that prediction
fails, the thesis is narrower than claimed and the document should say so.

---

## 4. Emergent properties: measured, and testable

An advantage is *emergent* here if no silo can produce it independently and it is not merely a
hint away. Five are measured, clarified by the separation between host memory arbitration and
topological dataflow placement; two are proposed.

**Measured within Host DDR (Orchestrator-Owned Memory):**

1. **A cross-class DDR shadow price.** One `marginal_price` for host DDR balances microVM warm pools
   (`Snapshot`) against accelerator offload tiers (`LMCache`) and local services. Fixed budgets cost
   4.0% service and 2.7pp goodput; oracle-tuned on a single node, soft floors beat hard cgroup
   partitions by 32%. While multi-GPU production hosts will rarely co-locate heavy non-AI services,
   soft floors excel where co-located tool cells compete directly with host-side KV offload.
2. **Placement options that static host partitioning forecloses.** With soft floors the score
   ships 90% of tool calls to idle host DDR and gets a 53% warm rate; with hard pools that host's
   slice is capped no matter how much memory sits free beside it, the option disappears, and the
   warm rate halves to 19%. This is about `Snapshot` cells in host DDR -- orchestrator-owned --
   and survives intact because it does not depend on HBM authority.

**Measured across Topology (Dataflow, Macro-Capacity, and Placement):**

3. **Congestion and residency in one argmin.** Neither an inference router nor a FaaS control
   plane can price "place the tool call as close to the active GPU context as possible, unless the
   link is congested or local host memory is full". Measured consequence: the tool-placement decision
   **inverts** between the unpressured and memory-bound regimes, and again at region distance.
4. **Cross-workload atomic admission.** All-or-nothing placement of a fan-out across nodes is
   not expressible per request. Where it binds: +22% fan-outs completed, inference stall −10%.
5. **One currency for host hints.** A prewarm, a retention directive and an eviction priced in the
   same host DDR units can be traded against each other. A siloed hint is advisory and unpriced.

**Proposed, and the reason to do §2 and §3:**

6. **Learned cross-class retention.** "This agent returns to this tool in ~800 ms, confidence
   0.7" driving a *FaaS warm-cell* retention decision in host DDR, priced against what holding it
   displaces. A silo can receive that as a hint; it cannot weigh it.
7. **Authority-driven speculative scheduling.** Pre-executing idempotent (`ReadOnly`) tool calls
   concurrently with model decode, and scheduling `DraftOnly` tasks into burstable capacity with
   zero-compensation preemption rights.
8. **Two-dimensional coupling as a published quantity.** Not an advantage but the measure of one,
   separating host memory efficiency from topological dataflow affinity.

Worth stating plainly: the largest defensible effects are **topological dataflow co-placement**,
**macro-orchestration of model weights and gang fan-outs**, and **targeted host DDR multiplexing**
between offload tiers and local tool snapshots—not magical cross-hardware arbitration of HBM bytes.
Once we stop pretending the orchestrator allocates HBM KV blocks, what remains is an orchestrator that
solves three genuine problems existing stacks fail at: joint dataflow placement across network boundaries,
macro-scale capacity and gang coordination, and authority-aware speculative execution. Phase 3 and
Phase 7 test whether that advantage holds up under rigorous scrutiny.

---

## 5. Reading `sched_lm`

### What it gets right, and worth importing

- **The argmin *is* the decision.** `cost.py` computes `wait + transfer + prefill(missing)` and
  takes the minimum; the policy is not a heuristic layered over a cost model, it is the cost
  model. Polyproto arrived at the same shape independently in `Machine::plan`, which is decent
  evidence the primitive is right.
- **Regret on the policy's own state.** Separating routing quality from cache-state quality is a
  subtle decomposition and the reason its comparisons need no tuned baseline.
- **`RequestView`.** Refusing the policy access to ground truth, by type.
- **Coupled %.** The best single metric for the unified question.
- **Learning from timing metadata alone.** `ToolGapIndex` needs no new instrumentation, which is
  what makes it adoptable in a real stack.
- **Deadline-scoped directives.** `ttl` and `scope` rather than an unbounded priority bump.
- **Using residual regret to justify infrastructure before it is built.** The transfer regime is
  not expressible in stock llm-d, so the sim's job is to show the regret that would justify a KV
  connector tier. That is the correct use of a simulator, and it is the posture this prototype
  should copy.
- **A promotion path.** Tier 0 config / Tier 1 plugin / Tier 2 serving-stack change, so a
  finding has somewhere to go.

### Where a unified system needs more

- **It is inference-only.** One workload class in the cost model: no warm pool, no service
  replicas, no cross-class contention -- so the effects that dominate every result here are
  invisible to it by construction.
- **One pool per node.** No HBM/DDR split and no offload tier priced as a recovery path, so
  eviction cannot be priced as `min(rebuild, promote, fetch)`. That mispricing was worth two
  orders of magnitude in the displacement term here before it was fixed.
- **No refusal in the argmin.** Load shed is tallied in the live stack; the offline cost model
  never declines a request, so admission is not part of the decision.
- **Unpriced retention.** A directive is per-request and advisory, with no global shadow price to
  weigh a pin against what it evicts.
- **No atomic multi-worker admission.** Nothing expresses a fan-out that must be placed whole.
- **TTFT-only objective.** This is the transferable warning, learned the hard way here: once
  decode cost depends on batch occupancy, **placement moves execution and not just waiting**, and
  a wait-only metric misranks policies. Polyproto's headline had to move from stall to
  end-to-end service time for exactly this reason, and stall is TTFT's analogue. Any policy
  comparison on TTFT alone is exposed to the same inversion, and adding goodput alongside it
  catches the related trap where an arm looks fast because it refused the expensive work.

### The naming collision

`sched_lm` uses Tier 0/1/2 for *promotion effort into production llm-d*. Polyproto uses
Tier 1/Tier 2 for *information-sharing versus joint-decisions*. Same words, orthogonal axes.
One should be renamed before the two vocabularies meet: **promotion tier** and **coupling tier**
would do.

---

## 6. Constants are a method, not a debt

This prototype is for understanding architecture and tradeoffs, so modelled constants are
legitimate. The discipline that makes them legitimate is **sensitivity, not precision**:

> For every constant, know which conclusions are robust to it across its plausible range, and
> which flip. Publish the ones that flip.

This has already bitten once and is worth generalising from. The FaaS snapshot cost was a
cold-container constant (200 ms / 32 MiB, linear); replacing it with a Firecracker lazy-restore
model (~9 ms, roughly flat in image size) **inverted the ns/byte ordering** and reversed a
headline about which bytes are cheapest in the machine. The constant was not slightly wrong; the
conclusion depended on it entirely.

So the rule for this document's proposals: a result may rest on constants, but it must come with
the range over which it holds. Where a crossover is the finding -- KV ships within a rack and is
rebuilt across a zone; tool calls go local at region distance -- the crossover's *position* moves
with the constants while its *existence* does not, and the existence is the claim. Where an
ordering is the finding, it is only worth as much as the gap between the constants that produce
it.

---

## 7. Feasibility

### Is the correction tractable at all?

Yes, and more cheaply than the size of the claim suggests, for one reason: **the ownership
boundary coincides with a partition the code already has.** `accelerated(kind)` exists in
`cache.rs` to route classes between HBM and DDR, and it separates exactly the engine-owned classes
from the orchestrator-owned ones. Every site that needs to change is a site that already asks that
question, or is reachable from one.

The affected surface is small and concentrated:

| area | what changes | scale |
|---|---|---|
| `cache.rs` | HBM `TierPool` becomes an engine-cache *model* with its own policy; host pools keep current semantics | the bulk of it |
| `machine.rs` | residency reads go through a belief; `could_admit` for KV disappears; displacement for KV becomes an estimate | moderate, mechanical |
| `main.rs` | new arms (engine honours / ignores directives; precise / approximate index) | additive |
| `work.rs` | unaffected by ownership; changes only for §2.2 and §3 | none for this correction |
| `engine.rs` | gains the cache alongside the batch model -- they belong to the same component | small |

### What gets harder, and what might not survive

Honest list, worst first.

1. **Admission control over inference state disappears.** An engine evicts and recomputes; it does
   not refuse for lack of KV. `Admission::Pending` stops being reachable for `KvBlock` and
   `WeightShard`. Three things currently lean on it:
   - `Hierarchy::could_admit`, hence **gang feasibility**. A fan-out can no longer be refused for
     HBM reasons. It can still be refused on host memory and on engine slots, so the primitive
     survives with a different binding constraint -- but the measured +22% needs re-earning.
   - `can_satisfy`, hence the **downstream-aware gate**. Same treatment: the gate becomes a
     queue-depth and host-memory judgement.
   - **Goodput as an outcome.** Refusals for inference move to the router, so the goodput numbers
     change shape even where they do not change size.
2. **Displacement becomes an externality, not a decision.** Routing a request to a node still
   *causes* the engine to evict something there; the orchestrator simply does not choose what. So
   the term stays in the score but as an estimate from observed eviction pressure, which is noisier
   and lagged. The regret-discounted, recovery-aware pricing built earlier applies unchanged to
   host classes and becomes an inference for engine classes.
3. **Two sources of truth, permanently.** What the engine holds and what the router believes are
   different objects that drift. That is not a defect to engineer away; it is the architecture, and
   the drift is a metric (§2.6).
4. **The unified memory thesis may not survive.** Named plainly in §4. If the arbitration effects
   vanish once the engine allocates its own memory, what is left is a router with good cost
   accounting, and the honest report is that the ledger's value was an artifact of assumed
   authority. Phase 3 is designed to be able to return that answer.

### What gets easier

- **The engine cache is simpler than a `TierPool`.** LRU over block hashes, no quotas, no bands,
  no floors, no refusal. Modelling vLLM means modelling *less* policy, not more.
- **Model agnosticism becomes checkable.** Once the orchestrator cannot see inside the engine, the
  interface it consumes is small enough to write down: capacity, hit/miss/eviction counts, queue
  depth, load and unload cost per model, and a directive channel. Anything the scheduler needs
  beyond that list is a model-specific dependency, and the compiler enforces the list.
- **Weights become orchestration rather than caching** (phase 6), which is both more realistic and
  a capability no arm has today.

---

## 8. Phases

Each phase compiles, runs, and ends with a number. None requires a later one. The measurement
apparatus comes before the thing it measures, so the phase that may invalidate the thesis is not
also the phase that invents its own scoring.

### Phase 1 -- Name the boundary in types, change no behaviour

Introduce `Ownership { Orchestrator, Engine }` derived from `BlobKind`, and route every ledger
access through it while preserving today's semantics exactly. Engine-owned state becomes reachable
only through one narrow interface.

- **Deliverable:** results byte-identical to today, plus a count of call sites that assume
  authority over engine state. That count *is* the feasibility answer, generated by the compiler
  rather than by reading.
- **Risk:** none to results; the phase is a no-op by construction.
- **Size:** small.

### Phase 2 -- Oracle, regret, coupling

Add the routing oracle, a clairvoyant eviction baseline, per-request regret on the policy's own
state, and coupled %. No architectural change.

- **Deliverable:** every existing comparison restated as regret against an oracle instead of a
  delta against a hand-built baseline. This retires the fairness caveat attached to every number
  produced so far.
- **Risk:** the oracle may reveal that the scored policy's margin over the specialists is mostly
  baseline weakness. Better to find that here than after phase 3 muddies it.
- **Size:** medium.

### Phase 3 -- The engine owns its cache

Replace the HBM `TierPool` with an engine-cache model: LRU over block hashes, capacity-bounded,
**never refuses**, reports metrics. The orchestrator routes and observes. Host classes keep every
current mechanism.

- **Deliverable:** the price of the boundary. Re-run each contaminated result from §1 and publish
  how much survives, against the expectations tabulated there.
- **Risk:** the headline result. Gang feasibility and the gate both lose their HBM test and must be
  re-grounded on queue depth and host memory.
- **Size:** large -- the core of the correction.

### Phase 4 -- Belief, not truth: Step-aligned ZMQ IPC telemetry

Split what the engine holds from what the router thinks it holds. Ingest telemetry via step-aligned
micro-batches over ZMQ IPC (`PUSH/PULL`), packing `TelemetryBatchHeader` and truncated eviction/allocation
hashes at engine forward-step cadence (40–100 Hz). Retire `Control::Gossip`, which models a stale *exact*
view that no telemetry provides. Add monotonic sequence gap detection, periodic reconciliation sync
batches, and explicit divergence reporting.

- **Deliverable:** what routing quality costs when residency is a lossy, step-aligned belief. Evaluate
  under synthetic telemetry loss rates (0%, 1%, 5% dropped micro-batches) to quantify router robustness.
- **Risk:** low. Contained, and replaces a mechanism known to be unphysical.
- **Size:** medium.

### Phase 5 -- Influence: retention directives

Emit RFC-0001-shaped directives on the request path, with two engine arms: honours, and ignores.

- **Deliverable:** what a directive is worth, and what the orchestrator can achieve through routing
  alone when the serving stack does not cooperate. The second number is the one that matters for
  planning, and it is the first Tier-1 result earned by influence rather than assumed by
  declaration.
- **Risk:** low.
- **Size:** small to medium.

### Phase 6 -- Model placement as orchestration

Weight shards stop being per-request cache entries. The orchestrator decides which models load on
which nodes on a slow timescale, with load and unload costs, through a model-agnostic interface:
size, load time, no internals.

- **Deliverable:** a result no arm can produce today -- a heterogeneous fleet serving several model
  types under a shifting request mix. This is where model agnosticism stops being a constraint and
  starts being the capability.
- **Risk:** needs a workload with a realistic model mix, which `taxo.md` supplies.
- **Size:** medium.

### Phase 7 -- Learned flows, speculative authority, and the taxonomy

Predicted flows replacing declared ones (§2.2), a tool-gap estimator, taxonomy presets, the RAG
class, durable retention, authority-driven speculative scheduling (speculative tool pre-execution for
`ReadOnly`, burst preemption for `DraftOnly`, non-preemptible gang leases for `SideEffecting`), then
per-pattern coupled %.

- **Deliverable:** what the Tier-1 win is worth against estimates rather than oracles; the latency
  and goodput delta bought by authority-driven speculative scheduling; and a table saying for which
  workload patterns a unified orchestrator can help at all.
- **Risk:** the per-pattern coupling table may show the advantage is confined to a few cells. That
  is a result, not a failure.
- **Size:** large, and separable into its own increments.

### Ordering logic

Phases 1 and 2 are prerequisites for trusting anything after them: one makes the boundary visible,
the other makes measurement fair. Phase 3 is the correction and the decision point. Phases 4 and 5
are the two halves of the engine interface -- observe, then influence. Phase 6 adds the capability
the corrected architecture makes central. Phase 7 is orthogonal to the ownership question and can
run in parallel with 4 through 6.

Phases 1, 2, 4 and 5 are low-risk and additive. Phase 3 is the one that can return a negative
verdict on the project's central claim, which is why it comes early and why phase 2 precedes it.
