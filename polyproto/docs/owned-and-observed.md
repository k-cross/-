# Owned, Inferred, Observed

The design for where polyproto's scheduler draws its boundaries: what it owns, infers and only
observes (§1), what carries a request (§2), what that changes in the engine (§3), the workload
taxonomy as a scheduler input (§4), and which advantages are emergent rather than assumed (§5). §6
reads a sibling prototype, §7 states the method for constants, §8 covers feasibility and the system
of record, and §9 is the phase plan.

Two boundaries, resolved in opposite directions. §1 hands the engine's memory back to the engine.
§2 refuses to put a process boundary on the request path -- a statement about where the code runs,
not about who wrote it; the HTTP itself is a linked library's (§2.6). **Cede the bytes, keep the
path** -- and the second is only defensible because of the first, since routing and cancellation
are what is left to decide with once allocation is gone.

**Status.** Phases 0-10 are built and measured; Phase 11 is design (§9). Current
results are in [`residency-ledger.md`](residency-ledger.md); each phase's plan, predictions and
outcomes are in its own `phase-N.md`. The corrected architecture runs behind bits that are off by
default -- `--engine-cache` (Phase 3), `--belief` (Phase 4), `--directives`, `--prefill-ahead` and
`--retain` (Phase 5), `--model-batches`, `--prefill-time`, `--model-keyed` and `--fleet`
(Phase 6), and `--engine-wait`, `--queue`, `--admit quantile | tiered | gate`, `--cancel`,
`--victim`, `--disconnect`, `--leak`, `--batch` and `--stream-buffer` (Phase 9), and `--hint-grade`
and `--learn-gate` for the base trace and `polyphonic programs` for closed-loop agent programs
(Phase 7), and `--count-writes`, `--track-flights`, `--observe`, `--node-check`,
`--snapshot-estimators` and `--copy-durable` with `polyphonic durability` for faults (Phase 10) --
so a published number is the ledger's unless it is marked otherwise.

**On the numbers.** Four grades of evidence, kept apart:

- **Measured on the host.** The boundary ladder -- syscall, pipe, socket, ring, WASM, `ext_proc`,
  gRPC -- times real crossings, best-of-10, timer overhead subtracted. It is the firmest evidence
  here, and §2 leans on it. The host is Apple silicon, the cheap rungs spread up to ~2-3x between
  runs and the unix-socket rung is not monotone in payload, so the **ordering** is the result and
  no constant survives being quoted to two digits.
- **Simulated on modelled constants.** Every residency, placement and arbitration result. The
  ledger's *Measured constants* names the modelled ones -- link latency, PCIe, HBM/DDR capacities,
  the workload's `exec_ns` -- as the numbers most worth replacing with real traces.
- **Simulated on the ledger's own KV allocation.** The default side of `--engine-cache` still has
  the ledger allocating the engine's KV, which §1 disclaims. §1 says which results survive the
  correction; the ledger's *Standing* table says which claims hold, failed or were retracted.
- **Not from the runs.** Arithmetic on published figures -- §3.8's HBM budget, §1's
  consensus-commit latency -- worth exactly what its inputs are and labelled wherever it appears.

So a number here is a reason to run an experiment, not a result to build on (§7).

Two inputs shape this document. [`taxo.md`](taxo.md) is the workload taxonomy it has to serve.
[`k-cross/sched_lm`](https://github.com/k-cross/sched_lm) is a sibling prototype reaching the same
central decision from the llm-d/vLLM side, read here as **evidence, not direction** (§6).

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
   evictions, placements, retention directives and gang membership are owned by construction.
2. **Correctness dependency.** A wrong value makes an outcome *invalid* rather than slow. "Is this
   gang fully admitted", "is this replica still serving", "does durable state still have a home"
   are correctness questions. "Which node is hottest" is not.
3. **Hot-path rate.** Consulted per request, orders of magnitude above any export interval.
   Nothing read per request can come from a 15-second scrape.

Something stays **observed** if all hold: another component owns the fact, it is an aggregate or
derivable statistic, and losing it degrades explanation rather than decisions.

Anything else is **inferred**, and inferred state carries three obligations: a confidence, a decay,
and a deadline on any action it drives. For residency the first two are one object -- a belief's
confidence *is* its decay -- and §3.7 is where it reaches the argmin. The third is §3.3.

### Owned is not durable

The three categories say who decided a fact and how fresh it is. They do not say whether it must
survive a crash, and "owned" reads as though it must. Kubernetes answers that once, for everything
-- every object durable, linearizable and watched in etcd -- and that is the answer that fails
under churn. Google's AX moved task state out of custom resources into Redis because "storing
millions of short-lived tasks as Kubernetes CRDs pushes etcd past its comfort zone (single-digit GB
storage limits, write-rate bottlenecks, control plane degradation)"
([`DESIGN.md`](https://github.com/google/ax/blob/main/DESIGN.md)); Kyverno moved its policy
reports into a separate database for the same reason. Neither bought a faster etcd. Each moved one
class of state into a store chosen for that class, in its own failure domain.

So durability is a column of its own, with one value per clock:

| durability | changes | holds | lives in |
|---|---|---|---|
| **soft** | per decision | admission, placement, flow graph, directives with a TTL, staged reservations, all inferred state | process memory, rebuilt from node agents and engines on restart |
| **logged** | per session transition | `SideEffecting` intents, suspended-session records, approval pauses, per-tenant usage | an append-only log in its own failure domain: a second FoundationDB cluster (§8) |
| **record** | per provisioning change | membership and leases, quotas, tenancy, partition sizes, model placement | the system of record (§8), strictly serializable, low rate |

§2.2's rule decides the first row. A consensus commit costs at least an fsync and a quorum round
trip -- hundreds of microseconds at best and low milliseconds typically, **on published figures,
not measured here** -- roughly 5-100x the 44-75 us per request §2.3 measures for the sidecar path
this document removes. Group commit raises throughput, not latency; writing behind removes the
latency and leaves process memory as the authority, which is the soft tier by another name. And
§2.6 puts the data path in the scheduler's address space, so per-request state is **fate-shared**
with the connections it describes: persisting it records requests that died with the process.

Throughput is not the objection -- a scale-out store absorbs the write rate. Latency on the path
is, and so is blast radius: one store holding every row lets a surge of session state take
membership and quotas down with it, which is the half of the etcd lesson a faster store does not
touch.

**`authority` is the per-request classifier** (§4). `ReadOnly` and `DraftOnly` are defined by zero
rollback and zero compensation, so after a crash they are simply redone. `SideEffecting` is the one
class that writes to the log before dispatch, and it can afford to: the external action dwarfs a
commit, and a side effect run twice is a correctness failure rather than a cost.

**The column assumes a failure model, and Phase 10 injected each crash.** A scheduler crash loses
soft state, and what it costs is mostly not the state. With the streams held below the scheduler, a
restart costs the arrivals its outage delays, `λD²/2`: 0.7-1.3 request-seconds at 0.1 s, 126-139 at
1 s and 32,000-39,000 at a 15 s lease, on four nodes at 250 req/s. A 15 s lease is therefore not a
takeover for a scheduler on the request path, and node agents enforcing their own partitions are
what let a standby take over without one. With the streams fate-shared with the scheduler (§2.6) a
crash adds 61-73 decode-seconds thrown away and costs 63-75 request-seconds at a 0.1 s takeover,
284-370 at 1 s with a restart and 233-276 with the client's continuation. Of the soft state, the
belief, the flow graph, the tenant meters and the length estimators cost nothing a run can see when
lost (within 10 request-seconds where memory does not bind hardest): residency is re-learned from
the scheduler's own dispatches within a second or two. A reservation ledger that is not rebuilt from
node agents over-admits 3-30% of what it admits blind, and only at 0.6x of the granted partition and
below; the engine's wait absorbs it, and node agents that check their own partitions leave 0-4. The
estimators' snapshot (32 KiB every 10 s) is not needed, since lengths observed when a decode ends
re-learn within a fraction of a second. An engine crash costs its replica's restart, 3.6-5
request-seconds for every second one engine of four is down whatever happens to its KV or its spill,
so every belief about it going to `P(resident) = 0` costs nothing measurable. A node is dead when
its lease in the record expires, which makes liveness **owned**, not observed: "is this replica
still serving" is a correctness question by the second test above, and two schedulers must never
hand out one node's capacity. But routing cannot wait for the lease: a router that learns of a lost
node after 10 s pays 4,200-4,800 request-seconds and after 40 s about 65,000, the requests placed on
it parked until then, so suspicion routes at the first failed dispatch and the lease decides only
re-placement and budgets. Utilisation and power stay observed.

**The tiers' rates are counted** (Phases 7 and 10), on four nodes at 250 req/s with the engine
allocating. The soft tier makes 3.1 owned changes a request with nothing enforced and 5.0 under
Phase 9's enforcement -- 770 and 1,200 a second, 190 and 300 a node -- of which decisions are 1.23 a
request; the KV event stream a scheduler would ingest is a further 51-77 events a request, thirteen
to seventeen times that and inferred, rebuilt rather than stored. The logged tier is Phase 7's: an
intent and an outcome per `SideEffecting` call are 20% of the soft tier's decision rate on the
agentic preset (7.1 writes a second against 36.3 decisions), 38% with MCP's defaults for edits,
40-44% on the pipeline and multi-agent presets, and the long-running preset writes 0.59 a second
against 0.9 decisions; the five presets with no side effect write nothing. That is within a factor
of five of the soft tier, not orders of magnitude below it. The record tier has two writers: the
planner, 0.046 a second on eight nodes, and liveness, which at Kubernetes' 10 s lease renewal is 0.1
a second a node -- 94-95% of the record's writes, seventeen times the planner's on eight nodes, and
set by the fleet's size rather than its traffic. The record is three orders below the soft tier at
four nodes (1,400-3,000 times) and the logged tier is not. At 10,000 nodes the soft tier is 3.0
million owned changes a second (0.14 cores a node at FoundationDB's published cluster write rate),
the logged tier 146,000-491,000 a second on the agent presets, under the 820,000 writes a second of
FoundationDB's published 384-core benchmark, and the record about 1,060 a second. A commit per
decision costs a request 72 us on a protected drive within a rack, 0.8 ms across a zone and 1.5-2.5
ms at FoundationDB's published commit -- 0.6-21 times a warm `FaaS` invocation -- and 4 ms with this
host's full flush (an append is 1.6 us and an `fsync` 18 us), so write-through is ruled out on
latency before any restart is priced.

### Where polyproto's state falls

| state | category | durability | where it is now |
|---|---|---|---|
| per-blob residency, host classes (`Snapshot`, `ServiceHeap`) | **owned** | soft | `TierPool.entries` |
| per-blob residency, `KvBlock` | **inferred** | soft | `EngineCache` under `--engine-cache`, read through the event-stream belief under `--belief`; by default the ledger still owns it in `TierPool.entries` |
| per-blob residency, `WeightShard` | **inferred** | soft | `TierPool.entries` by default; under `--fleet` a model is a placement, owned (next row), and no weight reaches the ledger |
| offloaded KV in host DDR | **inferred** | soft | the connector's LRU tiers under `--engine-cache`; `TierPool.entries` by default |
| quotas: floors, bands, limits | **owned** | record | `Quota`, operator config |
| admission outcome, refusals | **owned** | soft | `Admission`, `TierPool.refused`; the router's partition check under `--admit` |
| gang membership, staged reservations | **owned** | soft; logged when `SideEffecting` | `staged_*`, `cancelled` |
| retention directives it issued | **owned** | soft | `Entry.retain_until` on the ledger (`--retain`); on the engine a mark on a dispatch, believed once the stream acknowledges it (§3.3) |
| placement decisions, flow graph | **owned** | soft | `upstream`, `tool_anchor`, `origin` |
| partition sizes, model placement | **owned** | record | `Fleet` under `--fleet`: a replica per node, its role, and the partition its weights leave, changed by the planner about once in 20 s on the rotating mix (§9, Phase 6) |
| side-effect intents, suspended sessions, approval pauses | **owned** | logged | **counted, not stored** (Phase 7): `programs::LogCause` counts the writes by cause; the log's store is §8's and the simulator has none |
| tenant identity and per-tenant quota | **owned** | record | identity declared on `Request`, and the router's two per-tenant meters built (§3.8); the per-tenant axis on `Quota` is **missing** |
| node liveness and membership | **owned** | record | counted, not stored (Phase 10): a lease renewal a node every 10 s is the record's largest writer; a node declared lost leaves placement, and a router that waits for the lease parks a quarter of its traffic |
| shadow price per pool | **inferred** | soft | `TierPool::marginal_price` |
| regret rate per class | **inferred** | soft | `TierPool::regret_rate`, from a ghost list |
| per-tool re-arrival gap | **inferred** | soft | built for programs (Phase 7): the tool-transition estimator and the idle-gap survival the lifecycle reads; not on the base trace |
| P(turn calls a tool), which tool, payload | **inferred** | soft | a hint carries a grade (Phase 7, `--hint-grade`): declared, template, or learned per function, whose probability is the observed rate; the published defaults still declare at `1.0` |
| output length of a decode | **inferred** | soft | the observed mean under `--observables`; the exact length by default |
| peer residency | **inferred** | soft | the event-stream belief under `--belief`; otherwise `Gossip`, a stale *exact* set |
| workload class | **inferred** | soft | built for programs (Phase 7): read from observables, right for 60.6% of requests at their first call and 85.7% at their last |
| realised TTFT / ITL, batch occupancy | **observed** | -- | modelled internally |
| engine prefix-cache hit rate | **observed** | -- | the engine's under `--engine-cache`; conflated with owned residency by default |
| node utilisation, power | **observed** | -- | absent |

Two inferred entries are estimates by design. `marginal_price` is deliberate, because a faithful
dry run costs as much as the eviction it prices; `regret_rate` is measured from a bounded ghost list
rather than assumed. Both show the engine already has somewhere to put this kind of quantity.

Three entries were cheats -- information no deployment has -- and two now have honest replacements
behind bits. The score read the exact output length; under `--observables` it reads the observed
mean, which costs nothing on this workload (§3.2). `Control::Gossip` handed over a stale but
**exact** engine residency set, which telemetry cannot produce (below); under `--belief` the
engine's KV comes from its event stream instead. The third is open and load-bearing:
`FlowHint.probability` is `1.0` in the published runs, so every published cross-workload flow result
rests on the scheduler being *told* the future with certainty (§3.2); Phase 7 prices what is left
when it is not.

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

**The default still runs the old model.** Without `--engine-cache`, `TierPool` *is* the KV cache:
it admits, evicts by its own GDSF priority, refuses when full, and is the single source of truth.
The bit stays off so published results remain comparable across phases; *Which results the
correction moved*, below, says what changes when it is on.

### Ownership is per class

The boundary runs between workload classes. `accelerated(kind)` in `cache.rs` separates HBM from
DDR and comes close to drawing it -- but it is a **tier** predicate, not an **ownership** one, and
the table below cuts across it twice:

| class / resource | who owns the bytes | what the orchestrator does |
|---|---|---|
| **HBM partition** | **the orchestrator** -- it provisions the engine | decide how much HBM a replica gets, scale replicas, partition the hardware; a replica is one model on one node and its partition is what its weights leave |
| `KvBlock` | **the engine** (vLLM's block manager) | observe an approximate index; influence by directive; **route** |
| `WeightShard` | **the engine**, once loaded; the placement is **the orchestrator's** | decide which models load where, and when to unload -- slow, coarse, genuinely orchestration. In host DDR or on NVMe a model is a file in a node agent's cache: owned outright |
| `Snapshot` | **the orchestrator** -- it starts and stops microVMs | own outright: admit, evict, refuse |
| `ServiceHeap` | **the orchestrator** -- it scales replicas | own outright |
| offloaded KV in host DDR | the engine's KV connector (`LMCache`, NIXL) | observe; ownership follows the connector, so assume the engine's |

The two exceptions are the ones that matter: the orchestrator sizes an HBM partition the engine
allocates *within*, and an engine-owned offload tier sits *inside* host DDR's quota. So "HBM" is
not a synonym for engine-owned and "host" is not a synonym for orchestrator-owned. §8 returns to
what that costs.

The table has an executable form: `own::authority(kind, tier, question)` in `own.rs` (Phase 1),
total over every `(BlobKind, Tier)` cell and over both questions -- *capacity*, how big the pool
is, and *allocation*, which bytes occupy it right now -- since a class alone cannot say which side
answers which. Under a fleet the two host cells of `WeightShard` answer `Orchestrator` on both
questions, and the tier axis discriminates on allocation for the first time.

This is a **two-tier control system** for memory: macro authority over the hardware (provisioning,
partition sizing, model loading) stays with the orchestrator; micro, per-request authority (KV
block eviction) goes to the engine.

**The loss: no *per-block* admission control over inference state.** The orchestrator cannot
refuse a specific KV admission, choose an eviction victim, or hold a block against the engine's
will. An engine under pressure evicts, preempts and recomputes; it does not reject for lack of KV.
So refusal moves out of the ledger and into the router -- where it can still be authoritative,
because the orchestrator granted the partition and knows its capacity. A byte- or token-depth
threshold against that partition is a real check, not a naive queue-depth guess.

**That check has a hole worth stating.** It is authoritative only if its inputs are. Prompt bytes
are known at admission; output length is not. Two options, neither clean:

- **Admit against `max_tokens`.** A declared bound, so the check stays authoritative -- but
  `max_tokens` is routinely set far above actual output, so reserving against it strands most of
  the partition and refuses work the node could have served.
- **Admit against an estimate.** Usable, and **not authoritative**: an estimator wrong in the
  optimistic direction admits what the partition cannot hold, and the engine resolves the
  overcommit by preempting someone.

Per-block authority is not replaced by partition authority, then. It is replaced by a
**bound-versus-utilisation tradeoff** -- and the tradeoff is not a scalar, because an overcommit's
cost does not land on the request that caused it. The engine resolves it on its own order, blind to
tenant and priority (§3.8). Measured, the cost is **class-blind**: whichever sequence the engine
preempts pays, and interactive turns and task stages lose p99 about equally (Phase 3). So an
optimistic admission converts a *mean* utilisation gain into a *tail* loss spread across everyone,
and a sweep reporting mean service time prices that at approximately zero. Phase 9 corrected the
size and not the direction. At half the partition, the engine Phase 3 measured on ran a sixth to a
fifth of its decodes with no memory instead of making them wait (on the `belief` cluster), so the
half partition is an overload and the bracket understated it. The loss is class-blind on the engine's own order, and it
moves onto the throughput class once the router chooses the victim (below).

The asymmetry is also where a fix would live -- a **per-class mix of the two**, on an axis §4 turns
out to need:

- **Reserve conservatively for latency-bearing work** -- whatever a user or a blocked agent is
  waiting on. Admit it against a high quantile of the predicted output length rather than its mean,
  so the check stays near-authoritative for the class whose tail *is* the product.
- **Let throughput-bearing and `DraftOnly` work borrow the remainder** against the mean, as the
  designated victim when the conservative class expands into the slack.

§3.7 arrives at the same quantile from the routing side, which is the reason to believe the axis is
real rather than convenient. **Phase 9 built it, and the mean half survives only where memory binds
lightly.** Phase 3's admission sweep found the bracket narrow at the published partition and wide at
half of it -- `bound` refuses 17%, `perfect` 8.6%, and `none` refuses nothing but preempts 10.5% --
and while the loss is class-blind, a two-tier mix buys nothing a scalar bound would not until
requests carry a class it can act on -- the `slo` field (§4), declared since Phase 4 -- and the
router can choose the victim, which only cancellation allows (below). Phase 9 gave it both and
measured the form that works: **one declared quantile of each class's own observed distribution, a
priority order at a router queue, and a victim class** ([`phase-9.md`](phase-9.md), P5). Where the
classes are drawn alike one p90 and the tiered claim are a scalar; where they are not, the pooled
quantile lets the interactive first-token p99 reach 0.5-6.7 s with a batch class and a class's own
quantile holds it to 0.18-0.59 s, and 63-67 ms with a cancel. Borrowing against the mean lets more
throughput work in and the cancel evicts it: at 0.6x of the grant with prefill taking engine time it
finishes the throughput class 1.5-1.7x later than its own p90 does and the interactive class's first
token 2.5-5.1x later, so the mean half is retired there; at 0.75x it is level, or ahead on two seeds
of three. The half partition Phase 3 used is an overload for the engine once a sequence it cannot
hold waits (`--engine-wait`), and its bracket compared different sets of served requests.

**The catch is that the orchestrator cannot cash that understanding inside the engine.** Ceding
eviction ceded the **choice of victim**: the engine still preempts and recomputes under pressure,
but on its own order, and nothing the orchestrator can say makes it drop a draft to spare a chat
turn. The only preemption primitive left never crossed into the allocator -- **cancel the request
on the path it arrived on** (§2.3). Two-tier admission without that is a reservation policy with no
enforcement arm. vLLM's scheduler does reach the choice of victim for a running request's growth,
by priority, and not for admission, where a waiting request never preempts; the arm that is
authoritative for admission is the cancel (`phase-9.md` §1.2).

**The save: macro-orchestration, dataflow placement, and targeted host DDR arbitration.** HBM and
DDR are physically separate pools, so pricing an HBM KV block against a host DDR microVM cell in one
eviction shadow price is a category error: GPU prefill recompute (compute-bound, non-linear) and
microVM restore (I/O-bound, flat) do not compete for the same bus. Nor do expensive multi-GPU hosts
run arbitrary background services -- their DDR is provisioned for PCIe bounce buffers, NUMA-pinned
staging and offload connectors, and co-locating CPU compute there risks throttling the accelerator.

What remains is three real mechanisms:

1. **Dataflow locality.** The relationship between GPU inference and tool execution is *spatial*,
   not memory contention: place the dependent tool workload near the active GPU context (same
   node, rack, or zone) to cut serialisation, link latency and congestion tolls.
2. **Macro-scale orchestration.** What engines cannot see: multi-node gang admission, model weight
   loading across heterogeneous hardware, partition sizing, and the tenancy policy that follows
   from it (§3.8).
3. **Targeted intra-host DDR arbitration.** Where tool execution is co-located with inference,
   shadow pricing binds between **host-side offload connectors** and **ephemeral tool cells**
   (`Snapshot`). Soft floors beat hard partitions by balancing local tool working sets against
   offloaded inference state without starving the PCIe pipeline.

So the unified advantage is macro capacity and gang orchestration, joint dataflow placement across
topological boundaries, and targeted host DDR multiplexing -- not cross-hardware arbitration of HBM
bytes. Whether that beats siloed specialists is the empirical question.

### Which results the correction moved

Ceding allocation costs almost nothing where decode dominates: on the cluster, the
engine-allocating side moves mean service by -0.2% and p99 by -0.4%, and a 0.5x-2x partition
sweep moves it by at most 1.2%. What it changes is which results survive. Each row is the ledger's
result and the same run with `--engine-cache` (ledger *Engine allocation*):

| result on the ledger | with the engine allocating |
|---|---|
| soft floors beat hard partitions by 31.6% (oracle-tuned, 20k ops) | **grows**: +36.2% at 20k ops, +44.5% at 60k (+38.5% on the ledger); soft-floor weight hit 0.66 -> 0.77; unified memory goes from a tie (-4.0%) to +7.3%. The mechanism is weights, which inherit the slack KV used to borrow |
| fixed per-class budgets cost 4.0% service and 2.7pp goodput | **holds**: +4.2-4.5% service, 2.7pp goodput. The refusals are host classes', which stay owned |
| 90% of tool calls shipped to idle model-host DDR | **holds**: 90.1%. The connector's DDR sub-budget does not reach the `Snapshot` slice the option uses |
| all-or-nothing fan-out admission completes 22% more fan-outs | **holds on the router's partition check**, winning or tying wherever admission binds; the size is sensitive to the control crossing (+2% with none charged), because gang feasibility has a cliff at one model of slack |
| tool placement inverts under memory pressure | **holds**: ~10% of tool calls kept home under pressure, 65.7% at region, 57.9% under hard pools |
| KV ships within a rack and is rebuilt across a zone | **holds**: `scored + fetch` fetches 25.8% of what it acquires at rack (30.5% on the ledger), 0.1% from zone out |
| boundary ladder, origin round trip, congestion toll | **unaffected** |
| `announce` buys 11-18% task latency | **shrinks**: KV prewarm carried 50% of the margin on split memory and 71% on unified; with the engine allocating, announce buys 9.0% (split) and 3.2% (unified) from what it can still prewarm. **A dispatch buys it back, and more**: prefilling the declared downstream's prompt when its hint arrives buys 24-28% (Phase 5, §3.3) |
| 54-85% of DDR evictions are cross-class at 4 GiB DDR per node | **mostly gone**: 0-26%, and 0-2% on the scored arms. Most of it was the ledger allocating the engine's offload |

The pattern: results about **costs** survive; results about **per-block authority over inference
memory** do not; results about **budgets** survive in whichever pool the orchestrator sizes, and
grow where the partition hands the weights KV's slack. The two losses do not divide along HBM/host
lines -- `announce` was a result about per-block authority wearing a flow's clothes, and
per-eviction DDR arbitration was mostly the ledger allocating the engine's offload -- which is why
"host" is not usable as shorthand for "survives".

### Observability is not a stale exact view

`Control::Gossip` hands the scheduler a stale but **exact per-blob** residency set. No
observability system provides that -- you cannot scrape "does node 3 hold prefix X". Real
telemetry offers llm-d's precise-versus-approximate split:

- **`Metrics { interval }`** -- exact aggregates, no per-blob detail: hit rate, queue depth, pinned
  usage, batch occupancy.
- **`Events { loss }`** -- an approximate per-blob index from a KV event stream: fresh, lossy,
  probabilistic.

Replacing `Gossip` with these is a correctness fix, not a refinement. Under `--belief` the engine's
KV comes from an event stream and `Gossip` keeps only owned state (Phase 4). The scored arm moves by
under 0.05%; residency-greedy moves +21% at the published partition and -30% at half of it --
staleness was suppressing its herding in one regime and routing it onto evicted prefixes in the
other.

#### Step-aligned ingestion over ZMQ IPC

Out-of-band monitoring (Prometheus/OTel at 5-15 s) is far too slow for hot-path decisions.
Per-block RPCs at request time flood the host with interrupts. In-band response trailers arrive
only after a multi-second generation finishes. So the orchestrator ingests telemetry the engine
emits directly, batched at the engine's own step boundary:

1. **Step-aligned cadence (40-100 Hz).** Engines execute discrete forward iterations: prefill
   chunks ~10-50 ms, decode steps ~10-25 ms, with allocations, preemptions and evictions happening
   at step boundaries. Flushing at the end of each iteration rate-limits event volume to the step
   frequency and keeps belief fresh to within ~15 ms.
2. **Transport: ZMQ over Unix domain sockets** (`ipc:///tmp/poly_engine_{id}.ipc`), giving
   lock-free queueing and clean framing between Python workers (`pyzmq`) and the Rust
   orchestrator. **Chosen for adoptability, not speed.** At 40-100 Hz and ~400 bytes a batch, one
   engine costs ~0.6 ms of wakeups per second, and 6 us of crossing is 0.04% of the 15 ms freshness
   the cadence already sets -- every boundary on the ladder is free at this rate. What decides it
   is that **vLLM already publishes KV events over ZMQ** (`kv_events`, what llm-d's KV-aware
   routing consumes; confirm the event set against the targeted release), making this promotion
   tier 0/1 with no serving-stack change. Ring-grade transport belongs on the request path, where
   cost is paid per request and per decision (§2.2).
3. **Asymmetric, eviction-centric wire format.** The router knows which prefixes it dispatched, so
   allocations can be tracked optimistically. What it cannot guess is which blocks the engine
   **evicted** under pressure and where capacity stands. A 32-byte `TelemetryBatchHeader`
   (`engine_id`, `epoch`, `seq`, `free_kv_blocks`, `total_kv_blocks`, `queued`, `running`, `flags`
   for normal/yellow >80%/red >95%, eviction and allocation counts) followed by truncated 64-bit
   blake3 hashes. 100-400 bytes typical, under 15 KB/s per engine. Counted (Phase 10), the removals
   from GPU and host offload alone are 1,140-1,590 a second an engine, 9-13.5 KB/s of hashes; the
   full stream, stores included, would be 26-35 KB/s.
4. **Drop detection.** The router tracks `seq`; a gap means the ZMQ high-water mark dropped a
   message, so the engine's reported eviction count becomes a **lower bound** rather than a fact
   (§3.7 turns that into a decision, by extrapolating the missing evictions at the last observed
   rate instead of assuming none happened). Every 1-2 seconds, or immediately on a gap, the engine
   pushes a **sync batch** -- a prefix-tree snapshot or block bloom filter -- to reconcile drift.

The simulator models this channel's cadence, lag and loss (`--belief`, with `--loss`, `--recovery`
and `--silence`), not its transport cost, which is free at this rate whichever boundary carries it.

#### Two control loops, two clocks

Step-aligned ingestion closes the loop on **routing and admission**: the orchestrator learns of
memory pressure within 15 ms and diverts incoming prefill before the engine descends into
preemption cascades. It cannot close the loop on **provisioning**, which is physical: slicing new
partitions, pulling 140 GB of weights, initialising contexts takes 5-30+ seconds.

That gap is the point. Near-instant micro-actions (diversion, backpressure, cancellation) buy the
time that slow macro-actions (provisioning, weight swapping) need, which is what makes the two-tier
model viable.

**The gap has a number (Phase 6).** A replica that changes model serves nothing for its start time
and the copy of its weights -- 344 ms from a peer, 43 ms from its own agent's cache, 8 s cold --
and leaves the node's KV behind. On eight nodes at 500 req/s under a rotating model mix, a placement
that is always right costs +0.5% over the pooled engine; one whose loads take 2 s costs +1.0% more,
8 s +7% and 30 s +77-82%; one that is 10 s late costs +17-19% more than one that is on time and 30 s
late +110-120%. A placement made once collapses: 6.2-6.6 s of mean service and half the decodes
arriving at a full batch. A planner that moves when the loss suffered pays for the move lands
+8.5-9.6% over the clairvoyant one, and its lateness is the whole of the difference: +3.6% at a 1 s
interval, +56% at 15 s. So the start time is the number worth asking a serving stack for, and the
planner is a bound on lateness.

**Freshness matters little, for a structural reason.** Under `--belief`, mean service stays within
0.16% of the exact view at every point measured -- up to 20% batch loss with no recovery, with the
belief wrong 37-60% of the time -- because the terms that would have to be fooled, `engine` and
`congestion`, read the in-flight count, which the integrated router knows exactly: it carries every
request and response (Phase 4). So "how fresh is your view" is the wrong question to organise the
experiment around. The right one is "does the score price congestion" -- and a score that does
needs no fresh view to avoid concentrating.

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
| per request | dispatch to the engine | sidecar -> localhost HTTP, a 15.5 us loopback round trip | direct, UDS: **5.2-7.5 us** (the ladder's least stable rung, non-monotone in payload) | sockets; a ring for short-request classes |
| **per decision, in an argmin** | **routing and policy hooks** | **`ext_proc` callout, open stream: 36 us (measured)** | **in-process, 0 ns / wasm 13-25 ns / ring 70-180 ns** | **`Native`, `Wasm` and `Ring`** |

The dispatch row is the seam that cannot be removed -- removing it means implementing the engine,
which §1 forbids. It can only be made cheaper, and a shared ring would make it two orders of
magnitude cheaper. That is a serving-stack change (promotion tier 2), and §6 says how to ask for
one: measure the regret that justifies it first.

**The last row sets the architecture, and `machine.rs` already argues it.** `decide()` charges
`Control::Unified` nothing and `Control::Query` one crossing per placement, incrementing
`control_rpcs` by `self.active.len()`: a fan-out query costs one crossing of *latency* but N
crossings of *work*, which is what caps the decision rate. The extension seam has the same
multiplier at a higher rate.

`best_scored` is an argmin over candidates, so a pluggable scoring term runs once per candidate. An
`ext_proc`-shaped callout on a stream the hook service keeps open costs **36 us**. At 4 nodes that
is **142 us per placement** -- more than a warm FaaS invocation's modelled ~129 us. At a fleet of 32
it is **1.1 ms**, and the scheduler's decision rate becomes the cluster's throughput ceiling. The
same fleet over a ring costs 2.1 us; over WASM, 0.4 us.

That is not a performance difference, it is an **expressiveness** difference. At 36 us a hook runs
once, at the gateway, with the policy pre-collapsed into a single score. At 13-25 ns it runs per
candidate, per eviction candidate, per telemetry batch. A warm WASM call measures *below* the ring,
so this is where the README's zero-cost-extension claim cashes out for a sandboxed extension
specifically, not just an in-process one.

### 2.3 The honest accounting

Phase 8 made the sidecar question an arm -- `data_path: { Integrated, Sidecar, SidecarPluggable }`,
charged on the same trace through the same scored placement policy, so only the data path varies.
The realized tax is **43.97-74.97 us per request** across the two deployment shapes: an `ext_proc`
stream the processor keeps open (36 us a callout) and Envoy's documented default of a new stream per
HTTP request (63 us). That is at `--fanout 0.10` with **`d = 1.23`** decisions per request,
*measured, not assumed* -- a gang's agents share one decision but each tool call is another, so `d`
rises with agentic traffic.

The tax is measured; the denominator is not. Shares are `T / S` for the two shapes, arithmetic on
the measured tax:

| work being scheduled | provenance | what the sidecar path adds |
|---|---|---|
| agent turn, ~1 s of decode | modelled | under 0.01% |
| 30 ms classification or extraction | illustrative | 0.15-0.25% |
| 1 ms of work | illustrative | 4.4-7.5% |
| warm service request, 250 us | `SERVICE_EXEC_NS`, a **chosen constant** | 18-30% |
| warm FaaS invocation, ~129 us | `FAAS_EXEC_MIN_NS` 40 us + `U[0, 160 us]`, a **chosen constant** | 34-58% |

The bottom two are workload constants, not measurements.

**So argue from the crossover, which needs no denominator.** `S* = T x (1/f - 1)` for the realized
tax `T` (`phase-8.md` §1.1). Across four runs on this host, the sidecar path costs more than 5% of a
request below **0.83-0.90 ms** with the stream kept open and **1.38-1.43 ms** on Envoy's default,
and less than 1% above **4.34-4.71 ms** and **7.18-7.43 ms** respectively. The claim that survives
any constant, and either shape: *the data-path choice binds for sub-millisecond work and dissolves
an order of magnitude above it.* Whether that matters is a question about the **request mix**,
answerable from published FaaS duration distributions rather than polyproto's `exec_ns`.

**A pluggable sidecar policy is a different, larger number, and conflating the two overstates the
deployed tax.** `SidecarPluggable` charges the hook once per *candidate* rather than once per
*placement* -- what extending the sidecar's own scoring logic out of process would cost, not what
llm-d's Endpoint Picker (which scores in-process and returns one decision) pays. It measures
**148.14 us/request**, crossing at **2.81-3.04 ms / 14.67-15.83 ms** -- roughly 3.4x the deployed
tax, the 4-node candidate count. The two arms stay separate so neither number is quoted for the
other's question. The figure is a **lower bound**: a fan-out pays one `decide()` for the whole gang
while `place_agent` runs an argmin per agent, so a hook that really ran per candidate would be
consulted `agents x candidates` times.

**For inference alone, killing the sidecar is not worth doing on latency grounds, and this
document does not claim it is.** A decode-bound turn sits three orders of magnitude above the
crossover; no plausible constant moves it. Four things make it worth doing anyway, and only the
first is about speed.

1. **One data path serves both denominators.** An inference-only stack can buy a proxy hop out of
   the decode budget; a FaaS control plane cannot, which is why none of them have one on the
   invoke path. Polyproto serves both from one path, so choosing Envoy makes the FaaS and service
   classes pay the inference class's overhead budget. No silo can make this argument, because no
   silo has both denominators.
2. **Expressiveness in the argmin** (§2.2). Hook cost decides whether a policy can be a function of
   the candidate or must be a constant attached to the request.
3. **One belief, one actor.** Residency is a belief maintained at ~15 ms freshness. If the decision
   point is a hop from the belief, the belief is stale again when used, and divergence has two
   referents instead of one.
4. **Cancellation is the only preemption left.** The engine still preempts; §1 ceded the choice of
   victim, so none of it runs in the orchestrator's priority order. What remains is to stop sending,
   and to stop a stream already in flight, since an abort propagated to the engine frees its blocks
   at the next step boundary. Measured (Phase 9), the cancel frees the pins, the reservation and the
   batch slot together, a **continuation** -- the prompt plus the blocks already decoded -- beats a
   restart, which finishes the batch class 26-183% later, and an llm-d-shaped utilisation gate costs
   the evicted class 2.2-4.4x in completion against a claim where memory binds. That makes the
   request path the **enforcement arm for every priority policy in this document** -- two-tier
   admission (§1), `DraftOnly` preemption (§4) and the tenancy trade (§3.8) are reservation policies
   whose only teeth are a cancel. A sidecar can carry a cancel; it carries it one hop from the
   component that decided to issue it, and when the client simply disappears it decides on its own
   partial view whether that was a preemption or a retry.

### 2.4 The split moves; it does not vanish

Polyproto still has two processes, a global scheduler and a node agent. What changes is where the
seam falls, and it falls where §1 puts the memory seam:

| | clock | §1: who owns the bytes | §2: who owns the decision | state (§1) |
|---|---|---|---|---|
| **provisioning** | seconds to minutes | HBM partition sizing, model placement | replica counts, the P:D ratio, which tenants share a replica set | record |
| **routing** | per request | -- | cross-node routing, P/D pairing, gang admission | soft |
| **local** | per request, per step | KV block allocation and eviction (engine) | dispatch, backpressure, policy hooks (node agent) | soft, and the engine's own |

A decision goes to the tier holding the state it needs, and the tier boundary is crossed at the
rate of the **coarser** tier. The sidecar pattern inverts this: it cuts the path at a seam every
request must cross, rather than at one only globally-informed decisions cross.

Provisioning and routing can share the global process; they share neither a clock nor a store. Only
provisioning writes to the system of record, and nothing on the request path waits for it.

### 2.5 Prefill/decode: pair the request, size the fleet

The sidecar's most substantial job is one polyproto already has machinery for, at one of its two
timescales.

**Per request**, choosing a prefiller and a decoder is one placement decision over a pair, not two
independent ones: the transfer between them is a link cost `Topology` already prices, and its
magnitude depends on both endpoints. It is also all-or-nothing -- a prefill placed without a decoder
to receive its KV is wasted GPU work -- which is the primitive behind fan-out admission, applied to
a gang of two with a direction.

**Per fleet**, prefill is FLOPs-bound and decode is memory-bandwidth-bound, so the right **P:D
replica ratio** shifts with traffic shape: long prompts and short outputs want more prefill
capacity; agent turns with short prompts and long generations want less. That is a slow capacity
decision, so it sits in §2.4's provisioning tier and in Phase 6. Per-request pairing without it
only distributes the imbalance evenly.

The obvious objection: §1 removed the ability to refuse a per-block KV admission, so how can a pair
be refused? Because this is a **placement** refusal made before dispatch -- against the granted
partition budget, engine slots and queue depth -- not an eviction decision inside the engine.

The transfer itself stays out of the data path: NIXL or Mooncake move bytes GPU-to-GPU over RDMA,
and the orchestrator owns the **handshake, not the bytes**. Same discipline as §1 -- the value is
in deciding the pair, not carrying their traffic.

**"Owns the handshake" means owns the pairing, not the transition.** The other reading puts a
control-plane round trip in the middle of TTFT: prefill finishes, the engine reports up, the
orchestrator then dispatches the decode. Both endpoints are chosen *before* dispatch and both are
told then, so prefill completion signals its paired decoder directly, peer to peer alongside the KV
transfer, and the orchestrator learns of it on the telemetry clock like everything else. Relaying
it instead would cost one intra-cluster round trip -- hundreds of microseconds within a rack, past
a millisecond across zones -- on a prefill of tens of milliseconds: single-digit percent, pure
loss, and avoidable by construction.

An orchestrator picks the pair and sets the ratio; a sidecar picks a prefiller from a list. That
difference is a **coupling-tier-2** joint decision, and coupled % (§3.4) sizes it.

**Measured (Phase 6).** Eight replicas of one model, `--prefill-time` on, against eight aggregated.
Three things §2.5's statement leaves out:

- **A prefiller needs its prefix.** A session's next turn is four new blocks at the decoder that ran
  the last one and the whole chain at a prefiller that did not, so the prefill tier duplicates the
  cache as well as the compute: a prefiller does 1.9 times the work it replaces at one in eight on
  the published mix, and a list, which scatters a session's turns, 2.4-2.7 times. Letting a
  prefiller fetch the prefix from the decoder brings it to 1.1 and buys about two points.
- **Not pairing is an option of the same argmin.** `joint` pairs when the prefiller's queue, its
  work, its toll and the transfer are under what the prefill costs at the decoder, which includes
  every sequence in flight there. A list pairs every prefill, and its prefiller is past saturation
  at every ratio `joint` prefers: +3,500% at one in eight on fresh prompts, where `joint` declines
  76% of the pairs.
- **The right ratio moves with the mix and with the load.** On the published mix `joint` at one
  prefiller in eight is 1.6-1.7% faster at 300 req/s and 2.3-2.6% faster at 500, and every larger
  ratio loses (+104-107% at three in eight at 500 req/s). With fresh 64-block prompts at 0.15 a
  request and 300 req/s the best split is two in eight, 9.6-9.8% faster, and three in eight
  7.2-7.4%; at 500 req/s one in eight is the only split that wins. A planner's second pass ends at
  2, 3 and 0 prefillers in the cells where one, two and one were best, and lands within 1-6 points
  of the best static split in each.

Coupled % between `joint` and `independent` is 83-96% when `independent` pairs only prefills over
10 ms and 4-20% when it pairs them all: the quantity is a property of the threshold a pairing rule
was handed. The fresh-prompt stream is a lighter load than the one the ratio was first predicted
on (decodes stretched by 13% where 22% had been measured), so the fresh rows are sizes for that
stream.

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
     decode loop as head-of-line blocking that looks exactly like engine slowness. For
     text the first is cheap, and Phase 9 retracts the sentence that called it an occupant of the
     pool §5 prices: if every decode in flight on a node stalled for its whole decode the buffer
     peaks at 4,600 tokens a node on the published workload and 8,900 with a batch class, 0.9 and
     1.8 MB at 200 bytes a token, 0.01-0.02% of a node's host DDR, against 15 KiB of HBM a token
     for pausing the decode instead. The rule is to buffer and never pause; the sentence stands for
     audio and image output, where a stalled stream carries tens of kilobytes a second.
   - **Retry, timeout and hedge against a stateful backend.** Re-issuing a partly-decoded request
     is not idempotent and throws away a warm prefix; hedging one duplicates prefill. These are
     scheduling decisions wearing transport clothes, which is an argument for holding them here,
     but they still have to be made.
   - **Whose crash ends the stream.** In the scheduler's address space a scheduler crash ends every
     stream through it, and Phase 10 prices that: the node agent aborts the engine's sequence, and
     with a 0.1 s takeover the crash throws away 61-73 decode-seconds and costs 63-75
     request-seconds beyond the outage's own, 284-370 at 1 s with a restart and 233-276 with the
     client's continuation, against nothing beyond the outage with the stream held. Holding it below
     the scheduler -- the node agent keeps the engine's connection and its buffer, under 1.8 MB a
     node, and the client re-attaches through whichever scheduler answers, as OpenAI's background
     mode resumes a stream `starting_after` a sequence number -- makes a crash cost its outage and
     no more. The scheduler owns the decision and the node agent the connection.
2. **In-process extensions trade isolation for the 0 ns.** `ext_proc`'s 36-63 us buys a separate
   address space. A first-party ABI extension can corrupt the scheduler, and a segfault takes the
   node's control plane with it.
3. **Ecosystem.** SPIFFE/mTLS wiring, the WASM filter catalogue, observability that assumes an
   Envoy in the path.

Item 2 is a scope decision and resolves below. **Item 1 does not**: these are semantics no library
chooses for us, they are east-west by definition, and §8 lists them as the largest gap between this
design being right and being shipped.

**North-south stays commodity; east-west is ours.** Directly from §2.2: a proxy is acceptable
where its cost amortises per connection and unacceptable where it is paid per decision. TLS
termination, HTTP/3, WAF and DDoS handling are per-connection concerns at the internet edge and can
stay behind a commodity proxy without touching anything claimed here. What gets replaced is the
per-decision path from the trust boundary inward -- also the only part llm-d puts a sidecar on.

**Isolation becomes a costed choice**, which is `README.md`'s ring 0 / ring 3 split made
measurable. WASM has two rows because they answer different trust questions: a warm sandbox shared
across calls, and a fresh instance per call.

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

"Can this extension be trusted" then has an answer in nanoseconds, priced by the ladder rather
than settled by an architecture review -- and a different answer depending on whether the trust
boundary is per tenant (share the instance, pay 13-25 ns) or per call (pay ~9.5 us for isolation
that survives a hostile input). Per-call isolation costs **400-750x** a warm instance, so it leaves
the argmin entirely: it belongs off to the side with the foreign-runtime row, not inside a
per-candidate score.

### 2.7 What was measured

Both measurements this section called for are done.

- **The ladder** (Phase 0) has a warm-instance WASM rung, a fresh-instance WASM figure, an
  `ext_proc` rung in both deployment shapes and a batch-timed `Ring` rung, and publishes the
  per-decision cost of a policy hook at each isolation level (§2.2, §2.6).
- **The `data_path` arm** (Phase 8) charges measured seam costs per request -- `Machine::decide`
  charges the hook and `run_here` the dispatch hop -- and counts decisions, dispatches and
  candidates exactly rather than assuming them (§2.3).

The crossover is a closed form: `S* = T x (1/f - 1)` follows from `overhead share = T / (T + S)`,
so a simulator that charges `T` on the critical path can only reproduce that arithmetic, and the
simulated share agrees with it exactly on a fan-out-free trace. What the simulator adds is the two
things the arithmetic cannot supply: `d`, the measured decisions per request, and the fleet-size
ceiling. An unsharded scheduler scoring `ext_proc` callouts saturates between 19 and 20 nodes at
this workload's decision rate, against ~1000 for a warm WASM hook (§5).

---

## 3. What changes in the engine

Ordered by what makes the rest trustworthy: the boundary first (3.1), then the estimates that
replace declarations (3.2-3.3), then the apparatus that makes any of it measurable (3.4-3.6), then
confidence and tenancy (3.7-3.8), and three smaller points (3.9-3.11). Built: 3.1, 3.3, 3.4, 3.5,
3.6, 3.7, 3.11, 3.10's term, and 3.8's measurements (Phase 6). Open: the flow estimator in 3.2
(Phase 7) and the per-tenant axis on `Quota` in 3.8.

### 3.1 A `Telemetry` boundary

**Built** (`tele.rs`, Phase 1; `RequestView`, Phase 4). One type mediates everything a policy may
read. Owned state stays directly accessible to the ledger; the *scheduler* reaches residency,
costs and load only through it. Inferred quantities live behind it with their uncertainty attached
in the shape the score consumes -- for residency that is `P(resident)` (§3.7), not a flag;
observed quantities arrive sampled.

This is `sched_lm`'s `RequestView` discipline -- "body observables, never the workload's
ground-truth class" -- applied to the whole engine rather than one policy signature. The scoring
path takes a `RequestView` -- chain, dependencies, a token count, the request's class and its
`slo` -- so it cannot name the blocks a request will produce or its `exec_ns`, and the compiler
finds every site that tries. The token count is the one remaining piece of truth, and a bit
replaces it: exact by default, the observed mean under `--observables` (§3.2).

### 3.2 Predicted flows and predicted lengths replace declared ones

**Flows.** Replace `FlowHint { probability: 1.0, lead_ops, payload_bytes }` with an estimator over
observed history, one per (tool, session-class): P(this turn calls a tool); which tool, as a
distribution; re-arrival gap as EWMA mean **and variance**, so confidence is available rather than
invented; and payload size.

`Hierarchy::anticipate(id, weight)` already takes a probability and already caps an announced
access at one real access. It has only ever been fed `1.0`. Feed it the predicted probability and
the mechanism becomes honest with no change to the ledger.

This unlocks the falsification the prewarm results need: **how much of the coupling-tier-1 win
survives when the hint is an estimate?** Against a perfect declaration, announce buys 11-18% task
latency on the ledger and 3.2-9.0% with the engine allocating (§1). Against an EWMA with real
variance it buys less, and the amount it loses is the honest value of the mechanism.

**Measured (Phase 7).** Hints have three grades: the declared downstream, the declared template
only (the leading blocks the flow shares), and a template learned per function. On the published
trace in its published order, prefill-ahead cuts the flow downstream's stall by 63% with the
declared downstream, 47% with the template only and 24-31% with a learned template (which exists
for 74% of flows when their upstream arrives); at half the partition 40-49%, 37-40% and 17-25%.
Those are open-loop figures: the trace submits a downstream 28 ms after its upstream whether or not
the upstream has finished, and 100% of an agent turn's tool calls arrive before the turn ends.
Released when the upstream finishes, the same hints cut the stall by 0-5%, and the learned template
by nothing (-0.1 / +2.9 / -0.0%). The published prefill-ahead and announce figures stay on record
as open-loop figures. The name of a tool arriving in the stream warms its sandbox as well as
prediction does, since a restore takes about 3 ms against a decode of a second.

**Lengths.** For the score, the observed running mean of completed output lengths is enough: under
`--observables` it lands within 0.13% of the exact length on this workload, and no estimator can do
better here, because output length is independent of everything the router can see (Phase 4). A
quantile of it makes the score worse: tokens scale every candidate's engine terms by the same
factor, so a quantile only shifts load's weight against acquire everywhere at once.

The consumer that wants more is admission. §1's two-tier admission reserves latency-bearing
classes against a **high quantile** of remaining output, so what the estimator must publish for it
is a *predictive distribution* over remaining tokens, conditioned on the observables §3.1 permits
and the inferred workload class. That distribution has to be **calibrated** rather than merely
accurate, since a quantile drawn from a miscalibrated distribution is a number with a decimal point
and no meaning -- the same obligation §3.7 puts on `P(resident)`, for the same reason.

### 3.3 Retention directives

**Built** (Phase 5; `--directives`, `--retain`). Two forms, split by who allocates.

*On the engine* a directive is RFC-0001's, carried on a dispatch: retain until a deadline (`50;
ttl=<window>`) or evict first (`-1`). It ranks evict-first < unmarked < retained, expires on its
lease with no message, and is soft under pressure: when every candidate is marked and live the
soonest-expiring is evicted and counted, so a mark never causes a preemption. A touch without a
directive leaves a live mark alone. The engine acknowledges an honoured mark on its stream as the
store event's trailing `priority` and `retain_until`, and the router believes a mark only when it is
acknowledged (§3.6). A *dispatch* is the other way to act on engine memory: a prefill of a declared
downstream's prompt, sent when its hint arrives, makes the engine allocate the blocks by its own
rules.

*On the ledger*, for the classes the orchestrator owns, retention is a decision:
`Entry.retain_until`, a soft pin with a deadline in place of the unbounded `expect` bump, and
`evict_first`, which sends a blob to the front of its class. This is RFC-0001's vocabulary applied
to owned state, and a deadline is strictly better than the bump for the reason it always was: a
priority inflation with no deadline never self-corrects when the prediction was wrong, while a lease
does.

Measured, a directive is worth almost nothing on this workload, and a dispatch is worth a great
deal. None of `announce`'s KV margin was retention: the bump alone on what was resident carried
between -5% and +9% of it, and the prewarm -- admitting what was not resident -- carried all of
it, so a retention directive cannot buy back what Phase 3 removed. Holding what a declared flow
needs costs nothing and buys nothing (0.0-0.1pp of task latency); retaining a fan-out's parent chain
is within ±0.5% of stall at the published partition and costs 0.2-1.0% at half of it; evict-first on
one-shot scopes is within ±2%; and an oracle emitter that knows every block's next use, a ceiling
and never an arm, buys at most 1.4% of stall and 0.1% of service with the sign changing across
seeds. Prefill-ahead buys 24-28% of task latency on one node against the ledger's 10-18%, and on the
cluster cuts a flow downstream's stall by 64% at rack, 31% at zone and 7% at region. On the ledger a
deadline is the better form: bounded state, and one to two points of task latency even with honest
hints, because a hinted prefix's interior otherwise keeps an inflated priority indefinitely.

The unified angle: a directive priced in the same ns/byte as everything else can be weighed against
what it displaces, where in a siloed stack a retention hint is advisory and unpriced. On this
workload the weighing comes out at almost nothing either way, because routing, peer fetch and the
connector's offload tier already capture what an eviction order could.

### 3.4 Oracle, regret, coupling

**Built** (`oracle.rs`, `Machine::oracle_pick` and `finish_regret`, Phase 2). "Oracle" names three
different things, only one of which is buildable: an *information* oracle (the model's own cost
over true state) is identical to the scored policy by construction and its regret is trivially
zero; a *clairvoyant* oracle over the whole future trace is intractable, since a decision changes
the residency the next decision faces. What is built is the **realized-cost oracle** -- at each
decision, read-only, price what the simulator would actually charge to serve this request at each
candidate, against truth rather than belief, and take the minimum. It is a lower bound on what a
better policy could win, not an upper one: it is myopic, so it cannot see a policy whose value is
the residency it creates -- `flow only` beats `scored` on service time at a 512 MiB flow payload
while carrying far larger regret.

Three metrics, imported from `sched_lm`:

- **Routing regret** -- `policy_cost - oracle_cost` computed on *the policy's own state*, which
  separates decision quality from state quality, and decomposed into four causes: `execution`,
  `heuristic`, `belief` and `model`.
- **A clairvoyant eviction baseline** (`oracle-belady`), distinct from the routing oracle, so
  ledger quality and placement quality are separable too.
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

Measured: `scored`'s regret is all model gap -- its heuristic and execution gaps are exactly zero at
the published defaults. Coupling is a statement about a regime: memory coupling is 0.0% at the
`distributed` defaults, where nothing binds, and 54-85% of evictions at 4 GiB DDR per node on the
ledger -- 0-26% with the engine allocating (§1). Locality coupling is 0.8-1.7% of scored decisions
in both regimes. Clairvoyant eviction wins 22.8pp of `KvBlock` hit rate over GDSF and loses 3.7% on
stall, because GDSF gives up the cheap hits. Results stated as regret no longer carry the fairness
caveat a delta against a hand-built baseline does; results not re-run through the oracle still do
(ledger *Method*).

### 3.5 Regime mix, including wait

**Built** (Phase 2). Each request is classified resident, wait, transfer or recompute by its
largest term, which makes the acquisition decision legible and comparable with `sched_lm`'s
wait / transfer / recompute split. The congestion toll prices queueing, and the wait regime names
it.

### 3.6 Divergence as a first-class signal

Once cache state is observed rather than owned, the gap between what the router believes is
resident and what the engine holds is itself the metric -- the only honest measure of how well a
directive-plus-belief architecture tracks reality. Report it, do not smooth it:

```
Divergence(e, t) = |Belief(e) \ Actual(e)| / |Belief(e)|
```

It spikes on four things: eviction cascades the engine runs between batches; telemetry drops (a
`seq` gap, after which the router's eviction count is a lower bound); ignored retention directives;
and **preemption**, which an eviction-centric stream never reports -- the router believes blocks of
a sequence the engine could not place until that step's batch arrives without their stores.
`Belief::apply` reconciles them by the absence of the store. Tracking it isolates whether a routing
mistake came from a bad cost model or from a belief that drifted.

**Built** (Phase 4, Phase 5): phantom share `|B \ A| / |B|` and miss share `|A \ B| / |A|` are
sampled per engine, and each divergent block is attributed to exactly one cause: a removal not yet
due, dropped or silenced, by its batch's fate; an optimistic dispatch the engine never stored, or
one it stored whose store was lost with no repair path to clear it (*stranded*); and, for a miss, a
block re-dispatched while its eviction was undelivered. Under replay every phantom at the published
partition is an undelivered removal, under no recovery 1-8% are stranded, and at half the partition
about 17% are never stored. Misses are zero at every sample.

An ignored directive is not a cause. It changes no event: the engine evicts as its LRU would and
publishes the removal like any other, so it cannot make the index wrong. It changes confidence,
which is why the router believes a mark only when the stream acknowledges it -- byte-identical to no
directives when the engine ignores them -- and why one that believes its own requests is
over-confident only where removals go undelivered (0.975 realised against 1.000 predicted, at half
the partition with 20% loss and no recovery).

### 3.7 Confidence has to reach the argmin

**Built** (`belief.rs`, `Machine::plan`, Phase 4), with `--scoring` selecting the rule.

§1 requires inferred quantities to carry a confidence, §1's telemetry detects a sequence gap, and
§3.6 publishes divergence -- but none of that changes a placement unless the score consumes it. An
argmin over means is blind to spread: a node whose belief just went stale keeps whatever mean it
last had and keeps winning on it. The feared failure is **"the router herds onto whichever node
has stopped reporting"**, because silence reads as calm. Measured, it does not occur on the
integrated path, where load is read from the traffic the router carries; it does for a router that
reads load from the stream, which herds or starves by luck of the node's load when it went quiet --
and `P(resident)` changes none of that. It is §2.3's third reason for integrating, measured.

**A risk penalty is the wrong shape.** `cost = E[cost] + lambda * sigma[cost]`, with sigma widened
by belief age, gap count and divergence, fails twice. First, the distribution is not one a mean and
a standard deviation describe: a prefix is resident or it is not, the acquire term is ~0 ns or a
full prefill recompute, and nothing lives between them. Sigma on a bimodal variable is largest
exactly where the mean is least informative, so a penalty scaled by it moves for the right reason
by an arbitrary amount. Second, lambda is dimensionless, which is §3.9's objection to
`alpha * overlap - beta * load` reappearing inside the fix: a knob with no exchange rate, swept per
deployment, is a policy wearing a constant's clothes.

**The bimodality is the structure, so price it.** What is uncertain is a binary fact -- does node
`e` still hold block `b` -- and both branches already have costs the model computes:

```
E[acquire] = P(resident) * cost_hit + (1 - P(resident)) * cost_rebuild
```

The only new quantity is `P(resident)`, and §1's telemetry supplies it. Under LRU over block hashes
a block survives until the pool turns over past its stack depth, so the estimator is a **turnover
count**, not a fitted curve: with `V` blocks evicted since the belief was last confirmed and a
partition of `B` blocks, `P(resident) ~ max(0, 1 - V/B)` under a uniform-rank assumption, sharpened
by how recently the router last dispatched that block. The `TelemetryBatchHeader` carries eviction
and allocation counts and free/total blocks for precisely this.

Confirmation is **per block**, which makes `P` a survival curve along a chain -- non-increasing,
because a request touches every block of its hit prefix. A session's chain has at least two
confirmation times: the tenant prefix, touched by every session of that tenant, and the session's
own turns. So the expectation is a sum over depths, `sum_k P(depth = k) * cost(depth k)`, and a
peer's segment is one fact, the survival of its deepest block.

Three properties make this better than the penalty it replaces:

- **A sequence gap acquires a meaning rather than a magnitude.** A gap does not widen a variance;
  it makes `V` a **lower bound**, and the honest move is to extrapolate at the last observed rate.
  A node that goes quiet has its eviction count estimated from the pressure that preceded the
  silence, so its `P(resident)` decays on its own, with nothing tuned.
- **Staleness becomes self-limiting**, because the belief is *used* as a probability rather than
  *penalised* as a risk.
- **Everything stays in nanoseconds.** No new constant enters the score.

**Risk aversion does not vanish; it moves to where it has units.** One convexity survives the
mixture: if a node went quiet because it is in a preemption cascade, the miss branch is not a clean
recompute but a recompute behind a queue, and an expectation over a heavy tail underweights the
tail a latency SLO is about. The answer is not a dimensionless multiplier but a statement of
**which quantile of the predictive distribution a class is scored on** -- itself in nanoseconds,
and a field §4 needs:

| class | scored on | behaviour |
|---|---|---|
| latency-bearing | p90 of predicted cost | conservative; pays for certainty |
| throughput-bearing | the mean | utilisation-seeking; absorbs the tail |

For a two-point mixture this has no free parameter at all: the p90 of
`{cost_hit w.p. p, cost_rebuild w.p. 1-p}` **is** `cost_hit` when `p >= 0.9` and `cost_rebuild`
otherwise. Along a chain, the q-quantile is the plan at the deepest depth whose survival is at
least `q`. Scoring latency-bearing traffic at p90 therefore means *assume the block is gone unless
belief is at least 90% confident*, and the quantile the SLO names is the entire input. It is also
the object §1's two-tier admission reserves against, so one statement per class governs both the
routing score and the admission bound. The quantile applies to the acquire term only: displacement
is a price, engine load is exact on the path, and output length wants the mean (§3.2).

Measured (Phase 4):

- **Flapping** at the threshold did not appear at `q = 0.9` -- placement churn stayed within 10% of
  `expected`'s -- but at `q = 0.99` churn rises with unrepaired loss (37% to 43%).
- **Calibration.** The turnover estimator is close to the diagonal with replay and no loss (0.865
  predicted, 0.929 realised), and **over-confident** where a dropped batch is never recovered (0.96
  predicted, 0.70 realised at half the partition), because the unknown evictions land on the
  stalest blocks an LRU takes first, not uniformly. The chosen node is not worse than the field, so
  the argmin is not exploiting the miscalibration.
- **The declared SLO buys nothing a user would see.** `expected`, `quantile 0.9` and `slo` land
  within 1% of each other on service p99, on three seeds.

Pricing the belief as a probability also makes an accident into a principle: residency-greedy
improves when a stale view stops it concentrating (§1), and a probability does the same on
purpose -- and unlike a lambda, it is the effect with a unit.

### 3.8 Tenancy is soft, and the engine is tenant-blind

The target is Kubernetes-shaped **soft** multi-tenancy: teams inside one company, separated by
quota and policy, not mutually hostile. §1's cession has a consequence for that which is not
obvious.

**Ceding eviction cedes tenant fairness on that pool.** vLLM's block manager evicts LRU over block
hashes and has no tenant concept. So when one team's agent loop floods a shared engine with unique
prefixes it evicts another team's warm blocks, and the orchestrator cannot choose otherwise:
directives are advisory and the engine may ignore them. Under the ledger's model this *was*
expressible -- `TierPool` evicts by a GDSF priority the orchestrator controls, and a tenant term
could go into it.

**That blindness is contingent, not structural, and the difference decides how to ask.** A tenant
id on a block group and a per-tenant eviction floor is bookkeeping over opaque hashes: it needs
nothing about block layout, attention scheme or quantisation, so it is *not* the model-specific
dependency §1 refuses. What it is, is a change to somebody else's scheduler -- promotion tier 2,
under §6's rule that you measure the residual regret first and ask second. The design must
therefore assume a tenant-blind engine while being able to say what a tenant-aware one would have
been worth. Calling it impossible would be wrong; assuming it available would be worse.

**What remains is the partition**, which lands tenancy back on a decision the orchestrator owns:

| | one shared partition | a partition per tenant |
|---|---|---|
| cross-tenant prefix sharing | **a price only where something is shared.** `work.rs` roots each tenant's prefix at its own block, so on the published workload no block is touched by two tenants (0 of 384,159 touches) and the price is zero; with a prefix per model under every tenant's, 9-10% of touches cross tenants and the hit rate rises 2-3 points for no measurable service | no |
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

The first row prices the whole question, and it is layout-independent: however the GPUs are
sliced, a node's KV pool is `HBM - tenants x weights`. On declared capacities rather than any
measurement here -- a 70B model at fp16 is ~140 GB, an 8xH100 node holds ~640 GB -- that is ~500 GB
of KV at one partition, ~360 GB at two, ~80 GB at four and infeasible at five. **The first split
costs 140 GB, more than a quarter of the KV pool, and the curve steepens from there.**

So fairness on KV is purchasable only in units of partition, at three named prices: duplicated
weights, lost cross-tenant prefix sharing, and narrower batches. There is no hint-shaped
workaround. And because the quantum is that large, the realistic unit below a handful of tenants
per model is a **replica**, which makes tenancy a question of *which tenants share a replica set*
-- a routing and capacity decision, in §2.4's provisioning tier, and the reason Phase 6 settles
isolation and sizing in one act rather than two.

**Measured (Phase 6): "no hint-shaped workaround" holds for the cache and not for compute.** The
engine's cache is where §3.8 said it would be unfair -- 12-22 points of hit rate separate the
busiest tenant from the quietest twelve of twenty-four, and another owner's request causes about
90% of GPU evictions -- and it is not where a neighbour's damage lands. A tenant sending fresh
64-block prompts for a fifth of the run costs the others +1-3% while prefill is free and +11% to
+62% once prefill takes engine time, at 500 req/s on eight replicas of one model: the damage is the
steps its prefills lengthen, and every prefill is a dispatch the router makes. So the router is the
arm for compute:

- **A quota on prefill work at the router** refuses the neighbour's excess -- 79% at 0.25
  engine-seconds a second, 58% at 0.5, 17% at 1.0 -- and costs the others +1.9%, +3.5% and +9.0%
  in the burst against +11% shared, and nothing outside it.
- **A replica set** costs the others a replica's decode slots: one of eight +3% in the burst and
  nothing outside it, two of eight +8% and +12-14%, since six replicas are past the published
  load's knee. At the same allowance the set is ahead of the quota or level with it at every share
  of the run the neighbour bursts for, from 5% to 100% (+3.5-4.5% against +20% at all of it): a
  quota admits its engine-seconds wherever they land, and a set confines them to one replica.
- **A cap on sequences in flight per tenant** cannot tell a neighbour from a busy tenant: it refuses
  16-27% of the others' requests.
- **A second engine on the node** remains the worst instrument, on the pre-measurement's +91-96%.

The tenant-aware block manager §3.8 anticipates as promotion tier 2 was built as a ceiling, a
per-tenant floor in the engine's eviction: at the published partition it moves any tenant group's
mean service by at most 0.3% while raising the quietest twelve's hit rate by 5-10 points; at half
the partition with prefill taking engine time the quiet half gain 0.2-1.8%. That is the regret
number to ask with, and it is small where the partition is not tight.

**The quota axis is missing on the ledger and built at the router.** `Quota` is per *class*:
`band`, `floor` and `limit` are all `[_; BlobKind::N]`. Soft tenancy needs a second axis per tenant,
and the two interact the standard way -- when a tenant is over quota and an under-floor class wants
its bytes, one has to yield. A request now declares its tenant, as it declares its `slo`, and the
router meters prefill work and sequences in flight per tenant; host DDR's `Snapshot` and
`ServiceHeap` still have no tenant axis, and no measurement says it binds.

One reframing falls out. The soft-floors-beat-hard-partitions result **is** this argument in
miniature: a soft floor lets a class borrow idle capacity where a hard partition strands it, which
is Kubernetes' requests-versus-limits trade one axis over. Read that way it is less a claim about
memory arbitration than about **quota policy**, and it is the form in which it survives §1's
correction.

### 3.9 What the score already does

Two objections arrive reliably enough to answer here rather than in review.

**"Routing on prefix overlap causes herding."** Correct, and already priced. `Machine::plan`
scores five named terms -- `acquire`, `displaced`, `handoff`, `engine`, `congestion` -- and
`Engine::congestion_ns` charges what joining a batch does to *every sequence already in it*. In the
recorded runs congestion alone changes 4.7% of placements and load 13.6%. The failure shows from
the other side too: a stale view helps residency-greedy at the published partition and hurts it at
half of it, while the scored arm moves by under 0.05% either way (§1).

**"Use `alpha * PrefixOverlap - beta * TokenLoad`."** The same idea, weaker. A weighted sum needs
alpha and beta tuned per deployment and they are not commensurable -- a unit of overlap and a unit
of load have no exchange rate, so the tuning *is* the policy. The cost model denominates every term
in **nanoseconds**, which have an exchange rate by construction. Nothing is tuned because nothing
needs converting, which is also why a term can be added without re-tuning the others.

That rule is why §3.7 turns down the risk penalty: `lambda * sigma` would have been the first
dimensionless constant in the score, and the shape of the uncertainty -- one binary fact with two
already-priced branches -- made it unnecessary. The single input there that is not a nanosecond is
the quantile a class is scored on, and a quantile is a **declared SLO, not a fitted constant**: it
comes from the workload, means something before any sweep, and is the same number §1's admission
reserves against.

### 3.10 A shared L2 tier, priced before it is built

The acquire argmin is `min(resident, fetch from a peer, this node's NVMe spill, rebuild)`.
Production stacks add a fifth option: a **cross-node NVMe pool** shared by every replica (SageMaker
HyperPod mounts Curvine over FUSE for this), turning a cold replica's miss into a read rather than a
recompute.

Its value is a crossover. A shared read is slower than a rack-local P2P pull and faster than
recomputing a long prefill, so it wins in a band -- long contexts, cold replicas, peers that do not
hold the prefix -- and loses outside it. **The term is in the argmin** (`--shared-l2`, Phase 3); the
tier is not built. Measured, it fires on KV within a rack -- 1.5-20.7% of served requests read KV
from a 64 GiB pool at rack -- and on weights only from zone out, because a contiguous segment pays
the hop, the seek and the PCIe launch once. Building the tier is infrastructure, and §6 names the
way to ask: measure the residual regret that would justify it.

### 3.11 Tracing at the rate the ladder allows

Standardised spans across the decision path (llm-d defines contracts like
`gen_ai.latency.time_to_first_token` and `llm_d.kv_cache.lookup.cache_hit`) are worth adopting,
and §2.2's rule decides where they go rather than taste.

**One span per request**, recording the winning node and which of the five terms decided it: yes,
and built (`span.rs`, Phase 2). A request costs hundreds of microseconds at minimum, so a span is
lost in it, and this is the `observed` category doing its job. **One span per candidate inside the
argmin**: no. That is the per-decision rate, where a bare syscall (97 ns) already exceeds an entire
ring round trip. Instrumentation costing more than the decision it describes has stopped being
instrumentation.

Not a compromise -- the same rule that rejected `ext_proc`. An orchestrator that gets this wrong for
telemetry has reintroduced as observability precisely the overhead §2 removed as architecture.

---

## 4. The taxonomy as a scheduler input

[`taxo.md`](taxo.md) is ten patterns across four dimensions. That is right as analysis and wrong as
an engine input: the scheduler needs a handful of fields it can act on, not a pattern name.

### Four dimensions, five fields

| dimension | scheduler field | status |
|---|---|---|
| Control flow | `flow: None \| Declared \| Predicted(dist) \| Fanout(n)` | 4 of 4 built; **Predicted** is a learned template (§3.2, Phase 7) |
| Knowledge grounding | which blob classes, and their sharing shape | KV / snapshot / weights / **retrieved chunks** built (Phase 7) |
| State and time horizon | `retention: evict_first \| until(deadline) \| durable` | **built** for the first two (Phase 5): `evict_first` declared over one-shot scopes, `until(deadline)` from a hint's lead, both consumed by §3.3; `durable` is **built** for sandboxes (Phase 7): a cell that is demoted and never dropped; only the loss of its node takes it, unless it was copied when marked (Phase 10) |
| Authority to act | `authority: ReadOnly \| DraftOnly \| SideEffecting` + `pause_tolerance` | **built** (Phase 7): derived from MCP's `ToolAnnotations` defaults, pessimistically, so an unannotated tool is `SideEffecting`; drives speculation, the lease and the log write. `pause_tolerance` is not built as a field |
| *no dimension -- see below* | `slo: Interactive \| Throughput` | **declared** (Phase 4, `--throughput`); read by the scoring quantile (§3.7), by admission and the router queue (Phase 9). `Deadline(t)` waits for something to consume it |

**The fifth field has no dimension behind it.** §1's admission and §3.7's score both need to know
how much of the cost distribution a request is priced against, and that is a property of the
**latency objective**, not of authority: a `ReadOnly` search can be the thing a user is blocked on,
and a `SideEffecting` write can be the last step of an overnight job. Authority correlates with it
and is not it. `taxo.md`'s patterns *imply* the axis -- a conversational assistant is interactive,
batch inference is not -- but none of its four dimensions expresses it, so the scheduler needs a
field the taxonomy does not supply. Two independent mechanisms arriving at the same missing number
is the reason to think it is real rather than a knob.

Unlike the other four it is **declared, not inferred**: a caller states a latency objective the way
it states `max_tokens`, so this field needs none of the estimator machinery below. That is also
what keeps §3.7 free of a tuned constant -- the quantile arrives with the request instead of being
swept into existence.

Each of the ten patterns becomes a named preset over those fields, the way `sched_lm` takes
`--mix tool=0.5,rag=0.3,oneshot=0.2`. Three consequences:

**Control flow maps onto machinery that exists.** Single-call is a plain request; fixed multi-step
is the declared flow; parallel/delegated is the fan-out; dynamic multi-step -- the defining agentic
case -- is definitionally the one that cannot be declared and must be predicted. That is why
declared hints felt natural: they are the *fixed*-pipeline case, and the prototype has been testing
the easy half of the dimension.

**RAG-grounded was missing, and is built (Phase 7).** Retrieved chunks are shared across *sessions* with Zipf
popularity, not chain-structured like a KV prefix, so they evict differently from anything modelled
and contend with KV for the same pool. `sched_lm` models this (`--rag-docs`, `--rag-zipf`);
polyproto has no equivalent. It was the cheapest high-value addition, and the one grounding mode
that changes the ledger's contention shape. Under prefix caching a finite partition reuses 36% of the
chunk tokens a call retrieves when the chunks arrive in a fixed order and 12% when they arrive in
relevance order (k = 5, 10,000 chunks), against 52% and 28% with an infinite cache; reuse
independent of position finds 64% (96% infinite) and nets 54% after recomputing about 15%. The
position-independent figure is a residency counterfactual, not an executed arm.

**Durable memory breaks an invariant, and the invariant holds (Phase 7).** Every class in the ledger is evictable at a priced cost.
Durable state must never be *lost*, only demoted -- a correctness constraint, not a cost tradeoff.
Before Phase 7 that existed only as `ServiceHeap`'s serving pin; a durable cell is now pinned to
the cold tier and demoted, never dropped, and no durable cell was lost in any run. The one fault
it does not hold against is losing the node (Phase 10): a long-running run that loses a node half
way through the arrivals loses 18-20 durable cells and leaves a program each holding lost state --
a correctness failure that turn latency does not show (0.1-0.4%) -- while a copy made when each cell
is marked loses none, for 6.0-6.2 GiB over the run, 0.14-0.29 MiB/s a node, 0.012-0.024% of a zone
link.

### Authority drives speculation, preemption, and idempotency

Authority is not an audit label. It sets what the scheduler may speculatively execute, branch,
preempt or checkpoint:

1. **`ReadOnly`** (search, reads, analysis, summarisation) -> **speculative dispatch and parallel
   pre-warming.** When turn N predicts a tool call with probability P, the orchestrator can
   pre-warm the tool microVM or dispatch the query concurrently with the final decode tokens. If
   the model veers away, the branch aborts with zero rollback.
2. **`DraftOnly`** (drafts, staged patches, proposed invites) -> **burstable scheduling with
   zero-compensation preemption.** These can occupy burstable slack and be reclaimed the moment
   high-priority work arrives, with no saga. The reclaim primitive differs by pool, and §1 is the
   reason: in host DDR the orchestrator still evicts, so a draft's `Snapshot` cell is taken
   directly; inside an engine it does not, so the only lever is to **cancel the request** and
   requeue it, which frees its blocks at the next step boundary. Same policy, two mechanisms, and
   the second is why §2 keeps the path.
3. **`SideEffecting`** (transactions, mutations, webhooks, deployments) -> **strictly
   non-speculative.** Durable checkpoint before dispatch -- the logged tier (§1) -- and
   non-revocable leases so execution cannot be torn down mid-flight.
4. **Human-approved** (unbounded pauses) -> demote the whole execution context out of HBM and host
   DDR into cold storage until the approval callback arrives.

**Measured (Phase 7).** Speculating read tools moves turn latency by -1.0 to +1.6% on coding tools
at 27% top-1 accuracy, -0.1 to -1.3% at 50% and -3.2 to -3.7% on research tools of 2-10 s
(open-world reads allowed, wasting twice what they save), against -17% for a perfect predictor.
Unpriced, it shortens the queue other sessions' tools wait in, since a hit takes its tool out of the
queue; priced, it stays within 2% of none. A lease pins 7% of a node's DDR at its peak at 8 GiB and
27-30% at 4 GiB, and reclaiming drafts moves turn latency by under 1% in either direction. Retention
through a tool call with Continuum's TTL moves turn latency by 0.00%: the excess rebuild a call pays
(0.03-9 ms) is a preemption of the sequence, which a mark cannot undo. The two timers disagree on
19% of turn boundaries; one suspend decision for the KV and the sandbox differs from the pair on
45.5% of them, frees 63% of idle time to the timers' 65%, and costs under 0.15% of turn latency at 8
and at 2 GiB.

### Generator-side truth, scheduler-side inference

**The taxonomy exists twice, and telemetry is the only bridge.** The workload generator uses the
full taxonomy as ground truth to synthesise traces. The scheduler never sees the class; it infers a
profile from observables. The one deliberate exception is `slo`, which is declared rather than
inferred -- and marking it as such is the point, since a field the caller supplies is not evidence
that inference works. Then classification accuracy, and the cost of getting it wrong, become
measurable -- what `class_aware` plus `ToolGapIndex` do in `sched_lm`. `RequestView` (§3.1) already
keeps the truth out of the score, which is what turns `taxo.md` into the experiment's independent
variable. The same split applies to tenancy (§3.8).

**Measured (Phase 7).** From observables a request's class is right for 60.6% of requests at their
first call, 85.7% at their last with its history and 79.1% over every call; conversational, pipeline
and multi-agent requests look like agentic ones at their first call. The consumers hardly care:
speculation's gate is unchanged, a claim by inferred class overruns 2-5% less than by the true
class, and joint suspension frees 15% more, -5% and -1% idle time on three seeds. A role's own p90
claim overruns on 9-13% of its agents against a pooled claim's 63% for reviewers, and changes fan-out
service by -2.8% to +3.3% at the one partition where claims bind.

### Per-pattern coupling is the falsifier

Run coupled % per taxonomy cell on both axes. The output is a two-column table saying, for each
pattern, whether a unified orchestrator can help at all.

Phase 7 built it. Locality coupling on the programs is 0% for one-shot, extraction, conversational,
retrieval and batch programs, 13-15% for tool pipelines, 4% for agentic, 7% for multi-agent and 2%
for long-running ones; memory coupling is 0% for every pattern, because no program contends with a
second class in host DDR. On the published trace at 2 GiB it is 4-41% by pattern, driven by the
services and function cells. The table is smaller than the expected shape below: coupling lives on
flows, and the host-memory half of the claim for long-running agents did not appear.

Expected shape, stated in advance so it can be wrong: batch inference and one-shot generation show
near-zero coupling on both axes (independent requests, nothing to co-decide); multi-agent and
long-running agents show high locality coupling (shared context, cross-node dataflow, atomic
admission) and moderate-to-high memory coupling on the host. If that fails, the thesis is narrower
than claimed and this document should say so.

---

## 5. Emergent properties

An advantage is *emergent* if no silo can produce it independently and it is not merely a hint
away. Five are simulated, and all five were re-run with the engine allocating KV (§1); two are
measured end to end (Phase 8); five are proposed.

**On the first five:** simulated on modelled constants, so treat each as directional rather than
sized. The properties worth most are the ones whose *existence* does not depend on a constant.

**Within host DDR (orchestrator-budgeted memory).** Budgeted, not owned: the orchestrator sizes the
pool and sets its floors, but one occupant it arbitrates -- the engine's offload tier -- is
engine-owned. The decision measured is a **budget**, and the budget stays the orchestrator's
whoever fills it.

1. **A cross-class DDR shadow price.** One `marginal_price` balances microVM warm pools against
   offload tiers and local services. Its budget form holds and grows: soft floors beat hard
   partitions by 32% on the ledger and 36% with the engine allocating, and fixed per-class budgets
   cost ~4% service and 2.7pp goodput. Its per-eviction form mostly does not: with the engine
   allocating, 0-26% of DDR evictions are cross-class, against 54-85% on the ledger.
2. **Placement options static partitioning forecloses.** With soft floors the score ships 90% of
   tool calls to idle host DDR for a 53% warm rate; with hard pools that slice is capped however
   much memory sits free beside it and the warm rate falls to 19%. This concerns `Snapshot` cells,
   which the orchestrator owns, and it is unchanged with the engine allocating.

**Across topology.**

3. **Congestion and residency in one argmin.** Neither an inference router nor a FaaS control plane
   can price "place the tool call near the active GPU context unless the link is congested or local
   memory is full". Measured consequence: the tool-placement decision **inverts** between
   unpressured and memory-bound regimes, and again at region distance.
4. **Cross-workload atomic admission.** All-or-nothing placement of a fan-out across nodes is not
   expressible per request. Where it binds: 22% more fan-outs completed and 10% less inference
   stall; it holds on the router's partition check, though its size is sensitive to the control
   crossing (§1). It is also where a lost node shows first (Phase 10): refused fan-outs rise from 0
   to 23-28 at the published load and from 28-44 to 103-142 at 0.75x of the partition.
5. **One currency for host hints.** A prewarm, a retention directive and an eviction priced in the
   same host DDR units can be traded against each other. A siloed hint is advisory and unpriced. On
   the ledger the trade comes out lopsided (Phase 5): a host hint's retention half is worth 0.0-0.7%
   of task latency and its prewarm half 4.1-6.0%.

**Measured end to end (§2, Phase 8).**

6. **One data path serving two denominators.** Control-plane overhead is a fraction set by the work
   being scheduled; the **crossover** is its durable form (§2.3): 0.83-0.90 ms / 4.34-4.71 ms with
   the sidecar's stream kept open, 1.38-1.43 ms / 7.18-7.43 ms on Envoy's per-request default. An
   inference-only stack buys a proxy out of the decode budget; a FaaS control plane cannot. **Only a
   unified orchestrator is forced to pick one path for both**, which makes "integrate, do not
   proxy" a consequence of unification rather than a preference. It has a price under failure
   (Phase 10), at 250 req/s: with the streams held and a 0.1 s takeover a crash costs as much as
   the sidecar's tax saves in under 90 s, and with a 1 s takeover in 1.9-3.2 hours; with streams
   that die, 1.0-1.8 hours at 0.1 s and 4.4-7.5 hours at 1 s with a restart; under a 15 s lease
   2.9-5.8 weeks. A fail-open proxy's own window, 15 s of hash-only routing, costs 12-53
   request-seconds, less than any restart that kills streams, so the property holds as a
   consequence of unification only while the integrated process crashes no more often than about
   once a minute or two with its streams held and once an hour or two with them fate-shared.
7. **Extension cost as an expressiveness bound, and a fleet-size ceiling with a number on it.** A
   hook at 36-63 us (`ext_proc`) must be a constant attached to the request; at 0-180 ns (native
   through ring) it can be a function of each candidate inside the argmin. The same ladder prices
   trust. For a per-candidate hook the bound is quadratic in fleet size (`phase-8.md` §1.4): at
   this workload's decision rate (`d = 1.23`), one unsharded scheduler scoring `ext_proc` callouts
   saturates between **19 and 20 nodes**; scoring a warm `Wasm` hook, **~1000**. No silo needs this
   ordering, because no silo is simultaneously a scheduler and a data plane.

**Proposed, and the reason to do §3 and §4.**

8. **Learned cross-class retention (Phase 7: measured, and small).** "This agent returns to this tool in ~800 ms, confidence 0.7"
   driving a FaaS warm-cell retention decision priced against what holding it displaces. A silo can
   receive that as a hint; it cannot weigh it. Phase 5 bounds what it could be worth through a
   directive on this workload: an emitter that knows every next use buys at most 1.4% of stall.
9. **Authority-driven speculative scheduling (Phase 7: measured, and small where tools are short).** Pre-executing `ReadOnly` tool calls concurrently
   with decode, and scheduling `DraftOnly` work into burstable capacity with zero-compensation
   preemption -- reclaimed by eviction where the orchestrator still owns the pool and by
   cancellation where the engine does (§4). Both require knowing the authority class, which is a
   property of the *workload*, not of any one runtime. Its non-speculative core is built (Phase 5):
   a declared flow's downstream prefilled when its hint arrives. The cancellation half is measured
   (Phase 9): with a victim class, a cancel takes the interactive first-token p99 to 63-67 ms where
   a pooled quantile lets it reach 0.5-6.7 s, and what it costs lands on the throughput class's
   completion.
10. **Joint prefill/decode pairing and ratio.** Disaggregation as a two-member gang with a
    direction, plus the fleet ratio behind it (§2.5). A sidecar picks a prefiller from a list.
    Measured (Phase 6), it is worth 1.6-2.6% at one prefiller in eight on the published mix and
    9.6-9.8% at two in eight on fresh prompts, and the same split costs +104-107% at three in eight
    at 500 req/s: the property is that the ratio is a function of mix and load, and that declining
    to pair is the decision a list cannot make.
11. **Tenant fairness across a tenant-blind engine.** An engine evicts LRU and cannot see tenants,
    and a partition is physically an engine -- so isolation is paid in a duplicated copy of the
    weights as well as in lost prefix sharing and batch width (§3.8). That makes it a *capacity*
    decision, not a policy toggle, and only a component that both sizes partitions and routes into
    them can make the trade deliberately: shared where prefixes overlap and load shapes are
    compatible, separated where one tenant is bursty enough to evict the others and the HBM exists
    to pay for it. A siloed router can only pick one side in advance. Measured (Phase 6), the damage
    is engine time and not cache, so the cheaper arm is the router's quota on prefill work
    (+1.9-3.5% to the others where sharing costs +11%), with a replica set the hard version of it,
    and the engine-side floor a ceiling worth under 1% at the published partition.
12. **Two-dimensional coupling as a published quantity.** Not an advantage but the measure of one.

Plainly: the largest defensible effects are **topological dataflow co-placement**,
**macro-orchestration of weights, partitions and gangs**, and **targeted host DDR multiplexing** --
not cross-hardware arbitration of HBM bytes. Macro-orchestration of weights has a size and a
condition (Phase 6): one model per node at the partition its weights leave is within 0.3% of the
pooled engine and 71% ahead of weights cached per request with a batch per model, and it holds only
while the placement follows the mix -- a placement that is late by 30 s costs 110-120% more than
one that is on time. Once the orchestrator stops pretending it allocates KV
blocks, what remains is a system solving three problems existing stacks fail at: joint dataflow
placement across network boundaries, macro capacity and gang coordination, and authority-aware
speculative execution. Phase 3 showed the first two survive the correction; Phase 7 tested the third: speculation is worth
about 1% on coding tools and 3-4% on research tools at published accuracies, and a perfect predictor
bounds it at 17%.

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
- **Using residual regret to justify infrastructure before it is built.** The transfer regime is
  not expressible in stock llm-d, so the sim's job is to show the regret that would justify a KV
  connector tier. That is the correct use of a simulator and the posture this prototype copies --
  for the shared L2 tier (§3.10) and for a request-path ring (§2.2).
- **A promotion path.** Promotion tier 0 config / tier 1 plugin / tier 2 serving-stack change, so a
  finding has somewhere to go.

### Where a unified system needs more

- **It is inference-only.** One workload class in the cost model: no warm pool, no service
  replicas, no cross-class contention, so the effects dominating every result here are invisible to
  it.
- **One pool per node.** No HBM/DDR split and no offload tier priced as a recovery path, so
  eviction cannot be priced as `min(rebuild, promote, fetch)` -- a mispricing worth two orders of
  magnitude in this prototype's displacement term.
- **No refusal in the argmin.** Load shed is tallied in the live stack; the offline cost model
  never declines a request, so admission is not part of the decision.
- **Unpriced retention.** A directive is per-request and advisory, with no global shadow price to
  weigh a pin against what it evicts.
- **No atomic multi-worker admission.** Nothing expresses a fan-out that must be placed whole.
- **TTFT-only objective.** Once decode cost depends on batch occupancy, **placement moves execution
  and not just waiting**, and a wait-only metric misranks policies. That is why polyproto's
  headline is end-to-end service time rather than stall, TTFT's analogue, with goodput beside it to
  catch the arm that looks fast because it refused the expensive work.

### The naming collision

`sched_lm` uses Tier 0/1/2 for *promotion effort into production llm-d*; polyproto uses Tier 1/2
for *information-sharing versus joint decisions*. Same words, orthogonal axes. **Every use is
qualified**: *promotion tier* for effort-to-ship, *coupling tier* for how tightly a decision is
joined. The two meet inside §2 alone -- a shared ring is promotion tier 2, P/D pairing is coupling
tier 2 -- which is what makes the qualification a rule rather than a suggestion.

---

## 7. Constants are a method, not a debt

This prototype is for understanding architecture and tradeoffs, so modelled constants are
legitimate. What makes them legitimate is **sensitivity, not precision**:

> For every constant, know which conclusions are robust to it across its plausible range, and which
> flip. Publish the ones that flip.

Three cases show why.

**A wrong value.** The FaaS snapshot cost was once a cold-container constant (200 ms / 32 MiB,
linear); the Firecracker lazy-restore model (~9 ms, roughly flat in image size) **inverted the
ns/byte ordering** and reversed a headline about which bytes are cheapest. The conclusion depended
on the constant entirely.

**The same constant, still untested.** The restore model is `4 ms + bytes * 0.15 * 1.0 ns/byte` for
UFFD page-in -- but `UFFDIO_COPY` (a major fault that copies) and `UFFDIO_CONTINUE` (a minor fault
mapping a page already in the page cache) differ by enough to move that slope several-fold, and work
restoring snapshots through hardware decompression or overlay VMAs reports 3.7-4.3x cold-start
improvements over the naive path. Since §5's second property is an *ordering* claim, and orderings
are worth only the gap between the constants producing them, any result ranking snapshot bytes
against KV bytes must be published across that range.

**A wrong label, which is worse.** `exec_ns` is chosen -- `FAAS_EXEC_MIN_NS + rng.below(SPAN)` --
but was once published as "measured", and an argument was built on the label before anyone checked.
A wrong constant gets sensitivity-tested; one wearing the word *measured* is exempted from the test
by its own label. Hence: **provenance is part of the number.** Any figure quoted here names where it
came from, or it is not quoted.

So the rule for this document's proposals: a result may rest on constants, but it must come with
the range over which it holds. Where a crossover is the finding -- KV ships within a rack and is
rebuilt across a zone; the data path binds below a millisecond -- the crossover's *position* moves
with the constants while its *existence* does not, and the existence is the claim. Where an
ordering is the finding, it is worth only the gap between the constants producing it.

---

## 8. Feasibility

### Is the correction tractable?

**Yes, and more cheaply than the size of the claim suggested.** Phase 1's census found **13** entry
points that assume allocation authority over engine state, every one in `cache.rs` and none in
`machine.rs`: the scheduler held residency and cost *queries*, never allocation decisions, so it had
no authority to disclaim. The dynamic census showed admission is under a third of what the ledger
does on the engine's behalf -- demotion, cascade spill and superseded-copy removal are the rest --
so Phase 3's engine-cache model had to cover the offload and promote paths, not just admission.
Phase 3 did, behind `--engine-cache`, and adds none of its own to the census.

The shape mattered more than the count. `accelerated()` is a tier predicate, not an ownership one,
and §1's table cuts across it twice; ownership is `own::authority(kind, tier, question)`, a function
of kind, tier and whether the question is capacity or allocation.

### What gets harder

Worst first.

1. **Per-block admission control over inference state disappears.** An engine evicts and
   recomputes; it does not refuse for lack of KV, so `Admission::Pending` stops being reachable for
   an individual `KvBlock`, and for `WeightShard` once Phase 6 moves weights. What it does *not*
   lose is the partition: the orchestrator sized it, so its capacity stays an owned fact and a
   byte- or token-depth check against it stays authoritative. The loss is **granularity, not
   authority**. Three things lean on the granularity:
   - `Hierarchy::could_admit`, hence **gang feasibility**. A fan-out can no longer be refused block
     by block, only against the partition budget, host memory and engine slots. The primitive holds
     on that coarser test (§1), though its size is sensitive to the control crossing.
   - `can_satisfy`, hence the **downstream-aware gate**, which becomes a partition-budget,
     queue-depth and host-memory judgement.
   - **Goodput as an outcome.** Refusals for inference move to the router, so the numbers change
     shape even where they do not change size: router refusals and engine preemptions are counted
     apart and never summed.
2. **Tenant fairness on KV goes with it** (§3.8). LRU is tenant-blind, so a noisy tenant's eviction
   of another's blocks is unpreventable inside the engine, and a partition is an engine, a second
   copy of the weights. Measured, it matters less than it reads: the damage a neighbour does is
   engine time, which the router can meter, and per-tenant eviction floors -- bookkeeping over
   block hashes, askable at promotion tier 2 -- are worth under 0.3% of service at the published
   partition and up to 1.8% to the quiet half where the partition is tight.
3. **Displacement becomes an externality, not a decision.** Routing still *causes* the engine to
   evict; the orchestrator does not choose what. The term stays in the score as an estimate from
   observed eviction pressure -- noisier and lagged. §3.7 covers that only partly: the turnover
   estimator behind `P(resident)` prices *this* node's belief going stale, not what dispatching here
   does to someone else's warm prefix. Displacement's uncertainty has no estimator yet.
4. **Two sources of truth, permanently.** What the engine holds and what the router believes drift.
   Not a defect to engineer away; it is the architecture, and the drift is a metric (§3.6).
5. **The cross-class arbitration thesis narrowed.** The budget form survives and grows; the
   per-eviction form mostly does not (§1). What remains is quota policy over host DDR and the
   placement options it opens, not per-eviction arbitration of engine state. (Called the
   *cross-class arbitration* thesis, not "the unified memory thesis": in this project "unified
   memory" means the **hardware** -- Apple silicon's single pool against the datacenter's split.)
6. **Owning the path means owning stream semantics -- not HTTP.** The framing, parsing and TLS are
   a linked library's (§2.6). What does not delegate is what to *do* with a stream: turn a client
   cancel into an engine abort, decide where a stalled stream's tokens accumulate, and choose retry
   and hedge against a backend holding warm state. It threatens no result here -- the simulator
   charges seam costs from a measured ladder and never parses a byte of HTTP -- but it is the
   largest gap between this design being right and being shipped, and one piece of it has a
   modelling consequence, which Phase 9 measured and retracted for text: a stalled stream's buffer
   peaks under 2 MB a node, a counter and not a term of the ledger. A departure does matter: with
   20% of clients leaving, an unpropagated one holds 200-830 sequence-seconds of decode and costs
   the interactive first-token p99 1.7-3.2x where memory binds.

### What gets easier

- **The engine cache is simpler than a `TierPool`.** LRU over block hashes: no quotas, bands, floors
  or refusal. Modelling vLLM means modelling *less* policy, not more.
- **Model agnosticism becomes checkable.** Once the orchestrator cannot see inside the engine, the
  interface it consumes is small enough to write down: capacity, hit/miss/eviction counts, queue
  depth, load and unload cost per model, declared context window, a directive channel, and **request
  cancellation**. As built, the catalogue states a model's size, its start time and its context
  window; a load costs the start time plus the cheapest copy, and a replica's window is the smaller
  of the model's and what its partition holds. Anything beyond that list is a model-specific
  dependency, and the compiler enforces the list.

  Cancellation is listed apart from the directive channel because it is the one entry that is not
  advisory. A retention directive the engine ignores costs a worse placement, and §3.6 counts it;
  an abort the engine ignores costs the priority model its only enforcement (§2.3). Both are
  model-agnostic -- neither needs to know what a block contains -- but an engine that cannot be
  asked to stop is one this design cannot schedule priorities on. As built (Phase 9), cancellation
  is three things the engine provides: an abort that frees a sequence's batch slot and pins and
  drops its never-written output, as vLLM's `finish_requests` does; a per-request priority by which
  it orders the sequences waiting for blocks (`--engine-wait priority`); and the tokens a sequence
  has decoded, returned with the stream (`return_token_ids`), so that the router can re-send them
  as the prompt of a continuation. The reservation the cancel releases is the router's own.

  Context window earns its place because it is *declared metadata*, like size and load time, and
    Phase 6's fleet uses it: dispatching a 200k-token prompt to a 32k model produces a
  failure and a retry costing more than the placement saved. It also marks where the list stops.
  Routing by **model accuracy** needs per-model, per-workload evaluation, which is model-specific by
  definition and is what §1 gave up KV ownership to avoid. A capability *gate* is in scope; a
  quality *ranking* is not, and the test is whether the engine can state the fact about itself.
- **Weights become orchestration rather than caching** (Phase 6) -- more realistic, and a
  capability no arm has today; the ledger never sees a weight again under `--fleet`.

### The system of record

**FoundationDB**, chosen over TiKV, etcd and CockroachDB against five requirements -- four from
§1's durability tiers, one from Oxide's experience -- rather than from a benchmark:

1. **Strict serializability for invariants.** The record holds quotas, capacity and membership, the
   tier whose job is invariants. Under snapshot isolation a quota check write-skews: two admissions
   read a tenant's usage, both see room, both write different keys, both commit. FoundationDB's
   read conflict ranges reject the second by default; TiKV's Percolator transactions are snapshot
   isolation and need the read keys locked (`lock_keys`) or funnelled through a shared counter.
2. **Small values, bytes elsewhere.** A record row is a quota, a lease, a partition size. Anything
   larger -- WASM extension modules, model manifests, snapshots -- goes to object storage with a
   content hash in the record, as Agent Substrate does with its snapshots. FoundationDB's 100 KB
   value limit then never binds, and a value near it is a design smell.
3. **No transaction spans a physical action.** Loading weights takes 5-30+ seconds (§1) and moving
   a tenant between replica sets longer, and no store's transaction spans either. Macro actions are
   state machines of short transactions, and the Phase 6 planner reads, computes outside any
   transaction, and commits behind a version check -- FoundationDB's own advice for its 5-second
   limit, and the pattern Kubernetes already follows with `resourceVersion` on etcd's narrower
   transactions.
4. **Leases built in a layer.** FoundationDB has no leases and watches single keys: membership
   leases use read versions as a clock, and change notification is a versionstamped log plus a
   watch on one key.
5. **A license its owner has no reason to change.** Apache 2.0, held by an owner with no database
   business to monetise. CockroachDB's relicensing, below, is the case this rules out.

Two more count against TiKV. Its Rust client describes itself as "not suitable for production use"
([`client-rust`](https://github.com/tikv/client-rust)) -- the mature client is Go's -- so a Rust
server's leverage runs the other way for a Rust caller; and without TiDB the application is its own
MVCC garbage collector. Against etcd the deciding property is **size**, not write rate, which the
tiers already took out of the record: registrations for agent fleets at Agent Substrate's target of
hundreds of millions are far past etcd's recommended 8 GB.

Against CockroachDB the evidence is Oxide's. Their
[RFD 53](https://rfd.shared.oxide.computer/rfd/0053) is the closest published requirements
document to this record: strong consistency "within a particular scope (e.g., within a datacenter
or region)", optimistic concurrency "similar to HTTP conditional requests", hands-off operation,
zero planned downtime, online schema migration. Its design point is the instance lifecycle -- 1.2
to 1,670 requests per second, ~150 ms per access, ~100 GiB at 1,000 racks -- which is this
document's record tier with nothing faster beside it, because RFD 53 predates inference and agent
scheduling as a common control-plane concern. Oxide chose CockroachDB, and the choice has since
become a fork: Cockroach Labs went proprietary in November 2024, Oxide found the free tier's revenue
cap and mandatory telemetry "entirely unacceptable", and it now self-maintains 22.1/22.2 and "will
not upgrade CockroachDB beyond 22.2" ([RFD 508](https://rfd.shared.oxide.computer/rfd/0508)) --
the maintenance of a database without the fit of one built for them. CockroachDB's multi-region SQL
is still the most developed of the candidates and would express this section's per-region and
global split in one cluster; if SQL comes to matter, YugabyteDB, also on Oxide's shortlist, is the
openly licensed form of that idea.

**Multi-region is a property of the tiers, not of the store.** FoundationDB's multi-region mode
keeps data in at most two regions and commits in one of them. Satellites hold the latest commits in
a second datacenter of the active region, so losing the primary datacenter loses nothing
acknowledged; but every transaction from the other region pays a WAN round trip for its read
version and its commit, and a region cut off from the active one cannot run a transaction at all.
That is disaster recovery rather than active-active -- still more than etcd, whose quorum across
regions puts the WAN on every write.

So the shape is **a record per region** for what must keep working through a WAN partition --
leases above all, since a lease renewed across the WAN expires when the WAN does -- and **a global
record**, in the two-region mode, for tenancy, the model catalogue and each region's **budget**.
The global tier sizes regional budgets on the provisioning clock and each region admits within its
own: §1's capacity-versus-allocation seam, one level up. `README.md`'s "spin up resources close to
that region" is a decision on that clock, the one clock where a WAN round trip is affordable.

**Build the layer, not the database.** RFD 53 rejects FoundationDB as "more of a foundation for
building a custom storage system than a full-featured system", with indexes "significantly more
work for our 1.0 product". For a fit-for-purpose store that is the reason to choose it. RFD 53 also
weighs building on Raft in Rust and finds it "likely quite expensive and not substantially simpler
than the problem that many existing distributed database technologies seek to solve", which holds
here too: consensus, storage, recovery and transactions are generic, take years of fault testing to
trust, and would put a new database's correctness under every orchestration claim in this
document. What is specific to polyproto is the data model, and that is what a layer holds:

- **Typed records with secondary indexes** -- by tenant, by region, by lease expiry.
- **Leases**, on read versions (requirement 4).
- **A change feed**, the versionstamped log the scheduler follows.
- **Capacity budgets.** The global record grants each region a budget per tenant, model and
  accelerator class, and the region admits within it -- the inference and multi-region piece no
  general-purpose store supplies.
- **Selective replication** of the global record into each regional one, driven by the change feed,
  so tenancy and catalogue reads stay regional despite the two-region limit; the global record
  changes only on the provisioning clock. This is RFD 53's "logical replication of chunks of the
  namespace", in this document's shape.

The layer is Rust, which is where the leverage credited to TiKV actually lands, and
[`foundationdb-simulation`](https://docs.rs/foundationdb-simulation/latest/foundationdb_simulation/)
runs Rust workloads inside FoundationDB's deterministic simulator (fdbserver 7.4.6 or newer), so the
layer is tested under the same fault injection as the store beneath it -- this repository's
simulate-first method, one level down.

**The logged tier is FoundationDB too** -- a second cluster, appending with versionstamps, so one
technology spans two failure domains; AX chose Redis for the same tier. It stays FoundationDB until
evidence says otherwise, and the evidence that would is a per-tier count showing FoundationDB cannot
carry the rate. Phases 7 and 10 counted it: at 10,000 nodes the logged tier writes 146,000-491,000 a
second on the agent presets, under the 820,000 writes a second of FoundationDB's published 384-core
benchmark (7-25 cores at its single-core write rate), so the case for a purpose-built log does not
arise from the rate; it remains the one place building below the layer could pay if side-effecting
work proves denser per node than those presets. The simulator models no store at all, so no result
here depends on the choice.

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

Linking a proxy core rather than writing one (§2.6) adds one class, the dependency's CVE stream.
It is **operational, not architectural** -- patching is a redeploy rather than a config change, it
is the same exposure an Envoy deployment already carries, and it changes no decision here.

One benefit follows from a decision made on other grounds: §1 chose **ZMQ over Unix domain
sockets**. A UDS has no network surface -- it is scoped by filesystem permissions and unreachable
off-node -- so the telemetry channel needs no transport authentication to be un-spoofable
remotely. `tcp://` would have needed mTLS to reach the same place.

The tenancy model is **soft** multi-tenancy (§3.8), which moves the question from confidentiality
to **fairness** -- a scheduling problem, answered in §3.8, not here. Two residuals tracked rather
than fixed:

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

Each phase compiles, runs, and ends with a number, and the measurement apparatus comes before the
thing it measures. Each has its own `phase-N.md` with the plan, the predictions stated before the
run, and what was measured; the current numbers are in the ledger.

| phase | what | status | headline |
|---|---|---|---|
| [0](phase-0.md) | price the seams: WASM and `ext_proc` rungs on the ladder | done | a warm WASM hook costs 13-25 ns; an `ext_proc` callout 36-63 us |
| [1](phase-1.md) | name the memory boundary in types: `own::authority`, the `Telemetry` boundary, the census | done | 13 allocation-authority entry points, all in `cache.rs`; results byte-identical |
| [2](phase-2.md) | oracle, regret, coupling, the wait regime, per-request spans | done | `scored`'s regret is all model gap; coupling is a statement about a regime |
| [3](phase-3.md) | the engine allocates; the orchestrator sizes the partition (`--engine-cache`) | done | ceding allocation moves mean service by -0.2% where decode dominates; budgets survive and grow, per-block authority does not (§1) |
| [4](phase-4.md) | belief, not truth: lossy telemetry and `P(resident)` (`--belief`) | done | within 0.16% of the exact view at 20% batch loss with no recovery |
| [5](phase-5.md) | influence: retention directives, prefill-ahead, divergence by cause (`--directives`, `--prefill-ahead`, `--retain`) | done | an oracle's retention directives buy at most 1.4% of stall; a prefill of a declared downstream buys 24-28% of task latency where `announce` bought 10-18% |
| [6](phase-6.md) | macro authority: weight placement, partitions, disaggregated prefill/decode, tenancy (`--model-batches`, `--prefill-time`, `--model-keyed`, `--fleet`, `--planner`, `--pairing`, `--neighbour`) | done | one model per node is within 0.3% of the pooled engine and a late placement is the whole price (30 s start +77-82%); a pair wins 1.6-2.6% at one prefiller in eight and loses past it; a router quota beats sharing against a neighbour |
| [7](phase-7.md) | learned flows, speculative authority, sessions that suspend, the taxonomy (`--hint-grade`, `--learn-gate`, `polyphonic programs`) | done | closed-loop turns are 3.6-3.8x the open-loop trace's, and the same hints cut the flow stall by 0-5% rather than 47-63%; speculation is worth about 1% on coding tools; the logged tier writes 20-66% of the soft tier's decisions on agent presets |
| [8](phase-8.md) | the data path as an arm | done | the sidecar path binds below ~1 ms; an `ext_proc` hook caps one scheduler at ~20 nodes |
| [9](phase-9.md) | enforcement: a queue at the router, cancellation on the path, and two-tier admission (`--engine-wait`, `--queue`, `--admit`, `--cancel`, `--victim`, `--disconnect`, `--batch`, `--stream-buffer`) | done | a cancel by declared class takes the interactive first-token p99 to 63-66 ms, a restart is 26-183% later than a continuation, and the stalled-stream buffer is under 2 MB a node |
| [10](phase-10.md) | durability: what each tier writes, and what a crash costs (`--count-writes`, `--track-flights`, `--observe`, `--node-check`, `--snapshot-estimators`, `--copy-durable`, `polyphonic durability`) | done | a restart costs its outage (112-143 request-seconds at 1 s, 32,000-39,000 at a 15 s lease) and the streams that die with it; the soft state it loses costs nothing a run can see; the record's largest writer is liveness; a router that waits 40 s for a lease pays about 65,000 request-seconds |
| 11 | regions: a scheduler per region under global budgets | planned | |

Built bits are off by default, and every result behind them is an A/B against the run without them.
Phase numbers are stable once cited, so phases added later take new numbers and *Ordering* sets the
sequence.

### Phase 6 -- Macro authority: placement, partitions, prefill/decode, tenancy

Plan, predictions and outcomes: [`phase-6.md`](phase-6.md). **Status:** built and measured, behind
`--model-batches`, `--prefill-time`, `--model-keyed` and `--fleet` with their planner, pairing and
tenancy bits; current numbers are in the ledger's *Fleet* section. The slow, coarse,
orchestrator-owned decisions -- §2.4's provisioning tier, and between them everything Phase 3
froze -- are also the decisions the system of record holds (§8).

The phase is two corrections to the engine and three decisions.

**The engine's two corrections.** The published engine decoded four models in one batch and
charged prefill nothing, so neither placement nor pairing had anything to act on.

- **A batch per model.** A step reads each resident model's weights once, so a node decoding four
  models pays four reads a round: a four-model node is +241-279% of the pooled figure, and pricing
  the batch in the score recovers 21-29 points of it. No score finds the placement that avoids it.
- **Prefill takes engine time.** A prefill stretches the decodes it overlaps, by 13-17% of mean
  service at the published partition and 63-81% at half of it. Prefill-ahead keeps its stall
  saving (-56%) and costs +2.7% of service. KV keyed by model adds 27-32% to prefill work where a
  fan-out crosses models, which is 48-49% of its agents.

**Three decisions.**

- **Placement.** Weights leave the ledger: a replica is one model on one node, the KV partition is
  what the weights leave, and a model's load costs its start time and a copy. One model per node is
  within 0.3% of the published engine and 71% ahead of the lazy cache. The clock is the result
  (§1's *Two control loops, two clocks*): a planner that moves by rent-or-buy lands +8.5-9.6% over
  a clairvoyant one, and three model sizes move the allocation to 1 / 2 / 2 / 3 replicas of eight
  with a saving of 24-36% against an even split.
- **Pairing and ratio** (§2.5). `joint` pairing with an unpaired option beats a list everywhere it
  is unsafe, and the ratio is one or two in eight depending on mix and rate.
- **Tenancy** (§3.8). Cross-tenant prefix sharing is zero on the published workload; a neighbour's
  damage is engine time; the router's quota on prefill work is the arm and a replica set the hard
  version of it; a tenant-aware block manager is worth under 0.3% at the published partition.

The decisions are the record tier's writers, and the planner's are counted: 0.046 a second on the
rotating mix.

- **Not built:** the per-tenant axis on `Quota` for host DDR, a second engine per node (still
  §3.8's +91-96% pre-measurement), a replica set per tenant, and a half-width node, so the regime
  in which a partition binds on a placed fleet is not reached.
- **Open:** whether `--model-batches` and `--prefill-time` become the default. They move every
  published cluster number, which the other bits did not, so they stay off and the published
  results stay the ledger's, read as a fleet of one model.

### Phase 7 -- Learned flows, speculative authority, and the taxonomy

Plan, predictions and outcomes: [`phase-7.md`](phase-7.md). **Status:** built and measured, behind
`--hint-grade` and `--learn-gate` for the base trace and `polyphonic programs` for the rest; every
bit is off by default and the byte-identity gate holds on the twelve-command set.

Predicted flows replacing declared ones (§3.2), a tool-gap estimator, taxonomy presets, the RAG
class, durable retention, and authority-driven speculative scheduling (`ReadOnly` pre-execution,
`DraftOnly` burst preemption, non-preemptible `SideEffecting` leases), then per-pattern coupled %.
`DraftOnly` preemption inside an engine is a cancel (§4), so this phase follows Phase 9.

**Sessions that suspend.** `taxo.md`'s long-running agent -- durable memory, checkpoints, human
approval -- needs a session lifecycle the workload does not have: active, idle, suspended to cold
storage, resumed. It is also the only writer of the logged tier (§1): `SideEffecting` intents,
suspended-session records and approval pauses, so Phase 7 extends Phase 10's count with them, and
§8's choice of store for the logged tier can now be tested against that count.

- **Deliverable:** what the coupling-tier-1 win is worth against estimates rather than oracles; the
  latency and goodput delta from speculative scheduling; the logged tier's write rate; and a table
  saying for which workload patterns a unified orchestrator can help at all.
- **Risk:** the coupling table may show the advantage confined to a few cells. That is a result.
  **Size:** large, separable into increments.

### Phase 9 -- Enforcement: cancellation on the path, and two-tier admission

Plan, predictions and outcomes: [`phase-9.md`](phase-9.md). **Status:** built and measured, behind
`--engine-wait`, `--queue`, `--admit quantile | tiered | gate`, `--cancel`, `--victim`,
`--disconnect`, `--leak`, `--batch` and `--stream-buffer`; current numbers are in the ledger's
*Enforcement* section. `--engine-wait` stays off by default, so the published engine and every
earlier phase's results are unchanged.

§1 ceded the choice of victim, so every priority policy in this document -- two-tier admission
(§1), `DraftOnly` preemption (§4), the tenancy trade (§3.8) -- has one enforcement arm: **cancel
the request on the path it arrived on** (§2.3). The simulator had no cancel and no engine that
waits, so the phase added both, with a queue at the router and the stream semantics Phase 8 left
out of scope:

- **The engine that waits.** A sequence the partition cannot hold waits at its node instead of
  running with no memory, which the published engine does at 18-20% of decodes at half the
  partition. The half partition is an overload once it does.
- **The router queue and the cancel.** Requests wait at the router in class order and are placed
  when they leave; a cancel releases a flight's batch slot, pins and reservation, and the victim
  continues from the blocks it decoded. It fires when the router's own check fails and when a
  higher-class request waits at an engine.
- **Departures and the stalled-stream buffer.** A client that leaves is an abort or a leak, and the
  buffer is a counter.

**What it found.** Where the loss lands: with a victim class the interactive first-token p99 is at
the 63 ms floor and the throughput class's completion pays. A claim at each class's own quantile is
what separates the arms; the tiered claim's mean costs the throughput class 1.5-1.7x at 0.6x with
prefill taking engine time, and at 0.75x it is level or ahead on two seeds of three. A second cancel
trigger at the engine, predicted to be needed, is not: at 0.75x the router's own check keeps the
interactive class under 1 s on every seed. A
restart finishes the batch class 26-183% later than a continuation. An llm-d-shaped gate evicts up
to 3.6x as often for a throughput completion 2.2-4.4x later where memory binds, and at 0.75x with
prefill taking engine time it costs the interactive class too. Attained service orders the
throughput class first and the interactive class last on this workload. A leaked departure costs
1.7-3.2x on the interactive first-token p99 at 20% leaving and 0.9-1.3x at 5%. The buffer peaks
under 1.8 MB a node. Several cells lie outside their stated bands and `phase-9.md` says which, and
the batch arms are graded on bands against a draw that is not the pre-measurement's.

### Phase 10 -- Durability: what each tier writes, and what a crash costs

Plan, predictions and outcomes: [`phase-10.md`](phase-10.md). **Status:** built and measured, behind
`--count-writes`, `--track-flights`, `--observe`, `--node-check`, `--snapshot-estimators` and
`--copy-durable`; current numbers are in the ledger's *Durability* section. Every bit is off by
default and the byte-identity gate holds on the thirteen-command set.

§1 made two claims about durability that no run tested: the three tiers' write rates sit orders of
magnitude apart, and soft state can be rebuilt rather than stored. The phase counted the tiers,
injected a scheduler restart, an engine crash and a node loss into runs whose requests were still
open, and priced the crossover against the sidecar's tax.

- **The count.** The soft tier makes 3.1-5.0 owned changes a request and the KV event stream 51-77
  events; the logged tier is 20-66% of the soft tier's decisions on the agent presets; the record's
  largest writer is liveness. The record is three orders below the soft tier and the logged tier is
  not (§1). A commit per decision is 0.6-21 times a warm `FaaS` invocation.
- **A restart costs its outage and its streams, not its state.** `λD²/2` with the streams held;
  61-73 decode-seconds more when they die with the scheduler. Losing the belief, the flow graph and
  the estimators costs nothing a run can see; a blind ledger over-admits only at 0.6x and below, and
  node agents that check their own partitions remove it. A client that backs off costs more
  request-seconds than a burst and refuses fewer requests.
- **An engine crash costs its replica's restart; a node loss adds its host work and its gangs, and
  the lease sets the price.** 3.6-5 request-seconds a second down; 32-62 more for a lost node;
  4,200-4,800 request-seconds if the router waits 10 s to learn of it and about 65,000 at 40 s.
- **The crossover.** A crash costs as much as the sidecar's tax saves in under 90 s to 7.5 hours at
  sub-second takeovers and weeks under a 15 s lease; a fail-open proxy's window costs 12-53
  request-seconds (§5).

**Predictions that failed:** that spreading retries by a client's backoff removes the burst's cost
(it adds 7-32%), that losing the estimators costs a first-token tail at the tightest partition (it
does not), that continuing a failed stream beats restarting it on every seed (it does on two of
three), and that the sidecar's failover costs 300-900 request-seconds (it costs 12-53).

**Not built:** the FoundationDB commit rung, a replaying subscriber, re-placement of a lost node's
replicas under `--fleet`, fan-outs parked on a lease, and active-active schedulers (Phase 11).

### Phase 11 -- Regions: a scheduler per region under global budgets

§8's shape puts a routing tier in each region, admitting within a budget the global tier sets on
the provisioning clock. Every region-distance result so far is one scheduler taking a global
argmin across regions, which that shape does not run.

Model a scheduler per region, a global tier that sizes each region's budget per tenant, model and
accelerator class, and rebalancing on the provisioning clock -- against today's single global
argmin.

- **Deliverable:** the price of the regional split -- what the global argmin buys across regions
  that regional schedulers under budgets give up, and how much rebalancing recovers. Predicted
  small: the scored arm already keeps tool calls in-region at region distance. The place it could
  bind is an agent host and its model host in different regions, where the origin round trip is
  61 ms and no placement moves it. A forecast-driven budget planner against a reactive one is an
  optional arm, and needs traces with real time structure.
- **Risk:** needs more than one scheduler in the simulator, which is a structural change to
  `Machine`. **Size:** medium.

### Ordering

**The memory chain: 1 -> 2 -> 3 -> 4 -> 5 -> 6**, done. Phase 6 unfroze what Phase 3 held fixed --
the partition's size and the weights' placement.

**The engine interface: 4 -> 5 -> 9.** Observe, influence, enforce -- the engine interface's three
channels. Cancellation is the only one that is authoritative rather than advisory (§8), and Phase 7
depends on it, so Phase 9 precedes 7.

**The path chain: 0 -> 8**, done.

**Phase 7** follows Phase 9. **Phase 10** was independent and ran after 7 added its writers.
**Phase 11** follows 6, whose budgets and partitions it moves across regions.

**The system of record** (§8) is built after Phase 6, whose decisions are now real and whose first
writer is counted, and sized by Phase 10's count, which finds the logged tier within FoundationDB's
published rate. It is infrastructure rather than a phase, since
the simulator models no store.