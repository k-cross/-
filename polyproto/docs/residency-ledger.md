# The Residency Ledger

Design notes for Polyphonic's scheduler core. Prototype: this records what is built, what
each mechanism is worth on the measured workload, and which constants are measured rather
than modelled. Numbers are from `darwin/arm64`; re-derive per host.

The host is Apple silicon, one unified memory pool. **The target is not**: a datacenter node
has accelerator HBM for model state and a separate host DDR pool for everything else (see
`docs/prototype.md`). Every experiment models the split by default; `--hbm 0` reproduces
unified memory for comparison.

## The claim under test

> A single scheduler that owns FaaS, AI inference, and long-running compute beats three
> best-in-class specialists, *on the boundaries between them*.

Two coupling tiers of cross-workload win (`owned-and-observed.md` §6 explains the qualifier), and
only one is a moat:

- **Coupling tier 1 — information sharing.** "The function will call inference, so prewarm the
  model." A siloed stack retrofits this with a hint API.
- **Coupling tier 2 — joint decisions.** Cannot be expressed as a hint without becoming a
  distributed agreement problem: co-placement fused with routing, preemption across classes, and
  **one memory ledger over every class the orchestrator owns**, sizing the partitions engines
  allocate within. On datacenter hardware that is one ledger over two pools, and the pools meet
  only where the accelerator offloads into host memory. See *Memory pools*.

This document is about coupling tier 2, specifically the ledger. By default the ledger also
allocates the engine's KV, which `owned-and-observed.md` §1 disclaims; `--engine-cache` is the
corrected side, and *Engine allocation* says what it changes. `--fleet` (Phase 6) takes weights out
of the ledger too: a model is a placement, and *Fleet* says what that adds.

## The model

### One object

The scheduler's world is a content-addressed, immutable chunk of state with a residency tier
and a recompute cost.

```
BlobId = blake3(...)

KvBlock     id = H(model, parent_block_id, token_span)   merkle chain
Snapshot    id = H(image, init_result, env)              merkle chain
WeightShard id = H(model, shard, quant)
ServiceHeap id = H(service, replica)
```

Merkle-chaining collapses prefix-cache routing and warm-pool lineage into one index:
longest-prefix match over a token chain and longest-common-ancestor over a snapshot lineage
become the same lookup. Kubernetes schedules on `(cpu, mem)` scalars and can see none of it.

Three kinds are **reconstructible** — eviction costs a recompute you can price. `ServiceHeap`
is not: a replica with live connections cannot be evicted at any price. That is the one
structural distinction in the model.

| | eviction means | cost |
|---|---|---|
| `KvBlock` | drop prefix | re-prefill (~400 µs / 512 KiB) |
| `Snapshot` | drop warm cell | lazy restore (~9 ms / 32 MiB) |
| `WeightShard` | drop weights | reload (~4 s / 512 MiB) |
| `ServiceHeap` **idle** | scale down | cold start (~15 s / 384 MiB) |
| `ServiceHeap` **serving** | *forbidden* | — |

**The FaaS substrate is Firecracker with lazy snapshot restore**, and the constant says so
(`work::snapshot_restore_ns`):

```
restore_ns(bytes) = 4 ms                              # VMM setup, device restore, mmap
                  + bytes × 0.15 × 1.0 ns/byte        # touched working set, UFFD page-in
```

The merkle lineage `H(image, init_result, env)` is Firecracker-shaped: a serialised memory image
with ancestry. Demand-paged restore costs a fixed few milliseconds plus the pages actually touched,
so it is roughly **flat** in image size, while the footprint stays the whole guest image, because
a warm cell holds all of it. So a snapshot is big to hold and cheap to rebuild, which is exactly
the profile of something to evict first. v8 isolates are explicitly not modelled: they would be a
third answer, with no image to restore.

A long-running replica is a blob whose recompute cost is its cold start. Scale-up and
scale-down are admission and eviction — which is what puts an autoscaler and a KV-cache
allocator in the same ledger.

### Memory pools

```
node
├── HBM   (accelerator)  KvBlock, WeightShard        -- hot model state, usable only here
├── DDR   (host)         Snapshot, ServiceHeap       -- function cells, service replicas
│                        + offloaded KvBlock, WeightShard
└── NVMe  (spill)        anything evicted from DDR
```

`Hierarchy` is one node. KV and weights are usable only in HBM; function cells and service
heaps only in DDR. **The pools meet in exactly one place**: state evicted from HBM is
offloaded to DDR rather than dropped, the way Dynamo's KV block manager, LMCache and
host-side weight caches (ServerlessLLM) all do. Promoting it back over PCIe is far cheaper
than rebuilding it:

| | rebuild | promote from DDR (PCIe, 50 µs + 0.04 ns/B) | read from NVMe |
|---|---|---|---|
| KV block, 512 KiB | 400 µs | **71 µs** | 182 µs + PCIe |
| weight shard, 512 MiB | 4 s | **21 ms** | 69 ms + PCIe |

So offloaded KV and weights are *host* state. They sit under DDR's quota beside function
cells and service replicas, in the most sacrificial band (`Quota::offloaded`): host memory
exists for host workloads, and an offload is a cache of state whose real home is elsewhere.
Eviction runs HBM → DDR → NVMe and costs the request nothing; promotion runs back up and is
charged. PCIe is **modelled**; this host has no link to measure.

`--hbm 0` collapses the node to one pool, where every class competes in DDR directly. That is
the Apple-silicon model, kept for comparison only.

### Monotone residency invariant

> A blob is resident only if its parent is resident.

Necessary for KV (block *i* is unusable without its prefix) and natural for snapshot
lineages. It pays twice:

- **Lookup** — residency along a chain is a prefix property, so the deepest resident ancestor
  is a `partition_point`: O(log depth) hash probes, no trie.
- **Eviction order** — only leaves are evictable, and evicting a leaf may promote its parent.
  Correct ordering falls out with no extra bookkeeping.

Enforced in `TierPool` via `resident_children`.

### Valuation

Eviction priority is GDSF, the known-good algorithm for variable-cost, variable-size objects:

```
priority = inflation + (min(freq, FREQ_CAP) + expect) × (recompute_ns / bytes)
```

`inflation` (`L`) rises to each victim's priority on eviction. It keeps the structure
O(log n) without periodic rescoring. It is *not* a price: it carries the frequency factor and
only ever rises. The shadow price of memory, the expected cost per byte of the cheapest state
a pool would give up now, is `marginal_price` (see *Scored placement*). There is one per
pool, because HBM and DDR are separate markets.

The cost term alone predicts something non-obvious:

| | size | recompute | **ns/byte** |
|---|---|---|---|
| FaaS snapshot | 16–64 MiB | 6.5–14 ms | **0.21–0.39** |
| KV block | 512 KiB | 400 µs | **0.76** |
| Weight shard | 512 MiB | 4 s | **7.45** |
| Service heap | 384 MiB | 15 s | **37.3** |

**Warm microVM cells are the cheapest bytes to rebuild**, with restore modelled as Firecracker
actually does it.

**Cells and hot KV do not compete for bytes on datacenter hardware.** Cells live in DDR and hot KV
in HBM; only on unified memory would the warm pool be holding memory KV wants. The comparison that
is real is against *offloaded* KV in DDR, where the cell's low rebuild cost and the offload's low
promote cost are both cheap, and the offload yields first by band. Two consequences:

- **Bigger cells are cheaper per byte to evict** (0.39 → 0.21 across the size range), because
  the fixed restore cost amortises.
- **Keep-alive matters little.** In the single-node arbitration run, FaaS costs 4–5 ms of stall
  per request whatever the snapshot hit rate, because a restore is cheap.

`freq` still cuts the other way. A hot function is reused hard, so the outcome is contested
per function rather than settled per class.

## Ownership

Phase 1 ([`phase-1.md`](phase-1.md)) names `owned-and-observed.md` §1's ownership table as a type,
`own::authority(kind, tier, question)` in [`own.rs`](../src/own.rs), and puts a census behind it: a
compiler-generated count of the entry points that assume allocation authority over engine-owned
state.

**The table.** Total over four classes, three tiers, two questions -- twenty-four cells, all
`Orchestrator` on the capacity question, `Engine` on allocation for `KvBlock` and `WeightShard` in
every tier, `Orchestrator` throughout for `Snapshot` and `ServiceHeap`:

| kind | tier | capacity | allocation |
|---|---|---|---|
| `KvBlock` | Hbm / Ddr / Nvme | Orchestrator | Engine |
| `WeightShard` | Hbm / Ddr / Nvme | Orchestrator | Engine |
| `Snapshot` | Hbm / Ddr / Nvme | Orchestrator | Orchestrator |
| `ServiceHeap` | Hbm / Ddr / Nvme | Orchestrator | Orchestrator |

Reproducible with `polyphonic ownership`, which prints the full table and the census below rather
than requiring either to be read off this file.

**Static census: 13.** `cargo build --release --features census 2>&1 | grep -c 'use of deprecated'`
-- every one of them in `cache.rs`, none in `machine.rs`. Twelve are the allocation-split entry
points `phase-1.md` §1.5 and §1.6 name:
`Hierarchy::{admit,anticipate,touch,demote,forget_cold,drop_superseded,spill_displaced,drain}_engine`,
`Quota::{floor,limit,band}_of_engine`, and `Quota::set_band_engine` — the one place the orchestrator
*writes* an engine class's eviction band rather than reading it. The thirteenth is
`Hierarchy::reprice_engine`, from Phase 2's clairvoyant arm, which writes an eviction priority into
an engine-allocated entry. Phase 3 adds none -- every engine-cache path is reached before any
`_engine` body. Thirteen is a count of *split entry points*, not of call sites into them:
`#[deprecated]` fires once per named item, so a dispatcher's one call to its census-marked sibling
is one warning no matter how many external callers route through the dispatcher.

**Under `--fleet`, two cells change.** A model's bytes in host DDR or on `NVMe` are a file in a node
agent's cache that no engine allocates, so `(WeightShard, Ddr | Nvme)` answers `Orchestrator` on
both questions (`own::authority_in`), and the tier axis discriminates on the allocation question for
the first time. `(WeightShard, Hbm)` stays `Engine` on allocation: which model a node loads is its
capacity question, already the orchestrator's. Nothing in the ledger reads those cells, since under
a fleet no weight reaches it: the `WeightShard` row of the dynamic census is zero, as `KvBlock`'s is
under `--engine-cache`. `polyphonic ownership` still prints the published table.

**`machine.rs`'s share is 0, and this is a real finding, not an artifact of the counting
mechanism.** Every ledger read the scheduler makes is a residency or cost *query* --
`Telemetry::resident`/`held`, `ground_truth_holds` on the execution path, `local_ns`,
`displacement`, `could_admit` -- never an allocation *decision*. There is no allocation authority in
this file to disclaim.

**Dynamic census: four to five orders of magnitude above the static count.** `polyphonic ownership`
on its default 15,000-op trace (4 GiB HBM / 8 GiB DDR, `Budget::Open`, announce flows, seed 1), one
counter per census-marked entry point:

| class | admit | touch | anticipate | demote | forget_cold | superseded | spill | total |
|---|---|---|---|---|---|---|---|---|
| inference-kv | 93,921 | 5,011 | 13,100 | 86,753 | 2,474 | 59,904 | 86,753 | **347,916** |
| weights | 11,206 | 936 | 0 | 13,759 | 0 | 11,198 | 11,140 | **48,239** |
| faas / service | 0 | 0 | 0 | 0 | 0 | 0 | 0 | **0** |

`forget_cold` and `superseded` are the same removal from two directions — after an admission
(`announce`, `supply`) and after a promotion (`materialise`).

The static count says how much *code* the correction touches; this says how much of the
simulator's *behaviour* rests on the authority `owned-and-observed.md` §1 disclaims. `drain` is an
eighth counter, zero here because a single-node trace never retires a domain; on the ledger side it
fires from `Machine::drain`, which relocates an entire engine's KV cache by orchestrator fiat.

**Admission is under a third of it.** Demotion matches admission one for one, the cascade spill
matches it again, and superseded-copy removal — dropping the DDR or NVMe copy once a blob is
promoted back — is two-thirds of it. So the engine-cache model (*Engine allocation*) covers the
offload and promote paths as well as admission; one that replaced admission alone would cover well
under half of what the ledger does on the engine's behalf.

**Drain and the spill tier.** By default `Hierarchy::drain_all` empties `hbm` and `ddr` and returns
what it took, but leaves `nvme`, so state spilled on a drained node is neither returned nor
migrated. `placement --drain-spill` fixes that behind its own bit, so its effect stays attributable:
it more than doubles the bytes a drain migrates (11.5 -> 25.8 GiB for `blind`) and moves stall by at
most 0.02 ms on any arm.

## Serving engines

A decode step reads the weights once whatever the batch size, so a second sequence is nearly
free and the sixty-fourth is not. Per-token latency rises with occupancy while throughput
saturates:

```
step_ns(batch) = STEP_BASE_NS + (batch - 1) × STEP_PER_SEQ_NS     # 7 ms + 40 µs
```

`Engine` holds one of these per domain, tracks in-flight sequences against an arrival clock
set by `--rate`, and queues a request that finds the batch at `MAX_BATCH`. Constants are
**modelled**; the base is chosen so a batch of one runs at 125 tok/s, the flat rate decode costs
with the engine off. Batch is sampled once at admission and held for the sequence — a step-accurate
engine is a different simulation, and the error is second-order next to modelling no batch at all.

This is not a refinement. **Modelling decode as a constant makes the central inference
scheduling tradeoff invisible**, because the reason to route by KV prefix is to avoid the
recompute a *full* node would force, and a node cannot be full if occupancy has no cost. `--rate 0`
turns the engine off; every claim below about placement depends on it being on.

### The congestion toll

`projected_ns` prices what an engine's current batch does to an arriving request. That is
only the private half. Joining a batch of `live` sequences also widens every one of *their*
steps, and a scheduler that sees only the private half will pile work onto the deepest node,
because joining a full batch costs the joiner barely more than joining an empty one.
`Engine::congestion_ns` prices the other half:

```
congestion = live × STEP_PER_SEQ_NS × tokens / (1 − min(live / MAX_BATCH, 0.95))
```

The numerator is the widening. On its own it is linear, and a linear toll cannot represent
the knee: what actually hurts near capacity is the wait every *future* arrival inherits once
the batch fills. That wait grows like `1 / (1 − u)`, the classical shape of a queue's
congestion toll. The factor is 1 on an empty engine, so at low load this is exactly the
linear term, and it diverges only where placement starts to matter. The cap keeps a
saturated node expensive rather than infinite, so a cluster where every node is full still
has an argmin. This is the same move as pricing displacement, applied to engine slots instead
of memory: a cost that someone else pays, charged to the request that causes it.

### A batch per model

The engine above batches every sequence on a node together, whichever model it decodes. A step reads
each resident model's weights once, so a node with `k` models in flight and `n` sequences in all
pays `k × STEP_BASE_NS + (n - k) × STEP_PER_SEQ_NS` a round: `--model-batches` keeps a batch per
model, the batches take turns, and saturation is per model. With one model it is the shared engine
to the digit (the gate); with four it is what the published node was hiding. `blind` leaves the
score unaware of it and `priced` puts the model in the `engine` and `congestion` terms. A replica
under `--fleet` sets its own step base, `STEP_BASE_NS × bytes / 1 GiB`, and its partition is
what its weights leave of the node's HBM, asserted at every load.

### Prefill as engine time

The engine also charged nothing for prefill, so a rebuild cost its request latency and nobody else
anything. `--prefill-time` reports each KV dispatch's rebuild to the engine it ran on and stretches
a decode admitted at `t` by `1 / (1 - ρ)`, `ρ` being that engine's prefill work over the trailing
window (1 s by default) less what a step carries for free (`--prefill-free`, none by default). The
score's `prefill` term is the sequences in flight times the prefill work the allowance cannot
carry, so with an allowance that carries every load it is zero and the run is identical to prefill
off (the gate). The window is modelled; the pre-measured sweep over 250 ms, 1 s and 4 s is in
`phase-6.md` §1.10.

## Policy

### Fan-out admission

A multi-agent orchestrator dispatches N sub-agents and cannot resume until every one of them
returns. A per-request scheduler admits them one by one, so a fan-out whose fourth agent is
refused still runs the other three. That work is wasted, and it was done on engines and memory
other requests needed. Ray gangs its actors for the same reason. The shape to schedule is the
fan-out, not the agent.

`Machine::serve_gang` places agents hardest first, the one that needs the most bytes, and
**stages** each placement before placing the next. Staging marks the agent's context and
weights as about to be resident on that node, reserves its bytes, and reserves its decode
slot. The next sibling is then priced against the node as it *will* be, which is what lets
the score see both sides of co-location:

- **for:** siblings share the orchestrator's context prefix, so a second agent on the same
  node finds it already paid for, and its result comes home without a handoff;
- **against:** every co-located sibling widens the same batch, and the fan-out finishes only
  when its *slowest* agent does. Congestion on one node sets the whole job's latency.

Every placement is checked against `Hierarchy::could_admit`, a read-only reclaimability test,
and nothing is admitted until every agent has a feasible node. If any agent has none, the
fan-out is refused, and so is the orchestrator's resume turn when it arrives.
`fanout_atomic` off is the baseline: the same assignment, but the agents that fit run anyway.

Each agent's tool calls are FaaS invocations placed like any other request, with the agent as
their flow. Arguments go out and the result comes back, so a remote call pays the link twice,
against restoring the function's snapshot beside the agent. Tool time is added to the agent's
wall time as a pause. The decode slot stays held through it, which overstates occupancy for
an engine that parks sequences during tool calls.

### Floors, bands, limits

The control plane does not know which of an operator's workloads matters most, so it must not
decide. Priority is **configuration**, and the three knobs map onto concepts operators have:

| Polyphonic | k8s analogue | role |
|---|---|---|
| `floor[k]` | `requests` | starvation guarantee — never preempted, by anyone |
| `band[k]` | `PriorityClass` | who gives up **burstable** bytes first |
| `limit[k]` | `limits` | ceiling on preemptive growth |

`limit[k] = floor[k] + slack`, where `slack = C − Σ floor[j]` — derived from floors rather
than separately tuned. `hard: true` caps a class at its floor, which is the partition
baseline and *the same code path*, one bit, so the comparison is exact.

**Unused floor does not strand.** `used` is accounted globally, so a class below its floor
leaves those bytes available to everyone. A floor buys protection, not reservation.

> **A floor must be at least its class's smallest indivisible working-set unit.** One model
> is two 512 MiB shards, so a weights floor under 1 GiB per node cannot hold a whole model
> and the ledger thrashes on something no policy can repair.

### Reclaim

Candidates are classes **strictly above their floor**, most-sacrificial band first, cheapest
within a band. A class at or over its limit may recycle its own bytes and take free space but
may not preempt anyone. Two properties, both load-bearing:

- **Floors are inviolable, including against a higher band.** Protecting a critical class
  only up to its floor is what prevents starvation; letting band 0 preempt below band 2's
  floor makes priority indistinguishable from starvation.
- **Nothing above a floor is owned.** Forbidding reclaim from a more critical band protects
  its *burstable* bytes as well as its guarantee, and one class then permanently owns slack
  it merely reached first.

### Admission refusal

`admit` returns `Admitted | Pending` and never overcommits. `Pending` when the blob exceeds
the class ceiling, or no reclaimable bytes exist. A chain aborts at the first `Pending`,
since the monotone invariant forbids a hole in the middle.

`Pending` is accounted as **goodput loss, never as stall**, so no arm can win the latency
metric by refusing work; both numbers are always reported together.

> **A refused request consumes nothing.** It must not admit its dependencies either —
> admitting a refused inference request's weight shards evicts live state for work that never
> runs, and flatters every policy that scores on residency.

This is a scheduling primitive, not an error path, and it is where elastic capacity
acquisition hooks in. Growing the pool is deliberately not modelled.

## Workload

Three request classes, matching `docs/prototype.md`. `WeightShard` is a *dependency* of
inference rather than a workload of its own.

- **Inference** — multi-turn agent sessions. Each turn extends the chain, requires its
  model's shards, and costs `tokens × step_ns(batch)` at whichever engine it lands on. 35% of
  turns call a tool.
- **Multi-agent fan-outs** — with `--fanout f` (default 0.10 in `distributed`), that fraction
  of agent turns dispatch 2–6 sub-agents instead. Each sub-agent forks the orchestrator's
  context with six blocks of its own. It runs on the orchestrator's model or, with a skewed
  pick, another one, decodes 24–224 tokens, and makes 0–4 function calls with 256 KiB of
  arguments and results each. About 400 requests later the orchestrator resumes, with two
  result blocks per agent handed back from wherever the agents ran. All of these shapes are
  **modelled**, and they are the numbers most worth replacing with traces from a real agent
  framework. Fan-outs add roughly half again as much decode load, which is why
  `distributed` defaults to 250 req/s rather than 350. `--fanout 0` turns them off.
- **FaaS** — function invocations. **Warm** when the snapshot is still resident, which is the
  ledger's decision rather than the workload's: the body runs either way, in 40–200 µs. 45%
  of invocations call into inference.
- **Service** — long-running replicas, 250 µs request handler, 15 s cold start.

Both directions of the cross-workload dependency exist: an agent reaching for a function, and
a function reaching for a model.

`Request::exec_ns` is the work a request does once its state is resident. It is kept out of
`Cost::total_ns` and exposed separately as `service_ns`:

> **Stall is what a residency policy can move; service time is the denominator an overhead is
> a fraction of.** Folding a fixed ~1 s decode into stall buries every arm under a constant.

### Flows

Two workloads registered independently are often one task. `FlowHint` carries the downstream
working set, its probability, and the observed lead.

`announce` raises resident downstream blobs' `expect`, so prewarming is a change of **value**
rather than a subsystem — an anticipated access competes with a real one in the same
currency, and `expect` clears on access. Blobs not resident are admitted only into free
space: **prewarming never preempts.**

`can_satisfy` gates admission of an upstream stage on whether the downstream set could land.
The estimate must be conservative about what is *actually* reclaimable — counting
pinned-while-serving bytes as burstable makes every downstream look satisfiable and the gate
never fires. A gated task must cancel its downstream stage and still count as attempted,
or "the gate eliminates broken tasks" is definitional.

## Boundary costs

Measured on the host, not modelled. Every row is from one run of
`cargo run --release --features grpc,wasm -- boundary --repeat 10`, best-of-10 per rung, timer
overhead subtracted from the per-operation rungs. The `wasm` and `ext_proc` rungs are Phase 0's
([`phase-0.md`](phase-0.md)).

| boundary | 64 B | 1 KiB | 8 KiB | ns/byte | spread |
|---|---|---|---|---|---|
| native call | 0 | 0 | 0 | 0.000 | 1.0× |
| wasm (warm instance) | 13 | 25 | 105 | 0.011 | 1.8× |
| shared ring (spin) | 70 | 78 | 180 | 0.014 | 1.8× |
| syscall floor | 97 | 97 | 97 | 0.000 | 1.0× |
| pipe (same thread) | 434 | 450 | 611 | 0.022 | 1.0× |
| unix socket RTT | 5733 | 5524 | 5233 | 0.000 | 1.4× |
| TCP loopback RTT | 15525 | 15525 | 15900 | 0.048 | 1.0× |
| ext_proc callout (open stream) | — | 36052 | 40733 | 0.633 | 1.1× |
| gRPC unary RTT | 47649 | 49316 | 52316 | 0.516 | 1.1× |

**`ext_proc` has no 64 B cell, and the dash is the point.** A realistic gateway header map at
a fixed field count encodes to **367 bytes** before any filler is added, so this rung's floor
is above the ladder's smallest payload and padding cannot go downwards. The fit is over the
two achievable sizes and its 64 B figures below are extrapolations, labelled as such.

Two more figures, priced once rather than swept across sizes, because what they charge for is
opening something rather than moving bytes through it:

| what | ns | models |
|---|---|---|
| wasm: fresh instance + one call | 9483 | per-call isolation instead of a shared warm instance |
| ext_proc: stream open + first callout | 63066 | Envoy's default `ext_proc` config — a stream per HTTP request |

`spread` is worst run over best. The ring rung is batch-timed, like `native()` and `syscall()`,
because timing it per operation against a ~35 ns timer gives a 1.5:1 signal-to-instrument ratio;
like those two it reports no tail (p99 "—").

No conclusion here rests on a spread's *size*, only on the ladder's *ordering*. Two rungs are
worth distrusting individually: WASM and the ring trade the noisiest spot run to run
(1.2–2.8×), and the **unix socket rung is not monotone in payload** — it came back
5733 / 5524 / 5233 here and 5983 / 7441 / 6649 on another run, wandering by more than its own
payload term in both directions. The step it feeds ("waking a blocked thread") moves between
5.07 µs and 6.97 µs across runs on that instability alone. **The ordering is the robust result;
no single constant here should be quoted to two digits.**

What each step adds, at 1 KiB:

| step | adds | × |
|---|---|---|
| sandbox entry + linear-memory copy (native → wasm) | 0.03 µs | 25 |
| cross-core cache line + spin detect (native → ring) | 0.08 µs | 78 |
| ring transition | 0.02 µs | 1.2 |
| kernel buffer copy + second syscall | 0.35 µs | 4.6 |
| **waking a blocked thread** | **5.07 µs** | **12.3** |
| loopback network stack | 10.00 µs | 2.8 |
| HTTP/2 framing on an open stream (tcp → ext_proc) | 20.53 µs | 2.3 |
| per-call stream setup (ext_proc → gRPC unary) | 13.26 µs | 1.4 |
| HTTP/2 framing + protobuf (tcp → gRPC unary) | 33.79 µs | 3.2 |

Five things to design against:

1. **A no-op syscall (97 ns) costs more than an entire shared-memory round trip (~70 ns).**
   Any hot path spending one syscall per decision has already given up more than the whole
   budget of the alternative. "Fewer syscalls" is not the lever; zero is.
2. **The largest single step below the network is waking a thread** — bigger than crossing the
   kernel. The tax to remove is the scheduler, which argues for spin-polled rings and against
   anything that blocks on the hot path. Its size is the least stable number in the table
   (5.07–6.97 µs across runs, for the reason given above), so treat it as "microseconds, and
   the biggest step before the wire", not as a constant.
3. **Two thirds of a gRPC round trip is framing and encoding** — 34 µs of 49 µs sits above
   raw TCP. The majority of the cost is self-inflicted.
4. **Marshalling slope is flat except for the two protobuf-over-HTTP/2 rungs.** Every
   in-process and socket rung sits at 0.000–0.048 ns/byte while `ext_proc` and gRPC run
   0.5–0.7, more than an order of magnitude above. Which of those *two* is steeper flips
   between runs (0.633 vs 0.516 here, the reverse on another), so the pair's internal ordering
   is not a result. At control-plane sizes the fixed cost still dominates, so batching
   decisions matters more than shrinking them.
5. **WASM lands below the ring, not beside it, and `ext_proc` only beats gRPC unary on a
   stream it gets to keep open.** A warm sandboxed call (13–25 ns) undercuts the shared-memory
   ring (70–78 ns), so a sandboxed hook belongs in the argmin on its own measured row.
   `ext_proc` on an already-open stream is real savings against gRPC unary (36 µs vs 48 µs
   fixed, ~26% cheaper). But Envoy's documented default opens a new stream per HTTP request, and
   that shape measures at 63 µs, *above* gRPC unary — the more realistic shape for an unmodified
   sidecar deployment.

### Policy hook cost in an argmin

The ladder prices a single crossing; a routing or admission decision pays it once per
candidate. `N × fixed_ns` is the per-placement tax and `1e9 / (N × fixed_ns)` is the
single-thread decision-rate ceiling it implies — a property of the measured ladder alone,
needing no workload, no `exec_ns`, and no simulator. Transcribed from the same run as the
table above, at 64 B, since an argmin's per-candidate payload is small:

```
                        isolation      4 nodes    32 nodes   128 nodes  decisions/s @ 32
native call             none           0.00 us     0.00 us     0.00 us         unbounded
wasm (warm instance)    sandbox        0.05 us     0.42 us     1.66 us           2403846
shared ring (spin)      threads        0.27 us     2.14 us     8.58 us            466418
ext_proc callout        process      142.30 us  1138.37 us  4553.47 us               878
gRPC unary RTT          process      192.79 us  1542.34 us  6169.34 us               648
```

Two labels carry caveats the numbers do not. The ring's isolation is **threads**, not
processes: `ring()` spins two threads over one address space, so it prices a cross-core
crossing, and a ring between real processes would pay mapping and a second scheduler domain on
top of this. And `ext_proc`'s row is its fit **extrapolated down** to 64 B from a 367 B floor,
for the reason given above.

`ext_proc` at 4 nodes (142 µs) already exceeds a warm FaaS invocation's modelled ~120 µs. At a
fleet of 32 or 128, `ext_proc` and gRPC unary both push the scheduler's decision rate below a
plausible cluster request rate; `Native` and `Wasm` do not. So the zero-cost-extension claim is
*conditional* on the boundary, and the condition is sharp: an out-of-process hook belongs where
decisions are coarse, while a native or warm WASM hook can run inside the argmin. *Data path*
turns the per-request share into a crossover.

## Data path

[`phase-8.md`](phase-8.md) turns the sidecar-versus-integrated question into an arm:
`data_path: { Integrated, Sidecar, SidecarPluggable }`, charging `phase-0.md`'s measured seam
costs — a routing hook and a dispatch hop — on top of the same trace, the same scored
placement policy and the same `Control::Unified` model every arm shares, so only the data path
varies. One run of
`cargo run --release --features grpc,wasm -- data-path --repeat 10`, `--fanout 0.10` (the
`distributed` default), 4 nodes:

| arm | hook | `d` | `disp` | upper bound/req | realized/req |
|---|---|---|---|---|---|
| integrated | 0.00 µs | 1.23 | 1.32 | — | — |
| sidecar (stream reuse) | 32.68 µs | 1.23 | 1.32 | 51.71 µs | **43.97 µs** |
| sidecar (stream/request) | 61.86 µs | 1.23 | 1.32 | 87.64 µs | **74.97 µs** |
| sidecar, pluggable policy | 4.0 × 32.68 µs | 1.23 | 1.32 | 172.46 µs | **148.14 µs** |

`d` is decisions per served request — measured, not assumed, and **not 1**: a gang's agents share
one `decide()` but each tool call is another, so `d` rises with agentic traffic (`d = 1.00` at
`--fanout 0`, `1.63` at `--fanout 0.30`). `disp` is dispatches per served request, tracked
separately because it is not always 1 either.

**The upper bound is not tight, and the gap is a finding, not noise.** Two effects pull in opposite
directions, both scaling with the fan-out rate, so the gap alone cannot separate them. *Dispatch is
over-counted:* `serve_gang` reports only its slowest agent's cost — every agent runs and
dispatches, but the orchestrator waits on the one that gates it — so every agent's dispatch is
counted in `disp` while only the gating agent's reaches the stall that feeds `Cost::total_ns()`.
*The hook is under-counted:* a gang pays **one** `decide()` for the whole fan-out, not one per
agent, while `place_agent` runs a separate argmin per agent — so a per-candidate hook that really
existed would be consulted `agents × candidates` times. **The pluggable-policy row is therefore a
lower bound**, and its crossover and fleet ceiling below are optimistic in the sidecar's favour. On
a fan-out-free trace the bound and the realized mean are bit-exact
(`machine::tests::sidecar_tax_matches_closed_form_without_fanout`); the gap opens only once gangs
enter the mix. The realized mean is what the crossover below is built from.

**Crossover against the integrated path**, `S* = T × (1/f − 1)` for the realized tax `T` — no
sweep needed, §1.1 of `phase-8.md`:

| arm | 5% of a request | 1% of a request |
|---|---|---|
| sidecar (stream reuse) | **0.84 ms** | **4.35 ms** |
| sidecar (stream/request) | **1.42 ms** | **7.42 ms** |
| sidecar, pluggable policy | 2.81 ms | 14.67 ms |

Four runs land the stream-reuse crossover at 0.83–0.90 ms / 4.34–4.71 ms, the stream/request
crossover at 1.38–1.43 ms / 7.18–7.43 ms, and the pluggable-policy crossover at 2.81–3.04 ms /
14.67–15.83 ms — host noise on this machine, not a different result. The *existence* of the
crossover is the claim; its position moves with the host (`owned-and-observed.md` §7).

**Fleet size at which one unsharded scheduler saturates**, `N_max = √(1e9 / (λ·d·c))` for a hook
costing `c` ns/crossing at 64 B, `λ = 62.5` req/s/node, `d = 1.23` measured:

| hook boundary | ns/crossing | `N_max` |
|---|---|---|
| native call | 0 | unbounded |
| wasm (warm instance) | 13 | ~1000 |
| shared ring (spin) | 67 | ~440 |
| ext_proc callout | 32 682 | **~20** |
| gRPC unary RTT | 47 145 | **~17** |

Assumes one scheduler thread and every active node scored — sharding the scheduler or pruning
candidates before the hook divides the ceiling by the shard count or the prune ratio instead. An
out-of-process hook forces one of those at a fleet size two orders of magnitude below where an
in-process one does (~20 nodes against ~1000), which is `owned-and-observed.md` §2.2's
expressiveness argument with a number attached rather than a direction.

## Distributed placement

`Topology::cluster` models separate hosts at `Distance::{Socket, Rack, Zone, Region}` — one
memory domain per node, because a node's DRAM is the unit another node cannot address.
Crossing is a copy, charged the **measured** transport tax on top of the **modelled** link.

### Acquiring state: place, fetch, or rebuild

A prefix cache is node-local process state — a replica cannot read another's KV blocks by
load/store — but that is a statement about addressing, not about transport. Blocks can be
*copied* over a link, and whether they should be is a question with a different answer every
time. `Machine::plan` prices all three routes at each candidate node and takes the cheapest:

```
acquire(node) = min over the missing suffix of
                  0                                  # already resident here
                  link_ns(node <- peer, bytes)       # ship it from a peer that holds it
                  nvme_fetch_ns(bytes)               # read it off this node's spill tier
                  recompute_ns                       # rebuild it
```

The chain case is a prefix property, so a peer supplies `chain[local_depth..peer_depth]` as
one transfer charged one latency, and everything past the deepest peer is rebuilt regardless.
Every peer is scanned rather than assuming the deepest is the cheapest, because the deepest
peer is only the cheapest peer when all links are identical.

The crossover is not a tuning constant, it falls out of the blob sizes already in the model:

| | rebuild | rack (30 µs, 0.32 ns/B) | zone (400 µs, 0.80 ns/B) |
|---|---|---|---|
| KV block, 512 KiB | 400 µs | **198 µs — ship** | 819 µs — rebuild |
| weight shard, 512 MiB | 4 s | **172 ms — ship** | **430 ms — ship** |

So the policy the system should follow is *KV travels within a rack and is rebuilt across a
zone, weights travel anywhere* — and nothing has to say so. Pricing the three routes produces
it, and re-produces it when the block size or the link changes.

`Hierarchy::supply` installs what arrives without a recompute charge, which is the entire
point; the caller has already paid for the traversal. A fetch is planned against the
scheduler's view and executed against the truth, so a source that has since evicted the state
supplies a shorter prefix or none, and the remainder falls back to a rebuild. That is what
makes a stale view cost something here instead of silently succeeding.

`Control` models where residency knowledge lives — `Unified` (a field read), `Query` (an RPC
per decision), `Gossip` (a periodically refreshed view). This is the architectural question:
a Kubernetes scheduler extender is `Query`; an informer cache is `Gossip`.

### Scored placement

```
cost(node) = acquire_ns                     # cheapest of resident / fetch / spill / rebuild
           + short_bytes × marginal_price   # expected re-pay for what claiming the room evicts
           + handoff_ns                     # what not co-placing costs
           + engine_ns                      # this request's own wait and step width here
           + congestion_ns                  # what it does to the batch it joins
```

Minimised, not maximised: the score is what the request costs at each node. Scoring the
recompute a node's residency *avoids* instead is the same decision only while rebuilding is the
only way to get state; once shipping it is an option, what a node avoids no longer determines
what the request costs there.

`TierPool::marginal_price` is the *expected* cost per byte of the cheapest state the pool
would give up, and `Hierarchy::displacement` charges each pool's shortfall at that pool's
price. It peeks each reclaimable class's heap top in the same band order `pick_class`
reclaims in, abstaining on a stale or pinned top. It is an estimate on purpose: a faithful
dry run costs as much as the eviction itself, per candidate, per request. Three properties
make it a price rather than a number:

- **Recovery, not rebuild.** An evicted blob moves down a tier rather than vanishing, so
  wanting it back costs the cheaper of a rebuild or a recovery from that tier: PCIe from DDR
  for HBM, the spill tier for DDR. Priced as a rebuild, a weight shard pushed off a full
  accelerator would cost 4 s where the real cost is a 21 ms promote -- an error that dominates
  every other term once fetching makes full accelerators candidates.
- **Units on the fallback.** When nothing is reclaimable, the price is the last price
  actually paid, in ns/byte -- not `inflation`, a GDSF priority that only ever grows. Under
  split memory the accelerator's two classes often both sit at their floors, so the fallback
  is hit often, and with the wrong units the fetch arm's displacement spread reads **26–49 s**
  instead of 14 ms.
- **Expected, because evicted state only costs anything if someone wants it back.** Each
  class's loss per byte is multiplied by its **measured regret rate**: the fraction of its evictions
that were later requested again. That comes from a bounded ghost list of recently evicted
ids, which is ARC's ghost cache used for pricing instead of admission. The rate is smoothed
as `(regrets + 1) / (evictions + 1)`, so a pool with no history prices displacement at the
full recompute cost, as before, and converges on the observed rate as evidence accumulates.

Without the discount, displacement prices every evicted byte as a certain rebuild, and its mean
spread across candidate nodes (**3.5 s**) swamps engine and congestion combined (**68 ms**). An
argmin decides on spread, so neither load term wins anything but ties: `moved by load` reads 0.0%.
The discount cuts displacement spread 4×, and the convex toll does the rest. `term spread` in the
`distributed` output reports the mean spread of every term, because a term whose spread is an
order of magnitude under another's is a comment, not a policy.

Two rules the score needs to be a decision rather than a suggestion:

- **The score is the whole decision.** Gating it on a separate residency threshold discards
  the scored choice using a metric the score never consulted.
- **Never move without a reason.** With nothing resident anywhere all costs are equal and an
  argmin over ties sends every cold request to one node; the chosen node must strictly beat
  the affinity node, otherwise content affinity stands.

## Results

All numbers below are split memory unless marked unified. Up to *Regret, oracle, coupling* they run
with the ledger allocating KV; from *Engine allocation* on, with the engine allocating -- the
corrected side -- unless a result says it is the ledger's. Two rules apply to everything here:

- **Service time leads.** Once decode cost depends on the batch a request joins, placement
  moves execution as well as waiting. `stall` excludes execution, so on its own it misreports
  which arm is better. The `distributed` table leads with end-to-end `service/req`, and shows
  fan-out completion, because arms that refuse the most expensive work look faster than they
  are.
- **Baselines filter before they choose.** The unscored policies filter infeasible nodes first,
  as a Kubernetes filter phase or a model-aware inference router would, and apply their rule to
  what remains. Without the filter they send every sibling to one node and refuse whenever the
  siblings do not fit there -- 55% of fan-outs under split memory, a failure no real system has.

### Memory arbitration

Single node, 20k requests, bands `0,1,2,1`, oracle-best split per arm. Split memory is 4 GiB
HBM + 8 GiB DDR; unified is the same 12 GiB as one pool. The search applies one split to
both pools. In HBM it keeps the split's KV-to-weights ratio over the whole pool, so a hard
partition does not strand accelerator budget on classes that never live there.

| arm | split: stall/req | inference | wt hit | unified: stall/req | inference | wt hit |
|---|---|---|---|---|---|---|
| `hard-partition` | 26.80 ms | 43.9 | 0.51 | **12.54 ms** | 9.0 | 1.00 |
| `soft-floor` | **18.35 ms** | **25.0** | **0.66** | 12.62 ms | **8.2** | 1.00 |
| `no-floor` | 76.03 ms | 168.4 | 0.06 | 65.11 ms | 139.3 | 0.04 |

**On datacenter memory, soft floors beat hard partitions by 32%. On unified memory they
tie.** The mechanism is the one coupling between the pools. A soft floor lets weights and KV
evicted from HBM grow into idle host DDR, and promote back over PCIe instead of being
rebuilt: weight residency rises 0.51 → 0.66. A hard partition caps the offload at its floor,
however much host memory sits unused. **Open sharing is still the worst arm by far** in both
models. Without floors the ledger evicts weights to near-zero residency and inference stall
multiplies.

The split itself costs something: 18.3 ms against 12.6 ms for the same total bytes, because
unified memory lets any class use any byte. That is a property of the hardware, not of the
policy, and it is exactly what this prototype must not count as a win.

### Flows

Single node, 15k requests, identical quota and trace. Critical-path only.

| flows | split: task e2e | net work | unified: task e2e | net work |
|---|---|---|---|---|
| `blind` | 31.10 ms | 312.45 s | 14.85 ms | 224.09 s |
| `announce` | **27.77 ms** | 312.79 s | **12.24 ms** | 224.91 s |

**Announce relocates work; it does not eliminate it.** Task latency improves 11% (split) to
18% (unified), and net work is slightly worse in both. The gate fires once per flow task
against a break-even budget of milliseconds, and a gRPC crossing is far under it, so **the
gate is coupling tier 1.**

### Placement across load

4 nodes, 4 GiB HBM + 8 GiB DDR each, rack distance, 12k requests, 10% of agent turns fanning
out. Every arm completes every fan-out.

| arm | 150 req/s | 250 | 350 |
|---|---|---|---|
| hash only | 469.6 ms | 520.5 | 974.0 |
| flow only | 474.8 | 520.2 | 934.1 |
| residency only | 502.9 | 2404.0 | 4121.1 |
| both, gossiped | 473.9 | 640.6 | 1454.5 |
| scored | 454.3 | 488.2 | 580.2 |
| **scored + fetch** | **451.9** | **487.3** | **575.7** |

(Mean service time per request, decode included.)

**The unified score is the best arm at every load.** Its end-to-end lead over the best
specialist is **4% at 150 req/s, 6% at 250 and 38% at 350.** That is much smaller than
the stall ratios suggested (2.3× at 250), because decode dominates service time and every arm
has to pay it. Greedy prefix affinity is still the arm that collapses past the knee.

### Placement across distance

Same cluster, 15k requests, `--rate 250`. At `region` each of the four nodes is a region of its
own and a request has no client, so nothing pays to reach a node or to come back: the column is a
global argmin across regions that cost nothing to reach. *Regions* puts clients in regions.

| arm | socket | rack | zone | region | stall (rack) | siblings co-located | tool calls beside agent: zone / region |
|---|---|---|---|---|---|---|---|
| hash only | 523.3 ms | 523.4 | 523.7 | 532.0 | 38.2 | 86% | 25% @ 1.8 ms / 25% @ **46.2 ms** |
| flow only | 518.9 | 518.9 | 518.9 | 520.0 | 35.6 | 86% | 100% @ 4.8 / 100% @ 4.8 |
| both, gossiped | 693.3 | 693.3 | 693.3 | 694.4 | 203.9 | 85% | 100% @ 4.8 / 100% @ 4.8 |
| residency only | 2902.6 | 2902.7 | 2903.0 | 2911.8 | 2398.8 | 77% | 24% @ 1.8 / 24% @ 46.8 |
| scored | 490.6 | 490.7 | 491.0 | 495.8 | 13.3 | 66% | 30% @ 1.8 / **100% @ 4.6** |
| **scored + fetch** | **490.7** | **489.9** | **490.0** | **494.3** | 14.8 | 63% | 29% @ 1.8 / 100% @ 4.8 |

Unified memory, same total bytes (12 GiB per node), for comparison at rack: `scored + fetch`
488.8, `scored` 490.6, gossiped 508.7, flow only 530.0, hash 535.7.

**The score is the best arm at every distance, by 5–6% end to end against the best
specialist (`flow only`).** Two behaviours still emerge without being told:

- **Fan-out siblings are split according to load.** The score co-locates 63–66% of them,
  against 85–86% for the specialists once they filter for feasibility (100% before).
- **Tool placement flips at the region boundary.** Within a zone, the score runs 70% of
  calls wherever the function is warm, at the same ~1.8 ms a hash router achieves. Across a
  region it runs all of them beside the agent, at 4.6 ms, where hashing pays **46 ms**.

**State transfer is roughly neutral.** Fetch ships 26–35 GiB at socket and rack and costs
about 1.5 ms more stall. It improves balance enough that service time comes out equal or
0.2% better. Across a zone or region it ships only weight shards (5–6 GiB), never KV. It leaves
the FaaS warm pool alone: function calls cost the same with and without fetch (1.07 vs 1.06 ms),
because shipped KV lands in HBM and cells live in DDR.

**The split changes which specialist wins.** Under unified memory, the gossiped residency arm
is the best specialist (508.7). Under split memory it is the worst but one (693.3): with
weights confined to a small HBM, a stale view of where they live costs far more. Hashing
improves under the split, because protected HBM stops function and service state from
evicting weights.

Counted at rack for `scored + fetch` (single requests and tool calls; agent placements not
counted): load moves 13.6% of decisions, congestion 4.7%, flow 3.1%, displacement 2.2%.
Mean term spreads are acquire 183 ms, congestion 30 ms, displacement 14 ms and engine 5 ms.

### Fan-out admission

15k requests, rack, `--rate 250`, `scored + fetch`, sweeping cluster capacity. `wasted
work` is the service time of agents, tool calls included, whose fan-out never resumed.

| HBM + DDR (cluster) | fan-outs run (per agent / atomic) | wasted work | stall/req | inference stall | served |
|---|---|---|---|---|---|
| 4 + 8 GiB | 0 / 0 of 450 | 0 | identical | — | 55.3% |
| 5 + 10 GiB | 7 / 7 | 6.6 s / 6.6 s | identical | identical | 56.1% |
| 6 + 12 GiB | 251 / **306** | 505 s → **0** | 38.55 → **35.93** ms | 65.51 → **58.90** ms | 97.4 → **98.1%** |
| 8 + 16 GiB | 450 / 450 | 0 | identical | identical | 100% |

**Where admission binds, all-or-nothing wins on every column.** At 6 + 12 GiB it completes 22%
more fan-outs, wastes nothing, and cuts inference stall 10%. The contended band is narrow.
Below it, accelerators are too small to hold a fan-out's models at all, and the system serves
half its requests. Above it, everything fits. At 5 + 10 GiB the atomic arm still wastes
6.6 s. That comes from fan-outs admitted against the conservative feasibility estimate that
then lose an agent at admission.

### Heterogeneous nodes: a model host and an agent host

`polyphonic code-review` is two unequal nodes at a swept distance. Node 0 has the accelerator
and is the only place a decode can run. Node 1 has host memory and **no engine at all**, which
is what an agent framework actually runs on. Three mechanisms make that expressible:

- **Per-node memory.** `Machine::new` takes `impl Fn(usize) -> NodeMemory`, so a cluster can be
  heterogeneous. `NodeMemory::can_decode` is separate from `hbm > 0` on purpose: zero HBM
  already means "unified memory, every node decodes", and a host-only node is a different
  claim.
- **A hard placement filter.** Decode-bearing work -- a `KvBlock`/`WeightShard` chain, or
  anything charged tokens -- is filtered to decode-capable domains before *any* policy runs,
  scored or not. No real scheduler routes inference to a node with no GPU; that is a node-pool
  constraint, not a policy quality question. A flow recorded at a host-only anchor cannot force
  a decode onto it either.
- **An origin round trip.** `set_origin` charges each reasoning request the trip from the agent
  host to the node that can serve it and back. `set_tool_anchor` pins each tool call's recorded
  origin to the agent host rather than to wherever the model ran the turn that asked for it --
  otherwise flow-affinity drags tool calls onto the accelerator, which is only right when the
  orchestrator and the engine share a host.

15k requests, 24 GiB HBM + 16 GiB DDR on the model host, 32 GiB DDR on the agent host, tool
calls on 70% of turns at 512 KiB each way.

| | socket | rack | zone | region |
|---|---|---|---|---|
| round trip / reasoning request | 0.16 ms | 0.48 | 1.72 | **61.13** |
| `hash only` service/req | 400.94 ms | 401.14 | 401.76 | 423.16 |
| `scored` service/req | **400.93** | **401.12** | **401.70** | **420.33** |
| `scored` stall/req | 15.28 | 15.47 | 16.04 | 34.68 |
| `scored` tool calls kept on the agent host | 57.7% | 57.7% | 57.7% | **65.7%** |
| `hash only` tool calls kept on the agent host | 31.9% | 31.9% | 31.9% | 31.9% |

**Every decode lands on the model host, in every arm** -- the filter is a constraint, not a
preference, and the table reports it as a check.

**The round trip is identical across arms and no policy can touch it.** 61 ms per reasoning
request at region distance, against 0.16 ms on the same socket: a 380× spread on the one term
placement cannot move. It is 15% of end-to-end service time at region and 0.04% at socket.
Separating the orchestrator from the accelerator is affordable within a zone and expensive
across regions, and that conclusion is independent of how good the scheduler is. What decides who
pays it is where the global tier puts each model: with Phase 6's replica counts laid across three
regions, 3.4% of client-facing requests find their model elsewhere, for +0.7% (*Regions*).

**What the scheduler can move is everything else, and it is worth about 0.7%.** `scored` beats
hashing by 2.8 ms at region, almost all of it from keeping tool calls off the link: it holds
65.7% of them on the agent host where hashing holds 31.9%, and moves half the handoff traffic
(39.5 s against 74.0 s). It also adapts -- 57.7% local within a zone, 65.7% across regions --
without being told the distance changed.

The honest reading is that this topology is dominated by two costs the scheduler does not
control: decode itself (~385 ms, the floor every arm pays) and the origin round trip. The
placement question only governs the remainder.

#### Against a siloed stack

None of the arms above is a siloed orchestrator. They share one ledger, one admission path and
one engine model, and differ only in placement policy. Siloing shows up on three axes, and only
two of them are what the arms measure. `--hard-pools` supplies the third: every class capped at
its floor, which is what separate orchestrators owning separate budgets looks like.

15k requests, memory deliberately tightened until it binds -- 4 GiB HBM and 16 GiB DDR on the
model host, 3 GiB DDR on the agent host. Best arm at each distance:

| | socket | rack | zone | region | goodput |
|---|---|---|---|---|---|
| one ledger, soft floors | **409.98 ms** | **410.20** | **410.86** | **430.32** | **100%** |
| fixed per-class budgets | 426.44 | 426.64 | 427.23 | 446.62 | 97.3% |
| cost of partitioning | +4.0% | +4.0% | +4.0% | +3.8% | −2.7pp |

| axis | cost |
|---|---|
| separate, non-borrowable budgets | **+4.0% service and 2.7pp of goodput, at every distance** |
| distance, socket → region | +5.0% service; the round trip goes 0.20 ms → 61.17 ms |
| cross-workload placement policy | +0.2% at socket, +0.5% at region |
| control-plane RPC (`unified` → `rpc query`) | +0.02 ms/request; **33.7% of a warm `FaaS` invocation** (against its chosen `exec_ns`) |

**Partitioning costs about as much as moving the agent host to another continent, and it costs
it at every distance.** The two are comparable in size and independent in cause: one is a
memory-accounting decision, the other is physics.

The per-class table says where it goes. Partitioned, the `FaaS` warm rate halves (53% → 19%)
and a function call costs 4.6 ms instead of 1.4 ms; inference pays 1.3% more because KV and
weights cannot borrow from each other; and 2.7% of requests are refused outright rather than
absorbed. Service replicas are unaffected -- they are pinned while serving under either policy.

**The unified ledger's win here is a placement option, not just a better eviction.** With soft
floors the score ships **90%** of tool calls to the model host, whose 16 GiB of DDR is mostly
idle, and gets a 53% warm rate for them. With hard pools that host's `FaaS` slice is capped at
its floor no matter how much memory is physically free beside it, so the option disappears and
the score keeps 58% of tool calls at home instead. Partitioning does not only misprice
eviction; it fences off capacity that exists, and forecloses the placement that would have used
it.

That inverts the no-pressure result above, where keeping tool calls local was right. Under
pressure the binding constraint is agent-host memory rather than link cost, so shipping wins --
until region distance makes the link expensive enough to flip it back (65.7% local).

**Prefix-cache-aware routing is inert in this topology.** `residency only` is byte-identical to
`hash only` at every distance and both pool models: with one decode-capable node there is no
routing decision to make for inference. The mechanism that llm-d is built around has nothing to
do in a single-model-host deployment.

Two things keep this from being a clean verdict:

- **The partition is not tuned.** Both arms use the same class split, so the hard arm can
  neither borrow nor size its slices for its own workload. A real operator tunes each silo, so
  4% is an upper bound at this split. The oracle-tuned version of this comparison is the
  *Memory arbitration* table, which sweeps every split and reports each arm at its best -- 32%
  on split memory, and that one is fair.
- **At the command's default capacities there is no memory pressure at all** (`FaaS` warm
  71–87%, service 100%, nothing fetched, no saturation), and hard and soft pools land within
  0.17%. Every number above comes from the tightened configuration; the defaults cannot speak
  to this question.

Still not modelled as siloed: separate admission control per workload, separate autoscalers,
separate retry and queueing. A refused request here simply vanishes -- no silo retries it,
which if anything flatters the partitioned arm. Each specialist remains a routing policy inside
a shared architecture, not an independent control plane.

**Caveats.** One engine serves the whole cluster, so the saturation knee is near half the
symmetric-cluster rate -- about 130 req/s at these defaults, and the sweep runs at 110 to stay
under it. The workload is the generic agent-plus-tool-call stream with its tool rate and
payload turned up, not a purpose-built code-review trace: there is no diff-shaped context, no
per-file fan-out, and tool calls are undifferentiated (a `grep` and a test run cost the same).
The round trip is sized by the tool payload, which is the largest term in a real context delta
but not the only one.

### The falsification test that fails

*Measured before the engine model, the Firecracker constants and the corrected score. It has
not been re-run, and the numbers below are from that model.* The structural conclusion does
not depend on them: a greedy per-request score cannot price residency its own decisions
create.

If the score prices the handoff, raising the handoff should buy more co-placement. Sweeping
the flow payload 512× at region distance:

| handoff payload | hash only | flow only | scored | flows co-placed |
|---|---|---|---|---|
| 1 MiB | 43.98 ms | 47.00 | **33.66** | 48.4% |
| 4 MiB | 44.06 | 47.00 | **33.89** | 48.4% |
| 64 MiB | 45.76 | 47.00 | **38.66** | 48.6% |
| 512 MiB | 57.66 | **47.00** | 72.75 | 47.6% |

**Co-placement does not move** — 47.6% to 48.6% across a 512× change in the thing it should
respond to — and at 512 MiB the scored arm is the worst of the three, losing to the
unconditional co-placement it was built to improve on.

The term is correctly signed and simply too small: a 512 MiB cross-region handoff is 5.7e8 ns
against a weight shard's 4e9 ns reload. But `flow only` wins there while being myopically
wrong on every individual request, because co-placing consistently makes tool and agent state
*converge*. **A greedy per-request score cannot represent a policy whose value is the
residency it creates.** Closing that needs a potential function over future residency, not a
better instantaneous score.

So: the gain and displacement terms earn the result; **the handoff term is decorative**, and
shipping only the first two would measure the same.

### Regret, oracle, coupling

`phase-2.md`, implemented. A realized-cost oracle prices every candidate against truth at each
decision (read-only, myopic by construction) and decomposes `charged(p) - R(o)` into four causes:
`execution` (a plan whose price was invalidated before it ran), `heuristic` (the policy is not an
argmin over its own belief), `belief` (the argmin was taken over a stale view), `model` (the score's
cost function is not the realized charge). `--regret` on `distributed` and `code-review`,
`--clairvoyant` on `residency`/`flows`/`volatility`.

**On precision.** `distributed` is not reproducible run to run (*Method*). The ns figures below are
one run, quoted to their printed precision but not stable in their last digits; the structural
facts -- which gaps are exactly zero, and the order-of-magnitude separation between arms -- are
what survive re-running.

**The scored arm's entire regret is model gap.** 15k requests, rack, `distributed --regret`
defaults:

| arm | regret (mean ns/decision) | execution | heuristic | belief | model |
|---|---|---|---|---|---|
| hash only | 50,827,511 | 416,466 | 49,633,935 | 0 | 777,110 |
| residency only | 676,237,616 | -421 | 547,157,998 | 0 | 129,080,039 |
| flow only | 44,737,944 | 1,097,688 | 41,744,050 | 0 | 1,896,206 |
| scored, no flows | 542,710 | 25 | 176 | 0 | 542,510 |
| **scored** | **544,374** | **0** | **0** | 0 | 544,374 |
| scored + fetch | 1,058,655 | 203,896 | 0 | 0 | 854,759 |
| scored + fetch, gossiped | 8,124,594 | 6,098,380 | -176,835 | 886,675 | 1,316,373 |

**`scored`'s heuristic gap is exactly zero; `scored, no flows`'s is 176 ns.** Both arms fall back to
content affinity when the score ties -- on 0.4% and 0.8% of decisions respectively -- and the
fallback is the only way the policy's pick can differ from the model's own argmin, so the heuristic
gap is confined to exactly those decisions by construction. For the flow-aware arm it is not merely
small but free: the ties it breaks are ties in realized cost too. Every unscored arm carries
heuristic regret in the tens to hundreds of millions of ns/decision while matching or beating
`scored` on end-to-end service time -- the large-regret, small-deficit signature of a policy whose
value sits in the trajectory rather than the decision, not a defect in those arms.

`belief` is zero everywhere except under `Control::Gossip` (29.7M and 887K ns/decision for the two
gossiped rows), which is the one mechanism built to produce a stale view. **`execution` is not zero
merely because the view is exact**: the arms with nonzero `execution` here are the arms with DDR
eviction pressure, and their `belief` is exactly zero, so staleness cannot be the cause. See the
residual paragraph at the end of this section.

**A myopic oracle cannot see the falsification that already failed, and re-running it proves it.**
The flow-payload sweep above, replayed through `--flow-payload` and `--regret` at region distance:

| flow payload | flow only service/req | scored service/req | flow only heuristic regret | scored heuristic regret |
|---|---|---|---|---|
| 1 MiB | 534.5 ms | 497.2 ms | 52,073,532 | 0 |
| 512 MiB | **534.5 ms** | **535.6 ms** | 38,312,514 | 0 |

At 512 MiB `flow only` overtakes `scored` on service time while carrying tens of millions of ns of
heuristic regret the whole way -- the metric's blind spot, reproduced on demand. (This sweep runs at
6k ops with the `distributed` fanout/tool mix, not the falsification section's pre-batching model,
so the two tables are not comparable cell for cell; the qualitative finding -- regret and service
time can disagree in this specific, named way -- is what carries over.)

**Coupled % is a statement about a regime, and at the published defaults the regime does not bind.**
Memory coupling (host DDR: workload-driven evictions where the class evicted differs from the class
being admitted, which is the trade a per-class quota's `pick_class` can never make) is **0.0% on
every arm** at `distributed`'s defaults, over eviction counts of 0 to 2,265 -- there is almost no
DDR pressure there, and none of what there is crosses classes. Tightened to 4 GiB DDR per node it
is **54.1-85.4% of 4.5k-16.1k evictions** across the eleven arms and three distances. Only
workload-driven admissions count; the `HBM -> DDR` demotion path reaches `TierPool` through `offer`
and is excluded, since spillover volume tracks accelerator sizing rather than the arbiter's policy.
Locality coupling (the scored arm's argmin against a silo's -- no handoff term, displacement in one
pool) is 0.8-1.7% of scored decisions in both regimes. So the axis behaves as
`owned-and-observed.md` §3.4 says it should: near zero where nothing binds, high where memory does,
and a single published figure would be a statement about a capacity choice rather than about an
architecture.

**Clairvoyant eviction wins hit rate and loses slightly on cost.** `residency --clairvoyant` at its
true defaults, compared against `no-floor` because both run the open budget and so differ only in
eviction quality: no-floor 74.097 ms/req at 0.51 `KvBlock` hit rate, clairvoyant 76.828 ms/req at
0.74 -- **+22.8pp hit rate for +3.7% stall/req**. `volatility --clairvoyant` reproduces +3.1-3.5% at
every volatility level. The diagnostic did the job `phase-2.md` §1.7 built it for, and the answer is
sharper for being small: GDSF gives up 22.8 points of hit rate to a perfect recency oracle and still
comes out ahead on cost, because the hits it gives up are the cheap ones. Eviction-quality research
is not where the next result is; cost-weighting is already doing the work. The tool also prints
`clairvoyant vs soft-floor` (+469.2%), labelled as two effects combined: almost all of it is the
budget, which is why the budget-matched line above is the eviction-quality number.

**A residual worth naming rather than hiding.** The nonzero `execution` gaps in the table above are
not belief staleness -- both the oracle's read and the ledger's own plan see the same, current, true
state at the moment each runs, and `belief` is exactly zero on those rows. It is a same-request
ordering effect: `Machine::plan` prices a request's chain and its dependencies independently against
one snapshot; `run_here` materialises the chain first and the dependencies second, and under
eviction pressure one side's admission can evict what the other side's price assumed would still be
there. It appears at the defaults for every arm with DDR pressure, not only in the tight-pool
fixture built to isolate it, and it stays bounded (under 20% of decisions even there) and isolated
(the other three gaps stay exactly zero wherever it fires). Left alone rather than patched
mid-phase, per `phase-2.md`'s own rule: the apparatus measures, it does not repair.

### Engine allocation: the price of the boundary

`phase-3.md`, implemented. Every result above ran on a ledger that allocated the engine's KV -- it
admitted, evicted by its own GDSF priority, refused, and relocated KV on a drain -- which
`owned-and-observed.md` §1 disclaims. `--engine-cache` hands allocation to an `EngineCache`:
leaf-first LRU over block hashes inside a partition the orchestrator sizes, never refusing,
preempting and recomputing when a sequence cannot fit, with the connector's own LRU tiers in a DDR
offload sub-budget and an `NVMe` spill sub-budget. The orchestrator keeps every read exact
(`belief` is zero with the bit on, by test) and loses every write. Refusal for inference moves to a
router check against the partition, over reservations the router made itself (`--admit`); decode
output can be modelled and held for the decode's length (`--decode-kv`). Each result in this
section is marked **ledger** or **engine** for the side of the bit that produced it; the sections
after it run on the engine side unless a result says it is the ledger's.

**Goodput is not comparable across the bit.** On the ledger a KV refusal is the ledger's; with the
engine it is a router refusal or an engine preemption, counted apart (`refused`, `refused by
router`, `preempted`) and never summed. An arm that refuses less is not thereby serving more, and a
stall or service figure over a smaller served set is not a better one. Every engine row prints
all three.

**Sizing.** The partition is the KV floor the ledger's own budget implies -- 25% of HBM at the
cluster defaults, 1.00 GiB per node -- and the offload the KV floor in DDR; a pool with no floor
gets the ledger's mean KV occupancy there, which is always the case for the spill tier. This holds
capacity fixed in the sense `phase-3.md` §1.11 meant and not in every sense: under soft floors the
slack KV used to borrow goes to the weights, and under an open budget the mean occupancy is most of
HBM (§8.3 there).

**Memory arbitration** (single node, 20k requests, oracle-tuned per arm on each side):

| arm | ledger stall/req | engine stall/req | ledger weight hit | engine weight hit |
|---|---|---|---|---|
| `hard-partition` | 26.80 ms | 26.03 ms | 0.51 | 0.52 |
| `soft-floor` | **18.35 ms** | **16.61 ms** | 0.66 | **0.77** |
| `no-floor` | 76.03 ms | 70.44 ms | 0.06 | 0.00 |
| soft over hard | **+31.6%** | **+36.2%** | | |

**Soft floors beat hard partitions by more with the engine allocating**: +38.5% -> +44.5% at
`residency`'s 60k-op defaults, and at every volatility level (`volatility`: 31.3-34.2% on the
ledger, 38.0-39.9% with the engine allocating, at every level from zero volatility to full swing).
The mechanism is weights, as the ledger always said, and the bit strengthens it: the partition takes
KV out of HBM arbitration and the weights inherit the slack. On unified memory, where the ledger
ties (-4.0%), soft floors now win (+7.3%).

**Host-DDR arbitration does not survive in its per-eviction form.** `distributed --regret`, 4 GiB
DDR per node: memory coupling is **54.1-85.4%** of 4.5k-16.1k evictions on the ledger and
**0.0-26.0%** of 1.0k-4.8k with the engine allocating, 0.0-2.0% on the scored arms. Most of what the
DDR arbiter arbitrated was the ledger offloading the engine's KV; what remains is Snapshot against
ServiceHeap on the arms that concentrate host work. The orchestrator still decides how much DDR the
connector gets, on a slower clock -- Phase 6's decision.

**Regret.** `belief` stays exactly zero with the bit on. `execution` collapses where nothing is in
flight -- `scored + fetch` 242,292 -> 109 ns/decision at the defaults, because a request can no
longer evict its own weights once KV has its own partition -- and rises thirty-fold where decode
output held against half the partition makes the engine preempt 8-11% of requests (`scored`
25 -> 801,599 ns/decision): the plan prices a prefix the engine is about to take away, and nothing
it reads can see that coming.

**Admission** (`price`, decode output held, **engine**):

| partition | `bound` (declared `max_tokens`) | `perfect` (exact output) | `none` (prompt only) |
|---|---|---|---|
| 1.00 GiB | 3.67% refused | 0.01% refused | 0.01% preempted |
| 0.50 GiB | 17.04% refused | 8.63% refused | 10.52% preempted |
| 0.25 GiB | 25.28% refused | 20.74% refused | 1.66% refused, 27.97% preempted |

Where the partition binds, the bracket is wide and each rule buys goodput with recompute or
recompute with goodput. `bound`'s cost is mostly its ceiling (0.10% refused at 1x slack, 9.61% at
8x). The p99 cost of `none` lands on a session's own turns and on stages other work waits on about
equally (+14% each at a quarter of the partition): class-blind, spread by arrival. The engine these
rows ran on decodes a sequence it cannot place with no memory rather than making it wait (18-20% of
decodes at half the partition on the `belief` cluster). With an engine that waits, the half
partition is past saturation, and the three rules served different sets of requests, so the
bracket's sizes there are an overload's. The loss is class-blind on the engine's own order; once the
router chooses the victim it moves onto the throughput class (*Enforcement*).

**Fan-out admission survives on the coarsened test** (`price` section 6, fan-outs completed per
agent / all-or-nothing):

| HBM + DDR (cluster) | ledger | engine | engine, decode KV held, `perfect` |
|---|---|---|---|
| 5 + 10 GiB | 7 / 7 | 0 / 0 | 0 / 0 |
| 6 + 12 GiB | 326 / **333** | 321 / **351** | 151 / **175** |
| 8 + 16 GiB | 396 / **397** | 449 / 449 | 350 / **366** |

All-or-nothing wins or ties wherever admission binds and wastes nothing. The ledger's gain at
6 + 12 GiB is +2% here, where `price` charges no control crossing, against the +22% above with a
measured one: the crossing changes which models each node holds, and at that capacity gang
feasibility is a cliff (`phase-3.md` §8.4). The direction is robust; the size is not.

**Announce was half KV.** `flows --engine-cache`: announce's task-latency margin is 10.7% on the
ledger, 5.3% of which survives with KV prewarm removed -- KV carried **50%** of it, **71%** on
unified memory (17.6% -> 5.1%). With the engine allocating, announce buys 9.0% (split) and 3.2%
(unified). The coupling-tier-1 claim rests on writing into the engine's cache more than it said.

**Where the boundary costs anything.** On the cluster -- `scored + fetch`, 250 req/s, where decode
is ~1 s of every inference request -- the bit moves mean service by -0.2% and p99 by -0.4%, and a
0.5x to 2x partition sweep moves it by at most 1.2%. On one node a clairvoyant block manager beats
LRU by 2.5-3.1% of stall at 17-23pp of KV hit rate at every partition size, while the partition
itself moves the A/B by under a point between 0.5x and 1x; past 1.5x the partition eats the weights'
own floor and a quarter of requests are refused, which is a sizing failure rather than a price.

**A shared L2 tier fires on KV within a rack.** `distributed --shared-l2 64GiB`, a write-through LRU
pool the size of the cluster's local `NVMe`: 1.5-20.7% of served requests read KV from it at rack,
0-2.3% read weights; from zone out it serves weights only. At 16 GiB KV reads fall to 0-13% and the
scored arms still read KV (6-8%) far more than weights (under 0.7%). A contiguous segment pays the
hop, the seek and the `PCIe` launch once, so two blocks already beat a rebuild.

**Drain.** With the engine allocating, a drained node's KV dies with it and only `Snapshot`,
`ServiceHeap` and weights migrate (`placement --drain-at 0.5`: 6.0-9.9 GiB migrated against
8.6-11.6 GiB on the ledger). The effect on stall is arm-dependent and small beside the drain's own:
`scored + fetch` 16.36 -> 14.60 ms, `scored` 15.03 -> 17.14 ms.

### Belief: what routing costs when residency is lossy

`phase-4.md`, implemented. With `--engine-cache` the router still read the engine's KV exactly
(`belief` was zero by test). `--belief` replaces that read with a belief fed by the engine's own
event stream: batches at the engine's step, a one-way hop of the distance's latency, optional loss,
and one of three recoveries -- replay by sequence number, a periodic snapshot, or none. Every number
below is `polyphonic belief` (seed 1, 15k ops, no control crossing, scored + fetch, rack unless
stated), marked against the same run with the exact view. `distributed --belief` runs the same
channel across all eleven arms and is not reproducible run to run, like the rest of `distributed`.

**The belief costs almost nothing, at every point measured.** Mean service against the exact view:
+0.000% / -0.011% at rack, +0.000% / -0.002% at zone and +0.023% / -0.021% at region (published
defaults / half the partition with decode output held), with 0.13-0.48% of KV decisions exposed to a
phantom at rack and 1.5-4.1% at region. Loss to 20% with replay recovery stays within 0.075%; a
periodic snapshot within 0.15%; **no recovery at all within 0.16%**, with the belief wrong about 37%
of the time at the published partition and 60% at half of it. The regret decomposition agrees:
`belief` is 0-1.7k ns/decision here against the gossiped arm's 887k. The recovery policy changes how
wrong the belief is (0.13% phantom entries under replay, 37% under none, at 20% loss) and not what
it costs.

**The reason is structural.** The terms that would have to be fooled for the scored arm to
concentrate on a quiet node -- `engine` and `congestion` -- read the in-flight count, which the
integrated router knows exactly because it carries every request and response. Silencing a node for
8 s moves almost nothing (its share of KV decisions inside the episode is 26.5% against 24.8%
outside under `face-value`, 25.1% against 25.0% under `quantile 0.9`). A router that reads load from
the stream instead herds or starves depending on the node's load when it went quiet, and one that
adds its own dispatches to the last report starves the silent node every time (1.6-4.8% of KV
decisions against ~25%, mean service +3.1% at 8 s). `P(resident)` -- pricing the belief as a
probability -- changes none of the stream-load cases and only removes a 1.5pp attraction on the
path.

**The estimator is not calibrated where it would matter.** Close to the diagonal with replay and no
loss (0.865 predicted, 0.929 realised); **over-confident** where a dropped batch is never recovered
(0.959 predicted, 0.695 realised at half the partition, 5% loss), because the unknown evictions land
on the blocks an LRU takes first. The chosen node is not worse than the field (0.721 against 0.695),
so the argmin is not exploiting the miscalibration; it is merely not being hurt by it.

**Gossip.** Engine KV from the channel instead of the snapshot: `scored + fetch, gossiped` within
0.05%; `both, gossiped` **+21.1%** at the published partition (676.4 -> 818.9 ms, the staleness was
suppressing herding) and **-29.6%** at half of it (1047.3 -> 737.7 ms, the staleness was routing
onto evicted prefixes). Every gossip result above is a result about an informer cache over owned
state in one regime.

**RequestView.** The score reading the observed mean output length instead of the exact one moves
mean service by -0.03% to -0.13% and p99 by up to -0.35%: closing the cheat costs nothing on this
workload, where output length is independent of everything the router can see, so no estimator can
beat the mean here.

**The declared SLO buys nothing a user would see.** `expected`, `quantile 0.9` and `slo` land within
1% of each other on service p99 for both classes under 5% loss with no recovery and under 2 s of
silence, on three seeds. Interactive stall p99 drops off its ~45 ms plateau (to 18-25 ms) only under
`slo`, in one of its two conditions on each seed and in none of the other twelve cells -- a
direction with no measurable size.

### Influence: what the router can do to memory it does not allocate

`phase-5.md`, implemented. With `--engine-cache` the router can no longer write a KV block or
reprice one. It can do two things: dispatch work that produces state (`--prefill-ahead`), and attach
an RFC-0001 directive -- retain until a deadline, or evict first -- to a request the engine may
honour or ignore (`--directives`, `--ignores`). `polyphonic influence` measures both on seeds 1-3,
15k ops, no control crossing charged, `scored + fetch` at rack unless stated; every number below is
a difference from the same run with the mechanism off, one value per seed, or a range.

**`announce` was prewarm, and a directive cannot prewarm.** Splitting `announce`'s KV half on the
ledger (task-latency margin over `blind`, split memory, seeds 1 / 2 / 3): the bump alone on what is
resident, 5.4 / 4.5 / 4.9%, is `host-only`'s 5.3 / 4.8 / 4.5%; the prewarm alone, 10.4 / 11.3 /
10.9%, is the published 10.7 / 10.6 / 10.3%. Unified memory says the same (5.2 / 5.8 / 3.8% against
`host-only`'s 5.1 / 6.4 / 4.3%; 16.2 / 18.1 / 16.2% against 17.6 / 17.9 / 16.0%), and so does owned
state: a `Snapshot` bump is worth 0.0 / 0.0 / 0.1%, admitting the cell 5.2 / 4.7 / 4.5%. So a
retention directive cannot buy back what the correction removed: a directive cannot create a block.

**A dispatch can, and buys more than the ledger's prewarm did.** Prefilling the declared
downstream's missing blocks when its hint arrives, through the engine's own allocator:

| | seeds 1 / 2 / 3 |
|---|---|
| one node, split: margin over `blind` | **24.4 / 24.5 / 24.5%** (engine's `announce` with KV skipped: 9.0 / 9.4 / 9.0%) |
| one node, unified | **28.4 / 28.2 / 28.5%** (3.2 / 3.6 / 3.2%) |
| one node, net work against `blind` | +0.06 / +0.06 / +0.09% split, -0.01 / -0.22 / -0.06% unified |
| cluster, defaults: flow downstream's stall | -63.8 / -63.5 / -63.5% |
| cluster, defaults: total stall, mean service | -4.07 / -4.13 / -3.59%, -0.11 / -0.12 / -0.11% |
| cluster, half partition, decode held: flow downstream's stall | -46.0 / -38.1 / -52.4% |
| cluster, first seed: flow downstream's stall at zone / region | -30.6% / -7.4% |

It beats the ledger because the ledger's prewarm admits only into free space and an engine never
refuses, so a prefill always lands and displaces the LRU tail; the displaced blocks' later rebuilds
are in the stall column and it still falls. The prefill lands where the downstream is placed 49-50%
of the time (40-42% if it goes to the deepest believed prefix instead, which saves about five points
less at rack), and costs 1.5-1.7x what it saves in prefill work, because a miss is a rack fetch and
past a rack it is waste. Prefill is not a resource in the engine model, so this is exact about when
the prefill runs and silent about whether an engine has room for it. Holding what is resident
instead is worth 0.0-0.1pp.

**A directive buys almost nothing.** Retaining a fan-out's parent chain until its declared resume:
stall +0.00 / -0.05 / +0.08% at the published partition, and worse on every seed at half of it
(+0.24 / +1.01 / +0.19%, 22k pressure evictions a seed). Evict-first over one-shot scopes: -0.76 /
-1.67 / -0.87% and +0.94 / -0.01 / -0.01%. The ceilings, against LRU with no directives: a
clairvoyant block manager -0.66 / -1.85 / -0.69% of stall at the published partition and +1.03 /
-2.40 / -0.74% at half; an oracle emitter that retains every block until its true next use, at 5 s,
-1.41 / -1.37 / +0.11% and +0.12 / +1.65 / +0.14%, at 30 s **worse** at the published partition on
all three seeds (+0.40 / +0.40 / +2.07%, 108k pressure evictions a seed). Service is within 0.07% in
every cell. The RFC's pressure rule evicts the soonest-expiring hold, which is the block needed
soonest, and it fires where holding more than the partition's evictable share is already the
mistake.

**Why: reuse sits past LRU residency, and it does not matter.** Blocks a session reuses come back at
a median 8.8 s after the node evicted them, against 2.3 s of median residency, so session reuse is
the shape retention was built for. But those misses, though they touch 35.9% of KV dispatches at
the published partition and 53.4% at half, cost 0.93 and 1.59 ms per served request -- 0.19% and
0.32% of service. Routing already sends a turn where its prefix is, fetch pulls it from a peer, and
the connector's offload catches most GPU evictions at a fifth of a rebuild.

**An ignored directive is invisible to an acknowledged belief.** The gate is identical on four cells
(declared and 5 s oracle emitters, both regimes, 5% loss). A router that believes its own requests
fills the top calibration bin four times over and is over-confident only where removals go
undelivered: 0.975 realised against 1.000 at half the partition with 20% loss and no recovery, 0.999
with replay, 1.000 at the published partition. Service moves by at most 0.12%.

**Divergence by cause.** Phantom share by cause, published partition / half: under replay every
phantom is an undelivered removal (0.069-0.132%; 0.198-0.371% with 16-17% never stored); under 5% /
20% loss with no recovery 15.1% / 36.6% (59.6% at half) are almost all dropped removals, and 0.9% /
7.8% stranded -- an optimistic entry whose store was lost, which no later removal clears; under 2 s
of silence 0.777% / 1.667%, of which 0.701% / 1.200% silenced. Misses are zero at every sample.

**A deadline on the ledger's bump.** Withdrawing a bump when its hint's lead has passed leaves no
stale entries at any rate of hints for flows that never come (against 27-852 under the bump), moves
net work the same as the bump does (+0.4% to +7.9% at 25-76% false hints), and wins task latency by
1.0 / 0.8 / 2.0 points even with no false hint, because a hinted prefix's interior otherwise keeps
an inflated priority indefinitely. With false hints the two differ by -4.4 to +5.3 points in both
directions.

### Fleet: weights, replicas, pairing and tenants

`phase-6.md`, implemented. `polyphonic fleet` runs every section below on seeds 1-3, with no
control crossing charged and `scored + fetch` at rack unless stated; every figure is one value per
seed or a range and carries its rate and its replicas per model. Eight replicas means eight of the
published node (4 GiB HBM, 8 GiB DDR, 16 GiB `NVMe`) on the four published models, at 500 req/s
unless stated, which is the published rate per node.

**A batch per model.** On the `belief` cluster (4 nodes, 250 req/s), a batch per model with the
weights cached per request costs mean service **+278.7 / +263.4 / +270.3%** with the score blind to
it and **+250.0 / +241.1 / +248.0%** with it priced, at the published partition; four models are in
flight at 79 / 76 / 77% of the priced arm's admissions. Pricing the batch recovers 21-29 points and
finds no placement. One model per node -- the partition the weights leave, 3 GiB -- is **+0.22 /
-0.10 / -0.19%** of the published engine and **-71.4 / -70.7 / -71.3%** of the lazy arm; at half
the published grant with decode output held it is -0.27 / -0.61 / -0.70%, because the published
engine preempts 10.5 / 12.4 / 11.1% of requests there and the fleet none. The partition's size and
the connector's offload grant are second-order on a placed fleet: the derived partition against the
published grant is within 0.13%, and the grant from nothing to 1.6 GiB moves service by at most 0.1%
at 8 and at 4 GiB of DDR per node while the function warm rate falls from 63 / 61 / 56% to 40 / 29 /
32% at 4.

**Routing.** `scored + fetch` over `hash only` on the published engine is -16.4 / -32.1 / -24.8% at
500 req/s and -58.9 / -65.7 / -62.3% at 700, the same within a point on eight replicas of one
model. With four models at two replicas each it is a function of where the replicas sit: over eight
rotations of one placement and three seeds the lead runs from -2.7% to -63.4% at 500 req/s and
-25.5% to -78.9% at 700, while the scored arm does not move by 0.1 ms. The knee is per model: at 700
req/s the placed fleet's scored arm is 10-25% slower than the published engine.

**The clock.** Eight nodes, 500 req/s, 240 s, demand by model 55 / 25 / 12 / 8% with the hot model
rotating one place each phase. Mean service, seeds 1 / 2 / 3: the published engine 456.7 / 454.1 /
454.3 ms; a placement that is always right, with no start time, +0.5 / +0.6 / +0.5%; with a start
of 2, 8 and 30 s, +1.0 / +1.2 / +1.0%, +7.2 / +6.8 / +6.9% and +82.0 / +76.8 / +81.1% more; 10 s
late (8 s start) +18.6 / +16.9 / +19.2% more than on time and 30 s late +117.9 / +109.6 / +120.3%; a
placement made once 6.6 / 6.2 / 6.5 s with 52% of decodes arriving at a full batch. A load is its
start time plus the cheapest copy -- 43 ms from the node's own agent cache, 344 ms from a peer at
rack, 8 s cold -- and the node drains its in-flight sequences first. The rent-or-buy planner
(`follow`: move when the loss suffered pays for the move) makes 11 moves, lands +8.5 / +9.6 /
+8.5% over the clairvoyant placement with the same start and 92% under a placement made once, and
writes the record tier 0.046 times a second; `eager` makes the same 11 moves here, because every
shift of this mix puts a model past its knee. With models of 0.5 / 1 / 1 / 2 GiB at equal demand
the cost function puts 1 / 2 / 2 / 3 replicas of eight, `follow` gets there in one move and is
-30.3 / -23.9 / -36.3% against two replicas a model, and `eager` makes 16-27 moves and is 4.5-6.9%
slower than `follow`. Its lateness is the price: `follow` at a 1, 5 and 15 s interval is +3.6,
+8.5 and +55.6% over the clairvoyant placement.

**Prefill as engine time.** Prefill taking engine time costs the `belief` cluster **+13.1 / +13.0 /
+13.3%** of mean service with the score pricing it (**+17.5 / +17.1 / +17.4%** blind) at the
published partition and +63.4 / +76.3 / +65.0% (+67.9 / +81.4 / +72.2% blind) at half with decode
output held, preemption rising from 10.5 / 12.4 / 11.1% to 23-26%; a 1 ms allowance a step leaves
+0.3 to +0.6%. Prefill-ahead under it costs +2.9 / +2.6 / +2.7% of service and keeps the flow
downstream's stall -56 / -57 / -56%, where it had cost nothing. On eight replicas at two a model it
is +16.6 / +16.2 / +15.3%, about five points of which are keying (unkeyed and blind to it, +12.8 /
+11.9 / +11.7%, against +17.4 / +17.0 / +16.0% keyed): an agent on another model than its parent's
(48-49% of them) rebuilds the parent's context, which adds **+32.0 / +26.7 / +27.5%** to prefill
work and +4.8 / +4.3 / +3.5% to service on the `belief` cluster.

**Prefill and decode.** Eight replicas of one model against eight aggregated, `joint` pairing,
seeds 1 / 2 / 3. At 300 req/s on the published mix (aggregated decodes stretched 5.0 / 5.1 / 5.1%)
one prefiller in eight is -1.7 / -1.7 / -1.6%, two +0.2%, three +3.7%, four +10%; at 500 req/s
(8.5 / 8.9 / 8.7%) -2.3 / -2.6 / -2.3%, then +10-14%, +104-107% and +610-630%. With fresh 64-block
prompts at 0.15 a request the best split at 300 req/s is two in eight, -9.6 / -9.7 / -9.8%, three
-7.2 / -7.3 / -7.4%, one +2.5% (it pairs 24% and its prefiller waits 450 ms); at 500 req/s one is
-3.8 / -5.3 / -5.6% and three +127-134%. A list pairing every prefill is +3,526 / +3,585 / +3,575%
at one in eight on fresh prompts and -6.5% at three; over 10 ms it pairs a third of the decisions
on fresh prompts and 7-8% on the published mix. A prefiller does 1.9 times the work it replaces at
one in eight and a list 2.4-2.7 times; letting it fetch the prefix brings that to 1.1 for about two
points. A planner's second pass ends at 2 / 2 / 2, 3 / 3 / 3 and 0 / 0 / 0 prefillers in the
published 300, fresh 300 and fresh 500 cells. The fresh stream stretches aggregated decodes 13.0% at
300 req/s, lighter than the 22% it was predicted on.

**Tenants.** On the `belief` cluster no block is touched by two tenants (0 of 384,159 / 413,463 /
405,143 touches); the tenant prefix carries 52 / 57 / 56% of reads at a 93 / 91 / 92% hit rate and
80 / 82 / 82% of hits; another owner's request causes 88.6 / 90.0 / 89.6% of GPU evictions; the
busiest tenant's hit rate is 76 / 75 / 79% and the quietest twelve of twenty-four's 54 / 63 / 59%
(40 / 44 / 42% at half the partition). A prefix per model under every tenant's puts 9.3 / 10.2 /
9.8% of touches across tenants, raises the quietest twelve's hit rate 2-3 points at the published
partition and 5-8 at half, and moves mean service by at most 0.5 ms. A tenant sending fresh
64-block prompts for a fifth of the run, on eight replicas at 500 req/s, costs the others in the
burst, against the shared fleet with no burst:

| at 0.1 and 0.2 of the request rate | prefill free | prefill takes engine time |
|---|---|---|
| shared | +1.3% and +2.7% | +11.1 / +10.4 / +11.2% and +61.5 / +55.5 / +53.0%; p99 +12% and +94-112% |
| 1 of 8 replicas the neighbour's | +1.1% and +1.9% | +3.0% and +3.0%; nothing outside the burst |
| 2 of 8 | +1.3% and +2.5% | +8.0% and +8.1%; +12-14% outside the burst |
| quota 0.25 engine-seconds a second | | +1.9% and +1.4%; refuses 79% and 90% of the neighbour's |
| quota 0.5 | | +3.5% and +2.9%; refuses 58% and 79% |
| quota 1.0 | | +9.0% and +8.3%; refuses 17% and 59% |

At 300 req/s the shared burst costs +0.8% and +1.6% free and +4.8% and +11.7% priced, and two of
eight +1.7% outside the burst. Over the whole run, varying the share of it the neighbour bursts for
(0.2 of the request rate, priced): a replica set is level with the quota at 1.0 engine-seconds a
second or ahead of it at every share from 5% to 100% -- +3.5 / +4.3 / +4.5% against +20.5 / +19.7 /
+19.7% at all of it, shared +134 / +129 / +145% -- and a quota a quarter of that is ahead of the set
by refusing 80-90% of the neighbour's prompts. A cap on sequences in flight per tenant refuses
16-27% of the others' requests and is a row about survivors. A tenant floor in the engine's block
manager -- each tenant's KV kept from other tenants' evictions down to a 24th, 12th or 6th of the
partition -- moves any tenant group's mean service by at most 0.09% with prefill free and 0.29%
taking engine time at the published partition, raising the quietest twelve's hit rate by 5-10
points; at half the partition with prefill taking engine time the quiet half gain 0.2-1.8%. Lazy
weight loads run 3.9 / 4.5 / 3.2 a second on the `belief` cluster and scored decisions 306-308 a
second at 250 req/s.

### Enforcement: a queue, a cancel, and who leaves

`phase-9.md`, implemented. `polyphonic enforce` runs its sections on seeds 1-3 on the `belief`
cluster at 250 req/s with `--throughput 0.3`, no control crossing charged, the partition at a stated
share of the published grant and prefill free or taking engine time. Every arm is graded on the
corrected engine, in which a sequence the partition cannot hold waits (`--engine-wait`), so none of
it is a figure for the published engine. Seeds are listed in order; first-token p99 is the time
before the first token, and completion p99 is a request's service.

**The overload.** At the half partition the published engine runs 18-20% of decodes with no memory
and the corrected one is past saturation, so the half partition is an overload and Phase 3's
bracket compared different sets of served requests.

**Where the loss lands.** With a batch class (5% of requests, 8-block unshared prompts, 200-599
tokens, throughput) at 0.75x, a pooled p90 lets the interactive first-token p99 reach 473 / 661 /
3960 ms with prefill free and 3794 / 1762 / 6746 ms with prefill taking engine time; a p90 of each
class's own lengths holds it to 180 / 241 / 290 and 586 / 264 / 547 ms, and a cancel takes it to 63
/ 63 / 63 and 67 / 64 / 63 ms, the floor. The tiered claim's throughput completion p99 is 1.00 /
1.01 / 0.98x the own-class p90's with prefill free, 0.87 / 1.49 / 0.88x with prefill taking engine
time, and 1.59 / 1.68 / 1.52x at 0.6x with prefill, where it also leaves the interactive first-token
p99 at 3.6-5.3 s against 1.0-1.5 s. At 1.0x the claims are within 3%. These arms are graded against
a batch draw that is not the pre-measurement's; they agree in kind and not to the digit.

**Triggers, restart and the gate.** With the tiered claim and prefill taking engine time at 0.75x,
a cancel at the router's check alone leaves the interactive first-token p99 at 75 / 65 / 691 ms and
the engine-side trigger too at 75 / 69 / 64 ms; at 0.6x with prefill the router's alone is 2.7 / 2.3
/ 2.7 s and both 4.2 / 5.3 / 3.6 s. Under each class's own p90 the engine trigger never fires. A
restart finishes the batch class +137 / +90 / +118% later than a continuation at 0.75x with
prefill, +109 / +119 / +183% with prefill free, and throws away 442-1007 sequence-seconds. An
llm-d-shaped gate at θ = 0.9 finishes the throughput class 2.2-4.4x later than the claim with a
continuation where memory binds, and the interactive class 1.0-1.1x with prefill free and 2.7-3.5x
with prefill taking engine time at 0.75x. Ordering by attained service puts the batch class's
first-token p99 at 71-147 ms and the interactive class's at 0.3-0.7 s free and 3.0-8.0 s with
prefill; arrival order is 1.0-2.2 s for both.

**Who leaves.** Under the engine's own wait a leaked departure over an aborted one is 1.7-2.3x on
the interactive first-token p99 at 20% leaving at 0.75x with prefill and 1.8-2.2x at 0.6x, 2.1-3.2x
at 0.6x with prefill free, and 0.9-1.3x at 5% with prefill; a leaked run holds 200-830
sequence-seconds of decode that an abort frees.

**The stalled-stream buffer.** If every decode in flight on a node stalled for its whole decode,
the busiest node holds 3853-4638 tokens on the published workload and 7110-8876 with the batch
class: 0.77-0.93 and 1.42-1.78 MB at 200 bytes a token, 0.009-0.021% of a node's DDR.

### Programs: closed-loop agents, and what the open-loop trace overstated

`polyphonic programs` (`phase-7.md`) submits agent programs step by step, each step when its
predecessor completes, on the belief cluster's shape with no control crossing charged: 900 programs
a run at 12 a second, compress 5, three seeds, every figure one value per seed. The published trace
is open loop and these are not its replacement; every row above is still the trace's.

**Causality.** The published trace submits a tool call 28 ms after its turn whatever the turn is
doing: 100% of an agent turn's tool calls and fan-outs arrive before the turn ends, by a median of
1.0-1.1 s, 18% of a session's consecutive turns overlap, and a `FaaS` call's inference arrives 28 ms
after it. Released when their upstream finishes, none does. A closed-loop turn is 3.6-3.8x the open
loop's (5.4-5.8 s against 1.5 s) and a session 1.9-2.0x (16.4-17.1 s against 8.1-8.7 s, the open
loop keeping each session's think time). Every flow result above that depends on a downstream
arriving before its upstream ends -- *announce*, *prefill-ahead*, the flow stall -- is an open-loop
figure and is kept as measured.

**Hints by grade.** Against prefill-ahead off, in published order, the flow downstream's stall falls
63 / 64 / 62% with the declared downstream, 47% with the declared template only and 31 / 28 / 24%
with a template learned per function (known for 74% of flows); gated at P(flow) 0.5 the learned
figure is 23 / 19 / 14%. Released when the upstream finishes, the figures are 0.5 / 1.9 / 3.2%, 0.4
/ 3.4 / 5.4% and -0.1 / +2.9 / -0.0%, and at half the partition the template-only hint costs +4.6 /
+4.7 / +10.7%. Warm-ups before a tool on long-running agents are -0.03 to -0.04% of turn latency
whether the name is predicted or read from the stream; a declared fixed pipeline's is -0.25 / -0.23
/ -0.26% at 2 GiB of DDR a node.

| | coding tools | research tools, open reads allowed |
|---|---|---|
| speculation at the measured top-1, turn latency | 27%: -0.5 / +1.6 / -1.0% | 28%: -3.2 / -3.4 / -3.7% |
| speculation at habit 0.6 | 50%: -0.1 / -0.5 / -1.3% | |
| a perfect predictor | -1.8 / +0.5 / -2.0% | -17.3 / -16.5 / -16.9% |
| wasted / saved tool-seconds | 312 / 124 (27%), 219 / 219 (50%) | 4098 / 2033 |

With executor slots, unpriced speculation shortens the queue other sessions' tools wait in by 15-19%
at 2-4 slots a node and 1% at 6, because a hit takes its tool out of the queue, and moves turn
latency by -1.1 to +2.6%; priced, it stays within 2.1% of none.

**Leases, retention, the lifecycle.** A lease on a `SideEffecting` call's cell pins 7.0% of a node's
DDR at its peak at 8 GiB and 27-30% at 4 GiB, with none broken and no durable cell lost; reclaiming
drafts moves turn latency by +0.00% at 8 GiB and by under 1% either way below it. Every agentic run
abandons 60-61 of 2700 programs when the router's admission refuses a call. Continuum's TTL emits
2.4-2.6 million marks a run and moves turn latency by +0.00% at 6, 12 and 18 sessions a second: the
excess rebuild a call pays, 0.03-7 ms (8-9 ms at half the partition), is the sequence's own
preemption, not an eviction between calls. The two timers (KV 300 s, sandbox 900 s, divided by the
compression) disagree on 19.2% of 4671 turn boundaries; the joint decision differs from the pair on
45.5% of them, frees 62.8% of idle time against the timers' 64.5%, and moves a turn by +0.07 / +0.13
/ +0.06% at 8 GiB and +0.03 / +0.08 / +0.03% at 2 GiB.

**The logged tier.** An intent and an outcome per `SideEffecting` call are 6.9 writes a second on
the pipeline preset (40% of its soft decisions), 7.1 on the agentic (20%; 13.9 and 38% with MCP's
defaults for edits) and 14.6 on the multi-agent (44%); the long-running preset writes 0.59 a second
(66% of its 0.9 decisions); one-shot, extraction, conversational, retrieval and batch write nothing.
The record tier's planner writes 0.046 a second.

**Retrieval.** At k = 5 over 10,000 chunks a finite partition reuses 35.7 / 36.1 / 35.5% of the
chunk tokens under prefix caching in a fixed order and 12.4 / 12.2 / 11.6% in relevance order (52.3%
and 27.6% infinite); reuse by content under any parent finds 64.0 / 64.1 / 63.2% (96.2% infinite), a
residency counterfactual that nets 54% after the 15% it recomputes. At 100,000 chunks: 32.1% and
10.3%, and 55.7%.

**Roles and class inference.** A pooled p90 claim of 242 tokens overruns on 63 / 65% of reviewers
and 23 / 24% of explorers; a role's own p90 on 8.5-13.3% of each, reserving 220 tokens an agent.
Fan-out service moves by -0.1% at a 256 MiB partition a node and -2.8% (claims) / +3.3% (score) at
160 MiB, where the pooled claim completes 1193 of 1242 fan-outs and the role's 1181 of 1245. The
class read from observables is right for 60.6% of requests at their first call, 85.7% at their last
and 79.1% over every call.

**The table.** Locality coupling by pattern, 8 GiB: one-shot, extraction, conversational, retrieval
and batch 0.0%; tool pipeline 15.2 / 14.8 / 13.0%; agentic 4.1 / 4.1 / 3.8%; multi-agent 7.2 / 7.6 /
6.9%; long-running 2.2 / 2.2 / 1.9%; the four together 8.1 / 9.3 / 7.7%. Memory coupling on the
programs is 0.0% at 8 and 2 GiB, because no program contends with a second owned class; on the
published trace at 2 GiB it is 6.1 / 7.6 / 41.4% for non-AI work, 7.8 / 6.4 / 37.9% for tool
pipelines, 5.2 / 5.6 / 41.3% for agentic and 4.0 / 6.3 / 40.4% for multi-agent, and 0.0% at 8 GiB.

### Durability: what each tier writes, and what a crash costs

`phase-10.md`, implemented. `polyphonic durability` runs its sections on seeds 1-3 on the `belief`
cluster (four nodes, 250 req/s, 10% fan-out, `--throughput 0.3`) with Phase 9's integrated arm --
each class's own p90 claim, the class-ordered queue, the engine waiting by class, the cancel with
continuation -- no control crossing charged, the partition at a stated share of the published grant,
and a fault at 40% of the trace. A fault's cost is the request-seconds of service it adds to the
same trace and seed with no fault; the window is 30 s of arrivals from the fault; five seeds at 0.6x
and 0.5x, where any perturbation moves a run by -300 to +600 request-seconds and only counts and
tails carry a signal.

**The count.** Owned changes by their owner are 3.1 a request with nothing enforced and 5.0 under
Phase 9's enforcement (770 and 1,200 a second, 190 and 300 a node; 5.2-5.5 at 0.6x and 0.5x), of
which decisions are 1.23; the KV event stream is 51-77 events a request, and its removals from GPU
and host offload 1,140-1,590 a second an engine, 9-13.5 KB/s of hashes. Liveness at a 10 s lease
renewal is 0.4 writes a second on four nodes, 94-95% of the record's writes and 17 times the
planner's on eight. At 10,000 nodes: 3.0 million owned changes a second (0.14 cores a node at
FoundationDB's published cluster write rate), the logged tier 146,000-491,000 on the agent presets
against its benchmark's 820,000, the record about 1,060. A durable append on this host is 1.6 us,
with `fsync` 18 us and a full flush 4.0 ms; a commit per decision is 72 us on a protected drive
within a rack, 0.8 ms across a zone and 1.5-2.5 ms at FoundationDB's published commit, 0.6-21 times
a warm `FaaS` invocation of 120 us.

**A scheduler restart.** Streams held below the scheduler: an outage of 0.1 s costs -0.5 to +6.9
request-seconds, 1 s costs 126-139 (`λD²/2` = 125) and a 15 s lease 32,426-38,829, with 236-374
requests unserved and a first-token p99 of 14.7-14.8 s; backing off instead of bursting costs 29-32%
more at 15 s and leaves 9-18% fewer unserved. Streams that die with it reach 102-112 (20-24 a gang's
agents), throw away 61-73 decode-seconds and cost 63-75 at a 0.1 s outage, 284-370 at 1 s with a
restart and 233-276 with the client's continuation. Of the state a restart loses, the belief and the
flow graph cost 10.0 / 0.6 / 3.9 request-seconds at 0.75x and 0.6 / -0.3 / -0.5 at 1.0x with
3,700-5,600 reads of a resident block unknown, and the estimators cost nothing, observed at dispatch
or when a decode ends (-0.5 to +9 at 0.75x; a first-token p99 of 63-69 ms at 0.6x with prefill
against 63-66), and a snapshot 4 s old restores them to the digit (32 KiB every 10 s). A reservation
ledger not rebuilt from node agents admits 131-203 requests blind and over-admits none at 1.0x and
0.75x, 3-30% at 0.6x and 11-23% at 0.5x with a 1 s outage (10 / 27 / 9 / 25 / 5 of 154 / 161 / 140 /
167 / 150 at 0.6x with prefill free); up to 118 sequences wait at an engine, against none, and the
interactive first-token p99 rises on one or two of five seeds. Node agents that check their own
partitions leave 0-4 over-admissions and no sequence waiting, and send 7-90 requests back to the
router.

**An engine crash and a node loss** (node 0 of four). An engine back after 2, 8 and 30 s costs 17.1
/ 9.9 / 13.4, 45.8 / 39.8 / 45.0 and 156.4 / 139.9 / 114.8 request-seconds at 1.0x (3.6-5.0 for each
second down) and 18.5 / 19.0 / 27.5, 59.8 / 24.9 / 49.5 and 217.6 / 246.0 / 112.5 at 0.75x; losing
the spill as well changes nothing while it loses 7,971-11,187 KV blocks against 3,099-3,614;
continuing the 34-42 streams it fails beats restarting them on two seeds of three. A node declared
lost at once costs 196.6 / 201.9 / 179.1 at 1.0x (32-62 over an engine that never returns) and 885 /
443 / 458 at 0.75x, refuses 23-28 more fan-outs at 1.0x and 75-98 more at 0.75x, and loses 96-115
host blobs. A router that learns of it after 2 s pays 343-407 (a first-token p99 of 1.4-1.6 s),
after 10 s 4,217-4,781, after 40 s 64,524-66,742 at 1.0x (a p99 of 39.4-39.6 s): the requests placed
on it wait, 2,481-2,510 of them, a quarter of the window's arrivals.

**Durable sandboxes and the crossover.** Losing a node half way through the long-running programs
takes 18 / 18 / 20 durable cells and leaves one program each holding lost state, with turn latency
moving 0.1-0.4%; a copy made when each cell is marked loses none, copying 6.0-6.2 GiB over the run,
0.14-0.29 MiB/s a node, 0.012-0.024% of a zone link. A crash costs as much as the integrated path's
saving over the sidecar (44-75 us a request, Phase 8) in: under 90 s with the streams held and a 0.1
s takeover; 1.9-3.2 hours held at 1 s; 1.0-1.8 hours with streams dying at 0.1 s and 4.4-7.5 at 1 s
with a restart; 2.9-4.9 weeks under a 15 s lease (1.1-8.9 hours and 3.4-5.8 weeks at 0.75x). The
sidecar's own window, 15 s of hash-only routing with every stream intact, costs 30 / 53 / 38
request-seconds at 1.0x and 21 / 12 / 41 at 0.75x.

### Regions: a scheduler per region under global budgets

`phase-11.md`, implemented. `polyphonic regions` runs three regions of four of the `belief`
cluster's nodes (4 GiB HBM, 8 GiB DDR, 16 GiB `NVMe` each), rack within a region and 30 ms one way
between regions, or Azure's published round trips between East US, West Europe and Japan East (83 /
162 / 233 ms); the engine allocating, `scored + fetch` within a region, no control crossing charged,
10% fan-out, a flat class mix, 60 s of arrivals at 250 and 325 req/s a region, seeds 1-3. A session,
and every turn of it, belongs to a region; a client-facing request served in another region pays the
round trip, in the score and when it runs, while tool calls, flow downstreams, resumes and a
fan-out's agents pay the handoff the score already prices. Every figure is mean service against
regional schedulers at equal demand on the same seed and load, the range over seeds, unless it says
otherwise. The burst is one region taking 75% of arrivals, 2.25 times its share, from 30% to 60% of
the run; the day moves each region's share as `1 + 0.75 cos` with its peak at 14:00 local. Every
cross-region arm is unenforced -- no router queue, no engine that waits -- and the command's p99 is
`round((n - 1) q)` rather than the other commands' `floor(n q)`.

**Clients.** Regional schedulers serve 459.5 / 454.7 / 461.4 ms at 250 req/s a region. The global
argmin as every other section runs it, with no round trip in its score, serves 66.3-66.7% of
client-facing requests in another region and a warm `FaaS` call in 40.4-40.7 ms instead of 0.6, at
+7.1 / +7.2 / +7.1%. With the round trip priced it still serves 36.9-40.1% elsewhere, at +4.3 to
+4.6% (+4.1 to +4.3% at 325 req/s, +11.2 to +12.2% on Azure's round trips): the terms that choose
among a region's nodes price an instant, while a round trip and the prefix a moved turn leaves
behind are certain.

**The burst and the day.**

| 250 req/s a region | equal demand | burst | day |
|---|---|---|---|
| regional, no cross-region rule | 0 | +71.7 to +82.1% | +16.6 to +23.3% |
| global argmin, round trip priced | +4.3 to +4.6% | +4.2 to +4.7% | +4.0 to +5.6% |
| node price, an exact view | +0.8% | +1.3 to +1.5% | +1.5 to +1.7% |
| node price, 1 s summary | +2.0 to +2.4% | +2.7 to +2.9% | +3.3 to +3.5% |
| the same with own forwards | +1.0 to +1.1% | +1.5 to +1.7% | +1.6 to +1.8% |
| the same, 5 s summary | +1.3 to +1.5% | +2.0 to +2.2% | +2.0 to +2.2% |
| region mean, 1 s summary, own forwards | +0.6 to +0.7% | +1.2 to +1.4% | +1.4 to +1.7% |
| region mean, 5 s summary, own forwards | +1.0 to +1.1% | +1.8 to +2.0% | +1.8 to +2.0% |
| threshold 0.5, 1 s summary | +1.4% | +2.0 to +2.1% | +2.5 to +2.9% |
| threshold 0.7, 1 s summary | 0.0% | +1.2 to +1.6% | +2.8 to +3.4% |
| table, 1 s epoch | +0.1% | +2.0 to +2.4% | +2.3 to +2.8% |
| table, 5 s epoch | 0.0% | +9.3 to +19.0% | +3.2 to +3.7% |
| table, 1 s epoch, over the region mean | +0.8% | +1.3 to +1.5% | +1.7 to +1.9% |

| 325 req/s a region | equal demand | burst | day |
|---|---|---|---|
| regional, no cross-region rule | 0 | +222.6 to +249.6% | +160.9 to +174.2% |
| global argmin, round trip priced | +4.1 to +4.3% | +4.0 to +4.3% | +4.2 to +4.3% |
| node price, an exact view | +1.0 to +1.1% | +1.5 to +1.7% | +1.7 to +1.8% |
| node price, 1 s summary | +3.1 to +3.2% | +3.3 to +3.6% | +3.8% |
| the same with own forwards | +1.2 to +1.3% | +1.8 to +1.9% | +2.0 to +2.1% |
| region mean, 1 s summary, own forwards | +1.0 to +1.2% | +1.7 to +1.8% | +1.9 to +2.1% |
| threshold 0.5, 1 s summary | 0.0 to +0.2% | +1.6 to +2.2% | +3.8 to +4.0% |
| threshold 0.7, 1 s summary | +1.2 to +1.4% | +1.8 to +2.0% | +2.5 to +2.8% |
| table, 1 s epoch | 0.0 to +0.1% | +39.2 to +51.8% | +17.6 to +20.8% |
| table, 5 s epoch | 0.0% | +70.3 to +89.7% | +35.2 to +44.8% |
| table, 1 s epoch, over the region mean | +1.1 to +1.2% | +1.7 to +2.0% | +2.1 to +2.3% |

A region alone collapses past about twice its share, and every cross-region rule recovers it to
within 5.6% of equal demand at 250 req/s and 4.3% at 325, the table near the knee aside. A node
price on a summary a second old herds -- a region sees another as idle until the summary refreshes,
and forwards to it all second -- and counting the sender's own forwards still in flight removes
most of it. Priced by each region's mean rather than its best node, the spill is within 0.4 points
of the exact view in every shape and below it at equal demand. A threshold is right at one load:
0.7 costs nothing at equal demand at 250 and 1.2-1.4% at 325, and 0.5 the reverse. The table costs
nothing at equal demand, misses what is shorter than its epoch and under-forwards near the knee,
where Phase 6's mean-value cost model sees a region below its capacity that still saturates on its
fluctuations; over the region-mean spill it lands within 0.3 points of the spill alone. On Azure's
round trips at 250 req/s a forwarded request pays more and fewer are worth forwarding -- 3% of
requests leave at equal demand against 6% at 30 ms -- and the region mean costs +0.6 to +0.7%, +1.9
to +2.0% and +3.1 to +3.5% at equal demand, on the burst and on the day; the exact-view node price
+0.8%, +2.0 to +2.1% and +3.2 to +3.5%; threshold 0.5 +3.0 to +3.4%, +4.4 to +4.7% and +6.0 to
+6.8%; and the global argmin +11.2 to +12.2% at equal demand and +9.5 to +12.3% on the day, every
rule within 6.8%. Every cell served every request.

**Budgets.** A 240 s day, a second of which is six minutes of a real one, over twelve running nodes
on six slots a region, against regional schedulers at equal demand over 240 s on four nodes a
region (about 457 / 455 / 454 ms at 250 req/s and 484 / 483 / 482 at 325). *On time*, *10 s late*
and *30 s late* are the clairvoyant allocation with an 8 s load, *30 s load* is on time with a 30 s
load, and *rent-or-buy* is Phase 6's planner lifted to regions, with an 8 s load and a 5 s epoch.
Regional schedulers with no cross-region rule:

| day | static | on time | 10 s late | 30 s late | 30 s load | rent-or-buy |
|---|---|---|---|---|---|---|
| 250 req/s, amplitude 0.5 | +3.1 to +3.2% | +1.0% | +1.4 to +1.5% | +6.1 to +11.2% | +3.9 to +5.6% | +1.8 to +3.0% |
| 250 req/s, amplitude 0.75 | +67.2 to +79.6% | +1.6 to +1.7% | +2.6 to +2.8% | +91.8 to +107.2% | +15.8 to +23.5% | +4.0 to +10.4% |
| 325 req/s, amplitude 0.5 | +181.9 to +207.1% | +1.5% | +4.3 to +5.7% | +153.7 to +160.6% | +50.9 to +55.9% | +9.6 to +12.8% |
| 325 req/s, amplitude 0.75 | +625.3 to +645.7% | +2.8 to +4.7% | +31.8 to +45.3% | +533.7 to +566.5% | +271.8 to +314.0% | +16.3 to +39.6% |

Budgets that follow the day recover it, and lateness is their price: thirty seconds late, three
hours of a real day, moves capacity away from the region about to peak and is worse than never
moving at 250 req/s. Rent-or-buy waits until the loss it has suffered covers a move, and on a day's
ramp it lands between 10 s and 30 s late, level with 10 s late at 325 and amplitude 0.75. A spill or
the table beneath the budget covers what it misses: with a 0.7 threshold or a 5 s table over it,
rent-or-buy is within 1.3 points of 10 s late seed by seed in every cell bar one, and at 250 req/s a
static budget costs +1.5 to +3.4%, on time +1.0 to +1.6% and rent-or-buy +1.4 to +3.2%. Near the
knee a threshold
misfires on budgets that follow -- at 325 req/s on time costs +3.2 to +3.7% against +2.0 to +2.1% on
a static budget at amplitude 0.5, and +4.3 to +5.1% against +2.6 to +2.9% at 0.75, every region
sitting near the threshold and spilling on noise -- and the table needs them: +57.1 to +65.0% on a
static budget at 325 and amplitude 0.75, +2.5 to +2.7% on time.

**Models by region.** Phase 6's fleet -- one model per node, a batch per model priced -- with four
models at 55 / 25 / 12 / 8% of demand, at 250 req/s a region, against regional schedulers on the
published engine; Phase 6's counts for twelve nodes are [6, 3, 2, 1]:

| placement | regional | global argmin | threshold 0.7 | served away; forced forwards |
|---|---|---|---|---|
| every model in every region | +268.7 to +298.9% | +270.9 to +297.2% | +268.7 to +298.9% | none |
| the fleet's counts, spread across regions | +0.7% | +3.2 to +3.3% | +0.7% | 3.4%; 5.0-5.1% of client-facing requests |
| the fleet's counts, each region filled in turn | +2.1 to +2.2% | +4.2 to +4.3% | +2.1 to +2.2% | 14.3-14.6%; 21.2-21.4% |

A model in every region gives the model with half the demand one replica a region, and 48-49% of
decodes arrive at a full batch. A request for a model its region lacks goes to the nearest region
that has one, never by an overflow rule, so the threshold's column is the regional one.

**Tenants.** Each tenant's quota is split into regional shares, a token bucket two seconds deep in
each region, metered on generated tokens because the region runs do not price prefill; a share is
fixed at an even split or leased, each region's share set at every refresh from its clients' recent
demand. Requests refused, as a share of all requests:

| headroom, at 250 / 325 req/s | 10%, equal demand | 10%, the day | 50%, equal demand | 50%, the day |
|---|---|---|---|---|
| static split | 2.5-2.6% / 2.2% | 4.8-5.0% / 4.5-4.8% | 0.6-0.7% / 0.5% | 2.2-2.3% / 1.9-2.2% |
| lease, every 0.1 s | 2.9-3.0% / 2.5-2.6% | 2.8-3.0% / 2.4-2.6% | 0.9% / 0.6-0.7% | 0.9-1.1% / 0.7-0.8% |
| lease, every 0.5 s | 2.6-2.7% / 2.3% | 2.7-2.9% / 2.2-2.4% | 0.7% / 0.5-0.6% | 0.8-1.0% / 0.5-0.6% |
| lease, every 2 s | 2.5-2.6% / 2.2-2.3% | 3.2-3.3% / 2.7-3.0% | 0.6-0.7% / 0.5% | 1.2-1.3% / 0.9-1.0% |

The day adds 2.2 to 2.6 points of refusals to a static split at 10% of headroom and 1.4 to 1.7 at
50%, and -0.2 to +0.3 to a lease refreshed every 0.1 or 0.5 s. A 0.1 s refresh chases noise, 0.1 to
0.4 points above the static split at equal demand, and a 2 s one lags, 0.3 to 0.8 points more on
the day than at equal demand. Both are the compression's: the run compresses the day 1,440 times
and its arrivals not at all, and in Doorman's 16 s refresh a region's share moves at most 0.04
points. Every split keeps a floor from each tenant's own bursts on a two-second bucket. Serving what
a static split refuses costs a region that cannot spill: at 325 req/s and 10% of headroom, mean
service is +106.3 to +121.6% with a lease and +13.4 to +15.2% with a static split that sheds it.

**Residency.** The burst under the region-mean spill on a 1 s summary with own forwards, with a
share of tenants confined to their client's region:

| tenants confined | 250 req/s | served away | 325 req/s | served away |
|---|---|---|---|---|
| 0% | +1.2 to +1.4% | 11.6-12.3% | +1.7 to +1.8% | 15.7-16.6% |
| 25% | +2.1 to +3.6% | 6.7-7.1% | +34.0 to +39.8% | 9.7-9.9% |
| 50% | +3.5 to +13.4% | 5.8-6.1% | +55.5 to +67.5% | 8.1-8.4% |
| 75% | +21.6 to +33.6% | 3.5-3.8% | +121.2 to +128.2% | 4.5-5.1% |
| 100% | +71.7 to +82.1% | 0% | +222.6 to +249.6% | 0% |

Residency is a graded cost, and near the knee a steep one: at 325 req/s a quarter of the tenants
staying home costs +34.0 to +39.8%, the hot region having no headroom for demand that cannot leave.

**Active-active schedulers.** Two or four schedulers a region, each taking a session's requests by
hash, knowing its own decodes in flight exactly and its peers' as last reported, against one
scheduler a region at equal demand:

| reports every | two, 250 req/s | four, 250 req/s | two, 325 req/s | four, 325 req/s |
|---|---|---|---|---|
| continuously | identical | identical | identical | identical |
| 25 ms | +0.0% | +0.0% | +0.0 to +0.1% | +0.1% |
| 250 ms | +0.0 to +0.1% | +0.2 to +0.3% | +0.0 to +0.1% | +0.3 to +0.5% |
| 1 s | +0.2% | +1.6 to +1.9% | +0.1 to +0.3% | +11.7 to +13.4% |
| 5 s | +0.2 to +0.6% | +27.6 to +30.2% | +0.0 to +0.1% | +69.9 to +73.7% |

Four schedulers on reports five seconds old put 51-53% of decodes at a full batch at 250 req/s.

**The arithmetic.** A global scheduler on the request path adds a round trip to two-thirds of
requests: a mean of 40.0 ms at 30 ms one way and 82.0, 105.3 or 131.3 ms on Azure's triangle as it
sits in East US, West Europe or Japan East -- 534-910 and 1,094-2,987 times the sidecar tax and
4.0% and 8.2-13.1% of a one-second turn -- and a region cut off from it for 60 s at 250 req/s loses
450,000 request-seconds (`λD²/2`). At 10,000 nodes in three regions, liveness is 333 writes a second
a region and the global record 0.16-0.19 a second -- the table's six fractions every five minutes
and the clairvoyant budget's 14 and 18 node moves a day, scaled -- 1,700-2,200 times fewer. A static
split of a tenant's quota by its mean share refuses 15.6% and 23.5% of its demand on a real day of
amplitude 0.5 and 0.75, 5.2% and 12.4% with 25% of headroom and 0.3% and 4.2% with 50%; in 16 s of a
real day a region's share moves at most 0.023 and 0.037 points.

## Method

**On the fairness caveat.** Every comparison above between arms this repository wrote is a delta
against a hand-built baseline, which is what `phase-2.md`'s regret decomposition exists to retire.
It does so for the arms in the *Regret, oracle, coupling* section, where regret is reported directly
rather than implied by a service-time delta; results published before that section -- everything
from *Memory arbitration* through *The falsification test that fails* -- still carry the caveat as
stated, since they were not re-run through the oracle. The regret section names which of them the
re-run touches (`scored`, `flow only`) and lets the rest stand as before.

Arms share one code path and one seeded trace. Both partitioned arms are swept over all
splits and reported **at their oracle-best**, so the baseline is stronger than an operator
could tune blind. With a priority order, total stall is the wrong target — `prefer()` compares
goodput first (2pp tolerance, so latency can never be bought by dropping requests), then
band-lexicographically on stall.

Reported every run: mean stall per *served* request, p99, goodput overall and per class,
per-class stall, stall by phase, share of stall by class, and **admission integrity** —
`over_capacity`, per-class `refused`, `pinned_skips`. A nonzero `over_capacity` invalidates
the run.

**Reproducibility.** `residency`, `flows`, `placement`, `volatility`, `price` and `belief` are
byte-reproducible from the seed. `distributed`, `code-review` and `data-path` are not: each embeds
`boundary::measure`'s live host timing in its link costs, so two back-to-back runs differ by ~1 ms
of stall and several points of split rate. Their structural facts -- which gaps are exactly zero,
the order-of-magnitude separation between arms -- survive re-running; their last digits do not.

### Regime selection

A configuration is degenerate more often than it looks. Three quantities decide:

```
P = peak pinned serving bytes     (unreclaimable)
A = C - P                         (the arena the ledger actually arbitrates)
W = reclaimable working set

rho    = W / A           eviction pressure   -- can policy matter at all?
lambda = max_blob / A    admission lumpiness -- does refusal ever bind?
```

| zone | symptom | what it actually measures |
|---|---|---|
| `P >= C` | every arm mass-refuses | nothing; the pinned set does not fit |
| `rho << 1` | all arms identical, 100% hit | nothing; everything fits |
| `rho >> 1` **at low skew** | all arms identical, low hit | the workload's unservability |
| `lambda << 1` | zero refusals anywhere | eviction only; admission untested |

The third is treacherous — it looks like scarcity. **Standing diagnostic:** if one class
exceeds ~40% of total stall *and* its per-class numbers are within noise across arms, the
experiment is measuring that class's unservability, not policy. `share of total stall by
class` prints every run for this reason.

Target regime: `rho` 2–4, `lambda` 0.1–0.3, `P/C` 0.3–0.5. The last measured values
(`C` = 8 GiB unified, `P` = 3.4 GiB, `A` = 4.6 GiB, `lambda` = 0.11, `P/C` = 0.42) predate
the memory split. Under the split these quantities are per pool: HBM has no pinned state,
and DDR's pinned set is the serving replicas. They have not been re-derived.

### Avoiding a tuned result

Workload parameters are chosen for realism, defensible without reference to which arm wins.
Capacity should be chosen by a criterion pre-registered and independent of the outcome, and
the whole sweep reported rather than a single cell. Picking the capacity that maximises a
favoured arm's margin is the easiest way to manufacture a result here.

## Measured constants

`polyphonic calibrate` measures the tier model on the host: page-aligned allocation and
first-touch for DRAM, direct I/O (`F_NOCACHE` / `O_DIRECT`) for spill so the page cache cannot
absorb it.

| constant | measured |
|---|---|
| NVMe fixed latency | **113 µs** |
| NVMe bandwidth | **7.8 GB/s** |
| DRAM first-touch | **28 GB/s** |

The constants in `tier.rs` are a darwin/arm64 fit, not a law.

**Modelled, not measured:** node link latency and bandwidth in `Distance`, PCIe between host
and accelerator (`TierSpec::pcie`), the HBM/DDR capacities and splits, and the workload's
`exec_ns` constants. These are the numbers most worth replacing with real traces.

The hot path — lookup, scoring, eviction — makes **no syscalls**, so it is portable at zero
cost. Platform-specific code lives only in promote/demote, which runs at µs–100 µs scale
where a vtable is free. Portability and performance collide only if the OS is allowed into
the decision loop.

## Next

[`owned-and-observed.md`](owned-and-observed.md) is the design this ledger is being corrected
toward: what the orchestrator *owns*, *infers* and only *observes*, the data path, the workload
taxonomy in [`taxo.md`](taxo.md) as a scheduler input, and the phases (§9 there). Phases 0-11 are
built, their results are above, and §9 there lists what they leave open.

## Not built

No VMM, no WASM ABI, no exec rings, no edge agent, no live migration, and no system of record
(FoundationDB is chosen, `owned-and-observed.md` §8). The byte store is real
but exercised by `calibrate` only; the residency experiments run on the calibrated model
rather than moving real bytes. `Topology::discover` probes the host but the host is one
unified memory domain, so every cross-node constant is modelled and the HBM/DDR split exists
only in the model. There is no accelerator runtime: KV and weights are sized and priced, never
computed. The fleet has no half-width node, no second engine per node, no per-tenant axis on
`Quota` for host DDR, and no replica set per tenant; fan-out agents are not paired with a prefiller;
a replica set is one neighbour's. The regions have one accelerator class, ship no KV between
regions, inject no WAN partition and plan no budget from a forecast; `distributed` and `code-review`
place no client in a region.

## Standing

| claim | status |
|---|---|
| eviction priced as expected recovery cost per byte | holds — regret-discounted, recovery from the tier below |
| admission that refuses rather than overcommits | holds |
| soft floors beat hard partitions | **holds on split memory (32%), carried by weight residency (a lazy cache a placed fleet does not have); ties on unified** -- with the engine allocating KV it grows (36% at 20k, 45% at 60k) and wins on unified too (+7%) |
| open sharing | worst arm in both memory models |
| warm microVM cells are the cheapest state to rebuild | holds — 0.21–0.39 ns/byte |
| cells and KV compete for the same bytes | **unified-memory only** — separate pools in the target |
| greedy prefix affinity | right at low load, collapses past the knee |
| scored placement | best arm at every load and distance on the published engine — **4–6% end to end at moderate load, 38% near the knee**, and on a fleet of several models a lead that depends on layout (*Fleet*); restated as regret — its heuristic and execution gaps are exactly zero, every ns of its regret is model gap |
| score adapts sibling co-location to load | holds — 63–66% vs 85–86% for filtered specialists |
| score adapts tool placement to distance | holds — all calls local across regions, where hashing pays 10×, on a global argmin with no client; with a scheduler per region a function call never leaves its region (*Regions*) |
| all-or-nothing fan-out admission | holds where it binds, on the ledger's per-block test and on the router's partition check alike; the **+22%** is sensitive to the control crossing (+2% with none charged) |
| heterogeneous nodes (model host + agent host) | expressible — per-node memory, decode filter, origin round trip; a fleet of replicas per model, each a node with its own model, step and partition, is the same case |
| separating the orchestrator from the accelerator | free within a zone (0.16–1.7 ms), **61 ms per turn across regions** -- paid by the demand whose model the global tier placed elsewhere, 3.4% of client-facing requests at Phase 6's counts (*Regions*) |
| placement policy on that topology | worth 0.7% — the round trip and decode dominate, and no policy moves either |
| KV state transfer | roughly neutral end to end |
| state transfer taxes the FaaS warm pool | **retracted** — a unified-memory and capacity artifact |
| the score's handoff term prices co-placement | **fails** (pre-batching model, not re-run); the myopic regret oracle reproduces the same blind spot on demand — `flow only` beats `scored` on service time at 512 MiB while carrying far larger heuristic regret |
| memory coupling | regime-bound — **0.0%** at the `distributed` defaults (nothing binds), **54–85%** at 4 GiB DDR/node; locality coupling 0.8–1.7% in both. **With the engine allocating KV, 0–26%** (0–2% on the scored arms): most of it was the ledger allocating the engine's offload |
| clairvoyant eviction vs GDSF | budget-matched: wins hit rate (+22.8pp), loses on cost (+3.7%) — GDSF gives up the *cheap* hits, so cost-weighting is already doing the work |
| unified control plane beats RPC-queried | rounding error in aggregate (+0.02 ms/request); 33.7% of a warm `FaaS` invocation against its chosen `exec_ns` |
| announce / anticipatory prewarm | 11–18% faster tasks, net work slightly worse — **half to three-quarters of it was KV prewarm**, which the engine does not let the orchestrator write; 3–9% with the engine allocating. **All of the KV margin was prewarm, none retention**, and a prefill of the declared downstream dispatched with the hint buys 24–28%. Open-loop figure: the trace submits the downstream before its upstream ends (*Programs*); the published number is kept. |
| downstream-aware gate | coupling tier 1 — replicable by a hint API |
| data path binds below ~1 ms, dissolves an order of magnitude above | holds — crossover 0.84–1.42 ms / 4.35–7.42 ms, measured tax not borrowed |
| out-of-process hook caps scheduler fleet size | holds — ~20 nodes (`ext_proc`) vs ~1000 (`Wasm`), `d` measured not assumed |
| the ledger allocates the engine's KV | **corrected** — `--engine-cache` hands allocation to an engine LRU inside an orchestrator-sized partition; off by default so every row above stays the ledger's |
| ceding KV allocation costs service time | **no, where decode dominates** — −0.2% mean, −0.4% p99 on the cluster; on one node it is cheaper, because the partition hands weights the slack |
| optimistic admission moves its cost onto another class | **class-blind on the engine's own order** — the preempted arrival pays; chat turns and task stages lose p99 about equally, sized at half the partition on an engine that gave memory away. **Class-shifted once the router chooses the victim** (*Enforcement*): the interactive first-token p99 at the 63 ms floor, the throughput class's completion paying |
| a better block manager is worth asking for | ~3% of stall at 17–23pp KV hit, at every partition size — more than the partition's size moves the A/B below 1x |
| shared L2 tier | fires on KV within a rack, on weights only from zone out |
| the router's view of the engine's KV can be a lossy belief | **yes, at almost no cost** -- within 0.16% of the exact view at 20% batch loss with no recovery, within 0.075% with replay; the belief is wrong 37-60% of the time in the worst cell and it does not show |
| silence reads as calm and herds the router onto a quiet node | **not on the integrated path**, where load comes from the traffic; a stream-fed load herds or starves by luck, and adding the router's own dispatches starves the node (+3.1% service at 8 s) |
| `P(resident)` as `1 - V/B` is calibrated | **no** -- near the diagonal with replay, over-confident (0.96 predicted, 0.70 realised) with unrecovered loss |
| the gossip result is about engine telemetry | **retracted** -- it was about an informer cache over owned state in one regime; engine KV from the channel moves residency-greedy +21% at the published partition and -30% at half of it |
| the score can stop reading the exact output length | **yes** -- the observed mean is within 0.13% and better; a quantile of it is not wanted |
| retention directives buy back what the correction removed | **no** -- holding a declared flow's blocks is worth 0.0-0.1pp; an oracle emitter that knows every next use buys at most 1.4% of stall and 0.1% of service, with the sign changing across seeds; the bump half of `announce` carried -5% to +9% of its KV margin |
| the engine lets the orchestrator prewarm | **yes, by dispatch** -- prefill-ahead buys 24-28% task latency on one node and cuts a flow downstream's stall by 64% at rack, 31% at zone and 7% at region, for 1.5-1.7x its saving in prefill work; only about half land where the downstream is placed. Open-loop figure: the trace submits the downstream before its upstream ends (*Programs*); the published number is kept. |
| an ignored directive can make the router's view wrong | **no** -- it changes no event, and an acknowledged belief is byte-identical to no directives; a belief that trusts its own requests is over-confident (0.975 against 1.000 realised) only where removals go undelivered |
| divergence has separable causes | **yes** -- undelivered removals dominate; 1-8% of the no-recovery column is stranded optimistic entries; misses are zero at every sample |
| a deadline on the ledger's bump | bounded state (no stale entries against 27-852) and 0.8-2.0 points of task latency with honest hints |
| session reuse is worth retaining for | **by shape yes, by size no** -- reuse returns 3.8x past median residency, and the misses cost 0.2-0.3% of service |
| a step reads every resident model's weights once, so a batch per model is what a node pays | **holds** -- a four-model node is +241-279% of the pooled engine; pricing it in the score recovers 21-29 points and no placement is found by a score |
| one model per node at the partition its weights leave | **holds** -- within 0.3% of the published engine at 500 req/s and -71% of the lazy cache; partition size and offload grant move service by under 0.15% |
| routing's lead over hashing is a count of replicas | **partly** -- equal on the published engine and one model of eight replicas (-15 to -32%); with two replicas a model it depends on layout, -2.7% to -63.4% |
| a placement has to follow the mix | **holds** -- made once, 6.2-6.6 s of mean service; a 30 s start +77-82% and 30 s late +110-120% over on time |
| a rent-or-buy planner bounds lateness | **holds** -- +8.5-9.6% over a clairvoyant one, 92% under a placement made once; it makes the same moves as a greedy one on the rotating mix and 1 against 16-27 at equal demand |
| prefill is engine time | **holds** -- +13-17% of service at the published partition, +63-81% at half; a 1 ms allowance leaves under 0.6%; prefill-ahead then costs +2.7% and keeps -56% stall |
| KV keyed by model prices a cross-model fan-out | **holds** -- +27-32% prefill work, +3.5-5.1% service under prefill time |
| disaggregated prefill beats aggregated | **only at one prefiller in eight on the published mix (-1.6 to -2.6%) and two in eight on fresh prompts (-9.6%)**; every larger ratio loses, by 100% or more at three in eight at 500 req/s |
| a sidecar's list of prefillers | **fails** where the prefiller saturates -- +3,500% at one in eight on fresh prompts; `joint` declines 76% of the pairs there |
| cross-tenant prefix sharing is the highest-value hit | **retracted** -- zero on the published workload (0 of 384,159 touches); with a prefix per model, 9-10% of touches and no measurable service |
| a noisy neighbour's damage is cache | **no** -- engine time: +1-3% while prefill is free, +11% to +62% once it takes engine time |
| a router quota on prefill work isolates a neighbour | **yes** -- +1.9-3.5% to the others against +11% shared; a replica set is the hard version, +3% and nothing outside at one of eight, and ahead of a quota at the same allowance at every duty cycle; a cap on sequences per tenant refuses the others |
| a tenant-aware block manager is worth asking for | **no at the published partition** (under 0.3% of service for every group), up to 1.8% to the quiet half at half of it |
| a cancel by declared class moves the loss to the class that waits | **yes** -- the interactive first-token p99 at the 63 ms floor and the throughput class's completion later; where the classes are drawn alike one p90 and the tiered claim are a scalar |
| borrowing against the mean for throughput work | **retired where memory binds hardest** -- at 0.6x with prefill taking engine time the throughput class finishes 1.5-1.7x later than under its own p90; level or ahead at 0.75x |
| restart against continuation | continuation, by +26% to +183% on the batch class's completion; a restart throws away 171-1007 sequence-seconds |
| an llm-d-shaped utilisation gate against a per-class claim | the evicted class pays 2.2-4.4x in completion where memory binds, and the interactive class up to 7.0x at 0.75x with prefill and at 0.6x |
| a quantile claim needs a second cancel trigger at the engine | **no** -- the router's check alone keeps the interactive first-token p99 under 1 s on every seed; the engine trigger takes one seed from 691 to 64 ms and costs 1.3-2.4x at 0.6x with prefill |
| a client that leaves is the cancel's first job | **holds at 20%** (1.7-3.2x on the interactive first-token p99 leaked against aborted), at 5% only with prefill free |
| the stalled-stream buffer is a memory-arbitration event | **retracted for text** -- under 1.8 MB a node |
| a closed loop is what the open-loop trace measured | **no** -- the trace submits 100% of tool calls and fan-outs before their turn ends, by about a second; a closed-loop turn is 3.6-3.8x as long, and *announce* and *prefill-ahead* are open-loop figures, kept as published |
| prefill-ahead survives a causal arrival and an estimate | **an estimate keeps about half of the open-loop win (24-31% of the flow stall against 63%), and a causal arrival almost none** -- 0-5% released at the upstream's end |
| a stream that names the tool warms what the model restores | **yes, and a predictor adds nothing** -- -0.03 to -0.04% of turn latency either way; a restore is 3 ms against a decode of a second |
| speculating read-only tools | **small where tools are short** -- -1.3 to +1.6% on coding tools, -3.2 to -3.7% on research tools wasting twice what they save; a perfect predictor -17% on research; unpriced, it shortens others' tool waits, since a hit leaves the queue |
| leases and drafts bind only where host DDR does | **leases bind at 8 GiB** -- 7% of a node's DDR pinned, 27-30% at 4 GiB; none broken; draft reclaim under 1% either way |
| retention through a tool call (Continuum's TTL) | **null** -- +0.00% of turn latency; a call's rebuild is its sequence's preemption |
| one suspend decision for KV and sandbox | **differs from two timers on 45.5% of idle gaps, frees no more** -- 62.8% against 64.5% of idle time; resumes cost under 0.15% of a turn |
| the logged tier is orders of magnitude below the soft tier | **no** -- 20-66% of the soft tier's decisions on the agent presets; the record tier is 1,400-3,000 times below at four nodes, and its largest writer is liveness (17 times the planner's) |
| prefix caching reuses retrieved chunks | **by order** -- 36% in a fixed order, 12% in relevance order; reuse by content 64% (counterfactual) |
| a role's own quantile replaces the pooled one | **yes where claims bind** -- overruns 9-13% against 63% for reviewers; fan-out service -2.8% to +3.3% at 160 MiB |
| the class is recoverable from observables | **partly** -- 60.6% at the first call, 85.7% at the last; the consumers move by under 15% |
| coupling is confined to the patterns with flows | **locality yes, memory not on programs** -- 13-15% on pipelines, 4-7% on agentic and multi-agent, 0% elsewhere; memory coupling 0% on programs, 4-41% on the published trace at 2 GiB |
| a restart's cost is its soft state | **no** -- its outage and its streams: `λD²/2` with the streams held (126-139 request-seconds at 1 s, 32,426-38,829 at a 15 s lease), 61-73 decode-seconds and 63-75 request-seconds more at a 0.1 s outage when they die with the scheduler; the belief, the flow graph and the estimators cost nothing a run can see |
| a 15 s lease is a takeover for a scheduler on the request path | **no** -- 32,000-39,000 request-seconds a crash; node agents enforcing their own partitions let a standby take over without one |
| spreading retries by a backoff beats a burst | **no** -- 7-32% more request-seconds for 9-18% fewer requests unserved |
| a blind reservation ledger over-admits | **only where memory binds hardest** -- 3-30% of blind admissions at 0.6x and below, none at 1.0x and 0.75x; the engine's wait absorbs it and a node check removes all but 0-4 |
| the estimators need a checkpoint | **no** -- lengths observed when a decode ends re-learn within a fraction of a second; a snapshot restores them to the digit at 32 KiB every 10 s |
| an engine crash costs its KV | **no** -- its replica's restart, 3.6-5 request-seconds a second down at 1.0x, whether or not the spill survives |
| a lost node is an engine crash that never returns | **plus 32-62 request-seconds** at 1.0x for its host work, its gangs (fan-outs refused 0 -> 23-28), and the lease: 4,200-4,800 request-seconds if the router waits 10 s and about 65,000 at 40 s |
| durable sandboxes survive every fault | **all but the loss of their node** -- 18-20 cells lost in a long-running run, none with a copy made when each is marked, at 0.14-0.29 MiB/s a node |
| write-through checkpoints | **ruled out** -- 0.6-21 times a warm `FaaS` invocation at a protected-drive or FoundationDB commit, 4 ms with this host's full flush |
| the logged tier outgrows FoundationDB | **no** -- 146,000-491,000 writes a second at 10,000 nodes against 820,000 published |
| the integrated path pays for its crashes | **while it crashes no more than about once a minute or two with its streams held and once an hour or two with them fate-shared**; a fail-open proxy's window costs 12-53 request-seconds |
| with clients in regions, the published global argmin serves two-thirds of requests elsewhere | **holds** -- 66.3-66.7% of client-facing requests, a warm `FaaS` call in 40.4-40.7 ms instead of 0.6, +7.1-7.2% over a scheduler per region; every other region-distance figure here has no client |
| the global argmin is the bound on regional schedulers | **no** -- with the round trip priced it still serves 37-41% of requests elsewhere and is 4.1-4.6% slower at 30 ms between regions and 11.2-12.2% on Azure's round trips; the score's engine and congestion terms decide within a region |
| a region absorbs its own peak | **no** -- a burst of 2.25 times its share costs +72-82% at 250 req/s a region and +223-250% at 325; every cross-region rule recovers it to within 5.6% and 4.3%, 6.8% on Azure's round trips |
| a spill priced on a stale summary | **herds**, and counting the sender's own forwards removes most of it; priced by each region's mean, within 0.4 points of the exact view in every shape; a threshold is right at one load (0.7: 0.0% at 250, +1.2-1.4% at 325; 0.5 the reverse) |
| a global routing table on the provisioning clock | free at equal demand, blind to a burst shorter than its epoch (+9.3-19.0% at 5 s) and short near the knee (+39.2-51.8% on the burst at 325); over the region-mean spill, within 0.3 points of the spill alone |
| node budgets that follow the day | **recover it** -- on time +1.0-1.7% with headroom and +1.5-4.7% near the knee, against fixed budgets' +3-80% and 2.8-7.5 times the service; lateness is the price, and 30 s late is worse than never moving at 250 req/s |
| a rent-or-buy budget planner lands between on time and 10 s late | **no** -- between 10 s and 30 s late; a spill or the table beneath the budget brings it within 1.3 points of 10 s late |
| a model in another region costs its round trip; every model in every region costs a knee | **holds** -- Phase 6's counts spread across regions forward 3.4% of client-facing requests for +0.7%; every model everywhere is 3.7-4.0 times the service, 48-49% of decodes at a full batch |
| a tenant's regional share is a lease | **holds** -- refreshed every 0.1-0.5 s it removes the 1.4-2.6 points of refusals a day adds to a static split; a faster refresh chases noise and a slower one lags, both artefacts of the compressed day |
| residency is a switch | **no, a graded cost** -- a quarter of tenants kept home costs +2.1-3.6% at 250 req/s a region and +34.0-39.8% at 325 |
| active-active schedulers in a region are free at the engine's step | **holds** -- within 0.1% on 25 ms reports; on 1 s reports four herd, +1.6-1.9% at 250 req/s and +11.7-13.4% at 325, and two stay within 0.6% at every age |
| a scheduler on the request path can be global | **no** -- 40-131 ms a request, 534-2,987 times the sidecar tax; a minute's partition costs a region 450,000 request-seconds at 250 req/s |
| the global record writes under once a second at 10,000 nodes | **holds** -- 0.16-0.19 a second, 1,700-2,200 times under one region's liveness |
