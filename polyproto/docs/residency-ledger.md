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
corrected side, and *Engine allocation* says what it changes.

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

`ext_proc` at 4 nodes (142 µs) already exceeds a warm FaaS invocation's modelled ~129 µs. At a
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

All numbers below are split memory unless marked unified, and run with the ledger allocating KV
unless marked **engine**: *Engine allocation* and *Belief* give the corrected side. Two rules apply
to everything here:

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

Same cluster, 15k requests, `--rate 250`.

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
across regions, and that conclusion is independent of how good the scheduler is.

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
output can be modelled and held for the decode's length (`--decode-kv`). Each result below is
marked **ledger** or **engine** for the side of the bit that produced it; every number without a
mark in this document is **ledger**.

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
equally (+14% each at a quarter of the partition): class-blind, spread by arrival.

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
taxonomy in [`taxo.md`](taxo.md) as a scheduler input, and the phase plan (§9 there). Phases 0-4
and 8 are built, and their results are above. Phase 5, retention directives, is next.

## Not built

No VMM, no WASM ABI, no exec rings, no edge agent, no live migration, and no system of record
(FoundationDB is chosen, `owned-and-observed.md` §8). The byte store is real
but exercised by `calibrate` only; the residency experiments run on the calibrated model
rather than moving real bytes. `Topology::discover` probes the host but the host is one
unified memory domain, so every cross-node constant is modelled and the HBM/DDR split exists
only in the model. There is no accelerator runtime: KV and weights are sized and priced, never
computed.

## Standing

| claim | status |
|---|---|
| eviction priced as expected recovery cost per byte | holds — regret-discounted, recovery from the tier below |
| admission that refuses rather than overcommits | holds |
| soft floors beat hard partitions | **holds on split memory (32%), carried by weight residency; ties on unified** -- with the engine allocating KV it grows (36% at 20k, 45% at 60k) and wins on unified too (+7%) |
| open sharing | worst arm in both memory models |
| warm microVM cells are the cheapest state to rebuild | holds — 0.21–0.39 ns/byte |
| cells and KV compete for the same bytes | **unified-memory only** — separate pools in the target |
| greedy prefix affinity | right at low load, collapses past the knee |
| scored placement | best arm at every load and distance — **4–6% end to end at moderate load, 38% near the knee**; restated as regret — its heuristic and execution gaps are exactly zero, every ns of its regret is model gap |
| score adapts sibling co-location to load | holds — 63–66% vs 85–86% for filtered specialists |
| score adapts tool placement to distance | holds — all calls local across regions, where hashing pays 10× |
| all-or-nothing fan-out admission | holds where it binds, on the ledger's per-block test and on the router's partition check alike; the **+22%** is sensitive to the control crossing (+2% with none charged) |
| heterogeneous nodes (model host + agent host) | expressible — per-node memory, decode filter, origin round trip |
| separating the orchestrator from the accelerator | free within a zone (0.16–1.7 ms), **61 ms per turn across regions** |
| placement policy on that topology | worth 0.7% — the round trip and decode dominate, and no policy moves either |
| KV state transfer | roughly neutral end to end |
| state transfer taxes the FaaS warm pool | **retracted** — a unified-memory and capacity artifact |
| the score's handoff term prices co-placement | **fails** (pre-batching model, not re-run); the myopic regret oracle reproduces the same blind spot on demand — `flow only` beats `scored` on service time at 512 MiB while carrying far larger heuristic regret |
| memory coupling | regime-bound — **0.0%** at the `distributed` defaults (nothing binds), **54–85%** at 4 GiB DDR/node; locality coupling 0.8–1.7% in both. **With the engine allocating KV, 0–26%** (0–2% on the scored arms): most of it was the ledger allocating the engine's offload |
| clairvoyant eviction vs GDSF | budget-matched: wins hit rate (+22.8pp), loses on cost (+3.7%) — GDSF gives up the *cheap* hits, so cost-weighting is already doing the work |
| unified control plane beats RPC-queried | rounding error in aggregate (+0.02 ms/request); 33.7% of a warm `FaaS` invocation against its chosen `exec_ns` |
| announce / anticipatory prewarm | 11–18% faster tasks, net work slightly worse — **half to three-quarters of it was KV prewarm**, which the engine does not let the orchestrator do; 3–9% with the engine allocating |
| downstream-aware gate | coupling tier 1 — replicable by a hint API |
| data path binds below ~1 ms, dissolves an order of magnitude above | holds — crossover 0.84–1.42 ms / 4.35–7.42 ms, measured tax not borrowed |
| out-of-process hook caps scheduler fleet size | holds — ~20 nodes (`ext_proc`) vs ~1000 (`Wasm`), `d` measured not assumed |
| the ledger allocates the engine's KV | **corrected** — `--engine-cache` hands allocation to an engine LRU inside an orchestrator-sized partition; off by default so every row above stays the ledger's |
| ceding KV allocation costs service time | **no, where decode dominates** — −0.2% mean, −0.4% p99 on the cluster; on one node it is cheaper, because the partition hands weights the slack |
| optimistic admission moves its cost onto another class | **class-blind, not class-shifted** — the preempted arrival pays; chat turns and task stages lose p99 about equally |
| a better block manager is worth asking for | ~3% of stall at 17–23pp KV hit, at every partition size — more than the partition's size moves the A/B below 1x |
| shared L2 tier | fires on KV within a rack, on weights only from zone out |
| the router's view of the engine's KV can be a lossy belief | **yes, at almost no cost** -- within 0.16% of the exact view at 20% batch loss with no recovery, within 0.075% with replay; the belief is wrong 37-60% of the time in the worst cell and it does not show |
| silence reads as calm and herds the router onto a quiet node | **not on the integrated path**, where load comes from the traffic; a stream-fed load herds or starves by luck, and adding the router's own dispatches starves the node (+3.1% service at 8 s) |
| `P(resident)` as `1 - V/B` is calibrated | **no** -- near the diagonal with replay, over-confident (0.96 predicted, 0.70 realised) with unrecovered loss |
| the gossip result is about engine telemetry | **retracted** -- it was about an informer cache over owned state in one regime; engine KV from the channel moves residency-greedy +21% at the published partition and -30% at half of it |
| the score can stop reading the exact output length | **yes** -- the observed mean is within 0.13% and better; a quantile of it is not wanted |
