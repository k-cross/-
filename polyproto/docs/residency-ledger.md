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

Two tiers of cross-workload win, and only one is a moat:

- **Tier 1 — information sharing.** "The function will call inference, so prewarm the model."
  A siloed stack retrofits this with a hint API.
- **Tier 2 — joint decisions.** Cannot be expressed as a hint without becoming a distributed
  agreement problem: co-placement fused with routing, preemption across classes, and **one
  memory ledger arbitrating every workload's state at once.** On datacenter hardware
  that is one ledger over two pools, and the pools meet only where the accelerator
  offloads into host memory. See *Memory pools*.

This document is about Tier 2, specifically the ledger.

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

The merkle lineage `H(image, init_result, env)` was always Firecracker-shaped: a serialised
memory image with ancestry. The old constant, 200 ms / 32 MiB charged linearly, was a cold
*container* start, which is a different substrate. Demand-paged restore costs a fixed few
milliseconds plus the pages actually touched, so it is roughly **flat** in image size. The
footprint stays the whole guest image, because a warm cell holds all of it. So a snapshot is
now big to hold and cheap to rebuild, which is exactly the profile of something to evict
first. v8 isolates are explicitly not modelled: they would be a third answer, with no image
to restore.

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

`--hbm 0` collapses the node to one pool, where every class competes in DDR directly. That
is the Apple-silicon model the earlier rounds ran on, and it is kept for comparison only.

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

**Warm microVM cells are the cheapest bytes to rebuild** once restore is modelled as
Firecracker actually does it. Under the cold-container constant they were 5.96 ns/byte.

An earlier version of this section compared them with KV blocks as if the two competed for
the same bytes, and concluded the warm pool was hoarding memory that KV should have. **That
only holds on unified memory.** In a datacenter node, cells live in DDR and hot KV lives in
HBM. The comparison that remains real is against *offloaded* KV in DDR. There the cell's low
rebuild cost and the offload's low promote cost are both cheap, and the offload yields first
by band. Two consequences survive the split:

- **Bigger cells are cheaper per byte to evict** (0.39 → 0.21 across the size range), because
  the fixed restore cost amortises. Under a linear model, size was neutral.
- **Keep-alive matters less than it used to.** In the single-node arbitration run, FaaS
  costs 4–5 ms of stall per request whatever the snapshot hit rate, because a restore is
  cheap.

`freq` still cuts the other way. A hot function is reused hard, so the outcome is contested
per function rather than settled per class.

## Serving engines

A decode step reads the weights once whatever the batch size, so a second sequence is nearly
free and the sixty-fourth is not. Per-token latency rises with occupancy while throughput
saturates:

```
step_ns(batch) = STEP_BASE_NS + (batch - 1) × STEP_PER_SEQ_NS     # 7 ms + 40 µs
```

`Engine` holds one of these per domain, tracks in-flight sequences against an arrival clock
set by `--rate`, and queues a request that finds the batch at `MAX_BATCH`. Constants are
**modelled**; the base is chosen so a batch of one reproduces the flat 125 tok/s the workload
used before, which keeps the unbatched arm comparable. Batch is sampled once at admission and
held for the sequence — a step-accurate engine is a different simulation, and the error is
second-order next to modelling no batch at all.

This is not a refinement. **Modelling decode as a constant makes the central inference
scheduling tradeoff invisible**, because the reason to route by KV prefix is to avoid the
recompute a *full* node would force, and a node cannot be full if occupancy has no cost. With
`--rate 0` the engine is off and the older results reproduce; every claim below about
placement depends on it being on.

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
  `distributed` defaults to 250 req/s rather than 350. `--fanout 0` reproduces the
  pre-fan-out results within the measured-crossing noise between runs.
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

Measured on the host, not modelled. `polyphonic boundary --repeat 5`, best-of-5 per rung,
timer overhead subtracted from the per-operation rungs.

| boundary | 64 B | 1 KiB | 8 KiB | ns/byte | spread |
|---|---|---|---|---|---|
| native call | 0 | 0 | 0 | 0.000 | 1.0× |
| shared ring (spin) | 52 | 65 | 232 | 0.023 | 4.2× |
| syscall floor | 97 | 97 | 97 | 0.000 | 2.2× |
| pipe (same thread) | 459 | 475 | 638 | 0.022 | 1.8× |
| unix socket RTT | 6023 | 6440 | 6649 | 0.059 | 1.2× |
| TCP loopback RTT | 15483 | 15400 | 15899 | 0.058 | 1.0× |
| gRPC unary RTT | 51232 | 49482 | 52483 | 0.253 | 1.1× |

`spread` is worst run over best, up to 4.2× on the cheap rungs because this host migrates
threads between core clusters and `pin_cluster` is only a QoS hint on macOS. **The ordering
is the robust result; no single constant here should be quoted to two digits.**

What each step adds, at 1 KiB:

| step | adds | × |
|---|---|---|
| cross-core cache line + spin detect | 0.07 µs | 65 |
| ring transition | 0.03 µs | 1.5 |
| kernel buffer copy + second syscall | 0.38 µs | 4.9 |
| **waking a blocked thread** | **5.96 µs** | **13.6** |
| loopback network stack | 8.96 µs | 2.4 |
| HTTP/2 framing + protobuf | 34.08 µs | 3.2 |

Four things to design against:

1. **A no-op syscall (97 ns) costs more than an entire shared-memory round trip (65 ns).**
   Any hot path spending one syscall per decision has already given up more than the whole
   budget of the alternative. "Fewer syscalls" is not the lever; zero is.
2. **The largest single step is waking a thread** — bigger than crossing the kernel, bigger
   than the network stack. The tax to remove is the scheduler, which argues for spin-polled
   rings and against anything that blocks on the hot path.
3. **Two thirds of a gRPC round trip is framing and encoding** — 34 µs of 49 µs sits above
   raw TCP. The majority of the cost is self-inflicted.
4. **Marshalling slope is flat except for gRPC.** At control-plane message sizes the fixed
   cost dominates, so batching decisions matters more than shrinking them.

### Where it bites: the denominator

The tax is a fraction, and the fraction is set by how long the scheduled work takes. At 4
decisions per request, against service times measured in the workload itself:

| work being scheduled | over gRPC | over a shared ring |
|---|---|---|
| **warm FaaS invocation (measured: 129 µs)** | **60.1%** | 0.23% |
| **warm service request (measured: 256 µs)** | **43.1%** | 0.12% |
| 1 ms of work | 16.2% | 0.03% |
| 100 ms of work | 0.2% | 0.00% |

**For a warm invocation, 43–60% of the request is the control plane talking to itself.** For
an agent turn that spends a second in decode, it is a rounding error. The zero-cost-extension
thesis is not wrong, it is *conditional*, and the condition is sharp: put the extension
boundary where decisions are coarse, never inside the ledger's hot path.

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

Minimised, not maximised. The earlier form scored the recompute a node's residency *avoided*,
which is the same decision by a constant whenever rebuilding is the only way to get state —
and stops being the same decision the moment shipping it is an option, because what a node
avoids no longer determines what the request costs there.

`TierPool::marginal_price` is the *expected* cost per byte of the cheapest state the pool
would give up, and `Hierarchy::displacement` charges each pool's shortfall at that pool's
price. It peeks each reclaimable class's heap top in the same band order `pick_class`
reclaims in, abstaining on a stale or pinned top. It is an estimate on purpose: a faithful
dry run costs as much as the eviction itself, per candidate, per request. Three corrections
make it a price rather than a number:

- **Recovery, not rebuild.** An evicted blob moves down a tier rather than vanishing, so
  wanting it back costs the cheaper of a rebuild or a recovery from that tier: PCIe from DDR
  for HBM, the spill tier for DDR. Priced as a rebuild, a weight shard pushed off a full
  accelerator cost 4 s, where the real cost is a 21 ms promote. Once fetching made full
  accelerators candidates, that error dominated every other term.
- **Units on the fallback.** When nothing is reclaimable, the price is the last price
  actually paid, in ns/byte. It used to fall back to `inflation`, a GDSF priority that only
  ever grows. On unified memory the fallback was rarely hit. Under split memory the
  accelerator's two classes often both sit at their floors. The fetch arm's displacement
  spread then read **26–49 s**, and fell to 14 ms once the units were fixed.
- **Expected, because evicted state only costs anything if someone wants it back.** Each
  class's loss per byte is multiplied by its **measured regret rate**: the fraction of its evictions
that were later requested again. That comes from a bounded ghost list of recently evicted
ids, which is ARC's ghost cache used for pricing instead of admission. The rate is smoothed
as `(regrets + 1) / (evictions + 1)`, so a pool with no history prices displacement at the
full recompute cost, as before, and converges on the observed rate as evidence accumulates.

Without the discount, displacement priced every evicted byte as a certain rebuild. Its mean
spread across candidate nodes was **3.5 s against 68 ms** for engine and congestion
combined. An argmin decides on spread, so neither load term could win anything but ties:
`moved by load` read 0.0%. The discount alone cut displacement spread 4×; the convex toll did
the rest. `term spread` in the `distributed` output reports the mean spread of every term,
because a term whose spread is an order of magnitude under another's is a comment, not a
policy.

Two rules the score needs to be a decision rather than a suggestion:

- **The score is the whole decision.** Gating it on a separate residency threshold discards
  the scored choice using a metric the score never consulted.
- **Never move without a reason.** With nothing resident anywhere all costs are equal and an
  argmin over ties sends every cold request to one node; the chosen node must strictly beat
  the affinity node, otherwise content affinity stands.

## Results

All numbers below are split memory unless marked unified. Two methodology changes since the
previous round apply to everything here:

- **Service time leads.** Once decode cost depends on the batch a request joins, placement
  moves execution as well as waiting. `stall` excludes execution, so on its own it
  misreports which arm is better. The `distributed` table now leads with end-to-end
  `service/req`. It also shows fan-out completion, because arms that refuse the most
  expensive work look faster than they are.
- **Baselines filter before they choose.** The unscored policies used to send every sibling
  to one node and refuse whenever the siblings did not fit there. Under split memory that
  meant 55% of fan-outs, a failure no real system has. They now filter infeasible nodes
  first, as a Kubernetes filter phase or a model-aware inference router would, and apply
  their rule to what remains.

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
gate is Tier 1.**

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
  Last round's claim that the score's tool calls are 3× cheaper within a zone does not
  survive: hashing lands on warm cells just as often.

**State transfer is roughly neutral.** Fetch ships 26–35 GiB at socket and rack and costs
about 1.5 ms more stall. It improves balance enough that service time comes out equal or
0.2% better. Across a zone or region it ships only weight shards (5–6 GiB), never KV.

**Retracted: "state transfer taxes the FaaS warm pool."** Last round, fetch was 7–9% slower
and function warm hits fell from 73% to 39%. Shipped KV copies were evicting microVM cells.
Both effects came from putting KV and cells in one pool. With separate pools, function calls
cost the same with and without fetch (1.07 vs 1.06 ms). With unified memory at matched
capacity (48 GiB) the effect also disappears, so it was partly a capacity artifact as well.

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
| control-plane RPC (`unified` → `rpc query`) | +0.02 ms/request; **33.7% of a warm `FaaS` invocation** |

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

That inverts the earlier no-pressure result, where keeping tool calls local was right. Under
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

## Method

Arms share one code path and one seeded trace. Both partitioned arms are swept over all
splits and reported **at their oracle-best**, so the baseline is stronger than an operator
could tune blind. With a priority order, total stall is the wrong target — `prefer()` compares
goodput first (2pp tolerance, so latency can never be bought by dropping requests), then
band-lexicographically on stall.

Reported every run: mean stall per *served* request, p99, goodput overall and per class,
per-class stall, stall by phase, share of stall by class, and **admission integrity** —
`over_capacity`, per-class `refused`, `pinned_skips`. A nonzero `over_capacity` invalidates
the run.

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

| | guessed | measured |
|---|---|---|
| NVMe fixed latency | 90 µs | **113 µs** |
| NVMe bandwidth | 3.0 GB/s | **7.8 GB/s** |
| DRAM first-touch | 20 GB/s | **28 GB/s** |

Correcting the NVMe bandwidth guess moved a headline by 42%. The constants in `tier.rs` are a
darwin/arm64 fit, not a law.

**Modelled, not measured:** node link latency and bandwidth in `Distance`, PCIe between host
and accelerator (`TierSpec::pcie`), the HBM/DDR capacities and splits, and the workload's
`exec_ns` constants. These are the numbers most worth replacing with real traces.

The hot path — lookup, scoring, eviction — makes **no syscalls**, so it is portable at zero
cost. Platform-specific code lives only in promote/demote, which runs at µs–100 µs scale
where a vtable is free. Portability and performance collide only if the OS is allowed into
the decision loop.

## Not built

No VMM, no WASM ABI, no exec rings, no edge agent, no live migration. The byte store is real
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
| soft floors beat hard partitions | **holds on split memory (32%), via host offload; ties on unified** |
| open sharing | worst arm in both memory models |
| warm microVM cells are the cheapest state to rebuild | holds — 0.21–0.39 ns/byte |
| cells and KV compete for the same bytes | **unified-memory only** — separate pools in the target |
| greedy prefix affinity | right at low load, collapses past the knee |
| scored placement | best arm at every load and distance — **4–6% end to end at moderate load, 38% near the knee** |
| score adapts sibling co-location to load | holds — 63–66% vs 85–86% for filtered specialists |
| score adapts tool placement to distance | holds — all calls local across regions, where hashing pays 10× |
| all-or-nothing fan-out admission | holds where it binds — +22% fan-outs, inference stall −10% |
| heterogeneous nodes (model host + agent host) | expressible — per-node memory, decode filter, origin round trip |
| separating the orchestrator from the accelerator | free within a zone (0.16–1.7 ms), **61 ms per turn across regions** |
| placement policy on that topology | worth 0.7% — the round trip and decode dominate, and no policy moves either |
| KV state transfer | roughly neutral end to end |
| state transfer taxes the FaaS warm pool | **retracted** — a unified-memory and capacity artifact |
| the score's handoff term prices co-placement | **fails** (pre-batching model, not re-run) |
| unified control plane beats RPC-queried | rounding error in aggregate; 43–60% of a warm invocation |
| announce / anticipatory prewarm | 11–18% faster tasks, net work slightly worse |
| downstream-aware gate | Tier 1 — replicable by a hint API |
