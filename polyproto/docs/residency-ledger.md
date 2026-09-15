# The Residency Ledger

Design notes for Polyphonic's scheduler core. Prototype: this records what is built, what
each mechanism is worth on the measured workload, and which constants are measured rather
than modelled. Numbers are from `darwin/arm64`; re-derive per host.

## The claim under test

> A single scheduler that owns FaaS, AI inference, and long-running compute beats three
> best-in-class specialists, *on the boundaries between them*.

Two tiers of cross-workload win, and only one is a moat:

- **Tier 1 — information sharing.** "The function will call inference, so prewarm the model."
  A siloed stack retrofits this with a hint API.
- **Tier 2 — joint decisions.** Cannot be expressed as a hint without becoming a distributed
  agreement problem: co-placement fused with routing, preemption across classes, and **one
  memory ledger arbitrating every workload's state at once.**

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
| `Snapshot` | drop warm cell | cold init (~200 ms / 32 MiB) |
| `WeightShard` | drop weights | reload (~4 s / 512 MiB) |
| `ServiceHeap` **idle** | scale down | cold start (~15 s / 384 MiB) |
| `ServiceHeap` **serving** | *forbidden* | — |

A long-running replica is a blob whose recompute cost is its cold start. Scale-up and
scale-down are admission and eviction — which is what puts an autoscaler and a KV-cache
allocator in the same ledger.

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
O(log n) without periodic rescoring, and it doubles as **the shadow price of memory** — the
marginal value of the cheapest byte the system will currently give up. That is the number an
autoscaler should consult before scaling a replica down, and it exists only because one
ledger holds every class.

The cost term alone predicts something non-obvious:

| | size | recompute | **ns/byte** |
|---|---|---|---|
| KV block | 512 KiB | 400 µs | **0.76** |
| FaaS snapshot | 32 MiB | 200 ms | **5.96** |
| Weight shard | 512 MiB | 4 s | **7.45** |
| Service heap | 384 MiB | 15 s | **37.3** |

KV bytes are the *cheapest* in the machine by a wide margin. A siloed stack handing vLLM a
large fixed KV pool is hoarding the least valuable bytes while the warm pool thrashes.
`freq` cuts the other way — hot prefixes are reused hard — so the outcome is contested
rather than foregone.

## Policy

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

Three classes, matching `docs/prototype.md`. `WeightShard` survives as a *dependency* of
inference, not a workload of its own; there is no training class.

- **Inference** — multi-turn agent sessions. Each turn extends the chain, requires its
  model's shards, and costs `tokens × 8 ms` of decode. 35% of turns call a tool.
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

A prefix cache is node-local process state: a replica on one node cannot read another's KV
blocks. Work runs against its own node's ledger, so landing in the wrong place means
recomputing, not fetching remotely.

`Control` models where residency knowledge lives — `Unified` (a field read), `Query` (an RPC
per decision), `Gossip` (a periodically refreshed view). This is the architectural question:
a Kubernetes scheduler extender is `Query`; an informer cache is `Gossip`.

### Scored placement

```
score(node) = resident_recompute_ns          # work this node's residency avoids
            - short_bytes × marginal_price   # what claiming the room makes someone re-pay
            - handoff_ns                     # what not co-placing costs
```

`TierPool::marginal_price` is the recompute cost per byte of the cheapest state the pool would
give up: it peeks each reclaimable class's heap top in the same band order `pick_class`
reclaims in, abstaining on a stale or pinned top, falling back to `inflation`. An estimate on
purpose — a faithful dry run costs as much as the eviction itself, per candidate, per request.

Two rules the score needs to be a decision rather than a suggestion:

- **The score is the whole decision.** Gating it on a separate residency threshold discards
  the scored choice using a metric the score never consulted.
- **Never move without a reason.** With nothing resident anywhere all scores are zero and an
  argmax over ties sends every cold request to one node; the score must strictly beat the
  affinity node's, otherwise content affinity stands.

## Results

### Memory arbitration

20k requests, 8 GiB, bands `0,1,2,1`, oracle-best split per arm. `weights` is a dependency
class, so its stall is charged to the inference requests that need it.

| arm | stall/req | goodput | inference | faas | service | wt hit |
|---|---|---|---|---|---|---|
| `hard-partition` | 44.21 ms | 99.6% | 75.3 | 19.1 | 28.4 | 0.52 |
| `soft-floor` | **43.73 ms** | **100%** | **74.5** | **19.0** | **28.1** | 0.53 |
| `no-floor` | 72.14 ms | 100% | 144.1 | 19.3 | 29.0 | **0.01** |

`soft-floor` dominates the tuned partition on stall, goodput, and every class — but the
margin is 1.1%, and **the headline is `no-floor`**: floors off, the ledger evicts weight
shards to near-zero residency (hit 0.53 → 0.01) and inference stall doubles. Unconstrained
sharing is 65% worse than either bounded arm. Soft floors are not a refinement of hard
partitions here; they are what stops the global price from eating the one class whose
working set is indivisible.

### Flows

15k requests, identical quota and trace. `task e2e` and `stall total` are critical-path only.

| flows | task e2e | stall total | prewarm work | net work | inference |
|---|---|---|---|---|---|
| `blind` | 102.65 ms | 711.98 s | 0.00 s | 711.98 s | 74.22 ms |
| `announce` | **90.64 ms** | 670.95 s | 44.88 s | 715.83 s | **73.03 ms** |
| `gate` | 90.64 ms | 670.95 s | 44.88 s | 715.83 s | 73.03 ms |

**Announce relocates work; it does not eliminate it.** Critical-path stall falls 41.0 s and
44.9 s reappears as background materialisation — net work is 0.5% *worse*. It is a latency
win (−11.7% task e2e) worth having when there is idle capacity to absorb the background work,
and worthless when the machine is saturated. Any claim that prewarming is free is an
accounting error.

The gate fires once per flow task against a break-even budget of milliseconds. At 49 µs a
gRPC crossing is orders of magnitude under it, so **the boundary is affordable here and the
gate is Tier 1.**

### Placement

4 nodes × 2 GiB, 15k requests, gRPC crossing charged per decision.

| arm | socket | rack | zone | region | node spread |
|---|---|---|---|---|---|
| hash only | 40.98 ms | 41.01 | 41.10 | 44.09 | 1.12 |
| residency only | 32.43 | 32.50 | 32.66 | 36.28 | 1.09 |
| flow only | 47.00 | 47.00 | 47.00 | 47.00 | 1.10 |
| both, unified | 38.24 | 38.24 | 38.24 | 38.24 | 1.11 |
| **scored** | **30.76** | **30.84** | **31.01** | **33.97** | **1.00** |

**Scored placement is the best arm at every distance** — 25% over hash, 5.2% over the best
alternative — and the only one that leaves the cluster balanced. Counted rather than assumed:
displacement moves **97.3%** of decisions, the flow term 15.5%, the affinity floor holds
31.7%. Co-placement becomes selective at 48.4% of flow requests, against 0% for hash and 100%
for the unconditional arm.

Three structural findings underneath it:

- **Consistent hashing captures all the locality it created, and none of the locality it did
  not.** Residency routing ties hashing exactly on session KV — a hash puts session S on node
  d, so S's blocks come to live on d. It wins 18% only because shared model weights are 4 GiB
  against 2 GiB per node: no node holds every model, the cluster must specialise, and a hash
  on session identity cannot see that.
- **Unconditional co-placement concentrates pressure without pricing eviction** — 15% worse
  than hash, because agents drag 16–64 MiB tool snapshots onto nodes holding 512 MiB weight
  shards. Pricing the displacement is the whole difference between `flow only` at 47.00 and
  `scored` at 30.76.
- **The control plane's own cost is 0.32% of total stall** with a measured gRPC crossing
  charged to every placement decision, and gossiping a stale view instead costs nothing
  measurable. Being in-process buys almost
  nothing at agent-turn service times. See the crossover table for where it does.

### The falsification test that fails

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

Target regime: `rho` 2–4, `lambda` 0.1–0.3, `P/C` 0.3–0.5. Current: `C` = 8 GiB, `P` = 3.4
GiB, `A` = 4.6 GiB, `lambda` = 0.11, `P/C` = 0.42.

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

**Modelled, not measured:** node link latency and bandwidth in `Distance`, and the workload's
`exec_ns` constants. These are the numbers most worth replacing with real traces.

The hot path — lookup, scoring, eviction — makes **no syscalls**, so it is portable at zero
cost. Platform-specific code lives only in promote/demote, which runs at µs–100 µs scale
where a vtable is free. Portability and performance collide only if the OS is allowed into
the decision loop.

## Not built

No VMM, no WASM ABI, no exec rings, no edge agent, no live migration. The byte store is real
but exercised by `calibrate` only; the residency experiments run on the calibrated model
rather than moving real bytes. `Topology::discover` probes the host but the host is one
memory domain, so every cross-node constant is modelled.

## Standing

| claim | status |
|---|---|
| eviction priced in recompute-cost per byte | holds |
| admission that refuses rather than overcommits | holds |
| soft floors beat both hard partition and open sharing | holds — 1.1% over partition, **65% over open** |
| placement scored as gain minus displacement | holds — 25% over hash, and balances the cluster |
| residency-aware routing beats consistent hashing | holds — 18%, on state the hash did not place |
| the score's handoff term prices co-placement | **fails** — invariant to a 512× payload sweep |
| unconditional flow co-placement | loses 15%, except where the payload dominates |
| unified control plane beats RPC-queried | 0.32% in aggregate; 43–60% of a warm invocation |
| announce / anticipatory prewarm | latency win, net work 0.5% worse |
| downstream-aware gate | Tier 1 — replicable by a hint API |
