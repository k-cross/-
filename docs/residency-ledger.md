# The Residency Ledger

Design notes for Polyphonic's scheduler core. This documents what is built, what is
designed but unbuilt, and which results are currently trustworthy.

## The claim under test

> A single scheduler that owns FaaS, AI inference, and long-running compute beats three
> best-in-class specialists, *on the boundaries between them*.

Not all cross-workload wins count. Two tiers:

- **Tier 1 — information sharing.** "The function will call inference, so prewarm the model."
  Real, but a siloed stack can retrofit it with a hint API. Not a moat.
- **Tier 2 — joint decisions.** Cannot be expressed as a hint across a process boundary
  without becoming a distributed agreement problem. Co-placement fused with routing;
  preemption across classes; **one memory ledger arbitrating every workload's state at once.**

This document is about Tier 2, specifically the ledger.

## The unifying object

The scheduler's world is made of one thing: a content-addressed, immutable chunk of state
with a residency tier and a recompute cost.

```
BlobId = blake3(...)

KvBlock     id = H(model, parent_block_id, token_span)   merkle chain
Snapshot    id = H(image, init_result, env)              merkle chain
WeightShard id = H(model, shard, quant)
ServiceHeap id = H(service, replica)
```

Merkle-chaining the id collapses prefix-cache routing and warm-pool lineage into one index:
longest-prefix match over a token chain and longest-common-ancestor over a snapshot lineage
are the same lookup. K8s schedules on `(cpu, mem)` scalars and cannot see any of this.

### Reconstructible vs. not

Three of the four kinds are **reconstructible**: evicting them costs a recompute you can
price. `ServiceHeap` is not. A replica with live connections cannot be evicted at any price.

This is not a special case bolted on — it is the one structural distinction in the model:

| | eviction means | cost |
|---|---|---|
| `KvBlock` | drop prefix | re-prefill (~400 µs / 512 KiB block) |
| `Snapshot` | drop warm cell | cold init (~200 ms / 32 MiB) |
| `WeightShard` | drop weights | reload (~4 s / 512 MiB) |
| `ServiceHeap` **idle** | **scale down** | cold start (~15 s / 512 MiB) |
| `ServiceHeap` **serving** | *forbidden* | — |

A long-running replica is a blob whose recompute cost is its cold start and whose access is
continuous. Scale-up and scale-down are admission and eviction. That is what puts an
autoscaler and a KV-cache allocator in the same ledger.

## The monotone residency invariant

> A blob is resident only if its parent is resident.

Necessary for KV (you cannot use block *i* without the prefix) and natural for snapshot
lineages. It pays for itself twice:

- **Lookup.** Residency along any chain is a prefix property, so the deepest resident
  ancestor is a `partition_point` — O(log depth) hash probes, no trie, no pointer chasing.
- **Eviction order.** Only leaves are evictable. Evicting a leaf may promote its parent to
  leaf. Correct ordering falls out with no extra bookkeeping.

One invariant, both hot paths. Enforced in `TierPool` via `resident_children`.

## Valuation: recompute cost per byte

Eviction priority is GDSF (Greedy-Dual-Size-Frequency), the known-good algorithm for
variable-cost, variable-size objects:

```
priority = inflation + min(freq, FREQ_CAP) × (recompute_ns / bytes)
```

`inflation` (`L`) is raised to the evicted item's priority on every eviction. It keeps the
structure O(log n) with no periodic rescoring — and it is also **the shadow price of
memory**: the marginal value of the cheapest byte the system is currently willing to give
up. That is the number an autoscaler should consult before scaling a replica down, and it
exists only because one ledger holds every class.

The cost term alone predicts something non-obvious:

| | size | recompute | **ns/byte** |
|---|---|---|---|
| KV block | 512 KiB | 400 µs | **0.76** |
| FaaS snapshot | 32 MiB | 200 ms | **5.96** |
| Weight shard | 512 MiB | 4 s | **7.45** |
| Service heap | 512 MiB | 15 s | **27.9** |

KV bytes are the *cheapest* by a wide margin. A siloed stack that hands vLLM a large fixed
KV pool is hoarding the least valuable bytes in the machine while the warm pool thrashes.
`freq` cuts the other way — hot prefixes are reused hard — so the outcome is genuinely
contested rather than foregone.

## Soft boundaries

Hard partitions strand memory; a free-for-all starves the class with the lowest ns/byte
(measured: a scalar objective made inference **89% worse** while winning on aggregate).
Neither is acceptable. The model is a floor, not a partition — `memory.low` semantics
generalized across workload classes.

```rust
pub struct Quota {
    pub floor: [u64; BlobKind::N],
    pub hard: bool,
}
```

Invariants:

- `sum(floor) <= capacity`; the remainder is unowned **slack**.
- A class at or below its floor is **never a victim**. That is the protection.
- Above its floor, a class's bytes compete in one global GDSF price. `pick_class` selects
  the globally cheapest reclaimable byte among classes strictly above floor.
- **Unused floor does not strand.** Accounting of `used` is global, so a class sitting below
  its floor leaves those bytes available to everyone. A floor buys protection, not reservation.
- `hard: true` additionally caps a class at its floor. That is the partition baseline, and it
  is the *same code path* — one bit — which makes the comparison exact.

### Elastic capacity

`capacity` is treated as a soft ceiling on an elastic pool with some larger hard maximum.
Growing the pool is deliberately **not modeled**. What matters here is that the ledger keeps
other workloads' soft-boundary limits in view when deciding, and that the refusal path below
is exactly the signal a real system would use to acquire more capacity.

## Scheduling policy: floor, band, limit

The control plane does not know which of an operator's workloads matters most, so it must not
decide. Priority is **configuration**, not a property of the workload kind — this is intended
as a highly configurable k8s replacement that the operator tunes to their workflow, and the
three knobs map onto concepts they already have:

| Polyphonic | k8s analogue | role |
|---|---|---|
| `floor[k]` | `requests` | starvation guarantee — never preempted, by anyone |
| `band[k]` | `PriorityClass` | who gives up **burstable** bytes first |
| `limit[k]` | `limits` | ceiling on preemptive growth |

`limit[k] = floor[k] + slack`, where `slack = C - Σ floor[j]` — derived from the floors
rather than separately tuned. Bands are set with `--bands inference,faas,training,service`,
defaulting to `0,1,2,1`.

### Reclaim

Candidates are classes **strictly above their floor**, taken most-sacrificial band first,
cheapest within a band. A class at or over its limit may recycle its own bytes and take free
space, but may not preempt anyone.

Two properties follow, and both took a wrong turn to find:

- **Floors are inviolable, including against a higher band.** Protecting a critical class
  only up to its floor is what prevents starvation. An earlier version let band 0 preempt
  band 2 *below* its floor and drove training to 6% goodput — priority became
  indistinguishable from starvation.
- **Nothing above a floor is owned.** An earlier version also forbade reclaiming from a more
  critical band at all, which protected inference's *burstable* bytes as well as its
  guarantee. Inference parked at 5.2 GiB against a 1 GiB floor and drove faas to 32%: a class
  permanently owning slack it merely reached first. The guarantee is the floor; band orders
  everything above it.

### Where architectural tradeoffs land

Runtime priority is the operator's call, but *design* tradeoffs in the control plane are
ours, and they resolve toward inference. The merkle-chained `BlobId`, the monotone residency
invariant, and `partition_point` prefix lookup all exist to make KV-chain residency O(log
depth) on the hot path; weight shards and snapshots are singleton blobs that get no such
treatment and do not need it. Where a data layout, index structure, or hot-path decision can
only favour one class, it favours inference, and training absorbs the cost.

### Sweep objective

With a priority order, total stall is the wrong target — it lets a config win by trading
inference away. `prefer()` compares overall goodput first (2pp tolerance, so latency can
never be bought by dropping requests), then band-lexicographically on mean stall (2%
tolerance). Splits are swept leaving explicit slack.

## Admission refusal

`admit` returns `Admitted | Pending`. It never overcommits.

`Pending` when either the blob exceeds the class ceiling, or no reclaimable bytes exist —
every candidate is at/below floor, pinned-serving, or a non-leaf. A chain aborts at the
first `Pending`, since the monotone invariant forbids a hole in the middle.

`Pending` is accounted as **goodput loss, never as stall**, so no arm can win the latency
metric by refusing work. Both numbers are always reported together.

This is a scheduling primitive, not an error path — it is `Pending` in the orchestrator
sense, and it is where elastic capacity acquisition would hook in.

## Flows: dependency across phases

Two workloads registered independently — a function and an inference service — are often one
task: `Invoke -> Prefill -> Decode`. Neither workload can see that; the orchestrator can.
45% of function invocations in the trace call into inference after a short lead, against a
per-function system prompt shared across that function's invocations.

```rust
pub struct FlowHint {
    task: u64,
    downstream: Vec<(BlobId, BlobMeta)>,  // the working set the next stage will need
    probability: f64,                     // P(to fires | from fired)
    lead_ops: u32,                        // observed delay
}
```

### Anticipatory value

`Hierarchy::announce` does two things when an upstream stage is admitted. Downstream blobs
already resident get `anticipate(probability)`, which adds to the GDSF numerator alongside
`freq`:

```
priority = inflation + (min(freq, FREQ_CAP) + expect) * value_per_byte
```

Prewarming is therefore a change of **value**, not a subsystem: an anticipated access
competes against a real one in the same currency, and `expect` clears on access. Downstream
blobs *not* resident are admitted if they fit in free space — **prewarming never preempts**,
because speculative work must not evict state someone is actually using.

### Downstream-aware admission

`can_satisfy` asks whether the task's remaining downstream working set could be made
resident before admitting the upstream stage. Admitting a function whose inference stage
cannot land burns a warm cell on work that will stall — the queue amplification that siloed
stacks suffer.

The estimate must be **conservative about what is actually reclaimable**. A first version
counted burstable service bytes that were in fact pinned-while-serving, so every downstream
looked satisfiable and the gate never fired.

### Results

8 GiB, 15k requests, identical quota and trace in every row. `task e2e` and `stall total` are
critical-path only; `prewarm work` is materialisation moved off it.

| flows | task e2e | stall total | prewarm work | net work | inference |
|---|---|---|---|---|---|
| `blind` | 39.45 ms | 377.77 s | 0.00 s | 377.77 s | 4.16 ms |
| `announce` | **34.35 ms** | 366.67 s | 8.74 s | 375.41 s | **2.87 ms** |
| `gate` | 34.35 ms | 366.67 s | 8.74 s | 375.41 s | 2.87 ms |

**Announce relocates work; it does not eliminate it.** Critical-path stall falls 11.1 s, but
8.74 s of that reappears as background materialisation — **79% of the apparent saving is
moved, not saved.** Net work drops 0.6%.

So announce is a *latency* win (task e2e -12.9%, inference stall -31%) and very nearly a
*work* no-op. It is worth doing when there is idle capacity to absorb the background
materialisation, and close to worthless when the machine is saturated. Any claim that
prewarming is free is an accounting error — an earlier revision of this document made
exactly that error, because `announce` admitted blobs into DRAM without charging the
`fetch_ns` or `recompute_ns` that `access` charges for the identical work.

Under scarcity the gate matters, but its accounting has to be honest too: a gated task must
*cancel its downstream stage*, and must count as an attempted task. Otherwise "gate
eliminates broken tasks" is definitional — the gated task simply leaves the denominator.
`task_completion` counts gated tasks as failures so a gate cannot win by refusing everything.

### Which tier is this? — measured

An earlier revision claimed the gate was Tier 2 because "across a gRPC boundary that query
costs more than the stall it avoids." **That was wrong, and the measurement refutes it.**

`cargo run --release --features grpc --bin rpcbench` measures a real tonic unary RPC against
the direct call on a populated ledger (6300 resident blobs, 7.5 GiB), with the real query
payload (28 downstream blobs, ~1.1 KB):

| admission query | p50 | p99 | p999 |
|---|---|---|---|
| in-process (direct call) | **0.96 µs** | 1.42 µs | 2.13 µs |
| gRPC unary (TCP loopback) | **55.6 µs** | 96.2 µs | 155.8 µs |
| raw TCP echo, same payload | 20.0 µs | 33.2 µs | 39.0 µs |

~58x, and ~64% of gRPC's cost is HTTP/2 framing and protobuf above the raw socket. Loopback
is the *best* case; a network-attached extender adds another 0.5-1 ms.

But the gate fires once per flow task, against a break-even budget of ~5775 µs (assumed:
7.7% broken x ~75 ms wasted upstream). At 55.6 µs, gRPC is ~100x under it. **The boundary is
entirely affordable for admission, and the gate is Tier 1.**

### Where the boundary actually bites

Not per-request latency — control-plane throughput. `Hierarchy::access` measured in-process
is **0.6-1.2 µs p50** and carries ~4.0 evictions per request:

```
single decision thread
  in-process   830k-1.6M req/s   (measured)
  over gRPC          ~3.6k req/s (extrapolated: 5.0 decisions x 55.6 us)
  ratio              230x-450x
```

The in-process figure is timer-resolution-limited — `Instant::now()` overhead is a
meaningful fraction of a sub-microsecond operation — so treat the ratio as two orders of
magnitude, not a precise number. The gRPC column is an **extrapolation** of what it would
cost if each in-process decision instead crossed a boundary; only `access` and the RPC are
directly measured.

Adding ~220 µs to a request whose mean stall is ~24 ms is under 1% of latency — invisible.
What it costs is a scheduler fleet two orders of magnitude larger for the same request rate.

This sharpens the README's "zero cost extension" goal and narrows it. The claim is false for
coarse decisions — admission, placement, scale-up — where a sidecar or extender is fine and
k8s is not obviously wrong. It holds for the **inner loop**: eviction, block placement, and
routing within a batch, where a decision costs under a microsecond and the boundary costs
fifty. Put the extension boundary where decisions are coarse; never inside the ledger's hot
path. That is what the ABI and WASM interfaces are for.

## Topology and placement

`topo.rs` models compute units, memory domains, and the links between them. The consequential
field on a link is **coherence**, not bandwidth: a coherent link is traversed by reference, a
non-coherent one requires a copy, and that decides whether co-placement saves a dereference or
a whole materialisation.

`Topology::discover()` probes the host — sysctl perflevels on darwin, sysfs NUMA nodes and the
kernel distance matrix on Linux. On this machine that is 8 performance + 4 efficiency units
over **one** memory domain.

### The host cannot measure what the model is for

Two attempts, both reported rather than buried. Streaming bandwidth is memory-bound and
identical across clusters. The right probe is a dependent-load chase sized against the two L2s
(16 MiB perf vs 4 MiB efficiency) — but macOS QoS is advisory and runs background threads on
performance cores when idle, so both probes land on the same cluster and the ratio column is
noise. The separation test requires **direction-consistency across working sets** rather than
tripping on a single outlier, and prints "did not separate".

What is real is the one-domain latency curve — ~6 ns in L2, ~20 ns at 16 MiB, ~120 ns at
256 MiB — which is what a per-domain link cost should be derived from. Everything cross-domain
below runs on `Topology::synthetic`, whose constants are **modelled**: coherent, ~2x per-byte,
120 ns hop. Re-derive them on multi-socket or multi-GPU hardware before trusting any number
that depends on them.

### Placement: a negative result

`machine.rs` gives each memory domain its own ledger. A chain is served from the domain that
already holds its deepest prefix, and the compute unit pays the interconnect cost to reach it
— so placement decides whether that link is local or a hop, with no cross-domain chains and no
double-charging.

Synthetic 4 domains x 2 GiB, 3 units each, 15k requests:

| placement | stall/req | materialise | interconnect | cold | spread |
|---|---|---|---|---|---|
| `blind` (round-robin) | 189.47 ms | 2610.7 s | 129.6 s | 39.3% | 1.13 |
| `sticky` (hash chain root) | **30.56 ms** | 357.0 s | 86.2 s | 38.6% | 1.17 |
| `aware` (residency lookup) | **30.56 ms** | 357.0 s | 86.2 s | 38.6% | 1.17 |

**Residency-aware placement adds exactly nothing over consistent hashing.** Identical on every
metric.

The first version of this experiment showed `aware` beating `blind` 3.3x, and that was a
strawman: round-robin re-scatters continuations of the same session, which no real load
balancer does. Against a consistent hash on the chain root the win vanishes entirely — because
in this workload the chain root **is** the tenant identity, so the hash already predicts
residency perfectly and the ledger lookup is redundant.

Note also that the `blind` gap was never mostly interconnect (129.6 s of 2740 s). Scattering
placement fragments a shared tenant prefix across domains, storing it up to 4x and evicting
far more. The cost of state-blind placement is duplicated state, not extra hops.

### When residency should beat hashing — three tests, three answers

A content hash predicts where state *belongs*. It is wrong exactly when state is somewhere
else. Three cases were named and all three were built. Columns below: mean stall per served
request, materialisation, cross-domain flow handoff, and the share of cross-workload tasks
whose two stages landed in different domains.

**1. Stale hash after a node drain.** Domain 0 is drained halfway through; its state migrates
to the survivors, so the bytes survive but placements pointing at it do not.

| placement | stall/req | materialise | tasks split |
|---|---|---|---|
| `sticky` (rendezvous hash) | **34.22 ms** | 394.9 s | 70.0% |
| `aware` (residency) | 34.73 ms | 422.9 s | 0.0% |

**Aware loses.** An intermediate version showed aware winning by 4%, but only because the
baseline hashed with `root % n`, which remaps nearly every key on resize. Against *rendezvous*
hashing — which remaps only the keys that lived on the drained domain, as a real balancer does
— the win inverts. The reason is worth keeping: migrated state lands scattered across the
survivors, so "follow the bytes" locks in a fragmented layout while the hash **re-normalises**
it. A residency-aware placer needs a compaction notion, not just pursuit.

**2. Sharing across identity boundaries.** Inference depends on its model's weight shards;
24 tenants map to 4 models, so the shards are shared across identities no caller hash can
co-locate.

| placement | stall/req | materialise | cold |
|---|---|---|---|
| `sticky` | **75.14 ms** | 844.4 s | 47.9% |
| `aware` | 147.00 ms | 1979.6 s | 41.7% |

**Aware loses badly — 2x.** Scoring a domain by resident bytes makes a 512 MiB shared shard a
*gravity well*: every tenant on that model is pulled onto one domain, KV locality is destroyed
and capacity blows out. The correct handling of shared, read-only, hot state is **replication**,
not co-location — and scattering by hash achieves that implicitly, giving every domain its own
copy and every access a local one. Duplication is the right answer here, which is the opposite
of the lesson from the `blind` arm.

**3. Cross-workload co-placement.** A task's FaaS snapshot (`fn:f`) and its inference prefix
(`fnprompt:f`) hash to different domains. The flow carries a 4 MiB intermediate payload, which
crosses the interconnect whenever the two stages are split.

| placement | stall/req | materialise | handoff | tasks split |
|---|---|---|---|---|
| `sticky` | 30.34 ms | 346.9 s | 0.35 s | 73.2% |
| `aware` (flow-aware) | **30.31 ms** | 346.8 s | **0.00 s** | **0.0%** |

**The mechanism works perfectly and is worth almost nothing.** Flow-aware placement drives
split tasks from 73.2% to zero and eliminates the handoff entirely — no hash on either
workload's own identity can do this, so it is a genuine Tier-2 joint decision. But the total
gain is 0.03 ms/req, **0.1%**, because a 4 MiB handoff over a coherent link is ~280 µs against
~30 ms of materialisation per request.

From the measured numbers, co-placement would need the handoff to grow ~50x — multi-hundred-MB
tensors — or the link to be ~50x slower — cross-AZ or cross-region rather than cross-socket —
before it pays for itself. Both are real regimes; neither is this one.

### What this says about placement

Three named cases, built and measured: one loss, one heavy loss, one win worth 0.1%.
**Placement is not where a unified ledger pays.** Consistent hashing on content identity is a
strong baseline that captures nearly all available locality, replicates shared state for free
by scattering, and survives a drain better than residency-following does.

The ledger's demonstrated wins remain where they started: eviction priced in recompute-cost per
byte, and admission that refuses rather than overcommits. That is a narrower claim than the
README makes, and it is the one the measurements support.

## Tier model, measured not guessed

`polyphonic calibrate` measures real costs on the host: page-aligned allocation and
first-touch for DRAM; direct I/O for the spill tier (`F_NOCACHE` on darwin, `O_DIRECT` on
Linux) so the page cache cannot absorb the measurement.

Measured on darwin/arm64, against the constants originally guessed:

| | guessed | measured |
|---|---|---|
| NVMe fixed latency | 90 µs | **113 µs** |
| NVMe bandwidth | 3.0 GB/s | **7.8 GB/s** |
| DRAM first-touch | 20 GB/s | **28 GB/s** |

The guess was 2.6× too pessimistic on NVMe bandwidth; correcting it moved a headline result
by 42%. Re-derive per host — the constants in `tier.rs` are a darwin/arm64 fit, not a law.

### Portability

The hot path — lookup, scoring, eviction — makes **no syscalls**, so it is portable at zero
cost. Platform-specific code (direct I/O, and later NUMA binding, hugepages, `mincore`,
io_uring) lives only in the promote/demote path, which already runs at µs–100 µs scale where
a vtable is free. Portability and performance only collide if the OS is allowed into the
decision loop.

## Experimental method

Three arms over identical seeded traces, all sharing one code path:

| arm | quota |
|---|---|
| `hard-partition` | per-class floors that are also caps — today's siloed reality |
| `soft-floor` | same floors, shared slack, one global price above floor |
| `no-floor` | open quota — unification with no QoS constraint |

Both partitioned arms are swept over all splits and reported **at their oracle-best**, so the
baseline is stronger than any operator could tune blind.

Reported every run: mean stall per *served* request, p99, goodput (overall and per class),
per-class mean stall, stall by workload phase, and **admission integrity** — `over_capacity`,
per-class `refused`, `pinned_skips`. A nonzero `over_capacity` invalidates the run.

That last line exists because an earlier silent overcommit — 19 GiB resident in an 8 GiB
tier — invalidated a result that otherwise looked like a clean win.

## Results

### Four classes under operator priority

15k requests, 8 GiB, bands `0,1,2,1`, oracle-best split per arm:

| arm | stall/req | goodput | inference | faas | training | service |
|---|---|---|---|---|---|---|
| `hard-partition` | 25.45 ms | 98.8% | 3.9 / 100% | 27.6 / 100% | 63.6 / 100% | 38.9 / 95% |
| `soft-floor` | **24.47 ms** | **100%** | **3.4 / 100%** | 27.4 / 100% | **57.4 / 100%** | **37.8 / 100%** |
| `no-floor` | 26.58 ms | 100% | 2.6 / 100% | 27.5 / 100% | 83.3 / 100% | 36.7 / 100% |

`soft-floor` strictly dominates the tuned partition — better on stall, goodput, and every
class. **No class is starved in any arm.**

`no-floor` is the same policy with floors at zero: bands alone push the cost onto training
(83.3 ms vs 57.4) and buy inference 2.6 ms vs 3.4. That is not a worse configuration, it is
a different operator choice — floors are the knob for how much training progress to protect,
bands are the knob for who wins the slack.

Bands are genuinely policy, not decoration. Same binary, same trace, `no-floor` arm:

| `--bands` | inference | training |
|---|---|---|
| `0,1,2,1` (inference first) | **2.6 ms** | 83.3 ms |
| `1,1,0,1` (training first) | 7.7 ms | **20.9 ms** |

A limit that binds is real backpressure: at a coarse sweep (`--step 0.2`) floors of 0.20 each
leave only 1.6 GiB slack, inference's working set exceeds `floor + slack`, and it refuses
39% of requests rather than preempt past its entitlement. The operator's remedy is to raise
inference's floor — which is exactly the contract `limits` implies in k8s.

### Earlier, three reconstructible classes only

- Unified vs. oracle-best static partition, same algorithm: **+8.5%** total stall. Refining
  the sweep from 36 to 171 partitions moved it 0.3pp, so it is not sweep granularity.
  Measured with the *guessed* tier constants; the measured ones would shrink it, plausibly
  to ~5%.
- **The advantage scales with volatility and vanishes without it**: monotone from 0.0 to 5.0%
  across volatility 0→1, and **−0.6% at zero volatility**, where a static partition is
  optimal by construction. It behaves like a mechanism, not a fit.

### Void, and retracted

- Every number involving `ServiceHeap` before admission refusal existed. The siloed arms
  overcommitted (22 admissions, 19 GiB in an 8 GiB tier) and collected a free 1.00 service
  hit rate. This includes both the "+21.1%" and the "−20.4%" figures.
- An intermediate soft-floor result showing 20% goodput and mass refusal was a bug, not a
  finding: the reclaim scan treated *non-leaf* chain blocks as pinned and gave up on them,
  so the inference class appeared unreclaimable. Non-leaves are now dropped from the heap
  (`unlink_parent` re-heaps them when they become leaves); only serving replicas are parked.

## Choosing a configuration

A configuration is degenerate more often than it looks, and in more than one way. Three
quantities determine the regime:

```
P = peak pinned serving bytes        (unreclaimable: the floor the workload imposes)
A = C - P                            (the arena: what the ledger actually arbitrates)
W = reclaimable working set          (unique bytes touched over a window)

rho    = W / A           eviction pressure   -- can policy matter at all?
lambda = max_blob / A    admission lumpiness -- does refusal ever bind?
```

Degenerate zones, all four observed:

| zone | symptom | what it actually measures |
|---|---|---|
| `P >= C` | every arm mass-refuses | nothing; the pinned set does not fit |
| `rho << 1` | all arms identical, 100% hit | nothing; everything fits |
| `rho >> 1` **at low skew** | all arms identical, low hit | the workload's unservability, not policy |
| `lambda << 1` | zero refusals anywhere | eviction only; admission untested |

The third is the treacherous one — it looks like scarcity. With 40 weight shards at zipf
skew 1.1 (near-uniform) over a 4.6 GiB arena, training was **47% of all stall and within 5%
across every arm**. The aggregate metric was reporting a class no policy could move, while
the class that *did* differ by 90% (inference) contributed 4%. Raising skew to 2.0 and
cutting to 8 shards — a node hosts a handful of models with heavily skewed popularity, not
40 uniform ones — dropped training to 10-20% of stall and the arms separated immediately.

**Standing diagnostic.** Before trusting any aggregate: if one class exceeds ~40% of total
stall *and* its per-class numbers are within noise across arms, the experiment is measuring
that class's unservability. `share of total stall by class` prints on every run for exactly
this reason.

Target regime: `rho` 2-4, `lambda` 0.1-0.3, `P/C` 0.3-0.5.

### Current configuration

`C` = 8 GiB, `P` = 3 services x 3 replicas x 384 MiB = 3.4 GiB, so `A` = 4.6 GiB,
`lambda` = 512 MiB / 4.6 GiB = 0.11, `P/C` = 0.42. Weights: 8 shards, skew 2.0.

### Avoiding a tuned result

Workload parameters are chosen for **realism**, defensible without reference to which arm
wins — model count and popularity skew, replica counts, cold-start times, blob sizes.
Capacity should be chosen by a criterion **pre-registered and independent of the outcome**,
e.g. *"the smallest C such that at least one arm refuses >=5% of requests and every arm
achieves >=50% goodput"* — then report the whole sweep, not the chosen point. Picking the
capacity that maximises a favoured arm's margin is the easiest way to manufacture a result
here, and the sweep is cheap enough that there is no excuse for reporting a single cell.

### Known limits of the current experiment

Goodput is 100% for the soft arms at both capacities tested, which means the configuration
is not scarce enough to stress admission hard. The refusal path is exercised (the hard
partition drops 134 service requests at 5 GiB) but not stressed. Sizing the pinned serving
set against capacity is load-bearing: at 6 services × 4 replicas × 512 MiB the pinned floor
*exceeds* an 8 GiB tier and every arm degenerates to mass refusal — correctly, but
uninformatively.

## Not built

No VMM, no WASM ABI, no exec rings, no edge agent, no live migration, no multi-region. No
topology or interconnect graph — placement cost is a two-tier table, not a fabric. The byte store is real but is exercised by `calibrate`
only; the residency experiments run on the calibrated model rather than moving real bytes.

## Re-aim: what `docs/prototype.md` says we should have been measuring

Three things, and the prototype to this point addressed none of them squarely.

**The cost model had no boundary term at all.** Every number above this section comes from
`materialise_bytes x ns_per_byte + recompute_ns`. No syscall, no copy, no serialisation,
anywhere. So every arm silently assumed boundary cost is *zero* -- the most favourable
possible assumption for Kubernetes, and precisely the tax the README claims to remove.
`rpcbench` measured a gRPC round trip but its number never entered the ledger.

**The topology was one machine.** `Topology::synthetic` models sockets: coherent links at
120 ns and 14 GB/s. That is NUMA, not a cluster. The placement conclusion was self-refuting
-- it said co-placement "would need a ~50x slower link to pay," and a cross-AZ link *is*
~50x slower. Placement was ruled out on the one topology that excludes the target regime.

**The workload carries four classes, and the wrong fourth.** Training is not in the three,
and inference is modelled as single-shot chains rather than agent loops.

### The boundary ladder, measured

`polyphonic boundary --repeat 7`, best-of-7 per rung, timer overhead subtracted from the
per-operation rungs. Apple M-series, unloaded.

| boundary | 64 B | 1024 B | 8192 B | fixed ns | ns/byte | p99 | spread |
|---|---|---|---|---|---|---|---|
| native call | 0 | 0 | 0 | 0 | 0.000 | - | 1.0x |
| shared ring (spin) | 66 | 66 | 192 | 58 | 0.016 | 234 | 2.0x |
| syscall floor | 97 | 97 | 97 | 97 | 0.000 | - | 2.4x |
| pipe (same thread) | 434 | 450 | 613 | 430 | 0.022 | - | 1.6x |
| unix socket RTT | 7149 | 6984 | 7130 | 7068 | 0.006 | 9318 | 1.1x |
| TCP loopback RTT | 15317 | 15318 | 15671 | 15294 | 0.046 | 18942 | 1.0x |
| gRPC unary RTT | 49233 | 48276 | 51150 | 48631 | 0.298 | 83942 | 1.1x |

The `spread` column is worst-repetition over best. It runs to 2.4x on the cheap rungs
because this host migrates threads between performance and efficiency clusters and
`pin_cluster` is only a QoS hint on macOS. **The ordering is the robust result; no single
constant here should be quoted to two digits.**

What each step adds, at 1 KiB:

| step | adds | multiplier |
|---|---|---|
| cross-core cache line + spin detect | 0.07 us | 66x |
| ring transition | 0.03 us | 1.5x |
| kernel buffer copy + second syscall | 0.35 us | 4.6x |
| **waking a blocked thread** | **6.53 us** | **15.5x** |
| loopback network stack | 8.33 us | 2.2x |
| HTTP/2 framing + protobuf | 32.96 us | 3.2x |

Four findings worth designing against:

1. **A single no-op syscall costs about what a whole shared-memory round trip costs**
   (97 ns vs 66 ns). Any design that spends one syscall per decision has already given up
   more than the entire budget of the shared-memory alternative. "Fewer syscalls" is not
   the lever; *zero* is.
2. **The largest single multiplier in the ladder is waking a thread**, not crossing the
   kernel and not touching the network: 450 ns -> 6984 ns, 15.5x. The tax to remove is the
   scheduler. That argues for spin-polled rings or busy-poll completion queues, and against
   any design whose hot path blocks.
3. **Two thirds of a gRPC round trip is framing and encoding** -- 33 us of the 48 us sits
   above raw TCP. This is the self-inflicted part, and it is the majority.
4. **Marshalling slope is nearly flat except for gRPC** (0.006-0.046 ns/byte for the
   kernel paths, 0.298 for gRPC -- 6-50x). At control-plane message sizes the fixed cost
   dominates everything, so *batching decisions matters more than shrinking them*.

The composite: a 48 us gRPC round trip against 0.07 us of shared memory, for the same bytes
and the same question. That ratio, ~700x, is the size of the prize the README is pointing
at -- and it is measured, not modelled. What it costs is a burned core per ring, which is a
real charge the design has to carry rather than wish away.

### What this does not yet show

Nothing above is wired into the arms. The ladder is a cost table; until `Cost` is charged
on every cross-node decision in a distributed topology, the residency results still assume
a free boundary and remain quoted under that assumption.

## Distributed: the control plane's own cost, charged

`polyphonic distributed` runs the same workload over a cluster of separate hosts rather than
sockets on one box, at four distances, and charges each placement decision the **measured**
boundary cost from the ladder above. Node link latency and bandwidth remain modelled;
`Distance::{Socket, Rack, Zone, Region}` carries the constants and says so.

Two bugs had to be fixed before any of it meant anything, and both had been inflating the
earlier placement conclusions:

- `choose_unit` returned the sticky unit for `Placement::Sticky` **regardless of the target
  the caller had just computed**, so flow co-placement was calculated and then discarded for
  every arm except `Aware`. Target selection is now the single decision point.
- `cold`/`local`/`remote` were counted before admission, so a refused request was charged a
  placement outcome. One configuration reported a 104.7% cold rate, which is what finally
  gave it away.

### The win is flow co-placement, and it is the only thing that moves

Mean inference stall, by node distance:

| arm | socket | rack | zone | region |
|---|---|---|---|---|
| hash only | 5.476 ms | 5.670 ms | 6.100 ms | 11.593 ms |
| residency only | 5.476 ms | 5.670 ms | 6.100 ms | 11.593 ms |
| flow only | 5.197 ms | 5.197 ms | 5.197 ms | 5.197 ms |
| both, unified | 5.197 ms | 5.197 ms | 5.197 ms | 5.197 ms |
| **flow win** | **5.1%** | **9.1%** | **17.4%** | **123%** |

Three things follow.

**Residency-aware routing contributes exactly nothing** -- `residency only` is bit-identical
to `hash only` at every distance. This is not a bug; it is the mechanism. **Consistent
hashing creates the residency it is later compared against.** A hash puts session S on node
d, so S's KV blocks come to live on d, so residency routing also picks d. The two policies
agree by construction. This retires the question properly: the earlier "aware loses to
sticky" results and this "aware ties sticky" result are the same fact seen twice.

**Flow co-placement is the entire win, and it scales with distance exactly as predicted.**
At socket distance it is worth 5.1%, consistent with the 0.1% measured earlier on the
end-to-end metric. At region distance it is worth 123%. The earlier conclusion -- that
co-placement "would need a ~50x slower link to pay" -- was right about the condition and
wrong to stop there, because a cross-region link *is* that link. **Placement was ruled out
on the one topology that excluded the regime where it pays.**

**Flow co-placement needs no residency knowledge at all.** The scheduler knows where the
upstream stage ran because it placed it. So the mechanism that pays is the one that needs
nothing from remote nodes, and the mechanism that needs remote state is the one worth zero.

### The boundary tax is a fraction, and the denominator is the work

Charging every placement decision a measured gRPC crossing costs **0.17% of total stall**,
0.9% of inference stall, at 4.2 control RPCs per request. Gossiping a stale view instead
produces 1.9% stale decisions and costs nothing measurable. On this workload the control
plane's own cost is irrelevant and being in-process buys almost nothing.

That is entirely an artifact of the denominator. The same measured crossing, against
different service times:

| work being scheduled | over gRPC | over a shared ring |
|---|---|---|
| warm FaaS invocation (10 us) | **95.1%** | 2.95% |
| FaaS snapshot restore (1 ms) | 16.3% | 0.03% |
| agent turn, cached prefix (5 ms) | 3.7% | 0.01% |
| inference request (30 ms) | 0.6% | 0.00% |
| cold start (1 s) | 0.0% | 0.00% |

Four decisions per request, 1 KiB each, measured crossings. For a warm FaaS invocation
**95% of the request is the control plane talking to itself.** For an inference request it
is 0.6%.

So the zero-cost-extension thesis is not wrong, it is *conditional*, and the condition is
sharp: it pays where the scheduled work is comparable to a boundary crossing. That is warm
FaaS and it is nothing else in this workload. Which is also the honest criticism of the
current experiment -- **it has no warm FaaS path**. Mean stall is 30 ms because every class
is modelled as a materialisation. The regime the ladder says matters most is the one the
workload does not contain.

### Standing

| claim | status |
|---|---|
| eviction priced in recompute-cost per byte | holds |
| admission that refuses rather than overcommits | holds |
| flow co-placement across workloads | **holds, and scales with distance: 5.1% socket to 123% region** |
| residency-aware routing beats consistent hashing | **retired -- hashing creates the residency** |
| unified control plane beats RPC-queried | unsupported at 30 ms service times (0.9%); untested where it should matter |
| announce / anticipatory prewarm | 79% relocated, not saved |
| downstream-aware gate | Tier 1, replicable by a hint API |
