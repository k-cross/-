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

## Flows: dependency across phases *(designed, not built)*

Two workloads registered independently — a FaaS function and an inference service — are
often one task: `Invoke → Prefill → Decode`. Nothing in the model above can see that, which
is precisely the boundary the whole thesis is about.

```rust
pub struct Stage { workload: WorkloadId, phase: Phase }

pub enum Phase {
    Init, Invoke, Drain,      // FaaS
    Prefill, Decode,          // inference
    Fetch, Step, Checkpoint,  // training
    Start, Serve,             // long-running
}

pub struct Flow {
    from: Stage,
    to: Stage,
    probability: f64,   // P(to fires | from fired)
    lead_ns: u64,       // observed delay
    payload_bytes: u64, // handoff size: decides pointer vs. copy
}
```

A task is the transitive closure of flows from an entry stage. Flows may be **declared**,
but the default should be **learned**: the content-addressed index can attribute each access
to the stage that caused it, so edges are inferable from observed `(stage, blob)` traces
without user annotation.

Three things a flow buys, all of them residency decisions in the existing currency:

1. **Anticipatory value — prewarm without a prewarm subsystem.** When `from` is admitted,
   every blob `to` will need has a known expected access at `now + lead_ns`. Add
   `probability × value_per_byte` to its priority for that window. Evicting a block that is
   about to be needed becomes expensive *in the same units as everything else*. No hint
   protocol, no separate warming service.

2. **Temporal floors.** Two stages in one flow can share a floor that *moves along the task*
   rather than holding two independent ones. A function that always calls inference does not
   need its snapshot resident while the inference stage runs. Soft boundaries become
   temporal, not merely per-class.

3. **Admission with downstream knowledge.** Refusal currently inspects only the requested
   blob. With flows it can ask whether the *whole task's* working set can be made resident
   before admitting stage one — refusing early instead of admitting a function that strands a
   warm cell blocking on an inference stage that will never be admitted. This is the
   queue-amplification collapse of siloed stacks, and it is a small extension of the refusal
   path.

The single hook is `TierPool::score`. A flow-aware valuation changes that one function.

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
topology or interconnect graph — placement cost is a two-tier table, not a fabric. Flows are
designed above but unimplemented. The byte store is real but is exercised by `calibrate`
only; the residency experiments run on the calibrated model rather than moving real bytes.
