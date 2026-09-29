# Phase 4 -- Belief, not truth: lossy telemetry and risk-adjusted scoring

Implementation plan for Phase 4 of [`owned-and-observed.md`](owned-and-observed.md): the router's
view of the engine-allocated KV cache stops being an in-process read and becomes a belief, fed by
the engine's own step-aligned event stream with its cadence, its lag and its losses; the acquire
term becomes an expectation over `P(resident)` (§3.7), scored at the quantile a request's declared
SLO names; `Control::Gossip`'s stale exact view of engine state is retired; divergence is reported
(§3.6); `P(resident)` is published against realised residency as a calibration curve; and the score
stops reading the exact output length -- §3.1's `RequestView`, which `phase-1.md` §7 and
`phase-2.md` §7 both assigned here.

**Status: implemented and measured.** The stream is `KvEvent` in [`stream.rs`](../src/stream.rs)
(recorded inside `KvTiers`); the publisher, subscriber, belief, channel conditions and estimator are
[`belief.rs`](../src/belief.rs); the boundary reads are `Telemetry::with_belief` in
[`tele.rs`](../src/tele.rs); the rules, `RequestView` and the truth/belief view separation are
[`machine.rs`](../src/machine.rs); the declared SLO is [`work.rs`](../src/work.rs); the instruments are
[`instruments.rs`](../src/instruments.rs); `polyphonic belief` ([`belief_cmd.rs`](../src/belief_cmd.rs))
runs §4.10's sweeps with no control crossing charged, so every number below is reproducible from the
seed (seed 1, 15,000 ops, the `distributed` cluster). §2's nine predictions are annotated with what was
measured, and §9 records what the build found that the plan did not anticipate. Two things did not get
built: the per-cause split of divergence (§4.9 -- phantom and miss shares are measured, their causes are
not) and two-tier admission (§4.11, the item the plan said could be cut).

Before the results, the plan's own text. §2's predictions are stated before the run, per
`owned-and-observed.md` §7. Unlike earlier plans, several of them lean on **pre-measurements**:
numbers taken on an instrumented copy of the Phase 3 commit (`5247d17`), run outside the repository
and not committed. They are labelled wherever quoted and collected with their configurations in
§8. They are reasons to predict, not results: §4.9 rebuilds each instrument in the repository, and
a pre-measurement the built instrument does not reproduce is reconciled before any prediction
resting on it is graded.

Three things make this phase unlike Phase 3.

- **Byte-identity comes back, in a new place.** With loss at zero and delivery immediate, the
  belief equals the truth on every read, `P(resident)` is exactly 0 or 1, and every scoring rule
  reduces to today's plan. That setting is the gate every work item passes (§3, rule 1) -- the
  no-op Phase 3 could not have, recovered one level up.
- **It measures information, not authority.** Phase 3 moved who decides which blocks occupy the
  partition and kept every read exact (`phase-3.md` §3, rule 2). Phase 4 keeps who decides and
  moves what the decider can see. `phase-2.md` built the regret decomposition with two slots for
  exactly this -- `belief` and `execution` -- and this is the phase that fills them without
  touching the instrument.
- **The pre-measurements predict a small answer, so the deliverable is the reason.** A partition
  that turns over every few seconds sounds like a hard target for a lossy view, and the probe says
  the scored placement barely notices one: freezing a node's view of its KV for eight seconds
  moves nothing measurable (§1.5). The phase is worth running anyway, because *why* is a statement
  about the architecture -- which quantities the integrated path keeps exact without being told,
  which it cannot, and how honest the one estimator that bridges them is -- and because §3.7's
  argument for that estimator rests on a failure mode that turns out to live somewhere other than
  where §3.7 put it.

§1 settles eleven decisions. Six are findings about the existing documents rather than about the
work ahead: §1.2 (half of what `Gossip` carries is physical, and it is most of what `Gossip`
costs), §1.3 (the engine's publisher already replays, and does not drop the way §3.7 assumes),
§1.4 (the transport `owned-and-observed.md` §1 chose implies a relay hop it never priced), §1.5
(silence reads as calm only for load, which the integrated path does not read from telemetry),
§1.7 (the estimator §3.7 specifies is under-confident), and §1.9 (the score does not want a
quantile of output length).

---

## 1. What has to be settled before the view is made lossy

### 1.1 The belief covers exactly what Phase 3 moved

Observability follows authority. `own::authority(kind, tier, Allocation)` says who decides which
bytes occupy a pool, and a read becomes a belief exactly where that answer is `Engine` *and* the
simulator has actually moved the decision. After Phase 3 that is `KvBlock` under
`--engine-cache`, in all three of the engine's tiers: the partition, the connector's DDR offload
and its `NVMe` spill (`phase-3.md` §4.2, §4.6, §8.5).

Three scope lines follow.

- **`--belief` requires `--engine-cache`.** With the bit off the ledger allocates KV and the
  orchestrator holds the truth by authority; a lossy view of state it decided itself would model a
  bug, not an architecture.
- **Weights stay exact.** `own::authority(WeightShard, Hbm, Allocation)` is already `Engine`, but
  Phase 3 kept weights on the ledger (`phase-3.md` §3, rule 4), so the simulator still decides
  their residency and a belief about them would be a belief about the orchestrator's own
  decisions. Phase 6 makes model loading an orchestration decision, at which point the
  orchestrator knows what is loaded because it ordered the load -- so weights may never need a
  belief at all. Recorded as the one cell where `own.rs` and the belief disagree, with the reason.
- **`Snapshot` and `ServiceHeap` stay exact** under `Unified`. They are owned outright. How a
  *global* scheduler learns a node agent's owned decisions is a separate question with a
  separate, physical answer, which §1.2 needs.

### 1.2 `Gossip` has two halves, §9's objection is to one of them, and the other is most of what it costs

§9 says retire `Control::Gossip`, "which models a stale *exact* view no telemetry provides". §1
gives the reason: "you cannot scrape 'does node 3 hold prefix X'". That is an objection to a stale
exact per-block view of **engine** state. `Gossip` also carries the owned classes -- `hot_ids`
snapshots `Snapshot`, `ServiceHeap` and the ledger's weights along with the engine's KV -- and
for those a periodically relisted exact set is precisely what an informer cache over a node
agent's own ledger provides. That half is physical, and it is the comparison `residency-ledger.md`
introduced the arm for ("an informer cache is `Gossip`").

The halves are separable, so the pre-measurement separated them (§8, *gossip*): the engine-side
gossiped arms at rack, once as published and once with the KV half of the view replaced by exact
reads.

| engine side, rack | KV from the snapshot | KV exact | unified |
|---|---|---|---|
| `scored + fetch, gossiped`: service | 496.57 ms | 496.33 ms | 489.40 ms |
| its `execution` / `belief`, ns/decision | 5.96M / 0.98M | 5.80M / 0.94M | 124 / 0 |
| `both, gossiped`: service | 676.4 ms | **818.9 ms** | 1536.5 ms |

For the scored arm the engine half is about 3% of what gossip costs (2.5% at region); the rest is
a stale view of weights, where a miss is a 21 ms promotion or a 4 s reload rather than a 400 us
block. For residency-greedy, the engine half was part of the crutch §1's aside describes --
staleness stopping the arm concentrating -- and making it exact removes about a sixth of it.

**Chosen: retire the engine half, keep the owned half.** Under `--belief`, `Gossip`'s snapshot
stops carrying engine-allocated state, which reaches `Unified` and `Gossip` through the channel
instead. The owned half keeps today's semantics and every published number. With `--belief` off,
`Gossip` is byte-identical to today. Two consequences, predicted in P5: the gossiped scored arm
barely moves, and `both, gossiped` gets worse -- the crutch, partly removed, not a regression.

`Query` needs the same split. Over owned state it is a scheduler extender and stays exactly as it
is. Over engine state, "asks every candidate node before placing ... always correct" requires a
synchronous per-block lookup into the engine, which vLLM does not expose (§1.3). Chosen: it keeps
reading truth, and its engine half is relabelled for what it is -- **the value of a synchronous
prefix-lookup API at the measured crossing**. That is a promotion-tier-2 ask with a number
attached, the posture §6 approves of for the shared L2 tier and the request-path ring.

### 1.3 What the engine actually publishes settles three things §1 left open

§1 designed its channel before reading the publisher it would consume, and says to "confirm the
event set against the release targeted". Read against vLLM's `vllm/distributed/kv_events.py` on
`main` (fetched 2026-09-28; the release targeted still has to be checked, and nothing below
survives a publisher that differs):

- **Events carry a tier.** `BlockStored` and `BlockRemoved` each carry a `medium` -- `"GPU"`,
  `"CPU"` or `"STORAGE"` -- and `AllBlocksCleared` wipes the index. Stores are published, not only
  removals.
- **Batches are sequenced and replayable.** `ZmqEventPublisher` numbers every batch, keeps the
  last `buffer_steps` (10,000 by default) and serves them on an optional ROUTER socket to any
  subscriber that names the sequence number to start from.
- **The publisher does not drop under load.** Its internal queue (`max_queue_size`, 100,000)
  *blocks* the caller when full -- backpressure into the engine loop, not loss -- and the PUB
  socket's high-water mark (100,000) counts *messages*, one per batch.

Three consequences for this phase.

1. **The stream is not eviction-only, but optimism still pays.** §1's "asymmetric,
   eviction-centric wire format" assumed stores could be omitted because the router can guess
   them. It can guess the ones it dispatched, and it still should, because the stream arrives a
   step late and the next request for the same prefix should not see a false miss. But stores are
   there, and a step's stores are what reconcile the router's optimism: a block dispatched and
   never stored -- a preempted or truncated sequence -- stops being believed when its step's
   batch arrives without it (§1.6).
2. **A gap is repaired by replay, not by a snapshot.** A sequence gap becomes a request for the
   missing batches, answered one round trip later with identities intact. §1's prefix-tree
   snapshot or bloom filter is needed only past the replay buffer -- 10,000 batches is over a
   minute of steps -- or across an epoch. Recovery in this phase is replay, itself lossy. The
   snapshot survives as the `periodic` arm, which is also the world in which the targeted release
   turns out to lack replay.
3. **The drop mechanism §3.7 assumes is not this publisher's.** §3.7: "A node under memory
   pressure bursts evictions, overruns the ZMQ high-water mark and drops batches". A burst makes
   one batch *bigger*, not more numerous; the queue blocks rather than drops; and a PUB socket
   drops only for a subscriber about 100,000 messages behind -- a stalled *router*, which is
   silence on every node at once, not on the one under pressure. Per-node silence has other
   causes -- a partitioned relay (§1.4), a stalled or dead publisher -- and they are independent
   of load. So the failure mode is tested as silence episodes (§1.5), and pressure-correlated loss
   is recorded as unmotivated by this publisher rather than modelled.

### 1.4 Time: the channel needs a clock, and the design's transport implies a hop it never priced

The simulator advances time only when an arrival rate is set, by `1e9 / rate` per request.
Everything this phase models runs on two clocks, and neither needs a new constant.

- **Cadence is the engine's own step.** A batch closes at each step boundary, and a step lasts
  `Engine::step_ns` at the engine's current occupancy -- 7 ms at a batch of one, about 8.4 ms at
  the mean batch of 35.6 the defaults run at. This runs faster than §1's 40-100 Hz (105-143 Hz),
  because the engine model's step is a chosen constant (`engine.rs` says so in its header); the
  lag sweep below spans the difference.
- **Lag is the topology's.** §1 chose ZMQ over Unix domain sockets, which is node-local, and §2.4
  puts cross-node routing in a *global* scheduler. So the global scheduler hears an engine through
  its node agent -- one more hop -- and §1 prices the ipc crossing but never the relay, which is
  the larger of the two from zone out. Chosen: a batch is delivered at its step boundary plus the
  one-way latency of the configured distance (30 us at rack, 400 us at zone, 30 ms at region), with
  the scheduler equidistant from every node. No real topology is equidistant; putting the
  scheduler on one node would make that node the most confidently believed and bias a
  confidence-scored policy toward it, which is a finding about where to run a scheduler, not about
  telemetry.

`--belief` therefore requires `--rate > 0`, and it is a cluster flag only: the single-node commands
have no router separate from the ledger.

### 1.5 Silence reads as calm only for load, and the integrated path does not read load from telemetry

§3.7's case for pricing the belief as a probability is a failure mode: "the router herds onto
whichever node has stopped reporting, because silence reads as calm". The pre-measurement tested
it before any estimator exists (§8, *silence*): one node's KV view frozen for a window, the router
still recording its own dispatches, the score reading the frozen view at face value.

| `scored + fetch`, rack | silent node's share of KV decisions | window service |
|---|---|---|
| no silence (2 s window / 8 s window) | 24.1% / 25.3% | 1036.5 / 977.6 ms |
| KV view frozen for 2 s / 8 s | 24.1% / 25.6% | 1036.5 / 977.6 ms |
| half the partition, decode output held: no silence -> frozen | 24.7% -> 24.7% / 25.3% -> 25.9% | within 0.04% |

No herding in either regime, with phantom depth at the chosen node on 1-8% of decisions. The
reason is structural. The terms that would have to be fooled for a scored arm to concentrate --
`engine` and `congestion` -- read the in-flight count, and on the integrated path the router knows
that count exactly, because it carries every request and every response. What went quiet was the
residency view, and phantom residency mostly flatters a node the request was going to anyway: the
`scored` arm's decisions under a frozen view were identical to its decisions without one.

So the failure mode needs a router that reads *load* from the stream. The pre-measurement ran that
too, over four staggered episodes, because a single episode turned out to depend on luck:

| silence; `scored + fetch` (`scored` in brackets) | load from the path | from the stream, frozen | stream + own dispatches |
|---|---|---|---|
| silent node's share per episode, 2 s | 25-30% | 19-34% | 5-7% |
| silent node's share per episode, 8 s | 25-27% | 16-33% | 2-3% |
| window service against the path's, 2 s | -- | +0.4% (+1.5%) | +2.4% (+3.8%) |
| window service against the path's, 8 s | -- | +0.6% (+1.0%) | +5.7% (+7.4%) |

A stream-fed load view does herd -- in one episode -- and starves the silent node in another: the
sign is set by how loaded the node happened to be when it went quiet. And the obvious repair,
adding the router's own dispatches to the last reported load, is worse than doing nothing, because
it can only see load rise; the half it is missing is completions, and only the path carries those.

Three readings, each stated as a prediction in P4.

- **§3.7's remedy acts on a quantity that does not go stale in the way that hurts.**
  `P(resident)` discounts residency; the failure mode is in load.
- **On the integrated path the failure mode is structurally absent**, which is §2.3's third
  reason ("one belief, one actor") arriving as a number: the component that carries the traffic
  knows occupancy without being told.
- **`tele.rs`'s "Phase 4 turns engine load into a sampled quantity" is right only off the path.**
  Load stays exact by default. A stream-fed load is an arm (`--load`), because it is where §3.7's
  failure mode lives and the only place this phase can measure it.

### 1.6 What the router knows without being told, and what the belief is made of

The integrated router is not a passive consumer of telemetry.

| the router knows | because | so |
|---|---|---|
| every sequence it dispatched, and when each finished | it carries the request and the response | blocks its in-flight sequences pin have `P(resident) = 1` |
| the prompt blocks it dispatched | it sent them | optimistic stores, reconciled by their step's batch |
| the output length of every completed decode | the response passed through it | the RequestView estimator's input (§1.9) |
| engine occupancy | dispatched minus completed | `engine` and `congestion` stay exact (§1.5) |
| the partition's capacity | it granted it (`phase-3.md` §1.11) | the router's admission check stays authoritative |

What it cannot know without the stream: evictions, demotions between tiers, decode-produced
blocks until their stores arrive, and preemption. A sequence the engine could not place leaves the
router believing blocks that were never stored. §3.6 lists three things divergence spikes on --
eviction cascades between batches, telemetry drops, ignored directives. **Preemption is a
fourth**, invisible to an eviction-centric stream and reconciled only by the absence of a store.

The belief is therefore, per engine: an index of blocks by tier, each with the step at which it
was last *confirmed* -- dispatched, stored, or recovered; the set of blocks the router's own
in-flight sequences pin; and, per epoch, the mean evictions per step, counted from the stream.
§1's `TelemetryBatchHeader` is mostly derivable from this -- free and total blocks from the index
and the granted partition, eviction counts from the events, `queued` and `running` from the path.
One field is not: a **heartbeat**, one batch per step whether or not anything happened, which is
what makes absence informative. Whether the engine publishes empty batches is one more thing to
check against the targeted release; if it does not, the node agent supplies the heartbeat.

The router never runs a model of the engine's allocator (§3, rule 3). The simulated engine *is*
an exact leaf-first LRU, so a router-side replay of it would be exact here by construction and in
no deployment.

### 1.7 The estimator §3.7 specifies is under-confident, and the plan builds it anyway

§3.7: `P(resident) ~ max(0, 1 - V/B)` for `V` blocks evicted since the belief was confirmed and a
partition of `B` blocks, "sharpened by how recently the router last dispatched that prefix". Made
concrete without a free parameter:

- **`V` counts only evictions whose identity the router lacks** -- the steps whose batch has not
  been applied, because it is not yet due, was dropped and not yet replayed, or was silenced --
  times the epoch's mean evictions per step. An eviction the router received by name removes the
  block from the index and needs no estimate.
- **"Since confirmed" is per block.** A block's clock restarts when the router dispatches it,
  sees it stored, or recovers it. That is the recency sharpening §3.7 asks for, and the only form
  of it that does not replay the allocator.
- **`B` is the unpinned resident count**: the index minus the router's in-flight set. Pinned
  blocks are certain.

§3.7 predicted its own weakness -- "uniform stack rank is a convenient lie" -- and the
pre-measurement priced it (§8, *calibration*). Every eviction in the last Δ is taken as unknown;
each candidate's believed prefix gets the estimator's prediction for its deepest block; the
prediction is binned against whether that prefix was truly still resident.

| unknown for | predicted -> realised, by bin; `scored + fetch`, rack, published partition |
|---|---|
| 7 ms (one step) | 0.99 -> 0.992 |
| 200 ms | 0.77 -> 0.811; 0.88 -> 0.923; 0.95 -> 0.973 |
| 1 s | 0.46 -> 0.555; 0.56 -> 0.668; 0.66 -> 0.763; 0.75 -> 0.845; 0.85 -> 0.962 |
| 2 s | 0.04 -> 0.000; 0.15 -> 0.086; 0.45 -> 0.581; 0.55 -> 0.766; 0.65 -> 0.892; 0.75 -> 0.982 |

Above the diagonal wherever `P` exceeds about 0.2, by up to 0.24 (0.36 with decode output held),
slightly below it under 0.2, and the `V = 0` bin realises exactly 1.000 in every configuration.
That is the signature of a step being fitted with a ramp, and it has a mechanism: a block
confirmed by a dispatch sits at the head of the LRU and survives roughly until the cold blocks
behind it are gone, so its survival is closer to a step than to §3.7's line. With half the
partition and decode output held even the lowest bin realises 0.15-0.46, because the probe did not
exclude in-flight blocks -- which is why `B` and the certain set exclude them above.

**Chosen: build §3.7's estimator as specified, and let the published curve judge it.** The
alternative -- rank each believed block by the router's own dispatch order and read survival off
the rank -- is the obvious repair and is one step from replaying the allocator (rule 3). It is
named, and if the built curve reproduces the probe's, the curve is the finding, not a cue for a
mid-phase swap (rule 8). The prediction this forces is uncomfortable for §3.7 (P3): an
under-confident estimator makes a p90 rule throw away hits that are there.

### 1.8 The score over a probability: survival over depth, and a quantile with nothing tuned

§3.7's two-point form, `P * cost_hit + (1 - P) * cost_rebuild`, is the special case of one
confirmation time per prefix. A session's chain has at least two: the tenant prefix, touched by
every session of that tenant, and the session's own turns, touched when it last ran. Per-block
confirmation makes `P` a survival curve along the chain -- non-increasing, because a request
touches every block of its hit prefix -- and the prefix invariant (`phase-3.md` §4.1) makes
`P(depth >= k)` the survival of block `k - 1`. So:

- **The expectation is a sum over depths**, `sum_k P(depth = k) * cost(depth k)`, `O(prefix)` with
  suffix sums over `local_ns`, and exact under the estimator's own model.
- **The q-quantile is the plan at the deepest depth whose survival is at least `q`.** §3.7's
  collapse -- the p90 "**is** `cost_hit` when `p >= 0.9` and `cost_rebuild` otherwise" -- is this
  with one depth.
- **A peer's segment is one fact**: the survival of its deepest block, with the fallback
  `apply_chain` already charges when a peer turns out not to hold it.

Where several uncertain facts meet in one plan -- a local prefix and a peer segment -- the
quantile rule is applied **per fact**: each is taken as present iff its survival is at least `q`.
That equals the quantile for one fact and is conservative for several; the joint quantile is not
worth computing when one fact, the local prefix, carries nearly all of it.

Four rules, one flag, all identical when every `P` is 0 or 1:

| `--scoring` | a believed block is | what it is |
|---|---|---|
| `face-value` | present | §9's control arm: today's score, reading the belief as if it were truth |
| `expected` | present with probability `P` | §3.7's expectation |
| `quantile` | present iff `P >= q`, for a swept `q` | §3.7's rule at a level the sweep chooses |
| `slo` | `quantile` at the level its request declares | §3.7's table: interactive at 0.9, throughput at the mean |

The quantile applies to **the acquire term only**. Residency is the bimodal fact §3.7 derived the
rule for; displacement's price is a constant of the configuration (§1.10); engine load is exact on
the path (§1.5); and output length is §1.9's, where the pre-measurement says a quantile hurts.

### 1.9 RequestView lands here, costs almost nothing, and the score does not want a quantile of output length

The placement score multiplies `req.tokens`, the exact output length, into both engine terms --
§1's table lists it as a cheat. §3.2 says its replacement must be a calibrated predictive
distribution, because "a mean will not close it".

The pre-measurement swapped the token count the score sees, leaving every charge exact (§8,
*tokens*): the `distributed` defaults, both sides of the Phase 3 bit, rack and region, 250 and 350
req/s.

| the score sees | mean service against exact | p99 against exact |
|---|---|---|
| the unconditional mean, 124 tokens | -0.05% to +0.26% | within 0.34% |
| its p90, 204 tokens | +0.03% to +0.58% | within 0.22% |
| `max_tokens`, the 4x ceiling (250 req/s only) | +0.52% to +1.96% | up to +1.19% |

Three consequences.

- **Closing the cheat is nearly free.** A running mean of the output lengths the router has
  watched complete -- per declared class, since the path carries every response -- lands within
  0.26% of the oracle.
- **No estimator can do better on this trace.** `Workload::tokens` draws uniformly on `[24, 224)`
  independently of everything else, so no observable predicts it and the unconditional mean is
  the best any estimator can reach. That small gap is therefore the whole value of output-length
  knowledge *to the score* on this workload, and Phase 7's estimator cannot be graded here on the
  score -- only on admission, where `phase-3.md` P4 found `perfect` and `bound` a wide bracket
  (8.6% against 17.0% refused at half the partition), or on a workload whose lengths depend on
  what the router can see.
- **§3.2 is right for admission and wrong for the score.** A quantile of output length in the
  engine terms is worse on the mean and no better on the tail: tokens scale every candidate's
  engine and congestion terms by the same factor, so a quantile shifts load's weight against
  acquire everywhere at once. The score uses the mean.

Where its error lands matters. The estimate is a property of the cost model, not of the view of
state, so `m_b` and `m_t` both use it and its cost appears in the **model** gap, leaving `belief`
meaning what `phase-2.md` §1.2 defined it as: an argmin taken over a stale view of state. It gets
its own bit, `--observables` (rule 5). `needs_decode`'s `req.tokens > 0` is a ground-truth read as
well, and becomes the declared "this is an inference call".

### 1.10 The other engine reads, and why displacement needs no estimator here

`owned-and-observed.md` §8's third item says displacement "becomes an externality ... noisier and
lagged" and that its "uncertainty has no estimator yet". On this workload it needs none.
`Hierarchy::kv_displacement` is the shortfall below free space times the LRU tail's price, and
every `KvBlock` is 512 KiB with a 400 us rebuild and the same recovery path, so the tail's price is
a constant of the configuration and only the free count is engine state -- which is the granted
partition minus the index's resident count. Displacement over the partition becomes a lagged count
times a constant. A fleet with several block sizes (Phase 6) is where it would need more, and that
is recorded rather than built.

The remaining engine-allocated reads follow the index: `resident_bytes`, `used`, the hit, miss and
eviction counters, and `local_ns`'s tier for a missing block, whose three branches -- promote from
offload, read from spill, rebuild -- become an expectation over the believed tier's survival, the
same estimator on each tier's own turnover. The router's admission check reads only owned state --
the partition it granted and the reservations it made (`phase-3.md` §8.11) -- and needs nothing
from the stream.

### 1.11 Where the new errors land in the four gaps, and how each is graded

`phase-2.md` built `execution` and `belief` as "two slots that are provably empty now" for this
phase.

| error | gap | why |
|---|---|---|
| the argmin moved because a candidate's belief was wrong or discounted | `belief` | `m_b` is taken over the belief, `m_t` over truth, under the same rule |
| the chosen node's plan was wrong: a phantom local block rebuilt, a stale peer segment fell back | `execution` | `charged(p)` ran the belief's plan against truth; `R(p)` priced truth |
| the output-length estimate (§1.9) | `model` | a property of the cost function, used by `m_b` and `m_t` alike |
| the scoring rule itself | none | under truth every `P` is 0 or 1 and the four rules coincide, so `m_t` is common to every arm |

The last row is what makes belief gaps comparable across rules. `heuristic` for `scored` stays what
`phase-2.md` P1 measured -- zero -- under every rule, because `m_b` is computed by the rule the
policy used.

Two grading rules follow. **Quantile arms are graded on tails, per class**, not on mean regret: the
realized-cost oracle prices means, and §3.7's rule exists for the tail a latency SLO names. And
every belief gap is split by the **direction** of the error behind it -- a phantom (believed,
absent), a miss (present, not believed), or a discount (believed and present, but `P < 1` moved
the argmin) -- because `face-value` can make only the first two, `expected` and `quantile` add the
third, and §1.7's estimator predicts the third will dominate.

`phase-2.md` §5 called `belief_gap_is_zero_under_unified_and_query` "the test that Phase 4 will
delete". It is not deleted. At the gate it still holds, and it becomes the gate's regression test.

---

## 2. Predictions, stated first

`owned-and-observed.md` §7's rule. Nine predictions, each attached to a claim it would rewrite.
Where a pre-measurement stands behind one, §8 says how it was taken.

**P1 -- Cadence and lag are nearly free at rack; at region the relay hop is the term that shows.**

At loss zero the belief differs from truth only inside the delivery window: a step at rack, a step
plus 30 ms at region. The pre-measurement counts the KV decisions on which a view that stale puts
phantom depth on at least one candidate (§8, *turnover*): 0.22% at one step (0.38% with decode
output held, 0.93% at half the partition), 1.8-5.4% at 37 ms, against 18.5-44.6% at the 800 ms the
gossiped arms refresh at. Predicted: at rack, every scored arm's mean service within 0.05% of the
exact view, and its `belief + execution` under 5% of the gossiped arm's 6.9M ns/decision; at
region, within 0.2%.

- *If right:* the transport §1 chose is adequate at any cadence the engine produces, and the relay
  hop -- not the channel -- is the latency worth engineering, which matters only from zone out.
- *If wrong* at rack: the delivery window cannot be the cause at 0.2% exposure, so the belief's own
  machinery -- optimistic stores, reconciliation, the in-flight pin set -- is introducing error,
  and that is an instrument defect to find before anything else is published.

**Measured: confirmed at both distances, and smaller than predicted at region.** `polyphonic belief`
section 2, scored + fetch, loss zero, each row against the same run with no belief:

| distance | regime | mean service vs exact | p99 vs exact | `belief` ns/dec | exposed KV decisions |
|---|---|---|---|---|---|
| rack | defaults | +0.000% | +0.000% | 0 | 0.13% |
| rack | half partition, decode held | -0.011% | +0.009% | 0 | 0.48% |
| zone | defaults | +0.000% | +0.000% | 0 | 0.14% |
| zone | half partition, decode held | -0.002% | +0.000% | -798 | 0.37% |
| region | defaults | +0.023% | +0.149% | 79 | 1.47% |
| region | half partition, decode held | -0.021% | -0.047% | 1,672 | 4.12% |

Every scored arm is inside the prediction (0.05% at rack, 0.2% at region), and the `belief` gap is 0-1.7k
ns/decision against the gossiped arm's 887k. The exposure column agrees with the pre-measurement's
0.22-0.93% at one step and 1.8-5.4% at 37 ms. The *if right* branch fires: the channel is not what
costs, and the relay hop is the only term that shows at all, at region. The *if wrong* branch did not: the
gate (§5) passes on the real cluster in both regimes, so the machinery adds no error of its own.
**P2 -- Uniform loss at the rates §9 names is nearly free where gaps are replayed; the recovery
policy, not the loss rate, is the lever.**

Replay bounds a dropped batch's cost to about a step plus a round trip. Without it, a dropped
eviction stays unknown until the next periodic snapshot -- §1's 1-2 s -- and the pre-measured
exposure at 1 s is 22-49%, which a 5% loss rate thins to roughly 1-2.5% of decisions. Without any
recovery, phantoms accumulate at the loss rate times the eviction rate, 30 blocks per second per
engine at 5%, and nothing clears a phantom that is never dispatched again. Predicted, `scored +
fetch`, rack, mean service against loss zero:

| recovery | 1% | 5% | 20% |
|---|---|---|---|
| replay on gap | within 0.05% | within 0.05% | within 0.1% |
| periodic snapshot, 1 s | within 0.1% | within 0.3% | beyond 0.3% |
| none | the phantom share of the index grows with run length under every rule | | |

In the last row what differs by rule is whether the score believes the phantoms: under `expected`,
the index's total `1 - P` tracks its realised phantom count; under `face-value` it is zero by
construction.

So §9's sweep "returns the same answer at every rate", as §9 said a sweep against a score that
cannot react would -- but under replay it does so for every rule, including the ones that can
react, because there is nothing left to react to. The 20% column is what keeps that falsifiable: a
sweep flat everywhere needs a point where it is not, or the flatness is untested.

The same arithmetic prices §1.2's relabelled `Query`. Under replay, a synchronous lookup API
removes a belief cost below any crossing on the ladder, so it is not worth asking for on the
integrated path; its number is non-trivial only without recovery.

- *If right:* the channel's reliability engineering -- replay, snapshots, loss budgets -- is not
  where this architecture's risk is, and §1's channel reduces to "consume the engine's stream,
  replay on a gap".
- *If wrong* (belief cost scales with uniform loss even under replay): replay is not repairing --
  the replay itself is lost too often, or reconciliation after it is incomplete -- and either is a
  defect.

**Measured: the channel does not cost, and the recovery policy moves the belief without moving service.**
Section 3, rack, mean service against the exact view, over the whole grid of loss (1, 5, 20%), recovery
and five scoring rules: replay stays within **0.075%** at every loss rate in both regimes (the plan said
0.05% to 0.1%). Periodic recovery at 20% loss stays within **0.15%** -- the plan said "beyond 0.3%", so
that column is wrong. No recovery at all stays within **0.16%** even at 20% loss, with the belief wrong
about a third of the time. The phantom share (belief entries the engine no longer holds, sampled every 16th
KV decision) is where the recovery policy shows:

| loss | replay | periodic (1 s) | none |
|---|---|---|---|
| 1% | 0.08% | 0.22% | 3.6% |
| 5% | 0.08% | 0.80% | 15.1% |
| 20% | 0.13% | 3.8% | 36.6% |

and at half the partition with decode output held, 0.22% / 0.60% / 8.7% at 1%, 0.23% / 2.1% / 32% at 5%,
0.37% / 9.9% / 60% at 20%. So §9's sweep returns "the same answer at every rate" for service, as §9 said a
sweep against a score that cannot react would -- and it does so for every rule, including the ones that can
react, at a point (20%, no recovery, 60% phantoms) where a belief that mattered would have shown. What the
plan did not measure is the "index's total `1 - P` tracks its realised phantom count" line: the instruments
do not sum the estimator, only bin it (§9.3). The synchronous-lookup ask for `Query` is therefore worth
nothing here on any recovery policy: the belief cost it would remove is under 0.16% of service at 20% loss
with no recovery.
**P3 -- The published calibration curve sits above the diagonal, and a p90 rule built on it
discards hits that are there.**

§1.7's pre-measurement. Predicted: the built curve has the same shape -- above the diagonal for
`P` between about 0.2 and 0.9, at or slightly below it under 0.2 once pinned blocks are excluded,
and the `V = 0` bin realising exactly 1 -- and `quantile` at `q = 0.9` shows the largest
discount-led belief gap (§1.11) of any rule, under every condition that leaves evictions unknown.

- *If right:* §3.7's rule is defended and its estimator is not. The repair is the estimator's
  shape -- survival from rank rather than from turnover -- and rule 3 makes whether that is
  admissible a later phase's decision.
- *If wrong* (calibrated, or over-confident): the probe's model of unknowns -- everything in a
  recent window -- misrepresents loss, which removes a random subset rather than a recent slice;
  the rank structure differs and the ramp may fit it. That would vindicate the "convenient lie" as
  convenient enough.

**Measured: half wrong, and the wrong half is the interesting one.** The built curve (section 5, rack) is
**not** above the diagonal where it counts. With replay and no loss it is close to it, and under-confident in
the mid bins the way the pre-measurement found (predicted 0.865 against realised 0.929 at half the
partition, 0.952 against 0.963 in the top bin). With loss and no recovery it is **over-confident**, and
badly so under memory pressure:

| half partition, decode held, 5% loss, no recovery | n | predicted | realised |
|---|---|---|---|
| 0.9 bin | 914 | 0.959 | 0.695 |
| 0.8 bin | 239 | 0.864 | 0.393 |
| 0.7 bin | 86 | 0.764 | 0.174 |
| 0.6 bin | 31 | 0.654 | 0.161 |

At the published partition the same condition reads 0.992 predicted against 0.977 realised in the 13.9k-
candidate top bin. The mechanism is the one §3.7 named and the pre-measurement could not see: a block
whose confirmation is oldest is exactly what an LRU evicts first, so the unknown evictions are not spread
uniformly over the resident blocks (`1 - V/B`) but concentrated on the stale ones. The pre-measurement's
"everything in the last window is unknown" model had a fresh window to be under-confident about; a dropped
batch that is never recovered leaves a permanent one, and the estimator has no rank to say which blocks it
takes. The *if right* branch's repair (survival from rank) is exactly what rule 3 forbids, and rule 8 says
report rather than repair, so it stays reported.

The second half of the prediction -- `quantile 0.9` shows the largest discount-led gap -- **did not hold**.
At half the partition with no loss, the discount-led share of the belief gap is 71% for `expected`, 42% for
`quantile 0.9` and 74% for `quantile 0.99`; and every share is a share of a gap that is itself 0-100
decisions, so none of it moves service (P3's curve is the finding, not this). `quantile 0.5` is identical to
`face-value` at the published partition in every cell, because survival never falls below 0.5 there.
**P4 -- Silence does not herd the integrated router; it herds or starves a stream-fed one by
chance; and `P(resident)` changes neither.**

§1.5's pre-measurement, as predictions for the built channel, with silence episodes of 2 s and 8 s
staggered one per node.

- **Load from the path.** Under `face-value`, the silent node's share of KV decisions stays within
  1pp of its no-silence share. Under `expected` and `quantile` it falls *below* that share, lowest
  under `quantile`: the estimator decays a silent node's residency as if a turnover's worth of
  evictions happened -- two seconds at 590 evictions per second is `P` near 0.4 at the published
  partition -- so the router moves sessions off a node that mostly still holds them. `P(resident)`
  turns silence into avoidance.
- **Load from the stream.** Herding or starvation per episode, its sign set by the node's load when
  it went quiet; mean window service 0.4-1.5% above the path's; and `P(resident)` scoring leaves
  both within noise, because the quantity that went stale is load.
- **Stream plus the router's own dispatches.** Starvation on every episode, and window service
  2.4-7.4% above the path's.

- *If right:* the failure mode that motivates §3.7 is a property of routers off the path, and
  §2.3's "one belief, one actor" has a number -- the component that carries the traffic needs no
  telemetry about load, and one that does not carry it cannot repair its view by counting what it
  sent.
- *If wrong* on the path (the silent node draws more than its share under `face-value`): phantom
  residency outweighs congestion after all, the congestion term does not protect a scored arm from
  its own belief, and §3.7's failure mode is real on the integrated path. That is the version of
  §3.7 worth having, and it would make `P(resident)` load-bearing.

**Measured: the integrated router is not herded by silence, and `P(resident)` neutralises a small attraction
instead of creating an avoidance.** Section 4, rack, published defaults, four staggered episodes, the
silent node's share of KV decisions inside its episode against outside it (about 25% outside), and mean
service against the same load source and rule with no silence:

| load read from | rule | 2 s | 8 s | service vs quiet, 8 s |
|---|---|---|---|---|
| the path | `face-value` | 26.5 / 24.9% | 26.5 / 24.8% | +0.014% |
| the path | `expected` | 24.5 / 25.0% | 25.5 / 24.9% | -0.013% |
| the path | `quantile 0.9` | 25.0 / 25.0% | 25.1 / 25.0% | -0.004% |
| the stream | `face-value` | 27.2 / 24.9% | 25.6 / 24.9% | +0.461% |
| the stream | `expected` | 25.3 / 25.0% | 24.6 / 25.1% | +0.472% |
| stream + dispatches | any rule | 4.0-4.8 / 25.7% | 1.6-2.0 / 28.4% | +3.15-3.17% |

- **Path.** Within 1.7pp of the baseline under `face-value` -- a real but small attraction, the prediction's
  "within 1pp" missed by 0.7 -- and exactly at it under `expected` and `quantile 0.9`. The prediction that
  those two would fall *below* the baseline is wrong: silence turns the attraction off, not into
  avoidance, because two seconds of a node's evictions barely discounts a belief of 2,000 blocks.
- **Stream.** Averaged over four episodes the share is at the baseline (23-27%), because the sign is set per
  episode by the node's load when it went quiet, as the pre-measurement found; the cost shows in the tail
  (p99 +0.06-0.36% at 2 s, +1.23-1.34% at 8 s) and in mean service (+0.46-0.49% at 8 s). `P(resident)`
  changes none of it, to within 0.03%.
- **Stream + the router's own dispatches.** Starvation, every episode and every rule: the silent node falls
  to 1.6-2.0% of KV decisions and mean service rises 3.15-3.17%, p99 3.8-4.0% -- the pre-measurement's
  5.7-7.4% window figure diluted over a whole run. Counting only what it sent cannot repair a view whose
  missing half is completions.

The *if right* branch fires: §3.7's failure mode needs a router that reads load from the stream, and
§2.3's "one belief, one actor" has its number. The estimator's contribution on the integrated path is one
of two things depending on the phase's question: it does remove the residual attraction (26.5% to 25.0%),
and it does nothing that changes a result. (Each row's baseline is now the same load source and rule
without silence; the first version of this table measured every row against path and `face-value`, which
put the load source's own cost inside the silence column. It moved each figure by under 0.03pp; §9.6.)

**P5 -- Retiring `Gossip`'s engine half barely moves the scored arm and removes a sixth of
residency-greedy's crutch.**

§1.2's pre-measurement, as a prediction for the channel at loss zero: `scored + fetch, gossiped`
within 0.1% of 496.3 ms at rack, with its `execution` gap still above 5M ns/decision because what
remains is a stale view of weights; `both, gossiped` within 5% of 819 ms.

- *If right:* every published gossip result is a result about informer caches over owned state,
  and says nothing about engine telemetry.
- *If wrong* (the scored gossiped arm moves by more than 0.5%): the channel's lag or
  reconciliation costs what the exact KV view did not, which P1 will already have flagged.

**Measured: confirmed at the published partition, and the direction moves with the regime at half of it.**
Section 6, rack, service of the gossiped arms with engine KV read from the snapshot against from the
channel:

| arm | published defaults | half partition, decode held |
|---|---|---|
| `scored + fetch, gossiped` | 496.590 -> 496.344 ms (-0.050%) | 498.600 -> 498.842 ms (+0.049%) |
| `both, gossiped` | 676.422 -> **818.932 ms (+21.1%)** | 1047.257 -> **737.696 ms (-29.6%)** |

The published-defaults column is the prediction to the digit (818.9 ms, as the pre-measurement had it):
the scored arm moves under the 0.1% line and residency-greedy loses a fifth of its crutch. What the plan
did not have is the second column: **under memory pressure the same removal helps residency-greedy by
30%.** The snapshot's staleness is a herding suppressor while the partition is roomy and a handicap once
the blocks it advertises are being evicted -- so "the gossip result was partly a crutch" is a statement
about the regime. Every published gossip result is a result about an informer cache over owned state
*and* one regime's staleness; neither survives being quoted alone.

**P6 -- RequestView costs under 0.3% of mean service, and lands in the model gap.**

§1.9's pre-measurement. Predicted: the A/B on `--observables`, with `--belief` off, moves mean
service by at most 0.3% and p99 by at most 0.5% at every published cluster configuration; `belief`
is unchanged by it to the nanosecond, and `model` absorbs the difference.

- *If wrong* (more than 0.5%): the engine terms are more sensitive than one seed showed, and the
  ceiling §1.9 hands Phase 7 is higher than stated.

**Measured: confirmed, and slightly favourable.** Section 7, rack, `--observables` against the exact output
length, with the belief off and at 5% loss with replay:

| regime | belief | mean service | p99 |
|---|---|---|---|
| defaults | off | -0.026% | -0.053% |
| defaults | 5% loss, replay | -0.032% | -0.034% |
| half partition, decode held | off | -0.129% | -0.353% |
| half partition, decode held | 5% loss, replay | -0.128% | -0.361% |

All four inside 0.3% mean and 0.5% p99, and all four *better* with the observed mean. `model` moves
(270,850 -> 213,860 and 309,874 -> 156,581 ns/decision at the two regimes) and `belief` is 0 with the
belief off, as predicted; with 5% loss it reads 56 and 137 ns/decision under exact lengths and 0 / 137
under observed ones, which is the scoring rule interacting with the mean and not RequestView leaking into
the belief gap. The plan's "no estimator can do better on this trace" makes the negative sign natural: the
exact length is a myopic input to a score whose other terms are means, and the unconditional mean is not.

**P7 -- The quantile has no tail to protect on this workload.**

The acquire term's miss branch is a prefix rebuild -- thirty blocks at 400 us is 12 ms -- against a
per-class service p99 near 1.9 s that decode and queueing set. Predicted: at every loss setting and
silence condition, `quantile` and `slo` land within 1% of `expected` on every class's **service**
p99, including the declared-throughput class; differences, where there are any, appear in **stall**
p99, which excludes the decode. So the two-tier scoring §3.7 proposes buys nothing a scalar would
not on the axis a user sees -- for the reason `phase-3.md` P4 found two-tier admission class-blind,
decode dominance -- and declaring the SLO axis does not rescue it.

- *If wrong:* residency decisions redirect load enough to change queueing, a second-order effect
  worth more than the acquire term itself, and the quantile rule acts through placement rather than
  through the price it was designed for.

**Measured: confirmed for service; in stall p99, `slo` alone moves, and only in direction.** Section 8,
rack, published defaults, 30% of sessions throughput-bearing, `expected` against `quantile 0.9` against
`slo`, under 5% loss with no recovery and under 2 s of silence, on seeds 1-3. Service p99 is within 1%
across every rule for both classes (seed 1: 1,911.0-1,911.3 ms interactive, 1,929.7-1,935.7 ms
throughput). Interactive stall p99 sits on a plateau near 45 ms (44.6-51.0 ms in all twelve `expected` and
`quantile 0.9` cells) and drops off it only under `slo`: to 17.6 ms under 2 s of silence on seed 1, 24.8 ms
under 5% loss on seed 2 and 22.9 ms under 5% loss on seed 3 -- three of six `slo` cells against none of the
other twelve, which would happen about 2.5% of the time if drops fell evenly. So there is a real difference in
stall, as P7 allowed, with no measurable size: the plateau flips on a handful of requests. `slo` differs
from `quantile 0.9` only in scoring throughput requests at the mean, so whatever it is works through where
throughput work lands. On the axis a user sees -- service -- the declared SLO does not rescue the two-tier
score, for the reason `phase-3.md` P4 found two-tier admission class-blind: decode dominates.

**P8 -- The chosen node is over-confident relative to the field.**

The argmin selects the candidates whose belief errs high. Predicted: at the same predicted `P`, the
chosen node's realised residency is below the all-candidates curve, by most under `face-value`,
whose selection runs toward phantoms, and least under `quantile`.

- *If wrong* (the two curves agree): acquire rarely decides placements here -- consistent with the
  `decided_by` counts -- and the estimator's calibration matters less than its existence.

**Measured: wrong -- there is no selection effect.** Section 5's second column is the same predicted-
against-realised curve restricted to the node the router chose. At half the partition with 5% loss and no
recovery the chosen node is *not* below the field: 0.721 against 0.695 in the 0.9 bin, 0.429 against 0.393
in the 0.8 bin, 0.172 against 0.174 in the 0.7 bin. At the published partition it is 0.975 against 0.977. The
argmin does not select for phantoms because acquire rarely decides placements here (the `decided_by`
counts have said so since `phase-2.md`), so whatever the estimator's calibration is, it is not being
exploited by the argmin.

**P9 -- There is no flapping to damp; there is needless migration to count.**

§3.7 warns that a threshold rule "can **flap**: a node oscillating around `p = 0.9` alternates
between two very different scores". Under §1.7's estimator `P` is non-increasing between
confirmations, and only a dispatch, a store or a recovery raises it; a router that has stopped
sending to a node cannot raise that node's `P` except by recovery. So a prefix's verdict can be
recrossed upward at most once per recovery, and oscillation is bounded by the recovery cadence.
Predicted: placement churn under `quantile` within 10% of `expected`'s, with the threshold's cost
appearing instead as discount-led migrations off nodes that truly held the prefix (P3), counted
directly. Hysteresis is not built.

- *If wrong* (churn rises with loss under `quantile`): something other than recovery raises `P` --
  stores of blocks the router did not dispatch, decode output most likely -- and hysteresis, a
  tuned constant, would be needed. That is a result worth publishing before anyone adds one.

**Measured: confirmed at `q = 0.9`, and the *if wrong* branch fires at `q = 0.99`.** Placement churn -- a
session's KV turn placed off the node its previous turn ran on -- is 36.7-43.3% across section 3's 100
cells. At `q = 0.9` it is within 4.9% of `expected` in every cell of both regimes, inside the predicted
10%, and `face-value` reads 37.0-38.9% throughout; there is no flapping to damp at the threshold §3.7
names, and hysteresis stays unbuilt. At `q = 0.99` churn rises with loss where nothing repairs it: 37.0% at
loss zero, 39.3% at 5% and 43.3% at 20% with no recovery, 40.4% at 20% with periodic snapshots. The
instrument counts moves, not reversals, so it cannot say whether that is the oscillation P9 was worried
about or one-way migration off nodes whose survival no longer reaches 0.99 once anything is unknown; §1.7's
monotonicity argument says the second, and the churn figure alone does not decide it. The other clause, that
the threshold's cost would show as discount-led migrations off nodes that truly held the prefix, cannot be
read from the migration counter: it is 99.9% "needless" at loss zero because the score routes off the
best-cache node for load reasons (§9.3). The one signal it does carry is small: `expected` makes about 4%
more migrations than `face-value` at 0% loss (3,080 against 2,964). (The first published churn,
23.6-25.1%, keyed placements by the tenant prefix's root rather than the session and counted non-KV
requests too; §9.6.)

---

## 3. What the belief must and must not do

Eight rules. The first is the gate; the third is the one most likely to be broken for a good
reason.

1. **The gate.** With loss zero and delivery immediate, the belief equals the truth on every read
   and every `P` is 0 or 1, and every output is byte-identical to the same run with `--belief` off.
   Checked after every work item, beside the standing check: with `--belief` off, byte-identical to
   `HEAD` on the reproducible set as `phase-3.md` left it (`placement --drain-at` and `price`
   included), by `phase-1.md` §5's discipline.
2. **Observability follows authority, exactly.** Only engine-allocated state as Phase 3 moved it
   becomes a belief (§1.1). Owned state stays exact; weights stay exact until Phase 6 changes who
   decides them.
3. **The router never replays the engine's allocator.** Its belief is fed only by what a
   deployment could see: its own dispatches, the responses its path carries, the partition it
   granted, and the engine's stream. A router-side LRU would be exact here by construction -- the
   simulated engine is one -- and in no deployment, so it is refusable by citation however much
   better it would make every number.
4. **Nothing tuned.** `P` from counts, `V` from epoch means, cadence from the engine's step, lag
   from the topology, the quantile from a declared SLO or a swept level printed with the result. No
   smoothing constant, no penalty weight, no hysteresis. Loss rates, recovery policies and silence
   episodes are *conditions* of the experiment, swept and printed, never parameters of a policy.
5. **One bit per mechanism.** `--belief`, `--scoring`, `--load`, `--observables`, `--throughput`
   and each condition are separately selectable. The headline sweeps run with `--observables` off,
   so the belief is the only change, and one cross-check row runs both.
6. **The channel's randomness belongs to the modelled system, not to the instrument.** Loss draws
   come from their own seeded stream, separate from the workload's, so the trace is byte-identical
   at every loss setting and a result moves with the channel, never with the trace.
   `phase-2.md`'s ban on random sampling in the measurement path stands; this RNG is on the other
   side of that line.
7. **Nanoseconds or counts** (`phase-2.md` rule 5). A calibration curve is a count over a count; so
   is divergence.
8. **Measure, do not repair** (`phase-2.md` rule 1). If the curve condemns §3.7's estimator,
   Phase 4 reports it and does not swap estimators mid-phase. If the path makes the failure mode
   unreachable, Phase 4 reports that and does not construct a regime where it is reachable.

---

## 4. Work items

Ordered so each lands compiling and checkable against rule 1, and so nothing that can move a number
lands before the items that cannot. §9's order -- "first, the belief becomes a probability" -- is
kept in the only form in which it is checkable: the scoring rules land (§4.4) before any condition
exists to make them differ (§4.5).

### 4.1 The engine's event stream

`KvTiers` records every transition it makes -- a store or a removal per tier, with the tier as
`medium` -- and a drain records an epoch change. This is where Phase 1's lesson applies twice over:
a census is only as complete as the set of paths someone thought to instrument (`phase-1.md` §4.4),
and `KvTiers` has several -- `place`, `offload`'s cascade into spill, `forget_cold`, promotion out
of a colder tier, preemption, `drain`.

So the item lands with its own invariant, the analogue of `phase-3.md`'s census zero-row:
**applying the recorded stream to an empty index reproduces all three of the engine's tiers exactly
after every request**, over a cluster run with decode output held, fetch, a shared L2 and a drain.
A path that moves a block without recording it fails at the first request it touches. Nothing
consumes the stream yet: a no-op.

### 4.2 `belief.rs`: the belief and the channel

```rust
pub enum Recovery { Replay, Periodic, None }
pub enum Scoring { FaceValue, Expected, Quantile(f64), Slo }
pub enum LoadSource { Path, Stream, StreamPlusDispatch }
```

`Channel` groups each engine's stream into step windows on the engine's clock (§1.4), delivers a
closed window at its boundary plus the one-way hop, drops batches under the loss condition, detects
a sequence gap at the next delivery, and answers it by replay one round trip later -- the replay
itself subject to loss. A silence episode is a window in which a node's batches are produced and
not delivered, ending in the gap that triggers the replay.

`Belief` applies what arrives, records the router's own dispatches as optimistic stores, reconciles
them against their step's stores, tracks the in-flight pin set, and answers `p_resident` and
`p_held` by §1.7's estimator. Wired and unread: a no-op.

### 4.3 The boundary reads the belief

Under `--belief`, every `Telemetry` read of engine-allocated state is served by `Belief`: `resident`
and `held` as "believed", for the rules that want a boolean; `p_resident` and `p_held`; the tier
`local_ns` prices; the free count `kv_displacement` multiplies (§1.10); `resident_bytes` and the
counters. `ground_truth_resident` and `ground_truth_holds` are untouched -- they are the other side
of §3.6's subtraction. `Gossip`'s snapshot drops engine-allocated ids; `Query`'s engine half keeps
reading truth (§1.2).

Checked twice: by the gate, and by a **boundary test** -- evicting a block from an engine without
recording its event changes no `Telemetry` read under `--belief`. That is the test that catches a
truth read left inside the belief, which is the failure `tele.rs`'s passthrough test was written to
catch in the other direction.

### 4.4 The score over a probability

`plan(View::Belief)` prices the local prefix as §1.8's survival sum and each peer segment as one
fact, under `--scoring face-value|expected|quantile|slo` and `--quantile q`. `View::Truth` reads
truth for residency *and* for load, whatever the flags, so `m_t` is the same argmin under every
rule. On an exact belief the four rules make identical decisions, by test, before any condition
exists to separate them.

### 4.5 The conditions

Cadence and lag (§1.4) switch on with `--belief`; the gate's immediate delivery is a test fixture,
not a flag. `--loss p`, uniform over batches. `--recovery replay|periodic|none`, the periodic arm at
§1's 1 s with 2 s printed as a sensitivity. `--silence D`, one episode per node, staggered evenly
across the run, deterministic. The first item that moves a number.

### 4.6 Where the argmin's occupancy comes from

`--load path|stream|stream+dispatch` (§1.5). `path` is today's in-flight count and the default;
`stream` reads the running count from the engine's last delivered batch; `stream+dispatch` adds the
router's own dispatches since. Two reads change -- `projected_ns` and `congestion_ns` -- and only
under `View::Belief`.

### 4.7 A declared SLO

`Request::slo`, `Interactive` or `Throughput`, declared and never inferred
(`owned-and-observed.md` §4). The generator labels a fraction `--throughput` of sessions as
throughput-bearing, deterministically from the session id rather than by a draw, so the trace is
byte-identical at every fraction and only the label moves.
A session's fan-out agents inherit its label; requests that do not descend from a session are
interactive. The default is zero, every decode interactive, which is what this workload's classes
are.

`Deadline(t)` is not built. Nothing consumes a deadline yet, and a field no mechanism reads is a
comment.

### 4.8 RequestView

A `RequestView` the scoring path takes instead of `&Request` -- chain, dependencies, flow,
`max_tokens`, `slo`, and whether it is an inference call -- so the score cannot name `tokens`,
`produces` or `exec_ns`, and the compiler finds every site that did. The realized-cost oracle and
execution keep `&Request`; the oracle's two model argmins, `m_b` and `m_t`, go through the view
like the policy does, which is what puts the estimate's error in the model gap (§1.9). The engine
terms' token count becomes the running mean of completed output lengths per `slo` class, observed
on the path, with the declared `max_tokens` standing in until the first completion.
`--observables`, off by default. `Reserve::Perfect` still reads `produces`: it is the cheat used
honestly as a bound (`phase-3.md` §1.4), which is what it is for.

### 4.9 Instruments

| instrument | measures | over |
|---|---|---|
| calibration | predicted survival of each believed prefix against truth, ten bins plus `V = 0` | all candidates, and chosen nodes (P8) |
| divergence | phantom share `|B \ A| / |B|` and miss share `|A \ B| / |A|` per engine, by cause: not yet due, dropped, silenced, never stored | every decision |
| exposure | share of KV decisions with phantom depth at any candidate, and at the chosen one | every KV decision |
| belief-gap direction | phantom-, miss- or discount-led (§1.11), carried on the span | every span with nonzero `belief` |
| migration | a request placed off the node holding its longest believed prefix, split by whether that node truly held it | every KV decision |
| silence window | the silent node's share of KV decisions, its queue, window service | each episode |

Aggregated inside `Machine` and never recorded per candidate (§3.11). Spans gain the belief-gap
direction, so its aggregate is a reduction over spans and cannot drift from them (`phase-2.md`
§4.4). Divergence by cause is exact, because the simulator knows each event's fate.

These are the instruments the pre-measurements approximated, and §8's figures are reproduced with
them first.

### 4.10 `polyphonic belief`

A reproducible sweep, as `price` is for Phase 3: no control crossing charged, seed-deterministic,
the `distributed` cluster at the two regimes Phase 3 already chose -- the published defaults, and
half the partition with decode output held -- at rack and at region.

1. the gate, as a printed check line
2. cadence and lag at loss zero (P1)
3. loss (0, 1, 5, 20%) against scoring (`face-value`, `expected`, `quantile` at 0.5, 0.9, 0.99)
   against recovery (P2, P3)
4. silence (2 s, 8 s) against load against scoring (P4)
5. calibration curves, all candidates and chosen (P3, P8)
6. the gossiped arms, engine half from the snapshot and from the channel (P5)
7. RequestView's A/B, and one row with 5% loss beside it (P6)
8. the SLO mix at `--throughput 0.3`: per-class service and stall p99 (P7)
9. churn and migration per rule (P9)

`distributed` and `code-review` take the same flags, so `distributed --engine-cache --belief --loss
0.05 --regret` runs all eleven arms through the channel at the measured crossing.

### 4.11 Two-tier admission, if there is time

§3.7: "one statement per class governs both the routing score and the admission bound -- which is
the test of whether this is a real axis or two knobs sharing a name". `--admit quantile` reserves
each request at its SLO's level of the observed output distribution -- p90 for interactive, the
mean for throughput -- beside `phase-3.md`'s `bound`, `perfect` and `none`. Phase 3 left "the
quantile itself to the phase that has a calibrated distribution to take it from", and this phase
has one: on this trace the unconditional distribution the path observes is calibrated by
construction (§1.9).

**The one item that can be cut without weakening the deliverable.** If it slips, it slips to Phase
7, which inherits the SLO field and the observed distribution.

### 4.12 Report and publish

| target | change |
|---|---|
| `owned-and-observed.md` §9 Phase 4 | a **Status** line, as every built phase carries |
| `owned-and-observed.md` §1, *Step-aligned ingestion* | what the publisher provides (§1.3); the relay hop (§1.4); replay in place of the sync batch inside the buffer |
| `owned-and-observed.md` §3.2 | output length: the mean for the score, the distribution for admission (§1.9) |
| `owned-and-observed.md` §3.6 | preemption as a fourth divergence source; divergence in both directions |
| `owned-and-observed.md` §3.7 | the calibration curve; the failure mode relocated to load (§1.5); the per-block form (§1.8) |
| `owned-and-observed.md` §5 | the gossip result restated as a result about informers over owned state (§1.2) |
| `tele.rs` | the load doc comments: exact on the path, sampled only off it |
| `phase-2.md` §5 | "the test that Phase 4 will delete": kept, as the gate's regression |
| `residency-ledger.md` | a *Belief* section, every figure marked with its conditions; *Standing* rows for the gossip restatement and the belief's cost |

Whether `--belief` becomes the default under `--engine-cache` is decided after the numbers, as
Phase 3 decided its own bit.

---

## 5. Verification

- **Byte-identity with `--belief` off**, against `HEAD`, on the reproducible set at a reduced
  `--ops` and a second seed, after every work item individually. `distributed`, `code-review` and
  `data-path` get the structural smoke run, for `phase-1.md` §5's reason.
- **The gate.** `--belief` on, loss zero, immediate delivery: belief equal to truth on every read,
  asserted per decision over a full cluster run with decode output held, fetch and a drain; and
  `belief`'s printed check line identical to the `--belief`-off run.
- **The stream reproduces the engine** (§4.1): replayed into an empty index, it equals all three
  tiers after every request.
- **The boundary holds** (§4.3): an eviction without its event changes no read.
- **The rules coincide on an exact belief** (§4.4): identical decisions under all four.
- **The decomposition still sums per decision.** `heuristic` is exactly zero for `scored` under
  every rule, and `belief_gap_is_zero_under_unified_and_query` passes unchanged at the gate.
- **The certain bin is certain.** The `V = 0` calibration bin realises exactly 1.0; a phantom with
  no unapplied step behind it is a reconciliation defect.
- **The channel's randomness is isolated** (rule 6): the request stream hashes identically at every
  loss setting.
- **Replay repairs.** After a silence episode ends and its replay lands with no further loss, the
  belief equals the truth on the silent node.
- **RequestView is enforced by type and by behaviour.** The scoring path compiles without access to
  `tokens`, and perturbing `tokens` under `--observables` changes no decision.
- `cargo fmt --check`, `cargo clippy --all-targets --all-features` and `cargo test` clean. The
  census build still emits 13 warnings: the stream is recorded inside `KvTiers`, which is the
  engine's own, and adds no entry point that assumes allocation authority. A moved count means it
  did.

---

## 6. Risks

1. **A small answer read as a broken instrument, or tuned into a large one.** The pre-measurements
   predict a near-zero belief cost almost everywhere on the integrated path, and the temptation is
   to find the configuration where it is large and lead with that. The regimes are Phase 3's,
   chosen for Phase 3's reasons (`residency-ledger.md` *Regime selection*); the conditions where a
   large answer is predicted -- no recovery, a stream-fed load -- are named in §2 before the run
   and reported as where the cost lives, not as the headline.
2. **The shadow-allocator trap** (rule 3). The cheapest way to make the belief better is to model
   the engine's LRU, which is exact here and wrong everywhere else. Every proposed improvement to
   the estimator gets one question: could a router in front of a real vLLM compute this?
3. **A missed event path** -- how both of the census's gaps happened (`phase-1.md` §4.4), and how
   `phase-3.md` §8.11's stale-copy cascade did. §4.1's replay invariant is the mitigation, and it
   fails loudly rather than producing plausible numbers.
4. **Decode dominance.** Every residency effect here is tens of milliseconds inside ~500 ms of mean
   service and ~1.9 s of p99 -- `phase-2.md` risk 4's small difference of large numbers. Per-class,
   stall-denominated and per-window reporting are the presentations in which the effect exists; an
   aggregate mean will read as a null.
5. **The simulator acts at arrival instants.** A real engine evicts through a prefill, interleaved
   with its own steps; here every eviction happens at the moment a request arrives, and a step
   window only groups them. So the delivery window's cost is a lower bound -- the busiest 5% of
   step windows already carry about half of all evictions (§8, *turnover*) -- and intra-step
   orderings, a block evicted and re-stored inside one step, cannot occur.
6. **The publisher as read is not the publisher targeted.** §1.3 rests on one reading of `main`. If
   the targeted release lacks replay, `--recovery periodic` is that world; if it drops under load,
   the pressure-correlated loss §1.3 declined to model becomes an arm; if it publishes no empty
   batches, the node agent supplies the heartbeat.
7. **An equidistant scheduler** (§1.4). An asymmetric one makes its own node the most confidently
   believed, which a `quantile` rule would prefer. Named, not measured.
8. **Two corrections, again.** The belief, RequestView and the SLO labels can each move a number.
   Rule 5's bits keep them apart, and §4.10 runs the headline with the other two off.
9. **The pre-measurements came from outside the repository.** They argue for the predictions and
   are evidence for no claim. §4.9 rebuilds each, and §8 says exactly how each was taken.

---

## 7. Out of scope

- **Beliefs about weights, and partition sizing as a decision.** Phase 6 (§1.1).
- **Retention directives, and the ignored-directive divergence source.** Phase 5. Divergence by
  cause has the slot, and `--belief` is what Phase 5's `ignores` arm will be read through.
- **A router-side model of the engine's eviction order**, whether rank-based survival or a replayed
  LRU (rule 3, §1.7).
- **Pressure-correlated loss** (§1.3), unmotivated by the publisher as read.
- **Engine load as a sampled quantity on the integrated path.** It is not sampled there (§1.5).
  `--load stream` is the off-path case; the full combination of a sidecar data path with a lossy
  belief is a Phase 8 × Phase 4 cell nobody has asked for yet.
- **The router's admission check inside the argmin** (`phase-3.md` §8.11). It reads only owned
  state, so it needs nothing from this phase, and moving it would change the goodput denominator
  inside a measurement of something else. It belongs to the admission line.
- **Bloom-filter recovery.** Replay recovers identities exactly within its buffer, and an
  approximate snapshot adds nothing inside it.
- **`Deadline(t)`, predicted flows, the tool-gap estimator, the RAG class, taxonomy presets.**
  Phase 7.
- **The shared L2 tier's observability.** It is a tier priced before it is built (§3.10); how its
  contents are observed is a question for whoever builds it, and it stays exact.
- **Transport cost.** `owned-and-observed.md` §1 settled that every rung is free at the step rate.
  The relay hop is modelled as *lag*, never charged as a crossing.
- **Per-candidate spans** (§3.11).

---

## 8. Pre-measurements

All taken on an instrumented copy of `5247d17`, run outside the repository and not committed. The
configuration, unless a row says otherwise, is `distributed`'s cluster -- 4 nodes, 3 units each,
16 GiB HBM, 32 GiB DDR and 64 GiB `NVMe` in total -- at 250 req/s, 10% fan-out, 15,000 ops, seed 1,
rack distance, no control crossing, with `--engine-cache` at its default grant (a 1.00 GiB partition
and 0.80 GiB of offload per node). The spill grant was fixed at 8 GiB rather than sized from the
ledger's run, which moves `scored + fetch` from 489.45 ms to 489.40 ms.

| name | what | how | headline |
|---|---|---|---|
| *turnover* | partition churn, and a stale view's exposure | engine evictions logged with their arrival time; at each KV decision, each candidate's chain depth under truth against its depth if evictions in the last Δ were unknown | 590 partition evictions per second per engine at the published partition (1,978 of 2,048 blocks resident, a full turnover every ~3.4 s), 737 with decode output held, 814 at half the partition; the busiest 5% of 7 ms windows carry 49-64% of evictions; exposure 0.22-0.93% at 7 ms, 1.8-5.4% at 37 ms, 18.5-44.6% at 800 ms, 22-49% at 1 s, 34-61% at 2 s |
| *calibration* | §3.7's estimator | each block's last touch recorded; per candidate, `1 - U/B` for `U` evictions since the later of its deepest believed block's last touch and `now - Δ`, and `B` the resident blocks; binned against whether the believed prefix was truly resident | §1.7's table; the `V = 0` bin realises 1.000 in every configuration |
| *tokens* | output length in the score | the engine terms fed the mean, the p90 or `max_tokens` in place of `req.tokens`, every charge exact; ledger and engine, rack and region, 250 and 350 req/s | §1.9's table |
| *gossip* | `Gossip`'s two halves | `Gossip` with the KV half of its view replaced by exact reads; rack and region | §1.2's table |
| *silence* | a node going quiet | one node's KV view frozen for a window with the router's dispatches still recorded; load read from the path, from a frozen reading, or from a frozen reading plus dispatches since; single episodes at 25 s, and four episodes at 12, 24, 36 and 48 s on nodes 0-3 | §1.5's tables |

---

## 9. What the build found

Six things the plan did not anticipate, in the order they were found.

### 9.1 The truth view was reading the belief

`plan(View::Truth)`, `placement_terms(View::Truth)` and the oracle's realized cost priced missing blocks
through `Telemetry::local_ns`, and read displacement and engine load through the same `Telemetry` the belief
was attached to. That put the belief inside `m_t` and inside `R(d)`: the `belief` gap would have been the
difference between a belief and a belief. The exact-belief gate cannot see it -- belief and truth are the
same there -- so it was found by reading every `self.telemetry(` call site against §1.11's table, not by a
test failing. `local_ns` and `local_run_ns` now take the view, `Machine::telemetry_in` returns a plain
`Telemetry` for `View::Truth`, and `the_truth_view_prices_from_the_truth_while_the_belief_view_keeps_believing`
pins it. Any future engine-allocated read has to be added to that split or the oracle stops being an
oracle.

### 9.2 The gate needs a fixture that churns every tier, and a belief that can refuse to call itself exact

The first gate fixture used `engine_machine`'s 8 GiB spill tier, which never evicts in 2,500 requests, so a
mutation that ignored spill-tier removals passed the Unified and Query rows and failed only in the Gossip
row at request 2,009. The gate now runs on a machine with a 64 MiB partition, 32 MiB offload and 48 MiB
spill; both mutations (ignore spill removals, never reconcile optimistic dispatches) fail it at once. And
`Machine::believes_resident`/`believes_held` `debug_assert` that an exact belief equals the truth on every
read, which is what makes "on every read" literal; tests that deliberately break the belief use an
`Episode` that never starts, so the belief behaves exactly but does not claim to be `Conditions::exact`.

### 9.3 Three instruments are coarser than the plan says

- **Divergence by cause is not built.** Phantom and miss shares are measured, sampled every 16th KV
  decision; splitting them by "not yet due / dropped / silenced / never stored" needs per-batch fates the
  channel does not keep. The miss share is 0.000% in every cell, which says only that every block the
  engine holds was at some point dispatched-to or stored-to a router that heard about it.
- **The migration metric counts the score's own spreading.** "Needless" migrations -- a request placed off the
  node holding its longest believed prefix, where that node truly held it -- are 99.9% of all migrations at
  loss zero (2,960 of 2,964), because the score places off the best-cache node for load reasons on nearly
  half of KV decisions (2,964 of 6,264). So it cannot isolate a threshold's cost, and P9's second
  clause is untested: the churn half is what was measured (36.7-43.3% across section 3, `quantile 0.9`
  within 4.9% of `expected` everywhere).
- **The estimator is binned, not summed.** §2's P2 line about the index's total `1 - P` was not measured.

### 9.4 The certain bin is empty in cadence mode, by construction

Plan §5: "the `V = 0` calibration bin realises exactly 1.0". In cadence mode the open window is always
unknown, so no block is ever certain and the bin is empty; the invariant is non-vacuous only at the gate,
where the exact-belief instrument test pins it (`every prediction certain and resident`). In cadence mode
the same claim is the top bin (`P = 1.0`, unknown steps present), which reads 1.000 realised in every cell.

### 9.5 Runtime and the job runner

`polyphonic belief` runs in 173 s for the loss section at 15k ops and 199 MB. A first attempt to run the
sweeps as a detached shell job was killed (exit 137) and truncated its output, twice, with the same binary
that completes in the foreground and as a properly backgrounded command; that was the job runner, not the
process, and is recorded only so a truncated output is not read as a result.

### 9.6 A review of the build moved three figures, and left four findings standing

An independent review of the implementation found fifteen issues. Eleven are fixed; every figure above
was re-run after the fixes, and outside the three below each is identical to the digit (the loss grid's 100
cells other than churn, gossip, calibration, lag and RequestView all reproduce exactly).

- **Churn was keyed by the tenant, not the session.** It compared consecutive requests sharing the tenant
  prefix's first block -- 24 tenants against 512 sessions -- and counted `FaaS` and service requests. It now
  compares a KV turn with the node its session's previous turn ran on: 36.7-43.3%, not 23.6-25.1% (P9).
- **The silence section had one baseline for every row**, path and `face-value`, so the stream rows
  carried the load source's own cost. Each row now has its own quiet baseline; figures moved by under 0.03pp
  (P4).
- **A fan-out's resume turn was always declared interactive**, even in a throughput session; `Queued` now
  carries the session's objective. Section 8's throughput class gains those turns (P7).

Also fixed, with no published number moved: `resident_value` and `truly_resident` binary-searched a belief
that can have holes and now scan it; a peer without an engine was priced as a certain miss under
`expected`; `stream+dispatch` counted a dispatch before knowing it ran; a node missing from the step array
fell back to a 1 ns step; the replay buffer was kept on channels that cannot lose anything; and the stream
load was computed on every `Telemetry` construction rather than where it is read.

Three findings stand, as limitations of this build rather than defects hidden in its numbers, and a
fourth is resolved:

- **A block evicted and re-dispatched before its eviction's batch lands stays unbelieved** until the next
  window arrives, because `Belief::dispatched` does not re-assert a block the index already holds. It is a
  miss the belief makes for itself, a window long, and grows with lag; whatever it costs is inside P1's
  region row.
- **Under no recovery, an optimistic entry whose window batch is lost is never reconciled.** That is right
  for a block the engine did store and wrong for one it did not (a refused or preempted dispatch), so part
  of the no-recovery column's phantom share is this rather than a lost eviction.
- **`--load stream`'s wait term still reads the engine's true earliest completion** when the reported load
  is at or above the batch limit. Stream-fed load is therefore slightly better informed than a real one;
  P4's stream rows are a lower bound on its cost.
- **Doc comments.** The review flagged doc comments this phase had rewritten to keep them true. They are
  removed, along with every other comment and `allow(..., reason = ...)` string in the crate, in the same
  change: the source carries no comments except `clap` help text (every `--help` screen is byte-identical
  before and after) and the 15 `// SAFETY:` blocks that `undocumented_unsafe_blocks` requires. The two
  pedantic lints that police doc sections, `missing_panics_doc` and `missing_errors_doc`, are allowed in
  `Cargo.toml`, since they only demand documentation. The explanation lives here.

## 10. Verification, as run

- **Byte-identity with `--belief` off**, against the commit before this phase: `residency` (split, unified,
  `--clairvoyant`, `--engine-cache --decode-kv`), `flows` (and `--engine-cache`), `placement` (split, unified,
  `--drain-at`, and with the engine), `volatility`, `ownership` (and `--engine-cache`), each at
  `--ops 3000` and two seeds: 26 outputs, identical after every work item. The baseline was run twice against
  itself first.
- **The stream reproduces the engine**: `the_event_stream_reproduces_every_tier_after_every_request`, on a
  small-tier machine with decode output held, fetch, a shared L2 and a drain; mutation-checked.
- **The gate**: `an_exact_belief_changes_nothing_and_equals_the_truth_after_every_request` (Unified, Query and
  Gossip; costs identical for the first two), plus the per-read `debug_assert`, plus
  `every_scoring_rule_makes_the_same_decisions_on_an_exact_belief`, plus `polyphonic belief`'s section 1 on
  the real cluster in both regimes.
- **The boundary holds**: `an_eviction_without_its_event_changes_no_telemetry_read`.
- **Channel behaviour**: nine unit tests in `belief.rs` (immediate application, optimistic reconciliation,
  lag and cadence, replay repair with in-order application, no-recovery, snapshots, survival arithmetic
  and the pinned-block rule, load semantics, retirement).
- **RequestView**: `the_score_cannot_see_the_exact_output_length_under_observables`; the scoring path
  compiles only against `RequestView`.
- **Instruments**: zero on an exact belief, non-zero on a lossy one, and the certain bin equals its residents.
- `cargo fmt --check`, `cargo clippy --all-targets` and `cargo test` clean; 75 tests. The census build
  still emits 13 warnings: the stream is recorded inside `KvTiers` and adds no entry point that assumes
  allocation authority.
