# Phase 4 -- Belief, not truth: lossy telemetry and risk-adjusted scoring

Implementation plan for Phase 4 of [`owned-and-observed.md`](owned-and-observed.md): the router's
view of the engine-allocated KV cache stops being an in-process read and becomes a belief, fed by
the engine's own step-aligned event stream with its cadence, its lag and its losses; the acquire
term becomes an expectation over `P(resident)` (§3.7), scored at the quantile a request's declared
SLO names; `Control::Gossip`'s stale exact view of engine state is retired; divergence is reported
(§3.6); `P(resident)` is published against realised residency as a calibration curve; and the score
stops reading the exact output length -- §3.1's `RequestView`, which `phase-1.md` §7 and
`phase-2.md` §7 both assigned here.

**Status: planned.** Nothing below is built. §2's predictions are stated before the run, per
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

**P5 -- Retiring `Gossip`'s engine half barely moves the scored arm and removes a sixth of
residency-greedy's crutch.**

§1.2's pre-measurement, as a prediction for the channel at loss zero: `scored + fetch, gossiped`
within 0.1% of 496.3 ms at rack, with its `execution` gap still above 5M ns/decision because what
remains is a stale view of weights; `both, gossiped` within 5% of 819 ms.

- *If right:* every published gossip result is a result about informer caches over owned state,
  and says nothing about engine telemetry.
- *If wrong* (the scored gossiped arm moves by more than 0.5%): the channel's lag or
  reconciliation costs what the exact KV view did not, which P1 will already have flagged.

**P6 -- RequestView costs under 0.3% of mean service, and lands in the model gap.**

§1.9's pre-measurement. Predicted: the A/B on `--observables`, with `--belief` off, moves mean
service by at most 0.3% and p99 by at most 0.5% at every published cluster configuration; `belief`
is unchanged by it to the nanosecond, and `model` absorbs the difference.

- *If wrong* (more than 0.5%): the engine terms are more sensitive than one seed showed, and the
  ceiling §1.9 hands Phase 7 is higher than stated.

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

**P8 -- The chosen node is over-confident relative to the field.**

The argmin selects the candidates whose belief errs high. Predicted: at the same predicted `P`, the
chosen node's realised residency is below the all-candidates curve, by most under `face-value`,
whose selection runs toward phantoms, and least under `quantile`.

- *If wrong* (the two curves agree): acquire rarely decides placements here -- consistent with the
  `decided_by` counts -- and the estimator's calibration matters less than its existence.

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
