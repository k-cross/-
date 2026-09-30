# Phase 6 -- Macro authority: replicas, partitions, prefill and decode, tenancy

Implementation plan for Phase 6 of [`owned-and-observed.md`](owned-and-observed.md): the decisions
§2.4 puts in the provisioning tier, and everything Phase 3 froze. Weights stop being cache entries
a request materialises and become a placement the orchestrator makes on a clock of seconds, with a
load that takes time. A node's KV partition stops being an input and becomes what the placement
leaves. Prefill and decode become roles a replica holds, paired per request and sized per fleet
(§2.5). And tenancy gets the axis `Quota` lacks and the one decision an opaque engine leaves it:
who shares a replica (§3.8).

**Status: planned.** Nothing below is built. §2's predictions are stated before the run, per
`owned-and-observed.md` §7, and like Phase 5's they lean on **pre-measurements**: numbers taken on
an instrumented copy of `a700562`, run outside the repository and not committed. They are labelled
wherever quoted and collected with their configurations in §8. They are reasons to predict, not
results: §4.13 rebuilds each instrument in the repository, and a pre-measurement the built
instrument does not reproduce is reconciled before any prediction resting on it is graded. More of
them than in Phase 5 are emulations of a mechanism this phase builds -- a placement imposed by a
filter on the candidates, a load as a node that answers nothing, prefill time as a stretch on the
decodes admitted after it -- and §8 says where each is cruder than the build.

Four things make this phase unlike Phase 5.

- **Its subject is not in the engine model, and the plan starts there.** A decode step reads one
  model's weights. The published node holds three of the workload's four models and decodes them in
  one batch at one read a step: 92-97% of decodes are admitted with three or four models in flight
  on their node (§1.1). Given a batch per model, the published cluster's service time rises by
  241-279%, and one model per node brings it back to within 0.5% of the published figure. So every
  published cluster number is the number of a fleet that had already been placed, and on the
  published engine a placement is worth under 1.2% -- the phase's first deliverable has nothing to
  measure until the engine is corrected. Prefill is the same kind of omission (§1.10), and so is a
  fan-out agent that reads its parent's KV out of another model (§1.11).
- **Its decisions move service time.** Phases 3 to 5 moved the cluster's mean service by under 1%,
  because decode is 97% of it and none of them touched decode. A placement decides how long a step
  is, and here arms differ by factors.
- **Its decisions take time.** Every cluster run so far is about a simulated minute. A load takes
  seconds, a mix shifts over tens of them, and the price of a placement turns out to be its lateness
  (§1.8), so the runs are four times longer and the clock is part of every result.
- **Two of its three arms are predicted to come out against the design's phrasing.** At the
  published mix and rate the right number of prefill replicas is zero, and what a joint decision
  buys over a list is mostly declining to pair (§1.13). And a noisy neighbour does ten times the
  damage through engine time that it does through anything the published engine charges, which
  moves tenancy's live question from the partition to the router (§1.15).

§1 settles eighteen decisions. Nine are findings about the existing model and documents rather than
about the work ahead: §1.1 (the engine batches across models), §1.2 (a per-request score cannot
find a placement), §1.3 (the published cluster is a fleet of one model), §1.5 (weights are half of
inference stall, and four-fifths of that is the cold start), §1.6 (the regimes that made the
partition bind do not bind on a placed fleet), §1.10 (prefill is a tenth of a node's engine time,
and prefill-ahead's gain changes sign when it is charged), §1.11 (half of all fan-out agents read
another model's KV), §1.14 (nothing is shared across tenants, and §3.8's table says otherwise) and
§1.15 (the neighbour's damage is not eviction).

---

## 1. What has to be settled before the orchestrator places anything

### 1.1 A batch is one model's, and the published node decodes four in one

`residency-ledger.md`, *Serving engines*: "A decode step reads the weights once whatever the batch
size, so a second sequence is nearly free and the sixty-fourth is not." `Machine` holds one
`Engine` per node, and `Machine::execute` admits a decode to it without asking which model it runs.
The workload has four models of two 512 MiB shards each; a node's weights pool holds three of them
at a time (§1.5); and all of them decode in the node's one batch. `step_ns(batch)` charges one
weight read a step for a batch that needs one per model.

The pre-measurement tagged every sequence in flight with its model (§8, *batches*: the `belief`
cluster with the engine allocating, seeds 1 / 2 / 3):

| models in flight on the node when a decode is admitted | published partition | half partition, decode output held |
|---|---|---|
| one or two | 2 / 7 / 5% | 2 / 10 / 6% |
| three | 74 / 62 / 76% | 55 / 39 / 54% |
| four | 23 / 30 / 19% | 43 / 51 / 39% |

It then gave each model on a node a batch of its own and let the batches take turns. A round is one
step per model with a sequence in flight, so with `k` models and `n` sequences in flight a token
costs `k x STEP_BASE_NS + (n - k) x STEP_PER_SEQ_NS`. Taking turns is the favourable reading: a
model with nothing in flight costs nothing, where a fixed slice of the accelerator per resident
model would cost every model its share whether the others were decoding or not.

| mean service, seeds 1 / 2 / 3 | published partition | half partition, decode output held |
|---|---|---|
| one batch per node, as published | 489.5 / 480.7 / 478.8 ms | 491.6 / 482.9 / 481.0 ms |
| a batch per model, the score as it is | +278.7 / +263.4 / +270.3% | +278.0 / +267.9 / +270.8% |
| a batch per model, the score pricing it (§1.2) | +250.0 / +241.1 / +248.0% | +251.0 / +242.2 / +252.4% |

The published engine is exact in two cases: one model per node, and adapters over one shared base,
which is §3.8's LoRA row and has one batch for the reason it has one copy of the weights. The
published workload is neither.

**Chosen: a batch per model behind `--model-batches`, taking turns on a node, off by default.** The
published numbers stay the published engine's; §1.3 says what they are numbers of.

### 1.2 The per-request score cannot find the placement, which is what a provisioning tier is for

The table's last row is the scored argmin with the model inside its `engine` and `congestion`
terms: joining a node is priced at the round the joiner's model makes there, and bringing a model
into flight on a node that is not decoding it charges a whole `STEP_BASE_NS` a token to every
sequence on that node. It buys back 22-29 points of 263-279, and four models are in flight at
76-79% of its admissions. Once every node decodes every model the term is the same at every
candidate and the argmin has nothing to choose on: a node becomes cheap for a model only after the
other models have left it, and no one request can make them leave. This is `residency-ledger.md`'s
*falsification test that fails* a second time -- "a greedy per-request score cannot represent a
policy whose value is the residency it creates" -- and it is the reason §2.4 has a tier above
routing at all.

A placement made once and imposed by a filter on the candidates does find it (§8, *placement*:
weights preloaded, a batch per model, the score pricing it):

| seeds 1 / 2 / 3, published partition | mean service | against lazy weights |
|---|---|---|
| lazy: weights cached per request | 1713.1 / 1639.7 / 1666.6 ms | |
| three models per node, 1 GiB partition | 1398.2 / 1328.0 / 1366.7 ms | -18.4 / -19.0 / -18.0% |
| two per node, 2 GiB | 956.1 / 919.5 / 934.2 ms | -44.2 / -43.9 / -43.9% |
| one per node, 3 GiB | 490.5 / 480.1 / 477.8 ms | -71.4 / -70.7 / -71.3% |

One model per node lands within +0.2 / -0.1 / -0.2% of the published engine's figure. The same
three placements under one batch per node are worth between -0.7% and +0.2% of service against lazy
weights, and -1.1% to -0.3% at half the partition: the weights' own stall and nothing else. So §9's
"result no arm can produce today" does not exist on the published engine, and on the corrected one
it is a factor.

**Chosen: lazy weights with a batch per model are printed as the old model's price, and are never
the baseline a planner is credited against.** No one deploys them. A planner's references are a
placement made once and a clairvoyant one (§1.9).

### 1.3 The published cluster is a fleet of one model, and routing is worth the replicas it chooses among

If a placed fleet reproduces the published figure, the published figure is a placed fleet's -- of
which shape? Pre-measured on eight nodes (§8, *routing*), `scored + fetch` against `hash only`:

| mean service, seeds 1 / 2 / 3 | 500 req/s | 700 req/s |
|---|---|---|
| any node, any model, one batch per node (published engine) | -16.4 / -32.1 / -24.8% | -58.9 / -65.7 / -62.3% |
| one model, eight replicas | -15.3 / -31.3 / -23.8% | -58.8 / -65.7 / -62.2% |
| four models, two replicas each | -2.3 / -2.9 / -2.6% | -21.8 / -22.9 / -22.3% |

The first two rows agree to about a point, and their mean service to 0.5%: the published engine on
`N` nodes is a fleet of `N` replicas of one model. Nothing published about routing is wrong for
that fleet. What four models in the workload added to it was weight churn worth half a percent
(§1.5) and agents that read KV across models (§1.11). Give the four models two replicas each and
the score's lead over hashing falls from 16-32% to 2-3% at the published rate, because the argmin
now chooses between two nodes; `code-review`'s "prefix-cache-aware routing is inert" was the
one-replica end of the same line.

The third row has a second reading. At 700 req/s the placed fleet serves in 719.5 / 659.2 /
655.9 ms where the published engine serves in 576.7 / 591.6 / 597.9: a model's decode slots are its
own replicas', and a full batch on one model cannot borrow an empty one on another. The knee
arrives per model, not per fleet, and no router moves it.

**Chosen: the fleet has more replicas than models, every figure travels with its replicas per
model, and the published cluster results are not re-run.** They hold for the fleet they describe.
Which of them a fleet of several models keeps is a table for §4.15, not a re-publication.

### 1.4 The unit is a replica, and it is one allocation made three times

§9: "a node's partition budget and its resident model set are one allocation made twice." §1.1
adds a third time. A **replica** is one model loaded on one node's accelerator, and loading it
fixes:

| | what it is | where it comes from |
|---|---|---|
| weights | the model's bytes, resident until it is unloaded | declared: the catalogue's size |
| partition | the node's HBM less the weights | derived, and asserted at every load |
| step | one read of those weights | the size over the accelerator's width |

`step = STEP_BASE_NS x (bytes / 1 GiB) / width`, with the published node at width 1 and the
published model at 1 GiB, so the published engine is the case its constants were chosen for. §3.8
already says a partition is physically an engine. What can share a node is then one of three
things, and the pre-measurement has priced the second:

- **one replica per node**, the unit here;
- **several engines taking turns on one accelerator**: two per node cost +91 to +96% of service
  against one (§1.2's table), whenever both are decoding;
- **a fixed slice each**: the same hardware as `k` nodes with a `k`-th of the HBM and a `k`-th of
  the width, which is a topology and not a mechanism.

**Chosen: one replica per node. A sliced host is `k` smaller nodes in `Topology`; engines that take
turns exist only as §1.2's lazy arm.** A planner that could put two cold models on one accelerator
is named in §7 and not built: it needs a partition per engine inside `Hierarchy`, and the case for
it is a fleet with more models than nodes, which this phase does not run.

### 1.5 Weights leave the ledger: what the cache did, and what replaces a refusal

What per-request weight caching does today, on the `belief` cluster with the engine allocating (§8,
*models*; seeds 1 / 2 / 3):

| | published partition | half partition, decode output held |
|---|---|---|
| samples with exactly three whole models resident on a node | 97.5 / 92.6 / 94.9% | 97.5 / 89.5 / 93.3% |
| shard reads that hit | 98.7 / 98.4 / 98.9% | 96.8 / 96.3 / 97.6% |
| loads into HBM, per simulated second | 3.9 / 4.5 / 3.2 | 9.0 / 10.2 / 6.8 |
| of those, promoted from the node's own DDR | 206 / 238 / 164 of 236 / 270 / 194 | 508 / 578 / 376 of 538 / 610 / 406 |
| KV dispatches that pay for weights | 1.5 / 1.7 / 1.2% | 3.3 / 3.8 / 2.5% |
| stall per KV dispatch, weights and chain | 5.0 / 5.2 / 5.0 and 5.1 / 5.3 / 5.2 ms | 5.8 / 6.2 / 5.6 and 9.0 / 10.6 / 9.8 ms |

A node holds three models and swaps the fourth in from its own DDR at 21 ms a shard, three to ten
times a second across the cluster. Weights are half of inference stall, and four-fifths of that is
the eight cold rebuilds at the start of the run -- 32 s of 40 on seed 1. Were each load a placement
decision, four nodes would write the record tier three to ten times a second; §1.9 counts what a
planner writes instead.

With `--fleet` on:

- **`WeightShard` leaves the HBM `TierPool`.** A replica's weights are resident until the
  orchestrator unloads it. `Machine::plan` stops pricing `requires`, `run_here` stops materialising
  it, and the dynamic census's `WeightShard` row reads zero, as `KvBlock`'s did in Phase 3.
- **A request names a model, and the router filters on it.** `requires` resolves through the
  catalogue to a model, and the candidates are the replicas serving it, the way `can_decode`
  already filters. A replica still loading is a candidate priced at the rest of its load, since a
  wait is a price here and not a refusal (`phase-3.md` §1.5).
- **A model with no replica is refused, and counted apart.** `unplaced` joins `refused`, `refused
  by router` and `preempted`, never summed with them, and goodput is not comparable across the bit,
  for Phase 3's reason.
- **`own::authority` changes two cells.** A model's bytes in host DDR or on `NVMe` are a file in a
  node agent's cache that no engine allocates, so `(WeightShard, Ddr | Nvme, Allocation)` becomes
  `Orchestrator` -- the first cells in which the tier axis discriminates on the allocation
  question, which is what `phase-1.md` §1.2 kept the axis for. `(WeightShard, Hbm, Allocation)`
  stays `Engine`; which model a node loads is that node's capacity question, already the
  orchestrator's.
- **A load takes time.** Its duration is the model's declared start time plus moving the bytes from
  the cheapest copy at the existing prices: host DDR, `NVMe`, a peer, or a cold pull at
  `WEIGHT_NS`. At the simulator's byte scale the move is under half a second from anywhere but
  cold, so the declared start time is the constant that matters, and §1.8 sweeps it.

**Chosen: as listed.** The catalogue is the interface §8 allows and nothing more: size, start time,
context window.

### 1.6 The partition follows the replica, and its size is second-order

With one model per node the partition is `HBM - weights`. The pre-measurement compared that with
the published grant under one placement (§8, *placement*; one model per node, seeds 1 / 2 / 3):

| | a 3 GiB partition | the published grant |
|---|---|---|
| published partition: mean service | 490.5 / 480.1 / 477.8 ms | 491.0 / 480.7 / 478.4 ms, at 1 GiB |
| half partition, decode output held: mean service | 490.2 / 479.9 / 477.6 ms | 492.4 / 482.2 / 479.8 ms, at 0.5 GiB |
| half partition, decode output held: preempted | none | 8.1 / 9.1 / 8.3% of requests |

Three times the partition is worth 0.1% of service where nothing is preempted and 0.4-0.5% where a
twelfth of requests were. Two consequences.

- **The regimes Phases 3 to 5 used to make the partition bind do not bind on a placed fleet.** Half
  the published grant with decode output held is 0.5 GiB; a node serving one model has 3 GiB. The
  fleet needs regimes of its own, and they come from the catalogue: a 2 GiB model leaves 2 GiB of
  a node's 4, and a 1 GiB model leaves 1 GiB of a half-width node's 2.
- **The context window is what the partition's size decides.** A sequence has to fit, so a replica
  declares `min(model window, partition)` and the router does not send it a longer one: §8's
  capability gate, which is a fact a replica can state about itself.

The other sizing decision is the one `phase-3.md` §1.6 handed over: how much host DDR the
connector's offload gets. Swept from nothing to 1.6 GiB a node at the published partition (§8,
*offload*; seeds 1 / 2 / 3):

| across the sweep | 8 GiB DDR per node | 4 GiB DDR per node |
|---|---|---|
| mean service | -0.12 / -0.12 / -0.10% | within 0.1% |
| stall per request | about 0.5 ms lower at the top, on every seed | within 0.5 ms, either sign |
| function warm rate | within 2 points | 63 -> 40 / 60 -> 29 / 56 -> 32% |

Inference does not notice the grant. Where DDR is tight the function warm pool does, by 23-31
points.

**Chosen: under `--fleet` the partition is derived and asserted, never configured. The offload
grant becomes a provisioning parameter (`--kv-offload`) with its sweep in the report, and no
controller is built for it.** What the sweep says is a rule rather than a policy: give the DDR
back.

### 1.7 The fleet, the mix and the clock

Three things the `distributed` cluster cannot express.

- **Replicas to choose among.** Four models on four nodes is one replica each: no routing decision
  (§1.3), no replica to give to prefill, none to set aside for a tenant, and no placement but one.
- **A mix that shifts.** Demand by model is 27-30 / 25-27 / 23-25 / 21-23% and static: tenants map
  to models by `tenant % 4`, and every phase draws the same tenants. The workload's phases shift
  the *class* mix, which moves how much decode there is and not whose.
- **Time.** 15,000 requests at 250 req/s is a simulated minute. A load takes seconds, and a shift
  worth following lasts tens of them.

**Chosen: the fleet is eight of the published node -- 4 GiB HBM, 8 GiB DDR and 16 GiB `NVMe` each,
at rack distance, no control crossing charged -- serving the four published models at two rates:
500 req/s, which is the published rate per node, and 300.** The second was added after the first
pre-measurement: at the published rate any split of the fleet puts some model or some role at its
knee (§1.13), and a result that exists only with headroom has to say so. Sections with a clock run
120,000 requests, which is four simulated minutes at the higher rate.

The generator gains three bits. Each is its own seeded stream or a deterministic function of the
request, so the published trace is unchanged with them off.

- `--model-mix`: the share of session and flow demand each model takes in each phase. Sessions
  belong to a tenant and tenants to a model, so a shift is other tenants becoming busy, never a
  tenant changing model.
- `--fresh`: a stream of requests with a long prompt nothing holds and a short output --
  `taxo.md`'s fixed-pipeline and extraction shape, and the one the prefill-to-decode ratio moves on
  (§1.13).
- `--neighbour`: one tenant sending such requests in a window (§1.15).

`taxo.md`'s patterns as presets, the RAG class and predicted flows stay Phase 7's.

### 1.8 A placement is priced in lateness

Pre-measured on the fleet with session and flow demand at 55 / 25 / 12 / 8% by model, rotating one
model each phase, and the class mix held flat (§8, *shift*). Decode tokens come out at 50 / 25 / 14
/ 10%, which is four, two, one and one replicas, and each boundary moves three nodes.

| 8 nodes, 500 req/s, 240 s; seeds 1 / 2 / 3 | mean service | decodes arriving at a full batch |
|---|---|---|
| any node, any model, one batch per node (published engine) | 455.9 / 458.1 / 457.2 ms | none |
| lazy weights, a batch per model | 965.9 / 1124.2 / 1163.9 ms | under 0.1% |
| placed once, for the first phase | 6459.5 / 6867.2 / 6542.1 ms | 49 / 50 / 50% |
| placed once, for the run's mean mix | 1798.1 / 1850.0 / 1827.1 ms | 51 / 51 / 51% |
| replaced at each boundary, at once | 457.7 / 460.4 / 459.2 ms | under 0.1% |

Following the mix gets the published engine's figure back to within half a percent. Not following
it is a collapse rather than a cost: a model with half the demand on two replicas is past its knee
for the rest of the run (§1.3). Against the replacement made at once:

| seeds 1 / 2 / 3 | mean service |
|---|---|
| a load of 2 s | -0.1 / -0.1 / 0.0% |
| a load of 8 s | +4.3 / +5.2 / +6.8% |
| a load of 30 s | +80.9 / +88.5 / +92.2% |
| 10 s late, then a load of 8 s | +27.8 / +30.4 / +32.9% |
| 30 s late, then a load of 8 s | +130.1 / +144.2 / +146.4% |

Eighteen seconds without the right placement costs 30%, and thirty cost 80-90%, against phases of
48-72 s. This is §1's *Two control loops, two clocks* with a number on the gap, and it says what a
planner is: a bound on lateness.

The replacement made at once is an emulation's convenience -- it knows where the boundaries are.
**Chosen: it is printed as the ceiling, a clairvoyant placement, and every planner arm is reported
as regret against it** (§1.18).

### 1.9 The planner: what it reads, how it decides, what it writes

**It reads aggregates.** Per model over the last interval: decode tokens admitted, prefill work,
sequences in flight, and requests unplaced or arriving at a full batch. These are sums over the
router's own dispatches -- the `Metrics { interval }` half of §1's telemetry, nothing per block --
and a `FleetView` type carries them, so the planner cannot name a request, a block or the trace.

**It decides in nanoseconds.** An allocation is replicas per model, and its predicted cost is the
fleet's token time under the engine's own step: for a model with `D` tokens a second of demand on
`R` replicas, the sequences in flight per replica solve `L = (D / R) x step(L)`, the cost is `D x
step(L)`, and a full batch is priced as the queue it grows. That cost is separable and convex in
`R`, so handing each next node to the model whose token time falls most is optimal. Roles (§1.13)
are a second pass with the same function. Nothing in it is tuned.

**It moves when the loss already suffered pays for the move.** A move is priced in the same
nanoseconds: the demand its node stops serving for the load, plus the rebuild of what its
partition held, discounted by the partition's observed hit share. Each interval the planner accrues
`cost(current) - cost(best)`, and when the accrued loss reaches the move's cost, it moves. This is
the rent-or-buy rule. It is chosen because it needs no forecast of how long a mix will last --
which would be an inferred quantity, owing §1 a confidence and a deadline -- and because with two
placements to choose between it never pays more than twice what knowing would have. With more than
two it is a rule with that shape, and `oracle` measures the gap.

| `--planner` | moves | what it is |
|---|---|---|
| `once` | never, after the first interval | the reference a planner has to beat |
| `follow` | when the accrued loss covers the move | the arm |
| `eager` | whenever the best allocation changes | what the rule is for |
| `oracle` | at each shift, read from the generator, to what the next phase's demand wants | a ceiling, never an arm |

**It writes the record tier.** A placement, a role and a tenant set are rows in §8's system of
record, and the simulator models no store, so each write is counted: writes per simulated second by
kind, beside `decisions` and `dispatches`. This is the part of Phase 10's count §9 assigns here.

**Chosen: as above, with the interval and the start time declared, swept and printed** -- 5 s and
8 s by default, 1-15 s and 2-30 s swept.

### 1.10 Prefill is a tenth of a node's engine time, and the engine charges nothing for it

`phase-5.md` §1.3 named the gap: a rebuild "is charged as latency to the request that needs it ...
never as step time". Its size, on the `belief` cluster (§8, *prefill*; seeds 1 / 2 / 3):

| | published partition | half partition, decode output held |
|---|---|---|
| prefill work per node, as a share of wall time | 11.6 / 11.9 / 11.5% | 23.0 / 26.5 / 24.1% |
| prefill's share of engine time, decode at its batch share | 9.9 / 10.2 / 9.8% | 17.9 / 20.1 / 18.5% |
| KV dispatches with 10 ms or more of prefill | 11.9 / 12.4 / 12.2% | 29.6 / 31.7 / 29.5% |
| their share of all prefill work | 40.8 / 41.1 / 41.2% | 78.5 / 82.7 / 80.8% |

At the published partition a chat turn prefills 3.0-3.1 ms at the mean and 16-18 ms at p99, and
chat turns carry 45-47% of the work; a flow's downstream prefills 6.3-6.6 ms and carries a third; a
fan-out's agents carry 15%.

`Engine::step_ns` is additive in tokens: a step costs its base plus `STEP_PER_SEQ_NS` for each
sequence it advances. A prefilled token is a token in a step. So the consistent model is that
prefill work lengthens the steps it rides in, and every sequence in flight on that engine waits for
it. Emulated as a stretch of `1 / (1 - rho)` on each decode, `rho` being the node's prefill work
over the trailing second:

| mean service against prefill free, seeds 1 / 2 / 3 | published partition | half partition, decode output held |
|---|---|---|
| the `belief` cluster, one batch per node | +17.5 / +17.1 / +17.4% | +68.0 / +80.4 / +71.8% |
| its preempted requests | none | 23.4 / 25.6 / 23.5%, from 10.5 / 12.4 / 11.1% |
| §1.7's fleet, two replicas per model | +12.7 / +11.9 / +11.7% | |

The right-hand column is a spiral and not a sum: a stretched decode holds its KV longer, more
sequences are preempted, a preempted sequence is a rebuild, and a rebuild is prefill. A window of
250 ms or 4 s in place of the second moves the left-hand column by 1.5 points either way, and the
right-hand one by more (§8).

It reaches one published result. `phase-5.md` found prefill-ahead's work to be 1.5-1.7 times the
stall it saved, and charged that work to no engine. Charged to the engine it lands on (§8,
*ahead*; seeds 1 / 2 / 3):

| prefill-ahead against none | prefill free | prefill takes engine time |
|---|---|---|
| published partition: mean service | -0.11 / -0.12 / -0.11% | +2.3 / +2.1 / +2.3% |
| published partition: the flow downstream's stall | -64 / -64 / -63% | -53 / -56 / -52% |
| published partition: landed where the downstream was placed | 51 / 48 / 47% | 35 / 39 / 36% |
| half partition, decode output held: mean service | -0.13 / -0.04 / -0.16% | +4.5 / +2.0 / +5.1% |
| half partition, decode output held: the flow downstream's stall | -46 / -38 / -52% | +3 / +4 / +7% |

**Chosen: `--prefill-time`, a bit of its own, with the additive step as the model and one constant
beside it: the prefill a step carries for free, zero by default and swept.** At zero it is the
table above. At one millisecond a step it is the published engine again, since 1 ms in an 8.4 ms
step is the whole 11.6%. §7's rule applies in full: every result below that depends on the bit is
published across that range. The score gains the term the step implies -- prefilling at a node
delays what is in flight there by `in flight x work`, the congestion toll's shape -- and only under
the bit.

### 1.11 KV is keyed by a model, and a fan-out's agents are not

`residency-ledger.md`, *One object*: `KvBlock id = H(model, parent_block_id, token_span)`. The
generator's ids carry no model, because a tenant has one. A fan-out breaks that: `Workload::fanout`
gives each agent its parent's chain and a model drawn as `(home + zipf(4, 2.0)) % 4`, so an agent
on another model starts from its parent's blocks.

Pre-measured (§8, *keyed*; seeds 1 / 2 / 3): 48.0 / 49.1 / 48.9% of fan-out agents run on a model
other than their parent's, and they find 74.5 / 73.3 / 73.4% of the parent-prefix blocks they read
resident -- KV another model wrote. Keying an agent's copy of its parent's context by the agent's
model raises prefill work by 31.9 / 30.7 / 33.7% (18.7% at half the partition, seed 1), and mean
service by 0.05% where prefill is free and by 7-8 points more where it is not (+24.4 / +25.3 /
+24.9% against +17.5 / +17.1 / +17.4%).

On a placed fleet the cheat is not there to keep: a replica of one model never holds another's
blocks, and a peer fetch that found them would be shipping KV between models.

**Chosen: `--model-keyed`, a generator bit, and `--fleet` requires it.** A fan-out that crosses
models re-encodes its parent's context once per model, siblings on the same model share it, and the
cost belongs to the multi-agent shape rather than to an accident of the ids.

### 1.12 A pair is a gang of two with a direction, and the prefiller needs the prefix

§2.5 fixes the shape: both endpoints chosen before dispatch, the transfer priced by `Topology`, the
handshake peer to peer. With `--prefill-time` on, a replica's role is `Both`, `Prefill` or
`Decode`, and one request's options are:

| | the request waits for | everyone else pays |
|---|---|---|
| prefill at its decoder | the work `W` | `W` for each sequence in flight there |
| pair with a prefiller `p` | `p`'s queue, the work at `p`, the transfer of what the decoder lacks | the work at `p`, to whatever queues behind it |

Three things follow from the second row that §2.5 does not say.

- **The work at `p` is not `W`.** A prefill needs its prefix. A session's next turn is four new
  blocks at the decoder that ran the last one, and the whole chain at a prefiller that did not. So
  a prefiller needs a partition and a history, the prefill tier duplicates the cache as well as the
  compute, and picking a prefiller in turn is picking one that has to start over.
- **Not pairing is an option of the same argmin.** "Deciding to disaggregate is admission" (§2.1).
  A rule with no unpaired option pairs a 1.6 ms turn.
- **A prefiller has a queue, not a batch.** It is FLOPs-bound: work waits behind work, and past one
  second of work a second it does not come back.

The emulation (§8, *pairing*) chooses the decoder by the scored argmin as today, then a pair or
not; it runs the prefill on `p` and lets the existing peer fetch ship the blocks. Three rules:

| rule | pairs | picks the prefiller |
|---|---|---|
| `list` | every prefill, or every one over a threshold | in turn |
| `independent` | every one over a threshold | by the prefiller's own queue and residency |
| `joint` | when `queue + work + toll + transfer` at the best prefiller is under `W x (1 + in flight)` at the decoder | by that sum |

`independent` is what §2.1 describes -- two schedulers, each choosing well on its own view -- and
was not pre-measured; `list` is its floor.

**Chosen: all three behind `--pairing`, `joint` as one argmin over pairs and the unpaired option,
and coupled % as the share of decisions on which `joint` and `independent` differ** -- §3.4's
instrument, computed read-only at each decision as the regret oracle is.

### 1.13 The ratio moves with the mix and with the load, and at the published mix and rate it is zero

Pre-measured on eight replicas of one model, so that only the split varies (§8, *pairing*; seeds 1
/ 2 / 3; mean service against eight aggregated replicas, the best `joint` row in bold):

| 300 req/s | published mix | plus fresh 64-block prompts, 0.15 a request |
|---|---|---|
| eight aggregated: decode stretched by | 5.5 / 5.7 / 5.5% | 22.0 / 22.2 / 21.8% |
| `joint`, 1 prefill : 7 decode | **-2.1 / -2.1 / -2.4%** | -1.4 / -1.1 / -1.7% |
| `joint`, 2 : 6 | -1.6 / -1.9 / -1.7% | -12.0 / -11.3 / -12.0% |
| `joint`, 3 : 5 | +1.2 / +1.0 / +1.2% | **-17.0 / -17.1 / -16.8%** |
| `joint`, 4 : 4 | +6.2 / +6.2 / +6.3% | -10.4 / -6.2 / -5.1% |
| `list`, every prefill, 1 : 7 | +195 / +268 / +198% | +5,578 / +5,530 / +5,488% |
| `list`, every prefill, 2 : 6 | -0.9 / -0.6 / -0.9% | +753 / +759 / +747% |
| `list`, every prefill, 3 : 5 | +1.7 / +1.5 / +1.7% | -5.2 / +12.4 / -6.2% |
| `list`, over 10 ms, 3 : 5 | +7.3 / +7.2 / +7.4% | -12.0 / -11.4 / -11.4% |

| 500 req/s, the published rate per node | published mix | plus fresh prompts, 0.15 a request |
|---|---|---|
| eight aggregated: decode stretched by | 9.3 / 9.7 / 9.3% | 31.7 / 31.4 / 31.9%, batches at 60 of 64 |
| `joint`, 1 : 7 | +3.4 / +4.1 / +3.3% | +2.1 / +4.3 / +2.0% |
| `joint`, 2 : 6 | +2.3 / +5.9 / +6.3% | +9.3 / +12.7 / +9.7% |
| `joint`, 3 : 5 | +93 / +95 / +95% | +127 / +124 / +131% |

Four readings.

- **At the published mix the right number of prefill replicas is zero at the published rate, and
  one in eight with headroom.** What a pair saves is the stretch, 5-10% of decode. What it costs is
  a replica's decode slots, and a prefiller doing about 1.7 times the prefill work the aggregated
  fleet did, because it has to hold the sessions as well (§1.12).
- **Fresh prompts move it to three in eight, for 17%.** There the work at a prefiller is the work
  at the decoder, nothing is duplicated, and a prefill that would have delayed thirty sequences
  delays none.
- **A ratio set for one mix is wrong for the other, in the direction §9 says.** Three in eight
  costs the agent mix 1% with headroom and doubles its service time without; one in eight leaves
  the fresh mix fifteen points short.
- **What a list costs is pairing what should not be paired.** Its prefiller is past saturation at
  every ratio `joint` prefers, and the ratio has to be over-provisioned before a list is safe.
  `joint` at one in seven pairs 80% of prefills on the published mix and 21% on the fresh one: it
  declines.

**Chosen: the ratio is the planner's second pass (§1.9), zero is an answer it can give, and the
prefill-and-decode arm is run at both rates and both mixes.** A ratio that is right at one load and
wrong at the other is the result, and quoting it without its load is quoting a capacity decision.

### 1.14 Nothing is shared across tenants, and LRU is already unfair

§3.8's table: "cross-tenant prefix sharing: **yes**, the highest-value hit in this workload --
teams share system prompts and company context, which is why `work.rs` gives each tenant a shared
prefix." `work.rs` roots each tenant's prefix at its own block. The prefix is shared by one
tenant's sessions and by no other tenant.

Pre-measured on the `belief` cluster (§8, *tenants*; seeds 1 / 2 / 3, published partition):

| | |
|---|---|
| touches of a KV block first touched by another tenant or function | 0 of 384,159 / 413,463 / 405,143 |
| tenant-prefix blocks: share of chain reads, hit rate, share of all hits | 52 / 57 / 56%, 93 / 91 / 93%, 80 / 82 / 82% |
| GPU evictions caused by another owner's request | 97.1 / 97.7 / 97.5% |
| hit rate of the busiest tenant | 76 / 75 / 80% |
| hit rate of the quietest twelve of twenty-four | 54 / 63 / 59%; 40% against 70% at half the partition, seed 1 |

So the highest-value hit is a tenant's own, the first of §3.8's three prices is zero on this
workload, and the fairness §3.8 says a shared partition lacks is already missing: 12-22 points of
hit rate separate the ends of the tenant distribution. By size it is small where prefill is free --
a quiet tenant's extra misses are about 3 ms a request, 0.3% of its service, which is **arithmetic
on the table and not a run**.

**Chosen: a `--shared-prefix` bit gives the first price something to be -- a prefix per model
under every tenant's -- and the published workload is reported with its zero.** §3.8's table is
corrected at publication (§4.15).

### 1.15 The neighbour's damage is engine time, not cache

§3.8's noisy neighbour "floods a shared engine with unique prefixes" and "evicts another team's
warm blocks". Pre-measured on eight replicas of one model, with one tenant sending fresh 64-block
prompts for a fifth of the run at 0.1 and 0.2 of the request rate (§8, *neighbour*; seeds 1 / 2 /
3; the other tenants' mean service inside the window, against no burst):

| | prefill free | prefill takes engine time |
|---|---|---|
| shared, burst at 0.1 | +3.6 / +3.6 / +3.5% | +35.1 / +32.5 / +32.8%; p99 +48 / +41 / +46% |
| shared, burst at 0.2 | +7.4 / +7.1 / +7.2% | +79.8 / +79.6 / +80.1%; p99 +74 / +76 / +77% |

On the published engine the neighbour costs the others what its decodes add to their batches. On
the corrected one it costs ten times that, through the steps its prefills lengthen. The cache is
not where the damage is, and that moves the question: eviction happens inside the engine, where the
orchestrator has no say, but every prefill is a dispatch, and the router makes those.

### 1.16 Three instruments, and the ledger's first result one axis over

| instrument | what it is | pre-measured cost to the others, seeds 1 / 2 / 3, prefill taking engine time |
|---|---|---|
| a second engine on the node | §3.8's "partition per tenant", literally | +91 to +96% whenever both decode (§1.4) |
| a replica set | one of eight replicas the neighbour's alone | +4.9 / +4.4 / +4.7% always, burst or none; two of eight, +12.5 / +12.4 / +12.8% |
| a quota at the router | a limit on the prefill work the neighbour is admitted, the excess refused | in the burst: +5.4 / +5.2 / +5.4% at 0.25 engine-seconds a second, +11.6 / +11.4 / +11.6% at 0.5, +26.7 / +26.3 / +26.5% at 1.0 |

And what each costs the neighbour at a burst of 0.1: its own replica serves it in 7.1 / 7.9 / 6.6 s
at the mean and two replicas in 2.8 / 2.9 / 2.8 s, while the quota refuses 80 / 79 / 79% of it at
0.25, 60 / 58 / 57% at 0.5 and 20 / 16 / 15% at 1.0.

§9 asks for isolation's cost on §3.8's three prices. At a replica's grain they are: **prefix
hits**, none lost on this workload, and none at any grain where a replica holds one copy whoever it
serves (§1.14); **batch width**, the others' slots on seven replicas in place of eight, which is
the +4.4-4.9% above; and **weights**, one replica's HBM -- 1 GiB of weights and 3 GiB of partition,
an eighth of the fleet's -- held for one tenant whether it is sending or not. Inside a node the
second and third are the second engine's +91-96% and a third of the node's partition.

Three readings.

- **A partition inside a node is the worst instrument on offer.** It isolates the cache and makes
  the compute interference worse: two engines are two weight reads a round.
- **A replica set and a quota charge at different times.** The set costs the others a replica's
  decode slots always; the quota costs them the neighbour's admitted work only while it bursts. At
  the same allowance -- one replica, or one engine-second a second -- the set protects better
  inside the burst (+4.9% against +26.7%) and worse outside it (+4.9% against nothing), so there is
  a crossover in how often the neighbour bursts, near a fifth of the time at these constants.
  **Arithmetic on the table.**
- **This is `hard-partition`, `soft-floor` and `no-floor` again**, with a replica set for the
  partition, the quota for the floor and the shared fleet for open sharing -- §3.8's
  "requests-versus-limits trade one axis over". With one difference the prediction has to carry:
  idle bytes cost nothing to lend, and engine time is never idle, so a tenant borrowing above its
  floor always costs someone.

**Chosen: replica sets and the router quota are the two arms, the shared fleet is the reference,
and a tenant-aware block manager is a ceiling and never an arm** -- a per-tenant eviction floor in
`EngineCache`, printed as what §3.8's promotion-tier-2 ask would be worth, as the clairvoyant block
manager was.

### 1.17 The tenant axis on `Quota`

§3.8: `band`, `floor` and `limit` are per class, and soft tenancy needs them per tenant. What a
tenant's quota can bind on follows `own::authority`, as retention did in Phase 5:

| resource | who meters it | a tenant's quota is | over it |
|---|---|---|---|
| host DDR: `Snapshot`, `ServiceHeap` | the ledger | a floor and a limit per tenant beside the class's | the ledger takes the tenant's own first |
| decode slots on a replica set | the router, which admits every sequence | a floor and a limit in sequences in flight | the router refuses |
| prefill work on a replica set | the router, under `--prefill-time` | a floor and a limit in engine-seconds a second | the router refuses |
| KV blocks inside a partition | the engine | not expressible | -- |

`limit = floor + slack`, and `hard` caps a tenant at its floor, as `Quota::from_split` does for
classes; a replica set is the hard arm of the same type. The last row is what cession cost, and it
is the row the ceiling prices.

A request carries its tenant as it carries its `slo`: declared, because a caller's identity is a
fact the record holds and not something to infer. Refusal is the only enforcement until Phase 9
adds a queue and a cancel.

**Chosen: as tabulated.**

### 1.18 Where the effects land, and how each is graded

- **The engine corrections** (`--model-batches`, `--prefill-time`, `--model-keyed`) are A/Bs on
  their own bit at a fixed everything else, as Phase 3's was. They price a model; they are not
  decisions.
- **Placements and ratios** change what exists, so they land in realized service and not in the
  regret decomposition's gaps. Each planner arm is graded as regret against `oracle`, which no
  deployment has, and against `once`, which every deployment has.
- **Pairing** is a routing decision and keeps the four gaps; its coupled % is §1.12's.
- **Tenancy** is graded per tenant and at p99, inside the burst and outside it, with the
  neighbour's own outcome beside the others': a mean over everyone cannot see a cost that lands on
  someone else (`phase-3.md` §4.11).
- Every cell is three seeds, a range and a count of signs, and carries its replicas per model and
  its rate.

---

## 2. Predictions, stated first

`owned-and-observed.md` §7's rule. Nine predictions, each attached to a claim it would rewrite.
Where a pre-measurement stands behind one, §8 says how it was taken.

**P1 -- A batch per model costs the published cluster two and a half times its service time, and
one model per node gives it back.**

`--model-batches` on the `belief` cluster: mean service up 240-280% on every seed and in both
regimes with the score unchanged, and within 35 points of that with the score pricing the batch
(pre-measured 22-29), four models in flight at more than 70% of its admissions. One model per node:
within 0.5% of the published engine's figure. Under one batch per node, any placement within 1.2%
of lazy weights.

- *If right:* every published cluster number is that of a fleet already placed,
  `residency-ledger.md` says so where it introduces the engine, and the provisioning tier has its
  reason: a score cannot create the placement it would profit from.
- *If wrong* (the score pricing the batch lands within 20% of one model per node): per-request
  pricing can find a placement, and the case for a tier above routing rests on load time alone.

**P2 -- Routing's lead is a count of replicas.**

`scored + fetch` over `hash only` on eight nodes at 500 req/s: 15-32% on the published engine and
on eight replicas of one model, within 2 points of each other on each seed; 2-3% with two replicas
a model. At 700 req/s: 59-66% and 21-23%, with the placed fleet 9-25% slower than the published
engine.

- *If right:* Phases 2 to 5's cluster results are restated as results about `N` replicas of one
  model, and the knee is a per-model quantity a planner has to leave room under.
- *If wrong* (two replicas a model keep the pooled lead): something other than the candidate count
  carries the score's lead, and the first suspect is fetch between a model's two replicas.

**P3 -- Following the mix recovers the pooled figure, and lateness is the whole price.**

On the rotating mix, 8 nodes at 500 req/s: `oracle` with no start time within 1% of the published
engine; `once` above 5 s of mean service with 45-55% of decodes arriving at a full batch. A start
time of 2 / 8 / 30 s: within 0.5%, +3-8% and +75-95% of `oracle` with none. `follow` at a 5 s
interval and an 8 s start: 9-12 moves, and +10% to +35% over that `oracle` -- it cannot see a
shift before an interval has passed, which puts it in the ten-seconds-late pre-measurement's
neighbourhood -- and more than 85% under `once`. `eager` makes more moves than `follow` and is not
faster. With sizes of 0.5 / 1 / 1 / 2 GiB at equal token demand the planner's replicas are 1 / 2 /
2 / 3 of eight -- **arithmetic, not pre-measured**: token time scales with size.

- *If right:* a planner is a bound on lateness, the start time is the number worth asking a serving
  stack for, and §5's "macro-orchestration of weights" has a size and the condition it holds under.
- *If wrong* (`follow` lands past the thirty-seconds-late arm): the rule waits too long, which
  means the accrued loss and the move's cost are not in the same units after all -- the first thing
  to check is the rebuild term.

**P4 -- The partition's size and the offload grant are second-order.**

One model per node at `HBM - weights` against the published grant: within 0.5% of service; at half
the grant with decode output held, preemption from 8-9% of requests to none and service within 1%.
The offload grant from nothing to 1.6 GiB: service within 0.2% at 8 and at 4 GiB of DDR per node;
the function warm rate down 20-32 points at 4 GiB and within 3 at 8.

- *If right:* "partition sizing as a decision" is decided by the replica set, the partition's own
  size matters through the context window it lets a replica declare, and `phase-3.md` §1.6's
  capacity arbitration has a one-sided answer on this workload.
- *If wrong* (the partition's size moves service by more than the replica count's neighbours do):
  in-flight KV binds on the placed fleet, and the regimes §1.6 says are gone are still there.

**P5 -- Prefill, charged, costs the published cluster a sixth of its service time, and the half
partition two-thirds.**

`--prefill-time` with nothing free: +15-19% at the published partition and +60-120% at half with
decode output held, its preemptions roughly doubling; +11-13% on the fleet with two replicas a
model; within 1% of prefill free with 1 ms free a step. Prefill-ahead under the bit: mean service
+2.0-2.5% at the published partition with the flow downstream's stall still down 50-57%; at half,
service +2-5% and the flow's stall no better.

- *If right:* `phase-5.md`'s "the engine lets the orchestrator prewarm" keeps its mechanism and
  gains its price, `residency-ledger.md`'s *Influence* says which, and the constant most worth
  replacing with a trace is how much prefill a decode step absorbs.
- *If wrong* (prefill-ahead still lowers service under the bit): the work it moves lands on engines
  with room, and landing is doing more than its 35-39% suggests.

**P6 -- A fan-out that crosses models pays for it.**

47-50% of fan-out agents run on a model other than their parent's. With `--model-keyed`: prefill
work +29-35% at the published partition, mean service within 0.1% where prefill is free, and 6-9
points above the unkeyed figure under `--prefill-time`.

- *If right:* the multi-agent shape has a cost the ids were hiding, and sibling agents on one model
  have a reason to co-locate that the published co-location rate never priced.
- *If wrong* (prefill work rises by under 10%): siblings on the same foreign model already share
  the re-encoded context, and the cost is per fan-out rather than per agent.

**P7 -- The ratio is zero at the published mix and rate, one in eight with headroom, and three in
eight on fresh prompts.**

Eight replicas of one model, `--prefill-time` on. Published mix at 500 req/s: every split at least
2% slower than aggregated under every rule. Published mix at 300 req/s: `joint` at 1:7 faster by
1.5-3%, and slower from 3:5 on. With fresh prompts at 0.15 a request and 300 req/s: `joint` at 3:5
faster by 15-19%; `list` at 3:5 between -8% and +15%; `list` at 1:7 and 2:6 slower by more than
500%. The planner's second pass gives 0, 1 and 3 prefillers. A prefiller does at least 1.5 times
the aggregated fleet's prefill work on the published mix. Coupled % between `joint` and
`independent` above 20% on the published mix.

- *If right:* §5's property 10 is restated as two mixes and two loads, "a sidecar picks a prefiller
  from a list" is mostly a statement about having no unpaired option, and the prefill tier's
  duplicate cache is a fourth price beside §3.8's three.
- *If wrong* (a split beats aggregated at the published mix and rate): decode slots are cheaper
  than §1.3 found, and the first thing to check is whether the prefiller's duplicate work
  disappears once it can fetch a prefix.

**P8 -- A quota at the router is the soft floor, a replica set is the hard partition, and the
neighbour's damage is engine time.**

No KV block is touched by two tenants; the tenant prefix carries 78-84% of hits; 97-98% of GPU
evictions are another owner's. A neighbour at 0.1 and 0.2 of the request rate: the others +3-8% on
the published engine, and +32-36% and +79-81% under `--prefill-time`, with p99 +40-50% and
+73-77%. A replica set of one in eight: the others +4.4-4.9% with or without a burst, the neighbour
served in 6-8 s; two in eight, +12.4-12.8% and 2.8-2.9 s. A quota: the others +5.2-5.4% at 0.25
engine-seconds a second and +26-27% at 1.0. Over a run in which the neighbour bursts a fifth of the
time, the quota at the replica's allowance and the replica set cost the others the same to within
2 points, and the quota wins below that duty cycle. A second engine on the node costs +90-100%.

- *If right:* §3.8 is rewritten around the router, its "no hint-shaped workaround" is kept for the
  cache and dropped for compute, and the soft-floors result has its second axis.
- *If wrong* (the quota protects no better than sharing): the damage is not the prefill work the
  router meters, and the neighbour's decodes -- which the pre-measurement's quota did not meter --
  are the first suspect.

**P9 -- A tenant-aware block manager is worth under 1% of any tenant's service, and the tiers'
write rates sit orders apart.**

A per-tenant eviction floor in the engine, as a ceiling: under 1% of mean service for every tenant
group at the published partition, **by arithmetic on §1.14's table**. The planner's record-tier
writes: under 0.1 a second on the rotating mix, against 3-10 loads a second from the lazy cache on
four nodes and about 600 decisions a second in the soft tier at 500 req/s.

- *If right:* the promotion-tier-2 ask §3.8 anticipates joins Phase 5's directive as one this
  workload does not justify, and §1's tiers have their first measured ratio.
- *If wrong* (the floor is worth more than 1% to the quiet half): the quiet tenants' misses are
  longer than the table's mean suggests, and fairness on KV has a number worth an upstream ask.

---

## 3. What macro authority must and must not do

Eleven rules. The first is the gate; the third is the one most likely to be broken for a good
reason.

1. **The gate.** With every new bit off, byte-identical to `HEAD` on the reproducible set as
   `phase-5.md` left it. And each bit has a case in which it must change nothing:
   `--model-batches` on a trace with one model; `--prefill-time` with a free allowance no step
   exceeds; `--pairing` with no prefill replica; a tenant quota no tenant reaches, and one tenant
   set holding every tenant; `--planner follow` on a stationary mix, which issues no move after
   the first. Checked after every work item.
2. **Macro authority follows `own::authority`'s capacity column.** The planner writes a replica
   set, a role, a tenant set and a sub-budget. It never writes, reprices or names a block.
3. **The planner reads aggregates.** `FleetView` holds sums over an interval and nothing else, by
   type, for `RequestView`'s reason: a planner that read the trace or the generator's phase would
   be exact here and available nowhere. `oracle` is the exception, and is labelled a ceiling
   wherever it prints.
4. **A macro action takes time and costs what it destroys.** A load has a duration in which its
   replica serves nothing, and a replica that changes model loses its partition. Nothing is
   instantaneous outside `oracle` with a start time of zero.
5. **Nothing tuned.** The planner's cost is the engine's step; its rule has no threshold; interval
   and start time are declared, swept and printed. `joint` has no threshold of its own. The
   thresholds of `list` and `independent`, the neighbour's rate and the quota's size are conditions
   of an experiment, never parameters of a policy.
6. **The engine acquires no policy** (`phase-3.md` rule 3, carried). No tenant floor, no per-tenant
   accounting, no quota inside a partition, even though §1.14 makes LRU look unfair. The
   tenant-aware block manager is a ceiling.
7. **One bit per mechanism.** `--model-batches`, `--prefill-time`, `--model-keyed`, `--fleet` (with
   `--planner`), `--pairing`, `--tenant-sets`, `--tenant-quota`, `--model-mix`, `--fresh`,
   `--neighbour`, `--shared-prefix` and `--kv-offload` are separately selectable, and the headline
   runs change one at a time.
8. **A condition's randomness is its own stream** (Phase 4's rule 6). The mix, the fresh prompts
   and the neighbour draw from streams separate from the workload's, so the base trace is
   byte-identical at every setting.
9. **Nanoseconds or counts.** A placement is priced in service and regret; loads, moves, unplaced
   requests and record writes are counts. No write is priced, because no store is modelled.
10. **Measure, do not repair** (`phase-2.md` rule 1). The published cluster results are not re-run
    under the corrected engine (§1.3), and if `follow` loses to `eager` it is reported and not
    retuned mid-phase.
11. **Three seeds, and the load with every number.** A figure near a knee is a figure about the
    knee (§1.3). Every cell carries its rate and its replicas per model, and the fleet is run at
    both rates.

---

## 4. Work items

Four increments, in order, each ending with a number: the engine, then weights and partitions,
then prefill and decode, then tenancy. Within each, nothing that can move a number lands before
the items that cannot. If the phase has to stop early it stops at an increment's end.

### 4.1 A batch per model

`Engine` tags each sequence in flight with a model and, under `--model-batches`, keeps a batch per
model: the round of §1.1, saturation per model, and `projected_ns` and `congestion_ns` taking the
joiner's model through `Telemetry`. An instrument counts the models in flight at each admission.
Off, and on a trace with one model, it is a no-op and is checked as one.

### 4.2 Prefill takes engine time

`Engine::prefill(now, work)` records prefill work, and under `--prefill-time` a decode admitted at
`now` is stretched by the prefill load over its own projected duration, less `--prefill-free` a
step. `Machine::run_here` reports a chain's rebuild to the engine it ran on, and `prefill_for`
reports prefill-ahead's. The score gains a sixth term, `prefill`, printed only under the bit: `in
flight x rebuild`. The first item that moves a published result (§1.10's prefill-ahead).

### 4.3 KV keyed by model

Under `--model-keyed`, `Workload::fanout` keys an agent's copy of its parent's chain by the agent's
model when that is not the parent's. Siblings on one model share the keyed prefix. The trace is
unchanged with the bit off.

### 4.4 The catalogue and the replica

A new `fleet.rs`: `Model { bytes, start_ns, context }` and a `Catalogue` that resolves a request's
`requires` to a model; `Replica { model, role, tenants, state }` with `state` one of serving or
loading until; `Fleet`, one replica per node, with the node's `width`; and `FleetView`, the
aggregates of rule 3. Sizes default to the published 1 GiB. No consumer yet: a no-op.

### 4.5 Weights out of the ledger

Under `--fleet`: the candidates for a request are the replicas serving its model; `plan` and
`run_here` leave `requires` alone; `Hierarchy::reload` drops the node's KV tiers, emits
`KvEvent::Cleared` and sets the partition to `HBM - weights`, asserted; the engine's step takes
the replica's size and width; a replica declares `min(model window, partition)` and the router
respects it; `unplaced` is counted. `own::authority`'s two cells change (§1.5), with their tests.
A fixed placement only, from a flag. This is the item that makes P1's second half and P4
measurable.

### 4.6 The mix and the fresh prompts

`--model-mix` and `--fresh` in `work.rs` (§1.7), each from its own stream.

### 4.7 The clock: loads, `once` and `oracle`

A load with a duration (§1.5); a schedule of placements; `once`, which places for the first
interval's demand and stops; `oracle`, which reads the generator's phases and moves at each
boundary to what the next phase wants. Placement instruments: moves, downtime, KV lost, service by
phase.

### 4.8 The planner: `follow` and `eager`

§1.9's cost, the marginal allocation, the accrued-loss rule, and the record-write counters. The
planner sees `FleetView` and returns moves; `Machine` applies them at the next arrival.

### 4.9 Roles and the prefill lane

`Role::{Both, Prefill, Decode}`. A prefill replica has a queue and a partition and admits no
decode; a decode replica prefills what it is not paired for. Needs `--prefill-time`.

### 4.10 Pairing, and the ratio

`--pairing list | independent | joint` (§1.12), the transfer through the existing supply path, the
handshake uncharged to the orchestrator as §2.5 has it. Coupled % between `joint` and
`independent`. The planner's second pass chooses prefillers per model; `--prefillers n` fixes them
for the sweeps.

### 4.11 Tenants

`Request::tenant` and `RequestView::tenant`, declared by the generator. `--tenant-sets` on
replicas. The tenant axis on `Quota` (§1.17), and the router's two meters behind `--tenant-quota`.
`--neighbour` and `--shared-prefix` in the generator. The tenant-floor ceiling in `EngineCache`,
wired like `clairvoyant`. If the phase runs short, the host-DDR row of §1.17 is the part to leave:
no pre-measurement says it binds.

### 4.12 Sizes

Three model sizes in the catalogue -- 0.5, 1 and 2 GiB -- so that a step, a partition and a context
window differ by model, which is the heterogeneous half of §9's deliverable and the regime in which
the partition binds (§1.6). Only the step's base scales with size; a block's bytes and its prefill
cost stay the generator's. A half-width node in `Topology` is the part that can be cut: nothing
else depends on it.

### 4.13 Instruments

| instrument | measures | over |
|---|---|---|
| models in flight | models decoding on a node at each admission; a model's own batch | every decode |
| weights | loads per second by source; models resident per node; the `WeightShard` census row | every request |
| placement | moves, downtime, KV lost to a move, service by phase, regret against `oracle` | every plan |
| occupancy | sequences in flight per replica, decodes arriving at a full batch, `unplaced` | every decode |
| prefill | work per node and by origin, its distribution, the stretch it causes | every KV dispatch |
| pairing | pairs, the prefiller's utilisation and wait, its work against the decoder's, bytes shipped, coupled % | every KV dispatch |
| tenants | cross-tenant touches, hits by origin, evictions by owner, hit rate and p99 per tenant group | every KV dispatch |
| neighbour | the others' service inside and outside the window; the neighbour's own; its refusals | every request |
| record writes | placements, roles, tenant sets and sub-budgets per simulated second | the run |

These are the instruments the pre-measurements approximated, and §8's figures are reproduced with
them first.

### 4.14 `polyphonic fleet`

A reproducible sweep, as `influence` is for Phase 5: no control crossing charged,
seed-deterministic, three seeds for every cell, the `belief` cluster for the engine sections and
§1.7's fleet at both rates for the rest.

1. the gate, as printed check lines
2. a batch per model on the `belief` cluster: the score as it is, the score pricing it, and one,
   two and three models per node; the same under one batch per node (P1)
3. routing on the fleet: `scored + fetch` against `hash only` at three shapes and two rates (P2)
4. the clock: the rotating mix under `once`, `follow`, `eager` and `oracle`; the start-time and
   interval sweeps; the writes (P3, P9)
5. the partition and the offload grant (P4); the rotating mix again with three model sizes (P3)
6. prefill time: the bit, the free allowance, prefill-ahead under it (P5)
7. KV keyed by model (P6)
8. prefill and decode: three rules, five ratios, two mixes, two rates; the planner's ratio (P7)
9. tenants: the sharing and eviction tables; the neighbour under sharing, a replica set and a
   quota; the ceiling (P8, P9)

`distributed` takes the same flags. `code-review` takes none, as it took none of Phase 4's or 5's.

### 4.15 Report and publish

| target | change |
|---|---|
| `owned-and-observed.md` §9 Phase 6 | a **Status** line; the phase restated as the engine's two corrections and three decisions |
| `owned-and-observed.md` §1 | the ownership table's `WeightShard` rows; a replica beside the partition in *Ownership is per class*; the clocks' gap with its number |
| `owned-and-observed.md` §2.5 | the prefiller's cache; pairing as an argmin with an unpaired option; the ratio by mix and load |
| `owned-and-observed.md` §3.8 | the first table's first row corrected; the instruments by when they charge; the router as the arm for compute |
| `owned-and-observed.md` §5 | properties 10 and 11 as measured; "macro-orchestration of weights" with its size and its condition |
| `owned-and-observed.md` §8 | *What gets harder* item 2 restated; the interface list's load cost and context window as built |
| `residency-ledger.md` | *Serving engines* says what the engine batches; a *Fleet* section, every figure with its rate and replicas per model; *Standing* rows for each prediction, and the rows that a fleet of several models changes |
| `phase-5.md` | nothing: it keeps its results as measured, and the *Influence* section of the ledger carries prefill-ahead's price |

Whether `--model-batches` and `--prefill-time` become the default is decided after the numbers, as
Phases 3 to 5 decided their own bits. They differ from those: the others put the corrected
architecture behind a bit, and these put a corrected engine there, so every published cluster
number would move.

---

## 5. Verification

- **Byte-identity with every new bit off**, against the commit before this phase, on the
  reproducible set -- `residency`, `flows`, `placement`, `volatility`, `ownership`, `price`,
  `belief` and `influence` -- at a reduced `--ops` and a second seed, after every work item.
  `distributed`, `code-review` and `data-path` get the structural smoke run.
- **The gate** (rule 1), one check line per bit.
- **A round is what §1.1 says.** On a fixture with two models in flight a token costs two bases and
  the widths of both batches; with one, `Engine::step_ns`. A model's full batch queues its own
  arrivals and no other model's.
- **Every decode lands on a replica serving its model**, in every arm, printed as a check as
  `code-review` prints its decode filter. No block keyed to one model is resident on a replica of
  another.
- **The partition and the weights sum to the node's HBM** after every load, asserted in
  `Hierarchy::reload` (`phase-3.md` §5's check, now per load).
- **A load takes its time.** No dispatch reaches a loading replica before its ready time except as
  a priced wait; a replica that changes model emits `Cleared`, and a belief over it resets. The
  stream invariant holds across a reload: replay reproduces every tier.
- **The census.** `cargo build --release --features census` still emits 13 warnings. With
  `--fleet` on, `EngineOps`' `WeightShard` row is zero on every counter and its `KvBlock` row stays
  zero. A moved count means the planner wrote engine state.
- **The planner cannot see a request.** `FleetView` has no field that names a block, a chain or a
  position; only `oracle` holds the generator's schedule, and the scoring path cannot reach it.
- **Moves are accounted.** Record writes equal the moves applied; downtime equals moves times the
  load; on a stationary mix `follow` and `eager` both stop after the first placement.
- **A pair is accounted once.** The prefiller's work and the transfer are each charged to one
  request, a paired decode prefills nothing at its decoder, and a prefill replica's engine admits
  no decode. With no prefill replica every rule equals `none`.
- **Tenant sets and quotas hold.** No request of a tenant runs on a replica outside its set; a
  tenant's admitted prefill work and slots never exceed its limit; the meters sum to the engine's
  own counts.
- **The ceiling cannot leak.** Only the tenant-floor engine reads a block's tenant, as only the
  clairvoyant one reads the trace.
- `cargo fmt --check`, `cargo clippy --all-targets` and `cargo test` clean.

---

## 6. Risks

1. **A modelling correction read as an orchestrator's win.** P1's factor is what the published
   engine was not charging. It is the price of a model, and the planner is credited only against
   `once` and `oracle`. A reader who quotes "-71%" is quoting §1.1, not §1.9.
2. **The engine's constants now carry the headlines.** `STEP_BASE_NS` against `STEP_PER_SEQ_NS`
   decides what a second model costs; `KV_BLOCK_NS` against a step decides what prefill costs; the
   start time decides what lateness costs. All three are modelled. Each is swept, and the free
   allowance of §1.10 can take prefill's cost from a sixth of service to nothing: the results that
   depend on it are published across it or not at all.
3. **Prefill time is a fluid approximation.** A decode is stretched by the load at its admission,
   because a batch is sampled once and held (`residency-ledger.md`, *Serving engines*). Means are
   sound; a p99 under `--prefill-time` is an approximation, and the emulation's window moved the
   half-partition figure by tens of points.
4. **Results at a knee.** Half the cells in §1.8 and §1.13 are queues. They are reported with
   their rate, and the fleet is run at two, but the second rate was chosen after seeing the first
   (§1.7), and that is said wherever it is quoted.
5. **A planner tuned to win.** An interval, a horizon or a hysteresis adjusted until `follow` looks
   good is the easiest way to manufacture a result here. The rule has none of the three, the sweep
   is published whole, and `eager` and `oracle` bracket it from both sides.
6. **The mix is a caricature.** A hot model rotating every minute is chosen to exercise the clock,
   not drawn from a trace. What survives is the shape -- lateness against phase length -- and §7's
   rule says the crossover's position moves with both.
7. **One replica per node.** A host that shares an accelerator between two cold models, and a swap
   that loads the new model before dropping the old, are both outside the unit (§1.4). The first
   costs the fleet nothing at four models on eight nodes; the second overstates a move's downtime.
8. **Three increments in one phase.** Each could be a phase. They are ordered so that each ends
   with a number and the phase can stop between them; tenancy's host-DDR row and the half-width
   node are the named cuts.
9. **Runtime.** A clocked section is eight times the requests of any earlier one, and the sweep is
   several hundred runs: tens of minutes as parallel processes, against `influence`'s eight.
10. **The pre-measurements came from outside the repository, and more of them are emulations**
    (Phase 4's risk 9). §8 says how each was taken and where it is cruder than the build. They
    argue for the predictions and are evidence for no claim.

---

## 7. Out of scope

- **Re-running the published cluster results under the corrected engine** (§1.3). They hold for a
  fleet of one model. §4.15 tabulates which a fleet of several changes.
- **A queue at the router, and cancellation.** Phase 9. A quota's only enforcement here is refusal,
  and a full batch stays a price (`phase-3.md` §1.5).
- **Predicted flows, the taxonomy's presets and the RAG class.** Phase 7. `--fresh` is a shape, not
  a class: nothing about its prompts is shared.
- **Several engines on one accelerator as a planner's option**, and **slicing a host at run time**
  (§1.4). Both are expressible as topologies and neither is a decision here.
- **Loading a model before dropping the one it replaces.** It needs two models resident on a node
  at once, which is the engine that takes turns.
- **Growing the fleet.** The nodes are fixed; `residency-ledger.md`'s "growing the pool is
  deliberately not modelled" stands.
- **Staging weights as a decision.** A model's bytes in host DDR follow the ledger's existing
  rules. At the simulator's byte scale the move is under half a second, and the start time is what
  a load costs (§1.5).
- **A prefiller that fetches its prefix from a decoder.** The pair ships KV one way, as §2.5
  describes it. If P7's *if wrong* fires, this is the first thing to build.
- **Binding a prefill-ahead to its decoder** (`phase-5.md` §1.4). It is the same pair decided
  early, and §4.10 makes it expressible; measuring it is left until hints are predictions.
- **Tenant-aware eviction as an arm**, and **fair queueing between tenants at a full batch**. The
  first is a ceiling (rule 6); the second needs Phase 9's queue.
- **A model store, and the record itself.** Writes are counted. Nothing is persisted, and no
  result depends on FoundationDB.
- **Regions.** Phase 11. The planner's allocation is what a regional budget will bound.
- **Routing by model quality.** §8 of the design: a capability gate is in scope, a ranking is not.

---

## 8. Pre-measurements

All taken on an instrumented copy of `a700562`, run outside the repository and not committed. With
every hook off the copy reproduces `polyphonic price` byte for byte, and its uninstrumented base
the published 489.451 ms. Rows on "the `belief` cluster" use that command's configuration -- 4
nodes, 16 GiB HBM, 32 GiB DDR and 64 GiB `NVMe` in total, 250 req/s, 10% fan-out, 15,000 requests,
rack, no control crossing charged, `scored + fetch`, the engine's grants sized from the ledger's
run at each regime, and the exact view. Rows on "the fleet" use eight of the same node at 500 req/s
unless a rate is given, 30,000 requests unless a duration is, and the published regime: with one
model per node a partition is 3 GiB, and half the published grant is not reachable (§1.6). Seeds 1
to 3 unless stated.

| name | what | how | headline |
|---|---|---|---|
| *batches* | what one batch per node hides | each sequence in flight tagged with its model; under the alternative, a batch per model on a node, a round of one step per model in flight, saturation per model; the score either unchanged or with the model in its `engine` and `congestion` terms; both regimes | §1.1's tables |
| *placement* | what a placement is worth, and what the partition's size adds | a mask of models per node, imposed as a filter on decode candidates, weights preloaded; one, two and three models per node with the partition at `HBM - weights` or at the published grant; repeated under one batch per node; both regimes | §1.2's and §1.6's tables |
| *models* | what per-request weight caching does | per node: shard reads by where they were served, loads and evictions per simulated second, whole models resident at each dispatch; the stall a dispatch pays for its chain and for its weights; both regimes | §1.5's table; the `WeightShard` census row on seed 1: 236 admissions, 212 demotions, 206 superseded copies |
| *routing* | the score's lead by replicas per model | the fleet at 500 and 700 req/s under `scored + fetch` and `hash only`: the published engine, four models at two replicas each, one model at eight | §1.3's table |
| *fleet* | the placed fleet against the published engine | the fleet under lazy weights with one batch per node and with a batch per model, and placed at two replicas a model, with and without prefill time | 476.7 / 471.7 / 476.8 ms placed against 475.2 / 471.4 / 476.7 published and 1597.6 / 1556.0 / 1577.3 lazy |
| *shift* | what a placement costs when the mix moves | a generator patch drawing each session turn's and each flow's model from a share per phase, tenants in blocks of six per model, the class mix flat; 120,000 requests; placements for each phase's token shares, applied at the phase boundaries from a schedule, a moved node answering nothing for the load and losing its KV; the ledger cannot replace a model in a pool smaller than its weights floor, so the emulation removes the old shards by hand | §1.8's tables |
| *offload* | the connector's DDR sub-budget | the offload grant overridden from nothing to 1.6 GiB a node on the `belief` cluster at 8 and at 4 GiB of DDR per node | §1.6's second table |
| *prefill* | prefill as engine work | each KV dispatch's chain rebuild recorded by origin; per node, prefill work over wall time; then each decode stretched by `1 / (1 - rho)`, `rho` the node's prefill work over the trailing 250 ms, 1 s or 4 s; both regimes | §1.10's first two tables; by window, +18.9 / +17.5 / +16.1% at the published partition and +64 / +68 / +89% at half, seed 1 |
| *ahead* | prefill-ahead once prefill is charged | `--prefill-ahead` with its work recorded on its target's engine, under the 1 s stretch; both regimes | §1.10's third table |
| *keyed* | agents on another model than their parent's | agents counted by model against their parent's tenant's; the parent-prefix blocks they find resident; then those blocks re-keyed by the agent's model at dispatch, with and without the 1 s stretch; both regimes | §1.11 |
| *pairing* | roles, rules and ratios | eight replicas of one model, the first `p` of them prefill-only, each a first-come queue; the decoder by the scored argmin, then a pair or not by rule; the prefill run on the prefiller and shipped by the existing peer fetch; `joint` pairing when `queue + work + toll + transfer` at the best prefiller is under `work x (1 + in flight)` at the decoder; the fresh stream injected at the instant of the request before it, 64 blocks and 24-64 output tokens, at 0.15 and 0.30 a request; 300 and 500 req/s | §1.13's tables; at 0.30 a request and 300 req/s, `joint` at 3:5 is -16.5 / -15.0 / -16.5% with its prefillers saturated |
| *tenants* | sharing and eviction by owner | each chain's owner taken from its root block, a tenant or a function; each block's first toucher recorded; chain reads classified by origin and owner; each GPU eviction attributed to the owner of the request that caused it; both regimes | §1.14's table |
| *neighbour* | a bursting tenant under three instruments | eight replicas of one model; an injected tenant sending fresh 64-block prompts between 40% and 60% of the run at 0.1 or 0.2 a request; shared, or confined to one or two replicas with the others confined to the rest, or shared under a token bucket on its prefill work that refuses the excess; with and without the 1 s stretch; the others' decodes inside the window | §1.15's and §1.16's tables |
