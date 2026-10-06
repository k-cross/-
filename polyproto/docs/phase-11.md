# Phase 11 -- Regions: a scheduler per region under global budgets

Implementation plan for Phase 11 of [`owned-and-observed.md`](owned-and-observed.md): §8's shape --
a routing tier in each region, admitting within budgets a global tier sets on the provisioning clock
-- against the single global argmin every region-distance result so far has run. §9 asks for the
price of the regional split: what the global argmin buys across regions that regional schedulers
under budgets give up, and how much rebalancing recovers. It predicts the price small, and names the
one place it could bind: an agent and its model host in different regions, where the origin round
trip is 61 ms and no placement moves it.

**Status: increments 1 to 4 built.** Regions in the topology, clients in regions, the round trip in
the score and regional schedulers (§4.1 to §4.5), summaries, own forwards, overflow and the forced
forward (§4.6 and §4.7), then the routing table, node budgets and models placed by region (§4.8 to
§4.10), then tenants' shares and residency, active-active schedulers and the arithmetic (§4.11,
§4.12 and §4.14), are in `topo.rs`, `work.rs`, `fleet.rs`, `machine.rs` and `regions_cmd.rs`, driven
by `polyphonic regions`, whose sections are `gate`, `even`, `burst`, `day`, `table`, `budget`,
`models`, `tenants`, `shards` and `arithmetic`; `--regions` on `distributed` and `code-review` is
the one work item not built, and §9 records what the four increments and a review of them found.
§2's predictions are stated before the run, per `owned-and-observed.md` §7, and like Phases 4 to 7,
9 and 10 they lean on **pre-measurements**: numbers taken on an instrumented copy of `7f16fef`, run
outside the repository and not committed, with every hook off reproducing `belief`, `residency`,
`enforce`, `fleet` and `durability` byte for byte on the sections §8 names. Nine emulate what this
phase builds -- clients in regions with their round trip in the score (*even*, *diag*), the
published class phases against a flat mix (*phases*), the regional knee (*knee*), spill (*spill*), a
global routing table (*table*), node budgets that follow demand (*budget*), models placed by region
(*models*) and the active-active schedulers Phase 10 deferred (*shards*) -- and three are
arithmetic, one on the generator's own demand (*tenants*) and two on published figures (*path*,
*records*). Each is labelled with its grade (`owned-and-observed.md`, *On the numbers*), and §8 says
how each was taken. They are reasons to predict, not results: §4.13 rebuilds each instrument in the
repository, and a pre-measurement the build does not reproduce is reconciled before a prediction
resting on it is graded.

Four things make this phase unlike the others.

- **Its baseline never had a client.** Every region-distance result put four nodes in four regions
  and the client nowhere: a request paid nothing to reach a node or to come back, so a global argmin
  "across regions" spread work at no cost to anyone it served. Placed in regions, the same argmin
  serves two-thirds of requests out of their region and a function call takes 40 ms instead of 0.6
  (§1.1).
- **The comparison it asks for inverts.** With the client's round trip priced, the global argmin is
  4% slower than regional schedulers at 30 ms between regions and 11-12% slower on Azure's published
  round trips, at equal demand, because the per-request score still sends 37-41% of requests to
  another region when nothing there is better on average (§1.3). It is not the bound §9 assumed. The
  price of the split is what a region does with demand it cannot serve, and the answer depends on
  the clock that demand moves on.
- **Its subject is three clocks.** A WAN round trip (60-230 ms) on the request clock; a burst,
  seconds to minutes; a day, hours. A rule on the request clock catches a burst and chases noise; a
  table or a budget on the provisioning clock follows a day and misses a burst; and moving capacity
  is priced, as Phase 6 found for a placement, by its lateness (§1.6 to §1.8).
- **Its comparisons have shipped, and argue against the design's instinct.** Google's load balancer
  fills the closest region and overflows by capacity; ServiceRouter routes by load thresholds and a
  global table and declines to trade queueing delay for cross-region round trips; GKE's
  multi-cluster inference gateway spills past 40% KV-cache use; Taiji rebalances every five minutes
  with a dampened step; Doorman leases capacity with a refresh; Bedrock and Azure route globally,
  with data-zone variants for residency (§1.6, §1.7, §1.10). §3.9's "nothing tuned" meets thresholds
  that every one of them tunes, and the pre-measurements say when that costs and when it does not.

§1 settles thirteen decisions. Five are findings about the existing model and documents rather than
about the work ahead: §1.1 (the published region distance had no client and no round trip in the
score), §1.3 (the global argmin spreads across regions), §1.4 (regional schedulers need no second
`Machine`; active-active ones do), §1.5's finding that the published class phases alone made a
follow-the-sun day look ten times as costly as it is, and §1.9 (a model in every region costs a
knee, not a round trip).

---

## 1. What has to be settled before a second scheduler runs

### 1.1 Every region-distance result is four one-node regions with no client

`Topology::cluster` puts every pair of nodes at one `Distance`, so at `region` each of the four
nodes is a region of its own. `Request` carries no origin: a request arrives at the scheduler from
nowhere and its response goes nowhere. `reach_ns`, the only round trip a client pays, exists for
`code-review`'s two-node topology (`set_origin`), is charged after placement and is not a term of
the score. So every published region-distance figure -- the tool call that flips home at 65.7%, the
61 ms round trip "no placement moves", prefill-ahead's 7% at region -- is a global argmin over
regions that a request reaches and leaves for free.

Pre-measured with clients placed (§8, *even*; three regions of four of the `belief` cluster's nodes
-- 4 GiB HBM, 8 GiB DDR, 16 GiB `NVMe` each -- rack within a region, today's `Region` latency of 30
ms one way between regions, 250 req/s a region, a flat class mix (§1.5), the engine allocating,
`scored + fetch`, seeds 1 / 2 / 3; a session belongs to a region and a function call is drawn by the
regions' shares):

| | client-facing requests served in another region | a warm `FaaS` call | mean service against regional schedulers |
|---|---|---|---|
| regional schedulers | none | 0.6 ms | 459.5 / 454.7 / 461.4 ms |
| the global argmin as published (no round trip in the score) | 66.3-66.7% | 40.4-40.7 ms | +7.1 / +7.2 / +7.1% |
| the same on Azure's round trips (83 / 162 / 233 ms) | 66.1-66.9% | | +18.6 / +19.2 / +18.9% |

The published argmin sends two-thirds of everything elsewhere -- the share three equal regions give
a placement that cannot see where its client is -- including the function calls whose 0.1 ms of work
now carries a 60 ms round trip.

**Chosen: a two-level topology, a client region on every request, and the client's round trip in the
score.** Regions are sets of nodes, rack within a region, and a matrix of one-way latencies between
regions: today's 30 ms everywhere, and Azure's published median round trips for East US, West Europe
and Japan East -- 83, 162 and 233 ms (§8, *Sources*). A session, and so every turn of it, belongs to
a region; a function call or a service request is drawn by the regions' shares at its instant; a
flow downstream, a resume and a fan-out inherit the region of what produced them. A request whose
client is in one region and which is served in another pays `reach`, the round trip between them
plus its bytes at the region link, as a term of the score and as a cost when it runs; a tool call, a
flow downstream and a fan-out's agents are internal and pay the handoff the score already prices.

### 1.2 A scheduler on the request path cannot be global

§2.2's rule at the WAN's scale. A single global scheduler sits in one region; a request from either
of the other two crosses the WAN before it is decided. *Arithmetic:* with three regions of equal
demand, two-thirds of requests pay one round trip -- a mean of 40 ms at today's 60 ms, and on
Azure's triangle 82, 105 or 132 ms as the scheduler sits in East US, West Europe or Japan East. That
is 500-3,000 times the 44-75 us sidecar tax §2.3 removed, 4-13% of a one-second agent turn and
330-1,100 times a warm `FaaS` invocation (`phase-10.md`'s 120 us, the chosen constants' mean).
Kubernetes can run one control plane for many clusters because its decisions are placements, off the
request path; a control plane that spans clusters schedules replicas into them, not requests.

A partition of the WAN then cuts a region off from its scheduler. With the outage priced as Phase 10
priced a scheduler's, `λD²/2` of the arrivals it delays: 450,000 request-seconds for a region of 250
req/s cut off for 60 s, against nothing for a region that schedules itself and loses only what it
could have sent elsewhere and the budget changes it could have received.

**Chosen: every per-request decision is regional.** The global argmin is run as §9 asks, with no
decision latency and an exact view of every region's engines -- the most favourable form a global
scheduler could take -- and printed as a comparison, never as an arm a deployment could run.

### 1.3 The global argmin is not a bound: the per-request score spreads across regions

With the round trip in the score, the global argmin should keep a request home unless another region
is cheaper by more than the trip. Pre-measured (§8, *even*, *diag*; mean service against regional
schedulers, seeds 1 / 2 / 3):

| at equal demand | served in another region | turn stall | mean service |
|---|---|---|---|
| global argmin, round trip priced, 250 req/s a region | 36.9-40.1% | 33.3-38.0 ms against 8.6-8.9 | +4.4 / +4.6 / +4.3% |
| the same at 325 req/s a region | 38.4-40.8% | 36.4-39.7 against 11.1-11.3 | +4.3 / +4.1 / +4.1% |
| the same on Azure's round trips, 250 req/s | 37.5-39.2% | 74.3-80.5 | +11.5 / +12.2 / +11.1% |
| no congestion term, 250 req/s | 37.4-47.3%, up to a fifth of decodes at a full batch | 38.8-68.9 | +13.4 / +5.3 / +15.9% |
| round trip priced at three times its cost | 34.8-38.3% | 28.9-35.0 | +4.4 / +4.1 / +4.4% |
| round trip priced at ten times its cost | 8.1-15.3% | 22.1-33.9 | +2.1 / +1.5 / +1.4% |

Two in five requests leave their region when no region is busier than another on average, and the
service of the turns that do is no shorter for it: the turns' mean rises by about the share moved
times the round trip, and the stall of a moved turn by the prefix it left behind. Removing the
congestion term makes it worse, not better -- without it the argmin piles onto whichever node looks
cheapest, so congestion is what keeps it from herding, not what moves it -- and pricing the trip ten
times over still sends one request in seven to twelve elsewhere. What moves them is the rest of the
score: the `engine` term's projected decode and the acquire term find a node in another region
cheaper, at the instant of the decision, by more than the trip. The terms that decide well among
nodes that cost the same to reach -- Phase 2's 4.7% moved by congestion and 13.6% by load, Phase 4's
freshness that matters little -- price an instant; a load difference between two nodes is gone
within a decode, while the round trip and the lost prefix are certain. This is
`residency-ledger.md`'s *falsification test that fails* one level up -- a greedy per-request score
cannot price what its decisions do to the next ones -- and ServiceRouter's reason for routing across
regions by load thresholds and a global table rather than by latency: "trading long queuing delay at
the RPC server for long cross-region network RTT is not a robust method".

**Chosen: the global argmin is a comparison, labelled as the per-request form.** "What the global
argmin buys that regional schedulers give up" is answered by it -- nothing, at any demand measured
-- and the price of the split is graded against the best cross-region rule instead (§1.6).

### 1.4 Regional schedulers need no second `Machine`; active-active ones do

§9 names the risk as a structural change to `Machine`. For disjoint regions it is smaller than that.
Every request that reaches a region's engines passes that region's scheduler -- its own clients' and
those forwarded to it -- so each regional scheduler carries every flight on its engines and its
in-flight view is exact, Phase 4's structural reason held region by region; and what one region
knows of another is only what it is sent. A scheduler per region is therefore a candidate set, a
queue and meters per region, and a summary between regions: a scope on `Machine`, not a copy of it.
In the copy a region was exactly that -- a filter on the candidates, the region's own queue and
meters, and a summary of the others -- and nothing else in `Machine` changed.

The structural change lands on **active-active schedulers over the same nodes** -- Phase 10's
leftover and llm-d's RFC for horizontally scaled endpoint pickers (llm-d/llm-d-router#1593), whose
replicas each keep their own "inflight request/token counters", "distorting the load view used by
load-aware routing". Each reads its own dispatches exactly and the others' through reports (§1.11).

**Chosen: a region per node and a scope per request on `Machine`; active-active schedulers as a
later increment with a split view of in-flight load.**

### 1.5 Demand moves between regions on two clocks

Published shapes. DynamoLLM's week of Azure's LLM services: conversation's peak is 1.7 times its
average and 3.3 times its valley, coding's 2.8 and 34.6 times, the latter through weekends. Taiji's
edge nodes: at one, daily peak traffic is 7 times the trough, and "the magnitude and peak time for
edge nodes differ significantly". So two shapes:

- **A day.** Each region's share follows `1 + a cos` with its peak at 14:00 local -- East US, West
  Europe and Japan East peak at 19:00, 13:00 and 05:00 UTC -- for `a` of 0.5 (a peak 1.5 times a
  region's mean and 3 times its trough, conversation's shape) and 0.75 (1.75 and 7, Taiji's edge).
  On the three regions a region's share peaks at 1.41-1.58 times its mean at 0.5 and 1.62-1.86 at
  0.75.
- **A burst.** One region takes 60% or 75% of arrivals -- 1.8 and 2.25 times its share -- for 18 s
  in the middle of a run.

The generator holds the total rate and moves the shares. The global sum has a day of its own, but it
is the same for every arm and is a question about the whole fleet's size, not about the split;
holding it constant isolates what distinguishes a regional scheduler from a global one. A region is
drawn from a stream of its own and a session's turns stay in its region, so with one region the
trace is the published one.

Two clocks need two compressions. A burst runs in real time. A day runs as the run: 60 s for rules
on the request clock, where only the shares' instantaneous level matters, and 240 s where capacity
moves, so that a second of the run is six minutes of a day and an 8 s load is 48 minutes -- a
pessimistic start for a replica, which §1.8 sweeps.

**A finding about the published workload.** Its class phases move the inference share of requests
from 0.18 to 0.58 over a run. Run under them, a day whose peak in Japan East falls in the
inference-heavy first phase cost regional schedulers +27-37% at amplitude 0.5, where the flat mix
costs +2.8-2.9% (§8, *phases*). **Chosen: a flat class mix (volatility 0)** for every region figure,
so that the regions' shares are the only time structure.

**Operating points.** The regional knee, measured on even demand over 240 s (§8, *knee*): 457 ms of
mean service at 250 req/s a region, 474 at 300, 484 at 325, 495 at 350, 506 at 375 with 6% of
decodes arriving at a full batch, and 529 at 400 with 29.5%, saturated but stable over the run.
**Chosen: 250 req/s a region**, the published load per node, two-thirds of the knee, so a region
takes 1.5 times its share before it saturates; **and 325**, a fleet sized near its mean, with
1.15-1.2 times of headroom.

### 1.6 What a region does with demand it cannot serve: spill, and how it is priced

A regional scheduler that only places in its own region is the arm §8 describes, and it has no
answer to a peak. What shipped systems do instead:

- **Google's global load balancer** keeps traffic in the region closest to the user until "an entire
  region reaches capacity as determined by the serving capacity set in the backend services"; then
  "traffic overflows to the closest region that has available capacity", and when every region is
  over, they are balanced "at the same relative level of overflow" -- capacity a configured number.
- **ServiceRouter** (Meta, OSDI'23) filters servers by locality rings -- by default same region,
  neighbouring regions, same continent, global -- and extends them with load thresholds (an example
  of 55, 65 and 80%), spilling to the next ring past a threshold. It measures cross-region round
  trips at a median of 35 ms and a p99 of 163 ms, routes about 16% of its RPCs across regions, and
  names the failure of clients deciding alone: a failed region's clients all move to the nearest,
  "overload it, bring it down, and together move onto the next region".
- **GKE's multi-cluster Inference Gateway** routes "overflow traffic to the next healthy region when
  the primary cluster crosses its 40% KV-cache utilization threshold", near-linear in throughput
  across three regions and within 1% of a local call.
- **Bedrock's cross-region inference** "attempts to fulfill requests from the source Region when
  possible, but it can seamlessly route requests to other Regions as needed", weighing regional
  capacity, latency and availability, at "a double-digit milliseconds latency add"; **Azure's**
  Global and Data Zone deployments "dynamically route traffic to available datacenters".

Every one of them spills on a threshold of capacity or utilisation, a tuned constant. The design's
alternative is the score: forward when another region's quote plus the round trip is below the home
region's. What the home region knows of another is a summary on a clock. Pre-measured (§8, *spill*;
mean service against regional schedulers at equal demand, the range over seeds 1-3; the burst is
2.25 times a region's share for 18 s, the day amplitude 0.75 over 60 s):

| at 250 req/s a region | equal demand | burst | day |
|---|---|---|---|
| regional, no cross-region rule | 0 | +72 to +82% | +17 to +23% |
| global argmin, round trip priced | +4.3 to +4.6% | +4.2 to +4.7% | +4.0 to +5.6% |
| priced spill on the best node, an exact view of every region | +0.8 to +0.9% | +1.1 to +1.2% | +1.2 to +1.3% |
| the same on a 1 s summary | +3.3 to +3.4% | +3.6 to +3.7% | +4.1 to +4.2% |
| the same counting its own forwards in flight | +1.3 to +1.4% | +1.5 to +1.7% | +1.8% |
| the same on a 5 s summary | +1.6 to +1.8% | +2.0 to +2.1% | +1.9 to +2.3% |
| priced by the region's mean, 1 s summary and own forwards | +0.6 to +0.7% | +1.2 to +1.4% | +1.4 to +1.6% |
| threshold 0.5 on a 1 s summary | +1.8% | +2.4 to +2.5% | +2.7 to +3.2% |
| threshold 0.7 on a 1 s summary | 0.0% | +1.3 to +1.5% | +2.7 to +3.4% |

| at 325 req/s a region | equal demand | burst | day |
|---|---|---|---|
| regional, no cross-region rule | 0 | +223 to +250% | +161 to +174% |
| global argmin, round trip priced | +4.1 to +4.3% | +4.0 to +4.3% | +4.2 to +4.3% |
| priced spill on the best node, an exact view | +1.1% | +1.4 to +1.5% | +1.4 to +1.5% |
| the same on a 1 s summary | +4.3 to +4.4% | +4.8 to +5.1% | +5.2 to +5.6% |
| the same counting its own forwards | +1.5% | +1.8 to +1.9% | +1.9 to +2.1% |
| priced by the region's mean, 1 s summary and own forwards | +1.0 to +1.2% | +1.7 to +1.8% | +2.0 to +2.1% |
| threshold 0.5 | 0.0 to +0.6% | +3.1 to +3.9% | +6.3 to +6.9% |
| threshold 0.7 | +1.6 to +1.7% | +2.3 to +2.5% | +3.0 to +3.4% |

On Azure's round trips at 250 req/s the region-mean rule costs +0.6 to +0.7% at equal demand, +1.9
to +2.0% on the burst and +3.1 to +3.5% on the day: a forwarded request pays more, and fewer are
worth forwarding.

Five findings.

- **A region alone collapses past about twice its share**, and near the knee at far less: at 1.8
  times for 18 s regional schedulers cost +5.5 to +11% at 250 req/s and +86 to +102% at 325. Every
  cross-region rule recovers the collapse, to within 1-6% of equal demand. Having a rule is worth a
  factor; the choice between rules is worth a percent or a few.
- **The price of a stale summary is herding,** ServiceRouter's domino in miniature: on a 1 s summary
  a region sees another as idle until the summary refreshes, and forwards to it all second -- 21-29%
  of client-facing requests leave their region at equal demand, and the run is 3-6% slower in every
  shape. Counting its own forwards still in flight on top of the summary -- Phase 4's
  `stream-plus-dispatch` across regions, a fact the sender holds exactly because it relays their
  streams -- removes most of it.
- **A per-node price spills on noise.** Comparing the home region's best node with the best node of
  two other regions takes the minimum of more draws on the remote side; priced by each region's mean
  instead, the rule forwards a third less often when nothing is wrong -- 6.0-6.5% of client-facing
  requests against 8.8-10.0% -- halves what that costs, and catches the same peaks.
- **A threshold is right only at the load it was tuned for.** 0.7 costs nothing at equal demand at
  250 and 1.6% at 325, where every region sits near it; 0.5 is the reverse, and at 325 it saturates
  on the day (+6.3 to +6.9%). §3.9's objection to a dimensionless constant, measured: the same
  constant is the best rule at one load and the worst at another.
- **Every rule pays something when nothing is wrong** except the threshold where it fits and the
  table (§1.7): a request rule cannot tell a peak from a fluctuation.

**Chosen: spill as the request clock's arm, priced by region-mean quotes on a summary plus the
sender's own forwards, with the summary's period swept** -- untuned, within 0.6% of the best-node
price on an exact view in every shape and below it when nothing is wrong. The threshold is built as
the shipped comparison and swept as the tuned constant it is; the exact view is printed as the node
price's information bound. A forwarded request pays the round trip once and is placed by the
receiving region's scheduler, which owns its capacity (§1, *capacity versus allocation*, one level
up). `FaaS` and service requests are never forwarded: their work is a tenth of a millisecond against
a round trip of sixty.

### 1.7 The global table is the provisioning clock's answer, and it needs headroom

ServiceRouter's cross-region routing service and Taiji both route by a table computed on an epoch
from global load: the fraction of each region's traffic each other region serves. Taiji's epoch is
five minutes, a step is dampened to 80% of its target and a data centre's utilisation may rise by at
most 0.04 an epoch; ServiceRouter's table is driven by a PID controller and drops traffic ("black
holes") when global capacity is short. Here the table is priced rather than thresholded: each epoch
the global tier moves demand from region to region while the token-time Phase 6's cost model charges
the receiving region's replicas, plus the round trip of every request moved, is below what it saves
at the sending region -- the planner's separable, convex cost, one level up, with no constant but
the epoch. Regions then forward a session by the table, so a session's turns stay together for the
epoch. Pre-measured (§8, *table*; same cells):

| | equal demand | burst | day |
|---|---|---|---|
| table, 1 s epoch, 250 req/s | +0.0 to +0.1% | +2.0 to +2.4% | +2.3 to +2.8% |
| table, 5 s epoch, 250 req/s | 0.0% | +9.3 to +19.0% | +3.2 to +3.7% |
| table, 1 s epoch, 325 req/s | 0.0% | +39 to +52% | +18 to +21% |
| table, 5 s epoch, 325 req/s | 0.0% | +70 to +90% | +35 to +45% |
| table, 1 s epoch, with the region-mean spill, 250 req/s | +0.7 to +0.8% | +1.3 to +1.5% | +1.7 to +1.9% |
| the same, 325 req/s | +1.1 to +1.2% | +1.7 to +2.0% | +2.1 to +2.3% |

The table is the only rule that costs nothing at equal demand at both loads: it moves nothing when
no region's mean is worse than another's. It misses what is shorter than its epoch -- a burst of 18
s is under four epochs of 5 s -- and near the knee it moves too little: Phase 6's cost model is a
mean-value model, and a region whose mean is below its capacity still saturates on its fluctuations,
which is ServiceRouter's other objection, that "modeling latency at high utilization is not robust".
With headroom it follows a day within 2.3-3.7%. Under the spill it adds nothing: the spill already
does its work.

**Chosen: the table as the global tier's routing decision on the provisioning clock, its epoch
swept, and never without the spill beneath it.** It is what the global record holds of routing
(§1.12), and its value is what it saves when nothing is wrong; the spill is what catches what it
misses.

### 1.8 What moves between regions is a node budget, and lateness is its price

`README.md` asks that "noticing that demand in a given region is high should spin up resources close
to that region". A region's hardware does not move: Meta's RAS re-solves which servers belong to
which reservation "at a regional level, continuously, every few tens of minutes", and its capacity
stays in its region. What can move is a **budget** -- how many nodes each region runs out of a
fleet-wide total -- where capacity is elastic: released in a region at its trough, acquired in
another at its peak, the acquisition a node's start plus its model's load. Where capacity is
reserved per region it cannot move at all, and only demand can (§1.6, §1.7).

Pre-measured on a day compressed into 240 s (§8, *budget*; three regions of six node slots, twelve
nodes running; the allocation that follows demand is the clairvoyant one, recomputed every 5 s from
the regions' shares and applied on time or late; a node acquired is cold and serves after its load;
mean service, seeds 1 / 2 / 3):

| 250 req/s a region; equal demand over 240 s, 457.0 / 455.2 / 454.1 ms | day, amplitude 0.5 | day, amplitude 0.75 |
|---|---|---|
| regional, four nodes a region throughout | 471.2 / 469.8 / 468.2 ms | 821.0 / 761.2 / 786.9 |
| budgets follow demand, 8 s load, on time | 461.4 / 459.7 / 458.9 | 464.5 / 462.8 / 461.4 |
| the same 10 s late | 463.5 / 461.9 / 460.8 | 468.9 / 467.7 / 466.7 |
| the same 30 s late | 491.4 / 506.1 / 481.7 | 918.4 / 941.8 / 871.2 |
| on time with a 30 s load | 475.3 / 480.3 / 471.9 | 558.2 / 561.8 / 525.8 |
| a static budget with a threshold spill (0.7, 1 s summary) | 465.8 / 463.6 / 463.5 | 471.3 / 469.4 / 469.1 |
| budgets on time with the same spill | 461.4 / 459.8 / 459.0 | 464.2 / 462.5 / 461.0 |
| a static budget with the table (5 s epoch) | 463.7 / 461.8 / 461.1 | 469.5 / 467.6 / 466.8 |
| budgets on time with the table | 461.5 / 459.8 / 459.0 | 463.9 / 462.3 / 461.1 |

| 325 req/s a region; equal demand over 240 s, 484.3 / 483.2 / 482.3 ms | day, amplitude 0.5 | day, amplitude 0.75 |
|---|---|---|
| regional, four nodes a region throughout | 1487.3 / 1362.1 / 1448.5 | 3591.9 / 3504.4 / 3596.6 |
| budgets follow demand, 8 s load, on time | 491.6 / 490.1 / 489.2 | 497.7 / 497.3 / 504.8 |
| the same 10 s late | 511.7 / 503.8 / 505.0 | 658.1 / 636.7 / 700.6 |
| the same 30 s late | 1249.6 / 1225.5 / 1256.8 | 3104.8 / 3060.9 / 3215.0 |
| on time with a 30 s load | 754.5 / 729.1 / 751.3 | 1814.5 / 1796.3 / 1998.5 |
| a static budget with a threshold spill | 496.7 / 495.2 / 494.2 | 499.4 / 498.1 / 497.7 |
| budgets on time with the same spill | 506.0 / 503.1 / 502.3 | 510.9 / 508.7 / 510.6 |
| a static budget with the table (5 s epoch) | 506.5 / 500.4 / 534.2 | 760.6 / 797.2 / 763.7 |
| budgets on time with the table | 491.3 / 490.3 / 489.0 | 496.2 / 495.4 / 495.1 |

Four findings.

- **A budget that follows the day recovers it.** On time, the day costs 1.0-1.7% against equal
  demand with headroom and 1.4-4.7% near the knee, where regional schedulers on fixed nodes cost
  3-80% with headroom and three to seven times the service near the knee.
- **Lateness is its price, as Phase 6 found for a placement.** Ten seconds late -- an hour of a real
  day -- costs 0.4-1.2% more than on time with headroom, and near the knee 3-4% at amplitude 0.5 and
  28-39% at 0.75; thirty seconds late is worse than never moving with headroom, because a budget
  three hours late moves capacity away from the region about to peak. A 30 s load costs 3-5% at
  amplitude 0.5 and 14-21% at 0.75 with headroom, less than 30 s of lateness because only the moving
  node is out. Real loads are minutes -- Foundry reports an initialisation of 3.9 s, vLLM's cold
  start is tens of seconds to minutes -- a fraction of a second of this run, so on a real day the
  lateness that matters is the planner's reaction, not the load.
- **Spill and budgets are complements, and a threshold misfires on budgets.** A static budget with a
  spill does almost as well as a budget that follows, because the spill moves demand instead of
  nodes. Near the knee a 0.7 threshold *with* following budgets is worse than without them: once the
  budgets equalise every region's utilisation near the threshold, every region spills to every other
  on noise -- 11-12.5% of client-facing requests leave their region, against 15% from the peak
  region alone on a static budget, where it pays.
- **The table needs the budget near the knee.** On a static budget at 325 req/s the table costs
  57-65% on the day at amplitude 0.75 -- §1.7's under-forwarding -- and with budgets that follow it
  costs 2.5-2.7%, the budget doing the work the table's mean-value model cannot.

**Chosen: a node budget per region, owned by the global tier and written to the global record,**
moved by Phase 6's rent-or-buy rule lifted to regions -- accrue the cost of the current allocation
against the best, and move when the accrued loss covers the move's cost (a node's start and load,
and the KV it leaves) -- with the clairvoyant allocation as the ceiling and the static one as the
floor. Loads and lateness are swept. A fleet of reserved capacity runs with the budget static.

### 1.9 A model in another region costs its round trip; a model in every region costs a knee

§9's "place it could bind": an agent and its model in different regions. With the fleet of Phase 6
-- one model per node, a batch per model, four models at 55 / 25 / 12 / 8% of demand -- three
regions of four nodes cannot host every model at the replica count its demand wants. Phase 6's
allocation over the whole fleet is [6, 3, 2, 1] replicas. Pre-measured (§8, *models*; 250 req/s a
region, equal regions, seeds 1 / 2 / 3; a request for a model its region does not host is forwarded
to the nearest region that does):

| placement | mean service, regional | the same, global argmin | client-facing requests served elsewhere |
|---|---|---|---|
| every model in every region, one replica each | 1833.0 / 1676.3 / 1794.0 ms | 1825.2 / 1686.2 / 1801.9 | none, regional |
| the fleet's counts, spread: two of model 0 and one of model 1 in each region, model 2 in two regions, model 3 in one | 462.6 / 457.7 / 464.6 | 474.2 / 469.7 / 476.7 | 3.4%, regional |
| the fleet's counts, concentrated: each region filled in turn | 469.3 / 464.4 / 471.4 | 478.9 / 474.4 / 481.2 | 14.3-14.6%, regional |

Putting every model in every region is not completeness, it is a knee: the model with half the
demand gets one replica a region, half of all decodes arrive at a full batch, and service is 3.6-4.0
times the alternative -- Phase 6's "a model with half the demand on two replicas is past its knee",
region by region. Forwarding the requests for models a region does not host costs their round trip:
the spread layout forwards 3.4% of client-facing requests and lands 0.7% above regional schedulers
on the published engine (§1.1), and the concentrated one forwards 14.3-14.6% and costs 1.4-1.5% more
than spread, about its extra share times the trip. The 61 ms "no placement moves" is paid only by
the demand whose model is elsewhere, and the decision that moves it is the global tier's count, not
any router's.

**Chosen: the global tier places replicas per region and model** with Phase 6's counts over the
whole fleet, laid out across regions by its demand; a region forwards a request for a model it does
not host to the nearest region that does, counted apart from spill. A replica moved into a region
for its own demand is the rent-or-buy of §1.8 with the round trip as the rent.

### 1.10 A tenant's regional share is a lease, and residency is a constraint

§8 has the global tier grant each region a budget per tenant. A tenant whose users follow the sun
moves its demand between regions daily. *Arithmetic on the generator's own day* (§8, *tenants*):
splitting a tenant's quota across regions by its mean share refuses 15.6% of its demand at amplitude
0.5 and 23.5% at 0.75, and with 25% of headroom 5.2% and 12.4%, all of it at the regions' peaks
while the quota idles elsewhere. Doorman, Google's global rate limiter, answers this with leases: a
server tree of clusters, regions and a root, each client granted capacity for a lease (60 s by
default) and expected to refresh (every 16 s), apportioned by fair or proportional share, and a
"safe capacity" for a client whose lease expires. In 16 s of a real day a region's share moves by at
most 0.02-0.04 points, so a refreshed lease refuses nothing measurable.

Residency is the other constraint. Azure's Data Zone deployments process prompts "only within the
specified data zone" (US, EU, Asia Pacific), and Bedrock's geographic cross-region inference keeps
requests within a geography. A tenant restricted to a set of regions cannot be spilled, tabled or
forwarded outside it, and in a set of one region it is regional with no cross-region rule at all --
the arms above that collapse.

**Chosen: a tenant's regional shares are leases from the global tier, refreshed on the provisioning
clock and soft** -- Doorman's servers hold them in memory, and a restarted master relearns them --
**while the tenant's global quota and its region set are in the global record.** Every cross-region
rule filters candidates by the region set. The share's split is arithmetic in the plan and a built
meter in the phase; residency is a condition, swept.

### 1.11 Active-active schedulers in a region are free at the engine's step

Two or four schedulers share a region's four nodes, each taking a session's requests by hash, each
reading its own decodes in flight exactly and the others' through reports. Pre-measured (§8,
*shards*; 250 req/s a region, equal demand; mean service, seeds 1 / 2 / 3, against one scheduler a
region at 459.5 / 454.7 / 461.4 ms):

| reports every | two schedulers | four schedulers |
|---|---|---|
| continuous (exact) | identical | identical |
| 25 ms, the engine's step | +0.0 / +0.0 / +0.0% | +0.0 / +0.0 / +0.0% |
| 250 ms | +0.1 / +0.0 / +0.1% | +0.3 / +0.2 / +0.2% |
| 1 s | +0.2 / +0.2 / +0.2% | +1.7 / +1.7 / +1.9% |
| 5 s | +0.2 / +0.4 / +0.6% | +30.2 / +27.6 / +29.4%; half of decodes at a full batch |

§1's step-aligned telemetry already carries what a scheduler needs from its peers -- the
`TelemetryBatchHeader`'s `queued` and `running` at 40-100 Hz -- and at that cadence sharing a region
costs nothing. The failure llm-d's RFC names appears where the reports are seconds old, and grows
with the number of schedulers: each sees its own dispatches and the others' load as it was, and all
of them choose the node that looked idle. Node agents' checks (`phase-10.md` §4.10) keep such
schedulers from over-admitting a partition; they do not keep them from herding.

**Chosen: active-active schedulers read their peers' load from the engines' step-aligned headers,**
the increment that makes `Machine` hold more than one view, with the report's age swept.

### 1.12 Two records, and what each writes

§8's shape: a record per region for what must keep working through a WAN partition, leases above
all, and a global record in FoundationDB's two-region mode for tenancy, the catalogue and the
regions' budgets. With §1.6 to §1.10 settled, what each holds and writes (*arithmetic*, §8,
*records*; at 10,000 nodes in three regions):

| record | holds | writes |
|---|---|---|
| per region | node leases, the region's replicas and roles, its partition sizes | liveness 0.1 a second a node (Phase 10): about 330 a second a region; the planner's placements, Phase 6's 0.046 a second on eight nodes |
| global | tenancy, global quotas and region sets, the catalogue, node budgets, model counts per region, the routing table | the table's six fractions an epoch (0.02 a second at Taiji's five minutes); budget moves, 14-18 a day for twelve nodes in the pre-measurement, 0.14-0.17 a second at 10,000; quota changes on the provisioning clock |
| soft at the global tier | tenants' regional leases, the regions' demand summaries | none: relearned after a restart, as Doorman's master relearns its clients' leases |

The global record changes well under once a second at fleet scale -- three orders below the regions'
records -- which is what FoundationDB's two-region mode, with a commit in one region and a WAN round
trip for the other, can afford. A region cut off from it keeps its last budget, table and leases
until they expire and then its safe values, its own capacity and its own demand: the static arms of
§1.6 to §1.8.

**Chosen: as tabled; the global record's writes counted, the regional ones as Phase 10 counts
them.**

### 1.13 Regimes, workloads, and how each effect is graded

- **The cluster.** Three regions of four of the `belief` cluster's nodes (4 GiB HBM, 8 GiB DDR and
  16 GiB `NVMe` each), rack within a region, 30 ms one way between regions and Azure's triangle as a
  second condition; six node slots a region where budgets move. The engine allocating KV (Phase 3),
  `scored + fetch` within a region, no control crossing charged. 10% fan-out, a flat class mix, 250
  and 325 req/s a region.
- **Shapes.** Equal demand; the 18 s burst at 1.8 and 2.25 times a region's share; the day at
  amplitude 0.5 and 0.75, over 60 s for request-clock rules and 240 s where budgets move.
- **The fleet** for models: Phase 6's corrected engine -- a batch per model, priced -- with four
  models at 55 / 25 / 12 / 8%.
- **Grading.** Mean service against regional schedulers at equal demand on the same seed and load;
  the share of client-facing requests served in another region, by class; the round trip they paid;
  the turn's stall, which carries the prefix a forwarded turn left behind; the worst region's turn
  p99; decodes arriving at a full batch; requests unserved (none in any pre-measurement). Three
  seeds a cell.

---

## 2. Predictions, stated first

`owned-and-observed.md` §7's rule. Eleven predictions, each attached to a claim it would rewrite.
Where a pre-measurement stands behind one, §8 says how it was taken; P8's split, P10 and P11 rest on
arithmetic alone, and say so. Any of them may be pre-measured again before the build; one whose
pre-measurement moves its band is restated before the run, with the original kept beside it.

**P1 -- With clients in regions, the published argmin serves two-thirds of requests elsewhere.**

At equal demand on three regions of four nodes, the global argmin with no round trip in its score
serves 60-70% of client-facing requests in another region, a warm `FaaS` call takes 35-45 ms instead
of under 1 ms, and mean service is 5-9% above regional schedulers at 30 ms one way and 15-22% on
Azure's triangle.

- *If right:* every region-distance figure published so far is restated as a figure about a cluster
  whose clients are nowhere, and the round trip becomes a term of the score (§1.1).
- *If wrong* (under half of client-facing requests elsewhere): the score's existing terms already
  keep a session where it started, and the round trip is a refinement rather than a correction.

**P2 -- The global argmin with the round trip priced is slower than regional schedulers when nothing
is wrong.**

At equal demand it serves 30-45% of client-facing requests elsewhere; mean service is 3-6% above
regional schedulers at 30 ms one way and 9-14% on Azure's triangle; a turn's stall is three to five
times the regional one at 30 ms; pricing the trip ten times over leaves 5-18% elsewhere and 1-3%
slower.

- *If right:* §9's "what the global argmin buys" is nothing at equal demand, the price of the split
  is graded against the best cross-region rule (§1.3), and §5's property 3 -- congestion and
  residency in one argmin -- is restated as a property within a region.
- *If wrong* (the global argmin within 1% of regional schedulers): the score's cross-region
  comparisons are sound, and the global argmin is the bound §9 assumed.

**P3 -- Regional schedulers alone collapse past about twice a region's share; any spill recovers to
within a few percent.**

At 250 req/s a region, the 2.25x burst costs regional schedulers +60 to +100% of mean service and
the day at amplitude 0.75 +12 to +30%; at 325, +180 to +280% and +130 to +200%. Every cross-region
rule built lands within 6% of equal demand at 250 and within 7% at 325, the table near the knee
excepted (P5).

- *If right:* §8's routing tier needs a spill beneath its budget, and the price of the split is what
  the spill costs: one to three percent.
- *If wrong* (regional schedulers within 10% on the burst at 250): a region's headroom absorbs its
  peak at the published load, and spill matters only near the knee.

**Measured (increments 2 and 3): P3 holds.** At 250 req/s a region the 2.25x burst costs regional
schedulers +71.7 to +82.1% and the day at amplitude 0.75 +16.6 to +23.3%; at 325, +222.6 to +249.6%
and +160.9 to +174.2%. Every cross-region rule built lands within 5.6% of equal demand at 250 and
4.3% at 325 on 30 ms round trips, the table near the knee excepted, and within 6.8% on Azure's.

**P4 -- A price on a stale summary herds; counting one's own forwards removes it; a region-mean
price halves what remains, and a threshold is right only at its load.**

On a 1 s summary at 30 ms one way the best-node price forwards 18-32% of client-facing requests at
equal demand and costs +2.5 to +6% in every shape. Counting its own forwards in flight brings it to
+1 to +2.5% at 250 and +1 to +3% at 325; pricing by each region's mean, to +0.4 to +1.2% at equal
demand and +1 to +2.5% under a peak (+3 to +4.5% on Azure's day). The best-node price on an exact
view lands at +0.5 to +1.5%. A threshold of 0.7 costs under 0.2% at equal demand at 250 and +1 to
+2.5% at 325; a threshold of 0.5 the reverse, and +5 to +8% on the day at 325.

- *If right:* the spill is priced and untuned, within about a point of the exact view, and the
  thresholds every shipped system tunes are a constant whose right value moves with the load.
- *If wrong* (counting own forwards no better than the raw summary): the herding is between senders,
  not self-inflicted, and a receiving region must grant an allowance -- Doorman's lease, for
  capacity rather than for a tenant.

**Measured (increment 2): P4 holds, with two rows below its band.** On a 1 s summary the node price
serves 15.8-19.1% of client-facing requests away at equal demand at 250 and 25.1-26.7% at 325
(18-32% predicted) and costs +2.0 to +2.4% there, below the band's +2.5, and +2.7 to +3.5% under a
peak; with own forwards +1.0 to +2.2% at 250 and +1.2 to +2.1% at 325 (+1 to +2.5% and +1 to +3%
predicted). The region mean with own forwards costs +0.6 to +0.7% at equal demand and +1.2 to +1.7%
under a peak at 250, +1.0 to +2.1% at 325, and +3.1 to +3.5% on Azure's day (+3 to +4.5%). The exact
view lands at +0.8 to +1.8% at 30 ms, above the predicted +0.5 to +1.5% on the day at both loads,
and +3.2 to +3.5% on Azure's. Threshold 0.7 costs 0.0% at equal demand at 250 and +1.2 to +1.4% at
325; threshold 0.5 the reverse, +1.4% and 0.0 to +0.2%, and on the day at 325 +3.8 to +4.0% where +5
to +8% was predicted (§9.6).

**P5 -- The table costs nothing at equal demand, misses what is shorter than its epoch, and
under-forwards near the knee.**

At equal demand the table is within 0.2% of regional schedulers at both loads and epochs. At 250
req/s a region it costs +1.5 to +3% on the burst at a 1 s epoch and +5 to +25% at 5 s, and +1.5 to
+4.5% on the day; at 325, +25 to +60% on the burst at 1 s and +12 to +30% on the day. With the
region-mean spill beneath it, it lands within 0.5% of the spill alone. On the day at 325 the 5 s
epoch costs more than the 1 s one, and is not graded.

- *If right:* the global tier's table is a provisioning-clock tool that needs headroom and a spill
  beneath it, and Phase 6's mean-value cost model is not a capacity model near the knee.
- *If wrong* (the table within 3% of the spill on the day at 325): the mean-value model suffices on
  the provisioning clock, and the spill is needed only for bursts.

**Measured (increment 3): P5 holds.** At equal demand the table is within +0.1% of regional
schedulers at both loads and both epochs, serving 0.2-0.8% of client-facing requests away. At 250
req/s a region it costs +2.0 to +2.4% on the burst at a 1 s epoch and +9.3 to +19.0% at 5 s, and
+2.3 to +3.7% on the day; at 325, +39.2 to +51.8% on the burst at 1 s and +17.6 to +20.8% on the
day. Over the region-mean overflow it lands within 0.3 points of the overflow alone.

**P6 -- Budgets that follow the day recover it, and lateness is their price.**

On a 240 s day with an 8 s load, budgets moved on time land within 0.5-2% of equal demand at 250
req/s a region and 1-5% at 325, where fixed budgets cost +2 to +5% at amplitude 0.5 and +50 to +100%
at 0.75 at 250, and 2.5 to 8 times the service at 325. Ten seconds late costs +0.3 to +1.5% more at
250 and +2 to +50% at 325; thirty seconds late is worse than fixed budgets at 250. A planner that
moves by rent-or-buy lands between on time and 10 s late.

- *If right:* `README.md`'s "spin up resources close to that region" is a budget on the provisioning
  clock whose price is its lateness, and on a real day -- loads of minutes, an hour being 10 s of
  this run -- the planner's reaction is what to measure.
- *If wrong* (the rent-or-buy planner worse than 10 s late): its accrual lags a day's ramp, and a
  forecast from the day before is the arm worth building.

**Measured (increment 3; regenerated after the review, §9.23): P6's costs hold and its rent-or-buy
clause fails.** On a 240 s day with an 8 s load, budgets moved on time cost +1.0 to +1.7% at 250
req/s a region and +1.5 to +4.7% at 325 (+3.2 to +5.1% under the threshold spill, §9.13), where
fixed budgets cost +3.1 to +3.2% at amplitude 0.5 and +67 to +80% at 0.75 at 250, and 2.8 to 3.1 and
7.3 to 7.5 times the service at 325. Ten seconds late costs +0.4 to +0.5 and +0.9 to +1.2 points
more at 250 and +2.8 to +4.2 and +29 to +41 points more at 325; thirty seconds late is worse than
fixed budgets at 250. The rent-or-buy planner lands not between on time and 10 s late but between 10
s late and 30 s late, and level with 10 s late in one cell of four (§9.13).

**P7 -- A model in another region costs its round trip; every model in every region costs a knee.**

With four models at 55 / 25 / 12 / 8% on three regions of four nodes, one replica of every model in
every region takes 3 to 5 times the service of the fleet's counts laid across regions, with 40-55%
of decodes arriving at a full batch. Spread across regions, the counts forward 2-5% of client-facing
requests for a model their region lacks and stay within 1.5% of regional schedulers on the published
engine; concentrated region by region, they forward 10-20% and cost +1 to +2.5% over spread.

- *If right:* §9's place-it-could-bind costs a percent, and the decision that moves it is the global
  tier's count, never a router's.
- *If wrong* (forwarding for a missing model costs more than 5%): sessions forwarded for their model
  lose their prefix every turn, and a region needs a replica of every model its sessions use.

**Measured (increment 3): P7 holds.** One replica of every model in every region is +268.7 to
+298.9% (3.7 to 4.0 times the service) with 48-49% of decodes arriving at a full batch. The fleet's
counts spread across regions serve 3.4% of client-facing requests in another region for a model
theirs lacks, and the forced forwards (which include flow downstreams and resumes) number 5.0-5.1%
of the client-facing requests; they cost +0.7% over regional schedulers on the published engine.
Concentrated region by region, they serve 14.3-14.6% away (forced 21%) and cost +1.4 to +1.5 points
more than spread.

**P8 -- A tenant's regional share is a lease.**

*Arithmetic on the generator's day:* a static split of a tenant's quota by its mean share refuses
15.6% of its demand at amplitude 0.5 and 23.5% at 0.75, 5.2% and 12.4% with 25% of headroom; a lease
refreshed every 16 s follows shares that move at most 0.04 points in that time. Built, a meter on
prefill work split statically refuses 10-25% of a global tenant's requests at the regions' peaks,
and a refreshed lease under 1%.

- *If right:* §8's per-tenant budget is a lease at the global tier, soft, and the record holds the
  tenant's global quota and region set.
- *If wrong* (a refreshed lease refuses more than 2%): the lease's refresh lags the peak's ramp and
  the share needs the table's epoch, not Doorman's.

**Measured (increment 4; regenerated after the review, §9.23): P8's mechanism holds and its figures
are not reproduced.** The arithmetic reproduces to the digit (§9.21). Built, on a meter of generated
tokens (§9.17), the day adds 2.2 to 2.6 points of refused requests to a static split with 10% of
headroom and 1.4 to 1.7 with 50%, at 250 and 325 req/s; to a lease refreshed every 0.1 or 0.5 s it
adds -0.2 to +0.3, and to one refreshed every 2 s +0.3 to +0.8. The absolute figures P8 states (a
static split refusing 10-25%, a lease under 1%) are for a meter on prefill work, which these runs do
not price; here every split has a floor from each tenant's own bursts, 2.2 to 3.0% of requests at
10% of headroom and 0.5 to 0.9% at 50% at equal demand (§9.18).

**P9 -- Active-active schedulers in a region are free at the engine's step and herd on seconds-old
reports.**

With reports every 25 ms, two and four schedulers a region land within 0.2% of one; at 1 s, +0.1 to
+0.5% for two and +1 to +3% for four; at 5 s, +0.1 to +1% for two and +20 to +40% for four, with
40-60% of decodes arriving at a full batch. With node agents' checks on, none over-admits at any
report age (not pre-measured: the published arm admits nothing to over-admit).

- *If right:* a region's scheduler scales out on the telemetry §1 already specifies, and the
  fleet-size ceiling §5 puts on one scheduler is divided by the number of schedulers at no cost.
- *If wrong* (four schedulers more than 1% slower at 25 ms): the step's headers miss what a peer
  dispatched since the last step, and active-active schedulers need a shared view, which is the
  stateful mode llm-d's RFC proposes.

**Measured (increment 4): P9 holds at 250 req/s a region and not at 325.** The pre-measurement
reproduces in all but one cell by 0.1 point. With reports every 25 ms two and four schedulers are
within +0.1% of one at both loads; at 1 s four cost +1.6 to +1.9% at 250 and +11.7 to +13.4% at 325,
and at 5 s +27.6 to +30.2% and +69.9 to +73.7%, half the decodes at a full batch at 250. Two
schedulers stay within +0.6% at every age and load. Node agents' checks are not built (§9.22).

**P10 -- A scheduler on the request path cannot be global, and a WAN partition is an outage for one
that is.**

*Arithmetic:* a global scheduler adds a round trip to two-thirds of requests -- a mean of 40 ms at
30 ms one way and 82-132 ms on Azure's triangle as it sits in East US, West Europe or Japan East,
500-3,000 times the sidecar tax §2.3 removed; a region cut off for 60 s at 250 req/s loses 450,000
request-seconds to it (`λD²/2`), against what it could not forward for a regional one.

- *If right:* §2.2's rule extends to the WAN: per-request decisions are regional, and §8's shape is
  forced, not chosen.
- *If wrong:* not applicable; this is arithmetic, and is checked only against the built round trip.

**Measured (increment 4): P10 holds.** On the built round trips a global scheduler adds a mean of
40.0 ms a request at 30 ms one way and 82.0, 105.3 or 131.3 ms on Azure's triangle as it sits in
East US, West Europe or Japan East: 534-910 and 1,094-2,987 times the sidecar tax, 4.0% and
8.2-13.1% of a one-second turn, and 450,000 request-seconds lost to a 60 s partition at 250 req/s.

**P11 -- The global record writes under once a second at 10,000 nodes.**

*Arithmetic:* the table's six fractions an epoch, budget moves of 0.1-0.2 a second, quota changes on
the provisioning clock: under 1 write a second globally, three orders below the regions' records,
whose liveness writes 1,000 a second.

- *If right:* FoundationDB's two-region mode, with its WAN round trip on every commit from the
  second region, carries the global record with room; the regions' records carry the rate.
- *If wrong* (above 10 a second): something on the request clock -- a tenant's lease refreshed
  through the record -- has been written durably, and belongs in the soft tier.

**Measured (increment 4): P11 holds.** Liveness is 333 writes a second a region at 10,000 nodes in
three regions. The global record writes the table's six fractions every 300 s (0.020 a second) and
the clairvoyant budget's 14 and 18 node moves a day at amplitude 0.5 and 0.75, which at 10,000 nodes
is 0.14 and 0.17 a second: 0.16 and 0.19 a second in all, 1,700 to 2,200 times fewer than one
region's liveness.

---

## 3. What a regional scheduler must and must not do

Eleven rules. The first is the gate; the third is the one most likely to be broken for a good
reason.

1. **The gate.** With every new bit off, byte-identical to `HEAD` on the thirteen-command set as
   `phase-10.md` left it; with one region, every region bit changes nothing. Checked after every
   work item.
2. **The trace is the same; regions differ in what they know.** A region is drawn from a stream of
   its own; every arm in a cell sees the same arrivals in the same regions, and differs only in what
   each region's scheduler knows and where it may send work.
3. **A region reads only its sources.** Its own engines exactly, because it carries every flight on
   them; another region through a summary on a clock plus its own forwards in flight; the global
   tier through the record. Arms that read more -- an exact view of other regions, a clairvoyant
   budget, the global argmin -- are bounds or comparisons, printed beside the arms and never called
   arms.
4. **A forwarded request is placed by the region it is sent to.** That region owns its capacity and
   admits within it; the sender chooses the region, never the node.
5. **The client pays the round trip once.** Client to serving region and back, charged to
   client-facing requests only -- a session's turn, a function call, a service request; tool calls,
   flow downstreams, resumes and a fan-out's agents pay the handoff the score already prices.
6. **KV does not cross regions.** A forwarded turn rebuilds what its new region lacks, as the
   acquire argmin already prices a region link; engine KV is never written (`own::authority`), and
   the census stays at 13.
7. **Nothing tuned.** Spill is priced in nanoseconds; the thresholds are the shipped comparison,
   swept and labelled as tuned; summaries, epochs, loads and lateness are conditions, swept.
8. **Residency is a filter.** A tenant's region set bounds every rule; a request is never served
   outside it.
9. **One bit per mechanism:** `--regions`, `--round-trip`, `--global`, `--spill`, `--summary`,
   `--own-forwards`, `--table`, `--budget`, `--load`, `--late`, `--schedulers`, `--report`,
   `--residency`; the headline runs change one at a time.
10. **Measure, do not repair** (`phase-2.md` rule 1). The global argmin is reported as it behaves;
    it is not retuned to stay home.
11. **Three seeds, both loads, and every figure with its shape, round trips and clock.** A figure on
    a compressed day says so, and says what a second of it is.

---

## 4. Work items

Five increments, in order, each ending with a number: regions and their clients; spill; the global
tier; active-active schedulers; and the command and the arithmetic. Within each, nothing that can
move a number lands before the items that cannot. If the phase has to stop early it stops at an
increment's end.

### 4.1 Regions in the topology

`Topology::regions`: regions of nodes, a distance within a region, and a matrix of one-way latencies
between regions at the region link's bytes. `Machine` gains a region per node. With one region it is
`Topology::cluster`, asserted.

### 4.2 Clients in regions

`Request` gains its client's region. The generator gains `--regions` with a shape -- even, a day
(`--amplitude`, peaks by region), a burst (`--burst region:share:from:to`) -- drawn from a stream of
its own; a session belongs to a region and its turns stay there; a function call or a service
request is drawn by the regions' shares at its instant; a flow downstream, a resume and a fan-out
inherit their producer's region.

### 4.3 The round trip

`reach` generalises `code-review`'s origin: a client-facing request served outside its client's
region pays the round trip between them, plus its bytes, as a term of the score (`--round-trip
priced`, the default with regions) and as a cost when it runs. `--round-trip unpriced` is the
published argmin, for P1.

### 4.4 Regional schedulers

A scope per request: the region whose scheduler places it. Candidates, the decode pool, tool calls,
fan-out agents and the host classes are filtered to the scope; a region's router queue and tenant
meters are its own. `--global` lifts the scope: the global argmin, as a comparison.

### 4.5 Instruments, first part

Per class: client-facing requests served in another region and the round trip they paid; the turn's
stall; requests forwarded for a missing model; by client region and over the run's windows: mean
service, the turn's p99, decodes arriving at a full batch.

### 4.6 Summaries and own forwards

Each region publishes, every `--summary` seconds, its nodes' decodes in flight. A sender adds its
own forwards still in flight to each region it reads (`--own-forwards`), which it knows exactly
because it relays their streams.

### 4.7 Spill

`--spill none | region-mean | node | threshold:<u> | exact`. Region-mean prices each region at its
nodes' mean engine and congestion terms on the summary, plus a cold rebuild of the chain and the
round trip, against the home region's mean on its own exact view plus its best acquire; node prices
the best node of each; threshold forwards to the nearest region below a utilisation; exact reads
every region's engines as they are, the node price's information bound. `FaaS` and service requests
are never spilled. A region with no replica of a request's model forwards it to the nearest region
that has one, counted apart.

### 4.8 The table

`--table <epoch>`: each epoch the global tier reads each region's decode tokens over the last epoch
and moves demand between regions while the token-time `Costs::cost_rate` charges the receiving
replicas, plus the round trip of the requests moved, is below what it saves; regions forward
sessions by hash at the table's fractions. Its writes are counted as the global record's.

### 4.9 Budgets

`--budget static | follow | clairvoyant` over node slots per region: a node released stops taking
work and drops its state; a node acquired is cold and serves after `--load`. `follow` is Phase 6's
rent-or-buy across regions -- accrue the cost of the allocation in place against the best, move when
the accrued loss covers a node's load and the KV it leaves -- and `clairvoyant` reads the
generator's shares, late by `--late`, a ceiling. Moves are the global record's writes.

### 4.10 Models by region

Under `--fleet` with regions, the planner's counts are taken over the whole fleet and laid across
regions by demand (`--layout spread | concentrated | everywhere`); the forced forward of §4.7 serves
a model a region lacks.

### 4.11 Tenants and residency

A tenant's regional share of its prefill quota: `--share static | lease:<refresh>`, the lease
re-split by each region's observed demand at every refresh. `--residency <share>`: that share of
tenants restricted to their region, filtering every rule.

### 4.12 Active-active schedulers

`--schedulers <k>` a region, taking sessions by hash; each holds its own decodes in flight exactly
and reads its peers' every `--report` seconds, the engine's step header by default. `Machine` holds
a view per scheduler of each node's load. With `--node-check`, a node refuses a dispatch its own
ledger does not admit, as in Phase 10.

### 4.13 Instruments

| instrument | measures | over |
|---|---|---|
| reach | client-facing requests served elsewhere, by class; the round trip paid | every run |
| stall | the turn's stall, by whether the turn was forwarded | every run |
| window | mean service and the turn's p99 by client region and by tenth of the run | every run |
| saturation | decodes arriving at a full batch, by region | every run |
| summary | the load a sender believed of a region against the load it found | every spill |
| table | fractions, recomputes and changes an epoch | every table |
| budget | moves, node-seconds loading, KV lost to releases | every budget |
| forced | requests forwarded for a missing model, by model and region | every fleet |
| lease | refusals by tenant, static and leased | tenants |
| views | per scheduler, the load read against the engine's own | active-active |
| records | the global record's writes, by kind | every run |

### 4.14 `polyphonic regions`

A reproducible sweep, as `durability` is for Phase 10: no control crossing charged,
seed-deterministic, three seeds a cell.

1. the gate, as printed check lines
2. clients in regions, the published and priced global argmin (P1, P2)
3. the burst and the day: regional schedulers and every spill (P3, P4)
4. the table (P5)
5. budgets on a 240 s day (P6)
6. models by region (P7)
7. tenants' shares (P8)
8. active-active schedulers (P9)
9. the arithmetic: a global scheduler on the path, a WAN partition, the records (P10, P11)

`distributed` and `code-review` take `--regions` for the published topologies with clients.

### 4.15 Report and publish

| target | change |
|---|---|
| `owned-and-observed.md` §9 Phase 11 | a **Status** line; the phase restated as the round trip, the spill, the table and the budget |
| `owned-and-observed.md` §8 | the record per region and the global one with what each writes (P11); the regional budget as a lease with a safe value through a partition; the global argmin's place |
| `owned-and-observed.md` §2.2 | the rule extended to the WAN (P10) |
| `owned-and-observed.md` §3.9 | the score's engine and congestion terms as a decision within a region (P2) |
| `owned-and-observed.md` §5 | property 3 within a region; the spill and the budget as a unified property or not (P3, P6) |
| `residency-ledger.md` | a *Regions* section; the region-distance results restated with clients; *Standing* rows for each prediction |
| `phase-6.md`, `phase-10.md` | nothing: they keep their results as measured |

---

## 5. Verification

- **Byte-identity with every new bit off**, against the commit before this phase, on the
  reproducible set at a reduced `--ops` and a second seed, after every work item; and with one
  region, every region bit on.
- **Every request closes once** or is counted unserved by cause; a forwarded request closes once, in
  the region that served it, with its round trip charged once.
- **A region reads only its sources:** with summaries off and spill on, no region reads another's
  engines; asserted.
- **KV never crosses a region:** no fetch between regions; a forwarded turn's acquire is a rebuild
  or a local hit; asserted.
- **Residency holds:** no request served outside its tenant's region set; asserted.
- **Budgets conserve nodes:** provisioned plus loading nodes equal the fleet's total at every
  instant; asserted.
- **The census.** `cargo build --release --features census` still emits 13 warnings.
- `cargo fmt --check`, `cargo clippy --all-targets` and `cargo test` clean.

---

## 6. Risks

1. **The day is compressed, and sessions are not.** A second of a 240 s day is six minutes; a
   session lives about two minutes of run, half a compressed day. A node released at a region's
   trough loses more sessions' KV than a real release would, so the cost of a move is overstated; a
   load of 8 s is 48 minutes, pessimistic. The direction of P6 holds; its sizes are for this
   compression.
2. **The engine is a mean-value engine at its knee.** Saturation in the simulator is a batch of 64
   with a step that grows by 40 us a sequence; a real engine near its knee preempts and recomputes.
   P3's and P5's figures near the knee are about this engine.
3. **Three equal regions are a small world.** Real regions differ in size -- the largest data centre
   Taiji routes to serves five times the traffic of its smallest -- and in their offsets; Azure's
   triangle is the one check against uniform round trips.
4. **The global sum is held constant.** A real day has a global peak where two regions overlap,
   which costs every arm the same capacity; holding the sum isolates the split and leaves the
   fleet's size to Phase 6.
5. **The pre-measurements ran the published arm, unenforced.** Phase 9's integrated arm --
   admission, the router's queue, the cancel -- adds a spill trigger, the router's own check
   failing, which no pre-measurement ran.
6. **A flat class mix.** The published phases interact with a region's peak (§1.5), and are set
   aside; a region's own class mix is not modelled.
7. **The table's cost model is Phase 6's,** a model of the mean; P5's failure near the knee may be
   the model's rather than the table's. A tail-aware cost -- the quantile Phase 9 claims against --
   is the repair, out of scope here.
8. **The pre-measurements are emulations outside the repository.** Regions were a scope on the
   copy's `Machine`; the summary a snapshot of engines' in-flight counts; the budget's allocation
   clairvoyant, recomputed every 5 s, and its released nodes drained through Phase 10's node loss;
   the table a greedy move of demand in steps of 0.5% of the total. §8 says how each was taken.
9. **Runtime.** A 60 s run of three regions takes about 4 s on this host and a 240 s day 15-40 s;
   the sweep is several hundred runs, an hour as parallel processes.

---

## 7. Out of scope

- **Injected WAN partitions.** P10 prices a partition by arithmetic on Phase 10's outage; a
  partition that cuts spill, the table and budget moves for a window is a fault arm for a later
  phase.
- **The records themselves.** FoundationDB's two-region mode, the regional records and the layer §8
  describes; the simulator counts the global record's writes and stores nothing.
- **KV across regions.** A prefix is rebuilt in its new region; shipping it over the WAN, which the
  acquire argmin prices and never chooses at 30 ms, is not built.
- **Capacity that cannot be acquired.** A budget assumes the node it moves to a region exists there;
  GPU stock-outs are the reason shipped services spill instead, and the static budget is their case.
- **Accelerator classes.** §8's third budget axis; the simulator's nodes are one class.
- **A forecast-driven budget planner.** §9's optional arm needs a day with real structure; on a
  sinusoidal day a forecast is the clairvoyant arm, which is built as the ceiling.
- **Fan-outs across regions.** A fan-out's agents stay in its region unless their model is
  elsewhere.
- **Edge devices,** the README's separate binary.

---

## 8. Pre-measurements

All taken on an instrumented copy of `7f16fef`, run outside the repository and not committed. With
every hook off the copy reproduces `belief --sections gate`, `residency`, `enforce --seeds 1
--sections gate,cancel`, `fleet --seeds 1` and `durability --seeds 1 --sections gate,count` byte for
byte at `--ops 3000 --seed 2`. Rows use three regions of four of the `belief` cluster's nodes -- 4
GiB HBM, 8 GiB DDR, 16 GiB `NVMe` and three units each -- rack within a region and 30 ms one way
between regions, `scored + fetch` within a region, the engine allocating KV with its grant sized
from the ledger's own run at equal demand, decode output not held, no control crossing charged, 10%
fan-out, a flat class mix (volatility 0), 60 s of arrivals and seeds 1-3, unless they say otherwise.
A session's region is the slot it occupies, so with one region the generator's draws are the
published ones; regions are drawn from a stream of their own.

| name | what | how | headline |
|---|---|---|---|
| *even* | clients in regions at equal demand | the global argmin over all twelve nodes with no round trip in its score, then with it; regional schedulers as a scope on the candidates; the round trip charged to client-facing requests served outside their region, 2 x 30 ms plus 16 KiB at the region link; 250 and 325 req/s a region, and Azure's triangle at 250 | §1.1, §1.3: 66% elsewhere unpriced, 37-41% priced; +4.1 to +4.6%, +11.1 to +12.2% on Azure's |
| *diag* | what the global argmin's spread is made of | as *even* at 250, with the congestion term removed, and the round trip priced at three and ten times its cost; first run under the published class phases, then under the flat mix | §1.3: no congestion term +5.3 to +15.9%; ten times the trip +1.4 to +2.1% |
| *phases* | the published class phases against a day | regional schedulers on the day at amplitude 0.5, under volatility 1 and under volatility 0 | §1.5: +27 to +37% against +2.8 to +2.9% |
| *knee* | the regional knee | regional schedulers at equal demand over 240 s, 250-400 req/s a region, by quarter of the run | §1.5: stable to 400, where 29.5% of decodes arrive at a full batch |
| *spill* | demand a region cannot serve | the burst (60% and 75% of arrivals to one region from 30% to 60% of the run) and the day (amplitude 0.5 and 0.75, peaks at 0.79, 0.54 and 0.21 of the run for East US, West Europe and Japan East); regional, global argmin, a priced spill on the best node of each region with an exact view or on a summary of each node's decodes in flight every 1 or 5 s, the same plus the sender's own forwards in flight, a region-mean price, a utilisation threshold of 0.5 or 0.7 forwarding to the nearest region below it; `FaaS` and service requests never forwarded; 250 and 325 req/s, and Azure's triangle | §1.6's tables |
| *table* | the global routing table | every 1 or 5 s, each region's decode tokens over the epoch; a greedy move of 0.5% of total demand at a time from region to region while `Costs::cost_rate` on the receiving replicas plus the round trip of the requests moved is below the saving; sessions forwarded by hash at the fractions; alone and over the region-mean spill | §1.7: 0.0% at equal demand; +39 to +52% on the burst at 325 |
| *budget* | node budgets that follow the day | six node slots a region and twelve nodes running, four a region to start; every 5 s the allocation by largest share per node, from the generator's shares, applied at once or 10 or 30 s late; a released node drained through Phase 10's node loss, an acquired one serving after 8 or 30 s; regional, a threshold spill and the table on top; a 240 s day at 250 and 325 req/s; equal demand over 240 s as the reference | §1.8's tables |
| *models* | models placed by region | Phase 6's fleet -- one model per node, a batch per model priced, models keyed -- with four models at 55 / 25 / 12 / 8% of demand in every phase and region; `best_counts` over twelve nodes ([6, 3, 2, 1]) laid out round-robin across regions or region by region, against one replica of every model in every region; a request for a model its region lacks forwarded to the nearest region with one; regional, global argmin, threshold spill | §1.9: 3.6-4.0 times for every model everywhere; 3.4% forwarded and +0.7% spread |
| *shards* | active-active schedulers | two or four schedulers a region taking sessions by hash; each scheduler's view of a node's load its own decodes in flight plus its peers' as last reported, every 0, 25 ms, 250 ms, 1 s or 5 s; equal demand at 250 req/s | §1.11's table |
| *tenants* | a tenant's regional shares | arithmetic on the generator's day: the share of demand above a region's mean share, with 0, 25 and 50% of headroom, and the largest change in a region's share over 16, 60 and 300 s of a real day | §1.10: 15.6 and 23.5% refused by a static split |
| *path* | a global scheduler on the request path | arithmetic: two-thirds of requests at one round trip; Phase 10's `λD²/2` for a region cut off | §1.2: 40-132 ms a request; 450,000 request-seconds a minute's cut |
| *records* | what each record writes | arithmetic: Phase 10's liveness of 0.1 a second a node; the table's fractions an epoch at Taiji's five minutes; the budget's moves in *budget*, scaled from twelve nodes to 10,000 | §1.12: the global record under once a second |

Sources read 2026-10-06: AWS's [cross-region inference
announcement](https://aws.amazon.com/blogs/machine-learning/getting-started-with-cross-region-inference-in-amazon-bedrock)
(2024-08-27), [global cross-Region
inference](https://aws.amazon.com/blogs/machine-learning/unlock-global-ai-inference-scalability-using-new-global-cross-region-inference-on-amazon-bedrock-with-anthropics-claude-sonnet-4-5)
(2025-10-03) and its [user
guide](https://docs.aws.amazon.com/bedrock/latest/userguide/global-cross-region-inference.html);
Microsoft's [deployment
types](https://learn.microsoft.com/en-us/azure/foundry/foundry-models/concepts/deployment-types) and
[network round-trip latency
statistics](https://learn.microsoft.com/en-us/azure/networking/azure-network-latency) (monthly P50
over 30 days, page dated 2026-07-30); Google's [capacity optimisation with global load
balancing](https://docs.cloud.google.com/load-balancing/docs/tutorials/about-capacity-optimization-with-global-lb)
and [multi-cluster GKE Inference
Gateway](https://cloud.google.com/blog/products/containers-kubernetes/gpu-and-tpu-utilization-with-multi-cluster-gke-inference-gateway)
(2026-09-21); Saokar et al., [ServiceRouter](https://www.usenix.org/system/files/osdi23-saokar.pdf)
(OSDI '23); Chou et al., [Taiji](https://tianyin.github.io/pub/taiji.pdf) (SOSP '19); Newell et al.,
[RAS](https://www.cs.cmu.edu/~dskarlat/publications/ras_sosp21.pdf) (SOSP '21); [Doorman's
design](https://github.com/youtube/doorman/blob/master/doc/design.md); Stojkovic et al.,
[DynamoLLM](https://arxiv.org/abs/2408.00741) (HPCA '25); and
[llm-d-router#1593](https://github.com/llm-d/llm-d-router/issues/1593) (2026-06-11).

---

## 9. What the build found

Increment 1, in the order the findings arrived.

### 9.1 Where it landed

`Topology::regions` builds regions of nodes with a distance within a region and a matrix of one-way
latencies between them; with one region it is `Topology::cluster`, which a test asserts link by
link. `Request` gains its client's region and `Request::client_facing`, true for what a client sent:
not a flow downstream or resume (`completes`), a tool call or a fan-out. The generator's
`with_regions` takes a `RegionDemand` (even, a skew, a day) and draws a region from a stream of its
own; a session occupies one region's slots, so its turns stay there, and with one region the trace
is the published one, asserted. `Machine` holds `Regions` -- the node-to-region map, the matrix, the
mode (`Global` or `Regional`), whether the round trip is priced, and the instruments -- a scope per
request, and the client's region. The round trip is a term of `Terms::full`, charged by `reach` when
the request runs, and the instruments live in `RegionStats`.

### 9.2 The build reproduces the pre-measurement to the digit

`polyphonic regions --sections even` on seeds 1-3 gives regional schedulers 459.5 / 454.7 / 461.4 ms
(§1.1), the global argmin with no round trip in its score +7.1 / +7.2 / +7.1% with 66.3-66.7% of
client-facing requests served away and a warm `FaaS` call at 40.4-40.7 ms, and with the round trip
priced +4.4 / +4.6 / +4.3% with 36.9-40.1% away (§1.3). P1 and P2's first halves stand as predicted
for the 30 ms case; the Azure-triangle rows are not yet built into the command.

### 9.3 A queued request enters its own region

The emulation set the scope when a request arrived and left it set for whatever the router queue
served next, which no pre-measurement noticed because none ran a queue. The router queue now enters
each waiting request's region before it decides, and restores the caller's scope after. Where a
request waits at the router, the region that served it is not recorded (`served_in` is empty), so
the served-away share counts only requests placed at once; every figure so far runs without a queue.

### 9.4 What increment 1 did not build

Everything from §4.6 on, which §9.5 to §9.9 cover for increment 2, §9.10 to §9.15 for increment 3,
and §9.16 lists for what is left.

### 9.5 Increment 2: where it landed

`Regions` gains an `overflow` rule (`Off`, `Node`, `RegionMean`, `Threshold`), a summary period, and
whether a sender counts its own forwards. The plan calls the rule *spill*; the type is `Overflow`,
because `fault::Spill` already names whether an engine crash keeps its `NVMe`. A summary is a
snapshot of every node's decodes in flight taken every `summary_ns` (zero is the exact view); a
sender's own forwards are the decodes it sent to another region that have not ended, which it knows
because it relays them, and the correction is those in flight now less those in flight at the
snapshot, per node of the receiving region. `enter_region` makes the decision for a request -- home,
or the region the rule picks -- and the router queue calls it again for each request when it leaves
the queue. A request whose model its chosen region lacks goes to the nearest region that has it
(`forced`); without a fleet no request is forced.

### 9.6 The build reproduces the pre-measurement, except where the emulation let work cross for free

At equal demand and 250 req/s a region the build gives regional schedulers 459.5 / 454.7 / 461.4 ms
and the exact-view node price +0.8%, the region-mean price on a 1 s summary with own forwards +0.6
to +0.7%, threshold 0.7 0.0%, the global argmin +4.3 to +4.6%, and under the 0.75 burst and the 0.75
day regional schedulers +71.7 to +82.1% and +16.6 to +23.3% -- §1.6's figures. Three families of
rows differ, all in the direction of less overflow: the node price on a 1 s summary is +2.0 to +2.4%
at equal demand where §1.6 has +3.3 to +3.4%, threshold 0.5 +1.4% where it had +1.8%, and at 325
req/s threshold 0.5 on the day is +3.8 to +4.0% where it had +6.3 to +6.9%.

The cause is in the emulation. Its node price and its threshold applied to every request that
decodes, not only to what a client sent, so a flow downstream or a fan-out's resume could move to
another region and pay no round trip, since the round trip is charged to client-facing requests only
(rule 5). §4.7 forwards client-facing requests that decode, and the build does. Nothing else
differs: the region-mean rows, which were always for client-facing requests, reproduce, and the
conclusions do not move -- the exact view is the bound, the summary's herding is removed by the
sender's own forwards, and the threshold's right value moves with the load -- but §1.6's node-price
and threshold-0.5 rows were optimistic by a point or two of overflow, and the threshold's failure at
325 req/s on the day is smaller than §1.6 said (+3.8 to +4.0% against 0.7's +2.5 to +2.8%).

### 9.7 What the build measured

Mean service against regional schedulers at equal demand on the same seed and load, the range over
seeds 1-3, three regions of four nodes, `polyphonic regions` (`--rate`, `--rtt`), 60 s. The burst is
region 0 taking 75% of arrivals from 30% to 60% of the run, the day amplitude 0.75.

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

On Azure's round trips at 250 req/s: the global argmin priced +11.2 to +12.2% at equal demand and
+9.5 to +12.3% on the day; the exact-view node price +0.8%, +2.0 to +2.1% and +3.2 to +3.5%; the
region-mean price +0.6 to +0.7%, +1.9 to +2.0% and +3.1 to +3.5%; threshold 0.5 +3.0 to +3.4%, +4.4
to +4.7% and +6.0 to +6.8%. A forwarded request pays more there and fewer are worth forwarding: 3%
of requests leave at equal demand against 6% at 30 ms. Every cell served every request. The rows
with own forwards were re-run after the defect of §9.11 and are the figures here; before it they
were 0.0 to 0.3 points different.

### 9.8 A queued request is re-decided when it leaves the router's queue

A request that waits at the router may wait long enough for the summary to have moved, so the
overflow decision is made again when it leaves, and its client's region is what `reach` charges.
`reach` asserts that the client region it holds is the placed request's own and that the request is
one a client sent; with the router-queue fix disabled the test for it fails on that assertion, which
the served-away count alone did not catch, because it compares against the same stale region.

### 9.9 A summary counts once, and a single region has nothing to forward to

With one region every overflow rule, summary and own-forwards setting changes nothing, which the
command's gate checks to the bit. The summary is refreshed on arrivals, so a sparse trace's summary
is as old as its last arrival and not as old as its period: a property of the clock the simulator
keeps, which a deployment's timer does not share.

### 9.10 Increment 3: where it landed

The table is a `RoutingTable` on `Regions`: each epoch the global tier reads the decode tokens each
region's clients sent over the last epoch, moves demand from region to region in steps of 0.5% of
the total while the token time `Costs::cost_rate` charges the receiving region's nodes, plus the
round trip of the requests moved, is below what it saves at the sender, and publishes each region's
fractions. A client-facing request is sent by a hash of its session and the epoch, so a session's
turns stay together for an epoch; a request the table leaves home meets the overflow rule, so the
two compose and `--table` alone is the table without a spill beneath it.

`Budgets` hold which node slots run, when each becomes ready, and one of three rules. `Static` is
the nodes that run. `Planned` applies a list of allocations at instants, which the command fills
from the generator's own shares, so it is the clairvoyant arm and its lateness is a parameter.
`Follow` is Phase 6's rent-or-buy lifted to regions: every epoch it reads the demand each region's
clients sent, allocates the fleet's nodes by `Costs::best_region_counts` (the planner's separable
convex cost, one node at a time to the region whose token time falls most), accrues what the
allocation in place costs over the best, and moves when the accrued loss covers the move: the token
time lost while the released nodes are gone and the acquired ones load, and the KV the released
nodes held. A released node is the one with the least KV worth keeping; it is drained as a lost node
is, and an acquired one is cold and serves after its load. Nothing places on a node that is not
running or still loading (`available`).

Models by region are the command's: the fleet's counts over the whole fleet (`best_counts`, [6, 3,
2, 1] for twelve nodes), laid across regions round-robin or region by region, each node a replica of
one model with the partition its weights leave, a batch per model priced; a request for a model its
region lacks takes the forced forward of §9.5.

### 9.11 A fan-out's agents were counted as forwarded clients

Increment 2 built the agents' probe requests with region 0, and a probe looks like a client's
request (no `completes`, not a tool call, not a gang), so an agent served outside region 0 was
recorded as a request forwarded from region 0 and added to the own-forwards correction of every
region it was served in. `serve_gang` now gives each probe its gang's region, and a forward is
recorded only for the request the scope was entered for. The own-forwards rows moved by 0.0 to 0.3
points and §9.7 is regenerated; the test that found it asserts that with no overflow rule and a
trace full of fan-outs nothing is ever forwarded, and fails on the earlier build.

### 9.12 The table

Mean service against regional schedulers at equal demand on the same seed, the range over seeds 1-3,
60 s; the burst is region 0 taking 75% of arrivals from 30% to 60% of the run and the day has
amplitude 0.75. It reproduces §1.7 to the digit.

| 250 req/s a region | equal demand | burst | day |
|---|---|---|---|
| regional | 0 | +71.7 to +82.1% | +16.6 to +23.3% |
| table, 1 s epoch | +0.1% | +2.0 to +2.4% | +2.3 to +2.8% |
| table, 5 s epoch | 0.0% | +9.3 to +19.0% | +3.2 to +3.7% |
| table, 1 s epoch + region mean | +0.8% | +1.3 to +1.5% | +1.7 to +1.9% |
| table, 5 s epoch + region mean | +0.6 to +0.7% | +1.3 to +1.5% | +1.7 to +2.0% |

| 325 req/s a region | equal demand | burst | day |
|---|---|---|---|
| regional | 0 | +222.6 to +249.6% | +160.9 to +174.2% |
| table, 1 s epoch | 0.0 to +0.1% | +39.2 to +51.8% | +17.6 to +20.8% |
| table, 5 s epoch | 0.0% | +70.3 to +89.7% | +35.2 to +44.8% |
| table, 1 s epoch + region mean | +1.1 to +1.2% | +1.7 to +2.0% | +2.1 to +2.3% |
| table, 5 s epoch + region mean | +1.1 to +1.2% | +1.8 to +1.9% | +2.1 to +2.3% |

### 9.13 The budgets, and what rent-or-buy costs

A 240 s day over twelve running nodes on six slots a region, mean service against regional
schedulers at equal demand over 240 s on four nodes a region (about 457 / 455 / 454 ms at 250 req/s
and 484 / 483 / 482 at 325), the range over seeds. *On time*, *10 s late* and *30 s late* are the
clairvoyant allocation with an 8 s load; *30 s load* is on time with a 30 s load; *rent-or-buy* is
the planner of §9.10 with an 8 s load and a 5 s epoch. Increment 3's build reproduced §1.8's
clairvoyant columns to the digit; the KV grant the review corrected (§9.23) moves the rows below by
at most 0.4 points a seed, except the threshold's at 325 req/s, by up to 6.0 (the 30 s load at
amplitude 0.75, a cell whose seeds already spanned 17 points).

| 250 req/s, amplitude 0.5 | static | on time | 10 s late | 30 s late | 30 s load | rent-or-buy |
|---|---|---|---|---|---|---|
| regional | +3.1 to +3.2% | +1.0% | +1.4 to +1.5% | +6.1 to +11.2% | +3.9 to +5.6% | +1.8 to +3.0% |
| threshold 0.7 | +1.9 to +2.1% | +1.0 to +1.1% | +1.3 to +1.4% | +2.4 to +2.5% | +3.4 to +3.5% | +1.4 to +1.7% |
| table 5 s | +1.5% | +1.0 to +1.1% | +1.3 to +1.4% | +2.2 to +2.3% | +3.4 to +3.6% | +1.4 to +1.6% |

| 250 req/s, amplitude 0.75 | static | on time | 10 s late | 30 s late | 30 s load | rent-or-buy |
|---|---|---|---|---|---|---|
| regional | +67.2 to +79.6% | +1.6 to +1.7% | +2.6 to +2.8% | +91.8 to +107.2% | +15.8 to +23.5% | +4.0 to +10.4% |
| threshold 0.7 | +3.2 to +3.4% | +1.5 to +1.6% | +2.2% | +3.6 to +3.9% | +5.3 to +5.5% | +2.3 to +2.4% |
| table 5 s | +2.7 to +2.8% | +1.5 to +1.6% | +2.1 to +2.2% | +5.6 to +7.4% | +6.2 to +8.2% | +2.2 to +3.2% |

| 325 req/s, amplitude 0.5 | static | on time | 10 s late | 30 s late | 30 s load | rent-or-buy |
|---|---|---|---|---|---|---|
| regional | +181.9 to +207.1% | +1.5% | +4.3 to +5.7% | +153.7 to +160.6% | +50.9 to +55.9% | +9.6 to +12.8% |
| threshold 0.7 | +2.0 to +2.1% | +3.2 to +3.7% | +3.1 to +3.4% | +3.8 to +4.1% | +11.4 to +12.7% | +3.3 to +3.6% |
| table 5 s | +3.6 to +10.8% | +1.4 to +1.5% | +2.3 to +2.7% | +13.2 to +15.0% | +16.7 to +20.4% | +3.1 to +3.6% |

| 325 req/s, amplitude 0.75 | static | on time | 10 s late | 30 s late | 30 s load | rent-or-buy |
|---|---|---|---|---|---|---|
| regional | +625.3 to +645.7% | +2.8 to +4.7% | +31.8 to +45.3% | +533.7 to +566.5% | +271.8 to +314.0% | +16.3 to +39.6% |
| threshold 0.7 | +2.6 to +2.9% | +4.3 to +5.1% | +4.9 to +5.1% | +9.2 to +10.4% | +45.6 to +60.1% | +5.8 to +6.2% |
| table 5 s | +57.1 to +65.0% | +2.5 to +2.7% | +6.0 to +7.3% | +99.4 to +109.6% | +117.9 to +144.7% | +5.0 to +9.2% |

Four findings, three of them §1.8's and one new.

- **Rent-or-buy is neither on time nor free.** It recovers the day -- +1.8 to +3.0% where fixed
  budgets cost +3.1 at amplitude 0.5, and +4.0 to +10.4% where they cost +67 to +80% at 0.75 -- and
  it lands between 10 s late and 30 s late in three cells of four and level with 10 s late at 325
  req/s and amplitude 0.75. Its lateness is the epoch plus the accrual: it waits until the loss it
  has suffered covers the move, which is its rule, and a day's ramp is too steep for that to be
  quick. P6 predicted it would land between on time and 10 s late, and it does not.
- **A spill beneath the budget absorbs the planner's lateness.** Seed by seed, with the threshold or
  the table over it, rent-or-buy is within 1.3 points of 10 s late in every cell bar one seed of the
  table at 325 req/s and amplitude 0.75 (+1.9, the other two seeds below 10 s late); without one it
  is 0.4 to 8.1 points behind in the three cells where it lands between: the request clock covers
  what the provisioning clock misses.
- **A threshold misfires on budgets near the knee** (325 req/s, amplitude 0.5): +3.2 to +3.7% on
  time against +2.0 to +2.1% on a static budget, and +4.3 to +5.1% against +2.6 to +2.9% at 0.75, as
  §1.8 found.
- **The table needs the budget near the knee.** On a static budget at 325 and amplitude 0.75 it
  costs +57.1 to +65.0%, and on time +2.5 to +2.7%.

### 9.14 Models by region

Mean service against regional schedulers at equal demand on the published engine (§1.1), 250 req/s a
region, four models at 55 / 25 / 12 / 8% of demand; the fleet's counts for twelve nodes are [6, 3,
2, 1]. It reproduces §1.9 to the digit.

| placement | regional | global argmin | threshold 0.7 | served away; forced forwards |
|---|---|---|---|---|
| every model in every region | +268.7 to +298.9% | +270.9 to +297.2% | +268.7 to +298.9% | none |
| the fleet's counts, spread | +0.7% | +3.2 to +3.3% | +0.7% | 3.4%; 5.0-5.1% of client-facing requests |
| the fleet's counts, concentrated | +2.1 to +2.2% | +4.2 to +4.3% | +2.1 to +2.2% | 14.3-14.6%; 21.2-21.4% of client-facing requests |

A forward for a missing model is not an overflow: the threshold never moves a request there, so its
column is the regional one, and the table and the budget are what would move a replica.

### 9.15 What differs from the emulation, and what is new

The table, the clairvoyant budget and the models are the emulation's to the digit. The allocation
the clairvoyant budget applies is the emulation's -- one node at a time to the region with the
largest share per node -- while `follow` allocates by the cost model, so the two differ in the
allocator as well as in what they know; the numbers do not show it, the rent-or-buy planner's
lateness being much larger than an allocator's difference. `follow` is new to the build. The
emulation's table called its spill rule the same way (`TableSpill`); here the two are a table and an
overflow rule that compose.

### 9.16 Increment 4: where it landed

**Active-active schedulers.** `Machine::set_shards(k, report_ns)` gives a region's requests to one
of `k` schedulers by a hash of the program, or of the request's number when it has none, so a
session's turns stay with one. A scheduler holds the end of every decode it dispatched, per node,
and reads the others' counts: exactly (`report_ns` of 0), or as of the last report, refreshed every
`report_ns`. Its view of a node's load is its own flights plus its peers', capped at the batch size
as an engine's own report is, and it is the last place a belief looks: an engine's header or a
shadow ledger, where one exists, wins. With one scheduler there is no view and the run is the
published one; the gate runs two schedulers with exact reports in one region.

**Tenants' shares.** `TenantShares` holds, per tenant, a token bucket in each region refilled at the
tenant's quota times the region's share, two seconds deep. A client-facing request is refused when
its region's bucket cannot cover its tokens. The share is fixed at an even split, or a lease: at
each refresh, the first a lease after the first request, the global tier reads the tokens each
region's clients offered since the last one, sets each region's share to its fraction of them with a
floor of 2%, and halves what it remembers. One share a region serves every tenant: the generator
draws a request's tenant independently of its region, so each tenant's regional shares are the
regions'. Refused requests are unserved and counted apart from the router's.

**Residency.** `Machine::set_residency(share)` restricts that share of tenants, chosen by a hash of
the tenant. A restricted tenant's request is confined: the table and the overflow rule do not move
it, the forced forward for a missing model does not take it, and a decode pool that is empty in its
region is not widened to every node, so a request whose model its region lacks goes unplaced and is
counted as such rather than leaving.

**The command.** `tenants` runs the shares at 10% and 50% of headroom over a quota set from the
tenant's own tokens in the trace, on equal demand and the day at amplitude 0.75, with the lease
refreshed every 0.1, 0.5 and 2 s, then residency at 0, 25, 50, 75 and 100% under the region-mean
overflow on the 0.75 burst. `shards` runs two and four schedulers at five report ages. `arithmetic`
prints the three arithmetic sections. The sections' arms are fixed in the command rather than flags:
`--share`, `--residency`, `--schedulers` and `--report` of §4.11 and §4.12 are fields of the arms'
configuration, set by each section.

### 9.17 The tenants' meter is on generated tokens

§4.11 says a tenant's share is of its prefill quota. A prefill meter exists on `Machine`
(`set_tenant_quota`), and it needs the prefill priced (`set_prefill_time`), which the region runs do
not do and which would change every baseline in this phase's sections. The shares meter a request's
generated tokens instead, which every request carries. The quota is the tenant's mean token rate
over the run, scaled by its headroom. Two things follow. A tenant's requests are bursty on a
two-second bucket, so a static split refuses a floor of its demand even at equal demand, and P8's
figures for prefill work are not comparable to these. And the refused requests are not served, so a
mean service against regional schedulers at equal demand falls with them (-6.5% for a static split
at equal demand at 250 req/s) and the rows below report refusals, which are what a lease changes,
rather than service.

### 9.18 The tenants

Requests refused, as a share of all requests, the range over seeds 1-3, 60 s, at 250 and 325 req/s a
region; the day has amplitude 0.75. Regenerated after the review corrected the lease (§9.23).

| 250 req/s, headroom | 10% equal demand | 10% the day | 50% equal demand | 50% the day |
|---|---|---|---|---|
| static split | 2.5-2.6% | 4.8-5.0% | 0.6-0.7% | 2.2-2.3% |
| lease, every 0.1 s | 2.9-3.0% | 2.8-3.0% | 0.9% | 0.9-1.1% |
| lease, every 0.5 s | 2.6-2.7% | 2.7-2.9% | 0.7% | 0.8-1.0% |
| lease, every 2 s | 2.5-2.6% | 3.2-3.3% | 0.6-0.7% | 1.2-1.3% |

| 325 req/s, headroom | 10% equal demand | 10% the day | 50% equal demand | 50% the day |
|---|---|---|---|---|
| static split | 2.2% | 4.5-4.8% | 0.5% | 1.9-2.2% |
| lease, every 0.1 s | 2.5-2.6% | 2.4-2.6% | 0.6-0.7% | 0.7-0.8% |
| lease, every 0.5 s | 2.3% | 2.2-2.4% | 0.5-0.6% | 0.5-0.6% |
| lease, every 2 s | 2.2-2.3% | 2.7-3.0% | 0.5% | 0.9-1.0% |

- **A lease removes what the day adds.** Seed by seed, the day adds 2.2 to 2.6 points of refusals to
  a static split at 10% of headroom and 1.4 to 1.7 at 50%; to a lease refreshed every 0.1 or 0.5 s
  it adds -0.2 to +0.3, inside the seeds' spread.
- **A fast refresh chases noise and a slow one lags.** At equal demand a lease refreshed every 0.1 s
  refuses 0.1 to 0.4 points more than a static split, and one refreshed every 0.5 or 2 s is within
  0.1 of it; on the day the 2 s lease refuses 0.3 to 0.8 points more than at equal demand. Only the
  0.5 s refresh is within 0.1 of the static split at equal demand and within 0.3 of itself on the
  day.
- **Both failures are the compression's.** The run compresses a day 1,440 times and its arrivals not
  at all, so a refresh every 0.1, 0.5 and 2 s of the run sees 0.1, 0.5 and 2 s of arrivals and 2.4,
  12 and 48 minutes of the day's movement. Doorman's 16 s refresh sees 160 times the arrivals of the
  0.1 s arm and a ninth of its movement -- a region's share moves at most 0.04 points in it (§9.21)
  -- so neither the noise nor the lag measured here should reach a real lease.
- **Serving what a static split refuses costs a region that cannot spill.** Mean service with the
  refused requests served is +106.3 to +121.6% at 325 req/s and 10% of headroom for a lease and
  +13.4 to +15.2% for a static split that shed them, regional schedulers having nowhere to put a
  peak: the lease is the budget, and the overflow of §9.7 is what absorbs the peak it admits.
- **P8's "if wrong" clause, on its letter, trips and on its reason does not.** A lease refuses more
  than 2% on the day at 10% of headroom (2.2 to 3.3%), but a static split refuses 2.2 to 2.6% there
  at equal demand: what the lease refuses is the floor every split has, not lag at the peak's ramp.

### 9.19 Residency

Mean service against regional schedulers at equal demand, the 0.75 burst, region-mean overflow on a
1 s summary with the sender's own forwards, the range over seeds 1-3; a restricted tenant never
leaves its region. The share of client-facing requests served away is 12.3 / 11.8 / 11.6% with no
tenant restricted at 250 req/s, 15.9 / 15.7 / 16.6% at 325.

| tenants restricted | 250 req/s | served away | 325 req/s | served away |
|---|---|---|---|---|
| none, regional schedulers only | +71.7 to +82.1% | 0% | +222.6 to +249.6% | 0% |
| 0% | +1.2 to +1.4% | 11.6-12.3% | +1.7 to +1.8% | 15.7-16.6% |
| 25% | +2.1 to +3.6% | 6.7-7.1% | +34.0 to +39.8% | 9.7-9.9% |
| 50% | +3.5 to +13.4% | 5.8-6.1% | +55.5 to +67.5% | 8.1-8.4% |
| 75% | +21.6 to +33.6% | 3.5-3.8% | +121.2 to +128.2% | 4.5-5.1% |
| 100% | +71.7 to +82.1% | 0% | +222.6 to +249.6% | 0% |

- **Residency is a graded cost, not a switch.** At 250 req/s a quarter of the tenants staying home
  costs 2.1 to 3.6%, half 3.5 to 13.4%, three quarters 21.6 to 33.6%, and all of them is the
  regional collapse. At 325 even a quarter costs +34.0 to +39.8%, because the hot region has no
  headroom for the demand that cannot leave.

### 9.20 Active-active schedulers

Mean service against one scheduler a region at equal demand, the range over seeds 1-3 (the 250 req/s
column reproduces §1.11 in all but one cell: four schedulers at 1 s, seed 1, +1.6 here against
+1.7).

| reports every | two schedulers, 250 | four, 250 | two, 325 | four, 325 |
|---|---|---|---|---|
| continuous (exact) | identical | identical | identical | identical |
| 25 ms | +0.0% | +0.0% | +0.0 to +0.1% | +0.1% |
| 250 ms | +0.0 to +0.1% | +0.2 to +0.3% | +0.0 to +0.1% | +0.3 to +0.5% |
| 1 s | +0.2% | +1.6 to +1.9% | +0.1 to +0.3% | +11.7 to +13.4% |
| 5 s | +0.2 to +0.6% | +27.6 to +30.2%; 51-53% of decodes at a full batch | +0.0 to +0.1% | +69.9 to +73.7% |

- **At the engine's step a region's scheduler scales out for nothing.** P9's if-wrong threshold,
  four schedulers more than 1% slower at 25 ms, is not approached at either load.
- **Herding grows with the schedulers and with the load.** Four schedulers on reports a second old
  cost +11.7 to +13.4% at 325 where P9 gave +1 to +3% at 250: the herding P9 names, each choosing
  the node that looked idle, costs more with less headroom. Two schedulers do not herd at either
  load.
- **The exact view is the single scheduler's.** With continuous reports two and four schedulers
  place every request as one does, asserted by a test on the total service time of a trace; it fails
  if the view is left uncapped at the batch size, which an engine's own report is.

### 9.21 The arithmetic

The three arithmetic sections print what P8, P10 and P11 state, from the build's own round trips,
generator and plan. `tenants` reproduces §1.10's 15.6 and 23.5% and 5.2 and 12.4%, adds that 50% of
headroom refuses 0.3 and 4.2%, and that a region's share moves by at most 0.023 and 0.037 points in
16 s of a real day, 0.088 and 0.137 in 60 s and 0.439 and 0.686 in 300 s. `path` reproduces P10's 40
ms and 82-132 ms. `records` reproduces §1.12's 14-18 node moves a day, taking them from the
clairvoyant plan of §9.13 and scaling the twelve nodes to 10,000.

### 9.22 What is not built

- `--regions` on `distributed` and `code-review`, which would give their published topologies
  clients. Those commands' output is byte-identical to the build before this phase, and the question
  the published figures raise -- what a request reaches and leaves for free -- is answered by the
  `even` section (§9.2).
- Node agents' checks for active-active schedulers (§4.12, `--node-check`), which P9's last clause
  concerns and which the published arm gives nothing to over-admit.
- The per-scheduler `views` and per-tenant `lease` instruments of §4.13: the build counts refusals
  in all, not by tenant, and does not print a scheduler's view against the engine's.
- A tenant's region set larger than one: residency is a restriction to the client's own region.
- A forecast-driven budget planner, which §7 leaves out; P6's *if wrong* branch is that this is the
  arm worth building.
- A figure for P3 on a router queue or an engine that waits: every cross-region run is unenforced,
  as the pre-measurements were.


### 9.23 What a review found

A review of the phase's commit found two defects that move published numbers, six that move none,
and five it left open. Every section was rerun with the fixes and compared with the outputs this
section records.

- **A lease refreshed on its first request.** `TenantShares` started with its next refresh at zero,
  so the first metered request refreshed the lease from that one sample: with three regions the
  sender's region took 96% of every tenant's share and the other two 2% each until the next refresh.
  The longer the lease, the longer that split held: at equal demand the 2 s lease refused 0.5 to 0.6
  points more than a static split and the 0.5 s lease 0.2 to 0.3 more, and both now refuse within
  0.1 of it; only the 0.1 s lease's excess, 0.1 to 0.4, remains. The first refresh now comes a lease
  after the first request, a test fails without it, and §9.18 and P8's note are regenerated.
  Increment 4's finding that a lease is not free at equal demand was mostly this defect.
- **The KV grant was sampled over nodes that were not running.** `arrive` averaged the KV held over
  every slot that can decode, so with four nodes running on six slots a region the mean each cell's
  engine grant is sized from was a third too small. The emulation sampled the same way, which is why
  increment 3 reproduced §1.8 to the digit, and §1.8's budget figures carry it too. Sampled over
  running nodes, §9.13 moves by at most 0.4 points a seed, except the threshold's rows at 325 req/s,
  by up to 6.0; with only this fix reverted the build reproduces increment 3's budget section to the
  digit, so it is the whole of the change. §9.13 and P6's note are regenerated.

Moving no published number -- even, burst and day at both loads and on Azure's triangle, the table,
models, residency and shards reproduce to the digit with every fix, and the budgets with every fix
but the grant's:

- A gang retried at a later arrival was placed under the scope, client region and shard left by the
  request before it, and an engine-queue start charged its decode to the previous request's shard
  and client region.
- A request that needs no decode, from a region whose only node is drained, was refused rather than
  served elsewhere as a decode is.
- Residency was ignored under the global argmin.
- With no summary, a sender kept a record of every forward for the whole run; and `forwarded_delta`
  spread a sender's forwards over every slot of a region rather than its running nodes, which no
  published arm reaches, since none counts its own forwards under a budget.
- The gate's shards row ran one scheduler, which builds no view, so it checked nothing; it now runs
  two with exact reports.
- `durability`'s help had moved onto `regions`, and the tenants and shards sections printed the
  models section's number.

Left open:

- **A summary's quote reads a remote engine's live state.** When a remote node's summarised load is
  at or above the batch size, its quote includes the exact time until that engine's earliest decode
  ends, which no summary carries (rule 3). A variant whose summary quotes never read it reproduces
  even, burst and day at both loads and on Azure's triangle, the table and residency to the digit:
  no published number rests on it. It would matter where a region's nodes are summarised full, and
  what a summary should carry for a full node is a modelling decision.
- **A cancelled decode stays counted.** Its flight in a scheduler's view and its forward stay until
  their original end, and a resubmitted decode is counted twice in the demand the table and budgets
  read. No section cancels.
- **A request that opens is tallied without its serving region.** A request that waits at the
  router's or an engine's queue, or runs as a tracked flight, closes with no region and is never
  counted as served away. No section queues, cancels or arms a fault, so every request in them
  closes when it is submitted.
- **The table moves a new set of sessions every epoch.** Its hash includes the epoch, so at steady
  fractions a different set of sessions leaves home each epoch and rebuilds its KV; a key without
  the epoch would move only the change in fractions. The emulation did the same, and §1.7 and §9.12
  are measured with it.
- **The command's p99 is a different quantile** from the other commands' (`round((n - 1) q)` against
  `floor(n q)`), so its p99s are not comparable with theirs to the last digit.

---

## 10. Verification, as run

Increment 1.

- **Byte-identity with every new bit off:** `belief --sections gate`, `residency`, `flows`,
  `placement`, `volatility`, `ownership`, `price`, `influence --seeds 1`, `fleet --seeds 1`,
  `enforce --seeds 1`, `programs --sections gate,logged` and `durability --seeds 1 --sections
  gate,count`, all at `--ops 3000 --seed 2`, produce output identical to the build before this phase
  (`7f16fef`). `distributed --crossing native --engine-cache --decode-kv --admit perfect --repeat 1
  --distances rack` is identical bar the host-measured lines, which two runs of the pristine build
  also disagree on.
- **The gate,** `regions` section 1: one region with `Global` priced, `Global` unpriced and
  `Regional` against no regions -- service, p99, turn mean and stall identical to the bit.
- **Tests,** increment 1 added 10, 308 in all: a link costs the distance of the regions it joins and
  one region is the cluster; one region is the published trace, requests name their client's region
  by the shares, a follow-up belongs to its producer's region and a session stays in one; a regional
  scheduler serves every client-facing request in its region, the global argmin charges the round
  trip once per request served away, one region with every bit on changes no request, and a region
  with no node falls back to every node.
- **The census** is 13. `cargo fmt --check` is clean; `cargo clippy --all-targets` shows only the
  three `assert_is_empty` warnings.

Increment 2.

- **Byte-identity with every new bit off:** the same twelve commands identical to `7f16fef`, and
  `distributed` identical bar its host-measured lines.
- **The gate,** `regions` section 1, adds `Overflow::Node`, `RegionMean` and `Threshold` with a 1 s
  summary and own forwards, one region, against no regions: identical to the bit.
- **Tests,** 7 new, 315 in all: a region that cannot serve its demand forwards it and the whole run
  is faster, under the node price and the region mean; overflow forwards only what a client sent and
  a decode needs, never a `FaaS` call or a service request; a threshold forwards only above its
  utilisation; a sender counts what it forwarded since the summary it holds and a blind one counts
  nothing; a summary is a snapshot on its clock; a request for a model its region lacks goes to the
  nearest region with one, and every decode lands on a replica serving its model; and a request that
  waits at the router is served in its own region (mutation-checked: it fails with the fix removed).
- **The census** is 13. `cargo fmt --check` is clean; `cargo clippy --all-targets` shows only the
  three `assert_is_empty` warnings.

Increment 3.

- **Byte-identity with every new bit off:** the same twelve commands identical to `7f16fef`, and
  `distributed` identical bar its host-measured lines.
- **The gate,** `regions` section 1, adds a table over a region-mean overflow and the rent-or-buy
  budget with one region against no regions: identical to the bit.
- **Tests,** 9 new, 324 in all: a fan-out's agents are never counted as forwarded clients (fails on
  increment 2's build); nodes follow a region's demand and every region keeps one, and a cap binds
  and a short fleet gives each region what there is; routing demand keeps a balanced fleet home and
  moves a hot region's excess; a table moves almost nothing at equal demand and forwards a hot
  region's demand by session with rows that sum to one; a node that is not running takes no work; a
  planned move releases a node and the new one serves only after its load; and budgets that follow
  demand move nodes to the hot region and not when demand is even.
- **The census** is 13. `cargo fmt --check` is clean; `cargo clippy --all-targets` shows only the
  three `assert_is_empty` warnings.
- **Reproduction:** `polyphonic regions --sections table` and `models` reproduce the
  pre-measurements to the digit; `budget` reproduces the clairvoyant columns to the digit.

Increment 4.

- **Byte-identity with every new bit off:** the same twelve commands identical to `7f16fef`, and
  `distributed` identical bar its host-measured lines.
- **The gate,** `regions` section 1, adds one scheduler with reports five seconds old, every tenant
  restricted under the region-mean overflow, and leased tenant shares of a quota never reached, one
  region against no regions: identical to the bit.
- **Tests,** 8 new, 332 in all: schedulers that see each other's decodes exactly change no request;
  reports seconds old herd, four more than two, and 25 ms ones less; a lease follows the day and a
  static split refuses at the peaks; a lease moves a region's share toward its demand; full
  residency makes every overflow rule and the table regional; a share of tenants stays home and the
  rest may leave; a confined request whose model its region lacks goes unplaced and never leaves;
  and a restricted tenant's request is never served outside its client's region, per request
  (mutation-checked: it fails with the restriction removed).
- **The census** is 13. `cargo fmt --check` is clean; `cargo clippy --all-targets` shows only the
  three `assert_is_empty` warnings.
- **Reproduction:** `polyphonic regions --sections shards` and `arithmetic` reproduce §8's *shards*,
  *path*, *records* and *tenants* to the digit but one cell; `tenants` is new to the build (§9.17).

After the review.

- **Byte-identity with every new bit off:** the same twelve commands identical to `7f16fef`, and
  `distributed` identical bar its host-measured lines.
- **The gate,** `regions` section 1: its shards row now runs two schedulers with exact reports,
  which builds the view the one-scheduler row never did; every row identical to the bit.
- **Tests,** 1 new and 1 strengthened, 333 in all: a lease moves no share until it has seen a lease
  of demand, and a region whose only node is drained sends every request elsewhere, not only
  decodes; both fail with their fixes reverted.
- **The census** is 13. `cargo fmt --check` is clean; `cargo clippy --all-targets` shows only the
  three `assert_is_empty` warnings.
- **Reproduction:** every section rerun against the outputs §9 records. Even, burst and day at 250
  and 325 req/s and on Azure's triangle, the table at both loads, models, residency and shards are
  identical to the digit; the budgets and the tenants' lease rows are regenerated (§9.13, §9.18);
  and a variant whose summary quotes never read a remote engine is identical on every section that
  prices a summary (§9.23).
