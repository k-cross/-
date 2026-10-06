# Phase 11 -- Regions: a scheduler per region under global budgets

Implementation plan for Phase 11 of [`owned-and-observed.md`](owned-and-observed.md): §8's shape --
a routing tier in each region, admitting within budgets a global tier sets on the provisioning clock
-- against the single global argmin every region-distance result so far has run. §9 asks for the
price of the regional split: what the global argmin buys across regions that regional schedulers
under budgets give up, and how much rebalancing recovers. It predicts the price small, and names the
one place it could bind: an agent and its model host in different regions, where the origin round
trip is 61 ms and no placement moves it.

**Status: planned.** Nothing below is built. §2's predictions are stated before the run, per
`owned-and-observed.md` §7, and like Phases 4 to 7, 9 and 10 they lean on **pre-measurements**:
numbers taken on an instrumented copy of `7f16fef`, run outside the repository and not committed,
with every hook off reproducing `belief`, `residency`, `enforce`, `fleet` and `durability` byte for
byte on the sections §8 names. Nine emulate what this phase builds -- clients in regions with their
round trip in the score (*even*, *diag*), the published class phases against a flat mix (*phases*),
the regional knee (*knee*), spill (*spill*), a global routing table (*table*), node budgets that
follow demand (*budget*), models placed by region (*models*) and the active-active schedulers Phase
10 deferred (*shards*) -- and three are arithmetic, one on the generator's own demand (*tenants*)
and two on published figures (*path*, *records*). Each is labelled with its grade
(`owned-and-observed.md`, *On the numbers*), and §8 says how each was taken. They are reasons to
predict, not results: §4.13 rebuilds each instrument in the repository, and a pre-measurement the
build does not reproduce is reconciled before a prediction resting on it is graded.

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

**P10 -- A scheduler on the request path cannot be global, and a WAN partition is an outage for one
that is.**

*Arithmetic:* a global scheduler adds a round trip to two-thirds of requests -- a mean of 40 ms at
30 ms one way and 82-132 ms on Azure's triangle as it sits in East US, West Europe or Japan East,
500-3,000 times the sidecar tax §2.3 removed; a region cut off for 60 s at 250 req/s loses 450,000
request-seconds to it (`λD²/2`), against what it could not forward for a regional one.

- *If right:* §2.2's rule extends to the WAN: per-request decisions are regional, and §8's shape is
  forced, not chosen.
- *If wrong:* not applicable; this is arithmetic, and is checked only against the built round trip.

**P11 -- The global record writes under once a second at 10,000 nodes.**

*Arithmetic:* the table's six fractions an epoch, budget moves of 0.1-0.2 a second, quota changes on
the provisioning clock: under 1 write a second globally, three orders below the regions' records,
whose liveness writes 1,000 a second.

- *If right:* FoundationDB's two-region mode, with its WAN round trip on every commit from the
  second region, carries the global record with room; the regions' records carry the rate.
- *If wrong* (above 10 a second): something on the request clock -- a tenant's lease refreshed
  through the record -- has been written durably, and belongs in the soft tier.

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
