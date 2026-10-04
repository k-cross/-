# Phase 9 -- Enforcement: a queue at the router, cancellation on the path, and two-tier admission

Implementation plan for Phase 9 of [`owned-and-observed.md`](owned-and-observed.md): give the
router the one channel into the engine that is authoritative rather than advisory -- **cancel the
request on the path it arrived on** (§2.3) -- and a queue to hold what it cancels or cannot yet
admit, then use both on §1's test case, two-tier admission by declared `slo`. The deliverable §9
asks for is where an overcommit's tail loss lands once the router can choose the victim; Phase 3
found it class-blind, spread by arrival, because the engine picks victims on its own order.

**Status: built and measured.** Outcomes by request and the engine that waits (§4.1, §4.2) are
behind `--engine-wait` and `--probe-engine`; the router queue, the per-class claims and the program
id (§4.3 to §4.5) behind `--queue` and `--admit quantile`, `tiered` or `gate`; the abort and the
cancel (§4.6, §4.7) behind `--cancel`, `--victim` and `--cancel-at`; departures, the batch class and
the buffer (§4.8 to §4.10) behind `--disconnect`, `--leak`, `--batch` and `--stream-buffer`. Every
bit is off by default and every result behind one is an A/B against the run without it, so the
published numbers stay the ledger's. `polyphonic enforce` runs §4.12's sweeps. §9 records what the
build found and §10 how it was verified. §2's predictions are stated before the run, per
`owned-and-observed.md` §7, and like Phases 4 to 6 they lean on **pre-measurements**: numbers
taken on an instrumented copy of `01b2c1e`, run outside the repository and not committed, with
every hook off reproducing the published run to four decimals. They are labelled wherever quoted
and collected with their configurations in §8. They are reasons to predict, not results: §4.11
rebuilds each instrument in the repository, and a pre-measurement the built instrument does not
reproduce is reconciled before any prediction resting on it is graded. Most of them are emulations
of a mechanism this phase builds -- a queue polled at each arrival, a cancel that takes effect in
one step, an engine that waits -- and §8 says where each is cruder than the build.

Four things make this phase unlike Phase 6.

- **Its subject is not in the engine model, and the plan starts there.** An engine that cannot
  place a sequence makes it wait or preempts a running one. The model does neither: the sequence
  runs anyway, pays a rebuild, and holds no memory while it decodes. At the half partition Phases
  3 to 5 used as their tight regime that is 18-20% of decodes, and once they have to wait for
  their blocks the half partition is past saturation (§1.1). The loss §1 says lands class-blind
  was measured on an engine giving memory away.
- **Its mechanism has shipped, in the other shape.** llm-d's flow control queues requests at the
  gateway in priority bands, holds lower bands back, and evicts dispatched negative-priority work
  for blocked higher-priority demand; vLLM schedules by a per-request priority (§1.2, §1.3). The
  phase measures one shape against another, not a mechanism against its absence, and the
  difference turns out to land on the class that is cancelled, not the one that is protected.
- **Its outcomes are deferred.** A request's latency is known when it finishes, and a cancel
  changes a request already dispatched. Every phase so far has resolved a request completely at
  its arrival; this one keeps an outcome per request and closes it at completion (§4.1).
- **Its axis is the first token, not service.** Decode sets every class's service p99 and no arm
  here moves the interactive class's by more than a few percent once the engine is honest -- the
  reason Phase 4 found the declared SLO buys nothing a user would see. What moves is the time
  before the first token, the ledger's *stall*, and the throughput class's completion (§1.15).

§1 settles fifteen decisions. Seven are findings about the existing model and documents rather
than about the work ahead: §1.1 (the engine runs what it cannot place, and that hid an overload),
§1.2 (vLLM's priority reaches the choice of victim §1 says the orchestrator ceded, for growth
though not for admission), §1.3 (llm-d already queues, holds back and evicts), §1.4 (Phase 3's
bracket compared different sets of served requests), §1.8 (the two-tier mix is a scalar wherever
the classes are drawn alike), §1.12 (PLAS orders by a property this workload does not separate
classes by) and §1.14 (the stalled-stream buffer §2.6 calls a memory-arbitration event is three to
four orders of magnitude below the pool it would occupy).

---

## 1. What has to be settled before the router can enforce anything

### 1.1 The engine runs a sequence it cannot place, holding nothing, and that hid an overload

`Hierarchy::access_kv` places a dispatch's chain in the engine's partition; when every block it
could evict is pinned by a sequence in flight, it calls `preempt`, which charges the whole chain's
rebuild and unpins what it staged, and `run_paired` then decodes the request anyway.
`decode_output` does the same when the output does not fit. `phase-3.md` §8.8 records the choice
and its consequence for P4: the arrival pays, so the loss spreads by arrival. What it does not
record is that the arrival pays a rebuild and no wait, and decodes without a block to its name.

The pre-measurement probed it read-only: at each such preemption, the time until enough pins
release, from the engine cache's own schedule of in-flight ends (§8, *probe*; the `belief`
cluster with the engine allocating and decode output held, `none` at the router; seeds 1 / 2 / 3):

| partition | single-request decodes run with no memory | wait they would have had: mean | p99 |
|---|---|---|---|
| 0.5x, the half partition | 17.7 / 20.4 / 18.9% | 55.5 / 63.4 / 60.2 ms | 244 / 265 / 266 ms |
| 0.6x | 8.8 / 10.6 / 9.6% | 47.7 / 55.5 / 55.2 ms | 173 / 228 / 235 ms |
| 0.75x | 2.1 / 3.2 / 2.8% | 39.7 / 41.7 / 37.0 ms | 169 / 183 / 167 ms |
| 0.6x, prefill taking engine time | 24.0 / 30.3 / 26.4% | 88.7 / 108.1 / 94.2 ms | 482 / 533 / 518 ms |

Each is a wait the run did not charge and a sequence the partition did not hold. Charged -- a
sequence the engine cannot place waits in a per-node queue, first come first served with
head-of-line blocking, as vLLM's waiting loop does (§1.2), and holds its blocks once it starts --
the interactive class's tails become (§8, *wait*):

| partition | service p99, runs anyway -> waits | stall p99, runs anyway -> waits |
|---|---|---|
| 0.5x | 1912 / 1927 / 1924 -> 9781 / 16635 / 11211 ms | 56 / 64 / 57 -> 8803 / 15919 / 10067 ms |
| 0.6x | 1919 / 1913 / 1928 -> 4364 / 5435 / 5362 ms | 53 / 53 / 52 -> 3247 / 4560 / 4545 ms |
| 0.75x | 1910 / 1911 / 1928 -> 1975 / 2299 / 2110 ms | 49 / 49 / 49 -> 497 / 907 / 698 ms |
| 1.0x, the published partition | unchanged; 0.0-0.15% of decodes wait | unchanged |
| 0.75x, prefill taking engine time | 3582 / 3768 / 3654 -> 5319 / 5557 / 5110 ms | 548 / 526 / 492 -> 3634 / 3995 / 3716 ms |
| 0.6x, prefill taking engine time | 3812 / 4224 / 3918 -> 12166 / 16201 / 14882 ms | 677 / 893 / 864 -> 10885 / 14836 / 13860 ms |

At the half partition 80-83% of decodes wait and the engine's queue reaches 200-311 a node: the
partition is past saturation for an engine that has to hold what it runs. First fit in place of
head-of-line blocking halves the mean and leaves the p99 at 4.6-5.5 s, so it is capacity and not
the queue's order. At 0.75x the system is stable and memory binds: 9-17% of decodes wait, and the
first-token tail is 10-19x today's.

So the half partition is the overload regime, and Phase 3's admission bracket at 0.5x -- `none`
preempting 10.5% and costing chat turns +4.9% at p99 -- was measured on an engine that gave the
overcommit away. The class-blind reading of P4 holds; its size does not.

**Chosen: `--engine-wait`, off by default.** A sequence the engine cannot place, prompt and
output together, waits in its node's queue until its blocks fit, `fifo` with head-of-line
blocking or `priority` by a class the dispatch carries (§1.2). The fluid engine allocates a decode's
output at admission, so waiting for the whole sequence is the consistent correction; vLLM admits on
the prompt alone and preempts when a running sequence cannot grow, which needs a per-step
allocation the fluid engine does not have (§7). Every Phase 9 arm is graded on the corrected
engine; the published numbers stay the old engine's.

### 1.2 What vLLM does instead, and what its priority can and cannot do

`owned-and-observed.md` §1: "Ceding eviction ceded the **choice of victim** ... nothing the
orchestrator can say makes it drop a draft to spare a chat turn." vLLM's V1 scheduler
([`scheduler.py`](https://github.com/vllm-project/vllm/blob/main/vllm/v1/core/sched/scheduler.py),
main, read 2026-09-30) says otherwise in one of its two places and agrees in the other.

- **A running request that cannot grow preempts one.** Under `--scheduling-policy priority` the
  victim is `max(self.running, key=lambda r: (r.priority, r.arrival_time))` -- the lowest priority,
  latest arrival first; under FCFS it is `self.running[-1]`, the most recently scheduled. The
  preempted request frees its blocks, restarts from zero computed tokens, and goes back to the
  waiting queue.
- **A waiting request never preempts.** When `allocate_slots` fails for the head of the waiting
  queue the loop breaks, and no waiting request is scheduled in a step that preempted. An arrival
  waits; it does not displace.
- **The priority is a request field**, `priority` on the OpenAI-compatible request, "lower means
  earlier handling", and a non-zero value is an error unless the engine runs the priority policy.
  It is a deployment setting and a field -- promotion tier 0.

So the orchestrator can say which running sequence the engine gives up when one of them grows, and
cannot make the engine admit a chat turn by evicting a draft. In the model a decode's output is
allocated at admission, so there is no growth-time preemption to direct; the priority's one
modelled effect is the order of the engine's waiting queue, `--engine-wait priority`. The cancel
is then the arm for what the priority cannot reach: admission-time displacement, and anything that
crosses nodes.

**Chosen: as above.** `owned-and-observed.md` §1 is corrected at publication (§4.13).

### 1.3 llm-d already queues, holds back and evicts

llm-d's EPP flow control ([dev docs](https://llm-d.ai/docs/dev/architecture/core/router/epp/flow-control),
read 2026-09-30, after the 0.9 release) holds requests at the gateway "in centralized,
policy-aware queues" rather than on the model servers. Requests carry a priority band from their
`InferenceObjective` and a fairness id; the highest band is served first, a fairness policy picks
the flow within it, and an ordering policy -- FCFS, EDF, or a deadline from `x-llm-d-slo-ttft-ms`
-- picks the request. Dispatch is gated by a saturation detector, a utilisation threshold on queue
depth and KV cache or a concurrency cap, with head-of-line blocking; requests expire past a TTL.
Under `priority-holdback-policy` each band "holds back at a lower ceiling" -- two-tier admission
by threshold. And:

> In-flight eviction terminates an already-dispatched, negative-priority (`priority < 0`) request
> to reclaim capacity for a higher-priority request blocked at its dispatch ceiling ... The default
> victim policy selects the lowest priority first, then the most recently dispatched ... the
> capacity returns only once the model server aborts generation on the upstream reset and frees
> its KV blocks. Flow Control assumes a bounded engine reclaim time and does not observe it
> directly ... The evicted caller ... owns retry.

So every mechanism §9 assigns this phase -- a queue, a priority order, a designated victim class,
a cancel on the path -- has shipped, in the shape §2.3's fourth reason describes: a sidecar
carrying a cancel. The document does not describe passing the band down to the engine.

What the integrated path has that this does not is narrower, and the phase should measure that
and only that:

- **Continuation rather than retry.** The router relayed every token, so it can resubmit the
  prompt and the tokens already generated instead of dropping the request on its caller (§1.10).
  vLLM's API carries it at promotion tier 0: `return_token_ids` returns the ids on the stream,
  token-id prompts resubmit them, and `continue_final_message` is the chat-level form.
- **A claim in bytes at a declared quantile** rather than a utilisation threshold (§1.11).
- **Reclaim observed rather than assumed.** The step-aligned header carries free and total blocks
  (§1, *Step-aligned ingestion*), so the router sees the abort land. Recorded, not modelled: the
  simulator's engine frees at the next step by construction.

**Chosen: llm-d's shape is an arm** -- a utilisation gate, the queue in class order, eviction of
the most recently dispatched lower-class sequence, the request dropped and re-sent at once --
beside the integrated one, with its threshold swept, as Phase 8 built `Sidecar` beside
`Integrated` (§8, *gate*).

### 1.4 A refusal removes the tail it is measured on; a queue keeps it

Phase 3 compared `bound`, `perfect` and `none` by p99 over the requests each served, and said so:
"each arm serves a different set of requests: `bound`'s lower p99 is the tail it refused." With
the engine waiting, the difference is the whole result (§8, *queue*; `perfect` reservations):

| partition | `perfect`, refusing: refused, interactive stall p99 | `perfect`, a FIFO queue at the router: interactive stall p99 |
|---|---|---|
| 0.75x | 1.2 / 2.0 / 1.7%, 45 / 22 / 22 ms | 56 / 82 / 64 ms |
| 0.75x, prefill taking engine time | 2.4 / 3.4 / 2.5%, 56 / 55 / 53 ms | 161 / 106 / 129 ms |
| 0.6x | 4.6 / 6.0 / 5.1%, 23 / 33 / 22 ms | 294 / 221 / 252 ms |
| 0.6x, prefill taking engine time | 6.3 / 7.6 / 6.8%, 54 / 56 / 54 ms | 1311 / 1268 / 1360 ms |
| 0.5x | 8.9 / 9.8 / 9.4%, 50 / 53 / 51 ms | 2138 / 2360 / 2305 ms |

The queue serves everything but the fan-outs it cannot stage whole, with their continuations:
0.2-0.4% of requests at 0.75x, 1.4-1.9% at 0.6x, 3.0-3.4% at the half partition. The refusing
arm's tail is the tail of what it chose to serve, and no queueing arm can be compared with it
without the refused share beside it.

Two placements of the check follow. Phase 3 kept the router's check "at the node the score chose,
after placement", because the ledger refused there too, and §8.11 there named the alternative --
filtering candidates by headroom -- as a question for whenever the check belonged in the argmin. A
queued request has to go somewhere with room, so under a queue the check filters the candidates
and the request is placed when it leaves, among the nodes that admit it.

**Chosen: `--queue`, off by default; with it, refusal happens only to a fan-out that cannot be
staged whole.** Late binding under the queue, the old check without it. A queued request is owned
soft state, fate-shared with the connection it holds, which is §1's durability argument applied
to the queue; Phase 10 counts it. No TTL: every request is eventually served, and the tail is the
measurement. `Deadline(t)` is what a TTL would consume, and it stays unbuilt (§7). Placing at
dispatch is llm-d's "Dynamic Late Binding".

### 1.5 The order does more than the claim, and it has to be the router's

The queue's order and the claim it admits against are two choices. Pre-measured on the published
workload (§8, *order*; interactive stall p99, prefill taking engine time at 0.6x):

| claim | the router's queue, FIFO | the router's queue, by declared class |
|---|---|---|
| `perfect` | 1311 / 1268 / 1360 ms | 173 / 188 / 245 ms |
| a p90 of the observed output length | 1867 / 1495 / 1960 ms | 167 / 230 / 183 ms |
| tiered: interactive at p90, throughput at the mean | -- | 176 / 196 / 198 ms |
| `none`, the engine's own queue | 10885 / 14836 / 13860 ms, FIFO | 4820 / 8265 / 4491 ms, by class |

With prefill free at 0.6x the order buys 1.6-3.3x and the claims sit within 20% of each other
once it is by class. The engine's own queue ordered by class -- vLLM's priority policy as the model
can express it -- does 1.5-4.5x better than its FIFO and stays 5-36x above the router's order in
every regime: the engine admits optimistically and then blocks on the head of its queue, and an
order applied after admission cannot undo an admission.

**Chosen: `--queue fifo | slo | plas`**, `slo` serving the interactive class first with arrival
order inside a class.

### 1.6 A cancel is three releases and a continuation

A sequence in flight occupies three ledgers, and the model keeps none of them by identity:

| ledger | holds | today |
|---|---|---|
| the engine's batch | a slot until the decode's end | `Engine::inflight`, a heap of ends with no id |
| the engine's partition | the pins on its chain and output | `EngineCache::seal` files them under a sequence number it never returns |
| the router's reservations | its claim | `Reservations::commit` files it under a sequence number it never returns |

An abort releases all three at the engine's next step boundary. vLLM's `finish_requests` -- the
path a client disconnect takes -- removes the request from the running set and frees its blocks
at once unless an in-flight step or a connector defers it. Freed blocks stay in the prefix cache,
evictable, so whatever of the victim's prompt and decoded output survives until it returns is a
hit. Blocks the model placed for output not yet decoded were never written, and are removed.

The victim then needs somewhere to go. Requeued as a **continuation** it is its prompt plus the
whole blocks it decoded, with the tokens it has left, its original arrival and its class's place
in the queue; it loses the partial block and whatever the prefix cache evicts before it returns.
Requeued as a **restart** it is the original request, which is what a caller retrying llm-d's 429
sends. The router keeps the token ids it relayed for every sequence it might cancel: four bytes a
token.

Which sequence: llm-d's default, the most recently dispatched, loses the least work under a
restart; under a continuation little is lost either way. The pre-measurement found neither it nor
the sequence with the most remaining work better across cells: they differ by seed, in both
directions, by up to 1.8x on the throughput class's completion p99 (§8, *cancel*).

**Chosen: `--cancel continue | drop`, `--victim recent | remaining`,** the request admitted in a
victim's place charged one step for the abort to land, victims only among sequences of a lower
declared class, never a fan-out agent (§7).

### 1.7 The cancel reaches the rebuild floor, and the loss moves to the throughput class's first token

On the published workload with 30% of sessions declaring a throughput objective -- classes drawn
alike -- the queue in class order with and without a cancel (§8, *cancel*; tiered claim;
interactive stall p99, then the throughput class's completion p99):

| partition | no cancel | cancel, continuation | `perfect`, refusing |
|---|---|---|---|
| 0.75x | 56 / 73 / 64 ms; 1936 / 1896 / 1946 ms | 56 / 56 / 56 ms; 1922 / 1893 / 1945 ms | 45 / 22 / 22 ms |
| 0.75x, prefill taking engine time | 111 / 98 / 82 ms; 2421 / 2233 / 2183 ms | 63 / 61 / 61 ms; 2410 / 2163 / 2263 ms | 56 / 55 / 53 ms |
| 0.6x | 126 / 150 / 116 ms; 2319 / 2252 / 2483 ms | 63 / 63 / 63 ms; 2553 / 2629 / 4688 ms | 23 / 33 / 22 ms |
| 0.6x, prefill taking engine time | 176 / 196 / 198 ms; 7275 / 6817 / 6579 ms | 63 / 65 / 65 ms; 8712 / 7191 / 7539 ms | 54 / 56 / 54 ms |
| 0.5x | 182 / 306 / 284 ms; 9237 / 10339 / 10061 ms | 82 / 140 / 144 ms; 13707 / 12304 / 11456 ms | 50 / 53 / 51 ms |

Everything from 0.6x up lands at 55-66 ms, which is the prefill of the longest prompts in the
trace plus the one step a cancel waits for. That floor is a property of the rebuild constant, not
of any policy, and refusing arms reach it by refusing the requests that would sit above it. The
throughput class pays in its own first token -- its stall p99 rises 1.03-6x, the large factors on
tails that were small -- and in its completion p99 by under 20% in all but one cell from 0.6x up
(+89% on one seed at 0.6x) and by 3-48% at the half partition. Between 1% and 7% of decodes are
cancelled at least once from 0.75x down to 0.6x, 7.5-9% at the half partition. The interactive
class's service p99 moves by under 3%.

That is §9's prediction, on the axis where the loss lives: the tail does move onto the throughput
class, and it moves in time-to-first-token, never in service, because service was never where it
was.

### 1.8 Two quantiles are a scalar where the classes are drawn alike, and per-class distributions matter where they are not

§1's two-tier admission reserves latency-bearing work at a high quantile of output length and lets
throughput work borrow against the mean. On the published workload one p90 claim for both classes
lands where the tiered claim does, with the cancel and without it (§8, *tiers*; the queue in class
order with a cancel, interactive stall p99 and throughput completion p99):

| | one p90 claim | tiered |
|---|---|---|
| 0.75x, prefill taking engine time | 63 / 62 / 60 ms; 2400 / 2199 / 2308 ms | 63 / 61 / 61 ms; 2410 / 2163 / 2263 ms |
| 0.6x | 63 / 63 / 63 ms; 2711 / 2309 / 2860 ms | 63 / 63 / 63 ms; 2553 / 2629 / 4688 ms |
| 0.6x, prefill taking engine time | 63 / 66 / 63 ms; 8625 / 7383 / 8246 ms | 63 / 65 / 65 ms; 8712 / 7191 / 7539 ms |
| 0.5x | 73 / 84 / 94 ms; 10339 / 9091 / 10919 ms | 82 / 140 / 144 ms; 13707 / 12304 / 11456 ms |

From 0.6x up the two are within 10% on the interactive tail in every cell and on the throughput
class's completion in all but one, a seed on which the tiered claim runs 64% longer under the
most-recent victim and not under the other (§1.6); at the half partition the tiered claim's tails
are up to 1.7x and 1.35x longer.

`Workload::tokens` draws every output from `[24, 224)` whatever the class, so a p90 and a mean of
one distribution are a scalar and its offset. A throughput class with its own shape separates them:
`--batch 0.05` adds requests with an 8-block prompt nothing shares, 200-600 output tokens and a
throughput declaration, and moves the throughput class's own distribution away from the
interactive one (§8, *batch*; 0.75x, every session interactive):

| claim, queue in class order | no cancel: interactive stall p99 | with a cancel: interactive stall p99; throughput completion p99 |
|---|---|---|
| one p90 of the pooled distribution | 286 / 501 / 1878 ms; 1686 / 3620 / 4584 ms with prefill time | -- |
| a p90 of each class's own distribution | 178 / 159 / 428 ms; 408 / 317 / 439 ms | 63 / 63 / 63 ms, 5496 / 5674 / 6357 ms; 71 / 67 / 64 ms, 9085 / 7756 / 12352 ms |
| tiered: interactive p90, throughput its own mean | 168 / 173 / 249 ms; 444 / 312 / 608 ms | 64 / 63 / 63 ms, 6561 / 5742 / 8183 ms; 166 / 75 / 182 ms, 13092 / 14820 / 17345 ms |

The pooled quantile under-reserves the long class and the engine overflows behind it. What
separates the arms is that each class is claimed against its own distribution, not that the two
classes are claimed at different quantiles; borrowing against the mean lets more throughput work
in, which the cancel then evicts, and where prefill takes engine time every eviction's re-prefill
is someone's step.

**Chosen: `--admit quantile` claims each request at `--claim-quantile` of its own class's observed
distribution, `--pooled` for the pooled one, and `--admit tiered` keeps §1's form.** The classes'
distributions are observed per declared class on the path, Phase 4's per-class running mean
widened to a histogram.

### 1.9 A quantile claim under-reserves by construction, so a cancel needs a second trigger

A request claimed at its class's p90 overruns its claim one time in ten, and a request claimed at
its class's mean half the time. What overruns lands inside the engine, as a sequence that cannot be
placed behind ones that grew, and a cancel the router issues only when its own check fails never
sees it. With the batch class and the tiered claim (§8, *trigger*; 0.75x, interactive stall p99):

| cancel triggered by | prefill free | prefill taking engine time |
|---|---|---|
| the router's check only | 66 / 165 / 1316 ms | 218 / 3049 / 2985 ms |
| the router's check, or an interactive request waiting at an engine | 64 / 63 / 63 ms | 166 / 75 / 182 ms |

The second trigger fired 75-334 times a run under the tiered claim and never under each class's
own p90, which kept the engine from overflowing at all.

**Chosen: two triggers.** The router cancels when the head of its queue fits no node and a lower
class holds enough at one, and when a dispatched request of a higher class waits at an engine.
The second is the router acting on its own dispatch log -- it knows what it sent and that no first
token has come back -- and needs no new telemetry.

### 1.10 Continuation, not restart

The same queue and cancel, dropping the victim instead (§8, *drop*; throughput completion p99, and
the decode the restarts threw away):

| | continuation | restart | decode thrown away |
|---|---|---|---|
| 0.75x | 1922 / 1893 / 1945 ms | 1963 / 2239 / 2005 ms | 16 / 40 / 17 s |
| 0.75x, prefill taking engine time | 2410 / 2163 / 2263 ms | 3147 / 3145 / 3146 ms | 65 / 89 / 67 s |
| 0.6x | 2553 / 2629 / 4688 ms | 6099 / 4081 / 4876 ms | 116 / 159 / 127 s |
| 0.6x, prefill taking engine time | 8712 / 7191 / 7539 ms | 12661 / 9574 / 12738 ms | 165 / 233 / 202 s |
| batch, 0.75x | 6561 / 5742 / 8183 ms | 11139 / 15857 / 15981 ms | 418 / 641 / 534 s |

A run is about 60 s on four nodes with some forty sequences in flight on each, so 200 s of
sequence time is about 2% of the decode the fleet did, and the batch class's long outputs make it
4-9%. Across the nine seeds at 0.6x, prefill free or taking engine time, and at 0.75x with prefill
time, a restart finishes the throughput class 31-139% later on eight and 4% later on the ninth; at
0.75x with prefill free, 2-18%. The interactive class's stall p99 is within 1.4x either way from
0.6x up. So the difference between llm-d's eviction and the integrated path's is the victim's
completion, and it is the size of the work a restart repeats.

**Chosen: continuation is the integrated arm and restart the comparison**, the restart re-sent at
once, which is the most favourable retry a caller can make.

### 1.11 A threshold gate is a tuned constant, and its cost lands on the evicted class

The llm-d-shaped arm admits a request at a node while the node's pinned KV is under a fraction θ
of its partition -- the utilisation detector -- and otherwise evicts the most recently dispatched
throughput sequence there and drops it (§8, *gate*):

| | interactive stall p99 | throughput completion p99 | cancels |
|---|---|---|---|
| 0.6x, prefill taking engine time: claim and continuation | 63 / 66 / 63 ms | 8625 / 7383 / 8246 ms | 1050 / 1329 / 1137 |
| same, gate at 0.9 and restart | 67 / 137 / 67 ms | 15453 / 16402 / 13655 ms | 2967 / 4964 / 4555 |
| same, gate at 0.8 and restart | 201 / 440 / 230 ms | 25842 / 44740 / 36519 ms | 4618 / 3809 / 5209 |
| 0.6x, prefill free: claim and continuation | 63 / 63 / 63 ms | 2711 / 2309 / 2860 ms | 785 / 862 / 747 |
| same, gate at 0.9 and restart | 65 / 68 / 66 ms | 7613 / 10812 / 12992 ms | 2027 / 3509 / 2961 |
| batch, 0.75x: claim and continuation | 63 / 63 / 63 ms | 5496 / 5674 / 6357 ms | 1465 / 1606 / 1882 |
| same, gate at 0.9 and restart | 65 / 69 / 68 ms | 14251 / 21089 / 20588 ms | 3296 / 5464 / 4851 |

At θ = 0.9 the gate protects the interactive tail about as well, within 2.1x, and costs the
throughput class 1.1-4.7x in completion, the factor growing as memory binds harder; at θ = 0.8 the
interactive tail is up to 6.7x the claim arm's and the throughput class's completion 1.3-9x. A
retried request is the most recently dispatched one, so it is the next victim, and a gate that does
not know a request's size admits the ones that overflow. θ has no unit and no right value; the
claim's quantile is declared by the class it protects. That is §3.9's objection to a dimensionless
constant, with the class that pays for it named.

**Chosen: `--admit gate` with `--gate θ`, swept at 0.8, 0.9 and 0.95, a condition of the llm-d arm
and never a parameter of the integrated one.**

### 1.12 PLAS needs a class it can see

Autellix ([arXiv 2502.13965](https://arxiv.org/abs/2502.13965), NSDI '26 as Agentix) schedules an
agent's LLM calls by the service its program has already attained -- the execution time of its
completed calls -- "non-clairvoyant", needing no output length; it discretises the priorities into
queues, preempts by swapping KV to host memory when a call exhausts its queue's quantum, and
promotes a program whose wait-to-service ratio passes a threshold. §9 names it as a comparison arm
needing no prediction at all. Emulated as the router queue's order -- a program being a session, a
flow task or a fan-out, its attained service the decode time of its completed calls (§8, *plas*;
a p90 claim, no cancel):

| | interactive stall p99 | throughput stall p99 |
|---|---|---|
| 0.6x: FIFO / PLAS / by declared class | 362 / 289 / 411; 307 / 376 / 265; 111 / 148 / 128 ms | 385 / 302 / 425; 515 / 351 / 668; 1131 / 906 / 945 ms |
| 0.6x, prefill taking engine time: same | 1867 / 1495 / 1960; 2413 / 3196 / 1144; 167 / 230 / 183 ms | 1871 / 1497 / 1990; 3100 / 3080 / 8010; 6326 / 5406 / 5258 ms |
| batch, 0.75x: PLAS | 215 / 360 / 653 ms; 3680 / 3584 / 8013 ms with prefill time | 82 / 74 / 94 ms; 98 / 106 / 90 ms |

PLAS treats both classes alike, which on the published workload puts the interactive tail near
FIFO's where memory binds hard and spares the throughput class most of what class order costs it.
With the batch class it serves the batch class first, because each batch request is a program one
call long and most chat turns belong to sessions that have already been served. Ordered by
attained service, agent sessions go last. Cancelling the program with the most attained service
thrashed in every
regime -- 1,300-2,500 cancels and the interactive tail at 2-9 s at 0.6x -- because the victim's
continuation keeps its rank; Autellix demotes by quantum and swaps rather than aborts, and its
anti-starvation promotion is what this emulation lacks.

**Chosen: `--queue plas` is an order with no cancel, printed beside the class order on both
workloads.** It needs `Request::program`.

### 1.13 A client that leaves is the cancel's first job

§2.6 lists cancellation first among the stream semantics no library decides: "a client that
disconnects mid-generation has to become an engine abort, or the request decodes into a socket
nobody is reading and holds its KV blocks". It happens in practice and mostly at a proxy: vLLM's
server misses a disconnect behind a Starlette middleware
([#10087](https://github.com/vllm-project/vllm/issues/10087)), and two proxies keep generating
after the client has gone -- LiteLLM on streams
([#30244](https://github.com/BerriAI/litellm/issues/30244)) and GPUStack on non-streaming requests
([#6286](https://github.com/gpustack/gpustack/issues/6286)).

Emulated as a deterministic share of decodes whose client leaves at a uniform point of the decode,
either propagated as an abort or leaked to the decode's natural end (§8, *disconnect*; interactive
stall p99, the engine's own wait and `none`):

| | nobody leaves | 5%: leaked -> aborted | 20%: leaked -> aborted |
|---|---|---|---|
| 0.75x | 497 / 907 / 698 ms | 501 / 907 / 743 -> 351 / 597 / 627 ms | 490 / 901 / 676 -> 259 / 266 / 293 ms |
| 0.6x | 3247 / 4560 / 4545 ms | 3249 / 4560 / 4492 -> 2063 / 3825 / 2848 ms | 3265 / 4546 / 4516 -> 1665 / 1895 / 1568 ms |

Under the router queue in class order at 0.6x with prefill time the leak lands on the class that
waits: the throughput class's stall p99 is 6333 / 5392 / 5264 ms leaked and 2996 / 1272 / 1788 ms
aborted at 20%, and within 13% either way at 5%. A leaked decode is a reservation and a set of
pins the router keeps paying for.

**Chosen: `--disconnect p` and `--leak`, a stream of its own,** the share swept from 0 to 20%. It
has no provenance -- people press stop, tabs close, agent frameworks abandon a branch -- and is
published as a range.

### 1.14 The stalled-stream buffer is text, and text is small

§2.6 and §8 of the design say a slow client's tokens wait in host DDR, "an occupant of the pool §5
prices -- a few hundred stalled streams are a memory-arbitration event". The pre-measurement
counted the tokens every decode in flight had emitted, per node, at each arrival: if every stream
on a node stalled for the whole of its decode, the buffer would hold 3,200-4,700 tokens at most on
the published workload in every regime, and 8,200 with the batch class. A streamed token is about
200 bytes of server-sent event -- the chunk's id, model and delta. That is 0.6-1.7 MB, 0.01-0.02%
of the node's 8 GiB of DDR, three to four orders of magnitude below the pool the shadow price
arbitrates.

The choice §2.6 points at is where a stall lands, and it is lopsided. Buffering costs those
bytes. Pausing the decode instead holds the sequence's KV and its batch slot: 15 KiB a token in
this model (a 512 KiB block per 35 tokens) and 320 KiB for a 70B model at fp16 (80 layers, 8 KV
heads of 128, keys and values at two bytes), so 75x to 1,600x the buffer, in HBM.

**Chosen: a counter, not a ledger class.** Buffered bytes per node at a declared 200 bytes a token,
printed; the rule it supports is to buffer and never pause the decode. The design's sentence is
retracted for text at publication and kept for audio and image output, where a stalled stream
carries tens of kilobytes a second; nothing in this workload does.

### 1.15 Regimes, workloads, and how each effect is graded

- **The cluster** is the `belief` cluster: 4 nodes, 16 GiB HBM, 32 GiB DDR and 64 GiB `NVMe` in
  total, 250 req/s, 10% fan-out, 15,000 requests, rack, no control crossing, `scored + fetch`, the
  exact view, the engine's grants sized from the ledger's run, decode output held at 35 tokens a
  block.
- **The partition** at 1.0, 0.75, 0.6 and 0.5 of the published grant: nothing binds, memory binds
  and every honest arm is stable, memory binds hard, and the overload regime (§1.1).
- **Prefill free and prefill taking engine time** (`--prefill-time`, a 1 s window, nothing free).
  The second is where a cancel's re-prefill and a restart's repeated decode are paid by everyone
  on the node.
- **Two workloads.** The published mix with 30% of sessions declaring throughput, where the classes
  are drawn alike; and every session interactive with `--batch 0.05`, where the throughput class
  has a shape of its own (§1.8).
- **Grading.** Per declared class: the stall p99 -- the time before the first token, service less
  decode -- the service p99 and mean, and for the throughput class its completion p99. Waits by
  where they happened, router or engine; cancels per decode, by trigger; re-prefill work; decode
  thrown away by restarts; queue depths; buffered bytes. Queue arms serve the same requests;
  refusing arms print their refused share beside every tail. Every cell is three seeds and carries
  its partition, its prefill setting and its workload.

---

## 2. Predictions, stated first

`owned-and-observed.md` §7's rule. Eleven predictions, each attached to a claim it would rewrite.
Where a pre-measurement stands behind one, §8 says how it was taken.

**P1 -- The engine gives memory away, and the half partition is an overload.**

Under today's engine at the half partition with decode output held, 17-21% of single-request
decodes run with no memory, and the wait they would have had is 55-65 ms at the mean and 240-270 ms
at p99. With `--engine-wait fifo` and `none` at the router: the interactive class's service p99
rises by more than 4x at the half partition, 2-3x at 0.6x, 3-25% at 0.75x and under 0.2% at the
published partition; its stall p99 to 3-5 s at 0.6x and 10-20x today's at 0.75x. First fit in
place of FIFO leaves the half partition above 4 s at p99.

- *If right:* Phases 3 to 5's tight regime is restated as an overload regime wherever it is
  quoted, Phase 3's P4 keeps its class-blindness and loses its size, and the corrected engine is
  the baseline every Phase 9 arm is graded on.
- *If wrong* (the half partition stays within 2x under the wait): releases come faster than the
  pin schedule says, and the first thing to check is a pin held past its sequence's end.

**Measured: holds, with two bands exceeded by the router's check (§9.1).** `polyphonic enforce`,
section 2, seeds 1 / 2 / 3, prefill free. Today's engine ran 1111 / 1253 / 1166 sequences at the
half partition that it could not place, against the pre-measurement's 1111 / 1256 / 1167; the wait
each would have had is 39.6 / 45.4 / 43.8 ms at the median and 244 / 265 / 266 ms at p99. With
`--engine-wait fifo` the interactive class's service p99 against today's engine:

| partition | today's | waiting | factor |
|---|---|---|---|
| 0.5x | 1912 / 1927 / 1924 ms | 7689 / 13971 / 11546 ms | 4.0 / 7.3 / 6.0x |
| 0.6x | 1919 / 1913 / 1928 ms | 4364 / 6303 / 5204 ms | 2.3 / 3.3 / 2.7x |
| 0.75x | 1910 / 1911 / 1928 ms | 1975 / 2299 / 2110 ms | +3 / +20 / +9% |
| 1.0x | 1909 / 1911 / 1921 ms | 1909 / 1911 / 1923 ms | 0.0 / 0.0 / +0.1% |

The first-token tail at 0.75x is 497 / 907 / 698 ms against 49 / 49 / 49, 10-19x; at 0.6x it is
3247 / 5376 / 4396 ms. First fit leaves the half partition at 4494 / 4833 / 5250 ms of service p99,
above 4 s on every seed. The 0.6x service factor and the 0.6x first-token tail exceed the stated
bands, by 10% and 8%, on seed 2 only: those bands were set from a pre-measurement that skipped the
router's check for a queued sequence (§9.1). With prefill taking engine time every row is larger
-- at 0.6x the service p99 is 3812 / 4224 / 3918 ms today and 12166 / 16184 / 14652 ms waiting --
and the old engine's unplaced sequences number 1506 / 1865 / 1633.

**P2 -- A refusal removes the tail it is measured on.**

With a FIFO queue, `perfect` serves every request but the fan-outs it cannot stage, and its
interactive stall p99 is 1.2-4x the refusing arm's at 0.75x, 7-25x at 0.6x and 40-85x at the half
partition, where the refusing arm refuses 1-3.5%, 4.5-8% and 9-12% of requests.

- *If right:* Phase 3's bracket is restated as a comparison of different request sets, and every
  Phase 9 comparison holds the served set equal.
- *If wrong* (queue and refusal within 2x at 0.6x): the refused requests were not the ones whose
  wait sets the tail, and refusal is a cheaper enforcement than this plan assumes.

**Measured: holds, with one cell below its band.** `polyphonic enforce`, section 3, seeds 1 / 2 /
3, `perfect` reservations. The queue's interactive first-token p99 against the refusing arm's, and
what each arm leaves unserved:

| partition | refusing: unserved | queue: unserved | queue's first-token p99 over refusing's |
|---|---|---|---|
| 0.75x | 1.2 / 2.0 / 1.7% | 0.21 / 0.28 / 0.37% | 1.2 / 3.7 / 2.9x |
| 0.75x, prefill taking engine time | 2.4 / 3.4 / 2.5% | 0.8 / 0.9 / 0.9% | 2.9 / 1.9 / 2.4x |
| 0.6x | 4.6 / 6.0 / 5.1% | 1.4 / 1.9 / 1.8% | 13.4 / 6.7 / 11.5x |
| 0.6x, prefill taking engine time | 6.3 / 7.6 / 6.8% | 2.3 / 2.6 / 2.3% | 24 / 23 / 25x |
| 0.5x | 8.9 / 9.8 / 9.4% | 3.2 / 3.4 / 3.0% | 43 / 45 / 45x |

Seed 2 at 0.6x with prefill free is 6.7x against a stated 7-25x. The queue's remaining unserved
share is the fan-outs it cannot stage whole and the continuations their refusal cancels (§9.7).

**P3 -- The order does more than the claim, and it has to be the router's.**

The queue in declared-class order cuts the interactive stall p99 1.6-11x against FIFO at 0.6x under
`perfect` and a p90 claim alike; with the order by class, `perfect`, a p90 claim and the tiered
claim land within 40% of each other on the published workload. The engine's own waiting queue
ordered by class beats its FIFO by 1.5-4.5x and stays 5-36x above the router's order.

- *If right:* the first enforcement is the queue's order, which llm-d's priority bands already
  provide, and an engine priority is not a substitute for admission at the router.
- *If wrong* (the claim moves the tail more than the order does): admission is binding at the
  claim's margin, and the claim deserves the attention this plan gives the order.

**Measured: holds, with two cells past their bands.** `polyphonic enforce`, section 4, interactive
first-token p99, seeds 1 / 2 / 3. The router's queue in class order against arrival order at 0.6x:
`perfect` 2.6 / 1.6 / 1.8x with prefill free and 7.6 / 6.7 / 5.6x with prefill taking engine time;
a p90 of the pooled observed length 3.1 / 1.7 / 3.6x and 11.5 / 9.8 / 8.7x. The top of that range,
11.5x, is above the stated 11x. With the order by class, `perfect`, the pooled p90 and the tiered
claim are within 30% of each other in every cell at 0.6x, against a stated 40%: with prefill taking
engine time, 173 / 188 / 245 ms, 155 / 191 / 188 ms and 167 / 184 / 218 ms. The engine's own queue
by class beats its arrival order 1.5-3.5x across the three regimes and stays 6-50x above the
router's order under `perfect`; the 50x is seed 2 with prefill taking engine time (9481 against
188 ms) and is above the stated 36x. Re-run once every arm was on the corrected engine (§9.14),
only the tiered rows moved: with prefill taking engine time the tiered claim reads 177 / 184 /
236 ms, against `perfect`'s 173 / 188 / 245 ms and the pooled p90's 155 / 191 / 188 ms, still within
30% of each other.

**P4 -- The cancel takes the interactive class to the rebuild floor, and the loss moves.**

With the cancel, continuation, victims by declared class: the interactive stall p99 at 55-66 ms in
every regime from 0.6x up -- the floor `perfect` reaches by refusing -- 45-70% below the same queue
without a cancel at 0.6x, and 80-450 ms at the half partition. The throughput class pays against
the same queue without a cancel: its stall p99 rises 1.03-6x, and its completion p99 by under 20%
in at least eleven of twelve cells from 0.6x up and by 3-50% at the half partition; 1-7% of decodes
are cancelled at least once from 0.75x to 0.6x. The interactive class's service p99 moves by under
3% in every cell.

- *If right:* §9's deliverable is answered on the axis where the loss lives: the router choosing
  the victim moves the overcommit's tail onto the throughput class's first token, and the cancel's
  share of the protection is the last factor of two to three, after the order's.
- *If wrong* (the interactive tail stays above twice the floor with a cancel): the cancel frees
  less than the head of the queue needs, and the first suspect is the victims' exclusive bytes
  being smaller than their claims.

**Measured: the interactive half holds; the throughput half is wrong in three cells from 0.6x up
and on two seeds at the half partition.**
`polyphonic enforce`, section 5, the tiered claim, the queue in class order, continuation, victims
the most recently dispatched; seeds 1 / 2 / 3. The interactive first-token p99 with the cancel
against without it:

| partition | no cancel | cancel | change |
|---|---|---|---|
| 0.75x | 62 / 68 / 63 ms | 56 / 59 / 56 ms | -10 / -13 / -11% |
| 0.75x, prefill taking engine time | 114 / 101 / 88 ms | 60 / 61 / 60 ms | -47 / -40 / -32% |
| 0.6x | 112 / 135 / 108 ms | 63 / 63 / 63 ms | -44 / -53 / -42% |
| 0.6x, prefill taking engine time | 177 / 184 / 236 ms | 66 / 63 / 64 ms | -63 / -66 / -73% |
| 0.5x | 193 / 255 / 236 ms | 66 / 161 / 68 ms | -66 / -37 / -71% |

Every cell from 0.6x up is 56-66 ms, the floor `perfect` reaches by refusing, and the half partition
is 66-161 ms against a stated 80-450. The interactive service p99 moves by under 2.5% in every cell.
The throughput class pays, and more than predicted: its first-token p99 rises 4.0 / 8.9 / 4.5x at
0.75x, 2.7 / 3.4 / 4.0x with prefill taking engine time, 2.0 / 2.3 / 2.2x at 0.6x, 1.7 / 1.2 / 1.3x
with prefill, and 1.14 / 0.97 / 0.99x at 0.5x, against a stated 1.03-6x, two seeds under it. Its
completion p99 is within 20% in nine of the twelve cells from 0.6x up, against a stated eleven: +20
/ +22% on seeds 2 and 3 at 0.6x with prefill free and +54% on seed 1 with prefill taking engine time
(11895 against 7743 ms). At the half partition it is +9 / -6 / -4%, against a stated 3-50%: on two
seeds the cancel finishes the throughput class sooner. The share of decodes cancelled at least once
is 1.2 / 2.3 / 1.5% at 0.75x, 2.8 / 3.3 / 3.2% with prefill, 5.2 / 6.8 / 6.3% at 0.6x, 6.6 / 7.2 /
6.5% with prefill and 8.2 / 9.1 / 8.5% at 0.5x. The tail moves onto the throughput class as
predicted, and onto its completion more than the prediction allowed in the tightest cells.

**P5 -- Two quantiles are a scalar where the classes are drawn alike, and each class's own
distribution is what matters where they are not.**

On the published workload from 0.6x up, the tiered claim and one p90 claim are within 10% on the
interactive stall p99 in every cell, and on the throughput class's completion p99 in at least
eleven of twelve, with a cancel; at the half partition the tiered claim's tails are up to 1.7x
longer. With the batch class at 0.75x: a pooled p90 lets the interactive stall
p99 reach 0.3-1.9 s without a cancel (1.7-4.6 s with prefill time); a p90 of each class's own
distribution holds it to 0.16-0.44 s, and to 62-71 ms with a cancel; the tiered claim's mean for
the throughput class finishes it later than its own p90 does where prefill takes engine time
(completion p99 13-17 s against 8-12 s).

- *If right:* §1's "per-class mix of the two rules" is restated as one declared quantile of each
  class's own distribution, a priority order, and a victim class; borrowing against the mean is
  retired where prefill takes engine time.
- *If wrong* (the tiered claim beats a per-class p90 with the batch class): borrowing buys more
  admitted work than its evictions cost, and the mean half of §1 stands.

**Measured, the published-workload half: the scalar holds on the interactive class and not within
10% on the throughput class's completion.** `polyphonic enforce`, section 6, a cancel, the queue in
class order, seeds 1 / 2 / 3. A p90 of the pooled lengths and a p90 of each class's own are the
same run on seeds 1 and 2 in every regime and differ only on seed 3, which is the claim that two
quantiles are a scalar where the classes are drawn alike. Against the tiered claim, in the nine
cells at 0.75x with prefill and at 0.6x: the interactive first-token p99 is within 10% of the pooled
p90's in all nine (0.95-1.03x). The throughput class's completion p99 is within 10% in four: the
tiered claim reads 1.01 / 1.00 / 0.88x at 0.75x with prefill, 1.11 / 1.03 / 0.86x at 0.6x and 1.41 /
1.03 / 1.31x with prefill at 0.6x, against a stated eleven of twelve, and the 41% is the largest
(11895 against 8451 ms). At the half partition the tiered claim is 0.96 / 1.05 / 0.84x the pooled
p90's completion p99 and 0.99 / 1.85 / 0.93x its interactive first-token p99; the 1.85x is past the
stated 1.7x.

With the batch class, section 9, seeds 1 / 2 / 3. At 0.75x a pooled p90 lets the interactive
first-token p99 reach 473 / 661 / 3960 ms with prefill free and 3794 / 1762 / 6746 ms with prefill
taking engine time, without a cancel, against a stated 0.3-1.9 s and 1.7-4.6 s; the two seed-3
cells are past their bands. A p90 of each class's own lengths holds it to 180 / 241 / 290 ms and
586 / 264 / 547 ms, against a stated 0.16-0.44 s (the two prefill cells at 0.59 and 0.55 s are
past it), and with a cancel to 63 / 63 / 63 and 67 / 64 / 63 ms, against 62-71. The tiered claim's
throughput completion p99 under a cancel is 1.00 / 1.01 / 0.98x the own-class p90's with prefill
free and 0.87 / 1.49 / 0.88x with prefill taking engine time -- 8156 / 11722 / 10676 ms against
9396 / 7857 / 12080 -- where 13-17 s against 8-12 s was stated. On two seeds of three the tiered
claim finishes the throughput class sooner, which is the stated *if wrong*. At 0.6x with prefill it
is 1.59 / 1.68 / 1.52x later (36415 / 44009 / 43492 ms against 22972 / 26153 / 28620) and leaves
the interactive first-token p99 at 4164 / 5320 / 3613 ms against 1493 / 1040 / 1454. Where nothing
binds (1.0x) the claims are within 3% on the throughput completion p99. So what separates the arms
is a class's own distribution, as stated. Borrowing against the mean costs both classes where memory
binds hardest and not at 0.75x, so the condition the prediction named -- prefill taking engine
time -- is too broad: the mean half is retired at 0.6x with prefill and stands at 0.75x.

**P6 -- A quantile claim needs a second trigger.**

With the batch class and the tiered claim, a cancel only at the router's own check leaves the
interactive stall p99 above 1 s on at least one seed of three at 0.75x; triggered also by an
interactive request waiting at an engine, every seed is within 3x of the floor, and the second
trigger never fires under each class's own p90.

- *If right:* a cancel is two mechanisms, and the second is the one a sidecar cannot have without
  the dispatch log.
- *If wrong* (one trigger suffices): overruns land where the router's check already sees them,
  and the engine-side trigger is dropped from the build.

**Measured: wrong. At 0.75x the router's trigger alone stays under 1 s on every seed, and the
second trigger helps one seed there and hurts at 0.6x.** `polyphonic enforce`, section 9, the tiered
claim, the batch class, seeds 1 / 2 / 3. With prefill taking engine time at 0.75x the router's
trigger alone leaves the interactive first-token p99 at 75 / 65 / 691 ms, none above 1 s, against
75 / 69 / 64 ms with both; the engine-side trigger fires 0 / 30 / 8 times, and its eight firings on
seed 3 are the difference between 691 and 64 ms. With prefill free both arrangements are 63 / 63 /
63 ms, the engine trigger firing 1 / 0 / 13 times. At 0.6x with prefill the router alone is 2.7 /
2.3 / 2.7 s and both 4.2 / 5.3 / 3.6 s, the engine trigger firing 34 / 19 / 43 times. Under each
class's own p90 the engine trigger fires 0 times in every regime, as stated.

The first sweep graded this prediction as holding: the router's trigger alone at 3995 / 1174 / 770
ms, the engine trigger firing 86 / 230 / 285 times at 0.75x and 632-830 at 0.6x with prefill. That
build recorded a resubmitted request's remaining length as a new observation (§9.25), so the tiered
claim was drawn from a sample its own cancels had skewed, and the engines overflowed behind it. The
plan's consequence of a wrong P6 was to drop the engine-side trigger. The build keeps it behind
`--cancel-at`, with `both` the default: it differs from `router` only where it fires, and on one
seed in three at 0.75x it is what reaches the floor.

**P7 -- Continuation, not restart, is what the path adds.**

Restarting the victim instead of continuing it leaves the interactive stall p99 within 1.4x from
0.6x up, and raises the throughput class's completion p99 by 30-140% on at least seven of the nine
seeds at 0.6x and at 0.75x with prefill time, and by 30-180% with the batch class; restarts throw
away up to 2.5% of the run's decode on the published workload and 4-9% with the batch class.

- *If right:* §2.3's fourth reason has its measured form: a sidecar can carry a cancel, only the
  component that relayed the tokens can continue the request, and the difference is the victim's
  completion.
- *If wrong* (restart within 15% of continuation): the prefix cache has lost the victim's blocks by
  the time it returns, so a continuation re-prefills most of what a restart does, and the first
  thing to measure is how much of a continuation's chain hits.

**Measured: holds on the published workload.** `polyphonic enforce`, section 7, the tiered claim, a
cancel, victims the most recently dispatched; the throughput class's completion p99 under a restart
against a continuation, seeds 1 / 2 / 3.

| partition | continuation | restart | restart over continuation | decode thrown away |
|---|---|---|---|---|
| 0.75x | 1931 / 1905 / 1945 ms | 2694 / 2277 / 2099 ms | +40 / +20 / +8% | 24 / 42 / 20 s |
| 0.75x, prefill taking engine time | 2369 / 2124 / 2300 ms | 4021 / 3107 / 2845 ms | +70 / +46 / +24% | 75 / 83 / 67 s |
| 0.6x | 2486 / 2564 / 2756 ms | 4640 / 5693 / 4232 ms | +87 / +122 / +54% | 112 / 140 / 134 s |
| 0.6x, prefill taking engine time | 11895 / 6966 / 7368 ms | 14208 / 11436 / 11502 ms | +19 / +64 / +56% | 161 / 231 / 178 s |
| 0.5x | 8960 / 8903 / 9122 ms | 16399 / 18829 / 12628 ms | +83 / +111 / +38% | 239 / 320 / 227 s |

Of the nine seeds at 0.6x and at 0.75x with prefill, seven are 46-122% later under a restart and two
are 19% and 24%, against a stated seven of nine at 30-140%. The interactive first-token p99 is
within 1.15x either way in every cell from 0.6x up. The decode a restart throws away is up to 231
sequence-seconds at 0.6x; by §1.10's arithmetic of some 8,000 sequence-seconds decoded in a run
that is up to about 3%, against a stated 2.5%. The victim that decodes the most last is not better
than the newest: its completion p99 is 0.81-1.53x the newest victim's across the cells, in both
directions.

With the batch class, section 9, the tiered claim, both triggers; the batch class's completion p99
under a restart against a continuation, seeds 1 / 2 / 3: +109 / +119 / +183% at 0.75x with prefill
free, +137 / +90 / +118% with prefill, +57 / +52 / +84% at 1.0x with prefill and +40 / +26 / +26%
at 0.6x with prefill, against a stated +30-180%; the 183% is over it and the two 26% under. The
interactive first-token p99 is within 1.4x either way at 1.0x and at 0.75x with prefill free; with
prefill at 0.75x a restart makes it 1.59 / 1.07 / 1.67x (119 / 74 / 107 against 75 / 69 / 64 ms)
and at 0.6x 0.51 / 0.54 / 1.42x, so seven of the twelve cells are within 1.4x. The decode thrown
away is 442-606 sequence-seconds at 0.75x with prefill free, 553-1007 with prefill, 171-287 at 1.0x
and 648-700 at 0.6x. §1.10's batch row threw away 418 / 641 / 534 s at 0.75x, which is where the
stated 4-9% came from, and 442-606 s is in that range. The share of the run's decode is not graded,
since the run records the seconds thrown away and no total to divide them by.

**P8 -- A threshold gate's cost lands on the evicted class.**

At θ = 0.9 the llm-d-shaped arm protects the interactive stall p99 within 2.2x of the claim arm's,
and the throughput class's completion p99 is 1.5-2.3x the claim and continuation arm's with
prefill time at 0.6x and 0.75x, 2.5-4.7x with prefill free at 0.6x or with the batch class, and
within 1.5x at 0.75x with prefill free. At θ = 0.8 the throughput class's is 3-9x at 0.6x and at
0.75x with prefill time, and the interactive tail up to 7x. It cancels 2-6x as often.

- *If right:* the two shapes differ where §1.3 says they do and nowhere else: the protected class
  sees the same tail, and the evicted class pays for a constant with no unit and for a retry that
  makes it the next victim.
- *If wrong* (a θ exists within 1.3x of the claim arm in every regime): utilisation predicts
  overflow well enough on this workload, and the claim's advantage is the continuation alone.

**Measured: holds on the published workload, with one cell past its band.**
`polyphonic enforce`, section 8, the claim arm being a p90 of each class's own lengths with a
cancel and continuation, the gate arm a threshold, class order, eviction of the newest lower-class
sequence and a restart; seeds 1 / 2 / 3. At θ = 0.9 the interactive first-token p99 is within 1.1x
of the claim arm's at 0.75x, with prefill and at 0.6x, and 1.1 / 2.0 / 1.1x with prefill at 0.6x,
against a stated 2.2x. The throughput class's completion p99 is 1.0 / 1.3 / 1.2x at 0.75x, 2.1 / 2.2
/ 1.8x with prefill, 3.3 / 4.3 / 4.0x at 0.6x and 1.9 / 2.5 / 2.1x with prefill (the 2.5 is past
2.3). The gate aborts 2.3 / 3.5 / 4.0x, 2.6 / 5.8 / 4.2x, 3.2 / 4.0 / 3.5x and 3.5 / 4.2 / 4.7x
as many sequences, inside the stated 2-6x. At θ = 0.8 the completion p99 is 4.0 / 4.5 / 4.2x with
prefill at 0.75x, 5.4 / 8.6 / 5.1x at 0.6x and 3.0 / 6.6 / 4.9x with prefill, inside the stated
3-9x; the interactive tail reaches 6.9x with prefill at 0.6x, inside 7x; and the aborts are 3-12x
the claim arm's. A threshold has no unit and the claim does: the protected class sees the same
tail and the evicted class pays for the constant and for the retry that makes it the next victim.

With the batch class, section 9, the gate arm at θ = 0.9 against the claim arm (a p90 of each
class's own lengths, a cancel, a continuation), seeds 1 / 2 / 3. The interactive first-token p99 is
1.0 / 1.1 / 1.1x with prefill free at 0.75x, 2.7 / 3.0 / 3.5x with prefill taking engine time and
3.6 / 7.0 / 1.9x at 0.6x with prefill, against a stated 2.2x: five of the nine cells are past it,
and at 1.0x the gate arm is the faster, 0.6 / 0.4 / 0.8x. The throughput class's completion p99 is
2.47 / 4.17 / 3.30x with prefill free, 2.32 / 4.44 / 2.40x with prefill and 2.90 / 2.74 / 2.19x at
0.6x with prefill, against a stated 2.5-4.7x, four cells under it, and 1.3 / 1.5 / 1.6x at 1.0x.
The gate aborts 2.3 / 3.6 / 2.6x, 2.5 / 2.0 / 2.1x and 1.2 / 0.8 / 0.8x as many sequences, inside
the stated 2-6x in six of nine; at 0.6x with prefill its aborts are fewer than the claim arm's on
two seeds. With a batch class the protected class's tail is therefore not the same under the gate
where memory binds: the evicted class pays a little less than stated, and the interactive class
pays too.

**P9 -- PLAS needs a class it can see.**

With a p90 claim and no cancel, PLAS order puts the interactive stall p99 within about 2x of
FIFO's at 0.6x, on either side, and 2-15x above class order's. With the batch class it serves the
throughput class first: its stall p99 under 110 ms against the interactive class's 0.2-0.7 s with
prefill free and 3.6-8 s with prefill time. A cancel by attained service makes the interactive
tail worse than no cancel in every regime.

- *If right:* the comparison §9 names lands on its premise: an order needing no output length and
  no declaration orders by program length, which separates the classes only where their programs
  differ, and here ranks agent sessions last.
- *If wrong* (PLAS within 1.5x of class order at 0.6x on the published workload): attained service
  tracks the declared class on this trace, and the declaration is worth less than it looks.

**Measured, the order half: holds, with one cell past its band; the cancel half holds in all 15
cells.** Section 4, a pooled p90 claim,
no cancel, interactive first-token p99 at 0.6x. By attained service against arrival order, seeds 1
/ 2 / 3: 0.68 / 1.5 / 0.74x with prefill free and 1.2 / 1.9 / 0.72x with prefill taking engine
time, so within 2x on either side. Against class order: 2.1 / 2.5 / 2.7x and 14 / 19 / 6.3x; the
19x is above the stated 15x. The cancel half, as a comparison arm that takes the program with the
most attained service (`--victim attained`, §9.15), makes the interactive first-token p99 worse
than the same order without a cancel in every cell of section 5: 348 / 971 / 355 ms against 59 / 72
/ 61 at 0.75x, 1420 / 1605 / 1406 against 168 / 152 / 104 with prefill, 2064 / 2832 / 1724 against
260 / 400 / 219 at 0.6x, 6500 / 8013 / 7104 against 2134 / 3548 / 990 with prefill, and 9868 /
11684 / 9582 against 4814 / 6580 / 4356 at 0.5x.

With the batch class, section 9, a p90 of each class's own lengths, no cancel, seeds 1 / 2 / 3: by
attained service the batch class's first-token p99 is 71 / 83 / 78 ms with prefill free at 0.75x,
138 / 82 / 130 ms with prefill and 106 / 138 / 147 ms at 0.6x with prefill, under 110 ms in five of
the nine memory-bound cells and 147 ms at most. The interactive class's is 256 / 384 / 721 ms
(stated 0.2-0.7 s; the 721 is past it), 3.0 / 4.6 / 8.0 s (stated 3.6-8 s; the 3.0 is under and the
8.011 over) and 17.9-23.8 s. Arrival order puts both classes at 1.0-2.2 s with prefill at 0.75x.
Where nothing binds (1.0x) the two orders are indistinguishable: 288 / 219 / 308 ms against 268 /
201 / 282 ms for the interactive class.

**P10 -- A client that leaves is the cancel's first job.**

With the engine's own wait, 20% of clients leaving mid-decode costs the interactive class 1.9-3.4x
on stall p99 if the departure is leaked rather than propagated, and 5% costs it 1.2-1.6x. Under the
router queue in class order at 0.6x with prefill time, the leak lands on the throughput class: its
stall p99 2-4x at 20% and within 15% at 5%.

- *If right:* cancellation's correctness job is worth as much as its priority job wherever memory
  binds, and the proxies that have leaked it were leaking capacity.
- *If wrong* (a leak within 1.2x at 20%): the leaked decodes end before the queue would have used
  their space, which would put the cost in batch slots rather than memory.

**Measured: holds at 20%, and at 5% only where prefill is free.** `polyphonic enforce`, section 10,
the interactive first-token p99 of a leaked departure over an aborted one, seeds 1 / 2 / 3. Under
the engine's own wait: 20% leaving, 2.3 / 1.7 / 2.1x at 0.75x with prefill, 2.2 / 1.8 / 2.1x at 0.6x
with prefill and 2.1 / 2.4 / 3.2x at 0.6x with prefill free, against a stated 1.9-3.4x, two cells
under; 5% leaving, 1.1 / 1.1 / 0.9x and 0.9 / 1.0 / 1.3x with prefill and 1.2 / 1.3 / 1.3x free,
against a stated 1.2-1.6x, so the 5% prediction holds only with prefill free. The cell with nobody
leaving at 0.6x with prefill free is 3247 / 5376 / 4396 ms against §1.13's 3247 / 4560 / 4545: seed
1 to the millisecond and the others not, the cause not isolated. The leak holds 200-830
sequence-seconds of decode that an abort frees, in proportion to the share leaving, and a leaked
run's tails equal the run where nobody leaves: a departure that is not propagated costs nothing it
did not already cost. Under the router queue in class order at 0.6x with prefill, the throughput
class's first-token p99 is 2.3 / 6.8 / 3.5x at 20% (stated 2-4x; the 6.8 is past it) and 1.3 / 1.2 /
1.1x at 5% (stated within 15%; the 27% is past it), and with prefill free 2.2 / 2.3 / 1.8x at 20%.

**P11 -- The stalled-stream buffer is under two megabytes a node.**

If every stream on a node stalled for its whole decode, the buffered tokens peak at 3,200-4,700 a
node on the published workload and 8,200 with the batch class: 0.6-1.7 MB at 200 bytes a token,
0.01-0.02% of a node's 8 GiB of DDR. **Arithmetic on a counter, not a run.**

- *If right:* §2.6's memory-arbitration sentence is retracted for text, and buffering rather than
  pausing the decode is the rule, at 75-1,600x less memory.
- *If wrong* (a peak above 10 MB): long outputs stall together, and the buffer becomes a ledger
  occupant after all.

**Measured: holds, with the batch class's peak past its figure.** `polyphonic enforce`, section 11,
seeds 1 / 2 / 3, the tokens every single decode in flight has emitted, per node, at each arrival. On
the published workload the busiest node's peak is 3853 / 4053 / 4161 tokens at 1.0x, 4092 / 3956 /
4638 at 0.75x with prefill and 3884 / 4028 / 4340 at 0.6x with prefill, against a stated
3,200-4,700; the mean a node holds is 1936-2177. That is 0.77-0.93 MB at 200 bytes a token and
0.009-0.011% of a node's 8 GiB of DDR. With the batch class the peak is 7110-8876 tokens, 1.42-1.78
MB and 0.017-0.021%, against a stated 8,200 and 1.7 MB; the 8876 and the 1.78 MB are past them, and
every cell is under two megabytes. Fan-out agents, 22% of decodes, are in no flight and so in no
count.

---

## 3. What enforcement must and must not do

Eleven rules. The first is the gate; the fourth is the one most likely to be broken for a good
reason.

1. **The gate.** With every new bit off, byte-identical to `HEAD` on the reproducible set as
   `phase-6.md` left it, including the change to how outcomes are recorded (§4.1). And each bit has
   a case in which it must change nothing: `--engine-wait` at four times the published grant,
   where no sequence fails to fit (§9.2: at the published partition a few do); `--queue` where no
   check fails; `--cancel` with no lower-class sequence in
   flight; `--disconnect 0`; `--batch 0`. Checked after every work item.
2. **Enforcement follows `own::authority`.** The router cancels what it dispatched and releases
   what it reserved. It never names an engine block: the pins released and the never-written output
   removed are the engine's own response to an abort, as vLLM's `finish_requests` is. The census
   stays at 13.
3. **A cancel requeues.** No arm turns a cancel into a refusal; the restart arm re-sends at once.
   Fan-outs that cannot be staged whole are the only refusals under a queue, counted apart.
4. **Victims by declared class only.** A sequence is a victim only for a request of a higher
   declared class, never a fan-out agent (§7), never by inferred class or tenant. PLAS orders the
   queue and chooses no victims.
5. **Nothing tuned.** The claim is a declared quantile; the victim rules have no threshold; the
   gate's θ is a condition of the llm-d arm, swept and printed, never a parameter of the integrated
   one; the disconnect share is a condition, swept.
6. **The served set is held equal.** Queue arms are compared with queue arms. A refusing arm is
   printed with its refused share and never quoted against a queue arm without it.
7. **One bit per mechanism.** `--engine-wait`, `--queue`, `--admit quantile | tiered | gate` with
   `--claim-quantile`, `--pooled` and `--gate`, `--cancel`, `--victim`, `--disconnect`, `--leak` and
   `--batch` are separately selectable, and the headline runs change one at a time.
8. **A condition's randomness is its own stream** (Phase 4's rule 6). Departures and the batch
   class draw from streams separate from the workload's, so the base trace is byte-identical at
   every setting.
9. **The engine acquires no policy** (`phase-3.md` rule 3). Its waiting queue is first come first
   served or ordered by a priority the dispatch carries -- both vLLM's own. No class-aware eviction,
   no reservation inside a partition.
10. **Measure, do not repair** (`phase-2.md` rule 1). If the cancel does not reach the floor it is
    reported and not retuned mid-phase; the victim rules are built as listed.
11. **Three seeds, and the regime with every number.** A number at the half partition is a number
    about an overload, and its waits grow with the run's length; every cell names its partition,
    its prefill setting and its workload.

---

## 4. Work items

Four increments, in order, each ending with a number: the accounting and the engine, the queue and
its claims, the cancel, then the workload, the instruments and the command. Within each, nothing
that can move a number lands before the items that cannot. If the phase has to stop early it stops
at an increment's end.

### 4.1 Outcomes by request

A request gets an id at arrival, and `Machine` keeps an outcome per request: open while it waits,
closed when it is dispatched and its cost known, reopened when it is cancelled and closed again by
its continuation, with the latency measured from the original arrival. `drive` tallies closed
outcomes once the trace has drained, and `Machine::finish` advances the clock until nothing waits.
With every new bit off every request closes at its arrival, so the tallies are today's -- the
pre-measurement's copy reproduced `distributed` this way to four decimals on three seeds. No
consumer yet: a no-op, checked as one.

### 4.2 The engine waits

`Hierarchy::kv_need`: the bytes of a sequence's chain and output not already pinned, against the
partition less what is pinned. Under `--engine-wait`, a sequence that does not fit waits in its
node's queue -- `fifo` with head-of-line blocking, or `priority` by the dispatch's declared class --
and is started at the first arrival after it fits, holding its blocks from then on; a request the
router dispatched is never re-checked by the router. `run_paired` is split at the point the engine
admits. An instrument counts what would have waited under the old engine and for how long, from the
pin schedule (`EngineCache::release_schedule`). The first item that moves a number (§1.1).

### 4.3 The queue

`--queue fifo | slo | plas`. A request that no node's check admits waits at the router; at each
arrival the queue is served in its order with head-of-line blocking, and a request leaves placed
among the nodes whose check admits it (§1.4). A fan-out is staged whole as today and retried at
each arrival while it waits. The queue's depth and each request's wait there are recorded.

### 4.4 The claims

Observed output lengths, per declared class, as histograms on the path -- the per-class means of
Phase 4's `--observables` widened. `--admit quantile` claims each request at `--claim-quantile`
(0.9) of its own class's distribution, `--pooled` of the pooled one; `--admit tiered` claims
interactive work at the quantile and throughput work at its class's mean; `--admit gate` admits at a
node while its pinned KV is under `--gate` of the partition, requests with no KV exempt. `Reserve`
gains the three, and the claim a request was admitted against is what its reservation holds.

### 4.5 Programs and attained service

`Request::program`: the session for a chat turn, the task for a flow stage or a fan-out's
continuation, the request itself for a batch request. Attained service accrues a call's decode
time when it completes. Read only by `--queue plas`.

### 4.6 The abort

Handles on the three ledgers of §1.6: a sequence id on `Engine`'s batch entries, `EngineCache::seal`
returning the pin set it filed, `Reservations::commit` returning the claim. `Machine` keeps a
flight per dispatched decode -- node, start, end, class, program, its handles, its request -- and an
abort releases the three, removes the output blocks the decode had not written, and charges the
request admitted in its place one step.

### 4.7 The cancel

`--cancel continue | drop`, `--victim recent | remaining`. The router's trigger: the head of the
queue fits no node, and at some node the lower-class flights' exclusive bytes cover its deficit;
the fewest victims, in victim order, at that node. The engine's trigger: a request of a higher
class waits at an engine; victims there in victim order until it fits. A continuation is the
prompt plus the whole blocks decoded, with the tokens left and the original arrival, requeued at
its class's place; a restart is the original request, requeued at once. Counted by trigger.

### 4.8 Departures

`--disconnect p`, `--leak`: a share `p` of decodes, chosen from a stream of their own, whose client
leaves at a uniform point of the decode. Propagated, the departure is an abort at the next arrival;
leaked, the decode runs to its end and its output goes nowhere. The departed request leaves every
latency tally and is counted apart.

### 4.9 A throughput class with a shape

`--batch f`: after each request, with probability `f` from a stream of its own, a request with an
8-block prompt nothing shares, 200-600 output tokens, a throughput declaration and no session --
`taxo.md`'s batch inference at its plainest. Phase 7's presets replace it.

### 4.10 The buffer

In-flight emitted tokens per node, sampled at each arrival: the tokens every decode in flight has
produced so far. Printed as bytes at `STREAM_BYTES_PER_TOKEN` = 200, beside the node's DDR. No
ledger class (§1.14).

### 4.11 Instruments

| instrument | measures | over |
|---|---|---|
| tails by class | stall p99 and p90, service p99 and mean, completion p99, per declared class | every closed outcome |
| waits | time at the router and at the engine, by class; queue depths | every request that waited |
| probe | sequences the old engine ran with no memory; the wait they would have had | every preemption |
| cancels | by trigger and victim rule; share of decodes cancelled at least once; re-prefill work; decode thrown away by restarts | every abort |
| departures | aborted and leaked; decode run after a departure | every decode |
| buffer | in-flight emitted tokens per node, peak and mean; the bytes they imply | every arrival |
| served | closed, refused fan-outs, departed | every request |

These are the instruments the pre-measurements approximated, and §8's figures are reproduced with
them first.

### 4.12 `polyphonic enforce`

A reproducible sweep, as `fleet` is for Phase 6: no control crossing charged, seed-deterministic,
three seeds for every cell, the `belief` cluster at four partitions, prefill free and taking engine
time, on both workloads.

1. the gate, as printed check lines
2. the engine: today's against `--engine-wait` FIFO, first fit and by class, with the probe (P1)
3. refusal against a queue (P2)
4. the order: FIFO, by class, PLAS, under each claim, and the engine's own order (P3, P9)
5. the cancel, both triggers, continuation (P4, P6)
6. the claims: tiered against each class's own and the pooled quantile, both workloads (P5)
7. continuation against restart; the victim rules (P7)
8. the llm-d-shaped arm, θ at 0.8, 0.9 and 0.95 (P8)
9. departures (P10)
10. the buffer (P11)

`distributed` takes the same flags. `code-review` takes none, as it took none of Phases 4 to 6's.

### 4.13 Report and publish

| target | change |
|---|---|
| `owned-and-observed.md` §9 Phase 9 | a **Status** line; the phase restated as the engine's correction, the queue's order, and the cancel |
| `owned-and-observed.md` §1 | the victim paragraph corrected for vLLM's priority policy (§1.2); two-tier admission restated as one declared quantile of each class's own distribution, a priority order and a victim class, with the mean half by its result (P5); the half partition named as an overload wherever Phase 3's bracket is quoted |
| `owned-and-observed.md` §2.3, §2.6 | llm-d's eviction as the sidecar carrying a cancel; continuation as what the path adds; the buffer sentence retracted for text (§1.14) |
| `owned-and-observed.md` §4 | `slo` read by admission and by the queue; `Deadline(t)` still waiting for a consumer |
| `owned-and-observed.md` §5 | property 9's cancellation half with its size; continuation |
| `owned-and-observed.md` §8 | *What gets harder* item 6 with departures measured; the interface list's cancellation as built: abort, the priority field, `return_token_ids` |
| `residency-ledger.md` | an *Enforcement* section, every figure with its partition, prefill setting and workload; *Standing* rows for each prediction and for the class-blind row's size |
| `phase-3.md` | nothing: it keeps its results as measured |

Whether `--engine-wait` becomes the default is decided after the numbers, as Phases 3 to 6 decided
their own bits. It differs from them as Phase 6's engine corrections did: it moves every published
number at a partition that binds.

---

## 5. Verification

- **Byte-identity with every new bit off**, against the commit before this phase, on the
  reproducible set -- `residency`, `flows`, `placement`, `volatility`, `ownership`, `price`,
  `belief`, `influence` and `fleet` -- at a reduced `--ops` and a second seed, after every work
  item. `distributed`, `code-review` and `data-path` get the structural smoke run.
- **The gate** (rule 1), one check line per bit.
- **Every request is closed exactly once**, or counted as a refused fan-out or a departure; a
  cancelled request's latency runs from its original arrival; the queue is empty when `finish`
  returns.
- **The engine never starts what does not fit.** Under `--engine-wait` no sequence is preempted,
  and the sequences admitted at each step fit the partition with their pins, asserted.
- **An abort releases what the flight held, and only that**: the batch loses one entry, the
  reservations their claim, the partition its pins; pinned bytes, reserved bytes and batch width
  return to their values without the flight. The never-written output blocks leave the partition;
  the decoded ones stay, unpinned.
- **The queue serves in its order.** Under `slo` no throughput request leaves the queue while an
  interactive one waits that some node admits; under `fifo` none leaves before an earlier one.
- **Victims are of a lower class.** No interactive sequence is ever cancelled, no fan-out agent,
  and a cancel's freed bytes cover the deficit it was issued for or it is counted as futile.
- **The census.** `cargo build --release --features census` still emits 13 warnings.
- `cargo fmt --check`, `cargo clippy --all-targets` and `cargo test` clean.

---

## 6. Risks

1. **A modelling correction read as a policy's win.** P1's factor is what today's engine was not
   charging. The arms are credited against each other on the corrected engine, never against the
   old one, and a reader who quotes "10x" from §1.1 is quoting a model, not the cancel.
2. **The floor is a constant.** 55-66 ms is the prefill of the longest prompts plus one step; it
   moves with `KV_BLOCK_NS`, with the longest tenant prefix and with the one step a cancel waits.
   Results stated against it are stated against those.
3. **The engine waits where vLLM thrashes.** The correction holds an arrival until its whole
   sequence fits; vLLM starts it on its prompt and preempts a running sequence when one cannot
   grow. Both are honest about memory and they differ in who pays -- the arrival here, the most
   recent or lowest-priority runner there -- which is the subject of this phase. §7 says what
   growth-time preemption would need.
4. **The overload regime's numbers are about the run's length.** At the half partition queues grow
   for the whole run, so waits scale with 15,000 requests and would be larger at 60,000. Quoted only
   as an overload and never as a size.
5. **The classes are drawn alike on the published workload, and the batch class is a
   caricature.** Two of the predictions turn on the difference (P5, P9); the batch class's shape --
   5% of requests, 200-600 tokens, nothing shared -- is chosen to differ, not drawn from a trace.
6. **The llm-d arm is an emulation.** A utilisation gate stands in for a detector that can also
   read queue depth or a concurrency cap; eviction is restricted to the lower class, as llm-d
   restricts it to negative bands; a caller's retry is immediate, the most favourable case. The
   arm is a shape, swept over θ, and is not llm-d's numbers.
7. **PLAS is an order, not Autellix.** No quanta, no swap, no anti-starvation, so P9 speaks to the
   order's premise -- what attained service separates -- and not to the system's measured gains.
8. **Continuation assumes two things.** The router keeps the token ids it relayed, which vLLM can
   return on the stream; and the engine's prefix cache holds the aborted blocks until the
   continuation returns, which the model gives it exactly as it would any freed block.
   Retokenisation at the boundary is assumed away, which the token-id form makes true.
9. **Departures have no provenance.** The share is swept and published as a range (§1.13).
10. **The pre-measurements came from outside the repository and are emulations** (Phase 4's risk
    9): a queue polled at each arrival, so a dispatch can be up to 4 ms late; a cancel landing in
    one step; observed lengths recorded at dispatch rather than completion; fan-outs left on
    today's path. §8 says how each was taken.
11. **Runtime.** About 600 runs at under a second each on this host: minutes as parallel processes.

---

## 7. Out of scope

- **Growth-time preemption in the engine**, and with it the half of vLLM's priority policy that
  chooses a running victim (§1.2). It needs an engine that allocates output per step rather than
  at admission. Under it the engine would already move growth-time loss onto a lower priority, and
  the cancel's distinct job would narrow to admission-time displacement and to anything that crosses
  nodes, which is what this phase measures.
- **Authority.** `ReadOnly`, `DraftOnly` and `SideEffecting` are Phase 7's. Every victim here is
  requeued as a continuation, which is the `ReadOnly` and `DraftOnly` behaviour; no request in this
  workload has a side effect a cancel could tear, and `SideEffecting`'s non-revocable lease is Phase
  7's to build on this cancel.
- **Fan-out agents as victims, and fan-outs queued whole as anything but a retry.** A fan-out is
  admitted all-or-nothing (§5's property 4), and cancelling one agent strands its siblings' work; a
  fan-out cancelled whole, and a gang that holds its place in the queue rather than retrying at
  each arrival, are the multi-agent form of this phase's question and its natural successor. Agents
  are 22% of decodes on the published workload, and every class tail here excludes them.
- **`Deadline(t)` and a queue TTL.** The queue is what would consume a deadline; nothing in these
  arms expires, so the field stays unbuilt (§1.4).
- **Fair queueing between tenants.** `phase-6.md` deferred it to this queue. It is a third order
  beside `slo` and `plas`, and the neighbour scenario is where it would be measured; it lands on the
  same queue after this phase's class axis.
- **Hedging and retry against a stateful backend** (§2.6's third stream semantic). A hedge
  duplicates a prefill; the restart arm is the only retry here.
- **A real proxy, and backpressure as a mechanism.** No stream is parsed and no receive window
  modelled; the buffer is a counter (§1.14).
- **Observing reclaim on the step-aligned header.** The simulated engine frees at the next step by
  construction (§1.3).
- **The belief.** Every run uses the exact view; admission reads owned reservations, which the
  belief does not touch.
- **Audio and image output**, the regime in which a stalled stream's buffer would be a ledger
  occupant.

---

## 8. Pre-measurements

All taken on an instrumented copy of `01b2c1e`, run outside the repository and not committed. With
every hook set to today's semantics the copy reproduces `distributed`'s served count and mean
service to four decimals on seeds 1 to 3 (491.557 / 482.903 / 481.006 ms at the half partition).
Rows use the `belief` cluster of §1.15 with `--throughput 0.3`, the partition at 0.5, 0.6, 0.75 or
1.0 of the published grant, prefill free or taking engine time (a 1 s window, nothing free), seeds 1
to 3, unless they say otherwise. Every queue was polled at each arrival, so a dispatch could be up
to one inter-arrival late (4 ms); fan-outs took today's path throughout -- staged whole on the
router's partition check, refused if they did not fit, never queued or cancelled -- and every class
tail below excludes their agents.

| name | what | how | headline |
|---|---|---|---|
| *probe* | what today's engine hides | at each preemption, read-only, the earliest in-flight end by which the pins released cover the sequence's unpinned chain and output, counting a block when its last pin goes | §1.1's first table |
| *wait* | an engine that holds what it runs | a sequence whose unpinned chain and output exceed the partition less its pins waits in its node's queue -- FIFO with head-of-line blocking, first fit, or by declared class -- and starts at the first arrival after it fits; `none` at the router | §1.1's second table; first fit at the half partition, decode mean 1.44-1.58 s and p99 5.0-5.8 s |
| *queue* | refusal against a queue | `perfect` reservations; a request no node's check admits waits at the router, FIFO, head-of-line, and is placed when it leaves among the nodes that admit it; refusal arm as `price` runs it; engine as *wait* | §1.4's table |
| *order* | the queue's order against its claim | FIFO and class order under `perfect`, a pooled p90 and the tiered claim; the engine's own queue by class under `none` | §1.5's table |
| *cancel* | a cancel with continuation | tiered claim, class order, the router's trigger: when the head fits no node, the lower-class flights at the node needing the fewest are aborted in victim order -- batch entry, pins and reservation released, never-written output blocks removed -- and requeued as prompt plus whole decoded blocks with the original arrival; the head charged one step base (7 ms); victims most recent or most remaining | §1.7's table; the two victim rules differ by seed in both directions, up to 1.8x on the throughput class's completion p99 |
| *tiers* | one quantile against two | as *cancel*, a p90 claim for both classes against the tiered one | §1.8's first table |
| *batch* | a throughput class with a shape | every session interactive; after each request, with probability 0.05 from a stream of its own, an 8-block unique prompt, 200-600 tokens, throughput; claims from per-class histograms or the pooled one, both triggers | §1.8's second table; 727 batch requests among 6,991 decodes on seed 1 |
| *trigger* | the engine-side trigger | as *batch*, tiered claim, the router's trigger alone against both: an interactive request waiting at an engine aborts lower-class flights there in victim order until it fits | §1.9's table |
| *drop* | restart against continuation | as *cancel*, the victim requeued as the original request at once; decode thrown away is the victim's progress times its decode time | §1.10's table |
| *gate* | llm-d's shape | a node admits a request with KV while its pinned KV is under θ of the partition; when the head fits no node, the node with the most lower-class flights evicts the most recently dispatched until the gate admits, each requeued as a restart; class order; θ at 0.8 and 0.9 | §1.11's table |
| *plas* | Autellix's order | `Request::program` added -- the session, the task, the batch request -- and attained service accrued as each call's decode time when it completes; the queue ordered by its program's attained service; a cancel arm aborting the flight with the most attained service above the head's | §1.12's table; the cancel arm's 1,300-2,500 cancels at 0.6x |
| *disconnect* | departures | a deterministic 5% or 20% of decodes leave at a uniform point of the decode; propagated as an abort at the next arrival, or run to the end; under the engine's own wait with `none`, and under the router queue in class order with a p90 claim | §1.13's table |
| *buffer* | what a stalled stream holds | at each arrival, per node, the tokens every flight has emitted so far, its progress times its output | §1.14: peak 3,200-4,700 a node, 8,226 with the batch class |

---

## 9. What the build found

Increment 1, in the order the findings arrived.

### 9.1 The router's check, applied to a queued sequence, moves an overload's tail by up to 18%

The pre-measurement dispatched a sequence out of an engine queue without asking the router again,
and queued it without asking at all. The build asks once, when the sequence arrives, as every
dispatch has: a request whose prompt cannot be reserved is refused before it can wait. At the
published `none` reservation that refuses 0.01-0.06% of requests, and it moves the half-loaded
regimes by more than its size:

| 0.6x, prefill free, arrival order | seed 1 | seed 2 | seed 3 |
|---|---|---|---|
| interactive service p99, check skipped (pre-measurement) | 4364 ms | 5435 ms | 5362 ms |
| interactive service p99, check applied (built) | 4364 ms | 6303 ms | 5204 ms |
| interactive first-token p99, check skipped | 3247 ms | 4560 ms | 4545 ms |
| interactive first-token p99, check applied | 3247 ms | 5376 ms | 4396 ms |

With the check skipped, a build with the same switch reproduces every pre-measured figure of
§1.1's second table to the digit, first fit and class order included (2698 / 3094 / 2917 and 2522
/ 3127 / 3251 ms of service p99 at 0.6x), which is what reconciles the two. Seed 1's figures are
unchanged by the check. The lesson is §6 risk 4's, measured: a regime that is queueing without bound
amplifies a handful of changed admissions into a different tail, so a number at 0.6x or below is
quoted with its seeds and its router check, and the plan's bands for those cells were too narrow.
The check stays, because it is the router's existing policy and the queue, in increment 2, will
replace it with a wait.

### 9.2 At the published partition a few sequences do wait, so the gate moves to 4x

Rule 1 said `--engine-wait` changes nothing at the published partition, "where no sequence fails to
fit". Some do: 0.0 / 0.0 / 0.1% of requests queue with prefill free and 0.6 / 0.0 / 0.4% with
prefill taking engine time, and the old engine ran 0 / 1 / 3 and 19 / 15 / 10 sequences it could
not place. Their waits are short -- median up to 44 ms, p99 up to 116 ms -- and move no
interactive tail, but the arm is not byte-identical there and §3 is corrected. The gate is four
times the grant, where every sequence fits: identical to off for arrival order, first fit and class
order, with and without prefill time, on three seeds, and nothing queued.

### 9.3 A sequence larger than the whole partition runs at once

A sequence whose unpinned blocks exceed the partition can never fit, and waiting for it would
never end. It starts at once and is preempted as today, counted as `unfittable`. None is reached
at any partition measured -- 0 in each of 198 arm results over three seeds, three partitions and
both prefill settings -- because the router refuses a prompt larger than the partition first; the
case is exercised by a constructed sequence in the tests.

### 9.4 `serve_request` cannot defer, so the machine has `submit`

A request that waits has no cost when its call returns. `Machine::submit` returns `Closed(cost)` or
`Open(id)`, closed outcomes are drained with `drain_closed`, and `finish` advances the clock until
nothing waits. `serve_request` is a wrapper that panics if asked to defer. `drive` tallies a
request from a `Shape` taken at submission and the cost that closes it, so a request tallied late
is tallied as it would have been at once; with every bit off every request closes at submission and
the reproducible set is byte-identical.

### 9.5 `distributed` is deterministic once its measured lines are set aside

`phase-6.md` runs `distributed` as a structural smoke check because it charges a measured crossing.
With `--crossing native` its per-arm tables are byte-identical between builds and between runs; what
still varies is a trailing block of overhead lines measured on the host (the gRPC and ring cost per
decision, the warm-rate shares and the hypothetical-work rows). The gate compares `distributed`
under `--crossing native` with those four kinds of line filtered, so it is now a byte comparison
and not a smoke run.

### 9.6 The instruments

`--probe-engine` reads, for each sequence today's engine runs with no memory, the wait it would
have had from the cache's own release schedule, and changes nothing: the arm is byte-identical with
it on at 0.75x and 0.5x. `polyphonic enforce` takes sections `gate` and `engine`; the others are
§4.12's, later.

### 9.7 A fan-out is refused, not queued

§4.3 said a fan-out is "retried at each arrival while it waits". The build keeps today's path for
a gang, as the pre-measurement did: staged whole on the router's check and refused if it does not
fit, which also cancels the task its continuation would have run. Retrying from the queue would
need a feasibility test with no side effects -- staging counts a refusal and cancels a downstream
each time it fails -- and a gang that holds its place in the queue is §7's successor. The
consequence is the "not served" column: under a queue it is the fan-outs refused and the
requests their refusal cancels, 0.2-3.4% across the partitions, against 1.2-9.8% when the router
refuses single requests too.

### 9.8 The queue arms reproduce the pre-measurement to the digit

Unlike the engine arm (§9.1), every figure of §1.4's table reproduces exactly: the refusing arm's
first-token p99 at 0.6x is 22 / 33 / 22 ms and 54 / 56 / 54 with prefill taking engine time, the
queue's 294 / 221 / 252 and 1311 / 1268 / 1360, and at the half partition 2138 / 2360 / 2305. A
queued request is placed when it leaves, among the nodes whose check admits it, so no check is
skipped, which was the one place the engine arm's emulation differed from the build.

### 9.9 A request waits once and is placed once

Placement moved into `Machine::place`, which both a fresh arrival and a request leaving the queue
enter. A queued request is decided, scored and counted as a decision when it leaves and not when it
arrives, so `decisions` is the same under a queue as without, and `d` of §2.3 is unchanged. It
carries its original arrival through the router queue and the engine queue alike: a request that
waits at the router and again at an engine has both waits in its `queue_ns`, measured from the
arrival that started the first.

### 9.10 `--quantile` was taken

`--quantile` is Phase 4's scoring quantile in `BeliefArgs`, and a second flag of the name made
clap lose its default and fail every `distributed` run. The claim's quantile is `--claim-quantile`,
and §1.8, §3 and §4.4 say so.

### 9.11 Attained service accrues at completion

A call's decode time is added to its program when the call ends, not when it is dispatched, which is
Autellix's definition and the pre-measurement's. A completion heap holds `(end, program, decode)`
and is drained at each arrival before the queue is served. It is filled only under `--queue plas`.

### 9.12 What the claims observe

Output lengths are recorded per declared class, at dispatch, as a histogram of 2048 bins, and a
claim before any observation falls back to the request's declared `max_tokens`, Phase 4's rule. The
pooled claim merges the two classes' histograms at each decision. `--admit gate` reads the node's
pinned KV through the engine cache and exempts a request that holds no KV, as the pre-measurement
did; it is applied only to a request at the router and never to a fan-out agent's staging.

### 9.13 With a cancel every decode stays open until it ends

A cancel can reach a request after its first dispatch, so under `--cancel` no single decode closes
at submission. Each registers a flight -- its node, its handles on the three ledgers, its decode's
start and end, its request and its cost -- and `submit` returns `Open`; a flight closes at the first
arrival at or after its end, and `finish` runs the clock until none is left. A cancelled flight's
cost is discarded and its resubmission closes the request, so the tally is the continuation's cost
with the wait measured from the original arrival. The stall that follows includes the decode done
before the cancel, since stall is service less the final dispatch's decode; that is the
pre-measurement's convention and part of why the throughput class's first-token tail rises 1-9x.

### 9.14 Every arm in `enforce` is on the corrected engine

§1.1 chose to grade every Phase 9 arm on the engine that waits. Increment 2's `enforce` arms left
the quantile, tiered and gate arms on today's engine, which runs a sequence it cannot place. The
builder now defaults to a waiting engine, in arrival order and by class under a class-ordered queue,
and the sweep was run again. Only rows with an under-reserving claim moved, because only they
overflow an engine: the tiered rows of section 4, 167 / 184 / 218 ms becoming 177 / 184 / 236 ms
with prefill at 0.6x. The queue and `perfect` rows are identical.

### 9.15 P9's cancel half needs a victim rule §3 forbids

§3 rule 4 says PLAS orders the queue and chooses no victims, and §1.12 chose "an order with no
cancel", yet P9 predicts what a cancel by attained service does. The prediction can only be graded
by building the arm, so `--victim attained` is a labelled comparison arm that takes the sequence
whose program has attained more service than the head's. The integrated arm's victims are still
chosen by declared class alone.

### 9.16 The claims observe every decode, fan-out agents included

Output lengths are recorded where a decode is dispatched, which includes the agents of a fan-out,
as Phase 4's observed mean always did; the pre-measurement recorded only single requests. The
quantile arms move by up to 10% of a first-token p99 against the pre-measurement for that reason
alone: the pooled p90 with arrival order at 0.6x is 380 / 269 / 421 ms built against 362 / 289 /
411 pre-measured.

### 9.17 A decode in flight is identified by its end

`Engine` keeps its in-flight decodes as a heap of `(end, model)` with no id, and an abort removes
one entry that matches. Two decodes with the same end and model are interchangeable to every
reader, so an id would add nothing, and the abort's test asserts the entry is found. The pins and
the reservation, which readers distinguish, do carry sequence ids.

### 9.18 The cancel changes no request where nothing is overcommitted

Under a cancel the order in which requests close differs from submission, so the gate compares the
sorted per-class tallies and the totals and not the arrival-order vectors `identical` checks.

### 9.19 Grants were sized by whichever arm ran first

`Lab` sizes an engine's partition from a ledger run and caches it by distance, regime, trace and
batch class; the ledger run has no engine and reads no partition scale, so one run serves every
scale. The ledger run inherited the first arm's enforcement flags, and under a cancel `finish` ticks
the clock past the last arrival to close the decodes still in flight, each tick adding an occupancy
sample that is nearly empty. A sweep whose first arm at a partition cancelled therefore sized every
later arm's grant a little smaller, and the claims, restart and llm-d sections did exactly that:
their first arm at each partition was a cancel arm. The same configuration read 441 / 873 / 492 ms
of throughput first-token p99 in one section and 668 / 1156 / 669 ms in another. The ledger run now
takes no enforcement, `finish` samples no occupancy, a test asserts the second, and sections 6 to 8
were run again from a clean cache; sections 3 to 5, whose first arm never cancelled, are unchanged
row for row. Every P5, P7 and P8 figure above is from the clean run.

### 9.20 Which clients leave is fixed by submission order

A departure has to be the same decode in every arm or two arms are comparing different clients. The
build numbers each request at submission and carries that number through the router queue, the
engine queue and a cancel's resubmission; a client leaves when a hash of the number falls under the
share, at a point of the decode taken from a second hash of it. A test asserts that the set leaving
is the same under two queue orders.

### 9.21 Departures, cancels and the buffer share one registry of flights

A flight is registered when a cancel, a departure or the stream buffer is on, so a decode that no
cancel could reach is still closed when its decode ends. `--stream-buffer` is identical to off in
sorted tallies at 4x and at 0.75x the grant.

### 9.22 The batch class's draws are the build's own

The batch class is a stream of its own, so the base trace is the published one with the batch
requests removed, which a test asserts. It is not the pre-measurement's draw, and its arms do not
reproduce §1.8's and §1.9's tables to the digit: a p90 of each class's own lengths without a cancel
is 180 / 241 / 290 ms here against 178 / 159 / 428 ms, and with a cancel 63 / 63 / 63 ms, 5581 /
5515 / 5894 ms against 63 / 63 / 63 ms, 5496 / 5674 / 6357 ms. The predictions are graded on their
bands and every cell outside one is said so above. §4.11 asks for a pre-measurement the build does
not reproduce to be reconciled before a prediction resting on it is graded, and the batch arms
were not: the cause of the difference is not isolated, so the batch halves of P5 to P9 stand as
graded on bands against a different draw, and a reconciliation is open.
The ledger run that sizes each partition takes the batch class and none of the enforcement bits
(§9.19), and the grant key includes it.

### 9.23 §1.9's 1 s tail is not reproduced

§1.9's pre-measurement had the router's trigger alone at 66 / 165 / 1316 ms with prefill free and
218 / 3049 / 2985 ms with prefill taking engine time. The build, once it observes each request once
(§9.25), has 63 / 63 / 63 and 75 / 65 / 691 ms. In the first sweep the same arm's tail came from
claims drawn from a sample their own cancels had skewed; whether the pre-measurement's came from
the same mechanism is not isolated (§9.22).

### 9.24 `--engine-wait` stays off

§4.13 left the default to the numbers. §1.1 already chose it off, because the engine that waits
moves the published numbers wherever a partition binds, and the byte-identity gate is what lets
every earlier phase's results stand. None of Phase 9's results asks for more: its arms are all
A/Bs on the corrected engine, which §1.1 names as the baseline for them and for no other result.

### 9.25 A resubmission was observed as a new request

The claims' length histograms and Phase 4's per-class mean were recorded at every dispatch, so a
continuation added its remaining tokens as a fresh observation and a restart added its whole length
a second time. On a test trace with continuation, 1,635 lengths were recorded for 1,241 decodes. A
review found it after the first sweep. Lengths are now recorded at a request's first dispatch only,
a test asserts one observation per decode under continuation and restart, and every section was run
again. Nothing changes with every bit off, and nothing changes in an arm without a cancel. On the
published workload eight rows of sections 5 to 8 move, four distinct runs: the tiered claim at the
half partition and the own-class p90 on seed 3 at 0.6x and below. With the batch class 16 of 44 rows
move, every cancel arm with a quantile or tiered claim. The double counting changed the sample each
claim was drawn from, by the cancels' own choice of victims; which way it moved each quantile and
mean was not measured. What was measured is the engines: at 0.75x and 0.6x with prefill taking
engine time the engine-side trigger, which fires only when an interactive request waits at an
engine, fired 86-830 times a run on the first build and 0-43 on the corrected one, so the first
build's claims let the engines overflow. P6's grade reverses (wrong, where the first sweep had it
holding), P5's batch half narrows to 0.6x, and P7's and P8's batch figures move inside and across
their bands. Every figure above is from the corrected build.

### 9.26 What a review of the build fixed

The same review found and fixed, before the rerun, ten defects that moved no figure above: every
row of sections 2 to 11 was identical before and after. With a queue, a request whose claim no
node's partition could ever hold waited at the head forever and blocked the queue; it is now
refused, as it is without a queue. A request placed on its upstream's node bypassed the queue's
admission check; that node is now used only if it admits the request. Attained service was counted
twice when flights were tracked without a cancel. A fleet reload restarted reservation sequence
numbers, so an old flight could name a new holder. Flag checks were skipped when no other
enforcement flag was set, and `--victim attained` was accepted without `--queue plas`. The abort's
removal of never-written output went unlogged in the KV event log. The engine queue's wait
statistic included time at the router. A cancelled client that was due to leave was counted as
leaving again on resubmission. The oracle planner was built without the batch class. The grant
cache was keyed on a scale the ledger run never reads. Two remain open: a restart re-sends the
original request's hint and so re-registers its upstream, and a request waiting at an engine holds
no reservation until it starts. A third, `--admit quantile | tiered | gate` read silently as `none`
everywhere but `distributed`'s main arms, was fixed after the phase's commit. The six commands that
never apply a claim now refuse one. `distributed` names the claim in its header, and skips its
fan-out admission table when any Phase 9 bit is on, since that table drives fan-outs without them.
Nothing changes with every bit off.

---

## 10. Verification, as run

Increment 1.

- **Byte-identity with every bit off**: `residency` (ledger and `--engine-cache --decode-kv`),
  `flows`, `placement`, `volatility`, `ownership`, `price`, `belief`, `influence` and `fleet` at
  `--ops 3000 --seed 2`, and `distributed --crossing native --engine-cache --decode-kv --admit
  perfect` with its four measured line kinds filtered, all identical to the build before this
  phase.
- **The gate**, eight check lines in `enforce`'s section 1: three orders at 4x the grant with
  prefill free and taking engine time, identical to off with nothing queued; the probe at 0.75x and
  0.5x, identical to off.
- **Tests**, six new, 196 in all: an engine that waits preempts no single request where today's
  does; every request that waits closes once and carries its wait as queue time; a sequence larger
  than the partition runs at once and is preempted; first fit starts sooner than arrival order and
  class order serves interactive work first; the probe reads and changes nothing; and the wait
  changes nothing where every sequence fits.
- **The census**: `cargo build --release --features census` emits 13 warnings.
- `cargo fmt --check` and `cargo clippy --all-targets` clean; the three `assert!(..is_empty())`
  warnings the installed clippy reports in `cache.rs`, `fleet.rs` and `stream.rs` predate this
  phase.

Increment 2.

- **Byte-identity with every bit off**: the same eleven outputs, identical to the build before this
  phase, after the queue, the claims and the program id landed.
- **The gate**, nine more check lines in `enforce`'s section 1: `--queue` in each of its three
  orders with `--admit perfect`, `quantile` and `tiered` at 4x the grant, identical to no queue
  with nothing queued.
- **Tests**, eleven more, 207 in all: a queue holds what the router would refuse and closes every
  request once; a queued request carries its wait as queue time; a queue changes nothing where
  every check passes; class order serves interactive requests first; the queue's key orders by
  arrival, class and attained service; attained service accrues at completion and not before; a
  claim names the bytes of an observed quantile, the pooled quantile, a tiered mean or the declared
  bound; a gate admits below its threshold and queues above it; `Lengths` reports quantiles and a
  mean; an admission by claim counts the bytes it names; every decode names a program and a program
  spans several calls.
- **The census** is 13. `cargo fmt --check` and `cargo clippy --all-targets` are clean, apart from
  the three warnings noted above.

Increment 3.

- **Byte-identity with every bit off**: the same eleven outputs, identical to the build before this
  phase, after the abort and the cancel landed.
- **The gate**, four more check lines in `enforce`'s section 1: `--cancel` with continuation and
  with restart, each with `--admit perfect` and `tiered` at 4x the grant, equal to no cancel in
  sorted tallies with nothing cancelled.
- **Tests**, thirteen more, 220 in all: an abort unpins one sequence and leaves its blocks resident;
  cancelling an in-flight decode removes exactly one matching entry; a cancelled reservation
  returns its exclusive bytes and leaves the shared ones; a deficit is what a claim lacks; an abort
  releases the three ledgers a flight held and only those; only a lower-class sequence is a victim;
  a continuation is the prompt plus the whole blocks decoded; a cancelled request is resubmitted
  and every request closes once, with the protected class waiting less; restarting throws decode
  away and continuing does not; an interactive request waiting at an engine cancels a lower-class
  sequence there; a cancel changes no request where nothing is overcommitted; a gate that evicts
  resubmits what it drops; and finishing a trace samples no occupancy.
- **The census** is 13. `cargo fmt --check` and `cargo clippy --all-targets` are clean, apart from
  the three warnings noted above.
- **Not checked**: that a cancel's freed bytes cover the deficit it was issued for. The router's
  trigger takes victims only when their exclusive bytes cover it, so it cannot be futile; the
  engine's trigger takes victims until the head fits and a count of futile cancels is not kept.

Increment 4.

- **Byte-identity with every bit off**: the eleven outputs of the reproducible set identical to the
  build before this phase, after departures, the batch class, the buffer, the router-only trigger,
  the review's fixes (§9.26) and the length fix (§9.25) landed.
- **The gate**: two more check lines, `--stream-buffer` at 4x and at 0.75x the grant, identical to
  off in sorted tallies, and every earlier check line still identical. `--disconnect`, `--batch` and
  `--stream-buffer` default to off and `--cancel-at` to both.
- **Tests**, eleven more, 231 in all: a departing client has its sequence aborted and leaves every
  tally; a leaked departure runs to its end and still leaves the tally; which clients leave does
  not depend on the arm; the stream buffer counts the tokens emitted so far and changes no request;
  a router-only cancel never fires at an engine; a resubmitted request charges its recompute to the
  cancel; the batch class is a stream of its own and an unshared throughput shape; and, after the
  review, a claim no partition could hold is refused and blocks no queue, registering flights
  without a cancel accrues attained service once, a cleared ledger keeps its numbering so an old
  handle names no new holder, and a resubmitted request is observed once. The last fails without
  its fix, with 1,635 observations for 1,241 decodes.
- **The census** is 13. `cargo fmt --check` and `cargo clippy --all-targets` are clean apart from
  three `assert_is_empty` warnings in lines this phase did not write (`cache.rs`, `fleet.rs`,
  `stream.rs`).
- **The sweep**: `polyphonic enforce`, every section, three seeds, on the final build. Sections 9 to
  11 were also run in reverse order, with every row identical, so no section depends on what ran
  before it. The reviewed build reproduced every row of sections 2 to 11 of the first sweep, and the
  final build differs from it only in the rows §9.25 names.
