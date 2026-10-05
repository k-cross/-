# Phase 10 -- Durability: what each tier writes, and what a crash costs

Implementation plan for Phase 10 of [`owned-and-observed.md`](owned-and-observed.md): count what each
durability tier writes (§1, *Owned is not durable*), and test the failure model §1 states and no run
has exercised -- a scheduler restart, an engine crash, node loss by lease expiry -- by injecting each
into a run whose requests are still open. §9 asks for two deliverables: the soft tier's write rate,
which is what persisting every decision would face, and the cost of losing soft state --
over-admission, service and goodput through the rebuild window -- with the rule that if a restart
costs more than checkpointing would, soft state needs checkpoints in the logged tier and §1 changes.

**Status: built and measured.** Increment 1 is the count, the record's liveness and the
durable-append rung (§4.1 to §4.3); increment 2 is every decode a flight, observations at
completion, and the scheduler-restart fault with its outage, stream fate and client retry (§4.4 to
§4.8); increment 3 is the restarted scheduler's sources and node agents' enforcement (§4.9, §4.10);
increment 4 is the engine crash and node loss (§4.11, §4.12); increment 5 is the crossover and the
durable sandboxes under node loss (§4.14). They are in `writes.rs`, `durable.rs`, `fault.rs`,
`machine.rs` and `programs.rs`, behind `--count-writes`, `--track-flights`, `--observe`,
`--node-check`, `--snapshot-estimators` and `--copy-durable` and driven by `polyphonic durability`,
whose sections are `gate`, `count`, `logged`, `restart`, `routing`, `estimators`, `engine`, `node`,
`crossover`, `durable`, `rung` and `fleet`; §9 records what the builds found, including a review
that corrected the fault driver's clock and re-ran what it moves (§9.27), and §10 how they were
verified. The FoundationDB commit rung and a command-line flag for faults in `programs` (the hook is
`programs::Config::fault`) are not built. §2's predictions are stated before the run, per
`owned-and-observed.md` §7, and like Phases 4 to 7 and 9 they lean on **pre-measurements**: numbers
taken on an instrumented copy of `aeeb943`, run outside the repository and not committed, with every
hook off reproducing `enforce`, `belief`, `residency` and `fleet` byte for byte. One reads the
existing model through counters this phase builds (the tiers' writes); five emulate what it builds
(a scheduler restart whose streams die or are held, the ledger rebuilt or not where memory binds, an
estimator reset whose first observations arrive late, an engine crash, node loss); one measures the
host (a durable append); and three are arithmetic on published figures (liveness's write rate, the
count at fleet scale, and the crossover against the sidecar's tax). Each is labelled with its grade
(`owned-and-observed.md`, *On the numbers*), and §8 says how each was taken. They are reasons to
predict, not results: §4.13 rebuilds each instrument in the repository, and a pre-measurement the
build does not reproduce is reconciled before a prediction resting on it is graded.

Four things make this phase unlike the others.

- **Its subject is a transient, so its unit is an integral.** A fault's cost is the request-seconds
  of service it adds to the same trace with no fault, beside the requests it leaves unserved and a
  window of arrivals after it. A mean over a 60-second run dilutes a transient into noise, and
  every earlier phase graded means.
- **The design names the wrong costs.** §9 calls a restart "a belief reset plus a rebuild window",
  and §1 has it rebuild from node agents and engines. On the pre-measurements the belief, the
  estimators and the reservation ledger together cost nothing a run can see wherever memory does not
  bind hardest; the outage and the streams in flight cost the rest (§1.5 to §1.7).
- **Its comparisons have shipped.** Kubernetes elects its scheduler's leader on a 15 s lease and
  renews each node's lease every 10 s; llm-d runs its endpoint picker active-passive and lets Envoy
  fail open while the leader changes; vLLM replays 10,000 steps of KV events, and llm-d's subscriber
  cannot index a pod whose ring has rolled; OpenAI's background mode and llm-d-async keep a stream's
  progress so it can resume (§1.1, §1.5, §1.6, §1.8).
- **Its deciding constant has no source.** A crash rate is a property of code nobody has written, so
  the deliverable is a crossover -- the crash interval below which a choice pays -- which is §2.3's
  method applied to failure (§1.11).

§1 settles twelve decisions. Eight are findings about the existing model and documents rather than
about the work ahead: §1.1 (liveness, not the planner, writes the record, and §1's telemetry budget
holds only for an eviction-centric format), §1.3 (a crash reaches only a request the machine still
holds, and fan-out agents are not held), §1.5 (the streams are what a restart loses, and §2.6 puts
them in the scheduler), §1.6 (the outage is the cost, and a lease sets the outage), §1.7 (losing the
scheduler's routing state costs nothing measurable), §1.8 (replay cannot rebuild a cold subscriber),
§1.9 (an engine crash costs its restart, not its KV) and §1.10 (node loss fails gangs first and
takes Phase 7's durable sandboxes with it).

---

## 1. What has to be settled before a crash can be injected

### 1.1 The tiers' rates, counted: three to five owned changes a request, fifty to seventy events, and liveness in the record

`owned-and-observed.md` §1 asserts that the tiers' rates sit orders of magnitude apart and leaves the
count to this phase. Counting every change to owned state by its owner, and every KV event the
scheduler would ingest, on the `belief` cluster at 250 req/s (§8, *count*; seeds 1 / 2 / 3; per
simulated second, and per offered request in brackets):

| | engine allocating, nothing enforced | Phase 9's integrated arm, 0.75x of the grant | the same at 1.0x |
|---|---|---|---|
| decisions | 306-308 (1.23) | 293-296 (1.21-1.22) | 298-299 (1.23) |
| dispatches | 327-329 (1.31) | 311-315 (1.28-1.30) | 318-319 (1.31) |
| reservations committed / released | 0 | 126-129 / 124-127 (0.52) | 127-129 / 127-129 (0.53) |
| flights opened / closed | 0 | 102-103 / 99-101 (0.42) | 99-101 / 99-101 (0.41) |
| router queue enqueued / served, cancels | 0 | 4-7 / 6-11, 2.0-3.7 | 0 |
| flow-graph writes | 107-110 (0.43) | 103-106 (0.43) | 104-106 (0.43) |
| fan-outs staged, host-class admissions | 7.0-7.5, 6.8 | 6.8-7.3, 6.6 | 6.8-7.3, 6.6 |
| **owned changes** | **about 760 (3.0)** | **about 1,200 (4.9)** | **about 1,200 (4.9)** |
| KV events, every type and tier (inferred) | 12,800-13,200 (51-53) | 16,900-17,400 (69-72) | 15,700-16,000 (65-66) |
| of them, removals from GPU and host offload | 4,560-4,730 | 6,110-6,340 | 5,620-5,790 |
| length observations (inferred) | 131-133 (0.52-0.53) | 124-127 (0.51-0.52) | 127-129 (0.52-0.53) |

Decisions are 1.23 a request, Phase 8's `d`. Owned changes are about 3 a request with nothing
enforced and 5 with Phase 9's enforcement, which adds a reservation and a flight, each written twice:
190-310 a node a second. The KV event stream the scheduler would ingest under `--belief` is 51-72
events a request, 3,200-4,400 a second an engine, thirteen to seventeen times the owned rate; it is
inferred, rebuilt rather than stored, and never a candidate for persistence. Of it, the removals from
GPU and host offload -- what §1's eviction-centric format sends, since the router tracks its own
allocations -- are 1,140-1,580 a second an engine: 9-13 KB/s of 8-byte hashes, 10-16 KB/s with a
32-byte header at 40-100 Hz, at the edge of §1's "under 15 KB/s per engine". The full stream would be
26-35 KB/s.

The logged tier is Phase 7's count: an intent and an outcome per `SideEffecting` call, 20-66% of the
soft tier's decisions on the agent presets and nothing on the others.

The record tier has two writers, and §1's table names one clock for both. The planner moves
placements about once in 20 s (0.046 writes a second on eight nodes, Phase 6). Liveness renews on a
clock of its own: Kubernetes renews each node's `Lease` every 10 s, a quarter of its 40 s duration,
so every node writes 0.1 a second -- the design KEP-589 adopted after `NodeStatus` heartbeats of
15-20 kB every 10 s were costing etcd about 150 MB a minute at 5,000 nodes. At that renewal eight
nodes write 0.8 a second, seventeen times the planner, and the record's rate scales with the
fleet's size rather than its traffic. §1's table says the record changes "per provisioning change";
a lease renewal is not one.

**Chosen: the count by tier and owner** -- the scheduler's owned changes, the node agents', the
engine's inferred stream by event type and tier, the logged tier by cause, and the record's planner
and liveness -- per simulated second, per node and per request, so it multiplies to a fleet.
Liveness is counted at a swept renewal and never stored, since the simulator models no store.

### 1.2 Write-through is ruled out by the FaaS denominator, and its floor is measurable

A checkpoint either waits for its commit or does not. One that waits puts a commit on the request
path once per decision at least, 1.23 a request, and a commit is a durable append plus a quorum's
round trip. The append is the one term the host can measure (§8, *rung*; an M2 Max's internal SSD,
128-byte appends to one file, the lowest median and p99 of five runs):

| | median | p99 |
|---|---|---|
| append | 1.8 us | 4.4 us |
| append and `fsync` | 21 us | 35 us |
| append and `F_FULLFSYNC` | 3.98 ms | 4.14 ms |

On macOS `fsync` does not flush the drive's cache and `F_FULLFSYNC` does, so the host's durable
append is the third row, and it is a consumer drive's figure. Published fsync latencies are 1.6-12.4
us on datacenter drives with power-loss protection, which acknowledge from a protected cache, and
0.9-3.0 ms on consumer drives without it (Mark Callaghan, *Small Datum*, 2026-01-07). A quorum adds
a round trip: 60 us within a rack and 800 us across a zone at the model's links. FoundationDB
publishes 1.5-2.5 ms a commit below 75% load. So a commit per decision costs a request 70-90 us (a
rack, a protected drive) to 1.8-3.1 ms (FoundationDB's figure): 0.6-24 times a warm `FaaS`
invocation's ~129 us, which is a chosen constant, and under 0.35% of a one-second agent turn. A
checkpoint that does not wait is write-behind, which leaves process memory the authority -- the soft
tier by another name, as §1 says.

Throughput is not the objection, as §1 also says. 190-310 owned changes a node a second is 1.9-3.1
million a second at 10,000 nodes, 0.09-0.15 cores a node at FoundationDB's published cluster rate
(8.2 million operations a second at 10% writes on 384 cores, about 2,100 writes a core).

**Chosen: a durable-append rung on the ladder** -- append, flush, full flush -- published as an
ordering beside the others. The FoundationDB commit rung §9 makes optional stays optional, behind
its own feature: the host has no `fdbserver`, and a one-process cluster would measure the flush and
not the quorum. Write-through is priced in arithmetic and never run.

### 1.3 A crash reaches only a request the machine still holds

A request's cost is fixed when it is dispatched unless it registers a flight (`phase-9.md` §9.13),
so a fault injected into today's machine changes no request already dispatched. Phase 9's flights
are the precondition, with three gaps.

- **Fan-out agents are not flights.** They take today's path (`phase-9.md` §7) and are 22% of
  decodes, so every pre-measured fault left them running: every fault figure below under-counts the
  streams it reaches by up to a fifth.
- **Flights and the belief have never run together.** Phase 9 refuses `--cancel`, `--disconnect` and
  `--stream-buffer` with `--belief`, and a restart is a belief reset. An aborted flight keeps the
  belief's pin until its decode would have ended (`observe_pin`), which a fault has to release.
- **Lengths are observed at dispatch** (`phase-9.md` §9.12), so an estimator a restart loses is
  re-learned in one dispatch. Pre-measured, resetting every estimator mid-run changes no request at
  0.75x or 1.0x of the grant on any seed (§8, *relearn*). A deployment observes a length when its
  decode ends, about a second later; emulated as a blind window, §1.7 says what that costs.

**Chosen: every decode a flight, a fan-out's agents as one gang's flights; flights with the belief;
observations at completion behind a bit.** The first two change no number with every fault off,
which the gate checks. The third moves the claims' and the observed mean's inputs by one decode's
lag, a model correction with an A/B of its own.

### 1.4 Where each piece of soft state is rebuilt from

§1 says a restart "rebuilds residency and load from node agents and engines, which hold the facts".
`Machine` holds the scheduler's state and the node agents' together, so the phase has to say which
fields a scheduler restart loses and where each comes back from:

| state | `Machine` | category | held by | rebuilt from | with no source |
|---|---|---|---|---|---|
| engine KV residency | `observer`, or the exact read | inferred | scheduler | the engine's replay (§1.8) and the scheduler's own dispatches; a snapshot if the engine publishes one | re-learned from dispatches (§1.7) |
| decodes in flight, their claims | `flights`, `reserved` | owned | scheduler, and each node agent for its node | node agents | admission blind to them until they end (§1.7) |
| sequences waiting for blocks | `engine_queues` | owned | engine and node agent | -- they survive with held streams | die with their connections with shared ones |
| requests waiting at the router | `router_queue` | owned | scheduler | nothing: the clients retry | each waits out the outage |
| flow graph, prefill landings | `upstream`, `landing`, `prefill_ready` | owned | scheduler | nothing | a downstream placed without its upstream's node |
| gang staging | `staged`, `staged_bytes`, `cancelled` | owned | scheduler | nothing | a gang being staged is refused and retried |
| tenant meters | `buckets`, `tenant_flight` | owned | scheduler | in-flight counts from node agents; buckets from nothing | every tenant gets a full burst allowance |
| output lengths | `lengths`, `root_lengths`, `observed` | inferred | scheduler | nothing, or a snapshot | claims fall back to `max_tokens` (§1.7) |
| flow templates; Phase 7's estimators | `learner`; tool transitions, idle-gap survival, class history | inferred | scheduler | nothing, or a snapshot | hints, speculation and suspension blind until re-learned |
| attained service | `attained`, `completions` | inferred | scheduler | nothing | the PLAS order restarts from zero |
| planner accrual, demand by model | `planner`, `fleet_view` | inferred | scheduler | nothing | the next move late by up to one accrual |
| model placement, partitions | `fleet` | owned, record | the record | the record | -- |
| host-class residency, leases, durable marks, tool slots | `domains`, `slot_free` | owned | node agent | node agents; gone with the node | durable cells lost with their node (§1.10) |
| liveness | -- | owned, record | the record | the record | **missing** in the simulator |
| side-effect intents, suspended sessions | `programs::LogCause` | owned, logged | the logged tier | the logged tier | counted, not stored |

The rows with no source are what a restart loses, and every one is a learned estimate, re-learned
from traffic; a meter, which refills; or a request's own state -- its place in the router's queue,
its upstream's node, its gang's staging -- which dies with its connection anyway when the stream
does (§1.5).

**Chosen: a rebuild source per row, and an arm without it** -- node agents' report at once, after a
delay, or never; the engine's replay, a snapshot, or neither; estimators lost or restored -- so each
row's share of a restart's cost is measured rather than assumed.

### 1.5 The streams are what a restart cannot rebuild, and §2.6 puts them in the scheduler

`owned-and-observed.md` §2.6 puts the data path in the scheduler's address space, which makes
per-request state "fate-shared with the connections it describes". A scheduler crash then ends every
stream through it: the node agent sees its upstream close and aborts the engine's sequence (Phase 9's
abort, vLLM's `finish_requests`), and the client retries. The alternative keeps the stream below the
scheduler: the node agent holds the engine's stream and its buffer, and the client re-attaches through
whichever scheduler answers. Pre-measured on Phase 9's integrated arm (§8, *restart*; the crash at 40%
of the trace, 250 req/s, the router down for the outage and every arrival during it retried at its
end; request-seconds of service against the same trace with no fault, seeds 1 / 2 / 3):

| | 0.75x of the grant | 1.0x |
|---|---|---|
| streams held, 0.1 s outage | -11 to +8 (noise) | 0.5 / 1.2 / 0.8 |
| streams held, 1 s | 126 / 112 / 143 | 128 / 127 / 126 |
| streams die, 0.1 s, client restarts | 67 / 74 / 65 | 64 / 68 / 57 |
| streams die, 1 s, client restarts | 368 / 332 / 333 | 300 / 283 / 283 |
| streams die, 1 s, client continues | 256 / 220 / 234 | 227 / 225 / 219 |
| streams die, 1 s, client restarts, belief and estimators kept | 333 / 298 / 324 | 282 / 283 / 270 |

A crash reaches 82-88 streams, the decodes in flight at 250 req/s, and a restart throws away 49-57
decode-seconds of them. With a 0.1 s outage that is the whole cost; with a 1 s outage a crash that
ends the streams costs 2.2-3.0 times one that holds them. A client that kept what it received and
sends a continuation recovers 60-110 request-seconds of it -- Phase 9's continuation, made by the
client because the scheduler that relayed the tokens is gone.

Holding the stream below the scheduler is cheap. Phase 9 measured the buffer a stalled stream needs at
under 1.8 MB a node, and the published forms of a resumable stream exist: OpenAI's background mode
lets a client resume `starting_after` a sequence number and keeps a background response about 10
minutes; llm-d-async proposes checkpointing a stream's tokens to Redis so that a worker's crash
continues the request rather than restarting it (llm-d/llm-d-async#467, 2026-09-28).

**Chosen: the stream's fate as a bit** -- `shared`, §2.6 as written, and `held`, the node agent
keeping the engine's stream and its buffer for a client that re-attaches -- with the client's retry a
restart or a continuation of what it received.

### 1.6 The outage is the cost, and a lease sets the outage

The rows above already say where a restart's cost lies. Held streams lose nothing but the outage,
and the outage costs the arrivals it delays, `λD²/2`, plus the burst of their retries at its end
(§8, *restart*; 250 req/s; the window is 30 s of arrivals from the crash):

| outage | streams held | streams die, restart | interactive first-token p99 in the window |
|---|---|---|---|
| none | 0 | 0 | 56-60 ms at 0.75x, 19-21 ms at 1.0x |
| 0.1 s (`λD²/2` = 1.25) | 0.5-1.2 at 1.0x; noise (-11 to +8) at 0.75x | 57-74 | 19-64 ms |
| 1 s (`λD²/2` = 125) | 112-143 | 283-368 | 0.68-0.86 s |
| 15 s (`λD²/2` = 28,125) | 32,450-39,050; 238-377 unserved | 34,680-43,440; 260-375 unserved | 14.7-14.9 s |

At 1 s the burst costs almost nothing and the total is `λD²/2` to within 2% at 1.0x. At 15 s the
burst of 3,750 retries adds a sixth at 1.0x and over a third at 0.75x, and 240-380 requests are left
unserved.

What sets the outage is how a successor takes over. Kubernetes elects its scheduler's leader on a
15 s lease (`--leader-elect-lease-duration`, renewed within 10 s, retried every 2 s), which costs
nothing a request sees because its scheduler is off the request path. llm-d runs its endpoint picker
active-passive with warm standbys, and Envoy fails open while the leader changes
(`router.proxy.failOpen`), so requests reach model servers without a picker; its operations guide
reports 98.1-99.3% of requests succeeding through a leader's termination. A scheduler on the request
path has no proxy to fail open to. A takeover that waits for a 15 s lease costs 32,000-44,000
request-seconds a crash here.

A takeover need not wait for a lease if no two schedulers can hand out one node's capacity. §1 gives
that job to the lease ("two schedulers must never hand out one node's capacity"), and §2.4 gives node
agents backpressure. If each node agent enforces its own partition against its own ledger, refusing
or queuing a dispatch its claims do not admit, a standby can take over the moment the primary's
connections break, and two schedulers briefly alive at once over-admit nothing; they only route
worse.

**Chosen: takeover as a condition** -- by lease, the outage the lease's duration (5 and 15 s), or by
connection, the outage a detection plus a standby's start and rebuild (0.1 and 1 s), with node agents
enforcing their partitions locally. Retries arrive as one burst at the outage's end, and spread by a
client's backoff as the comparison.

### 1.7 Losing the scheduler's routing state costs nothing measurable, and its estimators only where memory binds hardest

With the outage and the streams accounted for, what remains is the state §9 expected to dominate.
Pre-measured (§8, *restart*, *overadmit*, *relearn*):

- **The belief.** A restarted scheduler that believes only what it dispatches after the restart makes
  3,000-5,600 reads of a resident block it does not know, and its service is within 10
  request-seconds of no fault at 0.75x (10.0 / 0.6 / 3.9) and within 1 at 1.0x: noise. The router
  re-learns the resident set from its own dispatches within a second or two, and Phase 4's
  structural reason holds -- the terms a stale view would have to fool read the in-flight count,
  which the router carries.
- **The reservation ledger.** Never rebuilt, with streams held, it admits nothing the full ledger
  would refuse at 0.75x or 1.0x. At 0.6x and 0.5x, 0-32% of the 129-196 admissions made blind
  exceed the partition; the engine's wait absorbs them (up to 109 sequences wait at an engine,
  against none without the fault), and on one seed in three the interactive first-token p99 in the
  window reaches 0.28-0.39 s against 0.06-0.07 s on the same seed with no fault. Rebuilt from node
  agents at once, nothing is over-admitted. The bound §1 expected node agents to set is the drain of
  the decodes in flight at the crash, about one decode.
- **The estimators.** Lost with observations at dispatch: nothing (§1.3). Lost with the first
  observation a decode late, so that every claim falls back to `max_tokens` -- four times the longest
  output -- for the blind window (request-seconds against no fault; requests unserved beyond it):

  | blind window | 1.0x | 0.75x, prefill free | 0.75x, prefill takes engine time | 0.6x, prefill takes engine time |
  |---|---|---|---|---|
  | 1 s | 0.0 | -13 to +15 | +1 to +23 | -135 to +83; first-token p99 67-149 ms |
  | 2 s | 0.0 | -8 to +5 | -10 to +30 | +11 to +121; 69-239 ms |
  | 5 s | 0.0 | -38 to -3 | -17 to +53; 13-40 more unserved | +8 to +464; 107-431 ms; 51-63 more unserved |

  The 0.6x column is an overload in which resetting the estimators with no blind window at all moves
  a run by -304 to +281 request-seconds, so only its first-token tail and its unserved count carry a
  signal: against 64 ms with no fault on every seed, the tail is 1.0-2.3x at a 1 s window, 1.1-3.7x
  at 2 s and 1.7-6.7x at 5 s.
- **In the retry burst**, the belief and the estimators together are worth 0-35 request-seconds of a
  1 s restart whose streams die, out of 283-368: the one moment the belief matters at all is when
  every request arrives at once.

So soft state is soft, measured: where memory does not bind hardest, nothing the scheduler holds
needs a checkpoint. The one candidate is the estimators where it does, and their checkpoint is small
-- per class key a 2,048-bin histogram of output lengths, tens of kilobytes in all -- and slow,
written on the provisioning clock rather than per decision.

**Chosen: the estimators' snapshot as the only checkpoint arm**, written every 10 s to the logged
tier and counted there; the belief rebuilt from the scheduler's own dispatches and whatever the
engine's replay holds; reservations and in-flight load from node agents.

### 1.8 Replay cannot rebuild a cold subscriber

§1's telemetry has the engine push "a prefix-tree snapshot or block bloom filter" every 1-2 seconds
or on a gap, and Phase 4's `Periodic` recovery models it. vLLM publishes no such snapshot. It buffers
the last `buffer_steps` batches for replay, 10,000 by default and the simulator's `BUFFER_STEPS`,
which is 100-250 s at 40-100 Hz, so a block stored before that and still resident is never replayed.
A subscriber that starts cold asks from sequence zero, and llm-d's router has an open issue in
exactly that shape: a replay-enabled subscriber that connects after a pod's ring has rolled past
sequence zero has its replay rejected, then drops live events, and never indexes the pod
(llm-d/llm-d-router#3124, 2026-10-01, on vLLM v0.28.0 with EPP v0.11.0).

On §1.7's numbers the snapshot is not needed: dispatches rebuild the belief faster than it matters.
What a cold subscriber needs is to accept the replay it can get rather than insist on sequence zero,
which is the issue's proposed fix.

**Chosen: a restarted subscriber gets the buffer's last 10,000 steps and what it dispatches**, and
anchors on the first batch it receives. The snapshot is an arm, and a promotion-tier-2 ask only if
the arm is worth something.

### 1.9 An engine crash costs its replica's restart, not its KV or its streams

An engine crash ends its sequences, drops its KV, and leaves its node unable to decode until the
engine is back; the node agent and the node's host work run on. Pre-measured on node 0 of four (§8,
*engine*; the crash at 40% of the trace, the node's flights continued elsewhere and its `NVMe` spill
kept unless stated; request-seconds against no fault, then requests unserved beyond it):

| the engine back after | 0.75x of the grant | 1.0x |
|---|---|---|
| 2 s | 11 / -9 / 17 (noise) | 9 / 7 / 8 |
| 8 s | 42 / 21 / 44 | 37 / 38 / 38 |
| 30 s | 187 / 127 / 95; 99-143 unserved | 139 / 137 / 119; 20-31 unserved |
| 30 s, flights restarted, not continued | 218 / 159 / 234 | 146 / 155 / 133 |
| 30 s, the spill lost too | 186 / 119 / 91 | 139 / 137 / 119 |
| never, with 36 s of the run left | 465 / 451 / 357; 140-196 unserved | 131 / 142 / 121; 57-61 unserved |

The cost is the restart, and at 1.0x it is linear in it: about 4.5 request-seconds for every second
one engine of four is down at 250 req/s. 18-24 streams on the node are lost and continued elsewhere,
and they cost under 10 request-seconds in all, the 2 s row; continuing them rather than restarting
them is worth 7-18 request-seconds at 1.0x and 31-139 at 0.75x, where every seed prefers it. The
node's 3,100-3,600 KV blocks are lost, or 8,000-11,200 with the spill, at no measurable cost: KV is
worth nothing once its node is out for seconds. vLLM's start is tens of seconds to minutes, CUDA
graph capture dominating, and Foundry reports Qwen3-235B-A22B's initialisation going from 10
minutes to 3.9 s (arXiv 2604.06664).

**Chosen: an engine's restart as Phase 6's start time plus the copy from its own agent's cache**,
swept at 2, 8, 30 and 120 s; its GPU and host-offload KV lost, its `NVMe` spill kept or lost; its
flights continued by the router, which relayed their tokens; the belief told by a `Cleared` event or
by the engine's sequence numbers restarting.

### 1.10 Node loss: suspicion routes, a lease re-places, and gangs fail first

Losing a node is an engine crash that never returns, plus what the node agent held. Pre-measured,
node 0 lost at 40% of the trace and never replaced (§8, *node*):

| | 0.75x of the grant | 1.0x |
|---|---|---|
| the node lost, flights continued | 629 / 403 / 661 | 191 / 187 / 188 |
| the node lost, flights restarted | 514 / 552 / 372 | 214 / 204 / 190 |
| an engine that never returns, for comparison | 465 / 451 / 357 | 131 / 142 / 121 |
| unserved | 130-193 | 46-54 |
| of them, fan-outs refused (with no fault) | 107-138 (28-44) | 23-27 (0) |

The node costs 45-70 request-seconds more than its engine at 1.0x -- its 96-115 warm cells and service
heaps rebuilt elsewhere, a heap's cold start being 15 s (a chosen constant), and its host work run on
three nodes -- and the difference is noise at 0.75x. With 36 s of the run left the rest is capacity.
What fails first is all-or-nothing admission: a fan-out that fit on four nodes does not stage on
three, and refused fan-outs are most of what is left unserved -- §5's property 4, where it is
fragile.

Two clocks follow. The router suspects a node at the first dispatch that fails, since it carries
every dispatch, and need not wait for anything. The lease decides when the node's capacity may be
given away: its replicas re-placed by the planner, its share of a region's budget returned (Phase 11).
Kubernetes' version of the second clock takes minutes: a node is unhealthy after a 40 s grace (50 s
from v1.32) and its pods are evicted 300 s later under the default tolerations.

The node's `NVMe` takes Phase 7's durable sandboxes with it. The invariant -- a durable cell is
demoted and never dropped -- holds against every fault but this one, and to hold against it a sandbox
needs a copy off the node when it is suspended. At Phase 7's long-running rate that is 0.07 suspends
a second on four nodes at 32 MiB each, about 0.6 MiB/s a node (arithmetic).

**Chosen: suspicion at the first failed dispatch or after a silence of a few steps (Phase 4's
episodes), death at the lease's expiry, swept from 10 to 40 s;** under `--fleet` the planner re-places
the node's replicas after death; durable cells on the node counted lost, with a copy at suspension as
the arm that keeps them.

### 1.11 The crossover: fate-sharing against the sidecar's tax

No crash rate has a source here, so the comparison is a crossover, §2.3's method: how often may the
scheduler crash before what its faults cost exceeds what integration saves? Phase 8 measured the
saving at 44-75 us a request, which at 250 req/s is 11-19 ms of request time each second. Dividing
each pre-measured cost by it (arithmetic on §1.5, §1.6 and §1.9):

| a crash, at 250 req/s | request-seconds | as much as the sidecar's tax over |
|---|---|---|
| streams held, 0.1 s takeover | 0.5-1.2 | under 2 minutes |
| streams die, 0.1 s takeover, restart | 57-74 | 0.85-1.9 hours |
| streams held, 1 s takeover | 112-143 | 1.7-3.6 hours |
| streams die, 1 s takeover, continuation | 219-256 | 3.2-6.5 hours |
| streams die, 1 s takeover, restart | 283-368 | 4.2-9.3 hours |
| a 15 s lease, streams held or not | 32,450-43,440 | 3-7 weeks |
| for comparison, one engine of four down for 30 s | 119-187 | 1.8-4.7 hours |

The sidecar's own failure is not free either. While llm-d's picker changes leader, Envoy routes by its
own balancer, and Phase 6 priced scored routing against hashing at 16-32% of service at this load per
node (`scored + fetch` against `hash only` at 500 req/s on eight nodes). A 15 s failover routed that
way costs 340-850 request-seconds by that arithmetic: one to three 1 s restarts whose streams die.

**Chosen: the crossover per arm**, against the sidecar's per-request tax and against its fail-open
window, the latter run as the router placing by `hash only` for the window with every stream intact.

### 1.12 Regimes, workloads, and how each effect is graded

- **The cluster** is the `belief` cluster: 4 nodes, 16 GiB HBM, 32 GiB DDR and 64 GiB `NVMe` in
  total, 250 req/s, 10% fan-out, 15,000 requests, rack, no control crossing, `scored + fetch`, the
  engine allocating with decode output held. **The arm** is Phase 9's integrated one: each class's
  own p90 claim (`--admit quantile`), the class-ordered queue, the engine waiting by class, and the
  cancel with continuation, with `--throughput 0.3`.
- **The partition** at 1.0, 0.75, 0.6 and 0.5 of the published grant, prefill free and taking engine
  time; the faults are graded at 1.0 and 0.75, and over-admission and the estimators at all four.
- **The fault** at 40% of the trace, request 6,000, about 24 s in; a second instant at 20% to check
  that no result is an artefact of the instant.
- **Programs** (Phase 7) for the logged tier's count and for durable sandboxes: the agentic,
  multi-agent and long-running presets.
- **Grading.** Request-seconds of service against the same trace and seed with no fault, over the
  whole run; requests unserved, apart and by cause; a 30 s window of arrivals from the fault for
  tails -- the interactive first-token p99 and each class's service p99; and counts: streams aborted,
  decode thrown away, admissions made blind and over-admissions, engine waits, reads of resident
  blocks the scheduler does not know, KV blocks lost, durable cells lost. Three seeds a cell, five at
  0.6x and 0.5x, where any perturbation moves a run by hundreds of request-seconds.

---

## 2. Predictions, stated first

`owned-and-observed.md` §7's rule. Ten predictions, each attached to a claim it would rewrite. Where
a pre-measurement stands behind one, §8 says how it was taken; P2, P9 and P10 rest on measurement of
the host and arithmetic, and P8's clauses on the lease and on copying durable cells on arithmetic
alone, and say so. Any of them may be pre-measured again before the build; one whose pre-measurement
moves its band is restated before the run, with the original kept beside it.

**P1 -- The soft tier writes three to five owned changes a request and reads fifty to seventy-five
events; the record tier is three orders below it, and liveness is most of the record.**

On the `belief` cluster at 250 req/s, owned changes are 2.8-3.4 a request with nothing enforced and
4.6-5.4 under Phase 9's integrated arm -- 700-1,400 a second, 175-350 a node -- with decisions at
1.20-1.26 a request. The KV event stream is 48-75 events a request, of which removals from GPU and
host offload are 1,000-1,700 a second an engine, 9-14 KB/s of hashes. The record tier writes 0.4-0.9
a second on four to eight nodes at a 10 s renewal, liveness over 90% of it. The logged tier
reproduces Phase 7's 20-66% of decisions on the agent presets and nothing elsewhere.

- *If right:* §1's durability table gets measured rates, per node and per request; "orders of
  magnitude apart" holds between the soft and record tiers and fails for the logged one, as Phase 7
  found; the record is sized by the fleet's node count, not its traffic; and §1's telemetry budget
  holds for an eviction-centric format and not for the full stream.
- *If wrong* (owned changes above 8 a request, or the record within two orders of the soft tier):
  something writes owned state per request that the counters missed, or liveness renews faster than
  Kubernetes does, and §8's record is sized from that writer.

**Measured: P1 holds at the two partitions it named, and is just above its band at the half.** Owned
changes are 3.08-3.10 a request with nothing enforced and 4.95-5.02 under Phase 9's integrated arm
at 0.75x and 1.0x of the grant (5.16-5.22 at 0.6x and 5.39-5.53 at 0.5x, the top of the band
exceeded in the overload), 770-776 and 1,200-1,218 a second, 193-194 and 300-305 a node; decisions
1.21-1.23 a request. The KV event stream is 51-53 events a request with nothing enforced, 69-72 at
0.75x, 65-66 at 1.0x, 73-74 at 0.6x and 75-77 at 0.5x, of which removals from GPU and host offload
are 1,140-1,590 a second an engine at the first three partitions and 9.1-13.5 KB/s of hashes. The
record tier is 0.40 a second on four nodes and 0.80 (plus the planner's 0.046) on eight at a 10 s
renewal, 94% of it liveness, three orders below the owned changes at four nodes (1,400-3,000 times).
The logged tier is Phase 7's to the digit: 40, 20, 44 and 66% of decisions on the pipeline,
agentic, multi-agent and long-running presets.

**P2 -- A commit per decision costs a warm `FaaS` invocation at least half its own service.**

*A measurement and arithmetic; no simulation.* The built rung reproduces the host's ordering --
append under 5 us, `fsync` 10-50 us, a full flush 2-6 ms -- and a commit per decision costs a request
70-90 us within a rack on a protected drive, about 1 ms across a zone, and 1.8-3.1 ms at
FoundationDB's published commit: 0.5-25 times a warm `FaaS` invocation, under 0.35% of an agent turn.

- *If right:* write-through is ruled out by the `FaaS` denominator with its floor measured, and a
  checkpoint can only be written behind, which leaves the state soft; §1's soft row stands on latency
  before any restart is priced.
- *If wrong* (a full flush under 100 us on the host): the host's drive acknowledges from a protected
  cache and behaves like a datacenter's, and the argument rests on the quorum's round trip alone, 60
  us within a rack -- still half a warm invocation.

**Measured: right, and the host's own flush is above the band.** The rung is 1.6 us for an append
(p99 4.6), 17.9 us with `fsync` (62.1) and 3.995 ms with a full flush (4.91), the lowest of five
runs. A commit per decision is 72.4 us on a protected drive in a rack (0.60 times a warm `FaaS`
invocation of 120 us, the chosen constants' mean), 812 us across a zone (6.8 times), 1.5-2.5 ms at
FoundationDB's published commit (12.5-20.8 times), and 4.07-4.81 ms with this host's full flush
(34-40 times, above the band's 25), 0.007-0.48% of a one-second agent turn.

**P3 -- Losing the scheduler's routing state costs nothing a run can see where memory does not bind
hardest.**

At 0.75x and 1.0x of the grant, a restart with no outage that loses the belief, the reservation
ledger and the estimators (observed at dispatch) costs within 15 request-seconds of no fault on every
seed, with 2,000-7,000 reads of resident blocks the scheduler does not know and no over-admission. At
0.6x and 0.5x a ledger never rebuilt over-admits 0-40% of what it admits while blind, the engine's
wait absorbs it, and the interactive first-token p99 in the window rises on at most one seed in
three; rebuilt from node agents at once, it over-admits nothing.

- *If right:* soft state is soft, measured, and §1's "restart rebuilds residency and load from node
  agents and engines" is restated: residency is re-learned from the scheduler's own dispatches, and
  node agents supply reservations and in-flight load, which matter only where memory binds hardest.
- *If wrong* (losing the belief costs more than 1% of the run's service at 1.0x): routing depends on
  pre-crash residency more than Phase 4 found, and the engine's snapshot (§1.8) is worth asking for.

**Measured (increment 2, the belief and the estimators): the clause holds where it was built.** A
restart with no outage that loses the belief -- the scheduler reads only what it has dispatched
since -- and the flow graph costs 10.0 / 0.6 / 3.9 request-seconds at 0.75x of the grant and 0.6 /
-0.3 / -0.5 at 1.0x (within 15 and within 1), with 3,700-5,600 reads of a resident block it does not
know across the two (the band's 2,000-7,000), and a snapshot after 1 s makes 3,000-3,800 and costs
8.1 / 8.8 / -2.1 and -0.1 / -0.1 / -0.2. Losing the estimators alone costs 0.0 on every
seed, observed at dispatch. The reservation ledger and the over-admission clause are not built.

**Measured (increment 3, the ledger): right where it was predicted, and the engine's wait absorbs
less than the tail suggested.** Rebuilt from node agents at once, a held restart admits nothing
blind at any partition. Never rebuilt, it admits 129-203 requests blind at every partition and
over-admits none at 1.0x and 0.75x. At 0.6x with a 1 s outage 11 / 27 / 7 / 25 / 4 of 154 / 159 /
139 / 169 / 150 are over-admissions (3-17%), 63 / 45 / 30 / 56 / 27 of 195 / 196 / 150 / 194 / 172
with prefill taking engine time (16-32%), and 15 / 17 / 29 / 30 / 31 of 129-149 at 0.5x (11-22%);
the 0.1 s outage gives 0-14, 8-42 and 4-46. The engine's wait absorbs them: 32 / 14 / 1 / 41 / 0
sequences wait at an engine, 8-109 with prefill and 42-103 at 0.5x, against none rebuilt. The
interactive first-token p99 in the window rises on one or two of five seeds at a 1 s outage by
50-190 ms over a tail the outage sets at 0.7-0.8 s, and at a 0.1 s outage on three of five seeds at
0.5x (0.17-0.28 s against 0.07-0.10 s) and on one with prefill (0.39 s against 0.07). Node agents
that check their own partitions leave 0 over-admissions at 0.6x with prefill free and 0-4 elsewhere,
send 5-90 requests back to the router (up to 5,070 refusals, one per arrival while a request waits)
and leave no sequence waiting at an engine; no cost moves outside the overload's noise of -800 to
+600 request-seconds, and the 0.1 s tail comes back on two of the four cells that rose (0.28 to 0.09
s, 0.17 to 0.07 s) and falls by a third to a half on the others (0.24 to 0.17 s, 0.39 to 0.20 s).
The residual over-admissions are fan-out agents staged together, whose claims the node checks one at
a time. Re-run with the driver's clock corrected (§9.27), the verdict stands and the figures move
within the overload's noise: 131-203 admitted blind; at 0.6x with a 1 s outage 10 / 27 / 9 / 25 / 5
of 154 / 161 / 140 / 167 / 150 over-admitted (3-17%), 53 / 49 / 38 / 59 / 23 of 189 / 202 / 151 /
199 / 170 with prefill taking engine time (14-30%) and 15 / 18 / 30 / 33 / 31 of 131-144 at 0.5x
(11-23%), and 0-15, 5-40 and 6-42 at 0.1 s; 33 / 24 / 1 / 37 / 0 sequences wait at an engine, 11-88
with prefill and 41-118 at 0.5x. The 1 s tail rises on one or two seeds of five by 56-190 ms. The
0.1 s tail rises on one seed at 0.5x (0.34 s against 0.07 s) and one with prefill (0.40 s against
0.07 s), and the node check brings the first back to 0.09 s and halves the second to 0.20 s. Node
agents that check leave 0-1 over-admissions at 0.6x with prefill free and 0-4 elsewhere, and send
7-90 requests back to the router (up to 5,168 refusals).

**P4 -- The one checkpoint worth taking is the estimators', and only where memory binds hardest.**

With observations at completion, losing the estimators costs nothing at 1.0x of the grant and stays
within noise at 0.75x with prefill free; at 0.6x with prefill taking engine time it multiplies the
interactive first-token p99 in the window by 1.0-4x and leaves up to 50 more requests unserved;
restoring them from a snapshot at most 10 s old brings both to within noise of no fault.

- *If right:* soft state needs one checkpoint, tens of kilobytes on the provisioning clock -- the
  logged tier's slowest writer, not a per-decision one -- and §1 says so, with the count carrying it.
- *If wrong* (the snapshot's arm no better than losing them): claims re-learn within one decode on
  this workload, and nothing the scheduler holds needs a checkpoint.

**Measured (increment 3): wrong.** With every length observed when its decode ends, losing the
estimators costs -0.5 to +9 request-seconds at 1.0x and 0.75x (zero with prefill) and moves no
count, and at 0.6x with prefill taking engine time -148 to +364 against the overload's noise of -300
to +300, with the interactive first-token p99 in the window at 63-69 ms on every seed against 63-66
with no fault, 14 fewer to 13 more requests unserved, the router queue within 5% and the cancels
within 16% of the run without the fault. Restoring a snapshot 4.0 s old (written every 10 s) gives
the kept run to the digit in every cell. Claims re-learn from the first decodes that end, a fraction
of a second at 100 decodes a second, so the dispatch-time observations of §1.3 did not hide a
transient and nothing the scheduler holds needs a checkpoint. The snapshot costs 6 writes of 32 KiB
in a 60 s run: 0.1 a second and 3.2 KiB/s.

**P5 -- A restart costs its outage, and a lease-length outage costs weeks of the sidecar's tax.**

With streams held below the scheduler, a restart costs nothing a run can see at a 0.1 s outage
(within 15 request-seconds), 105-155 at 1 s -- `λD²/2` to within 15% -- and 30,000-40,000 at 15 s,
where the burst of retries adds 10-45% and leaves 200-400 requests unserved. The interactive
first-token p99 in the window is 0.6-0.9 s at 1 s and 14-15.5 s at 15 s. Spreading the retries by a
client's backoff removes most of the burst's share at 15 s and changes nothing at 1 s.

- *If right:* a scheduler on the request path must take over in under a second, which a lease cannot
  give; node agents enforcing their own partitions are what make a takeover without a lease safe; and
  §1's failure model gains the outage as its largest term.
- *If wrong* (the 1 s cost more than 1.5 times `λD²/2`): the burst, not the outage's length, sets the
  price, and admission of retries at the restarted scheduler is the mechanism to build.

**Measured (increment 2): the held and burst figures are right; spreading the retries is wrong.**
With streams held, a 0.1 s outage costs -11 / -3 / +8 at 0.75x of the grant and 0.5 / 1.2 / 0.8 at
1.0x; a 1 s outage 125.5 / 111.6 / 142.7 and 127.6 / 126.7 / 125.6 against `λD²/2` of 125 (within
11%); a 15 s outage 38,698 / 39,050 / 38,043 and 32,950 / 32,875 / 32,454 request-seconds, 15-17% of
it beyond `λD²/2` at 1.0x and 35-39% at 0.75x, with 238-377 requests unserved. The interactive
first-token p99 in the window is 0.68-0.82 s at 1 s and 14.7-14.8 s at 15 s. A client that backs off
-- 100 ms doubling, jittered -- costs more, not less: at 15 s with streams held 45,011 / 45,289 /
44,260 and 42,007 / 42,109 / 41,871 (+16% and +28%) and a window p99 of 22-23 s, and 1 s with streams
dying 390 / 375 / 364 and 363 / 361 / 347 against 383 / 336 / 346 and 301 / 302 / 284 for the burst
(+2 to +22%). It leaves fewer requests unserved (99-260 against 238-377 at 15 s), so what the
burst loses is requests and what backoff loses is the time they wait: a retry lands up to twice as
late as the outage lasted. Re-run with the driver's clock corrected (§9.27): held, a 0.1 s outage
costs -0.5 / 6.9 / 5.2 at 0.75x and 1.3 / 0.7 / 0.8 at 1.0x; 1 s 130.2 / 127.7 / 139.1 and 127.3 /
126.8 / 125.8, within 12% of `λD²/2`; 15 s 38,829 / 38,673 / 38,535 and 32,993 / 32,860 / 32,426,
37-38% and 15-17% beyond it, with 236-374 requests unserved and the window's p99 unchanged. Backing
off costs 50,822 / 51,203 / 49,580 and 43,379 / 43,531 / 42,573 at 15 s held (+29% to +32%), with a
window p99 of 22-24 s, and 423 / 391 / 360 and 371 / 365 / 348 at 1 s with streams dying against 370
/ 346 / 338 and 297 / 303 / 284 (+7 to +25%). It leaves 197-324 requests unserved against 236-374 at
15 s, 9-18% fewer where the first run had 26-58%: the drift had thinned the arrivals after a
backoff's retries. The verdict stands, and backoff buys less than the first run showed.

**P6 -- Streams that die with the scheduler cost half a one-second outage, and held they cost
nothing.**

A restart whose streams die reaches 75-95 of them and costs 50-80 request-seconds at a 0.1 s outage,
40-60 decode-seconds of it thrown away; at 1 s, 270-380 against 105-155 held; a client's continuation
recovers 50-120 request-seconds of the 1 s figure.

- *If right:* §2.6's fate-sharing has a price per crash, and Phase 9's buffer makes the alternative
  cheap: the stream belongs to the node agent, and §2.6 is restated so that the scheduler owns the
  decision and the node agent the connection to the engine.
- *If wrong* (streams that die within 1.5x of held ones at 1 s): few decodes are in flight at a
  crash, or a continuation restores what a restart loses, and fate-sharing is cheap enough to keep.

**Measured (increment 2): the costs are in the band and the streams reached are above it.** A restart
whose streams die reaches 109 / 112 / 102 of them at 0.75x and 110 / 111 / 102 at 1.0x (75-95
predicted; 20-24 of them the agents of 4-5 gangs), throws away 68 / 73 / 61 decode-seconds (40-60), and
costs 75 / 66 / 65 and 72 / 74 / 62 request-seconds at a 0.1 s outage (half of a 1 s outage's 125;
50-80 predicted) and 383 / 336 / 346 and 301 / 302 / 284 at 1 s (270-380; the first seed is 3 over).
A client that continues recovers 106 / 103 / 99 and 57 / 62 / 52 of them (50-120), throwing away
13-16 decode-seconds, the agents' that a continuation cannot keep. Kept belief and estimators are
worth 11 and 15 request-seconds of the 1 s figure on two of six cells and at most 3 on the other
four. Re-run with the driver's clock corrected (§9.27), the streams reached and the decode-seconds
thrown away do not move; a 0.1 s outage costs 74 / 74 / 71 and 71 / 75 / 63 and 1 s 370 / 346 /
338 and 297 / 303 / 284, all inside 270-380; a continuation recovers 93 / 110 / 91 and 54 / 63 /
51; and kept belief and estimators are worth -25 to +9 request-seconds of the 1 s figure, inside the
noise on every cell.

**P7 -- An engine crash costs its replica's restart; its KV and its streams are second-order.**

On one node of four, an engine back after 2, 8 or 30 s costs under 20, 20-50 and 90-190
request-seconds, 3.5-5.5 for each second it is down at 1.0x; losing its `NVMe` spill as well changes
nothing; continuing its streams is cheaper than restarting them on every seed, by under 150; an
engine that never returns leaves 50-200 requests unserved, half or more of them fan-outs refused.

- *If right:* §1's "every belief about it goes to P(resident) = 0" is right and costs nothing, and the
  number to ask a serving stack for is its restart time, as Phase 6 found for a placement.
- *If wrong* (the KV's loss measurable, or continuation worth more than 50): sessions return to
  their crashed node's replacement often enough that its cache matters, and the connector's spill
  belongs outside the engine's process.

**Measured (increment 4): right on the costs, and wrong that continuing always wins.** On node 0 of
four, an engine back after 2, 8 and 30 s costs 17.1 / 9.9 / 13.4, 45.8 / 39.8 / 45.0 and 156.4 /
139.9 / 114.8 request-seconds at 1.0x of the grant (under 20, 20-50 and 90-190 predicted), 3.6-5.0
for each second it is down, and at 0.75x 18.5 / 19.0 / 27.5, 59.8 / 24.9 / 49.5 and 217.6 / 246.0 /
112.5 (27.5, 59.8 and 246.0 above their bands). A restart of 120 s, longer than the 36 s of the run
that is left, costs 165 / 140 / 117 and 565 / 635 / 344, with 50-61 and 72-129 more requests
unserved and 25-30 and 79-92 more fan-outs refused (half or more of the unserved at 1.0x, and all of
them at 0.75x). Losing the spill changes nothing (156.1 / 139.9 / 115.0 against 156.4 / 139.9 /
114.8) while it loses 7,971-11,187 KV blocks against 3,099-3,614. The crash fails 34-42 streams, 2-4
of them gangs' agents, where the pre-measurement had 18-24 and no gangs. Continuing them rather than
restarting is cheaper on two of three seeds at 1.0x (by 7 and 24 request-seconds, dearer by 6 on the
first) and on two of three at 0.75x (by 13 and 74, dearer by 45): a difference inside the noise, not
the 'every seed' predicted.

**P8 -- Node loss is an engine crash that never returns, plus its gangs and its durable sandboxes.**

Losing one node of four costs an engine crash that never returns plus 30-90 request-seconds at 1.0x
for the node's host work -- its cells and heaps rebuilt elsewhere and run on three nodes -- and
within noise of it at 0.75x; refused fan-outs rise from 0 to 20-35 at 1.0x and from 30-45 to 100-150
at 0.75x. *Arithmetic, no pre-measurement:* every durable sandbox on the node is lost unless copied
at suspension, which costs under 1 MiB/s a node at Phase 7's long-running rate; and a router that
waits for a 10-40 s lease before it stops routing to the node pays an outage of that length on a
quarter of its traffic, where one that acts on the first failed dispatch pays none.

- *If right:* liveness's lease decides re-placement and budgets, never routing; all-or-nothing
  admission is the first casualty of a lost node; and Phase 7's durable invariant needs a copy off
  the node, so §4's `retention: durable` is restated as demoted, never dropped, and copied at
  suspension.
- *If wrong* (the node's host work costs more than its engine): a lost node is mostly a host-DDR
  event -- its service heaps' cold starts -- and §5's host DDR properties are measured under loss.

**Measured (increment 4): the costs hold, and the lease is the largest term by two to three orders
of magnitude.** A node declared lost at once costs 196.6 / 201.9 / 179.1 request-seconds at 1.0x of
the grant, 32-62 over an engine that never returns (30-90 predicted), and 885 / 443 / 458 at 0.75x
against that engine's 565 / 635 / 344, which is inside the noise; refused fan-outs rise from 0 to 26
/ 23 / 28 at 1.0x and from 28 / 44 / 42 to 103 / 142 / 121 at 0.75x (20-35 and 100-150), and it
loses 96-115 host blobs (99 / 115 / 96 at 1.0x). A router that learns of it after 2 s pays 371 / 407
/ 343 and 933 / 783 / 902 and an interactive first-token p99 of 1.4-1.7 s; after 10 s, 4,286 / 4,781
/ 4,217 and 5,973 / 9,784 / 6,093 with a p99 of 9.4-9.7 s; after 40 s, Kubernetes' node grace,
65,786 / 66,742 / 64,524 and 70,255 / 71,402 / 68,772 with a p99 of 39.4-39.7 s. The requests parked
on the dead node are the quarter of the window's arrivals the arithmetic named: 2,481-2,510 against
2,500, waiting 55,442-56,755 s between them. The unit test checks that a node declared at once is
never parked on. The copy's clause is graded on the long-running programs, where losing node 0 half
way through the arrivals takes 18 / 18 / 20 durable cells and leaves one program each holding lost
state, with 23-25 host blobs gone; with each cell copied when it is marked none is lost, 18 / 18 / 20
are saved and 6.0-6.2 GiB is copied over the run, 0.14-0.29 MiB/s a node (0.012-0.024% of a zone
link; the arithmetic said about 0.6 MiB/s). Turn latency moves by 0.1-0.4% with or without the
loss, so a lost sandbox is a correctness failure and not a latency one.

**P9 -- The crossover: integration's saving pays for a crash whose streams die about once an hour or
two at a sub-second takeover, and for one whose streams are held about once a minute.**

*Arithmetic on P5, P6 and Phase 8's tax.* At 250 req/s a crash costs as much as the sidecar's tax
over under 2 minutes with streams held and a 0.1 s takeover, 0.8-1.9 hours with streams dying and a
0.1 s takeover, 4-9 hours at 1 s, and 3-7 weeks under a 15 s lease. The sidecar's own failover -- 15 s
of `hash only` routing with every stream intact -- costs 300-900 request-seconds.

- *If right:* §5's property 6 holds as a consequence of unification only with held streams and a
  sub-second takeover; with streams that die it holds only while the integrated process crashes no
  more than once every hour or two more often than a proxy would, which prices §2.6's item 2 --
  in-process extensions -- as a reliability requirement on policy code.
- *If wrong* (the sidecar's failover above three times a 1 s restart whose streams die): a fail-open
  proxy is this comparison's expensive failure, and the integrated path's case grows.

**Measured (increment 5): the table holds, and the sidecar's failover is an order of magnitude
cheaper than predicted.** At 1.0x of the grant, with the integrated path's saving at 11-19 ms of
request time a second (Phase 8's 44-75 us a request at 250 req/s), a crash costs in request-seconds
and as much of that saving: streams held with a 0.1 s takeover 0.5 / 1.2 / 0.8, under 75 s; streams
dying with a 0.1 s takeover and a client that restarts 72 / 74 / 62, 1.0-1.8 hours; streams held
with a 1 s takeover 128 / 127 / 126, 1.9-3.2 hours; streams dying at 1 s with a continuation 244 /
240 / 232, 3.5-6.0 hours, and with a restart 301 / 302 / 284, 4.4-7.5 hours; a 15 s lease 32,950 /
32,875 / 32,454, 2.9-4.9 weeks; one engine of four down for 30 s 156 / 140 / 115, 2.0-3.5 hours. At
0.75x the same arms cost 75 / 66 / 65, 126 / 112 / 143, 276 / 233 / 247, 383 / 336 / 346 and 38,698
/ 39,050 / 38,043 (1.0-9.0 hours and 3.4-5.8 weeks). The sidecar's fail-open window, 15 s of `hash
only` routing with every stream intact, costs 30 / 53 / 38 at 1.0x and 21 / 12 / 41 at 0.75x:
0.4-1.0 hours of the saving, not the 300-900 request-seconds predicted. It is cheaper than any
restart that kills streams (62-75 even at a 0.1 s takeover) and than a 1 s takeover that holds them,
so the *if wrong* branch does not fire, and its opposite does: an integrated scheduler whose streams
die in a crash may crash only about as often as a fail-open proxy fails over -- a 0.1 s takeover
costs 1.7 times the window at 1.0x -- and no more often than once an hour or two for the sum to
favour it over a proxy that never takes streams down. Re-run with the driver's clock corrected
(§9.27), only the held 0.1 s takeover moves at 1.0x: 1.3 / 0.7 / 0.8, or 51-87 s of the saving.
The others stay within a tenth of an hour (71 / 75 / 63, 127 / 127 / 126, 243 / 241 / 233, 297 / 303
/ 284 and 32,993 / 32,860 / 32,426). At 0.75x the held 0.1 s takeover costs -0.5 / 6.9 / 5.2, about
0.1 hours, and the rest 74 / 74 / 71, 130 / 128 / 139, 276 / 235 / 247, 370 / 346 / 338 and 38,829 /
38,673 / 38,535 (1.1-8.9 hours and 3.4-5.8 weeks). The engine and the fail-open window do not move,
a restart that kills streams costs 63-75 at a 0.1 s takeover, and the conclusion stands.

**P10 -- At fleet scale FoundationDB carries the logged tier with a cluster smaller than its
published benchmark.**

*Arithmetic on P1's per-node rates and FoundationDB's published figures.* At 10,000 nodes: owned
changes 1.8-3.5 million a second, 0.09-0.17 cores a node at FoundationDB's cluster rate, so
throughput is absorbable as §1 says; the logged tier 150,000-500,000 a second on agent mixes, 8-26
cores at its single-core write rate and 70-240 at its cluster rate, under the 820,000 writes a second
of its 384-core benchmark; the record 1,000-1,100 a second, 95% of it liveness.

- *If right:* §8's choice of FoundationDB for the logged tier stands on rate, the case for a
  purpose-built log does not arise from the count, and the per-region record of Phase 11 divides each
  figure by its regions.
- *If wrong* (the logged tier above 820,000 writes a second at 10,000 nodes): side-effecting work is
  denser per node than Phase 7's presets, and the log is the one place §8 allows that building below
  the layer could pay.

**Measured: right.** From the integrated arm at 1.0x of the grant at 10,000 nodes: owned changes
3.01 million a second, 151 cores at FoundationDB's single-core write rate and 1,411 at its cluster
rate, 0.14 cores a node; the logged tier 146,000 (agentic), 298,000 (pipeline), 330,000 (multi-agent)
and 491,000 (long-running) a second, 7-25 cores at the single-core rate and 68-230 at the cluster
rate, every one under the 820,000 writes a second of its 384-core benchmark; the record 1,058 a
second, 95% of it liveness. The agentic figure is just below the band's 150,000.

---

## 3. What a fault model must and must not do

Eleven rules. The first is the gate; the third is the one most likely to be broken for a good
reason.

1. **The gate.** With every new bit off, byte-identical to `HEAD` on the reproducible set as
   `phase-7.md` left it -- `residency` (twice), `flows`, `placement`, `volatility`, `ownership`,
   `price`, `belief`, `influence`, `fleet`, `enforce`, `programs` and `distributed --crossing native`
   with its measured lines filtered. Each bit has a case in which it must change nothing: counting
   writes; a fault scheduled past the trace's end; `--streams held` with no fault; a rebuild at once
   with no fault; observations at completion where nothing reads them (a static claim, no
   observables); every decode a flight with no fault. Checked after every work item.
2. **The trace is the same; a fault is an instant.** Arms differ in what fails and when, never in what
   arrives. A fault's cost is an integral against the run with no fault on the same seed, and
   unserved requests are counted apart and never folded into it.
3. **A restarted scheduler reads only its sources.** Whatever §1.4's table gives no source is gone;
   the belief it starts with is the replay it can get and what it dispatches; generator truth never.
   Arms that read the truth -- an instant snapshot, a ledger rebuilt at zero delay -- are bounds,
   printed beside the arms and never called arms.
4. **The router is down for the outage.** No placement, no queue service, no cancel; node agents
   and engines run on, and a held stream keeps decoding.
5. **Engine KV is never written** (`own::authority`). A crash drains the engine's tiers as the
   engine would lose them, a restart is the engine's own, and the census stays at 13.
6. **Durable entries are lost only with their node.** A node loss counts what it took; any other loss
   of a durable entry is a failure, asserted.
7. **Nothing tuned.** Outages, leases, restart times and renewals are swept conditions; a crash rate
   is never chosen, and every comparison that would need one is a crossover.
8. **One bit per mechanism:** `--fault`, `--fault-at`, `--streams`, `--outage`, `--retry`,
   `--client`, `--rebuild`, `--subscriber`, `--snapshot`, `--observe`, `--node-check`, `--restart`,
   `--spill`, `--suspect`, `--lease`, `--lease-renew`, `--copy-durable`; the headline runs change one
   at a time.
9. **A fault draws nothing.** Faults are scheduled, not sampled, and a retry's jitter draws from a
   stream of its own, so every arm's trace and every condition's stream are byte-identical to the run
   with no fault up to the fault's instant.
10. **Measure, do not repair** (`phase-2.md` rule 1).
11. **Three seeds, five where memory binds hardest,** and every figure with its partition, prefill
    setting, fault, instant and outage.

---

## 4. Work items

The phase is one pass through five increments, in order, each ending with a number: the count and
the rung; faults that reach requests; the scheduler's restart and its sources; engines and nodes;
the command and the crossover. Within each, nothing that can move a number lands before the items
that cannot. If the phase has to stop early it stops at an increment's end.

### 4.1 Writes by tier and owner

`Machine::writes()`: owned changes by owner -- the scheduler's decisions, dispatches, reservations
committed and released, flights opened and closed, queue entries, cancels, refusals, flow-graph and
gang-staging writes, tenant-meter updates; the node agents' host-class admissions and evictions,
leases and durable marks -- and the inferred stream by event type and tier, recorded with or without
an observer and drained at each arrival, beside length and template observations. Programs add the
logged tier by cause (Phase 7's `LogCause`). Per simulated second, per node, per request. No
consumer: a no-op, checked as one.

### 4.2 Liveness and the record

A lease per node, renewed every `--lease-renew` seconds (10 by default, Kubernetes' value), and the
planner's placements, counted as the record's writes and never stored.

### 4.3 The durable-append rung

`boundary.rs` gains `Append`, `Flush` and `FullFlush`: a 128-byte append to one file, then `fsync`,
then `F_FULLFSYNC` on darwin or `fdatasync` on linux, best of 10 with the timer subtracted, published
as an ordering beside the ladder's other rungs. `--features fdb` adds a FoundationDB commit rung
against a local `fdbserver` through `libfdb_c`, off by default and outside the gate.

### 4.4 Every decode a flight

A fan-out's agents register as flights of one gang. A fault that aborts any of them aborts the gang,
which retries whole, as all-or-nothing admission requires; the registry changes no outcome with no
fault. Flights run with `--belief`: an abort releases the belief's pin on the flight's blocks.

### 4.5 Observations at completion

`--observe dispatch | completion`. At completion, an output length, a flow template and Phase 7's
estimator samples are recorded when the decode closes; `dispatch` is today's. A model correction with
its own A/B, run at every partition before any fault is graded on it.

### 4.6 Faults

`Fault` and `Machine::inject(fault)`: a scheduler restart, an engine crash, node loss, and an
estimator reset, each at `--fault-at` (a share of the trace). The router is down until the outage
ends: no placement, its queue held; node agents and engines run on.

### 4.7 The outage and the retry

Arrivals during an outage wait for its end, with their latency measured from the arrival.
`--retry burst | backoff`: every waiting client retries at the outage's end, or each retries at the
first point of an exponential backoff (100 ms doubling, jittered from a stream of its own) after
it, so that the burst is a condition.

### 4.8 Stream fate and the client's retry

`--streams shared | held`. Shared: every flight is aborted at the crash, the node agent aborting the
engine's sequence (Phase 9's abort), engine queues returned, and the client retries as
`--client restart | continue` -- a continuation of the tokens it received. Held: flights decode on,
the node agent keeps each stream's buffer, and the client re-attaches at the outage's end, losing
only the wait.

### 4.9 The restarted scheduler's sources

Each row of §1.4. Reservations and in-flight load come from node agents: `--rebuild now | after:<s> |
never`, the last leaving the scheduler blind to them until the pre-crash decodes end. The belief:
`--subscriber cold | snapshot`, a cold subscriber holding the buffer's last 10,000 steps and what it
dispatches and anchoring on the first batch it receives (§1.8). Estimators: lost, or restored from a
snapshot taken every 10 s (`--snapshot estimators`), the snapshot's writes counted in the logged
tier. The flow graph, tenant buckets, the router queue and gang staging are lost.

### 4.10 Node agents enforce their partitions

`--node-check`: a node agent keeps its own ledger of the claims it has dispatched and queues a
dispatch its partition does not admit, as the router's check does, so that a scheduler whose ledger
is blind or a second scheduler cannot over-admit the node. Counted as over-admissions refused at the
node.

### 4.11 The engine crash

`--fault engine`: the node's GPU and host-offload KV lost and its `NVMe` spill kept or lost
(`--spill`); its flights failed and continued by the router, which relayed their tokens, or
restarted; its engine queue returned to the router; the node out of the decode pool for its restart
(`--restart`, Phase 6's start time plus the copy from its own agent's cache); the belief told by a
`Cleared` event.

### 4.12 Node loss

`--fault node`: everything on the node lost, durable cells counted (`durable_lost`, Phase 7's
counter, given its one legal writer). `--suspect dispatch | silence:<steps> | lease`: the router stops
placing on the node at its first failed dispatch, after a silence, or only at the lease's expiry
(`--lease`); under `--fleet` the planner re-places its replicas once the lease expires.
`--copy-durable` uploads a durable cell at suspension, charged its bytes at the model's region link,
so that node loss loses none.

### 4.13 Instruments

| instrument | measures | over |
|---|---|---|
| writes | owned changes by tier and owner, inferred events by type and tier, logged by cause, record by writer | every run |
| cost | request-seconds of service against the same trace with no fault | every faulted run |
| unserved | refused, unplaced, abandoned, fan-outs refused, by cause | every request |
| window | the interactive first-token p99 and each class's service p99 over 30 s of arrivals from the fault | every faulted run |
| streams | aborted, re-attached, continued, decode thrown away | every fault |
| blind | admissions made while the ledger was blind, over-admissions, refused at a node | every restart |
| belief | reads of resident blocks the scheduler does not know, by second after the restart | every restart |
| loss | KV blocks, host blobs and durable cells lost | every engine and node fault |
| rung | append, flush, full flush, and the optional commit | the ladder |

These are the instruments the pre-measurements approximated, and §8's figures are reproduced with
them first.

### 4.14 `polyphonic durability`

A reproducible sweep, as `enforce` is for Phase 9: no control crossing charged, seed-deterministic,
three seeds a cell and five where memory binds hardest.

1. the gate, as printed check lines
2. the count, and its fleet-scale arithmetic (P1, P10)
3. the rung (P2)
4. routing state: the belief, the ledger, the estimators observed at dispatch (P3)
5. the estimators observed at completion, lost and snapshotted (P4)
6. outages and takeovers, burst and backoff (P5)
7. stream fate and the client's retry (P6)
8. over-admission where memory binds, with and without node agents' checks (P3)
9. the engine crash (P7)
10. node loss, suspicion and the lease, durable cells (P8)
11. the crossover, and the sidecar's fail-open window (P9)

`distributed` takes none of this phase's flags; `programs` takes `--fault` for the logged tier and
durable cells.

### 4.15 Report and publish

| target | change |
|---|---|
| `owned-and-observed.md` §9 Phase 10 | a **Status** line; the phase restated as the count, the outage, the streams and the sources |
| `owned-and-observed.md` §1 | the durability table's rates measured per node and per request (P1); liveness as the record's writer on its own clock; the failure-model paragraph restated with the outage and the streams as its terms, each rebuild source as measured, and node agents' enforcement as what lets a takeover skip the lease (P3, P5, P6); the telemetry budget against the measured removals |
| `owned-and-observed.md` §2.6 | the stream's fate as a decision with its price, and the node agent holding it |
| `owned-and-observed.md` §4 | `retention: durable` with the copy node loss requires (P8) |
| `owned-and-observed.md` §5 | property 6 with its crossover under failure (P9); property 4's fragility to a lost node |
| `owned-and-observed.md` §8 | the logged tier's store against the fleet-scale count (P10); takeover without a lease |
| `residency-ledger.md` | a *Durability* section, every figure with its partition, fault, instant and outage; *Standing* rows for each prediction |
| `phase-7.md`, `phase-9.md` | nothing: they keep their results as measured |

---

## 5. Verification

- **Byte-identity with every new bit off**, against the commit before this phase, on the
  reproducible set at a reduced `--ops` and a second seed, after every work item.
- **The gate** (rule 1), one check line per bit.
- **Every request closes once** or is counted unserved by cause; a request a fault reached closes
  with its latency from its original arrival; the clock never moves backwards across an outage.
- **The router does nothing while down:** no placement, queue service or cancel inside an outage,
  asserted.
- **Held streams are never aborted and shared ones always are**, at the fault's instant, asserted;
  an aborted flight releases its batch slot, its pins, its reservation and the belief's pin.
- **A restarted scheduler reads only its sources:** with every source cut, the scheduler's ledger,
  belief and estimators are empty at the instant after the fault, asserted.
- **Durable entries are lost only by node loss**, asserted at every eviction and every fault.
- **The census.** `cargo build --release --features census` still emits 13 warnings.
- `cargo fmt --check`, `cargo clippy --all-targets` and `cargo test` clean.

---

## 6. Risks

1. **A fault is a transient inside a short run.** A crash at 40% leaves 36 s, so the cost of a node
   that never returns or an engine out for 120 s is bounded by the run's end and grows with its
   length; such figures are quoted with the time left, and per second of outage where they scale.
2. **Overloads are chaotic.** At 0.6x and 0.5x any perturbation -- even an estimator reset with no
   blind window -- moves a run by -300 to +280 request-seconds seed to seed, so those regimes are
   graded on counts and windowed tails, with five seeds, and never on a run's total.
3. **The burst is the worst case.** Every client the outage delayed retries at its end; real clients
   back off, and §4.7 makes the spread a condition.
4. **Fan-out agents were not flights in the pre-measurements.** Every fault left 22% of decodes
   running; §4.4 closes it, and the figures the build reports will be larger by up to that share.
5. **The host is not the target for the rung.** Apple silicon's full flush is a consumer drive's;
   published drives with power-loss protection flush in microseconds. The ordering is the result,
   and P2's conclusion is argued from the quorum as well.
6. **One scheduler.** Active-active schedulers would divide a crash's streams by their number but
   split the in-flight count each reads exactly -- the divergence llm-d's RFC for horizontally
   scaled pickers names (llm-d/llm-d-router#1593) and Phase 4's stream-fed load measures. More than
   one scheduler is Phase 11's structural change.
7. **No crash rate has a source**, and none is chosen; P9 is a crossover, and a reader who wants a
   verdict supplies the rate.
8. **A client's continuation assumes the client kept what it received.** An agent framework does; a
   browser tab may not, and OpenAI keeps a background response for about 10 minutes. Both retries are
   arms.
9. **Host state's price is the workload's.** A lost node's service heaps cost their 15 s cold start
   where they are rebuilt, and how many a node holds alone depends on how the trace spreads three
   services over four nodes; P8's host clause is a figure for this trace, with the heaps lost
   printed beside it.
10. **The pre-measurements are emulations outside the repository.** The belief was the exact view
    filtered to what the restarted scheduler had dispatched, with no replay; the blind ledger was a
    shadow of post-crash commits; the blind estimators suppressed observations for a fixed window;
    the engine's restart took its node out of the decode pool and left its host work running;
    retries arrived as one burst. The first engine emulation took the whole node out of placement,
    which made a lost node identical to an engine that never returned, to the request-second; it was
    caught before this plan and is not the one quoted. §8 says how each was taken.
11. **Runtime.** About a second a run on this host; the sweep is several hundred runs, minutes as
    parallel processes.

---

## 7. Out of scope

- **The store itself.** FoundationDB, the layer §8 describes, and its testing under
  `foundationdb-simulation` are infrastructure; the simulator counts writes and models no store, and
  the commit rung is optional.
- **More than one scheduler, and regions.** Active-active routers, a scheduler per region and the
  budgets between them are Phase 11's; this phase prices one scheduler's crash and its takeover.
- **Partitions, split brain and correlated failures.** Node agents' enforcement (§4.10) is what keeps
  two live schedulers from over-admitting; a partition's other consequences, a rack or a zone lost at
  once, and Byzantine faults are not modelled.
- **Gray failures.** A hung collective, a slow node, an engine that stops stepping without crashing:
  detection by silence is Phase 4's episodes, and the phase injects only clean faults.
- **Planned restarts.** A rolling deploy that drains a scheduler's streams before it stops loses
  nothing by construction, and live migration of sequences between engines -- the README's VM
  migration applied to KV -- is a successor.
- **The logged tier's own outage.** While the log cannot commit, `SideEffecting` dispatch waits; that
  is a fault of the store, priced when the store exists.
- **Real failure traces.** Crash and restart distributions from a production fleet would turn P9's
  crossover into a verdict; none is public at this grain.

---

## 8. Pre-measurements

All taken on an instrumented copy of `aeeb943`, run outside the repository and not committed. With
every hook off the copy reproduces `enforce --seeds 1 --ops 3000 --seed 2 --sections
gate,cancel,departures,buffer`, `belief --ops 3000 --seed 2 --sections gate`, `residency --ops 3000
--seed 2` and `fleet --seeds 1 --ops 3000 --seed 2` byte for byte, and counting events or scheduling a
fault past the trace's end changes nothing on the measured configuration (three seeds). Rows use the
`belief` cluster at 250 req/s with Phase 9's integrated arm -- `--admit quantile` at each class's own
p90, `--queue slo`, `--engine-wait priority`, `--cancel continue --victim recent`, `--throughput 0.3`
-- the engine allocating with decode output held, prefill free, seeds 1-3, and the fault at request
6,000 of 15,000, unless they say otherwise.

| name | what | how | headline |
|---|---|---|---|
| *count* | writes by tier and owner | counters on the copy: decisions and dispatches (the machine's own), reservations committed and released, flights opened and closed, queue entries and serves, cancels, refusals, flow-graph inserts and removals, fan-outs staged, engine-queue entries and starts, host-class admissions (misses of `Snapshot` and `ServiceHeap`) and evictions, KV events by type and tier recorded with no observer and drained at each arrival, length observations; per simulated second over the run and its drain; the cluster with nothing enforced, and the arm at 0.75x and 1.0x | §1.1's table |
| *rung* | a durable append on the host | a Python loop on an M2 Max's internal SSD: 128-byte `os.write` to one file, then nothing, `os.fsync`, or `fcntl(F_FULLFSYNC)`; 2,000 appends (300 for the full flush) a run, five runs, the lowest median and p99 of the five | §1.2's table |
| *restart* | a scheduler restart | at the fault: streams that die -- every flight aborted through Phase 9's abort and requeued with its original arrival as the original request or a continuation, engine queues returned to the router -- or streams held, the flights decoding on; the router queue and every placement held for the outage; arrivals during it deferred to its end and submitted together, their latency from their nominal arrival; the flow graph, landings, tenant buckets and gang cancellations cleared; estimators reset unless kept; the belief emulated as the exact view filtered to blocks dispatched since the restart, until a snapshot after 1 s or never; under held streams the router's ledger and in-flight load replaced by a shadow of post-crash dispatches until rebuilt, at once or when the last pre-crash decode ends | §1.5 to §1.7; 82-88 streams reached, 49-57 decode-seconds thrown away |
| *overadmit* | the blind ledger where memory binds | as *restart*, streams held, no estimator or belief loss, outages of 0.1 and 1 s, the ledger rebuilt at once or never, at 0.6x with prefill free and taking engine time and at 0.5x; an over-admission is a commit the full ledger would not have admitted | §1.7: 0-32% of 129-196 blind admissions |
| *relearn* | estimators lost, observed late | length histograms, observed means, flow templates, attained service and planner accrual reset at the fault, and length observations suppressed for 0, 1, 2 or 5 s after it; 1.0x, 0.75x with prefill free and taking engine time, and 0.6x with prefill taking engine time | §1.7's table |
| *engine* | an engine crash | node 0's flights failed and continued or restarted through Phase 9's abort, its engine queue returned to the router, its GPU and host-offload KV drained and its spill kept or lost, its batch flushed, its reservations cleared, and the node out of the decode pool for 2, 8, 30 or 120 s while its host work ran on | §1.9's table; about 4.5 request-seconds a second of restart at 1.0x |
| *node* | node loss | as *engine*, and everything on the node drained without migration and the node out of placement, never back; fan-outs refused and router refusals counted against no fault | §1.10's table |
| *liveness* | the record's writers | arithmetic: Kubernetes' 10 s renewal (a quarter of a 40 s lease) at 0.1 writes a node a second, beside Phase 6's planner at 0.046 a second on eight nodes | §1.1; 17x the planner on eight nodes |
| *fleet* | the count at 10,000 nodes | arithmetic: *count*'s owned changes a node (190-310 a second) and decisions a node (73-77), Phase 7's logged share of decisions on agent presets (0.20-0.66), against FoundationDB's published 20,000 writes a second on one core with the SSD engine and 8.2 million operations a second at 10% writes on 384 cores | P10 |
| *crossover* | fate-sharing against the sidecar's tax | arithmetic: *restart*'s and *engine*'s costs over Phase 8's 43.97-74.97 us a request at 250 req/s; the sidecar's failover as 15 s of Phase 6's `hash only` routing, 16.4-32.1% slower than `scored + fetch` at the same load per node, over a mean service of 0.48 s | §1.11's table |

Sources read 2026-10-05: FoundationDB's
[performance figures](https://apple.github.io/foundationdb/performance.html); Kubernetes'
[leases](https://kubernetes.io/docs/concepts/architecture/leases/), the
[`kube-scheduler` flags](https://kubernetes.io/docs/reference/command-line-tools-reference/kube-scheduler/)
and [KEP-589](https://github.com/kubernetes/enhancements/blob/master/keps/sig-node/589-efficient-node-heartbeats/README.md);
llm-d's [router operations guide](https://llm-d.ai/docs/dev/operations/router),
[llm-d-router#3124](https://github.com/llm-d/llm-d-router/issues/3124),
[llm-d-router#1593](https://github.com/llm-d/llm-d-router/issues/1593) and
[llm-d-async#467](https://github.com/llm-d/llm-d-async/issues/467); vLLM's
[`KVEventsConfig`](https://docs.vllm.ai/en/stable/api/vllm/config/kv_events/); OpenAI's
[background mode](https://developers.openai.com/api/docs/guides/background); Mark Callaghan,
[*SSDs, power loss protection and fsync latency*](http://smalldatum.blogspot.com/2026/01/ssds-power-loss-protection-and-fsync.html);
and Foundry, [arXiv 2604.06664](https://arxiv.org/abs/2604.06664).

---

## 9. What the build found

Increment 1, in the order the findings arrived.

### 9.1 Where it landed

`writes.rs` holds the counters (`Counted`), their snapshot (`Writes`), the KV events' seven slots and
liveness as arithmetic. `Machine::writes()` assembles the snapshot from the counters the machine
already kept (decisions, dispatches, queue, cancel and fan-out statistics, refusals) and the ones it
gained: reservations committed and released (`Reservations`), flights opened and closed, requests
served from the router's queue, sequences started from an engine's, flow-graph writes, length
observations, and the KV events an engine emits. `--count-writes` is an instrument on `EnforceArgs`
beside `--probe-engine` and `--stream-buffer`: it turns on the engines' event logs and drains them
at each arrival, or at each dispatch where an observer or the reuse instrument already does, and
changes nothing else. `durable.rs` holds the rung and `polyphonic durability` the sweep.

### 9.2 The rung is its own module, so no existing ladder output moves

§4.3 put the rung in `boundary.rs`. `boundary::measure` feeds every command that prints or charges a
ladder, so a rung there would have changed `distributed`'s and `data-path`'s output; `durable.rs`
keeps it out. A `Flush` is `fsync` through `libc`, and a `FullFlush` is `File::sync_all`, which Rust
implements as `F_FULLFSYNC` on darwin and `fsync` elsewhere, so on Linux the two rows measure the
same call. The module has the crate's one new `unsafe` block, with its `SAFETY` comment.

### 9.3 Flow-graph writes are counted wherever the graph changes

The pre-measurement counted the placement path's inserts and removals: 107-110 a second. The build
counts every insert and every removal that finds something, including a gang's, which is 120-125 a
second, and owned changes move from "about 760" to 770-776 with nothing enforced. The bands in P1
held either way.

### 9.4 A flight ends closed or cancelled, and a departure is a close

`flights_closed` counts a flight that completes or whose client departs, and a cancelled flight is
counted under `cancels`, so a run with no departures has `opened == closed + cancels`, asserted. A
cancelled request's continuation opens a new flight.

### 9.5 Liveness is arithmetic, and `Machine` holds no lease

`liveness_writes(nodes, seconds, renew)` multiplies nodes by renewals, and `--lease-renew` is a
parameter of the command. The simulator has no node death by lease expiry until increment 4, and
counting liveness as a `Machine` writer would charge it for a state the machine does not have.

### 9.6 The `FaaS` denominator is 120 us, not 129

§1.2 quotes a warm invocation at ~129 us. The command computes it from the constants it names, 40 us
plus the mean of a uniform 0-160 us, which is 120 us; every ratio to it is 7% larger than the same
cost against 129. The ordering and the bands are unchanged.

### 9.7 What is not built

- **Faults and everything they need:** `Fault`, `inject`, the outage and the retry, stream fate,
  the restarted scheduler's sources, node agents' enforcement, the engine crash and node loss
  (§4.6 to §4.12), and the four sections of `durability` they feed (§4.14, sections 4 to 11).
- **Every decode a flight, and observations at completion** (§4.4, §4.5): the preconditions for any
  fault to reach a fan-out agent or to be graded against a deployment's estimator lag.
- **The FoundationDB commit rung** (`--features fdb`), which stays optional.
- **`programs --fault`.** The logged tier is counted by `programs` as in Phase 7, and `durability`
  reads that count; nothing yet crashes a program.

---

### 9.8 Increment 2: where it landed

`fault.rs` holds the fault's types, the retry's timing and its counters. In `machine.rs`, `armed`
(`--track-flights`) registers every decode: a fan-out's agents as one `GangFlight` that closes when
its last agent ends, so a fault that aborts any aborts them all and retries the gang whole through
`retry_gangs`; a refused fan-out's agents, which ran before the refusal, as orphans that a fault
aborts and that expire at their end. `Machine::inject` takes a scheduler restart or an estimator
reset. A restart under shared fate aborts every flight, gang and orphan and returns the engines'
queues to the router; under held fate it aborts nothing. Both clear the flow graph, the landings,
the tenant meters and the gang cancellations, reset the estimators unless kept, and put the router
down for the outage: no queue service, no gang retry and no engine-side cancel, while engines run on.
`Belief::unpin` releases a flight's pin on abort, and the belief's reads are filtered to what the
scheduler has dispatched since the restart until a snapshot or never. `--observe completion` records a
length when its decode ends and drops it if the decode is aborted. `drive_with` in `main.rs` injects
the fault at a request, defers every arrival the outage covers and retries it as a burst or a
backoff, and `ClassTally` keeps a `Detail` per request for the window.

### 9.9 A refused fan-out's agents keep running, and a crash has to reach them

A fan-out is refused when one of its agents is, after the agents before it have been dispatched. Those
agents hold their pins and reservations until their decodes end, and the first version of the gang
registry dropped them: after a shared restart four reservations were still held. They are orphans
now, aborted by a fault and expired at their end, and the test that found them asserts that an
aborted stream holds no reservation, no pinned block and no batch slot.

### 9.10 The agents were 22% of the streams, and they move the figures they were predicted to

The pre-measurement's restart reached 82-88 streams; the build reaches 102-112, 20-24 of them a
gang's agents, and throws away 61-73 decode-seconds against 49-57. The held rows, which abort
nothing, reproduce the pre-measurement to the digit (125.5 / 111.6 / 142.7 at a 1 s outage, 38,698 /
39,050 / 38,043 at 15 s, and the belief and estimator rows), and so does the no-fault run.

### 9.11 Backing off costs request-seconds, and the burst costs requests

§1.6 predicted that spreading the retries would remove most of the burst's share at 15 s and change
nothing at 1 s. Measured, a retry that waits one, two and four doubling intervals lands up to twice
as late as the outage lasted, so every deferred request waits longer and the sum of service rises
2-28%, while the admission the burst overloads refuses fewer (26-58% fewer unserved at 15 s). The
price of a burst is paid in refusals and of a backoff in waiting; neither is free and a client's
backoff is not the remedy §1.6 expected. The remedy it named, admitting retries at the restarted
scheduler, is not built. Re-run with the driver's clock corrected (§9.27), the sum of service rises
7-32% and the unserved fall 9-18%: most of the 26-58% was the first run's drift, which thinned the
arrivals after a backoff's retries.

### 9.12 What is not built in increment 2

- **The reservation ledger's rebuild** (`--rebuild`) and node agents' enforcement (`--node-check`),
  so the over-admission clause of P3 and the 0.6x and 0.5x rows of §1.7 are not reproduced.
- **A subscriber that replays** (`--subscriber`): the belief is the exact view filtered to what the
  scheduler dispatched, with a snapshot after a delay or never, as in the pre-measurement, and not
  the engine's replay of its last 10,000 steps.
- **Observations at completion are built and gated, and not yet graded:** the estimator rows of §1.7
  (P4) need the sweep that suppresses observations for a window, which is increment 3's.
- **The engine crash, node loss, the crossover and `programs --fault`** (§4.11, §4.12, §4.14).

---

### 9.13 Increment 3: where it landed

`Restart` gains a ledger (`Now`, `After`, `Never`) and an estimator source (`Lost`, `Kept`,
`Snapshot`). Under held fate and a ledger not rebuilt, the router's admission check and its load view
read a shadow that holds only the claims committed since the restart, the flights then in the air
are marked and are no cancel's victims, and the shadow ends at its deadline or when the last marked
flight has ended. Each blind admission is checked against the real ledger, and counted as an
over-admission if the node would have refused it. `--node-check` makes a dispatch the real ledger
does not admit return to the router's queue, held back for one arrival, and covers a fan-out's
agents when they are placed. `--snapshot-estimators` copies the length histograms, the observed
means, the flow templates and the attained service every N seconds, counts the write and its bytes
in the logged tier, and a restart restores the last copy.

### 9.14 Over-admission is rare and the node check is cheap

A blind ledger over-admitted nothing in the unit fixtures at any partition, because the router's
queue backs up and waits for room before it has a stale claim to act on; the cases in the tests fill
the real ledgers directly. In the `belief` cluster it appears only at 0.6x and 0.5x, as predicted.

### 9.15 Refusals are counted once per request

The first count of requests refused at a node counted every retry, one per arrival while a request
waited for room: 3,206 against 54 requests at 0.6x. `refused_at_node` is now distinct requests and
`refusal_attempts` the retries.

### 9.16 A subscriber that replays is not built, and cannot matter at this length

§4.9's `--subscriber` has `Cold` and `Snapshot`, as the pre-measurement had, and `Warm` as the bound.
An engine's replay buffer is 10,000 steps, 70 s at the engine's 7 ms step, which holds a whole 60 s
run, so a restart that asks for the buffer sees every event and is the warm view. The case that
matters, a ring that has rolled (llm-d/llm-d-router#3124), needs a run of several minutes and a
cold subscriber that anchors on the first batch it gets; it is not built.

### 9.17 What is not built in increment 3

- **Fan-out claims staged together** are checked by a node one at a time, which is where the
  residual over-admissions come from.
- **The cancel's own deficit** still reads the real ledger; the router's blind view does not reach
  it.
- **Active-active schedulers.** The shadow is one scheduler's blind view; two live schedulers
  would each need their own, and are Phase 11's.
- **The engine crash, node loss, the crossover and `programs --fault`** (§4.11, §4.12, §4.14).

---

### 9.18 Increment 4: where it landed

`Fault::Engine` and `Fault::Node` in `fault.rs`; in `machine.rs`, `fail_node_streams` fails the
flights, the gangs with any agent on the node and the orphans on it, and returns the node's engine
queue to the router; `Hierarchy::crash_engine` drops the GPU and host-offload KV and the spill if
asked, and `lose_node` drains everything and lists the durable cells. An engine crash puts the node
out of the decode pool until its restart, and a node loss marks it lost until its declaration, when
it leaves placement for good. A request placed on a lost node before then is parked (`Limbo`) and
returned to the router at the declaration with its original arrival. `--copy-durable` records a copy
when a cell is marked, charged its bytes, and a node loss counts the cells it took apart from those
copied.

### 9.19 A gang dies with any of its agents' nodes

An engine crash aborts every gang with an agent on the node, all its agents, and retries it whole:
the 34-42 streams a crash fails are the node's own streams and the siblings those gangs' other
agents had on other nodes, which is why the figure is above a quarter of the 110 streams in flight.

### 9.20 The lease parks single requests, and fan-outs and tools see the dead node at once

Only single requests are parked. A fan-out's agents and a tool call skip a lost node from the moment
of the loss, so the lease's cost to fan-outs is understated, and at 0.75x with a 40 s grace the
parked quarter of the traffic is load the three live nodes do not carry: 5 / 21 / 16 fan-outs are
refused against 28 / 44 / 42 with no fault, and 46-53 fewer requests are unserved.

### 9.21 Suspicion is a delay, not a detector

`NodeLoss::declare_ns` stands for the router's detection: 0 for acting on the first failed dispatch,
2 s for a few silent steps (Phase 4's episodes), 10-40 s for a lease. No mechanism decides it, and
under `--fleet` the planner does not yet re-place the lost node's replicas after the declaration.

### 9.22 What is not built in increment 4

- **Durable cells in a program.** The count and the copy are unit-tested; running `programs` with
  a node loss is `programs --fault`.
- **Re-placement under `--fleet`** after a declaration.
- **The crossover section** (§4.14, section 11) and the sidecar's fail-open window.

---

### 9.23 Increment 5: where it landed

`Fault::Degrade` switches the router to hash-only placement with no flow awareness and no peer fetch
for a window and restores what it saved, aborting nothing: it is the sidecar's fail-open.
`programs::Config` gains `fault` (an instant and a fault) and `copy_durable`, a program driver event
injects the fault, and `Outcome` carries the fault's counters and the number of programs left
holding a lost durable sandbox. `durability` gains `crossover` (the table of §1.11, built) and
`durable` (section 12).

### 9.24 A copy has to be sized when a cell is marked

The first copy looked a cell's size up when it was marked, and a cell marked while it was not
resident was not copied: with copying on, 6 of 8 durable cells were still lost. A mark now carries
the cell's size, from the program's sandbox or the tool request's chain, and the unit test that
found it asserts that with copying on a lost node loses none.

### 9.25 The fail-open window costs a tenth of what the arithmetic said

§1.11 priced the sidecar's failover as 15 s of Phase 6's `hash only` penalty at 500 req/s on eight
nodes, 16-32% of service, which came to 340-850 request-seconds. At 250 req/s on four the ledger's
own lead of scored over hash is 4-6% end to end, and the window's extra service is 12-53
request-seconds. P9's conclusion moves with it (above).

### 9.26 What is not built, and what the phase leaves open

- **Re-placement under `--fleet`** after a declaration, and detectors for the declaration's delay.
- **A replaying subscriber**, and a ring that has rolled (§9.16).
- **Active-active schedulers**, and a scheduler per region: Phase 11's.
- **The FoundationDB commit rung** (`--features fdb`), which stays optional.
- **Fan-outs parked on a lease** (§9.20), so the lease's cost to fan-outs is understated.

### 9.27 The review: the driver's clock ran on after a retry

A review of the phase found that `drive_with` submitted each new arrival one interval after the
machine's clock rather than at its own slot. A released retry moves that clock to its retry time, so
every retry released late pushed the arrivals after it later: after a 15 s outage with backoff the
trace ended at 63.3 s instead of 60.1 s, and the recovery saw less load than the trace offers. A
burst moves the clock by at most one interval, once. The pre-measurement's driver did the same, which
is why the held rows reproduced it to the digit (§9.10). With a fault planned, a new arrival is now
submitted at its slot, and every restart arm ends at 60.08 s as the run without a fault does. A run
with no outage cannot move, and `estimators`, `engine`, `node` and `durable` are identical between
the two builds; `restart`, `routing` and `crossover` were run again.

Only backoff moved beyond the noise. It costs 7-32% more than a burst rather than 2-28%, and leaves
9-18% fewer requests unserved rather than 26-58%, so §9.11's trade holds but backoff buys less than
it seemed to. Every other figure moved within its noise: a 1 s outage with the streams held costs
126-139 request-seconds against 112-143, streams that die 63-75 at 0.1 s and 284-370 at 1 s
against 62-75 and 284-383, and the crossover's hours at 1.0x of the grant are the same to a tenth
except the held 0.1 s takeover's, 51-87 s against 43-74 s. The predictions keep their verdicts; P3,
P5, P6 and P9 give the re-run beside the first.

The review found three more things:

- **`router held` counted twice.** It was counted after a shared restart had returned the aborted
  streams and the engines' queues to the router, so it repeated `aborted`; it now counts what was
  queued before the restart, which is none in every arm here.
- **The snapshot's bytes were the histograms' alone,** though it restores the observed means, the
  flow templates and the attained service as well (§9.13). It now counts all of them. In
  `durability` the templates and the attained service are empty -- the first fill only with learned
  hints, the second only under the PLAS order -- so the snapshot stays 32 KiB.
- **Five fixes that move no published figure.** Unscored placement's flow shortcut skips an engine
  inside its crash restart and a node declared lost; `warm_cell` avoids a lost node before it is
  declared; a lost node's lease heap is cleared, so its leases do not expire as breaks; a retrieval's
  lead is the leading run of resident blocks, not a binary search on a predicate that is not a
  prefix; and the durable copy's rate is divided by the programs' nodes, not `durability --nodes`.
  The run that sizes a partition's grant never carries a fault, and `durability`'s default sections
  now include `engine`, `node`, `crossover` and `durable`.

Two findings are left as they are: a `FullFlush` on Linux is `fsync` (§9.2), and `--batch` requests
advance the counter that names fresh requests' blocks, which renames fresh traffic when both are on
without changing its shape.

---

## 10. Verification, as run

Increment 1.

- **Byte-identity with every new bit off:** `residency` (twice, the second with `--engine-cache
  --decode-kv`), `flows`, `placement`, `volatility`, `ownership`, `price`, `belief --sections gate`,
  `influence --seeds 1`, `fleet --seeds 1`, `enforce --seeds 1`, `programs --sections gate,logged`
  and `distributed --crossing native --engine-cache --decode-kv --admit perfect --repeat 1
  --distances rack`, all at `--ops 3000 --seed 2`, produce output identical to the build before this
  phase (`aeeb943`). `distributed` carries two lines of control-plane share that depend on live host
  timing (`warm faas` and `warm service`) and differ between two runs of the pristine build, 0.34% and
  0.27% on the first line; the rest of it is identical.
- **The gate,** `durability` section 1: `--count-writes` against off at 1.0x and 0.75x of the grant
  and on the published ledger run -- identical totals, service and stall, identical owned changes,
  and KV events counted only when on.
- **Tests,** 11 new, 272 in all: every KV event lands in exactly one slot by type and tier; the
  owned changes exclude the inferred stream and the record; liveness is a write per node per renewal;
  counting events changes no cost and each tier's stores less its removals is what it holds; every
  reservation is released and every flight ends closed or cancelled; a machine that enforces nothing
  writes no reservation, flight or queue entry; the flow graph counts each insert and each removal
  that finds something; each commit appends its payload once per iteration; measuring leaves no file
  behind and reports ordered percentiles; a missing directory is an error; percentiles read the
  sorted latencies.
- **The census** is 13. `cargo fmt --check` is clean, and `cargo clippy --all-targets` shows only the
  three `assert_is_empty` warnings in lines this phase did not write (`cache.rs`, `fleet.rs`,
  `stream.rs`).
- **Reproducibility:** `durability --sections gate,count,logged,fleet` run twice is byte-identical;
  the rung varies with the host, as the ladder does.

Increment 2.

- **Byte-identity with every new bit off:** the same thirteen commands, all identical to `aeeb943`.
  `distributed` was identical on this run; its two timing-dependent lines are the same ones as above.
- **The gate,** `durability` section 1, adds: `--track-flights` against off at 1.0x and 0.75x of the
  grant (identical totals, served counts, service and stall); a fault scheduled past the trace's end
  against no fault, both armed; and `--observe completion` where no claim reads a length (an
  `--admit perfect` arm) against dispatch -- all identical.
- **Tests,** 12 new, 284 in all: registering every decode as a flight changes no request; a shared
  restart aborts every stream and gang, leaves no reservation, pinned block or batch slot, and every
  request still closes once with every reservation released; a held restart aborts nothing and a
  continuation keeps the tokens decoded; the router serves nothing while down and everything when it
  is back; an aborted decode releases the belief's pin; an estimator reset sends every claim back to
  `max_tokens`; a length is observed when its decode ends and never if it is aborted; a cold scheduler
  does not know residency it did not dispatch and a warm one does; an unpinned belief hold stops
  certifying its blocks and expires without a second release; a burst retries at the outage's end; a
  backoff doubles from its base until it clears the outage; an outage is a scheduler's and an
  estimator reset has none.
- **The census** is 13; `cargo fmt --check` is clean; `cargo clippy --all-targets` shows only the
  three `assert_is_empty` warnings.
- **Reproducibility:** `durability --sections restart` is seed-deterministic like the other
  sections, and takes about 80 s for three seeds at two partitions.

Increment 3.

- **Byte-identity with every new bit off:** the same thirteen commands identical to `aeeb943`, bar
  `distributed`'s two timing-dependent lines on the run that differed (0.27% and 0.29% on the
  first).
- **The gate,** `durability` section 1, adds: `--node-check` and `--snapshot-estimators 10` with no
  fault, against off, at 1.0x and 0.75x of the grant -- identical costs, with snapshots written.
- **Tests,** 5 new, 289 in all: a blind ledger admits what the real one would refuse and a node check
  refuses it, and a rebuilt ledger sees it; a refused dispatch returns to the router and every request
  still closes once; the blind view ends when the pre-restart decodes have ended or at its deadline; a
  snapshot is written on its cadence and a restart restores what it held, and a lost one sends every
  claim back to `max_tokens`; taking snapshots changes no cost.
- **The census** is 13; `cargo fmt --check` is clean; `cargo clippy --all-targets` shows only the
  three `assert_is_empty` warnings.
- **Runtime:** `routing` and `estimators` each take several minutes on this host, with five seeds at
  the overloads and three elsewhere.

Increment 4.

- **Byte-identity with every new bit off:** the same thirteen commands identical to `aeeb943`, bar
  `distributed`'s two timing-dependent lines on the run that differed (0.27% and 0.28% on the first).
- **The gate,** `durability` section 1: `--node-check`, `--snapshot-estimators 10` and
  `--copy-durable` together with no fault, against off, at 1.0x and 0.75x of the grant.
- **Tests,** 5 new, 294 in all: an engine crash takes one node's streams and KV and its decoding
  until it restarts, and its host work stays placed; a crash that loses the spill loses every tier and
  one that keeps it loses two; a lost node parks what is placed on it until it is declared and then
  leaves placement; a node declared at once is never parked on; losing a node counts its durable
  cells and a copy made when they were marked saves them without calling a lost node a dropped cell.
- **The census** is 13; `cargo fmt --check` is clean; `cargo clippy --all-targets` shows only the
  three `assert_is_empty` warnings.

Increment 5.

- **Byte-identity with every new bit off:** the same thirteen commands identical to `aeeb943`, bar
  `distributed`'s two timing-dependent lines on the run that differed.
- **Tests,** 3 new, 297 in all: a degraded router places by hash for its window, restores its
  placement and aborts nothing; a lost node takes the durable sandboxes it holds unless they were
  copied, and counts the programs left with lost state; copying durable cells changes no program and
  charges their bytes.
- **The census** is 13; `cargo fmt --check` is clean; `cargo clippy --all-targets` shows only the
  three `assert_is_empty` warnings.

The review (§9.27).

- **Re-run:** `restart`, `routing` and `crossover` on the corrected driver. `estimators`, `engine`,
  `node` and `durable` are identical between the builds before and after the review, and so is
  `programs` with every section.
- **Byte-identity with every new bit off:** the same thirteen commands identical to `aeeb943`, bar
  `distributed`'s timing-dependent lines, which two runs of `aeeb943` also disagree on (`warm faas`,
  `warm service` and the two shortest hypotheticals).
- **The gate,** `durability` section 1: every line identical.
- **Tests,** 1 new, 298 in all: a snapshot counts the bytes of everything a restart restores.
- **The census** is 13; `cargo fmt --check` is clean; `cargo clippy --all-targets` shows only the
  three `assert_is_empty` warnings.

