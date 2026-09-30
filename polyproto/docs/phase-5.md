# Phase 5 -- Influence: retention directives, prefill-ahead, and divergence by cause

Implementation plan for Phase 5 of [`owned-and-observed.md`](owned-and-observed.md): the router gets
the two ways it has of acting on memory it does not allocate. The first is §3.3's: an
RFC-0001-shaped directive -- retain until a deadline, or evict first -- attached to the requests it
dispatches, against an engine that honours it and one that ignores it, and believed only when the
engine's own stream acknowledges it. The second is one Phase 3 already allowed and no phase has
used: dispatching the work that produces state -- a prefill of a declared downstream's prompt, sent
ahead of the request that needs it. Owned classes get §3.3's vocabulary as decisions rather than
requests (`Entry.retain_until` and `evict_first` in place of the unbounded `expect` bump). And
divergence is split by cause (§3.6), which `phase-4.md` §9.3 left unbuilt.

**Status: implemented and measured.** The marks are `Mark` and `Rank` in [`stream.rs`](../src/stream.rs)
(carried on `KvEvent::Stored`, kept by `Index`) and the ranked victims, expiry and pressure rule in
[`engine.rs`](../src/engine.rs); the directive, expiry and prefill API on `Hierarchy`, `announce`'s
retain and prewarm halves and the ledger's deadline are [`cache.rs`](../src/cache.rs); causes, batch
fates and acknowledged and trusted marks are [`belief.rs`](../src/belief.rs); emission, prefill-ahead
and the two targets are [`machine.rs`](../src/machine.rs); `Retention`, `Origins` and
`flow_downstream` are [`work.rs`](../src/work.rs); the ceilings' trace index is
[`foresight.rs`](../src/foresight.rs); the instruments are
[`instruments.rs`](../src/instruments.rs); `polyphonic influence`
([`influence_cmd.rs`](../src/influence_cmd.rs)) runs §4.10's sweeps on seeds 1-3 with no control
crossing charged, so every number below is reproducible from the seed (15,000 ops, the
`distributed` cluster). §2's eight predictions are annotated with what was measured, and §9 records
what the build found that the plan did not anticipate. Three things did not get built as planned:
`code-review` takes none of the new flags (nor Phase 4's), the instruments table's peer-fetch count
(a prefilled block is never fetched by a peer, so there is nothing to count), and §3 rule 1's second
clause is checked at the engine and not on the cluster (§9.4).

Before the results, the plan's own text. §2's predictions are stated before the run, per
`owned-and-observed.md` §7, and like Phase 4's they lean on **pre-measurements**: numbers taken on
an instrumented copy of `8beb019`, run outside the repository and not committed. They are labelled
wherever quoted and collected with their configurations in §8. They are reasons to predict, not
results: §4.9 rebuilds each instrument in the repository, and a pre-measurement the built
instrument does not reproduce is reconciled before any prediction resting on it is graded.

Three things make this phase unlike Phase 4.

- **Its premise does not survive its first pre-measurement, and the plan starts there.** §9: KV
  prewarm carried half or more of `announce`'s margin, "and a directive is the only honest way left
  to buy it back". Split in two, `announce`'s KV margin was all prewarm -- admitting blocks that
  were not resident -- while retention -- raising the priority of blocks that were -- carried
  between -5% and +9% of it across three seeds (§1.2). A retention directive can only do the
  second. The first can be done by dispatching the work that produces the blocks, which needs
  nothing from the engine (§1.3). So the phase measures two channels of influence, and the one the
  design named is predicted to be worth almost nothing on this workload.
- **The small answer is bounded from above, not only observed.** Phase 4's pre-measurements
  predicted a small belief cost and the build confirmed it. Here they also bound what any directive
  could have bought: an emitter that knows every block's true next use -- impossible, and printed as
  a ceiling beside the clairvoyant block manager -- moves stall on the cluster by at most about
  1.4%, and costs where the partition is short (§1.10). Phase 7's gap estimator inherits that
  ceiling.
- **It completes an instrument rather than adding one.** Divergence by cause was specified in
  Phase 4 and not built. It lands first, before anything in this phase can move a number, and
  measures Phase 4's belief as published -- exactly, because the simulator knows every batch's fate
  (§1.9).

§1 settles twelve decisions. Seven are findings about the existing documents rather than about the
work ahead: §1.2 (`announce` was prewarm), §1.3 (the engine does let the orchestrator prewarm, by
dispatch), §1.5 (RFC-0001's pressure rule evicts the most valuable hold when deadlines come from
next use), §1.6 (a directive rides on a request, so a flow's downstream has nothing to ride on),
§1.8 (an ignored directive is refused in the stream), §1.9 (Phase 4's belief strands optimistic
entries that no later removal can clear), and §1.10 (session reuse sits past `sched_lm`'s E1
boundary by shape and does not matter by size).

---

## 1. What has to be settled before the orchestrator asks the engine for anything

### 1.1 Influence follows authority

Phase 4's rule 2 was that observability follows authority: a read becomes a belief exactly where
`own::authority(kind, tier, Allocation)` is `Engine` *and* the simulator has moved the decision.
Influence follows the same table, and it splits retention in two.

| `own::authority(.., Allocation)` | classes, as the simulator stands | retention is | applied by | can be ignored |
|---|---|---|---|---|
| `Orchestrator` | `Snapshot`, `ServiceHeap` | a **decision**: `Entry.retain_until`, `Entry.evict_first` | the ledger | no |
| `Engine`, moved by Phase 3 | `KvBlock` under `--engine-cache` | a **directive** on a dispatched request | the engine, if it honours it | yes -- the `--ignores` arm |
| `Engine`, not moved | `WeightShard`; `KvBlock` with `--engine-cache` off | a decision, because the ledger still allocates it -- for weights until Phase 6 makes loading an orchestration decision | the ledger | no |

Three scope lines follow.

- **`--directives` requires `--engine-cache`.** On the ledger the orchestrator allocates the KV
  itself, and a directive to itself is a decision -- §1.11's `--retain`.
- **Weights keep the ledger's form**, for the reason `phase-4.md` §1.1 gives; and no hint's
  downstream carries a weight anyway, since `FlowHint.downstream` is the chain and never `requires`.
- **Dispatch is not in the table**, because it is neither a decision nor a directive: a
  prefill-ahead is a request, and what it leaves resident is whatever the engine's allocator
  decides (§1.3).

### 1.2 `announce` was prewarm, and a directive cannot prewarm

`Hierarchy::announce` does two things to a flow's downstream state, and Phase 3 measured them
together. For state already resident it raises `expect` -- **retention**, a priority bump. For
state that is not, it admits it into free space and then raises it -- **prewarm**, a write.
`phase-3.md` §1.8 removed both for `KvBlock` and found that KV had carried half of the margin (71%
on unified memory). Which half was never asked, and the answer decides this phase: a retention
directive can only ever do the first.

The pre-measurement split them (§8, *flows-split*): `flows --engine-cache` with `announce`'s KV half
reduced to each half alone on the ledger, and, on the engine's side, the downstream's resident
blocks held until it arrives -- a hold the engine honours -- or its missing blocks prefilled ahead
of it (§1.3).

| task-latency margin over `blind` | split, seeds 1 / 2 / 3 | unified, seed 1 |
|---|---|---|
| ledger: `announce` as published | 10.7 / 10.6 / 10.3% | 17.6% |
| ledger: KV retention only -- bump what is resident, admit nothing | 5.4 / 4.5 / 4.9% | 5.2% |
| ledger: KV prewarm only -- admit what is not, bump nothing | 10.4 / 11.3 / 10.9% | 16.2% |
| ledger: no KV at all (`host-only`) | 5.3 / 4.8 / 4.5% | 5.1% |
| engine: `announce`, KV skipped (published) | 9.0 / 9.4 / 9.0% | 3.2% |
| engine: + the downstream's resident blocks held until it arrives | 9.1 / 9.4 / 9.0% | 3.3% |
| engine: + the downstream's missing blocks prefilled ahead of it | **24.4 / 24.5 / 24.5%** | **28.4%** |

Retention-only lands on `host-only` in every column: it carried between -5% and +9% of the KV
margin, zero within the seeds' spread. Prewarm-only carries all of it. The same split over owned
state gives the same answer -- the `Snapshot` bump is worth 0.01 ms of task latency and admitting
the cell 1.43 ms (§8, *host-split*) -- so retention on a declared flow is worth nothing on either
side of the ownership boundary. The mechanism is the lead: a flow's downstream arrives six requests
after its hint, and almost nothing resident is at risk of eviction inside six requests.

**Chosen: the phase's first claim is a retraction.** "A directive is the only honest way left to
buy it back" is wrong twice over: a directive cannot create a block, and something else can.

### 1.3 The other channel is dispatch, and the engine does let the orchestrator prewarm

`phase-3.md` §1.8 said it in passing: after the correction "the orchestrator can cause a KV block to
exist only by dispatching work that produces it". That is a mechanism, not a restriction to route
around. A prefill of the downstream's prompt, dispatched when the hint is issued -- a prefill-only
request, the shape llm-d's disaggregated handshake already sends with `max_tokens=1` (§2.1) --
makes the engine allocate the blocks by its own rules, and the downstream arrives to find them
resident. **Prefill-ahead** needs nothing from the engine that any request does not already get.
It is the dispatch channel's twin of a retain directive, and it does what the directive cannot.

The last row of §1.2's table prices it on one node: **24.4-24.5% of task latency on split memory
and 28.4% on unified, against the ledger's own `announce` at 10.3-10.7% and 17.6%**, with net work
within 0.06% of `blind` (286.85 s against 286.69 s) -- the prefill moved off the critical path, not
added. It beats the ledger because the ledger's prewarm admitted only into free space
("prewarming never preempts", `residency-ledger.md` *Flows*), so it prewarmed when there happened to
be room. An engine never refuses, so a prefill always lands and displaces the LRU tail. The
displaced blocks' later rebuilds are in the stall column, and the column still falls.

On the cluster (§8, *prefill*), `scored + fetch` at rack with every FaaS->inference prompt prefilled
at its hint on the downstream's scored argmin (§1.4):

| seeds 1 / 2 / 3 | published defaults | half partition, decode output held |
|---|---|---|
| flow downstream's stall | 9.11 -> 3.30 / 9.58 -> 3.49 / 9.15 -> 3.34 ms | 11.13 -> 6.02 / 11.53 -> 7.15 / 10.86 -> 5.17 ms |
| total stall | -4.1 / -4.1 / -3.6% | -2.9 / -1.5 / -5.0% |
| mean service | -0.11 / -0.12 / -0.11% | -0.13 / -0.04 / -0.16% |
| prefill-ahead's work against the stall it saves (seed 1) | 13.5 s for 8.7 s | 13.5 s for 7.3 s |

The work premium is the price of guessing where the downstream will land (§1.4): a prefill that
misses is either fetched from, at a cost the downstream pays, or wasted.

Three limits:

- **Prefill is not a resource in the engine model.** A rebuild is charged as latency to the request
  that needs it and, here, as work to the prefill-ahead -- never as step time -- so neither arm
  charges the engine for it. The comparison is exact about *when* the prefill runs and silent about
  whether an engine has room for it. Phase 6 splits prefill from decode and makes it a resource.
- **Not every downstream qualifies.** Only a downstream whose content exists at the hint can be
  prefilled. A FaaS->inference prompt does -- the function composes the call when it decides to make
  it. A fan-out's resume does not: its result blocks are the agents' outputs.
- **It is not speculative here.** Every declared flow arrives (`FlowHint.probability` is 1.0). Phase
  7 makes the hint an estimate, at which point a prefill-ahead is speculative dispatch and
  `authority` gates it (`owned-and-observed.md` §4); §1.11's false hints price what a wrong one
  wastes.

**Chosen: `--prefill-ahead`, a bit of its own, in `flows` through `announce`'s engine branch and on
the cluster through the request path.** Its work is the acquisition the downstream would otherwise
have made at arrival, charged where it happens; its displacement is the engine's to decide.

### 1.4 A prefill-ahead lands where the router guesses, and distance prices the guess

On the cluster the downstream is placed when it arrives, by the same scored argmin as every other
request, and the prefill has to go somewhere first. The pre-measurement tried three targets (§8,
*prefill*): the node with the deepest believed prefix of the downstream's chain, where the
downstream landed **40-44%** of the time (seed 1, both regimes); the downstream's own scored argmin
at the hint -- its declared chain, the observed mean output length, the handoff from its upstream --
which raised landing to **47-51%**; and that argmin executing its acquire plan, fetching from a peer
where the plan would, which changed nothing measurable. Twenty-four milliseconds later the load has moved,
and the argmin moves with it.

A prefill that misses still helps at rack, because the node it warmed is one fetch away and
`scored + fetch` pulls from it. Across distance that stops being true (seed 1, published defaults,
deepest-prefix target):

| distance | flow downstream's stall | total stall | landed |
|---|---|---|---|
| rack | 9.11 -> 3.75 ms (-59%) | -3.2% | 40% |
| zone | 11.05 -> 7.97 ms (-28%) | -1.1% | 40% |
| region | 23.88 -> 21.63 ms (-9%) | -1.6% | 54% |

This is `residency-ledger.md`'s crossover -- KV ships within a rack and is rebuilt across a zone --
reached from the other side: a prefill that missed is a rack fetch, and past a rack it is waste.

**Chosen: the target is the downstream's scored argmin at the hint, and landing is an instrument,
not a constraint.** The hint does not carry the downstream's `requires`, so the argmin prices the
chain and the handoff and not the model; that is one of the things a landing rate under 100%
absorbs. Binding the downstream to its warmed node would make every prefill land and give up the
downstream's reaction to load in exchange -- a joint placement of two requests, coupling tier 2,
the same decision as Phase 6's P/D pairing over a prefill and a decode. Named, not built.

### 1.5 The directive: RFC-0001's semantics, and the choices it leaves open

Read against `sched_lm`'s RFC-0001 (`docs/rfc-0001-kv-cache-priority-directives.md` there) and the
upstream RFC it aligns with, vllm#37003:

- **Vocabulary.** Retain, RFC-0001's `50; ttl=<window>; scope=<id>`; and evict-first, its `-1`. The
  router never emits `pinned` (100), which RFC-0001 reserves for framework directives with a
  mandatory, server-capped lease.
- **Rank.** Evict-first < unmarked < retained, LRU within a rank. Leaf-first still holds, so a
  marked block with resident children is a candidate in no rank.
- **Deadline.** Wall clock from receipt, on both marks, since RFC-0001's leases apply at every
  priority; expiry returns the block to unmarked, with no message. A retain's deadline is its
  declared lead. An evict-first mark takes RFC-0001's default lease, 30 s -- the engine's policy,
  not a constant of ours -- which outlives every one-shot block on this workload.
- **A shared block** carries the latest live deadline of the directives covering it.
- **A touch without a directive leaves a live mark alone.** RFC-0001 resets it to unmarked; #37003
  lets any scope escalate and only the owning scope downgrade. With one issuer, the reset rule would
  make the router re-send every mark on every dispatch that touches a shared block -- the tenant
  prefix is touched by every turn of every session of a tenant -- so the model takes #37003's.
- **Soft under pressure.** If every candidate is marked and live, evict the soonest-expiring and
  count a pressure eviction (RFC-0001 §1, §4). A mark therefore never causes a preemption.

One consequence neither RFC discusses. When a deadline is a predicted next use -- the oracle's is
exactly that, and a gap estimator's would be -- the soonest-expiring hold is the block needed
soonest, which is Belady's worst victim. Pre-measured, the rule fires where holding more than the
partition's evictable share is already the mistake: no pressure evictions for the declared emitter
at the published partition, 20.6k at half of it, and 106-111k for the oracle at a 30 s horizon
(§8, *ceiling*). **Chosen: RFC-0001's rule as specified**, because it is what an engine would ship;
latest-expiring is the named alternative and is one line.

**Acknowledgement is RFC-0001 §4's.** An honoured mark is echoed on the engine's stream as the
`BlockStored` event's trailing `priority` and `retain_until` fields, re-emitted when the mark
changes -- no new event type, because the deployed indexer fails a whole batch on an unknown tag. In
the model, `KvEvent::Stored` gains an optional mark, and Phase 4's stream invariant extends to it:
replaying the stream reproduces every tier *and every live mark*.

### 1.6 A directive rides on a request, so it governs what that request leaves behind

Both RFCs attach a directive to a request -- a header on the router's path, `extra_body` on a
framework's -- and it governs that request's blocks once the request has run. Three consequences.

1. **A FaaS->inference downstream has nothing to ride on.** Its upstream is a function, not an
   engine request, and the prompt's blocks were last touched by the previous invocation of that
   function seconds earlier (§1.10). The only request that could carry a retain for them is a
   prefill-ahead, which makes the retain redundant: 24 ms of lead against more than two seconds of
   median residency.
2. **A fan-out's parent chain can ride on its agents.** Each agent reads the orchestrator's context,
   so each agent's dispatch can retain that context until the resume's declared lead. Pre-measured
   (§8, *declared*): 57k marks and nothing measurable at the published partition (stall -0.05% to
   +0.08% on three seeds), and a cost at half of it (+0.2% to +1.0%, 20.6k pressure evictions),
   because the parent chain is a tenant prefix and the session's own turns, and it is resident
   anyway.
3. **A standalone "pin these blocks now" is in neither RFC.** The llm-d agentic-runtime vision's
   move/pin/evict control plane would add one; it is not modelled. §1.2's engine hold emulated one
   at the hint anyway, and it bought 0.0-0.1pp.

### 1.7 What the router may know when it emits

Declared information only (rule 5):

- **A flow's downstream and its lead.** `FlowHint` carries the chain and `lead_ops`, and the
  deadline is the lead at the arrival rate -- the expected time a framework would declare.
- **A declared retention scope.** `owned-and-observed.md` §4's `retention` field, whose first value
  this phase gives a consumer: `evict_first` over a gang agent's private suffix and its output, and
  over a queued downstream's output. The workload makes these one-shot by construction -- every
  `agent`, `agentout` and `out` block is used once (§8, *reuse*) -- and a framework plausibly knows
  that its scratch is scratch: RFC-0001's own motivating case is an intermediate reasoning branch.
  `until(deadline)` stays with the hint, and `durable` with Phase 7's sessions.
- **Nothing learned.** A session's next turn is declared nowhere, so no Phase 5 emitter retains a
  session prefix. The re-arrival gap is `sched_lm`'s `ToolGapIndex` and Phase 7's.

The **oracle emitter** is the exception, and labelled as one wherever it prints: on every dispatch
it retains each block until its true next use, read from the trace, if that falls within a horizon
of 2, 5 or 30 s. Like the clairvoyant block manager, it is a ceiling -- what a perfect gap estimator
could buy through a directive -- and never an arm (§1.10).

`Request::retention` is declared the way `slo` is: generator-side truth, scheduler-side
declaration, deterministic from the request's structure, so the trace hashes identically whether or
not anything reads it.

### 1.8 An ignored directive is refused in the stream

§3.6 lists "ignored retention directives" as a source of divergence, and `phase-4.md` §7 said the
`ignores` arm would be read through `--belief`. But the belief's index changes only on events, and
an ignored directive changes no event: the engine evicts the block as its LRU would and publishes
the removal like any other. An ignored directive cannot make the index wrong. A block still believed
after an ignored directive failed to hold it is a block whose removal has not been delivered -- not
yet due, dropped or silenced -- and §1.9 attributes it there.

What a directive can change is **confidence**. A router that treats its own retain as a pin --
`P(resident) = 1` until the deadline -- is over-confident about precisely the blocks an ignoring
engine evicted, for as long as their removals are undelivered: a step and a hop under replay, and
until the deadline when a batch is lost for good.

**Chosen: the router believes a mark only when the stream acknowledges it (`--marks acked`).** A
live, acknowledged retain is certain until its deadline, as a block the router's own in-flight
sequence pins already is, and leaves the turnover denominator `B`. An unacknowledged directive
changes nothing the router believes. Under `--ignores` the acknowledged belief is then
byte-identical to directives off -- part of the gate -- and §3.6's third source exists only on the
comparison arm, `--marks trusted`, which believes its own requests. The `ignores` arm stops being a
divergence experiment and becomes the planning number §9 asked for: what routing achieves when the
serving stack does not cooperate.

### 1.9 Divergence by cause, exactly -- and the cause Phase 4's belief creates itself

`phase-4.md` §9.3: phantom and miss shares are measured and their causes are not, because the split
"needs per-batch fates the channel does not keep". The channel decides every fate as it happens -- a
batch is lost in `Observer::lost`, silenced by an episode, delivered in `deliver_due`, repaired by
`on_replay` or `on_snapshot` -- so keeping them is bookkeeping. The publisher records, per block,
the sequence number of the batch that last stored it and of the batch that last removed it from the
GPU; the channel records each sequence number's fate.

At each divergence sample a **phantom** -- believed in the GPU and absent -- is attributed to exactly
one cause:

| cause | the block | cleared by |
|---|---|---|
| not yet due | stored and removed; the removal's batch is open, sealed or in transit | the next delivery |
| dropped | the removal's batch was lost and is not yet repaired | replay, a snapshot, or nothing |
| silenced | the removal's batch fell inside a silence episode | the episode's replay |
| never stored | an optimistic dispatch the engine did not store -- preempted or refused -- whose window is unreconciled | its window's batch |
| stranded | an optimistic dispatch the engine *did* store, whose store's batch was lost; the block's later removal finds nothing in the index and leaves the optimistic entry | nothing |

A **miss** -- in the GPU and not believed -- takes the same undelivered-store causes, plus Phase 4's
standing case: re-dispatched while its eviction was undelivered (`phase-4.md` §9.6).

The last phantom row is new. `Belief::apply` removes a block from the index on a removal event and
does not touch an optimistic entry, so under no recovery a store whose batch is lost leaves an
optimistic entry that the block's eventual removal cannot clear. The pre-measurement split each
phantom by whether the belief holds it as an optimistic entry or as an index entry (§8, *causes*;
its phantom shares reproduce Phase 4's published ones):

| phantom share, and the optimistic part of it | replay 0 / 5 / 20% | periodic 5% | none 5% | none 20% | 2 s silence |
|---|---|---|---|---|---|
| published defaults | 0.069 / 0.082 / 0.132% | 0.80% | 15.1% | 36.6% | 0.78% |
| of which, optimistic | 0% | 0% | 0.9% | 7.5% | 0.1% |
| half partition, decode output held | 0.20 / 0.23 / 0.37% | 2.1% | 31.9% | 59.6% | 1.7% |
| of which, optimistic | 16.8 / 17.5 / 19.2% | 21.0% | 13.1% | 20.2% | 41.1% |

Under replay every store is eventually delivered, and under periodic recovery a snapshot reconciles
every optimistic entry it covers, so an optimistic phantom there is a sequence the engine never
stored -- preemption, present at half the partition at every loss rate. At the published partition
nothing is preempted, so every optimistic phantom under no recovery is stranded: 1-8% of that
column's phantoms, which is `phase-4.md` §9.6's second standing limitation with its size.

**Chosen: the instrument attributes and does not repair** (rule 9). Clearing an optimistic entry on
a removal is one line and a deployment could do it, but it moves Phase 4's published no-recovery
column, and a correction to a published number belongs in its own A/B, not inside a measurement of
influence (§7).

### 1.10 Where retention could pay: past E1's boundary by shape, under 2% of stall by size

`sched_lm`'s E1 (`docs/experiment-proposals.md` on its `experiments` branch) is the one experiment
either prototype has run in which directives beat LRU, and it found a boundary: retention wins
where a prefix's re-arrival gap exceeds LRU's residency, and nowhere else. Its E2 found that only a
hold captures that win -- promotion hints and SLRU captured none -- and its RFC phase-5 evidence
found retention inert below the boundary, where LRU already keeps what the router would pin.
Evidence, not direction (`owned-and-observed.md` §6): its TTFT is a fixed latency model, and its
gateway ignores the picker's endpoint choice.

This workload's place relative to that boundary is measurable (§8, *reuse*, `scored + fetch`, rack,
session blocks):

| | published defaults | half partition, decode output held |
|---|---|---|
| LRU residency, age at GPU eviction, p10 / p50 / p90 | 1.7 / 2.3 / 3.1 s | 0.1 / 1.0 / 1.7 s |
| gap before a hit on the same node, p50 / p90 | 0.2 / 1.7 s | 0.03 / 1.2 s |
| gap before a miss on a block this node evicted, p10 / p50 / p90 | 3.2 / 8.8 / 27.8 s | 1.6 / 6.9 / 27.0 s |
| session block reads: hit / never on this node / evicted from it | 35 / 35 / 30% | 22 / 30 / 48% |
| KV dispatches with an evicted-here miss, all origins | 14.5% | 21.6% |
| what those misses cost, per served request, at most | 0.93 ms: 6.6% of stall, 0.19% of service | 1.59 ms: 9.7% of stall, 0.32% of service |

By shape, sessions sit where E1 found the win: the misses a retention could prevent come back at
nearly four times the median residency. By size they do not matter. The evicted-here misses are the
whole of what retention on the chosen node could save, they are under a third of a percent of
service, and most land in the connector's offload or spill, where a promote costs 71-251 us rather
than a 400 us rebuild. The ceilings agree (§8, *ceiling*, *ephemeral*, three seeds):

| against LRU with no directives | stall, published defaults | stall, half partition | service |
|---|---|---|---|
| clairvoyant block manager (furthest next use, global future) | -0.7 / -1.9 / -0.7% | +1.0 / -2.4 / -0.7% | within 0.05% |
| oracle emitter, 5 s horizon | -1.4 / -1.4 / +0.1% | +0.1 / +1.7 / +0.1% | within 0.07% |
| oracle emitter, 2 s and 30 s (seed 1) | -0.05% and +0.4% | +0.2% and +1.0% | within 0.04% |
| evict-first on declared ephemeral scopes | -0.8 / -1.7 / -0.9% | +1.0 / 0.0 / 0.0% | within 0.05% |

Every row changes sign across seeds or regimes, and none reaches the 2.5-3.1% of stall a
clairvoyant block manager is worth on one node (`phase-3.md` P7). The difference is routing: the
scored argmin already sends a turn where its prefix is, fetch pulls the prefix from a peer when it is
elsewhere, and the offload tier catches a GPU eviction at a fifth of a rebuild. A single node has
none of the three. At half the partition the eviction order barely matters at all, because in-flight
decode output pins most of the partition and leaves the LRU little to choose among.

So what a gap estimator could buy through a directive here -- Phase 7's question -- is bounded at
about 1-2% of stall and a tenth of a percent of service. `sched_lm`'s E1 found far more because its
testbed has none of the three: returns land wherever the gateway sends them, and there is no
offload tier beneath the cache. **Chosen: build the emitter that could reach the bound as a ceiling
and not an arm, and let §2 predict the arms below it.**

### 1.11 A deadline in place of the unbounded bump needs a wrong prediction to matter

§3.3: `retain_until` "is strictly better than the current unbounded `expect` bump: a priority
inflation with no deadline never self-corrects when the prediction was wrong, while a TTL does". Two
facts narrow it on this workload.

- **Declared flows are never wrong.** Every hint's downstream arrives, so the only state a deadline
  could clean up is a bump the downstream's own access did not clear -- and there is one: the ledger
  touches only the deepest hit block of a chain, so a hinted prefix's interior keeps its bump
  indefinitely, a mild frequency prior rather than an error.
- **A wrong hint costs what it prewarms, not what it inflates.** Pre-measured with phantom hints --
  hints for flows that never come, drawn from their own seeded stream so the trace is byte-identical
  -- at rates that make 18%, 50% and 69% of FaaS->inference hints false, a deadline on the bump
  against no deadline moves task latency by -1.9% to +2.6%, in both directions and monotone in
  neither the rate nor the deadline (§8, *false-hints*). Net work rises 0.6%, 3.8% and 6.7% --
  prewarm spent on flows that never came, which no deadline recovers.

**Chosen: build §3.3 as specified -- a soft pin with a deadline, and `evict_first` -- behind
`--retain`, with the false-hint condition beside it (`--false-hints p`).** A bump that outlives its
reason is state nobody owns, and bounding it is right on those grounds; the plan predicts that it is
invisible in latency and says so in advance. The pre-measurement priced a deadline on the bump and a
hard hold on the engine's side, and both were worth nothing; a soft pin sits between them. This is
the one item that can be cut without weakening the deliverable.

### 1.12 Where the effects land

A directive or a prefill-ahead changes what the engine holds, which is truth: the realized-cost
oracle prices it, so its effect lands in realized costs -- service, stall, the flow downstream's
stall -- and not in the regret decomposition's gaps, which compare decisions over one state. The
exception is the belief rule: `--marks trusted` against `acked` changes what the router believes
without changing what the engine holds, so its cost is a `belief` and `execution` gap, graded as
Phase 4 graded its rules. Every directive cell is therefore an A/B on service, stall, the flow
downstream's stall and per-class p99, on three seeds, reported as a range with a count of signs
(rule 10).

---

## 2. Predictions, stated first

`owned-and-observed.md` §7's rule. Eight predictions, each attached to a claim it would rewrite.
Where a pre-measurement stands behind one, §8 says how it was taken.

**P1 -- A retention directive buys nothing where the declared flows are.**

One node: an honoured hold of the downstream's resident blocks within 0.5pp of `announce`'s engine
margin on three seeds and both memory models (pre-measured 0.0-0.1pp). Cluster: retaining a
fan-out's parent chain on each agent's node until the declared resume within ±0.5% of stall at the
published partition, and a cost at half of it -- more than 10k pressure evictions and stall worse on
every seed (pre-measured none and 20.6k; -0.05% to +0.08%, and +0.2% to +1.0%).

- *If right:* §9's "a directive is the only honest way left to buy it back" is retracted, and
  `residency-ledger.md`'s `announce` row loses its last clause.
- *If wrong* on one node: a six-request window evicts resident blocks, which more than two seconds
  of median residency says it cannot, and the probe and the build disagree about the partition -- to
  be reconciled before anything else is published.

**Measured: confirmed.** Section 3 (one node), the engine's hold of the declared downstream's resident
blocks against `announce` with KV skipped: 9.1 / 9.4 / 9.0% against 9.0 / 9.4 / 9.0% on split memory,
3.3 / 3.7 / 3.2% against 3.2 / 3.6 / 3.2% on unified -- 0.0-0.1pp, inside the prediction's 0.5pp.
Section 5 (cluster), retaining each fan-out agent's parent chain until the resume's declared lead,
by seed:

| | published defaults | half partition, decode output held |
|---|---|---|
| stall against off | +0.00 / -0.05 / +0.08% | +0.24 / +1.01 / +0.19% |
| marks honoured, pressure evictions (all seeds) | 181,737 and 0 | 155,546 and 66,990 |
| flow downstream's stall | +1.15 / +0.55 / +0.90% | +0.92 / +0.33 / +2.53% |

Inside ±0.5% at the published partition; worse on every seed at half of it, with 22k pressure
evictions a seed against the predicted "more than 10k". Ignored, the same directives are
byte-identical to none (§5's gate). The retention half of `announce` is worth nothing on either side
of the ownership boundary, and §9's "a directive is the only honest way left to buy it back" is
retracted in `owned-and-observed.md`.

**P2 -- Dispatch recovers more of `announce`'s margin than the ledger's prewarm ever bought.**

One node, `--prefill-ahead`: a task-latency margin of at least 24% on split memory and 28% on
unified on every seed, against the ledger's 10.3-10.7% and 17.6%, with net work within 0.1% of
`blind`. Cluster, rack: the flow downstream's stall down 35-65%, total stall down 1.5-5%, service
down 0.04-0.16%, and prefill-ahead's work 1.5-2 times the stall it saves (1.56 and 1.86
pre-measured on seed 1).

- *If right:* the engine lets the orchestrator prewarm, by dispatch, and a unified orchestrator's
  declared flows are worth more with the engine allocating than on the ledger that allocated for it.
- *If wrong* (below the ledger's margin on one node): the displacement a prefill causes costs more
  than the ledger's free-space rule avoided, and "prewarming never preempts" is a principle rather
  than a restriction.

**Measured: confirmed where it was measured, and two of its edges missed.** `polyphonic influence`
section 3 (one node) and section 4 (cluster, rack), by seed:

| | seed 1 / 2 / 3 |
|---|---|
| one node, split: margin over `blind` with prefill-ahead | 24.4 / 24.5 / 24.5% (`announce` as published: 10.7 / 10.6 / 10.3%) |
| one node, unified | 28.4 / 28.2 / 28.5% (17.6 / 17.9 / 16.0%) |
| one node, net work against `blind`, split / unified | +0.06 / +0.06 / +0.09% and -0.01 / -0.22 / -0.06% |
| cluster, defaults: flow downstream's stall | -63.8 / -63.5 / -63.5% |
| cluster, defaults: total stall, mean service | -4.07 / -4.13 / -3.59% and -0.11 / -0.12 / -0.11% |
| cluster, half partition: flow downstream's stall | -46.0 / -38.1 / -52.4% |
| cluster, half partition: total stall, mean service | -2.95 / -1.48 / -4.94% and -0.13 / -0.04 / -0.16% |
| prefill work against the stall it saves, pooled over seeds | 39.0 s for 26.1 s (1.49x), 38.4 s for 22.4 s (1.71x) |

The margins, the stall ranges and the service range are inside the prediction. Two edges are not:
net work on unified memory is 0.22% *below* `blind` on seed 2, outside "within 0.1%" and in the
favourable direction (the displaced blocks were ones the run would have rebuilt anyway), and the
work premium is 1.49x at the published partition, a hair under the predicted 1.5-2x. Half-partition
seed 2's total-stall saving, 1.48%, sits a hair under the predicted 1.5%. The *if right* branch fires:
the engine lets the orchestrator prewarm, by dispatch, and it buys about 2.3x the ledger's prewarm on
split memory and 1.6x on unified.

**P3 -- Prefill-ahead's value falls with distance, and landing does not decide it at rack.**

Landing 40-55% at every distance and under either target rule; the flow downstream's stall down
about 60% at rack, 25-30% at zone and 10-20% at region; at rack, the saving within the seeds'
spread whichever target is used.

- *If right:* prefill-ahead is a rack-local mechanism unless the downstream is bound to its warmed
  node, and that binding is a coupling-tier-2 decision for Phase 6 to price with P/D pairing.
- *If wrong* (the region saving matches rack): the downstream follows its prefill across a region,
  which would mean acquire outweighs load there -- consistent with the scored arm keeping tool calls
  beside the agent at region distance, and worth stating as the same result.

**Measured: half right.** Section 4, prefill-ahead, by target:

| | scored argmin | deepest believed prefix |
|---|---|---|
| landed, rack defaults / half / zone / region | 49% / 50% / 45% / 48% | 40% / 42% / 40% / 54% |
| flow downstream's stall, rack defaults, by seed | -63.8 / -63.5 / -63.5% | -58.8 / -57.1 / -60.1% |
| ... rack, half partition | -46.0 / -38.1 / -52.4% | -47.7 / -40.7 / -42.2% |
| ... zone / region (first seed) | -30.6% / -7.4% | -27.8% / -9.4% |
| total stall, rack defaults | -4.07 / -4.13 / -3.59% | -3.19 / -3.76 / -4.57% |

The landing range holds (40-55% under either rule), and the value falls with distance -- 64% at rack,
31% at zone, 7% at region -- so prefill-ahead is a rack-local mechanism unless the downstream is bound
to its warmed node. Two clauses missed. The region saving is 7-9%, under the predicted 10-20%. And the
saving is *not* within the seeds' spread across targets at rack: the argmin saves about 5 points more
than the deepest prefix at the published partition, where the seeds differ by 0.3, and the two swap
places at region and are mixed at half the partition. The target is worth a few points at rack and
nothing that generalises, which is the size of what a joint placement of the two requests could add
(§1.4). The zone saving, 28-31%, is at the top of the predicted 25-30%.

**P4 -- No directive emitter buys 2% of stall on the cluster.**

The oracle emitter at 2, 5 and 30 s: at best within -2% of stall and 0.1% of service at the
published partition; worse than no directives at 30 s and at half the partition; at the published
partition, no pressure evictions at 2 or 5 s and more than 100k at 30 s. The clairvoyant block
manager within -2% at the published partition and of either sign at half.

- *If right:* what Phase 7's gap estimator can win through a directive here is bounded at about
  1-2% of stall, and a promotion-tier-2 ask for a directive API has its number -- smaller than the
  ask for a better block manager on one node (`phase-3.md` P7).
- *If wrong* (the oracle beats the clairvoyant manager by more than the seeds' spread): retention is
  changing placement -- the router follows its holds -- and that co-design is a result worth having.

**Measured: confirmed at the published partition, and half wrong at half of it.** Section 6, by seed:

| against LRU with no directives | stall, defaults | stall, half partition | service, both |
|---|---|---|---|
| clairvoyant block manager | -0.66 / -1.85 / -0.69% | +1.03 / -2.40 / -0.74% | within 0.05% |
| oracle emitter, 2 s | -0.05 / -0.07 / +0.11% | +0.16 / -0.67 / -0.25% | within 0.03% |
| oracle emitter, 5 s | -1.41 / -1.37 / +0.11% | +0.12 / +1.65 / +0.14% | within 0.07% |
| oracle emitter, 30 s | +0.40 / +0.40 / +2.07% | +1.01 / -1.86 / -1.45% | within 0.06% |
| pressure evictions at 2 / 5 / 30 s, defaults, all seeds | 0 / 0 / 323,110 | 126,084 / 215,076 / 334,971 | |

At the published partition every clause holds: the best emitter is -1.41% of stall and -0.04% of
service, the 30 s horizon is worse on all three seeds, pressure evictions are absent at 2 and 5 s and
about 108k a seed at 30 s, and the clairvoyant manager is inside -2%. At half the partition "worse
than no directives" holds only at 5 s (all three seeds); 2 s and 30 s are mixed in sign. The *if
wrong* branch (the oracle beating the clairvoyant manager beyond the seeds' spread) did not fire.
What a gap estimator can win through a directive on this workload is bounded at about 1-2% of stall
and 0.1% of service, and the rate of pressure evictions at half the partition (126k-335k over three
seeds, at every horizon) is the RFC's soft rule doing what §1.5 said it would: taking the block
needed soonest.

**P5 -- Evict-first on declared ephemeral scopes is within ±2% of stall.**

One-shot blocks are 13-15% of what the GPU stores (pre-measured), not the scan pollution
`sched_lm`'s E4 needed before demotion won; routing and the offload tier absorb them.

- *If right:* the `retention` field's first value has a consumer and no measurable effect; it stays
  for Phase 7, where `DraftOnly` work is the natural declarer of scratch state.
- *If wrong* (a gain beyond 2% on every seed): one-shot blocks displace reusable ones enough to
  matter, and demotion is the cheapest directive with a number -- the E4 result, reproduced on a
  workload with routing.

**Measured: confirmed.** Section 5, evict-first over the declared one-shot scopes, by seed: stall
-0.76 / -1.67 / -0.87% at the published partition and +0.94 / -0.01 / -0.01% at half of it, service
within 0.05% in both, 30,714 and 56,659 marks honoured over three seeds. Its scope is 7.0% (published)
and 10.3% (half) of what the GPU stores, from section 9's stores column, which is the size of the
pollution it could relieve. The `retention` field's first consumer exists and buys nothing a user
would see.

**P6 -- An ignored directive is invisible to an acknowledged belief and costs a trusting one only
inside the unknown window.**

Under `--marks acked --ignores`, every output byte-identical to directives off. Under
`--marks trusted --ignores`: with replay, service within 0.01% and the top calibration bin within
0.5pp of `acked`; with 20% loss and no recovery, the top bin's realised residency below `acked`'s
and a nonzero `belief` gap, with service still inside Phase 4's 0.16%.

- *If right:* §3.6's third divergence source is a calibration error bounded by delivery, and the
  event stream is the refusal channel a directive API needs -- nothing has to be added to it.
- *If wrong* (`trusted` differs under replay): something other than delivery keeps an evicted block
  believed, and §1.9's stranded entries are the first suspect.

**Measured: confirmed for the acknowledged belief, and the trusting one is over-confident only at half
the partition.** The gate (section 1) is identical on all four cells -- declared and 5 s oracle, both
regimes, 5% loss with replay -- with 98k-307k marks emitted and none honoured. Section 7 first ran under
`face-value` scoring, which never reads `P`, so a trusted mark could not move a placement and every
service column was zero by construction; §9.2 records it, and the section now scores under `expected` and
`quantile 0.9`. By seed, trusted against acknowledged:

| condition | service | top bin realised, trusted (acked) |
|---|---|---|
| defaults, no loss or 5% loss, replay | -0.02 / +0.02 / -0.01% and -0.02 / +0.02 / +0.03% | 1.000 (1.000) |
| half partition, replay, `expected` | -0.05 / -0.00 / -0.02% | 0.999 (1.000) |
| defaults, 20% loss, no recovery | +0.00 / +0.02 / +0.04% | 1.000 (1.000) |
| half partition, 20% loss, no recovery | +0.01 / +0.04 / +0.12% | **0.975 (1.000)** (`quantile 0.9`: 0.974) |

With replay, service is within 0.05%, not the predicted 0.01% (a miss on size, not direction), and the
top bin is within 0.1pp. With 20% loss and no recovery the top bin's realised residency falls below the
acknowledged belief's at half the partition and is unchanged at the published one, where evictions are
too rare for a trusted block to have been evicted. The belief gap is nonzero for both. Service stays
inside Phase 4's 0.16%. The trusted belief also fills the top bin four times over (4,072 -> 17,800
predictions at the published partition), which is the over-confidence's whole size. The event stream is
the refusal channel: nothing has to be added to it.

**P7 -- Divergence by cause follows the condition, and two causes exist at loss zero.**

At the published partition, every phantom under replay and periodic recovery is an undelivered
removal -- not yet due at loss zero; dropped or not yet due with loss -- and under no recovery 1-8%
are stranded. At half the partition, 15-25% of phantoms are never stored at every loss rate under
replay, and 35-45% under 2 s of silence. The miss share stays under 0.01% of believed blocks in
every cell, all of it re-dispatched while evicted.

- *If right:* Phase 4's §9.6 limitations have their sizes, and stranded entries are the one cause a
  deployment could remove at no cost -- named for a correction (§7).
- *If wrong* (a cause appears under a condition that cannot produce it -- silenced with no episodes,
  dropped with no loss, stranded under replay): the fate bookkeeping is wrong, and no cause split is
  published until it is not.

**Measured: confirmed at the published partition, and the silence clause is wrong.** Section 2, share of
believed blocks that are phantoms, by cause (%):

| | phantom | not yet due | dropped | silenced | never stored | stranded |
|---|---|---|---|---|---|---|
| defaults, 0% / 5% / 20% loss, replay | 0.069 / 0.082 / 0.132 | 0.069 / 0.073 / 0.069 | 0 / 0.009 / 0.063 | 0 | 0 | 0 |
| defaults, 5% periodic | 0.801 | 0.061 | 0.740 | 0 | 0 | 0 |
| defaults, 5% / 20% none | 15.06 / 36.56 | 0.060 / 0.029 | 14.87 / 33.68 | 0 | 0 | 0.137 / 2.853 |
| defaults, 2 s silence, replay | 0.777 | 0.075 | 0 | 0.701 | 0 | 0 |
| half, 0% / 5% / 20% loss, replay | 0.198 / 0.233 / 0.371 | 0.166 / 0.166 / 0.162 | 0 / 0.028 / 0.144 | 0 | 0.032 / 0.039 / 0.064 | 0 |
| half, 5% / 20% none | 31.92 / 59.60 | 0.127 / 0.067 | 27.64 / 47.44 | 0 | 3.84 / 8.19 | 0.319 / 3.903 |
| half, 2 s silence, replay | 1.667 | 0.187 | 0 | 1.200 | 0.281 | 0 |

Every phantom under replay and periodic recovery at the published partition is an undelivered removal;
1-8% of the no-recovery column is stranded (0.9% and 7.8%); at half the partition 16-17% of phantoms
under replay are never stored (inside 15-25%), at every loss rate. **Under 2 s of silence it is 17%, not
35-45%**: 72% are silenced and 17% never stored. The pre-measurement's 41% counted every optimistic entry,
including stores that silence swallowed, and the built attribution puts those under *silenced*. The miss
share is 0.000% in every cell, inside the prediction's 0.01% and leaving "all re-dispatched" without a
case: the `Redispatched` cause exists in the instrument and never fires at a sample point.

**P8 -- A deadline in place of the unbounded bump changes nothing a user would see.**

The ledger on one node, `--retain` against `expect`, with 0, 18%, 50% and 69% of FaaS->inference
hints false: task latency within ±3% at every rate, of both signs; net work up with the rate by the
same amount under both (0.6%, 3.8%, 6.7% pre-measured under `expect`); and the count of entries
holding a retention whose flow has passed, zero under `--retain` and growing with the rate under
`expect`.

- *If right:* §3.3's case for `retain_until` is bounded state, not latency, and the document should
  say which.
- *If wrong* (the deadline wins by more than 3% on every seed): wrong hints inflate priority enough
  to displace real work, and §3.3 was right for the reason it gave.

**Measured: the deadline is not latency-neutral even with honest hints, and the prediction's size held.**
Section 8 (one node, split memory), unbounded bump against deadline, by seed; false share is the share
of KV-downstream hints that are false:

| hint rate | false share | margin over blind, unbounded | ... deadline | stale entries, unbounded (deadline) |
|---|---|---|---|---|
| 0 | 0% | 10.7 / 10.6 / 10.3% | 11.7 / 11.4 / 12.3% | 300 / 270 / 27 (0) |
| 0.1 | 25% | 11.7 / 6.0 / 8.8% | 11.4 / 11.3 / 10.5% | 244 / 144 / 81 (0) |
| 0.45 | 59% | 8.5 / 10.9 / 8.2% | 10.5 / 6.5 / 6.0% | 288 / 353 / 248 (0) |
| 1.0 | 76% | 9.0 / 10.8 / 10.9% | 8.5 / 10.8 / 7.6% | 684 / 852 / 304 (0) |

Net work against `blind` rises with the rate by the same amount under both (+0.3 to +7.7% under the
bump, +0.4 to +7.9% under the deadline), and the stale-entry count is zero at every rate under a deadline
and grows with the rate under the bump, as predicted. The latency clause is half wrong: the deadline's
margin differs from the bump's by -4.4 to +5.3 points, with three of twelve cells outside ±3 and both
signs present, and at rate zero -- no false hint anywhere -- it wins on every seed, by 1.0 / 0.8 / 2.0
points, with net work at -0.40 / -0.02 / -1.41% of `blind` against +0.11 / +0.23 / +0.12%. The mechanism is the one §1.11 called a mild frequency prior:
the ledger touches only a chain's deepest block, so a hinted prefix's interior keeps an inflated
priority indefinitely, and withdrawing it helps. The *if wrong* branch (a win beyond 3% on every seed)
did not fire; §3.3's case for `retain_until` is bounded state first and, by one to two points, latency
second.

---

## 3. What influence must and must not do

Ten rules. The first is the gate; the third is the one most likely to be broken for a good reason.

1. **The gate.** With every new bit off, byte-identical to `HEAD` on the reproducible set as
   `phase-4.md` left it. With `--directives` on under `--ignores` and `--marks acked`, byte-identical
   to directives off: the engine drops them and the router believes nothing it was not told. With
   directives honoured and every deadline already past on receipt, identical to `--ignores` -- the
   mechanical check that a mark does nothing until it is live. Checked after every work item.
2. **Influence follows authority, exactly** (§1.1). A directive is sent only for engine-allocated
   state; owned state gets a decision; weights stay on the ledger until Phase 6.
3. **The router learns a directive's fate only from the stream.** No router-side read of the
   engine's marks, for the reason Phase 4's rule 3 refused a router-side allocator: it would be exact
   here by construction and available in no deployment.
4. **Nothing tuned.** Deadlines come from declared leads; the oracle's horizon is a ceiling's
   parameter, swept and printed; the pressure rule is RFC-0001's; the prefill-ahead target is the
   router's own argmin. False-hint rates are conditions of the experiment, never parameters of a
   policy.
5. **Declared, not learned.** Every emitter reads only what is declared -- a hint's chain and lead, a
   request's retention scope -- except the oracle, which reads the trace and is labelled a ceiling
   wherever it prints.
6. **One bit per mechanism.** `--directives` (with `--emit declared|oracle` and `--horizon`),
   `--ignores`, `--marks acked|trusted`, `--prefill-ahead`, `--retain` and `--false-hints` are
   separately selectable, and the headline runs change one at a time.
7. **A condition's randomness is its own stream** (Phase 4's rule 6). False hints come from a seeded
   stream separate from the workload's, so the trace is byte-identical at every rate.
8. **Nanoseconds or counts** (`phase-2.md` rule 5). A directive is priced in stall and service;
   marks, acknowledgements and pressure evictions are counts.
9. **Measure, do not repair** (`phase-2.md` rule 1). A stranded entry is attributed and not fixed
   (§1.9); if the pressure rule's anti-Belady reading shows, it is reported, and latest-expiring is
   not swapped in mid-phase.
10. **Three seeds for every directive cell.** A directive's effect is smaller than the divergence
    any change to engine state sets off (§6, risk 3), so one seed cannot sign it.

---

## 4. Work items

Ordered so each lands compiling and checkable against rule 1, and so nothing that can move a number
lands before the items that cannot.

### 4.1 Divergence by cause

The publisher records, per node and per block, the sequence numbers of the batches that last
stored and last removed it from the GPU; the channel records each sequence number's fate -- open,
in transit, lost, silenced, applied, repaired. `Machine::sample_divergence` attributes every
phantom and miss by §1.9's table, and `Instruments` carries a share per cause. It measures Phase
4's belief as published and moves no number. Its invariant: on every sample the causes' shares
sum exactly to the phantom and miss shares.

### 4.2 Marks in the engine

`EngineCache` gains a per-block `Mark { rank, until }`, the rank retain or evict-first, ranked as
§1.5 says, expired lazily at `release(now)` and soft under pressure, with counts of marks applied,
expired and evicted under pressure. `KvEvent::Stored` gains the optional mark, re-emitted when a
mark changes, and `Index` keeps marks. Nothing marks yet: a no-op. Phase 4's stream invariant
extends -- replay into an empty index reproduces every tier and every live mark after every
request.

### 4.3 The acknowledged belief

`Belief` records acknowledged marks from `Stored` events. `survival` returns 1 for a live
acknowledged retain, and marked blocks leave `B`. `--marks trusted` records the router's own
directives at dispatch instead. Unread until 4.5 emits.

### 4.4 A declared retention scope

`Request::retention`, `None` or `EvictFirst { from }`, set by the generator: a gang agent's private
suffix and its output, a queued downstream's output. Metadata only -- the trace hashes identically.

### 4.5 Directives on the request path

`Machine::run_here` attaches a `Directive` to the dispatch: evict-first over the declared scope, and
a retain over a gang agent's parent chain until its resume's declared lead; under `--emit oracle`, a
retain on every block until its true next use within `--horizon`. `--ignores` drops the directive at
the engine boundary. The first item that moves a number.

### 4.6 Prefill-ahead

One node: under `--prefill-ahead`, `Hierarchy::announce`'s engine branch places the downstream's
missing blocks through the engine's own `place`, in chain order, and charges the acquisition as
`prefill_ahead_ns`. Cluster: `Machine::serve_request`, for a hint whose downstream is `KvBlock`,
takes the downstream's scored argmin over its declared chain and the observed mean output length,
acquires the chain's missing suffix there, and records the target so that landing can be counted
when the downstream arrives.

### 4.7 The ceilings on the cluster

The clairvoyant block manager -- Phase 3's `EngineKv.clairvoyant`, until now wired only on one
node -- over a trace index that includes gang agents, and the oracle emitter of 4.5. Both print as
signed differences and are never called optimal.

### 4.8 `retain_until` and `evict_first` on `Entry`, and false hints -- the item that can be cut

Under `--retain`, a `TierPool` entry's retention is a soft pin with a deadline in place of the
`expect` bump; `evict_first` sends it to the front of its class. `--false-hints p` draws phantom
hints from their own stream. If it slips, it slips to Phase 7, which inherits predicted flows and
the wrong hints that make a deadline matter.

### 4.9 Instruments

| instrument | measures | over |
|---|---|---|
| divergence by cause | phantom and miss shares per cause (§1.9) | every 16th KV decision, as Phase 4 samples |
| reuse | per origin: hit, never on this node, evicted from it by the tier it fell to; the gap before hits and evicted-here misses; age at GPU eviction; one-shot share | every KV dispatch |
| directives | emitted, acknowledged, honoured, expired, evicted under pressure, retained bytes | per node |
| flow downstream | stall, service and per-class p99 of the requests that complete a declared flow | every flow |
| prefill-ahead | work, blocks, landing, peer fetches of a prefilled block | every hint with a KV downstream |
| stale retention | entries holding a bump or a pin whose flow has passed | every request |

Origins -- tenant prefix, session, agent, agent output, resume result, flow prompt, flow call, task
output -- are tagged by the generator beside `BlobMeta` and read only by instruments; the scoring
path cannot name them, for `RequestView`'s reason.

These are the instruments the pre-measurements approximated, and §8's figures are reproduced with
them first.

### 4.10 `polyphonic influence`

A reproducible sweep, as `belief` is for Phase 4: no control crossing charged, seed-deterministic,
the `distributed` cluster at Phase 3's two regimes, rack unless stated, three seeds for every
directive cell.

1. the gate, as printed check lines
2. divergence by cause across Phase 4's loss, recovery and silence grid (P7)
3. `flows`: `announce` split by half, and the engine's hold and prefill-ahead, split and unified
   memory (P1, P2)
4. prefill-ahead on the cluster, rack, zone and region, with landing (P2, P3)
5. the declared emitter, honoured and ignored (P1, P5)
6. the ceilings: the oracle at 2, 5 and 30 s, and the clairvoyant block manager (P4)
7. `acked` against `trusted` under `--ignores`, with replay and at 20% loss without recovery (P6)
8. `--retain` against `expect` under false hints (P8)
9. the reuse tables (§1.10)

`distributed` and `code-review` take the same flags.

### 4.11 Report and publish

| target | change |
|---|---|
| `owned-and-observed.md` §9 Phase 5 | a **Status** line; the phase restated as two channels, with prefill-ahead beside the directive |
| `owned-and-observed.md` §3.3 | `retain_until` as built, and what the deadline is for (P8) |
| `owned-and-observed.md` §3.6 | divergence by cause, as measured; ignored directives restated as calibration, not divergence (§1.8) |
| `owned-and-observed.md` §4 | the `retention` field's first consumer; prefill-ahead beside `ReadOnly` speculative dispatch |
| `owned-and-observed.md` §5 | property 5's KV half restated; the directive ceiling beside property 8 |
| `residency-ledger.md` | an *Influence* section, every figure with its conditions; *Standing*: the `announce` row corrected, rows for directives and prefill-ahead |

Whether `--prefill-ahead` becomes the default under `--engine-cache` is decided after the numbers,
as Phases 3 and 4 decided their own bits.

---

## 5. Verification

- **Byte-identity with every new bit off**, against the commit before this phase, on the
  reproducible set at a reduced `--ops` and a second seed, after every work item. `distributed`,
  `code-review` and `data-path` get the structural smoke run, for `phase-1.md` §5's reason.
- **The gate** (rule 1): `--ignores --marks acked` equals directives off; honoured marks whose
  deadlines are past on receipt equal `--ignores`.
- **The stream reproduces the marks.** Replay into an empty index equals every tier and every live
  mark after every request, on the small-tier fixture with decode output held, fetch, a shared L2,
  a drain and marks of both kinds; mutation-checked -- a mark change that does not re-emit fails.
- **Causes sum and respect their conditions.** Per sample, the causes add to the phantom and miss
  shares exactly; on an exact belief every cause is zero; with loss and no episodes nothing is
  silenced; with episodes and no loss nothing is dropped; under replay and periodic recovery
  nothing is stranded.
- **The pressure rule.** On a fixture where every candidate is marked, the soonest-expiring is
  evicted and counted, and no preemption occurs that the same fixture without marks would not have.
- **Expiry.** A mark past its deadline is an unmarked block to every later eviction.
- **Prefill-ahead is accounted.** Its work equals the acquisition it performed, block for block;
  the `blind` rows are unchanged by the bit.
- **The oracle cannot leak.** Only the oracle emitter reads the trace index; the scoring path
  cannot reach it, by type.
- **The census.** `cargo build --release --features census` still emits 13 warnings: marks are
  applied inside `EngineCache`, which is the engine's own, and a prefill-ahead is a dispatch. The
  `KvBlock` row of `EngineOps` stays zero with `--engine-cache` on. A moved count means the router
  wrote engine state.
- `cargo fmt --check`, `cargo clippy --all-targets` and `cargo test` clean.

---

## 6. Risks

1. **A small answer read as a broken instrument, or tuned into a large one.** Every directive row
   is predicted within noise. The configurations where retention could look large -- a smaller
   partition, longer session gaps, no offload tier, placement that ignores residency -- are the
   ones `sched_lm` ran, and importing them to manufacture a win is Phase 4's risk 1 again. The
   regimes are Phase 3's, chosen for Phase 3's reasons.
2. **Prefill is not a resource in the engine model** (§1.3). Prefill-ahead's cost is counted as
   work and its displacement as later stall, never as step time. The result is exact about when and
   silent about whether; Phase 6 closes it.
3. **Chaos.** Any change to engine state reroutes later requests -- placement churn is 37-43% of
   session turns (`phase-4.md` P9) -- and deltas under about 2% of stall change sign across seeds
   (§1.10's table). Three seeds per cell, ranges with sign counts, and no single-seed directive
   number quoted anywhere.
4. **The simulator acts at arrival instants** (Phase 4's risk 5). A deadline expires at the next
   arrival rather than at its instant, and a 24 ms lead is six arrivals.
5. **RFC-0001 as read is not what ships.** #37003 is open, and its expiry, reset and pressure
   rules are unsettled upstream. Each choice in §1.5 is named so that a different one is a line,
   not a redesign.
6. **A framework's declarations will be noisier than the generator's.** `evict_first` is declared
   here with certainty because the workload makes the blocks one-shot by construction; Phase 7 is
   where declarations become predictions.
7. **The ceilings are strong references, not bounds.** The clairvoyant manager and the oracle both
   read a global future into a per-node cache, and a block's next use may land on another node --
   `sched_lm`'s own caveat for its Belady node. The evicted-here miss cost (§1.10) is the hard
   bound on retention at the chosen node.
8. **The pre-measurements came from outside the repository** (Phase 4's risk 9). They argue for
   the predictions and are evidence for no claim; §4.9 rebuilds each, and §8 says how each was
   taken.

---

## 7. Out of scope

- **Learned gaps and predicted flows.** Phase 7; §1.10's ceiling is what they are bounded by.
- **Binding a downstream to its prefilled node** (§1.4). Coupling tier 2, with Phase 6's P/D
  pairing.
- **A standalone pin/move/evict control plane** (§1.6).
- **Keep-alive touches.** Holding a block under LRU by re-requesting it is dominated by a retain
  directive, which holds it for nothing; with the directive's ceiling under 2% of stall, a touch's
  is lower.
- **Prefill as a resource, and disaggregated prefill and decode.** Phase 6.
- **Decode-token retention** -- #37003's `DecodeRetentionPolicy` -- which is framework-originated.
- **Per-tenant or per-scope pin budgets.** §3.8 and Phase 6; `sched_lm`'s E5 found budgets inert.
- **Clearing a stranded optimistic entry on a removal** (§1.9). A correction to Phase 4's published
  no-recovery column, for its own A/B.
- **A cache scope for isolation** (`owned-and-observed.md` §8's timing-channel opt-out). `scope` is
  carried and echoed, and nothing consumes it.
- **Cancellation** (Phase 9) and **authority-gated speculation** (Phase 7).
- **Prefill-ahead for owned state.** The ledger's own prewarm of `Snapshot` cells is `announce` as
  it stands on one node, and the cluster does not announce at all -- a separate item if a
  tool-placement result ever needs it.

---

## 8. Pre-measurements

All taken on an instrumented copy of `8beb019`, run outside the repository and not committed.
Cluster rows use `polyphonic belief`'s configuration -- 4 nodes, 3 units each, 16 GiB HBM, 32 GiB
DDR and 64 GiB `NVMe` in total, 250 req/s, 10% fan-out, 15,000 ops, rack unless stated, no control
crossing charged, `scored + fetch`, the engine's grants sized from the ledger's run at each regime,
and the exact view; the uninstrumented base reproduces the published 489.451 ms. Single-node rows
use `flows --engine-cache` at its defaults -- 4 GiB HBM and 8 GiB DDR, or 12 GiB unified, 15,000
ops, the oracle-best soft floors, and a 0.88 GiB partition (1.50 GiB unified).

| name | what | how | headline |
|---|---|---|---|
| *flows-split* | which half of `announce`'s KV margin was retention and which prewarm, and what an engine-side hold and a prefill-ahead buy | `announce`'s KV half reduced to the bump alone or the admission alone on the ledger; on the engine's side, the downstream's resident blocks held (not evictable, soft under pressure) until it arrives, or its missing blocks placed through the engine's `place` at the hint with the acquisition charged as prewarm work; seeds 1-3 split, seed 1 unified | §1.2's table |
| *host-split* | the same for owned state | the bump alone or the admission alone for `Snapshot` cells | the bump is worth 0.01 ms of task latency, the admission 1.43 ms |
| *false-hints* | a deadline against the unbounded bump when hints are wrong | phantom hints -- a random function's prompt and a fresh call -- drawn per FaaS request from a separate seeded stream with probability 0.1, 0.45 and 1.0; the bump withdrawn 12 or 60 requests after its hint unless touched | a deadline against none: -1.9% to +2.6% of task latency; net work +0.6, +3.8, +6.7% |
| *reuse* | reuse gap against LRU residency, by origin | every KV block tagged with its generator origin; at each dispatch, every chain block classified on the chosen node as a hit, never here, or evicted from here by the tier it fell to, with the gap since its last touch there; the age of every GPU eviction; the one-shot share at the end; both regimes | §1.10's first table; one-shot origins 13.4% and 14.6% of GPU stores |
| *ceiling* | what any eviction order or retention could buy | the clairvoyant engine cache on every node over an index of the whole trace, gang agents included; the oracle emitter retaining each dispatched block until its next use within 2, 5 or 30 s, with RFC-0001's soft marks; seeds 1-3 at 5 s | §1.10's second table; pressure evictions at the published partition none at 2 and 5 s and 106k at 30 s, at half the partition 38k, 68k and 111k |
| *declared* | retaining a fan-out's parent chain for its resume | each agent's dispatch retains the chain it forked from until the resume's declared lead; seeds 1-3 | 57k marks; stall -0.05% to +0.08% at the published partition, +0.2% to +1.0% at half with 20.6k pressure evictions |
| *ephemeral* | evict-first on declared one-shot scopes | after each dispatch, blocks of agent, agent-output and task-output origin moved to the front of the LRU; seeds 1-3 | §1.10's second table, last row |
| *prefill* | prefill-ahead on the cluster | at each hint with a KV downstream, the downstream's missing blocks acquired on a target before it arrives -- the deepest believed prefix, the downstream's scored argmin, or that argmin executing its acquire plan -- with landing counted; seeds 1-3 at rack, seed 1 at zone and region | §1.3's and §1.4's tables; landing 40-44% (deepest prefix, seed 1) and 47-51% (argmin) |
| *causes* | the optimistic part of Phase 4's phantoms | at each divergence sample, each phantom split by whether the belief holds it as an optimistic dispatch or as an index entry, over Phase 4's loss and recovery grid and 2 s of silence | §1.9's table; its phantom shares reproduce Phase 4's |

---

## 9. What the build found

Eight things the plan did not anticipate, in the order they were found.

### 9.1 A test caught the plan's own flow filter

The test that a false hint is built from the same blocks as a real flow selected real flows by "first
block is a KV block and the chain is 28 blocks long". A fan-out's resume chain -- the orchestrator's
context plus two result blocks per agent -- can be 28 blocks long, and the test failed on it. Flows are now
selected by origin (`Origin::FlowPrompt`), which is what the instruments do. `flow_downstream(f, call)` is
one function shared by the generator and the false hints, and `a_false_hint_is_built_from_the_same_blocks_a_real_flow_is`
pins that they cannot drift.

### 9.2 Section 7 first measured nothing, because the score did not read the thing it changed

The first version scored the acknowledged and trusted beliefs under `face-value`, the default rule, which
never reads `P(resident)`. A trusted mark changes only `P`, so every service column read +0.00% on every seed
for a reason that had nothing to do with the mechanism. The section now runs under `expected` and `quantile
0.9`, the two rules that consume `P`. The lesson is Phase 4's §9.1 again from the other side: check that
the quantity an experiment changes reaches the thing it measures. It cost one extra pass; the table above is
the corrected one.

### 9.3 The flow-downstream column was all zeros until origins were switched on

Every "flow stall" cell in the first full run read +0.00% and "0.0 s saved" against 39 s of prefill work,
because the instrument recognises a flow downstream by its origin and the comparison runs did not track
origins. They do now. Tracking is inert -- `tracking_origins_and_events_changes_no_cost` runs the same
trace with and without it and compares every cost -- so the fix moved no other number.

### 9.4 The gate's second clause is an engine property, not a cluster one

§3 rule 1 says honoured directives whose deadlines are past on receipt equal `--ignores`. No emitter produces a
deadline in the past, so there is no cluster run that could show it. It is checked where it can be: the
engine refuses a mark that is not live at its own clock (`a_mark_whose_deadline_is_past_on_receipt_does_nothing`)
and `Hierarchy::tick` advances that clock. The cluster gate is the first clause alone -- ignored, acknowledged,
byte-identical -- on declared and oracle emitters at both regimes.

### 9.5 The pre-measurement's false-hint shares were against the wrong denominator

§1.11 and P8 said 0.1, 0.45 and 1.0 make 18%, 50% and 69% of FaaS-to-inference hints false. The scratch copy
divided by every hint the run saw, tool hints included. The build counts against KV-downstream hints, which
is what "FaaS->inference" means, and the shares are 25%, 59% and 76%. The rates and the behaviour they
produce are unchanged; only the label was.

### 9.6 A prefill can change which flows complete, on a tight partition

`prefill_ahead_is_accounted_block_for_block_and_only_helps_flows` first asserted that the same number of flow
downstreams complete with and without prefill. On its fixture -- a 64 MiB partition, a deliberately tiny one
-- 155 complete without and 143 with, because a 28-block prefill displaces enough to change which later
requests fit. The test compares stall per completed flow instead, which is the claim. On the cluster at
Phase 3's partitions the served count is unchanged to the request.

### 9.7 Silence's optimistic entries were counted as never stored, and are not

The pre-measurement split each phantom by whether the belief held it as an optimistic dispatch. Under 2 s of
silence that put 41% of half-partition phantoms in the optimistic bin. The channel-level attribution asks a
different question -- did the engine store it -- and files a store swallowed by the silence under *silenced*,
leaving 17% never stored. P7's silence clause is wrong for that reason, and the table above has both columns.
The `Redispatched` cause, Phase 4's standing case, never fires: every sample's miss count is zero, so that
limitation of Phase 4's belief has a size of zero at 15,000 ops on these conditions.

### 9.8 Things that shrank

- **`code-review`** takes none of the new flags, as it takes none of Phase 4's; it has its own run loop and no
  `Scenario`. `distributed` and `flows` take them.
- **The engine's hold is a soft mark, not the pre-measurement's hard one.** The pre-measurement held blocks
  outright; the build marks them with RFC-0001's soft rule. The margins are identical to the digit
  (9.1 / 9.4 / 9.0%), which says the hold does nothing in either form.
- **The deepest-prefix target** was a pre-measurement only; it was built as `--prefill-target deepest` because
  P3's second clause cannot be graded without it.
- **`Machine::new`** is over clippy's line limit with the new fields and carries the same
  `allow(clippy::too_many_lines)` the CLI functions do. `serve_request` was split (`observe_landing`,
  `after_dispatch`) rather than allowed.
- **Runtime.** The full sweep is about eight minutes as four parallel processes; the one-node sections are
  the slow part (six `best_split` searches, three seeds of two memory models, and again for section 8).

### 9.9 A review of the build moved one figure

An independent review of the implementation found fourteen issues, and all are fixed. The reproducible set is
still byte-identical to the commit before this phase, and `polyphonic influence` (every cluster section, seeds
1-2, 3,000 ops) and `flows --engine-cache --prefill-ahead --hold` differ from the pre-review build in exactly
two lines each run: the reuse table's denominator and a relabelled summary line.

- **The reuse table's denominator counted every dispatch, not every KV dispatch.** Tool calls, services and
  `FaaS` invocations on engine nodes were counted, and so were they in the pre-measurement. The share of KV
  dispatches with a miss on a block the node evicted is **35.9% at the published partition and 53.4% at
  half**, not the 14.5% and 21.6% §1.10's table quotes. What those misses cost -- 0.93 and 1.59 ms per served
  request, 0.19% and 0.32% of service -- was always divided by served requests and does not move, so §1.10's
  conclusion stands with more force: by shape they are common, by size they do not matter.
- **A prefilled block was never repriced under the clairvoyant engine**, so with `--clairvoyant-kv` and
  `--prefill-ahead` together it would have been the next victim. No published run combines them.
  `prefill_block` now takes its cost from `local_ns` and reprices as every other placement does.
- **`--seeds 0`** printed "seeds 1 .. 0" and measured nothing; the argument now rejects it.
- **`owned-and-observed.md`.** The reflow that added this phase's status line collapsed the *On the numbers*
  bullet list into one paragraph and dropped the blank line that keeps §5's item 6 a list item. Both are
  restored.
- **Cleanups.** `Entry.evict_first` was written and never read, since the priority does the work;
  `FlowDownstream` kept two fields nothing read; the CLI's `quantile` duplicated `instruments::percentile`;
  `influence` recomputed each regime's no-mechanism baseline once per section, and now caches it; the `flows`
  summary called prewarm work prefill work; and `--reuse`'s help promised a report only `influence` prints.

## 10. Verification, as run

- **Byte-identity with every new bit off**, against the commit before this phase: the 32 outputs of `residency`
  (split, unified, `--clairvoyant`, `--engine-cache --decode-kv`), `flows` (and `--engine-cache`, split and
  unified), `placement` (split, unified, `--drain-at`, and with the engine, with and without the drain),
  `volatility`, `ownership` (and `--engine-cache`) at `--ops 3000` and two seeds, plus `belief` and `price`
  at 3,000 ops: identical after the engine and cache work, after the belief, generator and machine work, and
  on the final build. The baseline was run twice against itself first. `belief` being in the set is what says
  the cause bookkeeping does not touch the channel's random stream.
- **The gate**: `influence` section 1, identical on all four cells; and
  `ignored_directives_and_an_acknowledged_belief_change_nothing`, on 5% loss with replay and with no recovery.
- **The stream reproduces the marks**: `the_event_stream_reproduces_every_live_mark_after_every_request`, on the
  small-tier fixture with decode output held, fetch, a drain and both ranks live. Mutation-checked: dropping the
  event a mark change emits fails it at request 7, the first mark.
- **Causes**: `every_phantom_and_miss_has_exactly_one_cause_that_its_condition_allows` (shares sum to the
  phantom and miss shares; zero on an exact belief; nothing silenced without episodes, dropped without loss,
  stranded with a repair path), plus three channel-level tests and two belief tests for acknowledged and trusted
  marks.
- **The pressure rule, expiry, combination and the no-op cases** are eight engine tests, including
  `an_engine_with_no_marks_evicts_exactly_as_before`. Mutation-checked: putting unmarked victims ahead of
  evict-first fails two; letting `--ignores` reach the engine fails the gate; leaving marked blocks in the
  turnover denominator fails the belief test.
- **Prefill-ahead is accounted**: `prefill_ahead_is_accounted_block_for_block_and_only_helps_flows` (the
  machine's count equals the hierarchies' placed blocks; `blind` is unchanged by the bit is
  `prefill_ahead_is_what_announce_lost_and_a_hold_is_not`).
- **The census**: `cargo build --release --features census` emits 13 deprecation warnings; the marks are applied
  inside `EngineCache` and a prefill is a dispatch. The `KvBlock` row of `EngineOps` is unchanged.
- `cargo fmt --check`, `cargo clippy --all-targets` and `cargo clippy --all-targets --all-features` clean
  (the latter's only warnings are the census's 13), and `cargo test` passes with 110 tests, up from 77.

