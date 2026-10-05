# Phase 7 -- Programs: learned flows, speculative authority, sessions that suspend, and the taxonomy

Implementation plan for Phase 7 of [`owned-and-observed.md`](owned-and-observed.md): replace the
declared flow with what a scheduler can know (§3.2), gate speculation on the authority a tool
declares (§4), give a long-running agent a lifecycle (§9, *Sessions that suspend*), and run coupled
% per taxonomy pattern (§4). §9 asks for four deliverables: what the coupling-tier-1 win is worth
against estimates rather than oracles; the latency and goodput delta from speculative scheduling;
the logged tier's write rate; and a table saying for which workload patterns a unified orchestrator
can help at all.

**Status: planned.** Nothing below is built. §2's predictions are stated before the run, per
`owned-and-observed.md` §7, and like Phases 4 to 6 and 9 they lean on **pre-measurements**: numbers
taken on an instrumented copy of `4dcdefb`, run outside the repository and not committed, with every
hook off reproducing `influence`'s published prefill-ahead section to the digit, and with every
split counter summing to the machine's own. Two read the existing model through instruments this
phase builds (causality, coupling by origin); four emulate what it builds (a closed-loop driver for
agent programs, a learned flow template, speculative tool calls, a call's misses by gap); three are
arithmetic, one on a retrieval trace with no simulator under it and two on published distributions.
Each is labelled with its grade (`owned-and-observed.md`, *On the numbers*), and §8 says how each
was taken. They are reasons to predict, not results: §4.17 rebuilds each instrument in the
repository, and a pre-measurement the build does not reproduce is reconciled before a prediction
resting on it is graded.

Four things make this phase unlike Phase 9.

- **Its subject is not in the workload model, and the plan starts there.** The trace is open loop: a
  request arrives when the generator says, whatever the requests it depends on are doing. An agent
  turn's tool call arrives 28 ms after the turn, which decodes for a median 1.05 s; a fan-out's
  dispatch likewise; half of the fan-outs' resumes arrive before their slowest agent finishes; and
  17-18% of a session's turns arrive while its previous turn is still decoding (§1.1). An agent loop
  -- decode, tool, decode -- has no critical path in that trace, so speculation has nothing to
  shorten and a predictor no gap to learn. Replayed as programs whose steps wait for each other, the
  same scripts take 4.5-4.8 times as long a turn.
- **Its mechanisms are published, in inference-only form.** Pythia stages predicted prompts before
  they are sent; Continuum holds KV through a tool call for a time drawn from that tool's duration
  distribution; PASTE and toolspec speculate tool calls; Sutradhara dispatches tools from the decode
  stream; SAGA schedules a workflow as one unit (§1.3). The phase measures what one orchestrator
  over inference, tools and the memory both hold adds to them, pattern by pattern -- not the
  mechanisms against their absence.
- **Its inputs have provenance.** A production characterization of GitHub Copilot's coding agent
  (13.5M sessions, June 2026), TraceLab's Claude Code and Codex traces, the SWE-bench and BFCL
  traces Continuum reports, PASTE's pattern statistics and MCP's annotation schema give the presets
  distributions to carry. Constants with no source are named as chosen and swept (§1.14).
- **Its headline is a table.** Every deliverable is a row per pattern, and a row may read "nothing
  to couple" -- the risk §9 named, which the pre-measurements already show for every pattern without
  a cross-workload flow (§1.13).

§1 settles fifteen decisions. Six are findings about the existing model and documents rather than
about the work ahead: §1.1 (the trace is open loop), §1.2 (a declared flow names blocks nobody has
yet, ahead of a lead the trace invents), §1.4 (the integrated path already carries the strongest
flow signal), §1.8 (retention through a tool call is bounded at about a percent here), §1.10 (the
logged tier is not orders of magnitude below the soft one on an agent mix) and §1.13 (coupling lives
only on flows).

---

## 1. What has to be settled before a program can be scheduled

### 1.1 The trace is open loop, so an agent loop has no critical path in it

`Workload` emits a flow's downstream a fixed number of arrivals after its upstream (`FLOW_LEAD_OPS`,
`FANOUT_LEAD_OPS`, `RESUME_LEAD_OPS`) and picks a session's next turn by Zipf over 512 slots, so no
request waits for another. Instrumenting `drive` read-only (§8, *causal*; `distributed`'s `scored +
fetch` at rack, published defaults, seeds 1 / 2 / 3):

| flow | downstream arrives before its upstream finishes | by a median | lead |
|---|---|---|---|
| agent turn -> its tool call | 100 / 100 / 100% | 1051 / 1077 / 1051 ms | 28 ms |
| agent turn -> its fan-out | 100 / 100 / 100% | 968 / 1056 / 1007 ms | 28 ms |
| fan-out -> the orchestrator's resume | 52.2 / 50.7 / 48.0% | +20 / +4 / -25 ms | 1604 ms |
| `FaaS` call -> its inference | 0 / 0 / 0% | -- (the call takes 0.1-0.2 ms) | 28 ms |

And 18.2 / 17.9 / 17.0% of a session's consecutive turns overlap: the later arrives before the
earlier has finished. With the engine allocating and decode output held, none of these moves by more
than half a percent.

What that does to a latency the phase has to report: a closed-loop emulation (§8, *programs*) draws
agent sessions shaped after Copilot's traces (§1.14) and submits each step when its predecessor
completes. Replaying the same scripts open loop, each step 28 ms after the previous step's arrival:

| 12 sessions a second, seeds 1 / 2 / 3 | turn, mean | turn, p50 | session, mean |
|---|---|---|---|
| closed loop | 5536 / 5798 / 5363 ms | 3484 / 3699 / 3345 ms | 17.2 / 17.1 / 16.8 s |
| the same scripts, open loop | 1211 / 1205 / 1182 ms | 1207 / 1195 / 1173 ms | 7.3 / 7.0 / 7.4 s |

An open-loop turn takes as long as its slowest call, because its calls overlap; a closed-loop turn
is the sum of its calls and tools. So every task-latency figure the documents quote for a flow --
`announce`'s 11-18%, prefill-ahead's 24-28% and -64% (Phases 3 to 6) -- describes an open-loop
trace, where a downstream starts on a schedule rather than when its upstream ends.

**Chosen: programs.** A program is a script -- calls, tools, idle periods, fan-outs -- drawn from
its own stream, so its content is identical in every arm, and submitted step by step as each
predecessor completes, so its timing is the arm's. The base trace is untouched and programs are a
population beside it; with them off every published number stands. With them on, the machine's clock
moves to the next event rather than by one interval, and the base trace keeps its arrival instants.

### 1.2 A declared flow names blocks nobody has yet, ahead of a lead the trace invents

A `FaaS` call's declared downstream is its function's 24-block prompt template followed by four
blocks drawn from 64 per function (`flow_chain`): the call's own input to the model, which the
function computes. Prefill-ahead (Phase 5) prefills all 28 when the hint arrives. On the published
cluster (§8, *hints*; `influence` section 4, `scored + fetch` at rack, seeds 1 / 2 / 3; the flow
downstream's stall against prefill-ahead off):

| hint | published defaults | half partition, decode output held | prefill work for flow stall saved, defaults |
|---|---|---|---|
| the declared downstream, as published | -63.81 / -63.54 / -63.50% | -45.96 / -38.05 / -52.37% | 39.0 s for 26.1 s |
| the declared template, without its four call blocks | -46.90 / -48.21 / -47.18% | -41.17 / -37.36 / -39.34% | 32.0 s for 19.4 s |
| a template learned per function, sent on every call of it | -31.72 / -29.83 / -24.59% | -23.75 / -20.00 / -24.71% | 42.8 s for 11.8 s |

Total stall moves -3.6 to -4.1%, -2.0 to -3.5% and -0.7 to -1.8% in the three rows, and mean service
-0.11 to -0.12%, -0.07 to -0.11% and -0.02 to -0.05%. At zone the three are -30.6%, -24.7% and
-17.4% on the first seed; at region -7.4%, -4.8% and -0.2%.

The call blocks are a quarter of the published win, and no declaration made before the function runs
can name them. A learned template exists for 74.6 / 73.0 / 73.6% of flows when their upstream
arrives -- the rest are a function's first flow, or arrive before its first downstream has -- and it
is sent on the 55% of calls that never flow as well, so it costs 3.6 times the prefill work per
second of stall saved where the declaration costs 1.5. A gate on a learned probability cannot
separate the calls: `P(flow | function)` is 0.45 for every function in the generator, so a gate at
0.5 fires only where a small sample says otherwise (-15.8 to -22.7%).

The lead is the larger problem. The hint arrives with its upstream and the downstream 28 ms later at
250 req/s, so a template's 9.6 ms of prefill always lands first. Causally, a function that calls a
model calls it when it has run: the lead is its runtime, 40-200 us in the trace, under 2% of the
template's prefill. So for the README's own example -- a function that calls inference -- what a
prefill-ahead can find to do is what the previous caller left, which routing already finds.

**Chosen: a hint carries what its source can know, and its lead is earned.** A declaration names a
static template; a prediction names a template with a probability; an observation names the call
(§1.4). A hint's lead is the time between its source learning it and the downstream's submission,
measured on programs. Phase 5's prefill-ahead and `announce` figures are restated at publication as
open-loop figures resting on a declaration no function could make, and the published figures stay on
record: `phase-5.md` keeps them as measured, and the ledger's *Standing* rows keep them beside the
restatement, as they keep every retracted claim.

### 1.3 Every mechanism here is published, in inference-only form

Read 2026-10-04:

| mechanism | published | what it measured |
|---|---|---|
| prefill a predicted prompt before it is sent | Pythia ([arXiv 2604.25899](https://arxiv.org/abs/2604.25899)): "forward staging" of predicted templates in idle cycles, held in host DRAM rather than GPU memory because a prediction "could be incorrect" | 1.38-2.9x average JCT on two multi-agent workflows |
| hold KV through a tool call | Continuum ([arXiv 2511.02230](https://arxiv.org/abs/2511.02230)): a TTL taken as the argmax of `P(τ) × benefit − cost(τ)` over the empirical CDF of that tool's durations | 1.12-3.66x delay on SWE-bench and BFCL traces; "with fast asynchronous CPU offloading ... the reload cost becomes small ... Yet the queueing delay persists" |
| workflow-aware eviction and prefetch | KVFlow (NeurIPS 2025); PBKV ([arXiv 2605.06472](https://arxiv.org/abs/2605.06472)); CacheScout ([arXiv 2608.14624](https://arxiv.org/abs/2608.14624)) | up to 2.19x over SGLang's hierarchical radix cache (KVFlow), 1.85x over LRU (PBKV) |
| speculate tool calls | PASTE ([arXiv 2603.18897](https://arxiv.org/abs/2603.18897)); toolspec ([github.com/joelvarun/toolspec](https://github.com/joelvarun/toolspec)); SPORK; AOSpec | toolspec: top-1 39.3%, mean trajectory 20.20 -> 17.88 s (-11.5%), discarded calls +15.3% of tool-seconds, a hit saving at most `min(decode, tool)` |
| dispatch a tool from the decode stream | Sutradhara ([arXiv 2601.12967](https://arxiv.org/abs/2601.12967)); Conveyor | tool calls 30-85% of the time to the final answer's first token; median up to -15%, end to end up to -11% |
| schedule the workflow as a unit | SAGA ([arXiv 2605.00528](https://arxiv.org/abs/2605.00528)); Murakkab (OSDI '26) maps declared workflows onto models and hardware | SAGA: task completion 1.64x faster than vLLM at about 30% lower peak throughput |

Every one is an inference-side system, most of them one engine or one serving cluster with its own
scheduler. None holds the other half of the loop: the sandbox a tool runs in, the host memory it
occupies, the executors it competes for, and the topology between them and the engine. That is the
narrower claim the phase can test -- `owned-and-observed.md` §5's properties 8 and 9 restated as
what a unified orchestrator adds to these systems, not what these systems add to nothing.

**Chosen: each mechanism is an arm in its published shape** beside the unified one, as Phase 8 built
`Sidecar` beside `Integrated` and Phase 9 llm-d's gate beside the claim: Continuum's TTL, Pythia's
staging in host memory, toolspec's speculate-and-discard, Sutradhara's dispatch from the stream.

### 1.4 Flows come in four grades, and the path already carries the third

What a scheduler can know about a downstream, and when:

| grade | source | what it names | lead |
|---|---|---|---|
| declared | the framework's own graph -- `taxo.md`'s fixed pipeline | the step and its static template | the steps before it |
| predicted | an estimator over history | a distribution over the next step, and a gap with its spread | from the upstream's start |
| observed in the stream | the tool call as it is decoded | the tool, with certainty; its arguments when they end | the rest of the decode after the tool's name |
| observed at arrival | the downstream itself | everything | none |

The third is the one the integrated path already holds: it relays every token
(`owned-and-observed.md` §2.6), so it sees a tool's name as the model emits it. How early depends on
the output's shape. Tool-call tokens are 70.4-98.2% of instant models' output on agentic benchmarks
([arXiv 2605.26297](https://arxiv.org/abs/2605.26297)), so the name arrives after the first 2-30% of
the decode; a thinking model reasons first and names the tool late.

A gateway can see it too. The Gateway API Inference Extension's endpoint picker parses streamed
response bodies through `ext_proc` to read token usage
([kubernetes-sigs/gateway-api-inference-extension#178](https://github.com/kubernetes-sigs/gateway-api-inference-extension/issues/178)),
and vLLM streams one token a chunk, so a callout a chunk is 36 us (`owned-and-observed.md` §2.2)
against an 8 ms step: half a percent of inter-token latency. What the gateway lacks is not the
observation but the act: the tool runs somewhere the gateway does not schedule, so the name has to
travel to whoever does.

**Chosen: four grades, each an arm of every flow consumer.** The estimator is `ToolGapIndex`'s
shape, widened. Keyed by observables -- the prompt's template root and the previous tool -- it keeps
a distribution over the next step, the gap as an empirical distribution over the key's recent
observations (Continuum's CDF, from which a mean, a variance and any quantile are read), and the
payload. The stream grade names the tool at a point of the decode set by the preset's share of
tool-call tokens, swept.

### 1.5 The next tool is predictable about as often as the commonest tool is called

Published next-tool accuracy is low and near the base rate. PASTE's patterns reach a top-1 accuracy
of 27.8% and a top-3 recall of 43.9%; toolspec's back-off n-gram a top-1 of 39.3%; and in Copilot's
traces one tool, `get_file`, is 35.0% of invocations, so predicting it every time is right about as
often. Within classes the structure is real -- 55% of successful edits are followed at once by a
terminal call, and 51% of searches by fetches of their top results (PASTE) -- and it is what an
estimator keyed by the previous tool learns.

**Chosen: presets carry a transition structure between tool classes, calibrated so that a top-1
predictor lands in that band, and accuracy is a swept condition.** The estimator is graded against
the declared and stream grades on the same programs, never against a hand-built baseline.

### 1.6 Authority is what the tool declares, and an unannotated tool is SideEffecting

MCP's `ToolAnnotations` (schema
[2026-07-28](https://github.com/modelcontextprotocol/modelcontextprotocol/tree/main/schema/2026-07-28))
are four hints: `readOnlyHint` (default false), `destructiveHint` (default true), `idempotentHint`
(default false) and `openWorldHint` (default true). Clients "must treat them as untrusted unless
they come from a trusted server", and "a tool with no annotations is assumed to be non-read-only,
potentially destructive, non-idempotent, and open-world" ([MCP blog,
2026-03-16](https://blog.modelcontextprotocol.io/posts/2026-03-16-tool-annotations/)). So authority
is declared, by the tool's server, pessimistically -- the shape `owned-and-observed.md` §4 asked for
-- and a scheduler that inferred it would be inventing a field the protocol already carries.

| authority | annotations | what may be done ahead |
|---|---|---|
| `ReadOnly` | `readOnlyHint` | run it if its world is closed; warm it if open |
| `DraftOnly` | not read-only and not open-world, inside the session's own sandbox | stage it in a fork of the sandbox; reclaim it freely |
| `SideEffecting` | anything else, including every unannotated tool | nothing: log its intent before dispatch, lease its cell |
| human-approved | a client's policy over a tool, not an annotation | pause the session (§1.9) |

The open-world clause is a privacy result, not a correctness one: a speculative call discloses
intent to whoever receives it, and "no commit-time cleanup, read-only restriction, or access-control
allow-list unsends what an observer already holds" ([Ghost Tool Calls, arXiv
2606.02483](https://arxiv.org/abs/2606.02483)). `DraftOnly` needs a sandbox the session owns and can
roll back, which is what makes an edit a draft: a mutation the session's snapshot lineage can undo.

**Chosen: `authority` on a tool, from its annotations with MCP's defaults,** and approval as a
client-policy bit. Speculation, victims, leases and the logged tier read it; nothing infers it.

### 1.7 Speculation buys at most the decode a hit overlaps, and the tools worth it are the ones it may not run

A correct guess runs the tool during the call that will ask for it, so it saves at most `min(decode,
tool)`; a wrong one costs the tool's work. On the program emulation (§8, *speculation*; read tools
speculated at the call's start, a guess right with probability equal to the accuracy, 12 sessions a
second; turn latency against no speculation, seeds 1 / 2 / 3):

| read tools' median, before compression | accuracy 0.28 | 0.39 | 1.0 | wasted for saved tool-seconds, 0.39, seed 1 |
|---|---|---|---|---|
| 100 ms -- coding: `get_file`, searches | -0.7 / -0.5 / -1.0% | -0.0 / -1.2 / -1.1% | -1.4 / -1.5 / -2.6% | 79 for 49 s |
| 2 s -- web search (BFCL's mean is 1.9 s) | -3.7 / -3.6 / -3.2% | -5.3 / -4.5 / -5.0% | -11.9 / -12.1 / -12.1% | 1471 for 682 s |
| 10 s -- fetch (GAIA's `WebFetch` mean is 29.8 s) | -4.1 / -3.6 / -4.2% | -5.8 / -5.5 / -4.6% | -14.1 / -13.7 / -13.9% | 7693 for 1285 s |

Copilot's traces say which row coding sits in, and why speculation cannot leave it. Read and search
tools complete in tens of milliseconds and succeed "close to 100% of the time"; the latency tail is
`run_command_in_terminal` and `run_build`, with means of 68 and 78 s and success near 73%, and
neither is read-only. For tool batches under 500 ms, over 99% of their time already overlaps an
active call, and only 7.7% of all tool time does. The tools worth speculating are the ones authority
forbids running, and the ones it permits are already hidden behind the decode. Research is the other
row: fetches are read-only, open-world and slow, so the gain is real and §1.6's privacy clause
decides whether it may be taken.

The emulation gives tools no capacity, so a wrong guess costs nothing but its memory. That is the
model's gap, not speculation's merit: in production a wasted tool-second is an executor-second
another session could have used.

**Chosen: speculation in grades** -- warm (restore the sandbox or cell, any authority), run
(`ReadOnly`, closed world), stage (`DraftOnly`, in a fork), never (`SideEffecting`) -- **admitted
when its probability times its saving exceeds the toll its work imposes**, in nanoseconds, the
congestion toll's form (`owned-and-observed.md` §3.9). Tool executors gain a capacity behind their
own bit (`--tool-slots`), so the toll is not zero where they are busy.

### 1.8 Retention through a tool call is bounded by what the call misses, about a percent here

Continuum's gain comes from what an evicted program does next on vLLM: it reloads, and it waits in a
first-come queue behind requests that arrived while its tool ran. Phase 5 bounded retention on the
open-loop trace at 1-2% of stall. On programs (§8, *gaps*; seed 1), a call's rebuild of its own
session's prefix beyond the blocks it adds:

| load | calls rebuilding beyond their new blocks, by gap since the session's last call | excess rebuild per call | a call's mean service |
|---|---|---|---|
| 6 sessions/s, mean batch 13 | 0.5% under 0.1 s, 5.0% at 30-120 s | 0.07-0.22 ms | 936 ms |
| 12 sessions/s, batch 22 | 9.6% under 0.1 s, 66.7% at 30-120 s | 2.3-12.0 ms | 986 ms |
| 18 sessions/s, batch 30 -- the published engine's | 22.5% under 0.1 s, 76.5% at 30-120 s | 7.0-14.7 ms | 1033 ms |
| 12 sessions/s, half the partition | 30.5% under 0.1 s, 53.3% at 30-120 s | 7.7-11.6 ms | 978 ms |

At most 1.5% of a call at every load and gap measured, because the scored argmin sends a call where
its prefix is and fetch pulls it when it is elsewhere: wherever misses exist, half to nine in ten of
the calls fetch part of their prefix. The queueing half of Continuum's benefit is an order -- who
waits behind whom -- and the router's class-ordered queue (Phase 9) already chooses it.

**Chosen: Continuum's TTL as an arm on programs,** its argmax computed from the gap distribution the
estimator keeps, graded against the excess-rebuild bound above and run on the engine that waits
(`--engine-wait`), where order matters.

### 1.9 The turn boundary is the reclamation signal, and the sandbox is the state that cannot be rebuilt

Copilot's traces split idleness in two. Within a turn a session's KV sits idle a median 1.2 s (P95
37 s) and its container 5.8 s (P95 44 s); across a turn boundary 172 s (P95 75 min) and 243 s (P95
90 min), and the user is away a median 25.2 min between turns. Cache hit rate holds above 95% for
idle gaps under two minutes and collapses between two and ten -- "the signature of a time-based KV
cache eviction policy at the serving system"; the paper gives Claude's default retention as 5
minutes, and OpenAI's `prompt_cache_retention` offers `in_memory` (5-10 minutes) or `24h`, the
default for organizations without zero data retention, and its caching "may store encrypted
key/value tensors in GPU-local storage"
([OpenAI](https://developers.openai.com/api/docs/guides/prompt-caching), read 2026-10-04). A
LightGBM survival curve over turn features predicts an idle gap over 60 s at ROC-AUC 0.73 and
captures 86-90% of idle time.

The sandbox side has three published lifecycles: E2B pauses at about 4 s per GiB of RAM and resumes
in about 1 s ([docs](https://docs.e2b.dev/sandbox/persistence)); AgentCore's session microVM idles
out after 15 minutes by default and keeps its filesystem for 14 days, a fresh microVM mounting it
"in a matter of milliseconds"; Agent Substrate hibernates RAM and disk into object storage and
activates the actor on any worker.

The two halves differ in the one way `owned-and-observed.md` §4 called an invariant. KV is
reconstructible from the transcript at a price. A sandbox that a `DraftOnly` or `SideEffecting` tool
has written is not: rebuilding it means replaying those tools in order, which a side effect forbids
and a build or a test run does not reproduce. So it is durable -- demotable, never droppable.

**Chosen: a lifecycle -- active, idle, suspended, resumed -- with `durable` Snapshot entries that
eviction demotes and never drops.** A session is suspended by rent-or-buy against the survival
curve, holding priced at host memory's shadow price and resuming at the restore on the next turn's
critical path, for its KV and its sandbox in one decision. A pause for approval is a program step:
the MCP task's `input_required`.

### 1.10 The logged tier is written per side-effecting call, and on an agent mix it is within a factor of a few of the soft tier

`owned-and-observed.md` §1 puts `SideEffecting` intents, suspended sessions and approval pauses in
the logged tier and asserts that the tiers' rates sit orders of magnitude apart. Agents make
side-effecting calls at the rate they make tool calls: in Copilot's traces LLM and tool calls run
1:1, and `run_command`, `run_build` and the edit tools are about 40% of invocations. On the
emulation (§8, *programs*), with exec tools `SideEffecting` and edits `DraftOnly`, an intent and an
outcome per side-effecting call come to 14.0 writes a second against 69.8 decisions at 6 sessions a
second, and 19.5 against 97.3 at 12: 20% of the soft tier's rate. Under MCP's pessimistic defaults,
edits included, 39%. A suspend and a resume record at each turn boundary would add about 14% more.
The record tier's one measured writer, Phase 6's planner, writes 0.046 times a second.

**Chosen: every logged write is counted by cause** -- intent, outcome, suspended-session record,
approval, task state -- per simulated second and per pattern, beside the soft tier's decisions and
the record tier's writes. Phase 10 takes the count.

### 1.11 Retrieval under prefix caching reuses the leading chunks, and their order is worth as much as the cache

A retrieved chunk's KV is reusable under prefix caching only behind the same prefix: the same system
prompt and the same chunks before it, in order. With an infinite cache and chunks drawn by Zipf
popularity (§8, *rag*; 50,000 queries, Zipf 1.1; chunk tokens reusable):

| corpus | k | chunks in a canonical order | in relevance order | position-independent |
|---|---|---|---|---|
| 10,000 chunks | 3 / 5 / 10 | 61.9 / 52.3 / 41.5% | 45.3 / 26.5 / 12.5% | 94.3 / 96.2 / 98.0% |
| 100,000 chunks | 3 / 5 / 10 | 56.2 / 47.7 / 37.8% | 37.5 / 21.9 / 10.4% | 84.6 / 87.1 / 90.2% |

Position-independent reuse -- CacheBlend's, recomputing about 15% of tokens -- is an engine
capability, so to the orchestrator it is promotion tier 2 with a price. The orchestrator's own
levers are which node holds a chunk set's leading chunk, and whether the framework orders chunks
canonically.

**Chosen: RAG is a retrieval step and a call.** Retrieval is a `ReadOnly` tool against a vector
index held as a service heap in host DDR; the call's prompt is the tenant prefix, k chunks keyed by
position, and the question. Chunk order is a condition, and position-independent reuse an arm with
its recompute charged.

### 1.12 Output length follows the role, and the role is the prompt's root

Phase 4 found that no estimator could beat the mean, because output length is drawn independently of
everything the router sees, and left the estimator to "a workload whose lengths depend on what the
router can see". Multi-agent frameworks are that workload: Pythia reports per-role output
distributions with coefficients of variation of 0.15-0.45 and means from 60 tokens (planner) to
2,620 (reviewer). The role is observable in the chain's shape, because an agent's system prompt is
its template and the root of its prefix.

On those shapes (§8, *roles*; arithmetic, the engineer role's mean assumed), a p90 claim over the
pooled distribution overruns on 10% of sequences in aggregate, but on 46% of reviewers' and 17% of
explorers', and never on planners' or chroniclers'. A p90 per role overruns on 10% of each, for 17%
fewer reserved output blocks. That is Phase 9's per-class finding on a role axis.

**Chosen: presets draw output by role,** at those coefficients scaled to the simulator, and the
observed-length estimator and Phase 9's claims key by template root, against the pooled
distribution.

### 1.13 Coupling lives on flows, so the table needs the decisions this phase adds

Coupled % split by the origin of the request deciding (§8, *coupling*; seed 1, `scored + fetch` at
rack, `--regret`; locality coupling, the share of scored decisions a silo's argmin would have made
differently):

| origin | 8 GiB DDR a node | 4 GiB | engine allocating, 8 GiB | engine allocating, 4 GiB |
|---|---|---|---|---|
| session turn | 0.00% of 4,284 | 0.00% | 0.00% | 0.00% |
| `FaaS` call, no flow | 0.00% of 3,314 | 0.00% | 0.00% | 0.00% |
| service | 0.00% of 3,679 | 0.00% | 0.00% | 0.00% |
| agent turn's tool call | 4.41% of 1,314 | 4.34% | 4.26% | 2.82% |
| fan-out agents' tool calls | 5.78% of 3,479 | 5.63% | 5.72% | 4.11% |
| fan-out resume | 2.89% of 450 | 3.78% | 6.22% | 4.67% |
| `FaaS` -> inference | 2.88% of 1,530 | 3.20% | 5.10% | 6.27% |

Locality coupling is zero by construction for a request with no flow, since the silo's score differs
from the unified one only in the handoff term and the pool it prices displacement in. Memory
coupling on the ledger is carried by function cells, 75-79% of their DDR evictions where DDR binds;
with the engine allocating it is 0.6-1.3% of 228-683 evictions per origin. So on today's definitions
the per-pattern table counts how much each pattern flows, and the patterns `owned-and-observed.md`
§4 expects to couple most -- multi-agent and long-running agents -- can only show it through
decisions this phase adds.

**Chosen: coupled % per pattern on both published axes, attributed to the pattern of the deciding
request, and a coupled % for each joint decision the phase adds** -- where a speculative tool runs,
whether a session's KV and sandbox are suspended together, and whether a multi-agent program is
admitted whole -- each against a stated silo: an agent framework speculating in its own sandbox; an
engine's TTL and a sandbox platform's idle timeout at their deployed defaults; admission per agent.

### 1.14 Ten patterns, nine presets, and a clock five times fast

| pattern (`taxo.md`) | program | flow grade | grounding | authority | `slo` | calibrated on |
|---|---|---|---|---|---|---|
| one-shot generation | one call | none | closed | `ReadOnly` | interactive | the published draws |
| extraction / classification | one call, long prompt, short output | none | prompt | `ReadOnly` | throughput | Phase 6's `--fresh` shape |
| conversational assistant | turns of one call, user idle between | none | fixed context | `ReadOnly` | interactive | the published sessions; idle chosen |
| fixed-pipeline RAG | retrieve, then a call | declared | RAG | `ReadOnly` | interactive | §1.11 |
| workflow / tool pipeline | fixed steps: call, tool, call, side effect | declared | tools | mixed | either | chosen |
| agentic workflow | turns of a call-tool loop | predicted, stream | hybrid | mixed | interactive | Copilot, TraceLab |
| long-running agent | the same over hours, with approvals and suspension | predicted, stream | hybrid | mixed, approvals | throughput | Copilot's idle; AgentCore, E2B, Substrate |
| multi-agent system | an orchestrator's call fans out to agent programs; a call synthesizes | declared fan-out, predicted within | shared tools | mixed | either | Pythia's roles; PASTE's research patterns |
| multimodal real-time | -- | -- | -- | -- | -- | not modelled: no stream input, no per-frame deadline |
| batch inference | independent calls | none | optional | `ReadOnly` | throughput | Phase 9's `--batch` shape |

Copilot's traces set the agentic shape: per turn a median of 4.5 calls and 4 tools (means 6.6 and
7.6); 87% of calls agent-initiated; tools 35.0% `get_file`, 17.0% `run_command_in_terminal`, 9.8%
`replace_string_in_file`; a tool's duration median 166 ms, P90 4.4 s, P99 79 s, mean 16.7 s; three
user turns a session at the median. TraceLab's Claude Code and Codex traces give 8.8 steps and 10.8
tool calls a request, with 88% of LLM rounds answering a tool.

The simulator's call is about a fifth of Copilot's -- a mean near 1.0 s on the published engine
against a 5.3 s median -- so tool and idle durations are divided by the same factor, `--compress 5`,
and swept at 1 and 10: the ratios between calls, tools and idleness are the source's, the absolute
times the model's.

**Chosen: nine presets,** named by pattern and mixed by share as `sched_lm`'s `--mix` does;
multimodal real-time is named in every table and marked unmodelled. A request's pattern is
generator-side truth and never reaches the score (`owned-and-observed.md` §4, *Generator-side
truth*).

### 1.15 Regimes, workloads, and how each effect is graded

- **The cluster** is the `belief` cluster: 4 nodes, 16 GiB HBM, 32 GiB DDR and 64 GiB `NVMe` in
  total, rack, no control crossing charged, `scored + fetch`, the engine allocating with decode
  output held, its grants sized from the ledger's run.
- **Load** in programs a second: the rate that runs the engine at the published engine's mean batch
  (about 30 at 250 req/s; 18 sessions a second on the agentic preset), and half of it.
- **Capacity:** the published partition and half of it; host DDR at 8 and 4 GiB a node, the second
  where sandboxes, function cells and offloaded KV contend.
- **Presets** one at a time, and one mix; the mix's shares are chosen and printed.
- **Conditions swept:** `--compress` at 1, 5 and 10; a predictor's accuracy at 0.28, 0.39 and 1.0;
  the stream's tool-token share at 0.2, 0.7 and 0.98; read tools' latency for the research preset.
- **Grading,** per pattern: turn and program latency, mean, p50 and p99; the critical path split
  into call, tool, wait, restore and handoff; time to a tool's start; prefill work, speculative
  tool-seconds and restores, used and wasted; logged writes by cause; coupled % on each axis and
  each joint decision. Three seeds a cell, and every figure carries its preset, compression, load
  and partition.

---

## 2. Predictions, stated first

`owned-and-observed.md` §7's rule. Twelve predictions, each attached to a claim it would rewrite.
Where a pre-measurement stands behind one, §8 says how it was taken; P3, P5, P7, P10 and P11 rest on
arithmetic or on none, and say so, as do P12's added decisions. Any of them may be pre-measured
before the build; one whose pre-measurement moves its band is restated before the run, with the
original kept beside it.

**P1 -- The trace runs an agent's tool call during the turn that issues it.**

On the published trace, every agent turn's tool call and fan-out dispatch arrives before the turn
finishes, by a median of 0.9-1.1 s; 45-55% of resumes arrive before their slowest agent; 15-20% of a
session's consecutive turns overlap; and a `FaaS` call's inference arrives after the call. On
programs, a turn's mean latency is 4-5 times what the same scripts report replayed open loop, and a
session's 2-2.6 times.

- *If right:* every flow figure in the documents is restated as an open-loop figure, and Phase 7's
  latencies are taken on programs only.
- *If wrong* (programs within 2x of the replay): calls rarely overlap in the replay, the open-loop
  trace was a fair proxy for a task's latency, and the first thing to check is the calls' duration
  against the 28 ms lead.

**P2 -- Prefill-ahead keeps about half its open-loop win under an estimate, and nothing of it on a
causal lead.**

On the published cluster, the declared template without its call blocks cuts the flow downstream's
stall by 45-50% at the published partition and 35-43% at half of it; a learned template by 22-34%
and 18-27%, at 2.5-4 seconds of prefill work per second of stall saved against the declared hint's
1.5; and no gate on the learned probability keeps the template-only figure, since every function's
is the same. On programs, prefill-ahead of a `FaaS` call's inference moves its downstream's first
token by under 2% in every grade, because the lead is the function's 40-200 us; a pipeline whose
preceding step outlasts its template's 9.6 ms of prefill keeps the template-only figure within ten
points.

- *If right:* for the README's own example the coupling-tier-1 win is the residency routing already
  finds. What survives is a pipeline's declared template and the agent loop's warm-ups (P3), and
  `owned-and-observed.md` §5's `announce` row is restated as an open-loop figure.
- *If wrong* (a causal `FaaS` flow keeps more than 10%): prefill-ahead's value was never its lead
  but where it puts the template, and the landing target is the mechanism to keep.

**P3 -- The stream names the tool early enough to warm what the model restores, and a predictor adds
little to it.**

*Arithmetic on a published share; no pre-measurement.* With the name streamed after 2-30% of the
decode, a stream-graded warm-up lands before the tool is called wherever the restore is shorter than
the rest of the decode, which for a 9 ms cell on the published engine is every call: the stream
grade is within 0.2% of the declared grade's turn latency on the agentic preset, and a predictor at
0.28-0.39 adds under 0.5% beyond it. With a thinking model's share -- the name after 80% of the
decode -- the stream still covers a 9 ms restore and stops covering a 1 s resume of a suspended
sandbox.

- *If right:* the integrated path's knowledge of an agent's flows is mostly observation, and
  `owned-and-observed.md` §3.2's estimator earns its keep only for the time before a name streams.
- *If wrong* (a predictor adds more than 2% beyond the stream): warm-ups sit on the critical path
  for longer than the decode leaves, and prediction is worth building for that time.

**P4 -- Speculation is worth under 1.5% on coding and 3-7% on research at published accuracies.**

On the agentic preset with Copilot-shaped read tools, speculating them at accuracy 0.28-0.39 moves
turn latency by under 1.5% and wastes more tool-seconds than it saves; at a perfect accuracy, under
3%. On the multi-agent research preset with reads of 2-10 s, -3 to -7% at 0.28-0.39 and -11 to -15%
at perfect, with 2-10 times as many tool-seconds wasted as saved. With executor slots at 80%
utilisation, toolspec's unpriced shape lengthens other sessions' tool waits by more than it saves,
and the priced gate does not.

- *If right:* property 9's speculative half is a property of the research pattern, set by authority
  times latency and not by prediction: coding's slow tools are side-effecting.
- *If wrong* (coding gains over 3%): fast reads sit on the critical path more than Copilot's overlap
  figures say, and the overlap instrument is the first thing to read.

**P5 -- Leases and drafts bind only where host DDR does.**

*Arithmetic on the emulation's tool mix; no pre-measurement.* A `SideEffecting` execution's lease
pins under 2% of host DDR at every load and capacity measured; reclaiming `DraftOnly` sandboxes
moves an interactive program's turn latency by under 1% at 8 GiB a node, and by more only at 4 GiB;
and no `SideEffecting` execution is ever preempted, by assertion.

- *If right:* authority's preemption half is a correctness rule with no measurable price on this
  model; the lease is free and reclaiming drafts is a regime effect.
- *If wrong* (leases pin more than 5%): long side-effecting executions hold memory others need, and
  the lease becomes a cost the scheduler should price before dispatch.

**P6 -- Retention through a tool call buys at most the rebuild a call misses.**

Continuum's TTL on programs moves turn latency by under 1.5% at the published partition at mean
batches of 13 to 30, and by under 3% at half the partition, on the published engine and on the
engine that waits; its gain never exceeds the excess rebuild a call pays without it, 0.1-15 ms.
Under the router's class-ordered queue the ordering benefit Continuum measures on a first-come
engine does not appear.

- *If right:* property 8 (learned cross-class retention) is bounded at about a percent wherever
  routing, fetch and an offload tier exist, and that is the promotion-tier-2 number for a retention
  API.
- *If wrong* (the TTL gains more than 3% at the published partition): retention changes placement or
  order by more than the excess rebuild counts, and Phase 5's ceiling was measured on the wrong
  trace.

**P7 -- One suspend decision for KV and sandbox differs from two timers on about a fifth of idle
gaps.**

*Arithmetic on published figures, no pre-measurement.* Under Copilot's cross-turn idle (KV median
172 s, P95 75 min; container median 243 s, P95 90 min), an engine's 5-minute retention and a
sandbox's 15-minute timeout disagree on 15-25% of turn boundaries -- a lognormal through either pair
of quantiles gives 19-21%. On the long-running preset the joint rent-or-buy differs from the pair on
at least those, frees 10-20 points more of the idle sandbox-seconds than the 15-minute timeout's 73%
(a heavy tail lets a timeout free most of them), and costs the next turn under 2% of its latency in
resumes.

- *If right:* lifecycle coupling is a real joint decision and a modest one; the density Agent
  Substrate reports comes from suspending at all, not from suspending jointly.
- *If wrong* (the joint decision frees more than 50% more): the timers' blind spot is larger than
  the idle distribution suggests, and the survival estimate is the mechanism worth publishing.

**P8 -- The logged tier writes 20-60% of the soft tier's decisions on the agent presets.**

An intent and an outcome per side-effecting call put the logged tier at 18-22% of the soft tier's
decision rate on the agentic preset with exec tools side-effecting, and 35-43% under MCP's defaults
with edits included; suspended-session records at each turn boundary of the long-running preset add
10-20 points; one-shot, extraction, batch and chat without tools write nothing; and the record tier
stays three or more orders of magnitude below the soft one.

- *If right:* `owned-and-observed.md` §1's "orders of magnitude apart" holds for the record tier and
  fails for the logged one, so §8's logged-tier store is sized from the soft tier's rate and not the
  session rate, and Phase 10's count decides whether FoundationDB carries it.
- *If wrong* (the logged tier under 5% of the soft one): side-effecting calls are rarer or batched
  where it counts, and the tiers do sit apart.

**P9 -- Retrieval reuse is set by chunk order under prefix caching, and position independence is
worth more than any order.**

On the RAG preset at the published partition, the chunk tokens reused under prefix caching are at
most the infinite-cache figures and within 15 points of them -- about a quarter in relevance order
and half in a canonical one at k = 5 -- against 75-95% with position-independent reuse, whose 15%
recompute still leaves it ahead of a canonical order by a quarter to a third of the call's chunk
prefill.

- *If right:* for retrieval the orchestrator's lever is where the leading chunk sits, the
  framework's is the order, and the engine capability is the larger ask (promotion tier 2), with its
  number.
- *If wrong* (a finite partition costs more than 15 points): chunk working sets are wider than Zipf
  1.1 makes them, and the index's popularity skew is the constant to replace with a trace.

**P10 -- With lengths by role, a role's own quantile replaces the pooled one.**

*Arithmetic on Pythia's role shapes, no pre-measurement.* On the multi-agent preset, a pooled p90
claim overruns on 40-50% of the longest role's sequences and 15-20% of the most variable role's, and
never on short roles; a per-role claim overruns on 8-12% of each and reserves 10-25% fewer output
blocks; the longest role's first-token p99 under a pooled claim is the one Phase 9's cancel then
protects or sacrifices, and keying by template root removes the need. The score reading per-role
means moves mean service by 1-5% against the pooled mean, where Phase 4 found nothing to move.

- *If right:* Phase 4's estimator question has its workload at last, and Phase 9's per-class claim
  generalizes to whatever the prompt's root says about the request.
- *If wrong* (per-role means move service by under 0.5%): output length still matters only to
  admission, and the score's indifference to it is structural, not a property of the old trace.

**P11 -- The workload class is recoverable from observables, so its inference costs little.**

*No pre-measurement.* A classifier over tool results in the history, chain growth per call, the gap
since the session's last call and the template root names the preset of 90% or more of requests, the
misses concentrated on sessions' first calls; and on every consumer -- suspension timing, the
speculation gate, the claim's quantile -- the inferred arm lands within 1% of the truth arm.

- *If right:* `owned-and-observed.md` §1's "workload class | inferred" row is closed by observables
  already on the path, and `taxo.md` is a measurement variable rather than a declaration callers
  must make.
- *If wrong* (a consumer loses more than 3% to inference): presets overlap in what the router sees,
  and the class is worth declaring, as `slo` is.

**P12 -- The table: coupling is confined to the patterns with flows and to the decisions this phase
adds.**

Locality coupling is 0.0% for one-shot, extraction, chat without tools and batch, and 2-7% for
pipelines, RAG, agentic and multi-agent programs. Memory coupling with the engine allocating is
under 2% for every pattern but the long-running agent, where suspended sandboxes and offloaded KV
share host DDR and it reaches 5-20% where DDR binds. Of the added decisions, the joint suspend is
coupled on 15-25% of idle gaps (P7), speculation placement on under 10% of speculative runs, and
multi-agent admission completes more programs all-or-nothing where admission binds, as fan-outs do
today.

- *If right:* `owned-and-observed.md` §4's expected shape holds in direction and is narrower than
  its §5 states: the unified orchestrator helps where workloads flow into each other or share host
  memory over time, and nowhere else, which is a result.
- *If wrong* (a pattern with no flow couples): a decision the silo definition does not see is
  coupled, and the silos are drawn too narrowly to be honest.

---

## 3. What a program scheduler must and must not do

Twelve rules. The first is the gate; the third is the one most likely to be broken for a good
reason.

1. **The gate.** With every new bit off, byte-identical to `HEAD` on the reproducible set as
   `phase-9.md` left it -- `residency`, `flows`, `placement`, `volatility`, `ownership`, `price`,
   `belief`, `influence`, `fleet`, `enforce`, and `distributed --crossing native` with its measured
   lines filtered. Each bit has a case in which it must change nothing: programs at a rate of zero;
   `--hints declared` with programs off; `--speculate off`; `--tool-slots` unbounded; `--suspend
   never`; retrieval with k = 0; one role. Checked after every work item.
2. **Content is drawn, timing is earned.** A program's script comes from a stream of its own, keyed
   by the seed and the program; arms differ in when its steps run, never in what they are (Phase 4's
   rule 6, Phase 9's rule 8).
3. **Generator truth never reaches the score.** Pattern, next step, gap, output length and role are
   for instruments only; the score and its consumers read `RequestView`, which gains the template
   root and the tool's declared authority and nothing else. The stream grade reads the next tool
   only at the instant the stream would show it. Arms that read the truth earlier are ceilings,
   printed beside the arms and never called arms.
4. **Authority is declared, with MCP's defaults.** Nothing infers it. Speculation never runs a
   `SideEffecting` or open-world step; a running `SideEffecting` step is never preempted and its
   cell never evicted; its intent is counted before its dispatch.
5. **Nothing tuned.** A gate is an expected value in nanoseconds against the toll its work imposes;
   a TTL is Continuum's argmax over the gap distribution; suspension is rent-or-buy. The declared
   inputs are `slo`'s quantile and authority, as before.
6. **Every preset constant has provenance:** published, with its source, or chosen, with its sweep.
7. **One bit per mechanism,** and the headline runs change one at a time: `--programs`,
   `--compress`, `--hints`, `--speculate`, `--tool-slots`, `--suspend`, `--rag`, `--rag-order`,
   `--rag-pi`, `--roles`, `--classify`.
8. **Durable state is never dropped.** A durable entry leaves a tier only for a lower one; losing
   one is a correctness failure, asserted.
9. **Engine KV is never written** (`own::authority`). A TTL or an evict-first is a directive (Phase
   5), a warm-up of engine state is a dispatch, and the census stays at 13. Sandboxes, executors and
   the logged tier are orchestrator-owned.
10. **Measure, do not repair** (`phase-2.md` rule 1).
11. **The served set is held equal:** programs that complete, with refused and abandoned programs
    counted apart.
12. **Three seeds, and the preset, compression, load and partition with every number.**

---

## 4. Work items

The phase is one pass through four increments, in order, each ending with a number: programs and
the table's first column; flows; authority; sessions, retrieval and the full table. Within each,
nothing that can move a number lands before the items that cannot. If the phase has to stop early
it stops at an increment's end.

### 4.1 Patterns as tags

`Request::pattern`, set by the generator and read by instruments only, which the compiler keeps out
of `RequestView`. Coupled % on both axes is split by it on the existing trace: session turns as
conversational, agent turns calling a tool as agentic, `FaaS` calls with a flow as a pipeline,
fan-outs as multi-agent, other `FaaS` calls and services as non-AI. No consumer: a no-op, checked as
one.

### 4.2 The event clock

`Machine::submit_at(t, req)`: the clock moves to the later of now and `t`, the arrival's
housekeeping runs there, and the request is submitted at that instant. Base requests keep their
instants. With programs off nothing calls it.

### 4.3 Programs

A script of steps -- `Call` with its template root, appended blocks and output by role; `Tool` with
its tool, annotations, latency and result blocks; `Idle` for a user or an approval; `Fanout` over
child programs -- drawn per program from its own stream. A driver submits each step when its
predecessor closes, through `submit` and `drain_closed`, so a step that waits at the router or an
engine (Phase 9) delays its program. A program's chain grows by its calls' output and its tools'
results.

### 4.4 Sandboxes and executors

A session's sandbox is a Snapshot lineage of its own (32 MiB at the model's scale, chosen), restored
at the model's lazy-restore cost; a tool runs in it for its latency. Executor slots per node behind
`--tool-slots n`, unbounded by default: a tool waits for a slot, and the score prices the wait in
the congestion toll's form.

### 4.5 Presets

The nine of §1.14, each a distribution over scripts that prints its provenance, mixed by `--programs
agentic=0.4,multiagent=0.2,...`; `--compress`.

### 4.6 Instruments, first part

Program and turn latency; the critical path split into call, tool, wait, restore and handoff; the
decode-tool overlap; a call's excess rebuild by gap (§1.8); all per pattern.

### 4.7 Hints by grade

`--hints none | declared | predicted | stream`. Declared names a pipeline step's template at the
pipeline's start; predicted is the estimator's; stream names the tool once `1 - share` of the call's
output has streamed, `share` being the preset's tool-token share (`--tool-tokens`); none is today's
blind arm.

### 4.8 The estimator

Keyed by template root and previous tool: a next-step distribution from counts, the gap as an
empirical distribution over the key's last observations -- bounded for memory, as `ToolGapIndex`'s
table is -- and the payload; keyed by template root, output length as a histogram (Phase 9's
`Lengths`). The window is a memory bound, printed, not a policy knob.

### 4.9 Flow consumers

A warm-up restores a sandbox or function cell in host DDR when the probability times the restore it
saves exceeds the cell's displacement price -- the ledger's own, in nanoseconds. Prefill-ahead
prefills a declared template, Phase 5's mechanism and target. A TTL is Continuum's argmax over the
gap distribution, sent as a retention directive on Phase 5's channel.

### 4.10 Authority from annotations

`Tool::annotations`, MCP's four hints with their defaults, and `authority()` as §1.6 maps it;
approval as a client-policy bit on a tool.

### 4.11 Speculation

`--speculate off | warm | run`. `run` executes a predicted `ReadOnly`, closed-world call at its
calling step's start, or a streamed one once its arguments end; a hit serves the real call, a miss
is cancelled when the real tool is known. Admitted when the probability times `min(remaining decode,
tool)` exceeds the toll on executor slots. toolspec's shape -- speculate every predicted read and
discard on a miss -- is the comparison arm. Wasted tool-seconds are counted.

### 4.12 Drafts and leases

`DraftOnly` work is a victim class for Phase 9's cancel and a reclaimable cell in host DDR. A
`SideEffecting` execution holds a lease, its cell pinned and its slot unpreempted until it
completes; its intent is counted before its dispatch and its outcome after.

### 4.13 Lifecycle and durable entries

Idle and approval steps; a survival estimate per template root from completed idle gaps; suspension
at a turn boundary by rent-or-buy, for the sandbox (demoted to `NVMe`, or dropped if not durable)
and for the KV (an evict-first directive, or the connector's spill) in one decision; resume on the
next step. `durable` on a Snapshot entry a `DraftOnly` or `SideEffecting` tool has written: eviction
demotes it and never drops it, asserted. Two timers -- an engine retention of 5 minutes and a
sandbox timeout of 15 -- are the silo arm.

### 4.14 The logged tier's count

Writes by cause per simulated second and per pattern, beside the soft tier's decisions and the
record tier's planner writes.

### 4.15 Retrieval

A retrieval step is a `ReadOnly` tool against a service heap per index; chunk chains sit under the
tenant prefix by position; `--rag-order canonical | relevance`; `--rag-pi` keys a chunk by its
content and recomputes 15% of its blocks.

### 4.16 Roles and class inference

Per-role output distributions in the presets; the length estimator and Phase 9's claims keyed by
template root; a classifier over observables -- tool results in the history, chain growth per call,
the gap since the session's last call, the template root -- with `truth | inferred | none` arms for
each consumer.

### 4.17 Instruments

| instrument | measures | over |
|---|---|---|
| critical path | turn and program latency by pattern; the split into call, tool, wait, restore, handoff | every program |
| causality | downstreams arriving before their upstream ends; turns overlapping | every flow, on any trace |
| hints | grade, lead, landed, prefill work used and wasted | every hint |
| estimator | top-1 and top-3 accuracy, gap calibration, length error by key | every prediction |
| speculation | runs, hits, tool-seconds saved and wasted, executor wait imposed | every speculative step |
| retention | excess rebuild by gap; TTL pins and pressure evictions | every call |
| lifecycle | suspends, resumes, resume on the critical path, host bytes freed, durable demotions | every idle gap |
| logged tier | writes by cause, per second and pattern | every write |
| coupling | locality and memory coupled % by pattern; each joint decision against its silo | every decision |

These are the instruments the pre-measurements approximated, and §8's figures are reproduced with
them first.

### 4.18 `polyphonic programs`

A reproducible sweep, as `enforce` is for Phase 9: no control crossing charged, seed-deterministic,
three seeds a cell.

1. the gate, as printed check lines
2. causality on the published trace, and programs against their open-loop replay (P1)
3. hints by grade, on the published trace and on programs (P2, P3)
4. speculation by preset, accuracy and executor slots (P4)
5. leases and drafts (P5)
6. retention through tool calls (P6)
7. the lifecycle against two timers (P7)
8. the logged tier's count (P8)
9. retrieval (P9)
10. roles (P10)
11. class inference (P11)
12. the table (P12)

`distributed` gains `--programs` beside its trace; `code-review` takes none of this phase's flags.

### 4.19 Report and publish

| target | change |
|---|---|
| `owned-and-observed.md` §9 Phase 7 | a **Status** line; the phase restated as programs, flows by grade, authority and the lifecycle |
| `owned-and-observed.md` §1 | the durability table's rates measured, the logged tier's writers named and counted (P8); the state table's per-tool gap, `P(turn calls a tool)`, workload class and side-effect intents marked built |
| `owned-and-observed.md` §3.2 | flows by grade, with the estimator's accuracy and what each grade buys (P2, P3) |
| `owned-and-observed.md` §4 | `flow: Predicted` built; RAG built; `retention: durable` built for sandboxes; `authority` built from annotations; the per-pattern table |
| `owned-and-observed.md` §5 | properties 8 and 9 with their sizes and patterns; the `announce` and prefill-ahead rows restated as open-loop figures, the published figures kept in the ledger (P1, P2) |
| `owned-and-observed.md` §8 | the logged tier's store against Phase 10's count |
| `residency-ledger.md` | a *Programs* section, every figure with its preset, compression, load and partition; *Standing* rows for each prediction, and the `announce` and prefill-ahead rows marked open loop with their published figures kept |
| `phase-5.md` | nothing: it keeps its results as measured |

---

## 5. Verification

- **Byte-identity with every new bit off**, against the commit before this phase, on the
  reproducible set at a reduced `--ops` and a second seed, after every work item.
- **The gate** (rule 1), one check line per bit.
- **Every program completes or is counted** refused or abandoned; every step is submitted once and
  only after its predecessor closed; the clock never moves backwards.
- **Content is identical across arms:** a program's steps hash the same under every arm.
- **Truth stays out of the score:** `RequestView` has no pattern, next step, gap, length or role,
  which the compiler enforces.
- **Authority holds:** no speculative run of a non-`ReadOnly` or open-world tool; no lease broken;
  every `SideEffecting` dispatch preceded by its intent in the count; all asserted.
- **Durable entries are never dropped,** asserted at every eviction.
- **The census.** `cargo build --release --features census` still emits 13 warnings.
- `cargo fmt --check`, `cargo clippy --all-targets` and `cargo test` clean.

---

## 6. Risks

1. **Compression is a modelling choice.** The simulator's call is a fifth of a production one; tools
   and idle gaps are compressed to keep the source's ratios, and every result is swept at 1 and 10.
   A result that flips across the sweep is published as flipping.
2. **The presets lean on coding,** a weighting accepted with this plan. Copilot's and TraceLab's are
   the only production-scale traces; the research and multi-agent presets rest on benchmark
   characterizations (Pythia, PASTE, GAIA, BFCL), and say so.
3. **Executors with no capacity flatter speculation.** The pre-measurements had none; the slot model
   is chosen, and its count is swept.
4. **The stream's share is from instant models.** Thinking models emit reasoning first; the share is
   swept from 0.2 to 0.98, and P3 is graded at each end.
5. **The authority mapping decides the logged count.** Exec-only against MCP's pessimistic defaults
   is a factor of two (20% against 39%), and both are printed.
6. **Copilot's idle figures are not one distribution.** Cross-turn KV idle has a median of 172 s and
   user idle between turns one of 25.2 min, measured on different populations; both are quoted with
   their definitions, and P7's arithmetic uses the first.
7. **The pre-measurements are emulations outside the repository.** Coupling by origin is one seed;
   the program driver runs without the base trace, without a queue or a cancel, with a 3 s think
   time and with speculation accounted at the driver rather than inside the machine; the learned
   template reads the downstream's arrival rather than a framework's report. §8 says how each was
   taken.
8. **The first column is a mapping, not the presets.** Pattern tags on the existing trace preview
   the table; the presets replace them.
9. **Published systems as arms are shapes, not their numbers** (Phase 9's risk 6).
10. **Runtime.** A long-running preset covers hours of simulated time; the cost is the event count,
    not the simulated span, and runs are parallel processes.

---

## 7. Out of scope

- **Multimodal real-time.** No stream input, no per-frame deadline, no audio output; the row is
  named and unmodelled.
- **Context compaction and model switches.** Copilot's traces show both resetting a session's cache
  (compaction in 7.8% of sessions, a model switch leaving 8% of the cache), which is a placement
  question for Phase 6's planner, not a flow.
- **Privacy contracts for open-world speculation.** Open-world reads are warmed and never run; Ghost
  Tool Calls' issue-time policies are a successor.
- **Real traces.** Copilot's sanitized traces are promised; the presets take a trace when one lands.
- **Learned models beyond counts.** The survival curve and the next-step distribution are empirical
  per key; Copilot's gradient-boosted predictor (ROC-AUC 0.73) is the reference, not an arm.
- **Durable memory beyond the sandbox** -- a memory store across sessions. The logged tier counts
  its records; the store is §8's.
- **Fan-out agents as victims, and gangs queued whole.** Phase 9's successor; a multi-agent program
  is admitted all-or-nothing as fan-outs are today.
- **Regions** (Phase 11), and **the store itself** (infrastructure, §8).

---

## 8. Pre-measurements

All taken on an instrumented copy of `4dcdefb`, run outside the repository and not committed. With
every hook off the copy reproduces `influence --sections prefill` to the digit, and each split
counter sums to the machine's own at the same settings. Rows use `distributed`'s or `influence`'s
defaults -- 4 nodes, 16 GiB HBM, 32 GiB DDR and 64 GiB `NVMe` in total, rack, 250 req/s, 10%
fan-out, 15,000 requests -- unless they say otherwise.

| name | what | how | headline |
|---|---|---|---|
| *causal* | whether a downstream waits for its upstream | `drive` records each request's arrival, service and flow task, read-only; a downstream is early when it arrives before its upstream's arrival plus service; `scored + fetch` at rack, the ledger on seeds 1-3 and the engine allocating with decode output held on seed 1 | §1.1's first table |
| *coupling* | coupled % by origin | the locality and DDR memory counters split by the deciding request's origin -- turn, tool, fan-out agent tool, resume, `FaaS`, `FaaS` -> inference, service -- through a thread-local set at submission; `--regret --crossing native`, seed 1, 8 and 4 GiB DDR a node, the ledger and the engine allocating; totals equal the machine's own counters | §1.13's table |
| *hints* | prefill-ahead's hint, honest and learned | `influence` section 4 with the hint replaced: its first 24 blocks only; or a per-function template learned from the first downstream to arrive, sent on every later call of that function, the landing tracked only for calls that flow; seeds 1-3 | §1.2's table; a learned template for 74.6 / 73.0 / 73.6% of flows |
| *programs* | closed-loop agent sessions | a driver over the library's `Machine` with no base trace: 900 sessions a run, arriving Poisson at 6, 12 or 18 a second; turns 1 + geometric (continue 0.55, at most 6); a turn is a call then (tool, call) repeated, the count geometric (continue 0.78, at most 16); tool classes read, edit and exec by a Markov chain; latencies lognormal with medians 100 ms, 400 ms and 1.5 s and sigmas 1.0, 0.5 and 1.8, divided by 5; results 2-6 blocks; outputs 24-223 tokens; think time lognormal, median 3 s; a 32 MiB sandbox a session; the engine allocating with a 1 GiB partition, 0.82 GiB offload and 4 GiB spill, decode output held, prompt-only reservations, `scored + fetch`, flow-aware; each step submitted when its predecessor closes, or 28 ms after its arrival open loop | §1.1's second table; read / edit / exec 56 / 21 / 23% of tools; 69.8 and 97.3 decisions a second at 6 and 12 sessions a second |
| *speculation* | read tools run during their call | as *programs* at 12 sessions a second; a read tool that follows a call is guessed at the call's start, right with probability equal to the accuracy; a right guess runs it then, a wrong one runs another read tool then and the real one after the call; tools have no capacity | §1.7's table |
| *gaps* | a call's misses of its own prefix | as *programs*, closed loop at 6, 12 and 18 sessions a second and at half the partition; a call's recompute beyond its appended blocks, and whether it fetched, by the gap since its session's previous call ended; seed 1 | §1.8's table |
| *rag* | chunk reuse with no simulator | a Python trace of 50,000 queries drawing k distinct chunks by Zipf popularity over 10,000 or 100,000; a chunk reusable under prefix caching if every chunk before it in the query's order was seen before in that order, under position independence if it was seen anywhere; canonical order is by rank, relevance order a per-query permutation; infinite cache | §1.11's table |
| *roles* | pooled against per-role claims | arithmetic on Pythia's per-role means and coefficients of variation, lognormal by role, the engineer role's mean (1,400) and CV (0.30) assumed; planner, explorer, engineer, reviewer and chronicler weighted 1 / 3.5 / 4.5 / 1 / 1, the midpoints of its coding workflow's regular expression, the chronicler's weight assumed | §1.12: pooled p90 overruns 46% of reviewers', 17% of explorers'; per-role reserves 17% fewer blocks |
| *timers* | two silos' timers against one decision | arithmetic: a lognormal through Copilot's cross-turn container idle (median 243 s, P95 90 min, sigma 1.89) puts 21% of gaps between 5 and 15 minutes and lets a 15-minute timeout free 73% of idle sandbox-seconds; through the KV idle (median 172 s, P95 75 min, sigma 1.98), 19% | P7 |
