use crate::blob::{BlobId, BlobKind, BlobMeta, ROOT};
use crate::flow::FlowHint;
use crate::rng::Rng;
use std::collections::{HashMap, VecDeque};

/// A content-addressed chain: an ordered blob list where each entry's parent is its predecessor.
pub type Chain = Vec<(BlobId, BlobMeta)>;

pub const KV_BLOCK_BYTES: u64 = 512 * 1024;
pub const KV_BLOCK_NS: u64 = 400_000;
/// Guest memory of one warm microVM cell. Small for a Firecracker guest and deliberately so:
/// the VMM's own footprint is a few MiB, and a function's guest is sized to the function.
pub const SNAPSHOT_BYTES: u64 = 32 * 1024 * 1024;
/// Fixed cost of resuming a Firecracker snapshot: VMM setup, device restore, and mapping the
/// memory file. Independent of image size, which is the property that matters.
pub const SNAPSHOT_RESTORE_NS: u64 = 4_000_000;
/// Fraction of a restored guest's pages an invocation actually touches. Lazy restore faults
/// pages on demand, so this -- not the image size -- is what a cold start pays for.
pub const SNAPSHOT_WORKING_SET: f64 = 0.15;
/// Fault-driven page-in over `UFFD`, roughly 1 `GiB/s`: a userfaulting round trip per fault is
/// far short of a `memcpy`, which is why the touched fraction is worth modelling at all.
pub const SNAPSHOT_PAGE_IN_NS_PER_BYTE: f64 = 1.0;

/// Cost of bringing a warm cell back, under **lazy snapshot restore** rather than a cold
/// container start.
///
/// This is a deliberate commitment to the Firecracker model, and it is not a tuning change.
/// A cold container start is linear in image size and measured in hundreds of milliseconds; a
/// demand-paged snapshot resume is a fixed few milliseconds plus the working set, which makes
/// it roughly **flat** in image size. The two differ by more than an order of magnitude and
/// they order the ledger's eviction priorities differently, so the model has to say which one
/// it means. A v8-isolate substrate would be a third answer again and is explicitly not this.
#[must_use]
pub fn snapshot_restore_ns(bytes: u64) -> u64 {
    SNAPSHOT_RESTORE_NS
        + (bytes as f64 * SNAPSHOT_WORKING_SET * SNAPSHOT_PAGE_IN_NS_PER_BYTE) as u64
}
pub const WEIGHT_BYTES: u64 = 512 * 1024 * 1024;
pub const WEIGHT_NS: u64 = 4_000_000_000;

const TENANTS: u64 = 24;
const SESSIONS: usize = 512;
const FUNCTIONS: u64 = 400;
const MODELS: u64 = 4;
const SHARDS_PER_MODEL: u64 = 2;
const SERVICES: u64 = 3;
/// Shape of a multi-agent fan-out: an orchestrator turn dispatches between `AGENTS_MIN` and
/// `AGENTS_MIN + AGENTS_SPAN - 1` sub-agents, each of which extends the orchestrator's context
/// with its own role prompt and subtask and makes up to `TOOLS_MAX` tool calls. **Modelled**,
/// and the numbers most worth replacing with traces from a real agent framework.
const AGENTS_MIN: u64 = 2;
const AGENTS_SPAN: u64 = 5;
const SUBAGENT_BLOCKS: u64 = 6;
const TOOLS_MAX: u64 = 4;
/// Result blocks each sub-agent contributes to the orchestrator's resumed context.
const RESULT_BLOCKS: u64 = 2;
/// Task specification handed to each sub-agent, and the result each one hands back.
pub const DISPATCH_PAYLOAD_BYTES: u64 = 64 * 1024;
pub const RESULT_PAYLOAD_BYTES: u64 = 256 * 1024;
/// Ops between the orchestrator deciding to fan out and the sub-agents starting.
const FANOUT_LEAD_OPS: u32 = 6;
/// Ops between dispatch and the orchestrator resuming. Sub-agents decode for about a second,
/// which is several hundred arrivals at the rates the experiments run; the workload cannot see
/// the machine's clock, so this is a stand-in for "after the slowest sub-agent returns".
const RESUME_LEAD_OPS: u32 = 400;
pub const SERVICE_BYTES: u64 = 384 * 1024 * 1024;
pub const SERVICE_COLD_NS: u64 = 15_000_000_000;
const REPLICAS: [u64; PHASES] = [3, 1, 2, 3];
const MAX_TURNS: u32 = 24;

/// Execution time once state is resident, which is the whole point of a warm path: a keep-
/// alive `FaaS` instance answers in tens of microseconds because it restores nothing. These are
/// **modelled** -- a function body, a request handler, a decode step -- and are the numbers
/// most worth replacing with real traces.
pub const FAAS_EXEC_MIN_NS: u64 = 40_000;
pub const FAAS_EXEC_SPAN_NS: u64 = 160_000;
pub const SERVICE_EXEC_NS: u64 = 250_000;
/// One decoded token at a batch of one. The served cost is `engine::Engine::step_ns` of the
/// batch the request lands in; this is the unbatched floor, kept so an arm with no engine
/// model stays comparable.
pub const DECODE_NS_PER_TOKEN: u64 = 8_000_000;
const TOKENS_MIN: u64 = 24;
const TOKENS_SPAN: u64 = 200;

/// Fraction of agent turns that call a tool, and of function invocations that call a model.
/// Both directions of the cross-workload dependency exist: an agent reaching for a function,
/// and a function reaching for a model.
const TOOL_FRACTION: f64 = 0.35;
const FLOW_FRACTION: f64 = 0.45;
const FLOW_LEAD_OPS: u32 = 6;
const FLOW_PROMPT_BLOCKS: u64 = 24;
/// Function output handed to the model: a prompt plus retrieved context.
pub const FLOW_PAYLOAD_BYTES: u64 = 4 * 1024 * 1024;
/// A tool result handed back to the agent: far smaller than a prompt bundle.
pub const TOOL_PAYLOAD_BYTES: u64 = 256 * 1024;

/// A downstream stage waiting for its lead time to elapse.
#[derive(Clone, Debug)]
struct Queued {
    due: u64,
    task: u64,
    chain: Chain,
    requires: Chain,
    exec_ns: u64,
    tokens: u64,
    fanout: Option<Fanout>,
}

/// A dispatched fan-out and the orchestrator turn that resumes once it returns.
#[derive(Clone, Debug)]
struct Fanout {
    gang: Gang,
    resume: Chain,
    resume_requires: Chain,
    resume_tokens: u64,
}

#[derive(Clone, Debug)]
struct Session {
    tenant: usize,
    id: u64,
    turns: u32,
}

pub const PHASES: usize = 4;

/// One function invocation a sub-agent makes while it works.
#[derive(Clone, Debug)]
pub struct ToolCall {
    pub chain: Chain,
    pub exec_ns: u64,
    /// Arguments out and result back, each this size.
    pub payload_bytes: u64,
}

/// One sub-agent: a session forked from the orchestrator's context.
#[derive(Clone, Debug)]
pub struct Agent {
    /// The orchestrator's context followed by this agent's own blocks. Siblings share the
    /// prefix, which is what makes placing them together worth something.
    pub chain: Chain,
    /// Weight shards of the model this agent's role runs on.
    pub requires: Chain,
    pub tokens: u64,
    pub tools: Vec<ToolCall>,
}

/// An orchestrator's fan-out: sub-agents that must all be admitted or none of them.
///
/// The orchestrator cannot resume until every sub-agent has returned, so admitting three of
/// four does not buy three quarters of an answer -- it buys nothing, and the three decode
/// anyway. That is the admission shape a per-request scheduler cannot express. The job also
/// finishes when its *slowest* agent does, so the placement question is not where each agent
/// is cheapest but where the worst of them is least bad: siblings placed together reuse their
/// shared context and hand results back for free, and pile their decodes onto one engine.
#[derive(Clone, Debug)]
pub struct Gang {
    pub agents: Vec<Agent>,
}

#[derive(Clone, Debug)]
pub struct Request {
    pub phase: usize,
    pub chain: Chain,
    /// Non-chain dependencies: state this request needs resident but does not extend, such
    /// as the model weight shards behind an inference call. Shared across identities, so no
    /// hash of the caller can predict where it belongs.
    pub requires: Chain,
    /// Set on an upstream stage that will call into another workload.
    pub hint: Option<FlowHint>,
    /// Set on the downstream stage, naming the task it completes.
    pub completes: Option<u64>,
    /// Work this request does once its state is resident.
    pub exec_ns: u64,
    /// Tokens to decode, or zero for work that is not a decode. Carried separately from
    /// `exec_ns` because what a token costs is a property of the engine it lands on, not of
    /// the request.
    pub tokens: u64,
    /// Set on a multi-agent fan-out.
    pub gang: Option<Gang>,
}

impl Request {
    /// Ledger class this request bills against. A fan-out carries its agents rather than a
    /// chain of its own, so the class has to come from the first of them.
    #[must_use]
    pub fn kind_idx(&self) -> usize {
        self.chain
            .first()
            .or_else(|| {
                self.gang
                    .as_ref()
                    .and_then(|g| g.agents.first().and_then(|a| a.chain.first()))
            })
            .map_or(0, |(_, m)| m.kind.idx())
    }
}

/// Cumulative class boundaries over the request-driven mix. Fan-outs are not a class of their
/// own: they are what some agent turns turn into.
#[derive(Clone, Copy, Debug)]
pub struct Mix {
    pub inference: f64,
    pub faas: f64,
}

const CLASSES: usize = 3;

const PHASE_MIX: [[f64; CLASSES]; PHASES] = [
    [0.58, 0.12, 0.30],
    [0.18, 0.64, 0.18],
    [0.30, 0.15, 0.55],
    [0.38, 0.30, 0.32],
];

#[derive(Debug)]
pub struct Workload {
    rng: Rng,
    ops: u64,
    volatility: f64,
    issued: u64,
    tenant_prefix: Vec<Chain>,
    sessions: Vec<Session>,
    next_session: u64,
    pending: VecDeque<Queued>,
    prompt_cache: HashMap<u64, Chain>,
    next_task: u64,
    /// Fraction of agent turns that fan out to sub-agents. Zero by default so the single-node
    /// experiments, which have no second node to spread a fan-out across, see the same trace
    /// they always did.
    fanout_fraction: f64,
    /// Fraction of agent turns that call a tool, and the size of that call's arguments and
    /// result. Configurable per scenario: a chat agent's tool calls are small and occasional
    /// (`TOOL_FRACTION`/`TOOL_PAYLOAD_BYTES`); a code-review agent's are frequent and carry
    /// file-sized payloads.
    tool_fraction: f64,
    tool_payload_bytes: u64,
}

fn kv(parent: BlobId, tag: &[u8]) -> (BlobId, BlobMeta) {
    let id = BlobId::chain(parent, tag);
    let meta = BlobMeta {
        kind: BlobKind::KvBlock,
        bytes: KV_BLOCK_BYTES,
        parent: if parent == ROOT { None } else { Some(parent) },
        recompute_ns: KV_BLOCK_NS,
    };
    (id, meta)
}

impl Workload {
    #[must_use]
    pub fn new(seed: u64, ops: u64, volatility: f64) -> Self {
        let mut rng = Rng::new(seed);
        let mut tenant_prefix = Vec::with_capacity(TENANTS as usize);
        for t in 0..TENANTS {
            let depth = 8 + rng.below(56);
            let mut chain = Vec::with_capacity(depth as usize);
            let mut parent = ROOT;
            for d in 0..depth {
                let (id, meta) = kv(parent, format!("tenant:{t}:{d}").as_bytes());
                parent = id;
                chain.push((id, meta));
            }
            tenant_prefix.push(chain);
        }
        let mut w = Self {
            rng,
            ops,
            volatility,
            issued: 0,
            tenant_prefix,
            sessions: Vec::new(),
            next_session: 0,
            pending: VecDeque::new(),
            prompt_cache: HashMap::new(),
            next_task: 0,
            fanout_fraction: 0.0,
            tool_fraction: TOOL_FRACTION,
            tool_payload_bytes: TOOL_PAYLOAD_BYTES,
        };
        for _ in 0..SESSIONS {
            let s = w.fresh_session();
            w.sessions.push(s);
        }
        w
    }

    /// The same stream with a fraction of agent turns fanning out to sub-agents.
    #[must_use]
    pub fn with_fanout(seed: u64, ops: u64, volatility: f64, fraction: f64) -> Self {
        let mut w = Self::new(seed, ops, volatility);
        w.fanout_fraction = fraction;
        w
    }

    /// Override how often an agent turn calls a tool, and how big that call's payload is.
    /// Chainable onto any constructor: `Workload::new(..).with_tool_profile(0.7, 1 << 20)`
    /// shapes something more like a code-review agent reading files than a chat agent's
    /// occasional function call.
    #[must_use]
    pub fn with_tool_profile(mut self, fraction: f64, payload_bytes: u64) -> Self {
        self.tool_fraction = fraction;
        self.tool_payload_bytes = payload_bytes;
        self
    }

    /// Sub-agents of one orchestrator turn, and the turn that resumes once they return.
    ///
    /// Roles may run different models, but most share the orchestrator's: a skewed pick keeps
    /// that realistic without making every sibling identical.
    fn fanout(&mut self, parent: &Chain, tenant: usize, task: u64) -> Fanout {
        let n = AGENTS_MIN + self.rng.below(AGENTS_SPAN);
        let home_model = tenant as u64 % MODELS;
        let base = parent.last().map_or(ROOT, |(id, _)| *id);
        let mut agents = Vec::with_capacity(n as usize);
        for a in 0..n {
            let mut chain = parent.clone();
            let mut at = base;
            for b in 0..SUBAGENT_BLOCKS {
                let (id, meta) = kv(at, format!("agent:{task}:{a}:{b}").as_bytes());
                at = id;
                chain.push((id, meta));
            }
            let model = (home_model + self.rng.zipf(MODELS, 2.0)) % MODELS;
            let tools = (0..self.rng.below(TOOLS_MAX + 1))
                .map(|_| {
                    let f = self.rng.zipf(FUNCTIONS, 1.5);
                    ToolCall {
                        chain: self.faas_for(f),
                        exec_ns: self.faas_exec(),
                        payload_bytes: TOOL_PAYLOAD_BYTES,
                    }
                })
                .collect();
            agents.push(Agent {
                chain,
                requires: Self::shards_of(model),
                tokens: self.tokens(),
                tools,
            });
        }
        let mut resume = parent.clone();
        let mut at = base;
        for a in 0..n {
            for b in 0..RESULT_BLOCKS {
                let (id, meta) = kv(at, format!("result:{task}:{a}:{b}").as_bytes());
                at = id;
                resume.push((id, meta));
            }
        }
        Fanout {
            gang: Gang { agents },
            resume,
            resume_requires: Self::model_shards(tenant),
            resume_tokens: self.tokens(),
        }
    }

    fn fresh_session(&mut self) -> Session {
        let tenant = self.rng.zipf(TENANTS, 1.6) as usize;
        self.next_session += 1;
        Session {
            tenant,
            id: self.next_session,
            turns: 1,
        }
    }

    #[must_use]
    pub fn phase(&self) -> usize {
        let f = self.issued as f64 / self.ops as f64;
        if f < 0.30 {
            0
        } else if f < 0.55 {
            1
        } else if f < 0.75 {
            2
        } else {
            3
        }
    }

    #[must_use]
    pub fn mix(&self) -> Mix {
        let p = PHASE_MIX[self.phase()];
        let mut flat = [0.0; CLASSES];
        for m in PHASE_MIX {
            for i in 0..CLASSES {
                flat[i] += m[i] / PHASES as f64;
            }
        }
        let v = self.volatility;
        let mut w = [0.0; CLASSES];
        for i in 0..CLASSES {
            w[i] = flat[i] + v * (p[i] - flat[i]);
        }
        let sum: f64 = w.iter().sum();
        Mix {
            inference: w[0] / sum,
            faas: (w[0] + w[1]) / sum,
        }
    }

    fn tokens(&mut self) -> u64 {
        TOKENS_MIN + self.rng.below(TOKENS_SPAN)
    }

    fn faas_exec(&mut self) -> u64 {
        FAAS_EXEC_MIN_NS + self.rng.below(FAAS_EXEC_SPAN_NS)
    }

    fn service(&mut self) -> Chain {
        let s = self.rng.zipf(SERVICES, 1.2);
        let r = self.rng.below(REPLICAS[self.phase()]);
        let id = BlobId::leaf(format!("svc:{s}:{r}").as_bytes());
        vec![(
            id,
            BlobMeta {
                kind: BlobKind::ServiceHeap,
                bytes: SERVICE_BYTES,
                parent: None,
                recompute_ns: SERVICE_COLD_NS,
            },
        )]
    }

    /// Weight shards behind a tenant's model. Many tenants map to one model, so this set is
    /// shared *across* identities -- no hash of the caller can predict where it belongs.
    fn model_shards(tenant: usize) -> Chain {
        Self::shards_of(tenant as u64 % MODELS)
    }

    fn shards_of(model: u64) -> Chain {
        (0..SHARDS_PER_MODEL)
            .map(|i| {
                let id = BlobId::leaf(format!("shard:{}", model * SHARDS_PER_MODEL + i).as_bytes());
                (
                    id,
                    BlobMeta {
                        kind: BlobKind::WeightShard,
                        bytes: WEIGHT_BYTES,
                        parent: None,
                        recompute_ns: WEIGHT_NS,
                    },
                )
            })
            .collect()
    }

    /// One turn of an agent session: the tenant's system prefix, every turn so far, and the
    /// blocks this turn adds. The chain grows monotonically, which is what makes a resident
    /// prefix worth anything.
    fn agent_turn(&mut self) -> (Chain, usize) {
        let slot = self.rng.zipf(self.sessions.len() as u64, 1.3) as usize;
        let s = self.sessions[slot].clone();
        let mut chain = self.tenant_prefix[s.tenant].clone();
        let mut parent = chain.last().map_or(ROOT, |(id, _)| *id);
        for turn in 0..s.turns {
            for blk in 0..4 {
                let (id, meta) = kv(parent, format!("s:{}:{turn}:{blk}", s.id).as_bytes());
                parent = id;
                chain.push((id, meta));
            }
        }
        if s.turns >= MAX_TURNS || self.rng.chance(0.04) {
            self.sessions[slot] = self.fresh_session();
        } else {
            self.sessions[slot].turns += 1;
        }
        (chain, s.tenant)
    }

    /// The inference working set a given function calls into: a per-function system prompt
    /// (shared across that function's invocations) plus a short per-call suffix.
    fn flow_chain(&mut self, f: u64) -> Chain {
        // The per-function system prompt is deterministic and shared across that function's
        // invocations; rehashing 24 chained blake3 blocks per call is pure waste.
        let prompt = self.prompt_cache.entry(f).or_insert_with(|| {
            let mut p = Vec::with_capacity(FLOW_PROMPT_BLOCKS as usize);
            let mut parent = ROOT;
            for d in 0..FLOW_PROMPT_BLOCKS {
                let (id, meta) = kv(parent, format!("fnprompt:{f}:{d}").as_bytes());
                parent = id;
                p.push((id, meta));
            }
            p
        });
        let mut chain = Vec::with_capacity(prompt.len() + 4);
        chain.extend_from_slice(prompt);
        let mut parent = chain.last().map_or(ROOT, |(id, _)| *id);
        let call = self.rng.below(64);
        for d in 0..4 {
            let (id, meta) = kv(parent, format!("fncall:{f}:{call}:{d}").as_bytes());
            parent = id;
            chain.push((id, meta));
        }
        chain
    }

    fn faas_for(&mut self, f: u64) -> Chain {
        let id = BlobId::leaf(format!("fn:{f}").as_bytes());
        let scale = 1 + self.rng.below(4);
        let bytes = SNAPSHOT_BYTES * scale / 2;
        vec![(
            id,
            BlobMeta {
                kind: BlobKind::Snapshot,
                bytes,
                parent: None,
                recompute_ns: snapshot_restore_ns(bytes),
            },
        )]
    }
}

impl Iterator for Workload {
    type Item = Request;

    fn next(&mut self) -> Option<Self::Item> {
        let phase = self.phase();
        // pending is kept sorted by due, so only the front can ever be ready.
        let ready = self.pending.front().is_some_and(|q| q.due <= self.issued);
        if ready || self.issued >= self.ops {
            let q = self.pending.pop_front()?;
            if ready {
                self.issued += 1;
            }
            if let Some(f) = q.fanout {
                let payload = RESULT_PAYLOAD_BYTES * f.gang.agents.len() as u64;
                let hint = self.enqueue_after(
                    RESUME_LEAD_OPS,
                    f.resume,
                    f.resume_requires,
                    f.resume_tokens * DECODE_NS_PER_TOKEN,
                    f.resume_tokens,
                    payload,
                    None,
                );
                return Some(Request {
                    phase,
                    chain: Vec::new(),
                    requires: Vec::new(),
                    hint: Some(hint),
                    completes: Some(q.task),
                    exec_ns: 0,
                    tokens: 0,
                    gang: Some(f.gang),
                });
            }
            return Some(Request {
                phase,
                chain: q.chain,
                requires: q.requires,
                hint: None,
                completes: Some(q.task),
                exec_ns: q.exec_ns,
                tokens: q.tokens,
                gang: None,
            });
        }
        let mix = self.mix();
        let roll = self.rng.unit();
        self.issued += 1;
        if roll < mix.inference {
            return Some(self.inference_request(phase));
        }
        if roll < mix.faas {
            return Some(self.faas_request(phase));
        }
        Some(Request {
            phase,
            chain: self.service(),
            requires: Vec::new(),
            hint: None,
            completes: None,
            exec_ns: SERVICE_EXEC_NS,
            tokens: 0,
            gang: None,
        })
    }
}

impl Workload {
    fn enqueue(
        &mut self,
        chain: Chain,
        requires: Chain,
        exec_ns: u64,
        tokens: u64,
        payload: u64,
    ) -> FlowHint {
        self.enqueue_after(
            FLOW_LEAD_OPS,
            chain,
            requires,
            exec_ns,
            tokens,
            payload,
            None,
        )
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "one stage, described field by field"
    )]
    fn enqueue_after(
        &mut self,
        lead: u32,
        chain: Chain,
        requires: Chain,
        exec_ns: u64,
        tokens: u64,
        payload: u64,
        fanout: Option<Fanout>,
    ) -> FlowHint {
        self.next_task += 1;
        let task = self.next_task;
        let due = self.issued + u64::from(lead);
        // Lead times differ by stage, so arrival order is not due order; insert in place.
        let at = self.pending.partition_point(|q| q.due <= due);
        self.pending.insert(
            at,
            Queued {
                due,
                task,
                chain: chain.clone(),
                requires,
                exec_ns,
                tokens,
                fanout,
            },
        );
        FlowHint {
            task,
            downstream: chain,
            probability: 1.0,
            lead_ops: lead,
            payload_bytes: payload,
        }
    }

    /// An agent turn. Decode dominates its cost, and a fraction of turns reach for a tool,
    /// which is a function invocation the inference scheduler does not otherwise know about.
    fn inference_request(&mut self, phase: usize) -> Request {
        let (chain, tenant) = self.agent_turn();
        let tokens = self.tokens();
        let requires = Self::model_shards(tenant);
        let hint = if self.fanout_fraction > 0.0 && self.rng.chance(self.fanout_fraction) {
            let task = self.next_task + 1;
            let plan = self.fanout(&chain, tenant, task);
            let hint = self.enqueue_after(
                FANOUT_LEAD_OPS,
                Vec::new(),
                Vec::new(),
                0,
                0,
                DISPATCH_PAYLOAD_BYTES * plan.gang.agents.len() as u64,
                Some(plan),
            );
            debug_assert_eq!(hint.task, task);
            Some(hint)
        } else if self.rng.chance(self.tool_fraction) {
            let f = self.rng.zipf(FUNCTIONS, 1.5);
            let tool = self.faas_for(f);
            let exec = self.faas_exec();
            let payload = self.tool_payload_bytes;
            Some(self.enqueue(tool, Vec::new(), exec, 0, payload))
        } else {
            None
        };
        Request {
            phase,
            chain,
            requires,
            hint,
            completes: None,
            exec_ns: tokens * DECODE_NS_PER_TOKEN,
            tokens,
            gang: None,
        }
    }

    /// A function invocation. Warm when its snapshot is still resident, which is the ledger's
    /// decision rather than the workload's -- the body runs either way.
    fn faas_request(&mut self, phase: usize) -> Request {
        let f = self.rng.zipf(FUNCTIONS, 1.5);
        let chain = self.faas_for(f);
        let exec_ns = self.faas_exec();
        let hint = if self.rng.chance(FLOW_FRACTION) {
            let downstream = self.flow_chain(f);
            let requires = Self::model_shards(f as usize % TENANTS as usize);
            let tokens = self.tokens();
            Some(self.enqueue(
                downstream,
                requires,
                tokens * DECODE_NS_PER_TOKEN,
                tokens,
                FLOW_PAYLOAD_BYTES,
            ))
        } else {
            None
        };
        Request {
            phase,
            chain,
            requires: Vec::new(),
            hint,
            completes: None,
            exec_ns,
            tokens: 0,
            gang: None,
        }
    }
}
