use crate::blob::{BlobId, BlobKind, BlobMeta, ROOT};
use crate::flow::FlowHint;
use crate::rng::Rng;
use std::collections::{HashMap, VecDeque};

pub type Chain = Vec<(BlobId, BlobMeta)>;

pub const KV_BLOCK_BYTES: u64 = 512 * 1024;
pub const KV_BLOCK_NS: u64 = 400_000;

pub const SNAPSHOT_BYTES: u64 = 32 * 1024 * 1024;

pub const SNAPSHOT_RESTORE_NS: u64 = 4_000_000;

pub const SNAPSHOT_WORKING_SET: f64 = 0.15;

pub const SNAPSHOT_PAGE_IN_NS_PER_BYTE: f64 = 1.0;

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

const AGENTS_MIN: u64 = 2;
const AGENTS_SPAN: u64 = 5;
const SUBAGENT_BLOCKS: u64 = 6;
const TOOLS_MAX: u64 = 4;

const RESULT_BLOCKS: u64 = 2;

pub const DISPATCH_PAYLOAD_BYTES: u64 = 64 * 1024;
pub const RESULT_PAYLOAD_BYTES: u64 = 256 * 1024;

const FANOUT_LEAD_OPS: u32 = 6;

const RESUME_LEAD_OPS: u32 = 400;
pub const SERVICE_BYTES: u64 = 384 * 1024 * 1024;
pub const SERVICE_COLD_NS: u64 = 15_000_000_000;
const REPLICAS: [u64; PHASES] = [3, 1, 2, 3];
const MAX_TURNS: u32 = 24;

pub const FAAS_EXEC_MIN_NS: u64 = 40_000;
pub const FAAS_EXEC_SPAN_NS: u64 = 160_000;
pub const SERVICE_EXEC_NS: u64 = 250_000;

pub const DECODE_NS_PER_TOKEN: u64 = 8_000_000;
const TOKENS_MIN: u64 = 24;
const TOKENS_SPAN: u64 = 200;

pub const TOKENS_PER_KV_BLOCK: u64 = 35;
pub const MAX_TOKEN_SLACK: f64 = 4.0;

const TOOL_FRACTION: f64 = 0.35;
const FLOW_FRACTION: f64 = 0.45;
const FLOW_LEAD_OPS: u32 = 6;
const FLOW_PROMPT_BLOCKS: u64 = 24;

pub const FLOW_PAYLOAD_BYTES: u64 = 4 * 1024 * 1024;

pub const TOOL_PAYLOAD_BYTES: u64 = 256 * 1024;

#[derive(Clone, Copy, Debug)]
struct Turn {
    slot: usize,
    session: u64,
    index: u32,
}

#[derive(Clone, Debug)]
struct Queued {
    due: u64,
    task: u64,
    chain: Chain,
    requires: Chain,
    exec_ns: u64,
    tokens: u64,
    fanout: Option<Fanout>,
    slo: Slo,
}

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
    grown: Vec<u64>,
}

pub const PHASES: usize = 4;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Slo {
    #[default]
    Interactive,
    Throughput,
}

impl Slo {
    pub const ALL: [Self; 2] = [Self::Interactive, Self::Throughput];

    #[must_use]
    pub fn idx(self) -> usize {
        match self {
            Self::Interactive => 0,
            Self::Throughput => 1,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ToolCall {
    pub chain: Chain,
    pub exec_ns: u64,

    pub payload_bytes: u64,
}

#[derive(Clone, Debug)]
pub struct Agent {
    pub chain: Chain,

    pub requires: Chain,
    pub tokens: u64,
    pub tools: Vec<ToolCall>,
    pub produces: Chain,
    pub max_tokens: u64,
    pub slo: Slo,
}

#[derive(Clone, Debug)]
pub struct Gang {
    pub agents: Vec<Agent>,
}

#[derive(Clone, Debug)]
pub struct Request {
    pub phase: usize,
    pub chain: Chain,

    pub requires: Chain,

    pub hint: Option<FlowHint>,

    pub completes: Option<u64>,

    pub exec_ns: u64,

    pub tokens: u64,

    pub gang: Option<Gang>,
    pub produces: Chain,
    pub max_tokens: u64,
    pub slo: Slo,
}

#[derive(Clone, Copy, Debug)]
pub struct RequestView<'a> {
    pub chain: &'a [(BlobId, BlobMeta)],
    pub requires: &'a [(BlobId, BlobMeta)],
    pub tokens: u64,
    pub class: usize,
    pub slo: Slo,
}

impl Request {
    #[must_use]
    pub fn view(&self, tokens: u64) -> RequestView<'_> {
        RequestView {
            chain: &self.chain,
            requires: &self.requires,
            tokens,
            class: self.kind_idx(),
            slo: self.slo,
        }
    }

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

    fanout_fraction: f64,

    tool_fraction: f64,
    tool_payload_bytes: u64,

    flow_payload_bytes: u64,
    tokens_per_block: Option<u64>,
    max_token_slack: f64,
    throughput: f64,
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
            flow_payload_bytes: FLOW_PAYLOAD_BYTES,
            tokens_per_block: None,
            max_token_slack: MAX_TOKEN_SLACK,
            throughput: 0.0,
        };
        for _ in 0..SESSIONS {
            let s = w.fresh_session();
            w.sessions.push(s);
        }
        w
    }

    #[must_use]
    pub fn with_fanout(seed: u64, ops: u64, volatility: f64, fraction: f64) -> Self {
        let mut w = Self::new(seed, ops, volatility);
        w.fanout_fraction = fraction;
        w
    }

    #[must_use]
    pub fn with_tool_profile(mut self, fraction: f64, payload_bytes: u64) -> Self {
        self.tool_fraction = fraction;
        self.tool_payload_bytes = payload_bytes;
        self
    }

    #[must_use]
    pub fn with_flow_payload(mut self, payload_bytes: u64) -> Self {
        self.flow_payload_bytes = payload_bytes;
        self
    }

    #[must_use]
    pub fn with_decode_kv(mut self, tokens_per_block: u64) -> Self {
        self.tokens_per_block = Some(tokens_per_block.max(1));
        self
    }

    #[must_use]
    pub fn with_throughput(mut self, fraction: f64) -> Self {
        self.throughput = fraction;
        self
    }

    fn slo_of(&self, session: u64) -> Slo {
        let mixed = session.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 11;
        if (mixed as f64) < self.throughput * (1u64 << 53) as f64 {
            Slo::Throughput
        } else {
            Slo::Interactive
        }
    }

    #[must_use]
    pub fn with_max_token_slack(mut self, slack: f64) -> Self {
        self.max_token_slack = slack;
        self
    }

    fn max_tokens(&self, tokens: u64) -> u64 {
        if tokens == 0 {
            0
        } else {
            (self.max_token_slack * (TOKENS_MIN + TOKENS_SPAN) as f64) as u64
        }
    }

    fn produced(&self, parent: BlobId, tokens: u64, tag: &str) -> Chain {
        let Some(per_block) = self.tokens_per_block else {
            return Vec::new();
        };
        let mut at = parent;
        (0..tokens.div_ceil(per_block))
            .map(|blk| {
                let (id, meta) = kv(at, format!("{tag}:{blk}").as_bytes());
                at = id;
                (id, meta)
            })
            .collect()
    }

    fn fanout(&mut self, parent: &Chain, tenant: usize, task: u64, slo: Slo) -> Fanout {
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
            let tail = at;
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
            let tokens = self.tokens();
            agents.push(Agent {
                chain,
                requires: Self::shards_of(model),
                tokens,
                tools,
                produces: self.produced(tail, tokens, &format!("agentout:{task}:{a}")),
                max_tokens: self.max_tokens(tokens),
                slo,
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
            grown: vec![4],
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

    fn agent_turn(&mut self) -> (Chain, usize, Turn) {
        let slot = self.rng.zipf(self.sessions.len() as u64, 1.3) as usize;
        let s = self.sessions[slot].clone();
        let mut chain = self.tenant_prefix[s.tenant].clone();
        let mut parent = chain.last().map_or(ROOT, |(id, _)| *id);
        for turn in 0..s.turns {
            let blocks = if self.tokens_per_block.is_some() {
                s.grown[turn as usize]
            } else {
                4
            };
            for blk in 0..blocks {
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
        let turn = Turn {
            slot,
            session: s.id,
            index: s.turns,
        };
        (chain, s.tenant, turn)
    }

    fn grow(&mut self, turn: Turn, blocks: u64) {
        let s = &mut self.sessions[turn.slot];
        if s.id == turn.session {
            s.grown.push(blocks);
        }
    }

    fn flow_chain(&mut self, f: u64) -> Chain {
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
                    q.slo,
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
                    produces: Vec::new(),
                    max_tokens: 0,
                    slo: q.slo,
                });
            }
            let tail = q.chain.last().map_or(ROOT, |(id, _)| *id);
            let produces = if q.tokens > 0 {
                self.produced(tail, q.tokens, &format!("out:{}", q.task))
            } else {
                Vec::new()
            };
            return Some(Request {
                phase,
                chain: q.chain,
                requires: q.requires,
                hint: None,
                completes: Some(q.task),
                exec_ns: q.exec_ns,
                tokens: q.tokens,
                gang: None,
                produces,
                max_tokens: self.max_tokens(q.tokens),
                slo: q.slo,
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
            produces: Vec::new(),
            max_tokens: 0,
            slo: Slo::Interactive,
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
            Slo::Interactive,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn enqueue_after(
        &mut self,
        lead: u32,
        chain: Chain,
        requires: Chain,
        exec_ns: u64,
        tokens: u64,
        payload: u64,
        fanout: Option<Fanout>,
        slo: Slo,
    ) -> FlowHint {
        self.next_task += 1;
        let task = self.next_task;
        let due = self.issued + u64::from(lead);

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
                slo,
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

    fn inference_request(&mut self, phase: usize) -> Request {
        let (chain, tenant, turn) = self.agent_turn();
        let slo = self.slo_of(turn.session);
        let tokens = self.tokens();
        let tail = chain.last().map_or(ROOT, |(id, _)| *id);
        let produces = self.produced(tail, tokens, &format!("s:{}:{}", turn.session, turn.index));
        self.grow(turn, produces.len() as u64);
        let requires = Self::model_shards(tenant);
        let hint = if self.fanout_fraction > 0.0 && self.rng.chance(self.fanout_fraction) {
            let task = self.next_task + 1;
            let plan = self.fanout(&chain, tenant, task, slo);
            let hint = self.enqueue_after(
                FANOUT_LEAD_OPS,
                Vec::new(),
                Vec::new(),
                0,
                0,
                DISPATCH_PAYLOAD_BYTES * plan.gang.agents.len() as u64,
                Some(plan),
                slo,
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
            produces,
            max_tokens: self.max_tokens(tokens),
            slo,
        }
    }

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
                self.flow_payload_bytes,
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
            produces: Vec::new(),
            max_tokens: 0,
            slo: Slo::Interactive,
        }
    }
}
