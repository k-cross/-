use crate::blob::{BlobId, BlobKind, BlobMeta, ROOT};
use crate::engine::{MODEL_COUNT, Model};
use crate::flow::FlowHint;
use crate::rng::Rng;
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::rc::Rc;
use std::sync::OnceLock;

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

pub const TENANTS: u64 = 24;
const SESSIONS: usize = 512;
const FUNCTIONS: u64 = 400;
const MODELS: u64 = MODEL_COUNT as u64;
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
pub const FLOW_LEAD_OPS: u32 = 6;
const FLOW_PROMPT_BLOCKS: u64 = 24;

const MIX_SEED: u64 = 0x4D49_585F_5345_4544;
const FRESH_SEED: u64 = 0x4652_4553_485F_5345;
const NEIGHBOUR_SEED: u64 = 0x4E45_4947_4842_5352;
const BATCH_SEED: u64 = 0x4241_5443_485F_5345;
pub const BATCH_TENANT: u32 = 4_000;
pub const BATCH_BLOCKS: u64 = 8;
const BATCH_TOKENS_MIN: u64 = 200;
const BATCH_TOKENS_SPAN: u64 = 400;
pub const FRESH_TENANT: u32 = 3_000;
pub const NEIGHBOUR_TENANT: u32 = 2_000;
pub const SHARED_PREFIX_BLOCKS: u64 = 8;
const NEIGHBOUR_FROM: f64 = 0.4;
const NEIGHBOUR_TO: f64 = 0.6;
pub const FRESH_BLOCKS: u64 = 64;
const FRESH_TOKENS_MIN: u64 = 24;
const FRESH_TOKENS_SPAN: u64 = 40;

pub type ModelMix = [[f64; MODEL_COUNT]; PHASES];

#[must_use]
pub fn rotating_mix(head: [f64; MODEL_COUNT]) -> ModelMix {
    std::array::from_fn(|phase| {
        std::array::from_fn(|m| head[(m + MODEL_COUNT - phase % MODEL_COUNT) % MODEL_COUNT])
    })
}

#[must_use]
pub fn neighbour_window(duty: f64) -> (f64, f64) {
    let duty = duty.clamp(0.0, 1.0);
    ((1.0 - duty) / 2.0, f64::midpoint(1.0, duty))
}

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
    tenant: Option<u32>,
}

#[derive(Clone, Debug)]
struct Fanout {
    gang: Gang,
    resume: Chain,
    resume_requires: Chain,
    resume_tokens: u64,
    tenant: Option<u32>,
}

#[derive(Clone, Debug)]
struct Session {
    tenant: usize,
    id: u64,
    turns: u32,
    grown: Vec<u64>,
}

pub const PHASES: usize = 4;

const PROGRAM_TASK: u64 = 1 << 62;
const PROGRAM_FRESH: u64 = 1 << 61;
const PROGRAM_BATCH: u64 = 1 << 60;

#[derive(Clone, Copy)]
enum Unshared {
    Fresh { tenant: u32, neighbour: bool },
    Batch,
}

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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Retain {
    pub upto: usize,
    pub lead_ops: u32,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Retention {
    pub retain: Option<Retain>,
    pub evict_first_from: Option<usize>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Origin {
    Tenant,
    Session,
    Agent,
    AgentOut,
    Result,
    FlowPrompt,
    FlowCall,
    TaskOut,
}

impl Origin {
    pub const N: usize = 8;
    pub const ALL: [Self; Self::N] = [
        Self::Tenant,
        Self::Session,
        Self::Agent,
        Self::AgentOut,
        Self::Result,
        Self::FlowPrompt,
        Self::FlowCall,
        Self::TaskOut,
    ];

    #[must_use]
    pub fn idx(self) -> usize {
        match self {
            Self::Tenant => 0,
            Self::Session => 1,
            Self::Agent => 2,
            Self::AgentOut => 3,
            Self::Result => 4,
            Self::FlowPrompt => 5,
            Self::FlowCall => 6,
            Self::TaskOut => 7,
        }
    }

    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Tenant => "tenant",
            Self::Session => "session",
            Self::Agent => "agent",
            Self::AgentOut => "agent out",
            Self::Result => "result",
            Self::FlowPrompt => "flow prompt",
            Self::FlowCall => "flow call",
            Self::TaskOut => "task out",
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct Origins(Rc<RefCell<HashMap<BlobId, Origin>>>);

impl Origins {
    #[must_use]
    pub fn of(&self, id: &BlobId) -> Option<Origin> {
        self.0.borrow().get(id).copied()
    }

    fn register(&self, id: BlobId, origin: Origin) {
        self.0.borrow_mut().entry(id).or_insert(origin);
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
    pub retention: Retention,
    pub tenant: Option<u32>,
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
    pub retention: Retention,
    pub concurrent: bool,
    pub tenant: Option<u32>,
    pub program: u64,
}

#[must_use]
pub fn model_of(requires: &[(BlobId, BlobMeta)]) -> Option<Model> {
    static FIRST_SHARDS: OnceLock<[BlobId; MODEL_COUNT]> = OnceLock::new();
    let first = requires.first()?.0;
    let table = FIRST_SHARDS.get_or_init(|| {
        std::array::from_fn(|m| {
            BlobId::leaf(format!("shard:{}", m as u64 * SHARDS_PER_MODEL).as_bytes())
        })
    });
    table.iter().position(|id| *id == first).map(|m| m as Model)
}

#[derive(Clone, Copy, Debug)]
pub struct RequestView<'a> {
    pub chain: &'a [(BlobId, BlobMeta)],
    pub requires: &'a [(BlobId, BlobMeta)],
    pub model: Option<Model>,
    pub tokens: u64,
    pub class: usize,
    pub slo: Slo,
    pub tenant: Option<u32>,
}

impl Request {
    #[must_use]
    pub fn view(&self, tokens: u64) -> RequestView<'_> {
        RequestView {
            chain: &self.chain,
            requires: &self.requires,
            model: model_of(&self.requires),
            tokens,
            class: self.kind_idx(),
            slo: self.slo,
            tenant: self.tenant,
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
    origins: Option<Origins>,
    model_keyed: bool,
    one_model: bool,
    mix: Option<ModelMix>,
    mix_rng: Rng,
    fresh_fraction: f64,
    fresh_rng: Rng,
    fresh_pending: VecDeque<Request>,
    fresh_n: u64,
    neighbour_rate: f64,
    neighbour_window: (f64, f64),
    neighbour_rng: Rng,
    shared_prefix: bool,
    batch_fraction: f64,
    batch_rng: Rng,
}

fn kv(origins: Option<&Origins>, parent: BlobId, tag: &[u8], origin: Origin) -> (BlobId, BlobMeta) {
    let id = BlobId::chain(parent, tag);
    if let Some(o) = origins {
        o.register(id, origin);
    }
    let meta = BlobMeta {
        kind: BlobKind::KvBlock,
        bytes: KV_BLOCK_BYTES,
        parent: if parent == ROOT { None } else { Some(parent) },
        recompute_ns: KV_BLOCK_NS,
    };
    (id, meta)
}

fn flow_prompt(origins: Option<&Origins>, f: u64) -> Chain {
    let mut prompt = Vec::with_capacity(FLOW_PROMPT_BLOCKS as usize);
    let mut parent = ROOT;
    for d in 0..FLOW_PROMPT_BLOCKS {
        let (id, meta) = kv(
            origins,
            parent,
            format!("fnprompt:{f}:{d}").as_bytes(),
            Origin::FlowPrompt,
        );
        parent = id;
        prompt.push((id, meta));
    }
    prompt
}

fn flow_call(origins: Option<&Origins>, mut parent: BlobId, f: u64, call: u64) -> Chain {
    (0..4)
        .map(|d| {
            let (id, meta) = kv(
                origins,
                parent,
                format!("fncall:{f}:{call}:{d}").as_bytes(),
                Origin::FlowCall,
            );
            parent = id;
            (id, meta)
        })
        .collect()
}

#[must_use]
pub fn flow_downstream(f: u64, call: u64) -> Chain {
    let mut chain = flow_prompt(None, f);
    let tail = chain.last().map_or(ROOT, |(id, _)| *id);
    chain.extend(flow_call(None, tail, f, call));
    chain
}

pub const FLOW_FUNCTIONS: u64 = FUNCTIONS;
pub const FLOW_CALLS: u64 = 64;

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
                let (id, meta) = kv(
                    None,
                    parent,
                    format!("tenant:{t}:{d}").as_bytes(),
                    Origin::Tenant,
                );
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
            origins: None,
            model_keyed: false,
            one_model: false,
            mix: None,
            mix_rng: Rng::new(seed ^ MIX_SEED),
            fresh_fraction: 0.0,
            fresh_rng: Rng::new(seed ^ FRESH_SEED),
            fresh_pending: VecDeque::new(),
            fresh_n: 0,
            neighbour_rate: 0.0,
            neighbour_window: (NEIGHBOUR_FROM, NEIGHBOUR_TO),
            neighbour_rng: Rng::new(seed ^ NEIGHBOUR_SEED),
            shared_prefix: false,
            batch_fraction: 0.0,
            batch_rng: Rng::new(seed ^ BATCH_SEED),
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
    pub fn with_origins(mut self, origins: Origins) -> Self {
        for chain in &self.tenant_prefix {
            for (id, _) in chain {
                origins.register(*id, Origin::Tenant);
            }
        }
        self.origins = Some(origins);
        self
    }

    #[must_use]
    pub fn with_model_mix(mut self, mix: ModelMix) -> Self {
        self.mix = Some(mix);
        for slot in 0..self.sessions.len() {
            self.sessions[slot] = self.fresh_session_in(slot);
        }
        self
    }

    #[must_use]
    pub fn with_neighbour(mut self, rate: f64) -> Self {
        self.neighbour_rate = rate;
        self
    }

    #[must_use]
    pub fn with_neighbour_duty(mut self, duty: f64) -> Self {
        self.neighbour_window = neighbour_window(duty);
        self
    }

    #[must_use]
    pub fn with_shared_prefix(mut self, on: bool) -> Self {
        if !on || self.shared_prefix {
            return self;
        }
        self.shared_prefix = true;
        let mut shared: HashMap<u64, Chain> = HashMap::new();
        for t in 0..TENANTS as usize {
            let model = self.home_model(t);
            let prefix = shared.entry(model).or_insert_with(|| {
                let mut parent = ROOT;
                (0..SHARED_PREFIX_BLOCKS)
                    .map(|d| {
                        let (id, meta) = kv(
                            None,
                            parent,
                            format!("model-prefix:{model}:{d}").as_bytes(),
                            Origin::Tenant,
                        );
                        parent = id;
                        (id, meta)
                    })
                    .collect()
            });
            let own = std::mem::take(&mut self.tenant_prefix[t]);
            let mut parent = prefix.last().map_or(ROOT, |(id, _)| *id);
            let mut chain = prefix.clone();
            for (d, _) in own.iter().enumerate().skip(SHARED_PREFIX_BLOCKS as usize) {
                let (id, meta) = kv(
                    None,
                    parent,
                    format!("tenant:{t}:{d}").as_bytes(),
                    Origin::Tenant,
                );
                parent = id;
                chain.push((id, meta));
            }
            self.tenant_prefix[t] = chain;
        }
        if let Some(o) = &self.origins {
            for chain in &self.tenant_prefix {
                for (id, _) in chain {
                    o.register(*id, Origin::Tenant);
                }
            }
        }
        self
    }

    #[must_use]
    pub fn with_batch(mut self, fraction: f64) -> Self {
        self.batch_fraction = fraction;
        self
    }

    #[must_use]
    pub fn with_fresh(mut self, fraction: f64) -> Self {
        self.fresh_fraction = fraction;
        self
    }

    #[must_use]
    pub fn with_one_model(mut self, on: bool) -> Self {
        self.one_model = on;
        self
    }

    #[must_use]
    pub fn with_model_keyed(mut self, on: bool) -> Self {
        self.model_keyed = on;
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

    fn produced(&self, parent: BlobId, tokens: u64, tag: &str, origin: Origin) -> Chain {
        let Some(per_block) = self.tokens_per_block else {
            return Vec::new();
        };
        let mut at = parent;
        (0..tokens.div_ceil(per_block))
            .map(|blk| {
                let (id, meta) = kv(
                    self.origins.as_ref(),
                    at,
                    format!("{tag}:{blk}").as_bytes(),
                    origin,
                );
                at = id;
                (id, meta)
            })
            .collect()
    }

    fn keyed_parent(&self, parent: &Chain, model: u64) -> (Chain, BlobId) {
        let mut keyed = Vec::with_capacity(parent.len());
        let mut before: Option<BlobId> = None;
        for &(id, meta) in parent {
            let salted = BlobId::chain(id, format!("model:{model}").as_bytes());
            if let Some(o) = &self.origins
                && let Some(origin) = o.of(&id)
            {
                o.register(salted, origin);
            }
            keyed.push((
                salted,
                BlobMeta {
                    parent: before,
                    ..meta
                },
            ));
            before = Some(salted);
        }
        let tail = before.unwrap_or(ROOT);
        (keyed, tail)
    }

    fn fanout(&mut self, parent: &Chain, tenant: usize, task: u64, slo: Slo) -> Fanout {
        let n = AGENTS_MIN + self.rng.below(AGENTS_SPAN);
        let home_model = self.home_model(tenant);
        let base = parent.last().map_or(ROOT, |(id, _)| *id);
        let mut agents = Vec::with_capacity(n as usize);
        for a in 0..n {
            let drawn = (home_model + self.rng.zipf(MODELS, 2.0)) % MODELS;
            let model = if self.one_model { 0 } else { drawn };
            let (mut chain, mut at) = if self.model_keyed && model != home_model {
                self.keyed_parent(parent, model)
            } else {
                (parent.clone(), base)
            };
            for b in 0..SUBAGENT_BLOCKS {
                let (id, meta) = kv(
                    self.origins.as_ref(),
                    at,
                    format!("agent:{task}:{a}:{b}").as_bytes(),
                    Origin::Agent,
                );
                at = id;
                chain.push((id, meta));
            }
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
                produces: self.produced(
                    tail,
                    tokens,
                    &format!("agentout:{task}:{a}"),
                    Origin::AgentOut,
                ),
                max_tokens: self.max_tokens(tokens),
                slo,
                retention: Retention {
                    retain: Some(Retain {
                        upto: parent.len(),
                        lead_ops: RESUME_LEAD_OPS,
                    }),
                    evict_first_from: Some(parent.len()),
                },
                tenant: Some(tenant as u32),
            });
        }
        let mut resume = parent.clone();
        let mut at = base;
        for a in 0..n {
            for b in 0..RESULT_BLOCKS {
                let (id, meta) = kv(
                    self.origins.as_ref(),
                    at,
                    format!("result:{task}:{a}:{b}").as_bytes(),
                    Origin::Result,
                );
                at = id;
                resume.push((id, meta));
            }
        }
        Fanout {
            gang: Gang { agents },
            resume,
            resume_requires: self.model_shards(tenant),
            resume_tokens: self.tokens(),
            tenant: Some(tenant as u32),
        }
    }

    fn drawn_model(&mut self) -> u64 {
        let Some(mix) = self.mix else {
            return 0;
        };
        let row = mix[self.phase()];
        let u = self.mix_rng.unit();
        let mut share = 0.0;
        for (m, p) in row.iter().enumerate() {
            share += p;
            if u < share {
                return m as u64;
            }
        }
        MODELS - 1
    }

    fn fresh_session_in(&mut self, slot: usize) -> Session {
        let per_model = TENANTS / MODELS;
        let model = (slot / (SESSIONS / MODEL_COUNT)) as u64;
        let tenant = (model * per_model + self.rng.zipf(per_model, 1.6)) as usize;
        self.next_session += 1;
        Session {
            tenant,
            id: self.next_session,
            turns: 1,
            grown: vec![4],
        }
    }

    fn replacement_session(&mut self, slot: usize) -> Session {
        if self.mix.is_some() {
            self.fresh_session_in(slot)
        } else {
            self.fresh_session()
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

    fn home_model(&self, tenant: usize) -> u64 {
        if self.one_model {
            0
        } else if self.mix.is_some() {
            tenant as u64 / (TENANTS / MODELS)
        } else {
            tenant as u64 % MODELS
        }
    }

    fn flow_model(&self, function: u64) -> u64 {
        if self.one_model { 0 } else { function % MODELS }
    }

    fn model_shards(&self, tenant: usize) -> Chain {
        Self::shards_of(self.home_model(tenant))
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
        let slot = if self.mix.is_some() {
            let per_model = (SESSIONS / MODEL_COUNT) as u64;
            (self.drawn_model() * per_model + self.rng.zipf(per_model, 1.3)) as usize
        } else {
            self.rng.zipf(self.sessions.len() as u64, 1.3) as usize
        };
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
                let (id, meta) = kv(
                    self.origins.as_ref(),
                    parent,
                    format!("s:{}:{turn}:{blk}", s.id).as_bytes(),
                    Origin::Session,
                );
                parent = id;
                chain.push((id, meta));
            }
        }
        if s.turns >= MAX_TURNS || self.rng.chance(0.04) {
            self.sessions[slot] = self.replacement_session(slot);
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
        let origins = self.origins.clone();
        let prompt = self
            .prompt_cache
            .entry(f)
            .or_insert_with(|| flow_prompt(origins.as_ref(), f));
        let mut chain = Vec::with_capacity(prompt.len() + 4);
        chain.extend_from_slice(prompt);
        let tail = chain.last().map_or(ROOT, |(id, _)| *id);
        let call = self.rng.below(64);
        chain.extend(flow_call(self.origins.as_ref(), tail, f, call));
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
        if let Some(extra) = self.fresh_pending.pop_front() {
            return Some(extra);
        }
        let req = self.next_base()?;
        if self.batch_fraction > 0.0 && self.batch_rng.chance(self.batch_fraction) {
            let batch = self.unshared_request(req.phase, Unshared::Batch);
            self.fresh_pending.push_back(batch);
        }
        if self.fresh_fraction > 0.0 && self.fresh_rng.chance(self.fresh_fraction) {
            let fresh = self.unshared_request(
                req.phase,
                Unshared::Fresh {
                    tenant: FRESH_TENANT,
                    neighbour: false,
                },
            );
            self.fresh_pending.push_back(fresh);
        }
        let lo = (self.neighbour_window.0 * self.ops as f64) as u64;
        let hi = (self.neighbour_window.1 * self.ops as f64) as u64;
        if self.neighbour_rate > 0.0
            && (lo..hi).contains(&self.issued)
            && self.neighbour_rng.chance(self.neighbour_rate)
        {
            let burst = self.unshared_request(
                req.phase,
                Unshared::Fresh {
                    tenant: NEIGHBOUR_TENANT,
                    neighbour: true,
                },
            );
            self.fresh_pending.push_back(burst);
        }
        Some(req)
    }
}

impl Workload {
    fn unshared_request(&mut self, phase: usize, kind: Unshared) -> Request {
        self.fresh_n += 1;
        let n = self.fresh_n;
        let (tag, blocks, tokens_min, tokens_span) = match kind {
            Unshared::Fresh { .. } => ("fresh", FRESH_BLOCKS, FRESH_TOKENS_MIN, FRESH_TOKENS_SPAN),
            Unshared::Batch => ("batch", BATCH_BLOCKS, BATCH_TOKENS_MIN, BATCH_TOKENS_SPAN),
        };
        let rng = match kind {
            Unshared::Fresh {
                neighbour: true, ..
            } => &mut self.neighbour_rng,
            Unshared::Fresh {
                neighbour: false, ..
            } => &mut self.fresh_rng,
            Unshared::Batch => &mut self.batch_rng,
        };
        let model = if self.one_model { 0 } else { rng.below(MODELS) };
        let tokens = tokens_min + rng.below(tokens_span);
        let mut parent = ROOT;
        let mut link = |label: String| {
            let (id, meta) = kv(None, parent, label.as_bytes(), Origin::Session);
            parent = id;
            (id, meta)
        };
        let chain: Chain = (0..blocks)
            .map(|d| link(format!("{tag}:{n}:{d}")))
            .collect();
        let produces = self.tokens_per_block.map_or_else(Vec::new, |per_block| {
            (0..tokens.div_ceil(per_block))
                .map(|d| link(format!("{tag}:{n}:out:{d}")))
                .collect()
        });
        let (max_tokens, slo, tenant, program) = match kind {
            Unshared::Fresh { tenant, .. } => (
                self.max_tokens(tokens),
                Slo::Interactive,
                tenant,
                PROGRAM_FRESH | n,
            ),
            Unshared::Batch => (
                self.max_tokens(tokens).max(tokens),
                Slo::Throughput,
                BATCH_TENANT,
                PROGRAM_BATCH | n,
            ),
        };
        Request {
            phase,
            chain,
            requires: Self::shards_of(model),
            hint: None,
            completes: None,
            exec_ns: tokens * DECODE_NS_PER_TOKEN,
            tokens,
            gang: None,
            produces,
            max_tokens,
            slo,
            retention: Retention::default(),
            concurrent: true,
            tenant: Some(tenant),
            program,
        }
    }

    fn next_base(&mut self) -> Option<Request> {
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
                    f.tenant,
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
                    retention: Retention::default(),
                    concurrent: false,
                    tenant: None,
                    program: PROGRAM_TASK | q.task,
                });
            }
            let tail = q.chain.last().map_or(ROOT, |(id, _)| *id);
            let produces = if q.tokens > 0 {
                self.produced(tail, q.tokens, &format!("out:{}", q.task), Origin::TaskOut)
            } else {
                Vec::new()
            };
            let retention = Retention {
                retain: None,
                evict_first_from: (!produces.is_empty()).then_some(q.chain.len()),
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
                retention,
                concurrent: false,
                tenant: q.tenant,
                program: PROGRAM_TASK | q.task,
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
            retention: Retention::default(),
            concurrent: false,
            tenant: None,
            program: 0,
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
            None,
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
        tenant: Option<u32>,
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
                tenant,
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
        let produces = self.produced(
            tail,
            tokens,
            &format!("s:{}:{}", turn.session, turn.index),
            Origin::Session,
        );
        self.grow(turn, produces.len() as u64);
        let requires = self.model_shards(tenant);
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
                None,
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
            retention: Retention::default(),
            concurrent: false,
            tenant: Some(tenant as u32),
            program: turn.session,
        }
    }

    fn faas_request(&mut self, phase: usize) -> Request {
        let f = if self.mix.is_some() {
            let model = self.drawn_model();
            self.rng.zipf(FUNCTIONS / MODELS, 1.5) * MODELS + model
        } else {
            self.rng.zipf(FUNCTIONS, 1.5)
        };
        let chain = self.faas_for(f);
        let exec_ns = self.faas_exec();
        let hint = if self.rng.chance(FLOW_FRACTION) {
            let downstream = self.flow_chain(f);
            let requires = Self::shards_of(self.flow_model(f));
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
            retention: Retention::default(),
            concurrent: false,
            tenant: None,
            program: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn trace(origins: Option<Origins>) -> Vec<Request> {
        let w = Workload::with_fanout(2, 3_000, 1.0, 0.15).with_decode_kv(TOKENS_PER_KV_BLOCK);
        match origins {
            Some(o) => w.with_origins(o).collect(),
            None => w.collect(),
        }
    }

    #[test]
    fn every_decode_names_a_program_and_a_program_spans_several_calls() {
        let decodes: Vec<Request> = trace(None).into_iter().filter(|r| r.tokens > 0).collect();
        assert!(decodes.iter().all(|r| r.program != 0));
        let programs: HashSet<u64> = decodes.iter().map(|r| r.program).collect();
        assert!(
            programs.len() < decodes.len(),
            "a session's turns share one program"
        );
    }

    #[test]
    fn the_batch_class_is_a_stream_of_its_own_and_an_unshared_throughput_shape() {
        let plain: Vec<Request> = Workload::with_fanout(3, 2_000, 1.0, 0.1)
            .with_decode_kv(TOKENS_PER_KV_BLOCK)
            .collect();
        let mixed: Vec<Request> = Workload::with_fanout(3, 2_000, 1.0, 0.1)
            .with_decode_kv(TOKENS_PER_KV_BLOCK)
            .with_batch(0.1)
            .collect();
        let batch = |r: &&Request| r.tenant == Some(BATCH_TENANT);
        let (others, batched): (Vec<&Request>, Vec<&Request>) =
            mixed.iter().partition(|r| !batch(r));
        assert_eq!(format!("{plain:?}"), format!("{others:?}"));
        assert!(batched.len() > 100);
        for r in &batched {
            assert_eq!(r.slo, Slo::Throughput);
            assert_eq!(r.chain.len() as u64, BATCH_BLOCKS);
            assert!((BATCH_TOKENS_MIN..BATCH_TOKENS_MIN + BATCH_TOKENS_SPAN).contains(&r.tokens));
            assert!(r.concurrent && r.completes.is_none() && r.hint.is_none());
        }
        let programs: HashSet<u64> = batched.iter().map(|r| r.program).collect();
        assert_eq!(
            programs.len(),
            batched.len(),
            "every batch request is a program of one call"
        );
    }

    #[test]
    fn tracking_origins_changes_no_request() {
        let plain = format!("{:?}", trace(None));
        let tracked = format!("{:?}", trace(Some(Origins::default())));
        assert_eq!(plain, tracked);
    }

    #[test]
    fn every_kv_block_a_request_names_has_an_origin_that_fits_its_place() {
        let origins = Origins::default();
        let requests = trace(Some(origins.clone()));
        let mut seen = HashSet::new();
        for req in &requests {
            let agents = req.gang.iter().flat_map(|g| g.agents.iter());
            let chains = std::iter::once((&req.chain, &req.produces))
                .chain(agents.map(|a| (&a.chain, &a.produces)));
            for (chain, produces) in chains {
                for (id, meta) in chain.iter().chain(produces) {
                    if meta.kind != BlobKind::KvBlock {
                        continue;
                    }
                    let origin = origins.of(id).expect("registered when generated");
                    seen.insert(origin.idx());
                }
            }
            if let Some((id, _)) = req
                .chain
                .first()
                .filter(|(_, m)| m.kind == BlobKind::KvBlock)
            {
                assert!(
                    matches!(origins.of(id), Some(Origin::Tenant | Origin::FlowPrompt)),
                    "a chain starts at a tenant prefix or a flow prompt"
                );
            }
            for agent in req.gang.iter().flat_map(|g| &g.agents) {
                let split = agent.retention.retain.map_or(0, |r| r.upto);
                assert_eq!(origins.of(&agent.chain[split].0), Some(Origin::Agent));
                assert_ne!(origins.of(&agent.chain[split - 1].0), Some(Origin::Agent));
                assert!(
                    agent
                        .produces
                        .iter()
                        .all(|(id, _)| origins.of(id) == Some(Origin::AgentOut))
                );
            }
        }
        assert_eq!(seen.len(), Origin::N, "the fixture must reach every origin");
    }

    #[test]
    fn a_false_hint_is_built_from_the_same_blocks_a_real_flow_is() {
        let origins = Origins::default();
        let requests = trace(Some(origins.clone()));
        let hinted: Vec<BlobId> = requests
            .iter()
            .filter_map(|r| r.hint.as_ref())
            .filter(|h| {
                h.downstream
                    .first()
                    .is_some_and(|(id, _)| origins.of(id) == Some(Origin::FlowPrompt))
            })
            .filter_map(|h| h.downstream.last().map(|(id, _)| *id))
            .collect();
        assert!(hinted.len() > 20);
        let tips: HashSet<BlobId> = (0..FLOW_FUNCTIONS)
            .flat_map(|f| {
                (0..FLOW_CALLS)
                    .filter_map(move |call| flow_downstream(f, call).last().map(|(id, _)| *id))
            })
            .collect();
        assert!(hinted.iter().all(|id| tips.contains(id)));
        let phantom = flow_downstream(3, FLOW_CALLS + 1);
        assert_eq!(phantom.len(), FLOW_PROMPT_BLOCKS as usize + 4);
        assert!(phantom.windows(2).all(|w| w[1].1.parent == Some(w[0].0)));
    }

    #[test]
    fn a_fan_out_agent_declares_its_parent_and_its_scratch() {
        let requests = trace(None);
        let agents: Vec<&Agent> = requests
            .iter()
            .flat_map(|r| r.gang.iter().flat_map(|g| &g.agents))
            .collect();
        assert!(!agents.is_empty());
        for a in agents {
            let r = a.retention;
            let upto = r.retain.expect("declared").upto;
            assert_eq!(r.retain.map(|x| x.lead_ops), Some(RESUME_LEAD_OPS));
            assert_eq!(r.evict_first_from, Some(upto));
            assert_eq!(a.chain.len(), upto + SUBAGENT_BLOCKS as usize);
        }
        let queued_with_output = requests
            .iter()
            .filter(|r| r.completes.is_some() && !r.produces.is_empty())
            .all(|r| r.retention.evict_first_from == Some(r.chain.len()));
        assert!(queued_with_output);
        assert!(
            requests
                .iter()
                .filter(|r| r.completes.is_none())
                .all(|r| { r.retention == Retention::default() || r.gang.is_some() })
        );
    }
    #[test]
    fn model_of_names_the_model_a_request_requires() {
        let requests = trace(None);
        let mut seen = HashSet::new();
        for req in &requests {
            let expected = req.requires.first().map(|(id, _)| *id);
            match model_of(&req.requires) {
                Some(m) => {
                    let first = BlobId::leaf(
                        format!("shard:{}", u64::from(m) * SHARDS_PER_MODEL).as_bytes(),
                    );
                    assert_eq!(expected, Some(first));
                    seen.insert(m);
                }
                None => assert!(req.requires.is_empty()),
            }
        }
        assert_eq!(
            seen.len(),
            MODEL_COUNT,
            "the fixture must reach every model"
        );
    }

    #[test]
    fn a_one_model_trace_names_the_first_model_only() {
        let workload = Workload::with_fanout(2, 2_000, 1.0, 0.15).with_one_model(true);
        for req in workload {
            assert!(model_of(&req.requires).is_none_or(|m| m == 0));
            for agent in req.gang.iter().flat_map(|g| &g.agents) {
                assert_eq!(model_of(&agent.requires), Some(0));
            }
        }
    }

    fn ids(chain: &[(BlobId, BlobMeta)]) -> Vec<BlobId> {
        chain.iter().map(|(id, _)| *id).collect()
    }

    fn keyed_pair() -> (Vec<Request>, Vec<Request>) {
        let build = |keyed| {
            Workload::with_fanout(2, 3_000, 1.0, 0.15)
                .with_model_keyed(keyed)
                .collect::<Vec<Request>>()
        };
        (build(false), build(true))
    }

    #[test]
    fn keying_changes_only_the_parent_prefix_of_agents_on_another_model() {
        let (plain, keyed) = keyed_pair();
        assert_eq!(plain.len(), keyed.len());
        let mut foreign = 0;
        for (p, k) in plain.iter().zip(&keyed) {
            assert_eq!(p.chain.len(), k.chain.len());
            assert_eq!(p.tokens, k.tokens);
            let (Some(pg), Some(kg)) = (&p.gang, &k.gang) else {
                assert_eq!(format!("{:?}", p.chain), format!("{:?}", k.chain));
                continue;
            };
            for (pa, ka) in pg.agents.iter().zip(&kg.agents) {
                assert_eq!(pa.chain.len(), ka.chain.len());
                assert_eq!(model_of(&pa.requires), model_of(&ka.requires));
                let upto = pa.retention.retain.map_or(0, |r| r.upto);
                if ids(&pa.chain[..upto]) == ids(&ka.chain[..upto]) {
                    continue;
                }
                foreign += 1;
                assert!(ka.chain.windows(2).all(|w| w[1].1.parent == Some(w[0].0)));
                assert!(
                    ka.chain[..upto]
                        .iter()
                        .zip(&pa.chain[..upto])
                        .all(|(k, p)| k.0 != p.0)
                );
            }
        }
        assert!(
            foreign > 0,
            "the fixture must reach an agent on another model"
        );
    }

    #[test]
    fn siblings_on_one_foreign_model_share_the_keyed_prefix() {
        let (_, keyed) = keyed_pair();
        let mut shared = 0;
        for gang in keyed.iter().filter_map(|r| r.gang.as_ref()) {
            let upto = gang.agents[0].retention.retain.map_or(0, |r| r.upto);
            for (i, a) in gang.agents.iter().enumerate() {
                for b in &gang.agents[i + 1..] {
                    if model_of(&a.requires) == model_of(&b.requires)
                        && ids(&a.chain[..upto]) == ids(&b.chain[..upto])
                    {
                        shared += 1;
                    }
                    if model_of(&a.requires) != model_of(&b.requires) {
                        assert_ne!(a.chain[upto - 1].0, b.chain[upto - 1].0);
                    }
                }
            }
        }
        assert!(shared > 0);
    }

    #[test]
    fn a_keyed_block_carries_the_origin_of_the_block_it_was_keyed_from() {
        let origins = Origins::default();
        let requests: Vec<Request> = Workload::with_fanout(2, 3_000, 1.0, 0.15)
            .with_model_keyed(true)
            .with_origins(origins.clone())
            .collect();
        for req in &requests {
            for agent in req.gang.iter().flat_map(|g| &g.agents) {
                let upto = agent.retention.retain.map_or(0, |r| r.upto);
                for (id, _) in &agent.chain[..upto] {
                    assert!(matches!(
                        origins.of(id),
                        Some(Origin::Tenant | Origin::Session)
                    ));
                }
            }
        }
    }
    #[test]
    fn a_model_mix_moves_the_demand_between_models_phase_by_phase() {
        let ops = 20_000;
        let mix = rotating_mix([0.55, 0.25, 0.12, 0.08]);
        let trace: Vec<Request> = Workload::with_fanout(2, ops, 0.0, 0.0)
            .with_model_mix(mix)
            .collect();
        let mut demand = [[0u64; MODEL_COUNT]; PHASES];
        for req in trace
            .iter()
            .filter(|r| r.tokens > 0 && r.completes.is_none())
        {
            if let Some(m) = model_of(&req.requires) {
                demand[req.phase][usize::from(m)] += 1;
            }
        }
        for (phase, row) in demand.iter().enumerate() {
            let total: u64 = row.iter().sum();
            let hot = row
                .iter()
                .enumerate()
                .max_by_key(|(_, n)| **n)
                .map(|(m, _)| m);
            assert_eq!(hot, Some(phase), "phase {phase}: {row:?}");
            assert!(
                row[phase] as f64 > 0.45 * total as f64,
                "phase {phase}: {row:?}"
            );
        }
    }

    #[test]
    fn a_model_mix_maps_tenants_to_models_in_blocks() {
        let roots: HashMap<BlobId, usize> = (0..TENANTS)
            .map(|t| (BlobId::leaf(format!("tenant:{t}:0").as_bytes()), t as usize))
            .collect();
        let trace: Vec<Request> = Workload::with_fanout(3, 4_000, 0.0, 0.0)
            .with_model_mix(rotating_mix([0.4, 0.3, 0.2, 0.1]))
            .collect();
        let mut chats = 0;
        for req in trace
            .iter()
            .filter(|r| r.completes.is_none() && r.tokens > 0)
        {
            let Some(tenant) = req.chain.first().and_then(|(id, _)| roots.get(id)) else {
                continue;
            };
            chats += 1;
            assert_eq!(
                model_of(&req.requires).map(usize::from),
                Some(tenant / (TENANTS as usize / MODEL_COUNT))
            );
        }
        assert!(chats > 500);
    }

    fn base_of(trace: Vec<Request>) -> Vec<String> {
        trace
            .into_iter()
            .filter(|r| !r.concurrent)
            .map(|r| format!("{r:?}"))
            .collect()
    }

    #[test]
    fn fresh_requests_are_extra_concurrent_and_leave_the_base_trace_alone() {
        let plain: Vec<Request> = Workload::with_fanout(2, 3_000, 1.0, 0.1).collect();
        let fresh: Vec<Request> = Workload::with_fanout(2, 3_000, 1.0, 0.1)
            .with_fresh(0.2)
            .collect();
        let extra: Vec<&Request> = fresh.iter().filter(|r| r.concurrent).collect();
        assert!(plain.iter().all(|r| !r.concurrent));
        assert_eq!(base_of(plain), base_of(fresh.clone()));
        let share = extra.len() as f64 / (fresh.len() - extra.len()) as f64;
        assert!((share - 0.2).abs() < 0.03, "{share}");
        let mut seen = HashSet::new();
        for r in &extra {
            assert_eq!(r.chain.len() as u64, FRESH_BLOCKS);
            assert!((FRESH_TOKENS_MIN..FRESH_TOKENS_MIN + FRESH_TOKENS_SPAN).contains(&r.tokens));
            assert!(model_of(&r.requires).is_some() && r.gang.is_none() && r.hint.is_none());
            assert!(
                r.chain.iter().all(|(id, _)| seen.insert(*id)),
                "a fresh prompt is never shared"
            );
        }
    }

    #[test]
    fn fresh_prompts_on_a_one_model_trace_name_the_first_model() {
        let trace: Vec<Request> = Workload::with_fanout(2, 2_000, 1.0, 0.0)
            .with_one_model(true)
            .with_fresh(0.3)
            .collect();
        let fresh: Vec<&Request> = trace.iter().filter(|r| r.concurrent).collect();
        assert!(fresh.len() > 100);
        assert!(fresh.iter().all(|r| model_of(&r.requires) == Some(0)));
    }
    #[test]
    fn a_neighbour_bursts_only_inside_its_window_and_leaves_the_base_trace_alone() {
        let ops = 10_000;
        let plain: Vec<Request> = Workload::with_fanout(2, ops, 1.0, 0.05).collect();
        let loud: Vec<Request> = Workload::with_fanout(2, ops, 1.0, 0.05)
            .with_neighbour(0.2)
            .collect();
        assert_eq!(base_of(plain), base_of(loud.clone()));
        let mut base_seen = 0u64;
        let mut burst = Vec::new();
        for r in &loud {
            if r.concurrent {
                burst.push((base_seen, r));
            } else {
                base_seen += 1;
            }
        }
        assert!(
            burst
                .iter()
                .all(|(_, r)| r.tenant == Some(NEIGHBOUR_TENANT))
        );
        let inside = (burst.first().map(|b| b.0), burst.last().map(|b| b.0));
        assert!(inside.0 >= Some(ops * 4 / 10 - 50) && inside.1 <= Some(ops * 6 / 10 + 500));
        let share = burst.len() as f64 / (0.2 * 0.2 * ops as f64);
        assert!((share - 1.0).abs() < 0.15, "{share}");
    }

    #[test]
    fn sessions_declare_their_tenant_and_a_fresh_stream_another_id() {
        let trace: Vec<Request> = Workload::with_fanout(2, 3_000, 1.0, 0.1)
            .with_fresh(0.2)
            .collect();
        let tenants: HashSet<u32> = trace.iter().filter_map(|r| r.tenant).collect();
        assert!(tenants.contains(&FRESH_TENANT));
        assert!(tenants.iter().filter(|&&t| t < 24).count() > 10);
        assert!(
            trace
                .iter()
                .filter(|r| r.gang.is_some())
                .flat_map(|r| r.gang.iter().flat_map(|g| &g.agents))
                .all(|a| a.tenant.is_some_and(|t| t < 24))
        );
        assert!(
            trace
                .iter()
                .filter(|r| r.tokens == 0 && r.gang.is_none())
                .all(|r| r.tenant.is_none())
        );
    }

    #[test]
    fn a_shared_prefix_is_one_chain_per_model_under_every_tenants_of_that_model() {
        let plain = Workload::new(2, 100, 1.0);
        let shared = Workload::new(2, 100, 1.0).with_shared_prefix(true);
        for t in 0..TENANTS as usize {
            assert_eq!(plain.tenant_prefix[t].len(), shared.tenant_prefix[t].len());
        }
        let ids = |w: &Workload, t: usize| -> Vec<BlobId> {
            w.tenant_prefix[t].iter().map(|(id, _)| *id).collect()
        };
        let k = SHARED_PREFIX_BLOCKS as usize;
        let (a, b, other) = (ids(&shared, 0), ids(&shared, 4), ids(&shared, 1));
        assert_eq!(a[..k], b[..k]);
        assert_ne!(a[..k], other[..k]);
        assert_ne!(a[k..], b[k..]);
        assert!(
            plain.tenant_prefix[0]
                .iter()
                .zip(&plain.tenant_prefix[4])
                .all(|(x, y)| x.0 != y.0)
        );
        let first_own = shared.tenant_prefix[0][k].1;
        assert_eq!(first_own.parent, Some(shared.tenant_prefix[0][k - 1].0));
    }

    #[test]
    fn a_shared_prefix_off_is_the_published_trace() {
        let a: Vec<Request> = Workload::with_fanout(2, 2_000, 1.0, 0.1).collect();
        let b: Vec<Request> = Workload::with_fanout(2, 2_000, 1.0, 0.1)
            .with_shared_prefix(false)
            .with_neighbour(0.0)
            .collect();
        assert_eq!(base_of(a), base_of(b));
    }

    #[test]
    fn a_neighbour_duty_centres_its_window_and_the_default_is_a_fifth() {
        let ops = 10_000;
        let span = |w: Workload| -> (u64, u64) {
            let mut base = 0u64;
            let mut seen = (u64::MAX, 0u64);
            for r in w {
                if r.concurrent {
                    seen = (seen.0.min(base), seen.1.max(base));
                } else {
                    base += 1;
                }
            }
            seen
        };
        let (lo, hi) = span(Workload::with_fanout(2, ops, 1.0, 0.0).with_neighbour(1.0));
        assert!(lo >= 4_000 - 50 && hi <= 6_000 + 50, "{lo} {hi}");
        let wide = span(
            Workload::with_fanout(2, ops, 1.0, 0.0)
                .with_neighbour(1.0)
                .with_neighbour_duty(0.6),
        );
        assert!(wide.0 >= 2_000 - 50 && wide.1 <= 8_000 + 50, "{wide:?}");
        assert!(wide.1 - wide.0 > 5_500);
    }
}
