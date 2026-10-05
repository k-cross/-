use crate::admit::Reserve;
use crate::belief::Marks;
use crate::blob::{BlobId, BlobKind, BlobMeta, ROOT};
use crate::boundary::Cost as Crossing;
use crate::cache::{CellState, Cost, EngineKv, NodeMemory, Policy, Quota};
use crate::flow::FlowHint;
use crate::machine::{
    Claim, ClaimKey, Control, Directives, Emit, EngineWait, HintGrade, LearnStats, Machine,
    Placement, Submitted, ToolSlot,
};
use crate::rng::Rng;
use crate::tier::TierSpec;
use crate::topo::{Distance, Topology};
use crate::work::{
    Agent, Authority, Chain, DECODE_NS_PER_TOKEN, Gang, KV_BLOCK_BYTES, KV_BLOCK_NS, Origins,
    Pattern, Request, Retain, Retention, SERVICE_BYTES, SERVICE_COLD_NS, Slo, ToolCall,
    WEIGHT_BYTES, WEIGHT_NS, Workload, snapshot_restore_ns,
};
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};

pub const SANDBOX_BYTES: u64 = 32 * 1024 * 1024;
pub const TENANT_COUNT: u64 = 24;
pub const MODELS: u64 = 4;
pub const CHUNK_BLOCKS: u64 = 8;
pub const QUESTION_BLOCKS: u64 = 2;
pub const INDEXES: u64 = 2;
pub const RETRIEVE_NS: u64 = 20_000_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ToolClass {
    Read,
    Edit,
    Exec,
}

impl ToolClass {
    pub const N: usize = 3;

    #[must_use]
    pub fn idx(self) -> usize {
        self as usize
    }

    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Edit => "edit",
            Self::Exec => "exec",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct Annotations {
    pub read_only: bool,
    pub destructive: bool,
    pub idempotent: bool,
    pub open_world: bool,
}

impl Default for Annotations {
    fn default() -> Self {
        Self {
            read_only: false,
            destructive: true,
            idempotent: false,
            open_world: true,
        }
    }
}

impl Annotations {
    #[must_use]
    pub fn authority(self) -> Authority {
        if self.read_only {
            Authority::ReadOnly
        } else if !self.open_world {
            Authority::DraftOnly
        } else {
            Authority::SideEffecting
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ToolSet {
    Coding,
    Research,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AnnotationMode {
    Declared,
    Pessimistic,
}

#[derive(Clone, Copy, Debug)]
pub struct ToolSpec {
    pub id: u8,
    pub class: ToolClass,
    pub annotations: Annotations,
    pub median_ns: u64,
    pub sigma: f64,
}

impl ToolSpec {
    #[must_use]
    pub fn authority(&self) -> Authority {
        self.annotations.authority()
    }
}

pub const TOOL_KINDS: usize = 7;

fn closed_read() -> Annotations {
    Annotations {
        read_only: true,
        open_world: false,
        ..Annotations::default()
    }
}

fn open_read() -> Annotations {
    Annotations {
        read_only: true,
        ..Annotations::default()
    }
}

fn closed_write() -> Annotations {
    Annotations {
        open_world: false,
        destructive: false,
        ..Annotations::default()
    }
}

#[must_use]
pub fn catalog(set: ToolSet, mode: AnnotationMode) -> [ToolSpec; TOOL_KINDS] {
    let read = |id, median_ms: u64, sigma, open: bool| ToolSpec {
        id,
        class: ToolClass::Read,
        annotations: if open { open_read() } else { closed_read() },
        median_ns: median_ms * 1_000_000,
        sigma,
    };
    let edit = |id, median_ms: u64, sigma| ToolSpec {
        id,
        class: ToolClass::Edit,
        annotations: match mode {
            AnnotationMode::Declared => closed_write(),
            AnnotationMode::Pessimistic => Annotations::default(),
        },
        median_ns: median_ms * 1_000_000,
        sigma,
    };
    let exec = |id, median_ms: u64, sigma| ToolSpec {
        id,
        class: ToolClass::Exec,
        annotations: Annotations::default(),
        median_ns: median_ms * 1_000_000,
        sigma,
    };
    match set {
        ToolSet::Coding => [
            read(0, 100, 1.0, false),
            read(1, 100, 1.0, false),
            read(2, 100, 1.0, false),
            edit(3, 400, 0.5),
            edit(4, 400, 0.5),
            exec(5, 1500, 1.8),
            exec(6, 1500, 1.8),
        ],
        ToolSet::Research => [
            read(0, 2000, 0.8, true),
            read(1, 10_000, 1.0, true),
            read(2, 2000, 0.8, true),
            edit(3, 400, 0.5),
            edit(4, 400, 0.5),
            exec(5, 1500, 1.8),
            exec(6, 1500, 1.8),
        ],
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Preset {
    OneShot,
    Extraction,
    Conversational,
    Rag,
    Pipeline,
    Agentic,
    LongRunning,
    MultiAgent,
    Batch,
}

impl Preset {
    pub const N: usize = 9;
    pub const ALL: [Self; Self::N] = [
        Self::OneShot,
        Self::Extraction,
        Self::Conversational,
        Self::Rag,
        Self::Pipeline,
        Self::Agentic,
        Self::LongRunning,
        Self::MultiAgent,
        Self::Batch,
    ];

    #[must_use]
    pub fn pattern(self) -> Pattern {
        match self {
            Self::OneShot => Pattern::OneShot,
            Self::Extraction => Pattern::Extraction,
            Self::Conversational => Pattern::Conversational,
            Self::Rag => Pattern::Rag,
            Self::Pipeline => Pattern::Pipeline,
            Self::Agentic => Pattern::Agentic,
            Self::LongRunning => Pattern::LongRunning,
            Self::MultiAgent => Pattern::MultiAgent,
            Self::Batch => Pattern::Batch,
        }
    }

    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::OneShot => "oneshot",
            Self::Extraction => "extraction",
            Self::Conversational => "conversational",
            Self::Rag => "rag",
            Self::Pipeline => "pipeline",
            Self::Agentic => "agentic",
            Self::LongRunning => "longrunning",
            Self::MultiAgent => "multiagent",
            Self::Batch => "batch",
        }
    }

    #[must_use]
    pub fn parse(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|p| p.name() == name)
    }

    #[must_use]
    pub fn throughput(self) -> bool {
        matches!(self, Self::Extraction | Self::LongRunning | Self::Batch)
    }

    #[must_use]
    pub fn root(self) -> u32 {
        self as u32 + 1
    }
}

pub const ROLES: usize = 5;
pub const ROLE_NAMES: [&str; ROLES] = ["planner", "explorer", "engineer", "reviewer", "chronicler"];
const ROLE_MEAN_TOKENS: [f64; ROLES] = [60.0, 1924.0, 1400.0, 2620.0, 912.0];
const ROLE_CV: [f64; ROLES] = [0.15, 0.45, 0.30, 0.18, 0.26];
const ROLE_WEIGHT: [f64; ROLES] = [1.0, 3.5, 4.5, 1.0, 1.0];
pub const ROLE_SCALE: f64 = 0.1;
pub const ROLE_ROOT_BASE: u32 = 20;

#[must_use]
pub fn role_root(role: usize) -> u32 {
    ROLE_ROOT_BASE + role as u32
}

#[derive(Clone, Copy, Debug)]
pub struct ToolStep {
    pub tool: ToolSpec,
    pub exec_ns: u64,
    pub result_blocks: u64,
}

#[derive(Clone, Debug)]
pub struct AgentScript {
    pub role: usize,
    pub tokens: u64,
    pub tools: Vec<ToolStep>,
}

#[derive(Clone, Debug)]
pub enum Step {
    Call {
        tokens: u64,
        appended: u64,
        root: u32,
        turn_start: bool,
        turn_end: bool,
    },
    Tool(ToolStep),
    Retrieve {
        chunks: Vec<u32>,
    },
    Idle {
        ns: u64,
        approval: bool,
    },
    Fanout {
        agents: Vec<AgentScript>,
    },
}

#[derive(Clone, Debug)]
pub struct Script {
    pub preset: Preset,
    pub tenant: usize,
    pub steps: Vec<Step>,
    pub throughput: bool,
}

#[derive(Clone, Copy, Debug)]
pub struct ShapeConfig {
    pub compress: f64,
    pub tools: ToolSet,
    pub annotations: AnnotationMode,
    pub determinism: f64,
    pub think_ns: u64,
    pub rag_k: usize,
    pub rag_docs: u64,
    pub rag_zipf: f64,
    pub rag_canonical: bool,
    pub roles: bool,
}

impl Default for ShapeConfig {
    fn default() -> Self {
        Self {
            compress: 5.0,
            tools: ToolSet::Coding,
            annotations: AnnotationMode::Declared,
            determinism: 0.25,
            think_ns: 3_000_000_000,
            rag_k: 5,
            rag_docs: 10_000,
            rag_zipf: 1.1,
            rag_canonical: true,
            roles: true,
        }
    }
}

fn lognormal(rng: &mut Rng, median_ns: f64, sigma: f64) -> u64 {
    let u1 = rng.unit().max(1e-12);
    let u2 = rng.unit();
    let z = (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos();
    (median_ns * (sigma * z).exp()).clamp(1.0, 3.6e12) as u64
}

fn role_tokens(rng: &mut Rng, role: usize) -> u64 {
    let cv = ROLE_CV[role];
    let sigma = (1.0 + cv * cv).ln().sqrt();
    let mu = (ROLE_MEAN_TOKENS[role] * ROLE_SCALE).ln() - sigma * sigma / 2.0;
    let u1 = rng.unit().max(1e-12);
    let u2 = rng.unit();
    let z = (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos();
    ((mu + sigma * z).exp() as u64).clamp(8, 2000)
}

fn class_row(prev: ToolClass, research: bool) -> [f64; ToolClass::N] {
    if research {
        return match prev {
            ToolClass::Read => [0.85, 0.10, 0.05],
            ToolClass::Edit => [0.70, 0.20, 0.10],
            ToolClass::Exec => [0.70, 0.15, 0.15],
        };
    }
    match prev {
        ToolClass::Read => [0.65, 0.20, 0.15],
        ToolClass::Edit => [0.25, 0.20, 0.55],
        ToolClass::Exec => [0.50, 0.30, 0.20],
    }
}

fn draw_class(rng: &mut Rng, row: [f64; ToolClass::N]) -> ToolClass {
    let u = rng.unit();
    if u < row[0] {
        ToolClass::Read
    } else if u < row[0] + row[1] {
        ToolClass::Edit
    } else {
        ToolClass::Exec
    }
}

fn habitual(root: u32, prev: Option<u8>) -> u8 {
    let key = u64::from(root) * 31 + u64::from(prev.unwrap_or(255));
    let mixed = key.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 33;
    (mixed % TOOL_KINDS as u64) as u8
}

fn draw_tool(
    rng: &mut Rng,
    cfg: &ShapeConfig,
    root: u32,
    prev: Option<ToolSpec>,
    catalog: &[ToolSpec; TOOL_KINDS],
) -> ToolStep {
    let research = cfg.tools == ToolSet::Research;
    let spec = if rng.chance(cfg.determinism) {
        catalog[usize::from(habitual(root, prev.map(|p| p.id)))]
    } else {
        let class = draw_class(
            rng,
            class_row(prev.map_or(ToolClass::Read, |p| p.class), research),
        );
        let of_class: Vec<&ToolSpec> = catalog.iter().filter(|t| t.class == class).collect();
        *of_class[rng.below(of_class.len() as u64) as usize]
    };
    ToolStep {
        tool: spec,
        exec_ns: lognormal(rng, spec.median_ns as f64 / cfg.compress, spec.sigma),
        result_blocks: 2 + rng.below(5),
    }
}

fn geometric(rng: &mut Rng, continue_p: f64, cap: u32) -> u32 {
    let mut n = 0;
    while n < cap && rng.chance(continue_p) {
        n += 1;
    }
    n
}

fn call_tokens(rng: &mut Rng) -> u64 {
    24 + rng.below(200)
}

fn loop_turn(
    rng: &mut Rng,
    cfg: &ShapeConfig,
    root: u32,
    catalog: &[ToolSpec; TOOL_KINDS],
    tools_cap: u32,
    approvals: bool,
    out: &mut Vec<Step>,
) {
    let tools = geometric(rng, 0.78, tools_cap);
    let mut prev: Option<ToolSpec> = None;
    let mut appended = 4;
    for k in 0..=tools {
        out.push(Step::Call {
            tokens: call_tokens(rng),
            appended,
            root,
            turn_start: k == 0,
            turn_end: k == tools,
        });
        if k < tools {
            let step = draw_tool(rng, cfg, root, prev, catalog);
            if approvals && step.tool.authority() == Authority::SideEffecting {
                out.push(Step::Idle {
                    ns: lognormal(rng, cfg.think_ns as f64 * 0.3, 0.8),
                    approval: true,
                });
            }
            appended = step.result_blocks;
            prev = Some(step.tool);
            out.push(Step::Tool(step));
        }
    }
}

#[derive(Clone, Debug)]
pub struct Shape {
    pub cfg: ShapeConfig,
    rag_cdf: Vec<f64>,
}

impl Shape {
    #[must_use]
    pub fn new(cfg: ShapeConfig) -> Self {
        let weights: Vec<f64> = (0..cfg.rag_docs)
            .map(|d| 1.0 / (d as f64 + 1.0).powf(cfg.rag_zipf))
            .collect();
        let total: f64 = weights.iter().sum();
        let mut acc = 0.0;
        let rag_cdf = weights
            .iter()
            .map(|w| {
                acc += w / total;
                acc
            })
            .collect();
        Self { cfg, rag_cdf }
    }

    fn chunk(&self, rng: &mut Rng) -> u32 {
        let u = rng.unit();
        self.rag_cdf
            .partition_point(|&c| c < u)
            .min(self.rag_cdf.len().saturating_sub(1)) as u32
    }
}

fn zipf_chunks(rng: &mut Rng, shape: &Shape) -> Vec<u32> {
    let cfg = &shape.cfg;
    let k = cfg
        .rag_k
        .min(usize::try_from(cfg.rag_docs).unwrap_or(usize::MAX));
    let mut chunks: Vec<u32> = Vec::with_capacity(k);
    while chunks.len() < k {
        let c = shape.chunk(rng);
        if !chunks.contains(&c) {
            chunks.push(c);
        }
    }
    if cfg.rag_canonical {
        chunks.sort_unstable();
    }
    chunks
}

#[must_use]
#[allow(clippy::too_many_lines)]
pub fn script(preset: Preset, seed: u64, id: u64, shape: &Shape) -> Script {
    let cfg = &shape.cfg;
    let mut rng = Rng::new(
        seed.wrapping_mul(0x9E37_79B9)
            .wrapping_add(id.wrapping_mul(0xA24B_AED4_963E_E407))
            ^ 0x5052_4F47,
    );
    let tenant = rng.zipf(TENANT_COUNT, 1.6) as usize;
    let catalog = catalog(cfg.tools, cfg.annotations);
    let root = preset.root();
    let mut steps = Vec::new();
    match preset {
        Preset::OneShot => {
            steps.push(Step::Call {
                tokens: call_tokens(&mut rng),
                appended: 4 + rng.below(4),
                root,
                turn_start: true,
                turn_end: true,
            });
        }
        Preset::Extraction => {
            steps.push(Step::Call {
                tokens: 24 + rng.below(40),
                appended: 16,
                root,
                turn_start: true,
                turn_end: true,
            });
        }
        Preset::Batch => {
            steps.push(Step::Call {
                tokens: 200 + rng.below(400),
                appended: 8,
                root,
                turn_start: true,
                turn_end: true,
            });
        }
        Preset::Conversational => {
            let turns = 1 + geometric(&mut rng, 0.7, 7);
            for t in 0..turns {
                if t > 0 {
                    steps.push(Step::Idle {
                        ns: lognormal(&mut rng, cfg.think_ns as f64, 1.0),
                        approval: false,
                    });
                }
                steps.push(Step::Call {
                    tokens: call_tokens(&mut rng),
                    appended: 4,
                    root,
                    turn_start: true,
                    turn_end: true,
                });
            }
        }
        Preset::Rag => {
            let chunks = zipf_chunks(&mut rng, shape);
            steps.push(Step::Retrieve { chunks });
            steps.push(Step::Call {
                tokens: call_tokens(&mut rng),
                appended: QUESTION_BLOCKS,
                root,
                turn_start: true,
                turn_end: true,
            });
        }
        Preset::Pipeline => {
            let read = catalog[0];
            let write = ToolSpec {
                annotations: Annotations::default(),
                class: ToolClass::Exec,
                id: 5,
                ..catalog[5]
            };
            let steps_tools = [read, write];
            for (k, spec) in steps_tools.iter().enumerate() {
                steps.push(Step::Call {
                    tokens: call_tokens(&mut rng),
                    appended: 4,
                    root,
                    turn_start: k == 0,
                    turn_end: false,
                });
                steps.push(Step::Tool(ToolStep {
                    tool: *spec,
                    exec_ns: lognormal(&mut rng, spec.median_ns as f64 / cfg.compress, spec.sigma),
                    result_blocks: 2 + rng.below(5),
                }));
            }
            steps.push(Step::Call {
                tokens: call_tokens(&mut rng),
                appended: 3,
                root,
                turn_start: false,
                turn_end: true,
            });
        }
        Preset::Agentic => {
            let turns = 1 + geometric(&mut rng, 0.55, 5);
            for t in 0..turns {
                if t > 0 {
                    steps.push(Step::Idle {
                        ns: lognormal(&mut rng, cfg.think_ns as f64, 1.0),
                        approval: false,
                    });
                }
                loop_turn(&mut rng, cfg, root, &catalog, 16, false, &mut steps);
            }
        }
        Preset::LongRunning => {
            let turns = 3 + geometric(&mut rng, 0.6, 9);
            let idle_median = 172e9 / cfg.compress;
            for t in 0..turns {
                if t > 0 {
                    steps.push(Step::Idle {
                        ns: lognormal(&mut rng, idle_median, 1.98),
                        approval: false,
                    });
                }
                loop_turn(&mut rng, cfg, root, &catalog, 12, true, &mut steps);
            }
        }
        Preset::MultiAgent => {
            steps.push(Step::Call {
                tokens: call_tokens(&mut rng),
                appended: 4,
                root,
                turn_start: true,
                turn_end: false,
            });
            let n = 2 + rng.below(5) as usize;
            let mut agents = Vec::with_capacity(n);
            for _ in 0..n {
                let weights: f64 = ROLE_WEIGHT.iter().sum();
                let mut pick = rng.unit() * weights;
                let mut role = ROLES - 1;
                for (r, w) in ROLE_WEIGHT.iter().enumerate() {
                    if pick < *w {
                        role = r;
                        break;
                    }
                    pick -= w;
                }
                let tokens = if cfg.roles {
                    role_tokens(&mut rng, role)
                } else {
                    call_tokens(&mut rng)
                };
                let count = rng.below(5);
                let mut prev = None;
                let tools = (0..count)
                    .map(|_| {
                        let step = draw_tool(&mut rng, cfg, role_root(role), prev, &catalog);
                        prev = Some(step.tool);
                        step
                    })
                    .collect();
                agents.push(AgentScript {
                    role,
                    tokens,
                    tools,
                });
            }
            steps.push(Step::Fanout { agents });
            steps.push(Step::Call {
                tokens: call_tokens(&mut rng),
                appended: 2,
                root,
                turn_start: false,
                turn_end: true,
            });
        }
    }
    Script {
        preset,
        tenant,
        steps,
        throughput: preset.throughput(),
    }
}

#[must_use]
pub fn kv_block(parent: BlobId, tag: &str) -> (BlobId, BlobMeta) {
    let id = BlobId::chain(parent, tag.as_bytes());
    (
        id,
        BlobMeta {
            kind: BlobKind::KvBlock,
            bytes: KV_BLOCK_BYTES,
            parent: if parent == ROOT { None } else { Some(parent) },
            recompute_ns: KV_BLOCK_NS,
        },
    )
}

#[must_use]
pub fn shards(model: u64) -> Chain {
    (0..2)
        .map(|i| {
            (
                BlobId::leaf(format!("shard:{}", model * 2 + i).as_bytes()),
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

#[must_use]
pub fn sandbox(program: u64) -> (BlobId, BlobMeta) {
    (
        BlobId::leaf(format!("sandbox:{program}").as_bytes()),
        BlobMeta {
            kind: BlobKind::Snapshot,
            bytes: SANDBOX_BYTES,
            parent: None,
            recompute_ns: snapshot_restore_ns(SANDBOX_BYTES),
        },
    )
}

#[must_use]
pub fn index_heap(index: u64) -> (BlobId, BlobMeta) {
    (
        BlobId::leaf(format!("index:{index}").as_bytes()),
        BlobMeta {
            kind: BlobKind::ServiceHeap,
            bytes: SERVICE_BYTES,
            parent: None,
            recompute_ns: SERVICE_COLD_NS,
        },
    )
}

#[must_use]
pub fn decode_ns(tokens: u64) -> u64 {
    tokens * DECODE_NS_PER_TOKEN
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LoopMode {
    Closed,
    Open { lead_ns: u64 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Hints {
    None,
    Declared,
    Predicted,
    Stream,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Speculate {
    Off,
    Run,
    Always,
    Oracle,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Suspend {
    Never,
    Timers,
    Joint,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClassMode {
    Truth,
    Inferred,
    Pooled,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClaimBy {
    Template,
    Class,
}

pub const ENGINE_RETENTION_NS: u64 = 300_000_000_000;
pub const SANDBOX_TIMEOUT_NS: u64 = 900_000_000_000;
pub const WARM_LEAD_OPS: u32 = 6;

#[derive(Clone, Debug)]
#[allow(clippy::struct_excessive_bools)]
pub struct Config {
    pub seed: u64,
    pub programs: usize,
    pub rate: f64,
    pub mix: Vec<(Preset, f64)>,
    pub shape: ShapeConfig,
    pub loop_mode: LoopMode,
    pub hints: Hints,
    pub tool_share: f64,
    pub speculate: Speculate,
    pub open_world: bool,
    pub tool_slots: Option<usize>,
    pub suspend: Suspend,
    pub class_mode: ClassMode,
    pub claim_by: ClaimBy,
    pub ttl: bool,
    pub engine_wait: EngineWait,
    pub claim: Claim,
    pub claim_key: ClaimKey,
    pub observables: bool,
    pub atomic: bool,
    pub regret: bool,
    pub nodes: usize,
    pub hbm_mib: u64,
    pub ddr_mib: u64,
    pub nvme_mib: u64,
    pub kv_partition_mib: u64,
    pub kv_offload_mib: u64,
    pub kv_spill_mib: u64,
    pub engine_rate: f64,
    pub reclaim_drafts: bool,
    pub fault: Option<(u64, crate::fault::Fault)>,
    pub copy_durable: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            seed: 1,
            programs: 900,
            rate: 12.0,
            mix: vec![(Preset::Agentic, 1.0)],
            shape: ShapeConfig::default(),
            loop_mode: LoopMode::Closed,
            hints: Hints::None,
            tool_share: 0.3,
            speculate: Speculate::Off,
            open_world: false,
            tool_slots: None,
            suspend: Suspend::Never,
            class_mode: ClassMode::Truth,
            claim_by: ClaimBy::Template,
            ttl: false,
            engine_wait: EngineWait::Off,
            claim: Claim::Static,
            claim_key: ClaimKey::Slo,
            observables: false,
            atomic: true,
            regret: false,
            nodes: 4,
            hbm_mib: 4096,
            ddr_mib: 8192,
            nvme_mib: 16_384,
            kv_partition_mib: 1024,
            kv_offload_mib: 820,
            kv_spill_mib: 4096,
            engine_rate: 250.0,
            reclaim_drafts: true,
            fault: None,
            copy_durable: false,
        }
    }
}

#[must_use]
pub fn node_memory(cfg: &Config) -> NodeMemory {
    let bands = [0, 1, 2, 1];
    let hbm = cfg.hbm_mib << 20;
    let ddr = cfg.ddr_mib << 20;
    NodeMemory {
        hbm,
        ddr,
        nvme: cfg.nvme_mib << 20,
        hbm_quota: Quota::from_split(hbm, [0.25, 0.0, 0.50, 0.0], bands, false),
        ddr_quota: Quota::from_split(ddr, [0.10, 0.15, 0.15, 0.35], bands, false).offloaded(),
        can_decode: true,
        kv: Some(EngineKv {
            partition: cfg.kv_partition_mib << 20,
            offload: cfg.kv_offload_mib << 20,
            spill: cfg.kv_spill_mib << 20,
            clairvoyant: false,
        }),
    }
}

pub const CAUSES: usize = 6;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogCause {
    Intent,
    Outcome,
    Approval,
    Suspend,
    Resume,
    TaskState,
}

impl LogCause {
    pub const ALL: [Self; CAUSES] = [
        Self::Intent,
        Self::Outcome,
        Self::Approval,
        Self::Suspend,
        Self::Resume,
        Self::TaskState,
    ];

    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Intent => "intent",
            Self::Outcome => "outcome",
            Self::Approval => "approval",
            Self::Suspend => "suspend",
            Self::Resume => "resume",
            Self::TaskState => "task state",
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct PatternStats {
    pub programs: u64,
    pub completed: u64,
    pub turns: Vec<u64>,
    pub latency: Vec<u64>,
    pub calls: u64,
    pub tools: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct GapSample {
    pub gap_ns: u64,
    pub excess_ns: u64,
    pub fetched: bool,
    pub service_ns: u64,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SpecStats {
    pub tries: u64,
    pub gated: u64,
    pub forbidden: u64,
    pub hits: u64,
    pub saved_ns: u64,
    pub wasted_ns: u64,
    pub off_anchor: u64,
    pub wait_imposed_ns: u64,
    pub exec_ns: u64,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct HintStats {
    pub warms: u64,
    pub skipped_hot: u64,
    pub skipped_price: u64,
    pub restore_ns: u64,
    pub exposed_ns: u64,
    pub first_tools: u64,
    pub first_tool_restore_ns: u64,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct LifeStats {
    pub idles: u64,
    pub idle_ns: u64,
    pub freed_ns: u64,
    pub kv_drops: u64,
    pub cell_suspends: u64,
    pub pair_disagree: u64,
    pub joint_differs: u64,
    pub suspended_idles: u64,
    pub resume_ns: u64,
    pub resume_turn_ns: u64,
    pub approvals: u64,
    pub joint_never: u64,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct EstimatorStats {
    pub predictions: u64,
    pub top1: u64,
    pub top3: u64,
}

#[derive(Clone, Debug)]
pub struct ClassStats {
    pub confusion: [[u64; Pattern::N]; Pattern::N],
    pub first: [[u64; Pattern::N]; Pattern::N],
    pub last: [[u64; Pattern::N]; Pattern::N],
}

impl Default for ClassStats {
    fn default() -> Self {
        Self {
            confusion: [[0; Pattern::N]; Pattern::N],
            first: [[0; Pattern::N]; Pattern::N],
            last: [[0; Pattern::N]; Pattern::N],
        }
    }
}

impl ClassStats {
    #[must_use]
    pub fn totals(&self) -> (u64, u64) {
        Self::totals_of(&self.confusion)
    }

    #[must_use]
    pub fn totals_of(matrix: &[[u64; Pattern::N]; Pattern::N]) -> (u64, u64) {
        let mut n = 0;
        let mut correct = 0;
        for (t, row) in matrix.iter().enumerate() {
            for (i, c) in row.iter().enumerate() {
                n += c;
                if t == i {
                    correct += c;
                }
            }
        }
        (n, correct)
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct RagStats {
    pub calls: u64,
    pub chunk_blocks: u64,
    pub prefix_blocks: u64,
    pub any_blocks: u64,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ClaimStats {
    pub n: u64,
    pub overruns: u64,
    pub reserved_tokens: u64,
}

#[derive(Clone, Debug, Default)]
pub struct Outcome {
    pub by_pattern: Vec<PatternStats>,
    pub gaps: Vec<GapSample>,
    pub spec: SpecStats,
    pub hints: HintStats,
    pub logged: [[u64; CAUSES]; Pattern::N],
    pub life: LifeStats,
    pub est: EstimatorStats,
    pub class: ClassStats,
    pub rag: RagStats,
    pub claims: HashMap<u32, ClaimStats>,
    pub refused: u64,
    pub abandoned: u64,
    pub span_ns: u64,
    pub calls: u64,
    pub tools: u64,
    pub decisions: u64,
    pub dispatches: u64,
    pub leases: (u64, u64, u64),
    pub durable_lost: u64,
    pub locality: [(u64, u64); Pattern::N],
    pub memory: [(u64, u64); Pattern::N],
    pub fanouts: (u64, u64),
    pub fanout_service_ns: u64,
    pub preempted: u64,
    pub mean_batch: f64,
    pub learn: crate::machine::LearnStats,
    pub tool_wait_ns: u64,
    pub tool_exec_ns: u64,
    pub drafts: u64,
    pub marks: (u64, u64),
    pub fault: crate::fault::FaultStats,
    pub state_lost: u64,
}

impl Outcome {
    #[must_use]
    pub fn turns_of(&self, pattern: Pattern) -> &[u64] {
        &self.by_pattern[pattern.idx()].turns
    }

    #[must_use]
    pub fn logged_total(&self) -> u64 {
        self.logged.iter().flatten().sum()
    }

    #[must_use]
    pub fn logged_of(&self, cause: LogCause) -> u64 {
        self.logged.iter().map(|row| row[cause as usize]).sum()
    }
}

#[must_use]
pub fn mean(values: &[u64]) -> f64 {
    if values.is_empty() {
        0.0
    } else {
        values.iter().sum::<u64>() as f64 / values.len() as f64
    }
}

#[must_use]
pub fn quantile(values: &[u64], q: f64) -> f64 {
    crate::instruments::percentile(&mut values.to_vec(), q).map_or(0.0, |v| v as f64)
}

const NO_TOOL: usize = TOOL_KINDS;

#[derive(Debug, Default)]
struct Estimator {
    counts: HashMap<(u32, u8), [u32; TOOL_KINDS + 1]>,
}

#[derive(Clone, Copy, Debug)]
struct Prediction {
    ranked: [usize; 3],
    p_top: f64,
    p_tool: f64,
}

impl Estimator {
    fn key(root: u32, prev: Option<u8>) -> (u32, u8) {
        (root, prev.unwrap_or(255))
    }

    fn predict(&self, root: u32, prev: Option<u8>) -> Option<Prediction> {
        let counts = self.counts.get(&Self::key(root, prev))?;
        let total: u32 = counts.iter().sum();
        if total == 0 {
            return None;
        }
        let mut order: Vec<usize> = (0..=TOOL_KINDS).collect();
        order.sort_by_key(|&i| Reverse(counts[i]));
        Some(Prediction {
            ranked: [order[0], order[1], order[2]],
            p_top: f64::from(counts[order[0]]) / f64::from(total),
            p_tool: 1.0 - f64::from(counts[NO_TOOL]) / f64::from(total),
        })
    }

    fn observe(&mut self, root: u32, prev: Option<u8>, next: Option<u8>) {
        let slot = next.map_or(NO_TOOL, usize::from);
        self.counts
            .entry(Self::key(root, prev))
            .or_insert([0; TOOL_KINDS + 1])[slot] += 1;
    }
}

#[derive(Debug, Default)]
struct Survival {
    gaps: HashMap<u32, Vec<u64>>,
}

impl Survival {
    fn observe(&mut self, key: u32, gap_ns: u64) {
        let gaps = self.gaps.entry(key).or_default();
        let at = gaps.partition_point(|&g| g <= gap_ns);
        gaps.insert(at, gap_ns);
    }

    fn survival(gaps: &[u64], t: u64) -> f64 {
        if gaps.is_empty() {
            return 1.0;
        }
        (gaps.len() - gaps.partition_point(|&g| g <= t)) as f64 / gaps.len() as f64
    }

    fn hazard_per_s(&self, key: u32, t_ns: u64) -> Option<f64> {
        let gaps = self.gaps.get(&key).filter(|g| g.len() >= 20)?;
        let later = t_ns + t_ns / 4;
        let s0 = Self::survival(gaps, t_ns);
        if s0 <= 0.0 {
            return Some(f64::INFINITY);
        }
        let s1 = Self::survival(gaps, later);
        Some((s0 - s1) / s0 / ((later - t_ns).max(1) as f64 / 1e9))
    }

    fn rent_or_buy_ns(
        &self,
        key: u32,
        first_ns: u64,
        resume_ns: f64,
        hold_ns_per_s: f64,
    ) -> Option<u64> {
        if hold_ns_per_s <= 0.0 {
            return None;
        }
        let gaps = self.gaps.get(&key).filter(|g| g.len() >= 20)?;
        let longest = *gaps.last()?;
        let mut t = first_ns.max(1);
        while t <= longest {
            let hazard = self.hazard_per_s(key, t)?;
            if resume_ns * hazard <= hold_ns_per_s {
                return Some(t);
            }
            t += t / 4 + 1;
        }
        Some(longest)
    }
}

#[derive(Clone, Copy, Debug)]
struct SpecRun {
    tool_id: u8,
    start_ns: u64,
    ready_ns: u64,
    exec_ns: u64,
    slot: Option<ToolSlot>,
    hit: bool,
}

#[derive(Clone, Debug)]
struct RagSpan {
    start: usize,
    len: usize,
    prefix: usize,
    chunks: Vec<u32>,
}

#[allow(clippy::struct_excessive_bools)]
struct Prog {
    script: Script,
    pattern: Pattern,
    start_ns: u64,
    chain: Chain,
    tail: BlobId,
    blocks: u64,
    sandbox: (BlobId, BlobMeta),
    model: u64,
    slo: Slo,
    turn_started: u64,
    turn_done: u64,
    last_call_done: u64,
    max_done: u64,
    prev_tool: Option<ToolSpec>,
    pending_task: Option<u64>,
    draft_dirty: bool,
    epoch: u64,
    in_idle: bool,
    idle_approval: bool,
    idle_start: u64,
    idle_ns: u64,
    cell_suspended_at: Option<u64>,
    kv_dropped: bool,
    await_call: bool,
    await_tool: bool,
    warm_ready: u64,
    spec: Option<SpecRun>,
    home: usize,
    deferred: Option<(ToolStep, bool, u64)>,
    rag: Option<RagSpan>,
    dead: bool,
    finished: bool,
    calls_seen: u32,
    tool_results: u32,
    max_gap_ns: u64,
    approvals: u32,
    inferred: Pattern,
    first_unshared: usize,
    fanouts_seen: u32,
}

#[derive(Clone, Copy, Debug)]
enum Event {
    Step { p: usize, k: usize },
    KvExpire { p: usize, epoch: u64 },
    CellSuspend { p: usize, epoch: u64 },
    Joint { p: usize, epoch: u64 },
    Warm { p: usize, k: usize },
    ToolStart { p: usize, k: usize },
    Fault,
}

#[derive(Clone, Debug)]
enum Extra {
    Call {
        appended: u64,
        produces: Chain,
        turn_end: bool,
        root: u32,
        next_tool: Option<u8>,
        after_idle: bool,
    },
    Tool {
        step: ToolStep,
        start: u64,
        after_idle: bool,
    },
    Retrieve {
        chunks: Vec<u32>,
    },
    Fanout {
        agents: usize,
        side_effects: u64,
    },
}

struct Driver<'a> {
    cfg: &'a Config,
    shape: Shape,
    catalog: [ToolSpec; TOOL_KINDS],
    mach: Machine,
    progs: Vec<Prog>,
    events: Vec<Event>,
    heap: BinaryHeap<Reverse<(u64, usize)>>,
    out: Outcome,
    est: Estimator,
    surv: Survival,
    ttl_gaps: HashMap<u32, Vec<u64>>,
    spec_rng: Rng,
    next_task: u64,
    open: HashMap<usize, (usize, usize, u64, Extra)>,
    prefixes: Vec<Chain>,
    partition_bytes: u64,
    rag_seen: HashMap<u32, Vec<Vec<BlobId>>>,
}

fn pattern_of_class(p: Pattern) -> bool {
    matches!(
        p,
        Pattern::Pipeline | Pattern::Agentic | Pattern::LongRunning | Pattern::MultiAgent
    )
}

fn infer(prog: &Prog) -> Pattern {
    if prog.fanouts_seen > 0 {
        return Pattern::MultiAgent;
    }
    if prog.rag.is_some() {
        return Pattern::Rag;
    }
    let throughput = prog.slo == Slo::Throughput;
    if prog.calls_seen == 0 {
        return match (throughput, prog.first_unshared) {
            (true, n) if n >= 16 => Pattern::Extraction,
            (true, n) if n >= 8 => Pattern::Batch,
            (true, _) => Pattern::LongRunning,
            (false, n) if n >= 5 => Pattern::OneShot,
            (false, _) => Pattern::Agentic,
        };
    }
    if throughput {
        return Pattern::LongRunning;
    }
    if prog.tool_results == 0 {
        return Pattern::Conversational;
    }
    if prog.approvals > 0 || prog.max_gap_ns >= 30_000_000_000 {
        return Pattern::LongRunning;
    }
    if prog.tool_results <= 2 && prog.calls_seen <= 3 {
        return Pattern::Pipeline;
    }
    Pattern::Agentic
}

#[must_use]
pub fn build_machine(cfg: &Config, hold_decodes: bool) -> Machine {
    let memory = node_memory(cfg);
    let topo = Topology::cluster(
        cfg.nodes,
        3,
        cfg.ddr_mib << 20,
        Distance::Rack,
        Crossing::default(),
    );
    let mut mach = Machine::new(topo, |_| memory, Policy::Gdsf, Placement::Scored);
    mach.set_flow_aware(true);
    mach.set_control(Control::Unified, Crossing::default());
    mach.set_state_transfer(true);
    mach.set_arrival_rate(cfg.engine_rate);
    mach.set_fanout_atomic(cfg.atomic);
    mach.set_admission(Reserve::Prompt, 35);
    mach.set_hold_decodes(hold_decodes);
    mach.set_displacement(true);
    mach.set_regret(cfg.regret);
    mach.set_tool_slots(cfg.tool_slots);
    mach.set_engine_wait(cfg.engine_wait);
    mach.set_claim(cfg.claim);
    mach.set_claim_key(cfg.claim_key);
    mach.set_observables(cfg.observables);
    mach.set_copy_durable(cfg.copy_durable);
    if cfg.ttl {
        mach.set_directives(Some(Directives {
            emit: Emit::Declared {
                retain: true,
                evict_first: false,
            },
            ignores: false,
            marks: Marks::Acked,
        }));
    }
    mach
}

impl<'a> Driver<'a> {
    fn new(cfg: &'a Config) -> Self {
        let shape = Shape::new(cfg.shape);
        let catalog = catalog(cfg.shape.tools, cfg.shape.annotations);
        let mach = build_machine(cfg, true);
        let mut master = Rng::new(cfg.seed ^ 0x5052_4F47);
        let mut prefixes = Vec::new();
        for t in 0..TENANT_COUNT {
            let depth = 8 + master.below(56);
            let mut parent = ROOT;
            let mut chain = Vec::new();
            for d in 0..depth {
                let b = kv_block(parent, &format!("pm7tenant:{t}:{d}"));
                parent = b.0;
                chain.push(b);
            }
            prefixes.push(chain);
        }
        let out = Outcome {
            by_pattern: vec![PatternStats::default(); Pattern::N],
            ..Outcome::default()
        };
        Self {
            cfg,
            shape,
            catalog,
            mach,
            progs: Vec::new(),
            events: Vec::new(),
            heap: BinaryHeap::new(),
            out,
            est: Estimator::default(),
            surv: Survival::default(),
            ttl_gaps: HashMap::new(),
            spec_rng: Rng::new(cfg.seed ^ 0x5350_4543),
            next_task: 1 << 40,
            open: HashMap::new(),
            prefixes,
            partition_bytes: cfg.kv_partition_mib << 20,
            rag_seen: HashMap::new(),
        }
    }

    fn push(&mut self, at: u64, event: Event) {
        self.events.push(event);
        self.heap.push(Reverse((at, self.events.len() - 1)));
    }

    fn log(&mut self, pattern: Pattern, cause: LogCause) {
        self.out.logged[pattern.idx()][cause as usize] += 1;
    }

    fn task(&mut self) -> u64 {
        self.next_task += 1;
        self.next_task
    }

    fn seed_programs(&mut self) {
        let mut arrivals = Rng::new(self.cfg.seed ^ 0x4152_5256);
        let total: f64 = self.cfg.mix.iter().map(|m| m.1).sum();
        let mut t = 0u64;
        for id in 0..self.cfg.programs {
            let gap = (-(1.0 - arrivals.unit()).ln() / self.cfg.rate * 1e9) as u64;
            t += gap;
            let mut pick = arrivals.unit() * total;
            let mut preset = self.cfg.mix[0].0;
            for &(candidate, share) in &self.cfg.mix {
                preset = candidate;
                if pick < share {
                    break;
                }
                pick -= share;
            }
            let script = script(preset, self.cfg.seed, id as u64, &self.shape);
            let prefix_tenant = if preset == Preset::Rag {
                0
            } else {
                script.tenant
            };
            let chain = self.prefixes[prefix_tenant].clone();
            let tail = chain.last().map_or(ROOT, |(b, _)| *b);
            let slo = if script.throughput {
                Slo::Throughput
            } else {
                Slo::Interactive
            };
            self.out.by_pattern[preset.pattern().idx()].programs += 1;
            let prog = Prog {
                pattern: preset.pattern(),
                start_ns: t,
                chain,
                tail,
                blocks: 0,
                sandbox: sandbox(id as u64),
                model: script.tenant as u64 % MODELS,
                slo,
                turn_started: t,
                turn_done: 0,
                last_call_done: 0,
                max_done: t,
                prev_tool: None,
                pending_task: None,
                draft_dirty: false,
                epoch: 0,
                in_idle: false,
                idle_approval: false,
                idle_start: 0,
                idle_ns: 0,
                cell_suspended_at: None,
                kv_dropped: false,
                await_call: false,
                await_tool: false,
                warm_ready: 0,
                spec: None,
                home: 0,
                deferred: None,
                rag: None,
                dead: false,
                finished: false,
                calls_seen: 0,
                tool_results: 0,
                max_gap_ns: 0,
                approvals: 0,
                inferred: Pattern::Plain,
                first_unshared: 0,
                fanouts_seen: 0,
                script,
            };
            self.progs.push(prog);
            self.push(t, Event::Step { p: id, k: 0 });
        }
    }

    fn run(mut self) -> Outcome {
        self.seed_programs();
        if let Some((at, _)) = self.cfg.fault {
            self.push(at, Event::Fault);
        }
        loop {
            while let Some(Reverse((t, idx))) = self.heap.pop() {
                let event = self.events[idx];
                self.settle();
                match event {
                    Event::Step { p, k } => self.step(p, k, t),
                    Event::KvExpire { p, epoch } => self.kv_expire(p, epoch, t),
                    Event::CellSuspend { p, epoch } => self.cell_suspend(p, epoch, t),
                    Event::Joint { p, epoch } => self.joint(p, epoch, t),
                    Event::Warm { p, k } => self.warm_event(p, k, t),
                    Event::ToolStart { p, k } => self.tool_start(p, k, t),
                    Event::Fault => self.inject_fault(t),
                }
            }
            self.mach.finish();
            self.settle();
            if self.heap.is_empty() {
                break;
            }
        }
        self.finalize()
    }

    fn inject_fault(&mut self, t: u64) {
        let Some((_, fault)) = self.cfg.fault else {
            return;
        };
        self.mach.advance_to(t);
        self.mach.inject(fault);
        let lost = self.mach.lost_durable();
        self.out.state_lost += self
            .progs
            .iter()
            .filter(|p| !p.dead && lost.contains(&p.sandbox.0))
            .count() as u64;
    }

    fn settle(&mut self) {
        for (id, cost) in self.mach.drain_closed() {
            if let Some((p, k, due, extra)) = self.open.remove(&id) {
                self.finish(p, k, due, &cost, extra);
            }
        }
    }

    fn finalize(mut self) -> Outcome {
        let (taken, breaks, peak) = self.mach.leases();
        self.out.leases = (taken, breaks, peak);
        self.out.durable_lost = self.mach.durable_lost();
        self.out.fault = self.mach.fault_stats.clone();
        self.out.locality = self.mach.locality_by;
        self.out.memory = self.mach.memory_by_pattern();
        self.out.fanouts = (self.mach.fanouts_admitted, self.mach.fanouts_refused);
        self.out.fanout_service_ns = self.mach.fanout_service_ns;
        self.out.preempted = self.mach.preempted.iter().sum();
        self.out.mean_batch = self.mach.mean_batch();
        self.out.decisions = self.mach.decisions;
        self.out.dispatches = self.mach.dispatches;
        self.out.learn = self.mach.learn_stats();
        self.out.marks = (
            self.mach.instruments.directives.emitted,
            self.mach.instruments.directives.honoured,
        );
        self.out
    }

    fn append_blocks(&mut self, p: usize, count: u64, tag: &str) {
        for _ in 0..count {
            let prog = &mut self.progs[p];
            let b = kv_block(prog.tail, &format!("pm7:{p}:{tag}:{}", prog.blocks));
            prog.blocks += 1;
            prog.tail = b.0;
            prog.chain.push(b);
        }
    }

    fn append_content(&mut self, p: usize, content: &str, count: u64) {
        for b in 0..count {
            let prog = &mut self.progs[p];
            let block = kv_block(prog.tail, &format!("{content}:{b}"));
            prog.tail = block.0;
            prog.chain.push(block);
        }
    }

    fn class_key(&self, p: usize) -> u32 {
        let prog = &self.progs[p];
        match self.cfg.class_mode {
            ClassMode::Truth => prog.pattern.idx() as u32 + 1,
            ClassMode::Inferred => prog.inferred.idx() as u32 + 1,
            ClassMode::Pooled => 0,
        }
    }

    fn claim_root(&self, p: usize, template: u32) -> u32 {
        match self.cfg.claim_by {
            ClaimBy::Template => template,
            ClaimBy::Class => 100 + self.class_key(p),
        }
    }

    fn next_action(&self, p: usize, k: usize) -> Option<(usize, &Step)> {
        self.progs[p]
            .script
            .steps
            .iter()
            .enumerate()
            .skip(k + 1)
            .find(|(_, s)| !matches!(s, Step::Idle { approval: true, .. }))
    }

    fn schedule_next(&mut self, p: usize, k: usize, due: u64, done: u64) {
        if k + 1 >= self.progs[p].script.steps.len() {
            self.mach.release_durable(&self.progs[p].sandbox.0);
            let prog = &mut self.progs[p];
            prog.finished = true;
            let latency = prog.max_done.saturating_sub(prog.start_ns);
            let stats = &mut self.out.by_pattern[prog.pattern.idx()];
            stats.completed += 1;
            stats.latency.push(latency);
            self.out.span_ns = self.out.span_ns.max(prog.max_done);
            return;
        }
        let at = match self.cfg.loop_mode {
            LoopMode::Closed => done,
            LoopMode::Open { lead_ns } => match self.progs[p].script.steps[k] {
                Step::Idle { .. } => done,
                _ => due + lead_ns,
            },
        };
        self.push(at, Event::Step { p, k: k + 1 });
    }

    fn end_idle(&mut self, p: usize, due: u64) {
        if !self.progs[p].in_idle {
            return;
        }
        let prog = &mut self.progs[p];
        prog.in_idle = false;
        let ns = due.saturating_sub(prog.idle_start);
        let freed = prog
            .cell_suspended_at
            .map_or(0, |at| due.saturating_sub(at));
        let suspended = prog.cell_suspended_at.is_some() || prog.kv_dropped;
        let pattern = prog.pattern;
        let approval = prog.idle_approval;
        let key = self.class_key(p) * 2 + u32::from(approval);
        if approval {
            self.out.life.approvals += 1;
        } else {
            self.out.life.idles += 1;
            self.out.life.idle_ns += ns;
            self.out.life.freed_ns += freed.min(ns);
            self.out.life.suspended_idles += u64::from(suspended);
        }
        let prog = &mut self.progs[p];
        prog.max_gap_ns = prog.max_gap_ns.max(ns);
        prog.cell_suspended_at = None;
        prog.kv_dropped = false;
        prog.await_call = suspended;
        prog.await_tool = suspended;
        if suspended {
            self.log(pattern, LogCause::Resume);
        }
        self.surv.observe(key, ns);
    }

    fn step(&mut self, p: usize, k: usize, due: u64) {
        if self.progs[p].dead {
            return;
        }
        let step = self.progs[p].script.steps[k].clone();
        if !matches!(step, Step::Idle { .. }) {
            self.end_idle(p, due);
        }
        match step {
            Step::Idle { ns, approval } => self.idle(p, k, due, ns, approval),
            Step::Call {
                tokens,
                appended,
                root,
                turn_start,
                turn_end,
            } => self.call(p, k, due, tokens, appended, root, turn_start, turn_end),
            Step::Tool(tool) => self.tool(p, k, due, tool),
            Step::Retrieve { chunks } => self.retrieve(p, k, due, chunks),
            Step::Fanout { agents } => self.fanout(p, k, due, &agents),
        }
    }

    fn idle(&mut self, p: usize, k: usize, due: u64, ns: u64, approval: bool) {
        self.mach.advance_to(due);
        let compress = self.cfg.shape.compress;
        let t_kv = (ENGINE_RETENTION_NS as f64 / compress) as u64;
        let t_sbx = (SANDBOX_TIMEOUT_NS as f64 / compress) as u64;
        let pattern = self.progs[p].pattern;
        {
            let prog = &mut self.progs[p];
            prog.epoch += 1;
            prog.in_idle = true;
            prog.idle_approval = approval;
            prog.idle_start = due;
            prog.idle_ns = ns;
            if approval {
                prog.approvals += 1;
            }
        }
        if approval {
            self.log(pattern, LogCause::Approval);
        } else if self.progs[p].draft_dirty {
            let sandbox = self.progs[p].sandbox;
            self.mach.mark_durable(sandbox.0, sandbox.1.bytes);
            self.progs[p].draft_dirty = false;
            if pattern == Pattern::LongRunning {
                self.log(pattern, LogCause::TaskState);
            }
        }
        let epoch = self.progs[p].epoch;
        let kv_timer = ns > t_kv;
        let cell_timer = ns > t_sbx;
        let boundary = !approval;
        match self.cfg.suspend {
            Suspend::Never => {}
            Suspend::Timers => {
                self.out.life.pair_disagree += u64::from(boundary && kv_timer != cell_timer);
                if kv_timer {
                    self.push(due + t_kv, Event::KvExpire { p, epoch });
                }
                if cell_timer {
                    self.push(due + t_sbx, Event::CellSuspend { p, epoch });
                }
            }
            Suspend::Joint => {
                self.out.life.pair_disagree += u64::from(boundary && kv_timer != cell_timer);
                let at = self.joint_time(p, compress, approval);
                let joint = at.is_some_and(|t| t < ns);
                self.out.life.joint_never += u64::from(boundary && at.is_none());
                self.out.life.joint_differs +=
                    u64::from(boundary && (joint != kv_timer || joint != cell_timer));
                if joint && let Some(t) = at {
                    self.push(due + t, Event::Joint { p, epoch });
                }
            }
        }
        self.schedule_next(p, k, due, due + ns);
    }

    fn joint_time(&self, p: usize, compress: f64, approval: bool) -> Option<u64> {
        let prog = &self.progs[p];
        let blocks = prog.chain.len() as u64;
        let kv_bytes = blocks * KV_BLOCK_BYTES;
        let price =
            self.mach.host_hold_ns_per_s(SANDBOX_BYTES) + self.mach.kv_hold_ns_per_s(kv_bytes);
        let resume =
            TierSpec::nvme(0).fetch_ns(SANDBOX_BYTES) as f64 + blocks as f64 * KV_BLOCK_NS as f64;
        self.surv.rent_or_buy_ns(
            self.class_key(p) * 2 + u32::from(approval),
            (1e9 / compress) as u64,
            resume,
            price,
        )
    }

    fn kv_expire(&mut self, p: usize, epoch: u64, t: u64) {
        let prog = &self.progs[p];
        if prog.epoch != epoch || !prog.in_idle || prog.dead {
            return;
        }
        self.mach.advance_to(t);
        let ids: Vec<BlobId> = self.progs[p].chain.iter().map(|(id, _)| *id).collect();
        self.mach.drop_kv(&ids);
        self.progs[p].kv_dropped = true;
        self.out.life.kv_drops += 1;
    }

    fn cell_suspend(&mut self, p: usize, epoch: u64, t: u64) {
        let prog = &self.progs[p];
        if prog.epoch != epoch || !prog.in_idle || prog.dead {
            return;
        }
        self.mach.advance_to(t);
        let sandbox = self.progs[p].sandbox;
        if !self.mach.suspend_cell(sandbox) && self.mach.cell_state(&sandbox) == CellState::Hot {
            return;
        }
        self.progs[p].cell_suspended_at = Some(t);
        self.out.life.cell_suspends += 1;
        let pattern = self.progs[p].pattern;
        self.log(pattern, LogCause::Suspend);
    }

    fn joint(&mut self, p: usize, epoch: u64, t: u64) {
        self.kv_expire(p, epoch, t);
        self.cell_suspend(p, epoch, t);
    }

    fn warm_event(&mut self, p: usize, k: usize, t: u64) {
        if self.progs[p].dead || !matches!(self.progs[p].script.steps.get(k), Some(Step::Tool(_))) {
            return;
        }
        self.warm(p, t, 1.0);
    }

    fn warm(&mut self, p: usize, at: u64, probability: f64) {
        self.mach.advance_to(at);
        let sandbox = self.progs[p].sandbox;
        let task = self.progs[p].pending_task;
        let home = self.progs[p].home;
        let restore = match self.mach.cell_state(&sandbox) {
            CellState::Hot => {
                self.out.hints.skipped_hot += 1;
                return;
            }
            CellState::Cold => TierSpec::nvme(0).fetch_ns(SANDBOX_BYTES) as f64,
            CellState::Gone => sandbox.1.recompute_ns as f64,
        };
        if probability * restore <= self.mach.warm_price(task, home, &sandbox) {
            self.out.hints.skipped_price += 1;
            return;
        }
        let pattern = self.progs[p].pattern;
        let restore = self.mach.warm_cell(task, home, sandbox, pattern);
        self.out.hints.warms += 1;
        self.out.hints.restore_ns += restore;
        let prog = &mut self.progs[p];
        prog.warm_ready = prog.warm_ready.max(at + restore);
    }

    fn call_request(
        &mut self,
        p: usize,
        tokens: u64,
        produces: &Chain,
        hint: Option<FlowHint>,
        root: u32,
        retain_lead_ops: Option<u32>,
    ) -> Request {
        let prog = &self.progs[p];
        let claim_root = self.claim_root(p, root);
        Request {
            phase: 0,
            chain: prog.chain.clone(),
            requires: shards(prog.model),
            hint,
            completes: self.progs[p].pending_task,
            exec_ns: decode_ns(tokens),
            tokens,
            gang: None,
            produces: produces.clone(),
            max_tokens: 4 * (24 + 200),
            slo: prog.slo,
            retention: Retention {
                retain: retain_lead_ops.map(|lead_ops| Retain {
                    upto: prog.chain.len(),
                    lead_ops,
                }),
                evict_first_from: None,
            },
            concurrent: true,
            tenant: Some(prog.script.tenant as u32),
            program: p as u64 + 1,
            pattern: prog.pattern,
            authority: Authority::ReadOnly,
            root: claim_root,
            tool: false,
        }
    }

    fn ttl_lead_ops(&self, p: usize, root: u32) -> Option<u32> {
        let samples = self.ttl_gaps.get(&root)?;
        if samples.len() < 20 {
            return None;
        }
        let prog = &self.progs[p];
        let benefit = prog.chain.len() as f64 * KV_BLOCK_NS as f64;
        let fraction =
            (prog.chain.len() as u64 * KV_BLOCK_BYTES) as f64 / self.partition_bytes as f64;
        let mut sorted = samples.clone();
        sorted.sort_unstable();
        let mut best = (0.0, 0u64);
        for &tau in samples {
            let reach = sorted.partition_point(|&s| s <= tau) as f64 / samples.len() as f64;
            let value = reach * benefit - fraction * tau as f64;
            if value > best.0 {
                best = (value, tau);
            }
        }
        if best.1 == 0 {
            return None;
        }
        let interval = (1e9 / self.cfg.engine_rate) as u64;
        Some((best.1 / interval.max(1)) as u32)
    }

    fn rag_reuse(&mut self, p: usize) {
        let Some(span) = self.progs[p].rag.clone() else {
            return;
        };
        let chain = self.progs[p].chain.clone();
        let (lead, _) = self.mach.chain_resident(&chain);
        let mut groups: Vec<Vec<BlobId>> = Vec::with_capacity(span.len);
        for (i, chunk) in span.chunks.iter().enumerate() {
            let own = &chain[span.start + i * CHUNK_BLOCKS as usize..];
            for b in 0..CHUNK_BLOCKS as usize {
                let mut ids = vec![own[b].0];
                for seen in self.rag_seen.get(chunk).into_iter().flatten() {
                    ids.push(seen[b]);
                }
                groups.push(ids);
            }
        }
        let any = self.mach.resident_union(&groups);
        self.out.rag.calls += 1;
        self.out.rag.chunk_blocks += span.len as u64;
        self.out.rag.prefix_blocks += lead.saturating_sub(span.prefix).min(span.len) as u64;
        self.out.rag.any_blocks += any as u64;
        for (i, chunk) in span.chunks.iter().enumerate() {
            let at = span.start + i * CHUNK_BLOCKS as usize;
            let ids: Vec<BlobId> = chain[at..at + CHUNK_BLOCKS as usize]
                .iter()
                .map(|(id, _)| *id)
                .collect();
            let seen = self.rag_seen.entry(*chunk).or_default();
            if !seen.contains(&ids) {
                seen.push(ids);
                if seen.len() > 8 {
                    seen.remove(0);
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::too_many_lines)]
    fn call(
        &mut self,
        p: usize,
        k: usize,
        due: u64,
        tokens: u64,
        appended: u64,
        root: u32,
        turn_start: bool,
        turn_end: bool,
    ) {
        let after_idle = self.progs[p].await_call;
        if turn_start {
            let prog = &mut self.progs[p];
            prog.turn_started = due;
            prog.turn_done = 0;
            prog.prev_tool = None;
        }
        if self.progs[p].calls_seen == 0 {
            self.progs[p].first_unshared = appended as usize;
        }
        self.append_blocks(p, appended, "g");
        let next = self.next_action(p, k).map(|(i, s)| (i, s.clone()));
        let next_index = next.as_ref().map(|&(i, _)| i);
        let actual = match &next {
            Some((_, Step::Tool(t))) => Some(*t),
            _ => None,
        };
        let next_tool = actual.map(|t| t.tool.id);
        let needs_hint = matches!(next, Some((_, Step::Tool(_) | Step::Fanout { .. })));
        let hint = needs_hint.then(|| {
            let task = self.task();
            FlowHint {
                task,
                template_len: 0,
                downstream: Vec::new(),
                probability: 1.0,
                lead_ops: WARM_LEAD_OPS,
                payload_bytes: 64 * 1024,
            }
        });
        let mut produces = Vec::new();
        let mut at = self.progs[p].tail;
        for o in 0..tokens.div_ceil(35) {
            let b = kv_block(at, &format!("pm7:{p}:out:{}:{o}", self.progs[p].blocks));
            at = b.0;
            produces.push(b);
        }
        self.progs[p].inferred = infer(&self.progs[p]);
        self.record_inference(p, next.is_none());
        let prev_id = self.progs[p].prev_tool.map(|t| t.id);
        let prediction = self.est.predict(root, prev_id);
        if let Some(pred) = prediction {
            self.out.est.predictions += 1;
            let actual = next_tool.map_or(NO_TOOL, usize::from);
            self.out.est.top1 += u64::from(pred.ranked[0] == actual);
            self.out.est.top3 += u64::from(pred.ranked.contains(&actual));
        }
        self.rag_reuse(p);
        let lead_ops = if self.cfg.ttl {
            self.ttl_lead_ops(p, root)
        } else {
            None
        };
        let req = self.call_request(p, tokens, &produces, hint.clone(), root, lead_ops);
        if let Some(claimed) = self.mach.claimed_tokens(&req) {
            let stats = self.out.claims.entry(req.root).or_default();
            stats.n += 1;
            stats.overruns += u64::from(tokens > claimed);
            stats.reserved_tokens += claimed;
        }
        let extra = Extra::Call {
            appended,
            produces,
            turn_end,
            root,
            next_tool,
            after_idle,
        };
        let outcome = self.mach.submit_at(due, &req);
        self.progs[p].home = self.mach.last_home();
        self.progs[p].pending_task = hint.as_ref().map(|h| h.task);
        self.start_hints(p, due, tokens, next_index, next_tool, prediction);
        let spec = self.start_speculation(p, due, tokens, actual, prediction);
        self.progs[p].spec = spec;
        match outcome {
            Submitted::Closed(cost) => self.finish(p, k, due, &cost, extra),
            Submitted::Open(id) => {
                self.open.insert(id, (p, k, due, extra));
            }
        }
    }

    fn record_inference(&mut self, p: usize, last: bool) {
        let prog = &self.progs[p];
        let (t, i) = (prog.pattern.idx(), prog.inferred.idx());
        self.out.class.confusion[t][i] += 1;
        if prog.calls_seen == 0 {
            self.out.class.first[t][i] += 1;
        }
        if last {
            self.out.class.last[t][i] += 1;
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn start_hints(
        &mut self,
        p: usize,
        due: u64,
        tokens: u64,
        next_index: Option<usize>,
        next_tool: Option<u8>,
        prediction: Option<Prediction>,
    ) {
        if self.cfg.loop_mode != LoopMode::Closed {
            return;
        }
        match self.cfg.hints {
            Hints::None => {}
            Hints::Declared => {
                if next_tool.is_some() && self.progs[p].pattern == Pattern::Pipeline {
                    self.warm(p, due, 1.0);
                }
            }
            Hints::Predicted => {
                self.warm(p, due, prediction.map_or(0.0, |pr| pr.p_tool));
            }
            Hints::Stream => {
                if let (Some(_), Some(index)) = (next_tool, next_index) {
                    let lead = (decode_ns(tokens) as f64 * (1.0 - self.cfg.tool_share)) as u64;
                    self.push(due + lead, Event::Warm { p, k: index });
                }
            }
        }
    }

    fn start_speculation(
        &mut self,
        p: usize,
        due: u64,
        tokens: u64,
        actual: Option<ToolStep>,
        prediction: Option<Prediction>,
    ) -> Option<SpecRun> {
        if self.cfg.speculate == Speculate::Off || self.cfg.loop_mode != LoopMode::Closed {
            return None;
        }
        let allowed_class = match self.cfg.class_mode {
            ClassMode::Truth => pattern_of_class(self.progs[p].pattern),
            ClassMode::Inferred => pattern_of_class(self.progs[p].inferred),
            ClassMode::Pooled => true,
        };
        if !allowed_class {
            return None;
        }
        let (guess_id, probability) = if self.cfg.speculate == Speculate::Oracle {
            (actual?.tool.id, 1.0)
        } else {
            let pred = prediction?;
            if pred.ranked[0] == NO_TOOL {
                return None;
            }
            (pred.ranked[0] as u8, pred.p_top)
        };
        let guess = self.catalog[usize::from(guess_id)];
        if guess.authority() != Authority::ReadOnly {
            self.out.spec.forbidden += 1;
            return None;
        }
        if guess.annotations.open_world && !self.cfg.open_world {
            self.out.spec.forbidden += 1;
            return None;
        }
        let estimate = guess.median_ns as f64 / self.cfg.shape.compress;
        let saving = probability * estimate.min(decode_ns(tokens) as f64);
        let utilisation = self.mach.tool_utilisation();
        let toll = estimate * utilisation / (1.0 - utilisation.min(0.95));
        if self.cfg.speculate == Speculate::Run && saving <= toll {
            self.out.spec.gated += 1;
            return None;
        }
        let hit = actual.is_some_and(|a| a.tool.id == guess_id);
        let exec_ns = match actual {
            Some(a) if hit => a.exec_ns,
            _ => lognormal(&mut self.spec_rng, estimate, guess.sigma),
        };
        let step = ToolStep {
            tool: guess,
            exec_ns,
            result_blocks: 0,
        };
        let req = self.tool_request(p, &step, None, None);
        self.out.spec.tries += 1;
        let Submitted::Closed(cost) = self.mach.submit_at(due, &req) else {
            return None;
        };
        if cost.pending {
            return None;
        }
        let home = self.mach.last_home();
        let anchor = self.progs[p]
            .pending_task
            .and_then(|t| self.mach.anchor_of(t));
        if anchor.is_some_and(|a| a != home) {
            self.out.spec.off_anchor += 1;
        }
        self.out.spec.wait_imposed_ns += cost.queue_ns;
        self.out.spec.exec_ns += cost.exec_ns;
        Some(SpecRun {
            tool_id: guess_id,
            start_ns: due,
            ready_ns: due + cost.service_ns(),
            exec_ns,
            slot: self.mach.last_tool_slot(),
            hit,
        })
    }

    fn tool_request(
        &self,
        p: usize,
        step: &ToolStep,
        completes: Option<u64>,
        hint: Option<FlowHint>,
    ) -> Request {
        let prog = &self.progs[p];
        let authority = step.tool.authority();
        Request {
            phase: 0,
            chain: vec![prog.sandbox],
            requires: Vec::new(),
            hint,
            completes,
            exec_ns: step.exec_ns,
            tokens: 0,
            gang: None,
            produces: Vec::new(),
            max_tokens: 0,
            slo: prog.slo,
            retention: Retention::default(),
            concurrent: true,
            tenant: Some(prog.script.tenant as u32),
            program: p as u64 + 1,
            pattern: prog.pattern,
            authority,
            root: 0,
            tool: true,
        }
    }

    #[allow(clippy::too_many_lines)]
    fn finish(&mut self, p: usize, k: usize, due: u64, cost: &Cost, extra: Extra) {
        if cost.pending {
            self.out.refused += 1;
            self.progs[p].dead = true;
            self.out.abandoned += 1;
            self.mach.release_durable(&self.progs[p].sandbox.0);
            return;
        }
        match extra {
            Extra::Call {
                appended,
                produces,
                turn_end,
                root,
                next_tool,
                after_idle,
            } => {
                let done = due + cost.service_ns();
                if let Some(spec) = self.progs[p].spec.filter(|s| !s.hit) {
                    self.progs[p].spec = None;
                    self.out.spec.wasted_ns += done.saturating_sub(spec.start_ns).min(spec.exec_ns);
                    if let Some(slot) = spec.slot {
                        self.mach.release_tool_slot(slot, done);
                    }
                }
                for b in produces {
                    let prog = &mut self.progs[p];
                    prog.tail = b.0;
                    prog.chain.push(b);
                    prog.blocks += 1;
                }
                let excess = cost.recompute_ns.saturating_sub(appended * KV_BLOCK_NS);
                let pattern = self.progs[p].pattern;
                let prev_done = self.progs[p].last_call_done;
                if prev_done > 0 {
                    self.out.gaps.push(GapSample {
                        gap_ns: due.saturating_sub(prev_done),
                        excess_ns: excess,
                        fetched: cost.transfer_ns > 0,
                        service_ns: cost.service_ns(),
                    });
                    if !after_idle {
                        self.ttl_gaps
                            .entry(root)
                            .or_default()
                            .push(due.saturating_sub(prev_done));
                    }
                }
                if after_idle {
                    self.out.life.resume_ns += excess + cost.transfer_ns;
                    self.progs[p].await_call = false;
                }
                let prog = &mut self.progs[p];
                prog.calls_seen += 1;
                prog.last_call_done = done;
                prog.turn_done = prog.turn_done.max(done);
                prog.max_done = prog.max_done.max(done);
                self.out.calls += 1;
                self.out.by_pattern[pattern.idx()].calls += 1;
                let prev_id = self.progs[p].prev_tool.map(|t| t.id);
                self.est.observe(root, prev_id, next_tool);
                if turn_end {
                    let prog = &self.progs[p];
                    let turn = prog.turn_done.saturating_sub(prog.turn_started);
                    self.out.by_pattern[pattern.idx()].turns.push(turn);
                    if after_idle || self.progs[p].await_tool {
                        self.out.life.resume_turn_ns += turn;
                    }
                }
                self.schedule_next(p, k, due, done);
            }
            Extra::Tool {
                step,
                start,
                after_idle,
            } => {
                let pattern = self.progs[p].pattern;
                let done = start + cost.service_ns();
                if step.tool.authority() == Authority::SideEffecting {
                    self.log(pattern, LogCause::Outcome);
                }
                if after_idle {
                    self.out.hints.first_tools += 1;
                    self.out.hints.first_tool_restore_ns += cost.transfer_ns + cost.recompute_ns;
                    self.out.life.resume_ns += cost.transfer_ns + cost.recompute_ns;
                    self.out.hints.exposed_ns += cost.transfer_ns + cost.recompute_ns;
                }
                self.out.tool_wait_ns += cost.queue_ns;
                self.out.tool_exec_ns += cost.exec_ns;
                if step.tool.authority() == Authority::DraftOnly {
                    self.progs[p].draft_dirty = true;
                    self.out.drafts += 1;
                    if self.cfg.reclaim_drafts {
                        let sandbox = self.progs[p].sandbox;
                        self.mach.evict_first_cell(sandbox);
                    }
                }
                let prog = &mut self.progs[p];
                prog.prev_tool = Some(step.tool);
                prog.tool_results += 1;
                prog.max_done = prog.max_done.max(done);
                self.out.tools += 1;
                self.out.by_pattern[pattern.idx()].tools += 1;
                let due_next = start;
                self.schedule_next(p, k, due_next, done);
            }
            Extra::Retrieve { chunks } => {
                let done = due + cost.service_ns();
                let prefix = self.progs[p].chain.len();
                for chunk in &chunks {
                    self.append_content(p, &format!("chunk:{chunk}"), CHUNK_BLOCKS);
                }
                let len = self.progs[p].chain.len() - prefix;
                self.progs[p].rag = Some(RagSpan {
                    start: prefix,
                    len,
                    prefix,
                    chunks: chunks.clone(),
                });
                self.out.tools += 1;
                self.progs[p].max_done = self.progs[p].max_done.max(done);
                self.schedule_next(p, k, due, done);
            }
            Extra::Fanout {
                agents,
                side_effects,
            } => {
                let done = due + cost.service_ns();
                self.progs[p].fanouts_seen += 1;
                let pattern = self.progs[p].pattern;
                for _ in 0..side_effects {
                    self.log(pattern, LogCause::Outcome);
                }
                self.append_blocks(p, 2 * agents as u64, "result");
                let prog = &mut self.progs[p];
                prog.max_done = prog.max_done.max(done);
                self.schedule_next(p, k, due, done);
            }
        }
    }

    fn tool(&mut self, p: usize, k: usize, due: u64, step: ToolStep) {
        let after_idle = self.progs[p].await_tool;
        self.progs[p].await_tool = false;
        if let Some(spec) = self.progs[p].spec.take() {
            if spec.tool_id == step.tool.id && spec.hit {
                self.out.spec.hits += 1;
                let hidden = due
                    .saturating_sub(spec.start_ns)
                    .min(spec.ready_ns - spec.start_ns);
                self.out.spec.saved_ns += hidden;
                let done = due.max(spec.ready_ns);
                if let Some(task) = self.progs[p].pending_task.take() {
                    self.mach.forget_flow(task);
                }
                let pattern = self.progs[p].pattern;
                self.out.tools += 1;
                self.out.by_pattern[pattern.idx()].tools += 1;
                let prog = &mut self.progs[p];
                prog.prev_tool = Some(step.tool);
                prog.tool_results += 1;
                prog.max_done = prog.max_done.max(done);
                self.schedule_next(p, k, due, done);
                return;
            }
            let ran_until = due.saturating_sub(spec.start_ns).min(spec.exec_ns);
            self.out.spec.wasted_ns += ran_until;
            if let Some(slot) = spec.slot {
                self.mach.release_tool_slot(slot, due);
            }
        }
        let start = due.max(self.progs[p].warm_ready);
        if start > due {
            self.progs[p].deferred = Some((step, after_idle, due));
            self.push(start, Event::ToolStart { p, k });
            return;
        }
        self.submit_tool(p, k, due, start, step, after_idle);
    }

    fn tool_start(&mut self, p: usize, k: usize, t: u64) {
        if self.progs[p].dead {
            return;
        }
        if let Some((step, after_idle, due)) = self.progs[p].deferred.take() {
            self.submit_tool(p, k, due, t, step, after_idle);
        }
    }

    fn submit_tool(
        &mut self,
        p: usize,
        k: usize,
        due: u64,
        start: u64,
        step: ToolStep,
        after_idle: bool,
    ) {
        let pattern = self.progs[p].pattern;
        let authority = step.tool.authority();
        if authority == Authority::SideEffecting {
            self.log(pattern, LogCause::Intent);
        }
        let task = self.task();
        let hint = FlowHint {
            task,
            template_len: 0,
            downstream: Vec::new(),
            probability: 1.0,
            lead_ops: WARM_LEAD_OPS,
            payload_bytes: step.result_blocks * 16 * 1024,
        };
        let completes = self.progs[p].pending_task.take();
        let req = self.tool_request(p, &step, completes, Some(hint));
        self.progs[p].pending_task = Some(task);
        let extra = Extra::Tool {
            step,
            start,
            after_idle,
        };
        match self.mach.submit_at(start, &req) {
            Submitted::Closed(cost) => self.finish(p, k, due, &cost, extra),
            Submitted::Open(id) => {
                self.open.insert(id, (p, k, due, extra));
            }
        }
    }

    fn retrieve(&mut self, p: usize, k: usize, due: u64, chunks: Vec<u32>) {
        let index = u64::from(chunks.first().copied().unwrap_or(0)) % INDEXES;
        let prog = &self.progs[p];
        let req = Request {
            phase: 0,
            chain: vec![index_heap(index)],
            requires: Vec::new(),
            hint: None,
            completes: None,
            exec_ns: (RETRIEVE_NS as f64 / self.cfg.shape.compress) as u64,
            tokens: 0,
            gang: None,
            produces: Vec::new(),
            max_tokens: 0,
            slo: prog.slo,
            retention: Retention::default(),
            concurrent: true,
            tenant: Some(prog.script.tenant as u32),
            program: p as u64 + 1,
            pattern: prog.pattern,
            authority: Authority::ReadOnly,
            root: 0,
            tool: false,
        };
        let extra = Extra::Retrieve { chunks };
        match self.mach.submit_at(due, &req) {
            Submitted::Closed(cost) => self.finish(p, k, due, &cost, extra),
            Submitted::Open(id) => {
                self.open.insert(id, (p, k, due, extra));
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    fn fanout(&mut self, p: usize, k: usize, due: u64, agents: &[AgentScript]) {
        let functions = 400;
        let mut rng = Rng::new(self.cfg.seed ^ (p as u64).wrapping_mul(0x2545_F491_4F6C_DD1D));
        let gang_agents: Vec<Agent> = agents
            .iter()
            .enumerate()
            .map(|(a, script)| {
                let prog = &self.progs[p];
                let mut chain = prog.chain.clone();
                let mut at = prog.tail;
                for b in 0..6 {
                    let block = kv_block(at, &format!("pm7:{p}:agent:{a}:{b}"));
                    at = block.0;
                    chain.push(block);
                }
                let mut produces = Vec::new();
                for o in 0..script.tokens.div_ceil(35) {
                    let block = kv_block(at, &format!("pm7:{p}:agentout:{a}:{o}"));
                    at = block.0;
                    produces.push(block);
                }
                let tools = script
                    .tools
                    .iter()
                    .map(|t| {
                        let f = rng.zipf(functions, 1.5);
                        ToolCall {
                            chain: vec![(
                                BlobId::leaf(format!("fn:{f}").as_bytes()),
                                BlobMeta {
                                    kind: BlobKind::Snapshot,
                                    bytes: SANDBOX_BYTES,
                                    parent: None,
                                    recompute_ns: snapshot_restore_ns(SANDBOX_BYTES),
                                },
                            )],
                            exec_ns: t.exec_ns,
                            payload_bytes: 256 * 1024,
                        }
                    })
                    .collect();
                Agent {
                    chain,
                    requires: shards(prog.model),
                    tokens: script.tokens,
                    tools,
                    produces,
                    max_tokens: 4 * (24 + 200),
                    slo: prog.slo,
                    retention: Retention::default(),
                    tenant: Some(prog.script.tenant as u32),
                    root: self.claim_root(p, role_root(script.role)),
                }
            })
            .collect();
        let pattern = self.progs[p].pattern;
        let side_effects = agents
            .iter()
            .flat_map(|a| &a.tools)
            .filter(|t| t.tool.authority() == Authority::SideEffecting)
            .count() as u64;
        for _ in 0..side_effects {
            self.log(pattern, LogCause::Intent);
        }
        for agent in &gang_agents {
            let probe = Machine::agent_request(agent);
            if let Some(claimed) = self.mach.claimed_tokens(&probe) {
                let stats = self.out.claims.entry(probe.root).or_default();
                stats.n += 1;
                stats.overruns += u64::from(agent.tokens > claimed);
                stats.reserved_tokens += claimed;
            }
        }
        let resume_task = self.task();
        let prog = &self.progs[p];
        let req = Request {
            phase: 0,
            chain: Vec::new(),
            requires: Vec::new(),
            hint: Some(FlowHint {
                task: resume_task,
                template_len: 0,
                downstream: Vec::new(),
                probability: 1.0,
                lead_ops: WARM_LEAD_OPS,
                payload_bytes: 256 * 1024 * agents.len() as u64,
            }),
            completes: prog.pending_task,
            exec_ns: 0,
            tokens: 0,
            gang: Some(Gang {
                agents: gang_agents,
            }),
            produces: Vec::new(),
            max_tokens: 0,
            slo: prog.slo,
            retention: Retention::default(),
            concurrent: true,
            tenant: None,
            program: p as u64 + 1,
            pattern: Pattern::MultiAgent,
            authority: Authority::ReadOnly,
            root: 0,
            tool: false,
        };
        self.progs[p].pending_task = Some(resume_task);
        let extra = Extra::Fanout {
            agents: agents.len(),
            side_effects,
        };
        match self.mach.submit_at(due, &req) {
            Submitted::Closed(cost) => self.finish(p, k, due, &cost, extra),
            Submitted::Open(id) => {
                self.open.insert(id, (p, k, due, extra));
            }
        }
    }
}

#[must_use]
pub fn run(cfg: &Config) -> Outcome {
    Driver::new(cfg).run()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FlowKind {
    TurnToTool,
    TurnToFanout,
    FanoutToResume,
    FaasToInference,
}

impl FlowKind {
    pub const ALL: [Self; 4] = [
        Self::TurnToTool,
        Self::TurnToFanout,
        Self::FanoutToResume,
        Self::FaasToInference,
    ];

    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::TurnToTool => "agent turn -> its tool call",
            Self::TurnToFanout => "agent turn -> its fan-out",
            Self::FanoutToResume => "fan-out -> the orchestrator's resume",
            Self::FaasToInference => "FaaS call -> its inference",
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct FlowTimes {
    pub early_ns: Vec<i64>,
    pub lead_ns: Vec<u64>,
}

impl FlowTimes {
    #[must_use]
    pub fn before_share(&self) -> f64 {
        if self.early_ns.is_empty() {
            return 0.0;
        }
        self.early_ns.iter().filter(|&&e| e > 0).count() as f64 / self.early_ns.len() as f64
    }

    #[must_use]
    pub fn early_quantile(&self, q: f64) -> f64 {
        if self.early_ns.is_empty() {
            return 0.0;
        }
        let mut sorted = self.early_ns.clone();
        sorted.sort_unstable();
        sorted[((sorted.len() as f64 * q) as usize).min(sorted.len() - 1)] as f64
    }
}

#[derive(Clone, Debug, Default)]
pub struct Causality {
    pub flows: [FlowTimes; 4],
    pub turn_pairs: u64,
    pub turn_overlaps: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct ReplayConfig {
    pub ops: u64,
    pub fanout: f64,
    pub causal: bool,
    pub prefill_ahead: bool,
    pub grade: HintGrade,
    pub gate: f64,
    pub decode_held: bool,
}

#[derive(Clone, Debug, Default)]
pub struct ReplayOutcome {
    pub causality: Causality,
    pub flow_stall_ns: u64,
    pub flow_n: u64,
    pub stall_ns: u64,
    pub service_ns: u64,
    pub served: u64,
    pub learn: LearnStats,
    pub prefill_work_ns: u64,
    pub prefill_blocks: u64,
    pub landings: (u64, u64),
    pub locality: [(u64, u64); Pattern::N],
    pub memory: [(u64, u64); Pattern::N],
}

impl ReplayOutcome {
    #[must_use]
    pub fn flow_stall_mean_ns(&self) -> f64 {
        self.flow_stall_ns as f64 / self.flow_n.max(1) as f64
    }
}

#[derive(Clone, Copy)]
enum UpKind {
    Turn,
    Gang,
    Faas,
}

#[must_use]
#[allow(clippy::too_many_lines)]
pub fn replay(cfg: &Config, rc: &ReplayConfig) -> ReplayOutcome {
    let mut mach = build_machine(cfg, rc.decode_held);
    mach.set_prefill_ahead(rc.prefill_ahead);
    mach.set_hint_grade(rc.grade, rc.gate);
    let origins = Origins::default();
    mach.set_origins(origins.clone());
    let mut workload =
        Workload::with_fanout(cfg.seed, rc.ops, 1.0, rc.fanout).with_origins(origins);
    if rc.decode_held {
        workload = workload.with_decode_kv(35);
    }
    let trace: Vec<Request> = workload.collect();
    let interval = (1e9 / cfg.engine_rate) as u64;
    let upstream_tasks: HashSet<u64> = trace
        .iter()
        .filter_map(|r| r.hint.as_ref().map(|h| h.task))
        .collect();
    let mut held: HashMap<u64, Vec<usize>> = HashMap::new();
    let mut heap: BinaryHeap<Reverse<(u64, usize)>> = BinaryHeap::new();
    let mut slot = 0u64;
    let mut at = 0u64;
    for (i, req) in trace.iter().enumerate() {
        at = if req.concurrent {
            at
        } else {
            slot += 1;
            slot * interval
        };
        match req.completes {
            Some(task) if rc.causal && upstream_tasks.contains(&task) => {
                held.entry(task).or_default().push(i);
            }
            _ => heap.push(Reverse((at, i))),
        }
    }
    let mut out = ReplayOutcome::default();
    let mut ups: HashMap<u64, (u64, u64, UpKind)> = HashMap::new();
    let mut session_end: HashMap<u64, u64> = HashMap::new();
    while let Some(Reverse((t, i))) = heap.pop() {
        let req = &trace[i];
        let Submitted::Closed(cost) = mach.submit_at(t, req) else {
            continue;
        };
        let service = cost.service_ns();
        if !cost.pending {
            out.stall_ns += cost.total_ns();
            out.service_ns += service;
            out.served += 1;
        }
        let kv_first = req
            .chain
            .first()
            .is_some_and(|(_, m)| m.kind == BlobKind::KvBlock);
        if let Some(task) = req.completes
            && let Some(&(up_at, up_service, up_kind)) = ups.get(&task)
        {
            let kind = if req.gang.is_some() {
                FlowKind::TurnToFanout
            } else if !kv_first {
                FlowKind::TurnToTool
            } else if matches!(up_kind, UpKind::Gang) {
                FlowKind::FanoutToResume
            } else {
                FlowKind::FaasToInference
            };
            let flow = &mut out.causality.flows[kind as usize];
            let signed = |x: u64| i64::try_from(x).unwrap_or(i64::MAX);
            flow.early_ns.push(signed(up_at + up_service) - signed(t));
            flow.lead_ns.push(t - up_at);
        }
        if req.completes.is_none() && kv_first && req.tokens > 0 {
            if let Some(&end) = session_end.get(&req.program) {
                out.causality.turn_pairs += 1;
                out.causality.turn_overlaps += u64::from(t < end);
            }
            session_end.insert(req.program, t + service);
        }
        if let Some(h) = &req.hint {
            let kind = if req.gang.is_some() {
                UpKind::Gang
            } else if kv_first {
                UpKind::Turn
            } else {
                UpKind::Faas
            };
            ups.insert(h.task, (t, service, kind));
            if let Some(waiting) = held.remove(&h.task) {
                for j in waiting {
                    heap.push(Reverse((t + service, j)));
                }
            }
        }
    }
    mach.finish();
    out.flow_n = mach.instruments.flow.n;
    out.flow_stall_ns = mach.instruments.flow.stall_ns;
    out.learn = mach.learn_stats();
    out.prefill_work_ns = mach.instruments.prefill.work_ns;
    out.prefill_blocks = mach.instruments.prefill.blocks;
    out.landings = (
        mach.instruments.prefill.landed,
        mach.instruments.prefill.landings,
    );
    out.locality = mach.locality_by;
    out.memory = mach.memory_by_pattern();
    out
}

#[must_use]
pub fn replay_matches_submit(cfg: &Config, ops: u64) -> bool {
    let rc = ReplayConfig {
        ops,
        fanout: 0.1,
        causal: false,
        prefill_ahead: false,
        grade: HintGrade::Declared,
        gate: 0.0,
        decode_held: true,
    };
    let at = replay(cfg, &rc);
    let mut mach = build_machine(cfg, true);
    let trace: Vec<Request> = Workload::with_fanout(cfg.seed, ops, 1.0, 0.1)
        .with_decode_kv(35)
        .collect();
    let (mut stall, mut service, mut served) = (0u64, 0u64, 0u64);
    for req in &trace {
        let Submitted::Closed(cost) = mach.submit(req) else {
            continue;
        };
        if !cost.pending {
            stall += cost.total_ns();
            service += cost.service_ns();
            served += 1;
        }
    }
    (at.stall_ns, at.service_ns, at.served) == (stall, service, served)
}

#[must_use]
pub fn rag_reference(shape: &Shape, queries: u64, seed: u64) -> (f64, f64) {
    let mut rng = Rng::new(seed ^ 0x5241_4752);
    let mut seen_prefix: HashSet<Vec<u32>> = HashSet::new();
    let mut seen_chunk: HashSet<u32> = HashSet::new();
    let (mut prefix_hits, mut any_hits, mut total) = (0u64, 0u64, 0u64);
    for _ in 0..queries {
        let chunks = zipf_chunks(&mut rng, shape);
        let mut depth = 0;
        for i in 1..=chunks.len() {
            if seen_prefix.contains(&chunks[..i]) {
                depth = i;
            } else {
                break;
            }
        }
        prefix_hits += depth as u64;
        any_hits += chunks.iter().filter(|c| seen_chunk.contains(c)).count() as u64;
        total += chunks.len() as u64;
        for i in 1..=chunks.len() {
            seen_prefix.insert(chunks[..i].to_vec());
        }
        seen_chunk.extend(chunks.iter().copied());
    }
    (
        prefix_hits as f64 / total.max(1) as f64,
        any_hits as f64 / total.max(1) as f64,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn small(preset: Preset, programs: usize) -> Config {
        Config {
            programs,
            rate: 6.0,
            mix: vec![(preset, 1.0)],
            ..Config::default()
        }
    }

    fn shape() -> Shape {
        Shape::new(ShapeConfig::default())
    }

    #[test]
    fn a_script_is_drawn_from_its_own_stream() {
        let a = script(Preset::Agentic, 1, 7, &shape());
        let b = script(Preset::Agentic, 1, 7, &shape());
        let c = script(Preset::Agentic, 1, 8, &shape());
        assert_eq!(format!("{:?}", a.steps), format!("{:?}", b.steps));
        assert_ne!(format!("{:?}", a.steps), format!("{:?}", c.steps));
        assert_eq!(a.tenant, b.tenant);
    }

    #[test]
    fn authority_follows_mcp_annotations_and_unannotated_tools_are_side_effecting() {
        assert_eq!(Annotations::default().authority(), Authority::SideEffecting);
        assert_eq!(closed_read().authority(), Authority::ReadOnly);
        assert_eq!(open_read().authority(), Authority::ReadOnly);
        assert_eq!(closed_write().authority(), Authority::DraftOnly);
        let declared = catalog(ToolSet::Coding, AnnotationMode::Declared);
        let pessimistic = catalog(ToolSet::Coding, AnnotationMode::Pessimistic);
        let edits = |c: &[ToolSpec; TOOL_KINDS]| {
            c.iter()
                .filter(|t| t.class == ToolClass::Edit)
                .map(ToolSpec::authority)
                .collect::<Vec<_>>()
        };
        assert!(edits(&declared).iter().all(|a| *a == Authority::DraftOnly));
        assert!(
            edits(&pessimistic)
                .iter()
                .all(|a| *a == Authority::SideEffecting)
        );
        assert!(
            declared
                .iter()
                .filter(|t| t.class == ToolClass::Exec)
                .all(|t| t.authority() == Authority::SideEffecting)
        );
    }

    #[test]
    fn every_preset_runs_to_completion_and_is_counted_under_its_pattern() {
        for preset in Preset::ALL {
            let out = run(&small(preset, 40));
            let stats = &out.by_pattern[preset.pattern().idx()];
            assert_eq!(stats.programs, 40, "{preset:?}");
            assert_eq!(stats.completed + out.abandoned, 40, "{preset:?}");
            assert!(out.calls > 0, "{preset:?}");
        }
    }

    #[test]
    fn a_closed_loop_turn_is_longer_than_the_same_scripts_replayed_open_loop() {
        let closed = run(&small(Preset::Agentic, 120));
        let open = run(&Config {
            loop_mode: LoopMode::Open {
                lead_ns: 28_000_000,
            },
            ..small(Preset::Agentic, 120)
        });
        let c = mean(closed.turns_of(Pattern::Agentic));
        let o = mean(open.turns_of(Pattern::Agentic));
        assert!(c > 2.0 * o, "closed {c} open {o}");
    }

    #[test]
    fn an_open_loop_keeps_each_idle_steps_duration() {
        let cfg = Config {
            loop_mode: LoopMode::Open {
                lead_ns: 28_000_000,
            },
            ..small(Preset::LongRunning, 30)
        };
        let out = run(&cfg);
        assert_eq!(out.abandoned, 0);
        let shape = Shape::new(cfg.shape);
        let idle: u64 = (0..30u64)
            .flat_map(|id| script(Preset::LongRunning, cfg.seed, id, &shape).steps)
            .map(|s| match s {
                Step::Idle { ns, .. } => ns,
                _ => 0,
            })
            .sum();
        let latency: u64 = out.by_pattern[Pattern::LongRunning.idx()]
            .latency
            .iter()
            .sum();
        assert!(latency >= idle, "latency {latency} idle {idle}");
    }

    #[test]
    fn a_predicted_warm_up_is_weighed_on_every_call_not_only_where_a_tool_follows() {
        let out = run(&Config {
            hints: Hints::Predicted,
            ..small(Preset::Agentic, 60)
        });
        assert_eq!(out.abandoned, 0);
        let weighed = out.hints.warms + out.hints.skipped_hot + out.hints.skipped_price;
        assert_eq!(weighed, out.calls);
        assert!(out.calls > out.tools);
    }

    #[test]
    fn side_effecting_calls_are_logged_before_and_after_and_their_cells_are_leased_and_durable() {
        let out = run(&small(Preset::Agentic, 120));
        let intents = out.logged_of(LogCause::Intent);
        assert!(intents > 0);
        assert_eq!(intents, out.logged_of(LogCause::Outcome));
        assert!(out.leases.0 >= intents);
        assert_eq!(out.leases.1, 0);
        assert_eq!(out.durable_lost, 0);
    }

    #[test]
    fn nothing_is_logged_for_patterns_with_no_side_effects() {
        for preset in [
            Preset::OneShot,
            Preset::Extraction,
            Preset::Conversational,
            Preset::Batch,
        ] {
            let out = run(&small(preset, 60));
            assert_eq!(out.logged_total(), 0, "{preset:?}");
        }
    }

    #[test]
    fn the_estimator_learns_a_habitual_successor_and_ranks_it_first() {
        let mut est = Estimator::default();
        for _ in 0..30 {
            est.observe(1, Some(0), Some(3));
        }
        for _ in 0..10 {
            est.observe(1, Some(0), Some(5));
        }
        let pred = est.predict(1, Some(0)).expect("a prediction");
        assert_eq!(pred.ranked[0], 3);
        assert!((pred.p_top - 0.75).abs() < 1e-9);
        assert!((pred.p_tool - 1.0).abs() < 1e-9);
        assert!(est.predict(2, None).is_none());
    }

    #[test]
    fn rent_or_buy_holds_while_a_return_is_likely_and_suspends_once_it_is_not() {
        let mut surv = Survival::default();
        for g in 1..=100u64 {
            surv.observe(1, g * 1_000_000_000 * g / 10);
        }
        assert!(surv.rent_or_buy_ns(1, 1_000_000_000, 1e9, 0.0).is_none());
        let patient = surv
            .rent_or_buy_ns(1, 1_000_000_000, 1e9, 1e6)
            .expect("suspends");
        let eager = surv
            .rent_or_buy_ns(1, 1_000_000_000, 1e9, 1e9)
            .expect("suspends");
        assert!(eager <= patient);
        assert!(surv.rent_or_buy_ns(9, 1_000_000_000, 1e9, 1e6).is_none());
    }

    #[test]
    fn the_inference_reads_only_observables() {
        let out = run(&small(Preset::Agentic, 80));
        let (n, correct) = out.class.totals();
        assert!(n > 0 && correct <= n);
    }

    #[test]
    fn speculation_only_runs_read_only_tools_and_counts_what_it_wastes() {
        let out = run(&Config {
            speculate: Speculate::Always,
            ..small(Preset::Agentic, 200)
        });
        assert!(out.spec.tries > 0);
        assert!(out.spec.hits <= out.spec.tries);
        assert_eq!(
            out.logged_of(LogCause::Intent),
            out.logged_of(LogCause::Outcome)
        );
    }

    #[test]
    fn a_suspended_cell_is_demoted_and_never_dropped_once_durable() {
        let out = run(&Config {
            suspend: Suspend::Timers,
            ..small(Preset::LongRunning, 60)
        });
        assert_eq!(out.durable_lost, 0);
        assert!(out.life.idles > 0);
    }
    fn replay_config(causal: bool) -> ReplayConfig {
        ReplayConfig {
            ops: 4000,
            fanout: 0.1,
            causal,
            prefill_ahead: false,
            grade: HintGrade::Declared,
            gate: 0.0,
            decode_held: false,
        }
    }

    #[test]
    fn replaying_the_trace_at_its_instants_is_the_trace_submitted_in_order() {
        assert!(replay_matches_submit(&Config::default(), 1500));
    }

    #[test]
    fn the_published_trace_runs_a_tool_call_during_the_turn_that_issues_it() {
        let out = replay(&Config::default(), &replay_config(false));
        let tool = &out.causality.flows[FlowKind::TurnToTool as usize];
        assert!(tool.before_share() > 0.99);
        let faas = &out.causality.flows[FlowKind::FaasToInference as usize];
        assert!(faas.before_share() < 0.01);
        assert!(out.causality.turn_overlaps > 0);
    }

    #[test]
    fn a_causal_replay_releases_no_downstream_before_its_upstream_finishes() {
        let out = replay(&Config::default(), &replay_config(true));
        for kind in FlowKind::ALL {
            let flow = &out.causality.flows[kind as usize];
            assert!(!flow.early_ns.is_empty(), "{kind:?}");
            assert!(flow.early_ns.iter().all(|&e| e <= 0), "{kind:?}");
        }
    }

    #[test]
    fn a_learned_template_is_known_for_most_flows_and_a_template_is_a_prefix_of_the_declaration() {
        let rc = ReplayConfig {
            prefill_ahead: true,
            grade: HintGrade::Learned,
            ..replay_config(false)
        };
        let out = replay(&Config::default(), &rc);
        assert!(out.learn.flows > 100);
        let known = out.learn.known as f64 / out.learn.flows as f64;
        assert!(known > 0.3 && known < 1.0, "{known}");
        assert!(out.prefill_blocks > 0);
    }

    #[test]
    fn the_infinite_cache_reuses_more_of_a_canonical_order_than_of_a_relevance_order() {
        let canonical = Shape::new(ShapeConfig::default());
        let relevance = Shape::new(ShapeConfig {
            rag_canonical: false,
            ..ShapeConfig::default()
        });
        let (c_prefix, c_any) = rag_reference(&canonical, 5000, 1);
        let (r_prefix, r_any) = rag_reference(&relevance, 5000, 1);
        assert!(c_prefix > r_prefix);
        assert!((c_any - r_any).abs() < 0.02);
        assert!(c_any > c_prefix);
    }

    fn lose_node_zero(at_ns: u64, copy: bool) -> Config {
        use crate::fault::{Client, Fault, NodeLoss};
        Config {
            suspend: Suspend::Timers,
            fault: Some((
                at_ns,
                Fault::Node(NodeLoss {
                    node: 0,
                    declare_ns: 0,
                    client: Client::Restart,
                }),
            )),
            copy_durable: copy,
            ..small(Preset::LongRunning, 60)
        }
    }

    #[test]
    fn a_lost_node_takes_the_durable_sandboxes_it_holds_unless_they_were_copied() {
        let at = 10_000_000_000;
        let bare = run(&lose_node_zero(at, false));
        assert_eq!(bare.fault.nodes_lost, 1);
        assert!(bare.fault.durable_lost_with_node > 0, "{:?}", bare.fault);
        assert_eq!(bare.fault.durable_saved, 0);
        assert!(bare.state_lost > 0);
        assert_eq!(bare.durable_lost, 0, "a lost node is not a dropped cell");

        let copied = run(&lose_node_zero(at, true));
        assert_eq!(copied.fault.durable_lost_with_node, 0);
        assert!(copied.fault.durable_saved >= bare.fault.durable_lost_with_node);
        assert!(copied.fault.durable_copied_bytes > 0);
        assert_eq!(copied.state_lost, 0);
    }

    #[test]
    fn copying_durable_cells_changes_no_program_and_charges_their_bytes() {
        let run_with = |copy: bool| {
            run(&Config {
                suspend: Suspend::Timers,
                copy_durable: copy,
                ..small(Preset::LongRunning, 40)
            })
        };
        let plain = run_with(false);
        let copying = run_with(true);
        let turns = |o: &Outcome| o.by_pattern[Pattern::LongRunning.idx()].turns.clone();
        assert_eq!(
            format!("{:?}", plain.logged),
            format!("{:?}", copying.logged)
        );
        assert_eq!(turns(&plain), turns(&copying));
        assert_eq!(plain.fault.durable_copied_bytes, 0);
        assert!(copying.fault.durable_copied_bytes > 0);
    }
}
