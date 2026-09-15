use crate::blob::{BlobId, BlobKind, BlobMeta, ROOT};
use crate::flow::FlowHint;
use crate::rng::Rng;
use std::collections::{HashMap, VecDeque};

/// A content-addressed chain: an ordered blob list where each entry's parent is its predecessor.
pub type Chain = Vec<(BlobId, BlobMeta)>;

pub const KV_BLOCK_BYTES: u64 = 512 * 1024;
pub const KV_BLOCK_NS: u64 = 400_000;
pub const SNAPSHOT_BYTES: u64 = 32 * 1024 * 1024;
pub const SNAPSHOT_NS: u64 = 200_000_000;
pub const WEIGHT_BYTES: u64 = 512 * 1024 * 1024;
pub const WEIGHT_NS: u64 = 4_000_000_000;

const TENANTS: u64 = 24;
const SESSIONS: usize = 512;
const FUNCTIONS: u64 = 400;
const SHARDS: u64 = 8;
const MODELS: u64 = 4;
const SHARDS_PER_MODEL: u64 = 2;
const SERVICES: u64 = 3;
pub const SERVICE_BYTES: u64 = 384 * 1024 * 1024;
pub const SERVICE_COLD_NS: u64 = 15_000_000_000;
const REPLICAS: [u64; PHASES] = [3, 1, 2, 3];
const MAX_TURNS: u32 = 24;

/// Fraction of function invocations that call into inference -- the README's canonical
/// cross-workload task.
const FLOW_FRACTION: f64 = 0.45;
const FLOW_LEAD_OPS: u32 = 6;
const FLOW_PROMPT_BLOCKS: u64 = 24;
/// Function output handed to the model: a prompt plus retrieved context.
pub const FLOW_PAYLOAD_BYTES: u64 = 4 * 1024 * 1024;

#[derive(Clone, Debug)]
struct Session {
    tenant: usize,
    id: u64,
    turns: u32,
}

pub const PHASES: usize = 4;

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
}

#[derive(Clone, Copy, Debug)]
pub struct Mix {
    pub inference: f64,
    pub faas: f64,
    pub weights: f64,
}

const PHASE_MIX: [[f64; 4]; PHASES] = [
    [0.55, 0.12, 0.03, 0.30],
    [0.18, 0.62, 0.02, 0.18],
    [0.30, 0.15, 0.35, 0.20],
    [0.35, 0.28, 0.07, 0.30],
];

#[derive(Debug)]
pub struct Workload {
    rng: Rng,
    ops: u64,
    volatility: f64,
    share_weights: bool,
    issued: u64,
    tenant_prefix: Vec<Chain>,
    sessions: Vec<Session>,
    next_session: u64,
    pending: VecDeque<(u64, u64, Chain)>,
    prompt_cache: HashMap<u64, Chain>,
    next_task: u64,
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
            share_weights: false,
            issued: 0,
            tenant_prefix,
            sessions: Vec::new(),
            next_session: 0,
            pending: VecDeque::new(),
            prompt_cache: HashMap::new(),
            next_task: 0,
        };
        for _ in 0..SESSIONS {
            let s = w.fresh_session();
            w.sessions.push(s);
        }
        w
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

    /// Make inference requests depend on their model's weight shards, shared across tenants.
    #[must_use]
    pub fn with_shared_weights(mut self) -> Self {
        self.share_weights = true;
        self
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
        let mut flat = [0.0; 4];
        for m in PHASE_MIX {
            for i in 0..4 {
                flat[i] += m[i] / PHASES as f64;
            }
        }
        let v = self.volatility;
        let mut w = [0.0; 4];
        for i in 0..4 {
            w[i] = flat[i] + v * (p[i] - flat[i]);
        }
        let sum: f64 = w.iter().sum();
        Mix {
            inference: w[0] / sum,
            faas: (w[0] + w[1]) / sum,
            weights: (w[0] + w[1] + w[2]) / sum,
        }
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
        let model = tenant as u64 % MODELS;
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

    fn inference(&mut self) -> Chain {
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
        chain
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
        vec![(
            id,
            BlobMeta {
                kind: BlobKind::Snapshot,
                bytes: SNAPSHOT_BYTES * scale / 2,
                parent: None,
                recompute_ns: SNAPSHOT_NS * scale / 2,
            },
        )]
    }

    fn weights(&mut self) -> Chain {
        let s = self.rng.zipf(SHARDS, 2.0);
        let id = BlobId::leaf(format!("shard:{s}").as_bytes());
        vec![(
            id,
            BlobMeta {
                kind: BlobKind::WeightShard,
                bytes: WEIGHT_BYTES,
                parent: None,
                recompute_ns: WEIGHT_NS,
            },
        )]
    }
}

impl Iterator for Workload {
    type Item = Request;

    fn next(&mut self) -> Option<Self::Item> {
        let phase = self.phase();
        // pending is pushed in non-decreasing due order, so only the front can ever be ready.
        let ready = self
            .pending
            .front()
            .is_some_and(|(due, _, _)| *due <= self.issued);
        if ready {
            let (_, task, chain) = self.pending.pop_front().expect("front checked");
            self.issued += 1;
            return Some(Request {
                phase,
                chain,
                requires: Vec::new(),
                hint: None,
                completes: Some(task),
            });
        }
        if self.issued >= self.ops {
            // Drain anything still queued so every started task resolves, rather than being
            // counted as attempted but neither completed nor broken.
            if let Some((_, task, chain)) = self.pending.pop_front() {
                return Some(Request {
                    phase,
                    chain,
                    requires: Vec::new(),
                    hint: None,
                    completes: Some(task),
                });
            }
            return None;
        }
        let mix = self.mix();
        let roll = self.rng.unit();
        self.issued += 1;
        if roll >= mix.inference && roll < mix.faas {
            let f = self.rng.zipf(FUNCTIONS, 1.5);
            let chain = self.faas_for(f);
            if self.rng.chance(FLOW_FRACTION) {
                self.next_task += 1;
                let task = self.next_task;
                let downstream = self.flow_chain(f);
                self.pending.push_back((
                    self.issued + u64::from(FLOW_LEAD_OPS),
                    task,
                    downstream.clone(),
                ));
                let hint = FlowHint {
                    task,
                    downstream,
                    probability: 1.0,
                    lead_ops: FLOW_LEAD_OPS,
                    payload_bytes: FLOW_PAYLOAD_BYTES,
                };
                return Some(Request {
                    phase,
                    chain,
                    requires: Vec::new(),
                    hint: Some(hint),
                    completes: None,
                });
            }
            return Some(Request {
                phase,
                chain,
                requires: Vec::new(),
                hint: None,
                completes: None,
            });
        }
        if roll < mix.inference {
            let slot = self.rng.zipf(self.sessions.len() as u64, 1.3) as usize;
            let tenant = self.sessions[slot].tenant;
            let chain = self.inference();
            let requires = if self.share_weights {
                Self::model_shards(tenant)
            } else {
                Vec::new()
            };
            return Some(Request {
                phase,
                chain,
                requires,
                hint: None,
                completes: None,
            });
        }
        let chain = if roll < mix.weights {
            self.weights()
        } else {
            self.service()
        };
        Some(Request {
            phase,
            chain,
            requires: Vec::new(),
            hint: None,
            completes: None,
        })
    }
}
