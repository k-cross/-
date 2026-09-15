use crate::blob::{BlobId, BlobKind, BlobMeta, ROOT};
use crate::rng::Rng;

pub const KV_BLOCK_BYTES: u64 = 512 * 1024;
pub const KV_BLOCK_NS: u64 = 400_000;
pub const SNAPSHOT_BYTES: u64 = 32 * 1024 * 1024;
pub const SNAPSHOT_NS: u64 = 200_000_000;
pub const WEIGHT_BYTES: u64 = 512 * 1024 * 1024;
pub const WEIGHT_NS: u64 = 4_000_000_000;

const TENANTS: u64 = 24;
const SESSIONS: usize = 512;
const FUNCTIONS: u64 = 400;
const SHARDS: u64 = 40;
const SERVICES: u64 = 6;
pub const SERVICE_BYTES: u64 = 512 * 1024 * 1024;
pub const SERVICE_COLD_NS: u64 = 15_000_000_000;
const REPLICAS: [u64; PHASES] = [4, 1, 2, 4];
const MAX_TURNS: u32 = 24;

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
    pub chain: Vec<(BlobId, BlobMeta)>,
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
    issued: u64,
    tenant_prefix: Vec<Vec<(BlobId, BlobMeta)>>,
    sessions: Vec<Session>,
    next_session: u64,
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

    fn service(&mut self) -> Vec<(BlobId, BlobMeta)> {
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

    fn inference(&mut self) -> Vec<(BlobId, BlobMeta)> {
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

    fn faas(&mut self) -> Vec<(BlobId, BlobMeta)> {
        let f = self.rng.zipf(FUNCTIONS, 1.5);
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

    fn weights(&mut self) -> Vec<(BlobId, BlobMeta)> {
        let s = self.rng.zipf(SHARDS, 1.1);
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
        if self.issued >= self.ops {
            return None;
        }
        let mix = self.mix();
        let phase = self.phase();
        let roll = self.rng.unit();
        self.issued += 1;
        let chain = if roll < mix.inference {
            self.inference()
        } else if roll < mix.faas {
            self.faas()
        } else if roll < mix.weights {
            self.weights()
        } else {
            self.service()
        };
        Some(Request { phase, chain })
    }
}
