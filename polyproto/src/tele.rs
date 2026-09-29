use crate::belief::Belief;
use crate::blob::{BlobId, BlobKind, BlobMeta};
use crate::cache::Hierarchy;
use crate::engine::Engine;
use crate::tier::Tier;

pub type Need = [u64; BlobKind::N];

#[derive(Clone, Copy, Debug)]
pub struct Telemetry<'a> {
    hierarchy: &'a Hierarchy,
    engine: &'a Engine,
    belief: Option<&'a Belief>,
    load: Option<usize>,
}

impl<'a> Telemetry<'a> {
    #[must_use]
    pub fn new(hierarchy: &'a Hierarchy, engine: &'a Engine) -> Self {
        Self {
            hierarchy,
            engine,
            belief: None,
            load: None,
        }
    }

    #[must_use]
    pub fn with_load(mut self, load: Option<usize>) -> Self {
        self.load = load;
        self
    }

    #[must_use]
    pub fn with_belief(mut self, belief: Option<&'a Belief>) -> Self {
        self.belief = belief;
        self
    }

    fn sight(&self, kind: BlobKind) -> Option<&'a Belief> {
        self.belief.filter(|_| kind == BlobKind::KvBlock)
    }

    fn kv_view(&self) -> Option<(u64, f64)> {
        let belief = self.belief?;
        let (capacity, _) = self.hierarchy.kv_partition()?;
        Some((capacity.saturating_sub(belief.gpu_bytes()), belief.price()))
    }

    #[must_use]
    pub fn resident(&self, id: &BlobId, kind: BlobKind) -> bool {
        if let Some(belief) = self.sight(kind) {
            return belief.believes_gpu(id);
        }
        self.hierarchy.is_hot(id, kind)
    }

    #[must_use]
    pub fn held(&self, id: &BlobId, kind: BlobKind) -> bool {
        if let Some(belief) = self.sight(kind) {
            return belief.believes_held(id);
        }
        self.hierarchy.holds(id, kind)
    }

    #[must_use]
    pub fn capacity(&self, tier: Tier) -> u64 {
        self.hierarchy.pool(tier).spec().capacity
    }

    #[must_use]
    pub fn free(&self, tier: Tier) -> u64 {
        self.hierarchy.pool(tier).free_bytes()
    }

    #[must_use]
    pub fn reclaimable(&self, tier: Tier) -> u64 {
        self.hierarchy.pool(tier).reclaimable()
    }

    #[must_use]
    pub fn resident_bytes(&self, kind: BlobKind) -> u64 {
        if let Some(belief) = self.sight(kind) {
            return belief.gpu_bytes();
        }
        self.hierarchy.resident_bytes(kind)
    }

    #[must_use]
    pub fn used(&self) -> u64 {
        self.hierarchy.used()
    }

    #[must_use]
    pub fn marginal_price(&self, tier: Tier) -> f64 {
        if tier == Tier::Hbm
            && self.hierarchy.split()
            && let Some(price) = self.hierarchy.kv_tail_price()
        {
            return self.belief.map_or(price, Belief::price);
        }
        self.hierarchy.pool(tier).marginal_price()
    }

    #[must_use]
    pub fn partition(&self) -> Option<(u64, u64)> {
        self.hierarchy.kv_partition()
    }

    #[must_use]
    pub fn regret_rate(&self, kind: BlobKind) -> f64 {
        self.hierarchy.regret_rate(kind)
    }

    #[must_use]
    pub fn hits(&self, kind: BlobKind) -> u64 {
        self.hierarchy.hits[kind.idx()]
    }

    #[must_use]
    pub fn misses(&self, kind: BlobKind) -> u64 {
        self.hierarchy.misses[kind.idx()]
    }

    #[must_use]
    pub fn evicted(&self, kind: BlobKind) -> u64 {
        self.hierarchy.evicted()[kind.idx()]
    }

    #[must_use]
    pub fn refused(&self, kind: BlobKind) -> u64 {
        self.hierarchy.refused()[kind.idx()]
    }

    #[must_use]
    pub fn queue_depth(&self, now_ns: u64) -> usize {
        self.engine.load(now_ns)
    }

    #[must_use]
    pub fn batch_occupancy(&self) -> f64 {
        self.engine.mean_batch()
    }

    #[must_use]
    pub fn projected_ns(&self, now_ns: u64, tokens: u64, reserved: usize) -> u64 {
        match self.load {
            Some(live) => self.engine.projected_live(now_ns, tokens, live + reserved),
            None => self.engine.projected_ns(now_ns, tokens, reserved),
        }
    }

    #[must_use]
    pub fn congestion_ns(&self, now_ns: u64, tokens: u64, reserved: usize) -> u64 {
        match self.load {
            Some(live) => self.engine.congestion_live(tokens, live + reserved),
            None => self.engine.congestion_ns(now_ns, tokens, reserved),
        }
    }

    #[must_use]
    pub fn local_ns(&self, id: &BlobId, meta: &BlobMeta) -> u64 {
        if let Some(belief) = self.sight(meta.kind) {
            return self.hierarchy.kv_acquire_ns(belief.tier(id), meta);
        }
        self.hierarchy.local_ns(id, meta)
    }

    #[must_use]
    pub fn displacement(&self, need: &Need, reserved: &Need) -> f64 {
        match self.kv_view() {
            Some(kv) => self.hierarchy.displacement_seen(need, reserved, Some(kv)),
            None => self.hierarchy.displacement(need, reserved),
        }
    }

    #[must_use]
    pub fn displacement_in(&self, tier: Tier, need: &Need, reserved: &Need) -> f64 {
        match self.kv_view() {
            Some(kv) => self
                .hierarchy
                .displacement_in_seen(tier, need, reserved, Some(kv)),
            None => self.hierarchy.displacement_in(tier, need, reserved),
        }
    }

    #[must_use]
    pub fn could_admit(&self, need: &Need, reserved: &Need) -> bool {
        self.hierarchy.could_admit(need, reserved)
    }

    #[must_use]
    pub fn tier_of(&self, kind: BlobKind) -> Tier {
        self.hierarchy.tier_of(kind)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blob::{BlobId, BlobMeta};
    use crate::cache::{Admission, NodeMemory, Policy, Quota};

    fn fixture() -> (Hierarchy, Engine) {
        let bands = [0u8; BlobKind::N];
        let mem = NodeMemory {
            hbm: 4 << 30,
            ddr: 8 << 30,
            nvme: 64 << 30,
            hbm_quota: Quota::open(4 << 30, bands),
            ddr_quota: Quota::open(8 << 30, bands),
            can_decode: true,
            kv: None,
        };
        (Hierarchy::new(mem, Policy::Gdsf), Engine::new(32))
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn every_read_agrees_with_the_hierarchy_and_engine_it_wraps() {
        let (mut h, mut e) = fixture();
        for i in 0..8 {
            e.decode(i * 1_000_000, 128);
        }
        let id = BlobId::leaf(b"telemetry-fixture");
        let meta = BlobMeta {
            kind: BlobKind::KvBlock,
            bytes: 4096,
            parent: None,
            recompute_ns: 10_000,
        };
        let mut out = Vec::new();
        assert_eq!(h.hbm.admit(id, meta, &mut out), Admission::Admitted);

        let t = Telemetry::new(&h, &e);
        assert_eq!(
            t.resident(&id, BlobKind::KvBlock),
            h.is_hot(&id, BlobKind::KvBlock)
        );
        assert_eq!(
            t.held(&id, BlobKind::KvBlock),
            h.holds(&id, BlobKind::KvBlock)
        );
        for tier in [Tier::Hbm, Tier::Ddr, Tier::Nvme] {
            assert_eq!(t.capacity(tier), h.pool(tier).spec().capacity);
            assert_eq!(t.free(tier), h.pool(tier).free_bytes());
            assert_eq!(t.reclaimable(tier), h.pool(tier).reclaimable());
            assert_eq!(t.marginal_price(tier), h.pool(tier).marginal_price());
        }
        for kind in BlobKind::ALL {
            assert_eq!(t.resident_bytes(kind), h.resident_bytes(kind));
            assert_eq!(t.regret_rate(kind), h.regret_rate(kind));
            assert_eq!(t.hits(kind), h.hits[kind.idx()]);
            assert_eq!(t.misses(kind), h.misses[kind.idx()]);
            assert_eq!(t.evicted(kind), h.evicted()[kind.idx()]);
            assert_eq!(t.refused(kind), h.refused()[kind.idx()]);
        }
        assert!(
            e.mean_batch() > 0.0,
            "fixture must drive the engine, or the two assertions below compare 0 to 0"
        );
        assert_eq!(t.queue_depth(0), e.load(0));
        assert_eq!(t.batch_occupancy(), e.mean_batch());
        assert_eq!(
            t.projected_ns(0, 128, 0),
            e.projected_ns(0, 128, 0),
            "the argmin's engine term reads through here"
        );
        assert_eq!(t.congestion_ns(0, 128, 0), e.congestion_ns(0, 128, 0));
        for kind in BlobKind::ALL {
            assert_eq!(t.tier_of(kind), h.tier_of(kind));
        }
        let need = [4096u64, 0, 0, 0];
        let none = [0u64; BlobKind::N];
        assert_eq!(t.could_admit(&need, &none), h.could_admit(&need, &none));
        assert_eq!(t.displacement(&need, &none), h.displacement(&need, &none));
        assert_eq!(t.local_ns(&id, &meta), h.local_ns(&id, &meta));
    }

    #[test]
    fn blobkind_all_is_indexed_by_its_own_idx() {
        for (k, kind) in BlobKind::ALL.into_iter().enumerate() {
            assert_eq!(kind.idx(), k, "{kind:?}");
        }
    }
}
