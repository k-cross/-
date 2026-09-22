//! The `Telemetry` boundary of `owned-and-observed.md` §3.1, designed in `phase-1.md` §4.3.
//! Exact in Phase 1: it wraps `Hierarchy` and `Engine` rather than replacing them, and
//! Phase 4 is the edit that widens `resident`/`held` into a belief and moves the counters
//! onto §1's ZMQ channel.
//!
//! Three sets of reads stay outside it, and the distinction matters to Phase 4:
//!
//! - **Ground truth, by design** (`phase-1.md` §1.4): `ground_truth_holds` in `apply_chain`
//!   and `apply_deps`, and `truly_resident`. §3.6's divergence *is* the gap between these
//!   and the belief reads here, so folding them in would delete the metric.
//! - **Not a belief**: `hot_ids` (the `Gossip` arm's own snapshot), `can_decode` (static
//!   node capability).
//! - **Reporting, not deciding**: `arms.rs` and `bin/rpcbench.rs` read `Hierarchy`
//!   aggregates after a run. §3.1 scopes this boundary to the *scheduler*, so they are out
//!   of scope rather than overlooked -- but they are also why several reads below have no
//!   caller in `machine.rs` yet.

use crate::blob::{BlobId, BlobKind, BlobMeta};
use crate::cache::Hierarchy;
use crate::engine::Engine;
use crate::tier::Tier;

/// Per-class byte counts, as `Hierarchy::could_admit` and `displacement` take them.
pub type Need = [u64; BlobKind::N];

/// A read-only view of one domain's engine-owned and orchestrator-owned state, as the
/// scheduler is allowed to see it. Borrows rather than copies: constructing one costs two
/// references, not a snapshot.
#[derive(Clone, Copy, Debug)]
pub struct Telemetry<'a> {
    hierarchy: &'a Hierarchy,
    engine: &'a Engine,
}

impl<'a> Telemetry<'a> {
    #[must_use]
    pub fn new(hierarchy: &'a Hierarchy, engine: &'a Engine) -> Self {
        Self { hierarchy, engine }
    }

    /// Is this blob usable right now, with no copy. Exact in Phase 1; Phase 4 widens this
    /// to `P(resident)`, estimated from reported turnover.
    #[must_use]
    pub fn resident(&self, id: &BlobId, kind: BlobKind) -> bool {
        self.hierarchy.is_hot(id, kind)
    }

    /// Held anywhere on this domain, hot or offloaded -- what a peer could pull over
    /// RDMA. Exact in Phase 1; Phase 4 widens this the same way `resident` does.
    #[must_use]
    pub fn held(&self, id: &BlobId, kind: BlobKind) -> bool {
        self.hierarchy.holds(id, kind)
    }

    /// Exact today; Phase 3 is what makes the `Hbm` answer a partition rather than the
    /// whole accelerator.
    #[must_use]
    pub fn capacity(&self, tier: Tier) -> u64 {
        self.hierarchy.pool(tier).spec().capacity
    }

    #[must_use]
    pub fn free(&self, tier: Tier) -> u64 {
        self.hierarchy.pool(tier).free_bytes()
    }

    /// Free space plus what is reclaimable above each class's floor -- see
    /// `TierPool::reclaimable`'s own doc comment for why this is conservative rather than
    /// a promise.
    #[must_use]
    pub fn reclaimable(&self, tier: Tier) -> u64 {
        self.hierarchy.pool(tier).reclaimable()
    }

    /// Bytes of `kind` currently resident, at whichever tier is its home on this domain.
    /// Exact in Phase 1; Phase 4 turns this into an estimate for engine-allocated classes,
    /// since the orchestrator no longer allocates them directly after Phase 3.
    #[must_use]
    pub fn resident_bytes(&self, kind: BlobKind) -> u64 {
        self.hierarchy.resident_bytes(kind)
    }

    /// Total occupancy across the hot pools. Engine-allocated bytes are part of the sum,
    /// so this is a belief read like the rest and not a fact the orchestrator holds.
    #[must_use]
    pub fn used(&self) -> u64 {
        self.hierarchy.used()
    }

    /// The price of admitting one more byte into this pool right now -- see
    /// `TierPool::marginal_price`'s own doc comment. Exact in Phase 1, computed
    /// in-process; Phase 3 is what makes the engine-owned half of this an observed
    /// quantity rather than a value this process derives directly.
    #[must_use]
    pub fn marginal_price(&self, tier: Tier) -> f64 {
        self.hierarchy.pool(tier).marginal_price()
    }

    /// How often `kind`'s evictions, at its home tier, were later wanted back. Exact and
    /// in-process today; Phase 4's `Metrics { interval }` channel is what replaces this
    /// with a value reported by the engine instead of one this process measured itself.
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

    /// In-flight requests on this domain's engine at `now_ns`. From `Engine::load`, which
    /// is already an in-process read rather than a scrape -- Phase 4's step-aligned
    /// ingestion is about the *cross-process* case this simulator does not have.
    #[must_use]
    pub fn queue_depth(&self, now_ns: u64) -> usize {
        self.engine.load(now_ns)
    }

    #[must_use]
    pub fn batch_occupancy(&self) -> f64 {
        self.engine.mean_batch()
    }

    /// The two engine-load reads that actually reach the placement argmin, as the
    /// `engine` and `congestion` terms. They are here rather than only on `Engine`
    /// because Phase 4 turns engine load into a sampled quantity, and an edit there that
    /// the argmin does not see would leave the score reading exact state while the rest
    /// of the boundary reads a belief.
    #[must_use]
    pub fn projected_ns(&self, now_ns: u64, tokens: u64, reserved: usize) -> u64 {
        self.engine.projected_ns(now_ns, tokens, reserved)
    }

    #[must_use]
    pub fn congestion_ns(&self, now_ns: u64, tokens: u64, reserved: usize) -> u64 {
        self.engine.congestion_ns(now_ns, tokens, reserved)
    }

    /// What it costs to make one missing blob hot without leaving the node.
    #[must_use]
    pub fn local_ns(&self, id: &BlobId, meta: &BlobMeta) -> u64 {
        self.hierarchy.local_ns(id, meta)
    }

    /// Expected recompute this node's other work pays to make room, per `phase-1.md`
    /// §1.4's list of belief reads the scheduler makes when deciding.
    #[must_use]
    pub fn displacement(&self, need: &Need, reserved: &Need) -> f64 {
        self.hierarchy.displacement(need, reserved)
    }

    #[must_use]
    pub fn could_admit(&self, need: &Need, reserved: &Need) -> bool {
        self.hierarchy.could_admit(need, reserved)
    }

    /// Which pool a class's home is in on this domain. Without it a caller reaching for
    /// the tier-addressed reads has to guess, and guessing `Hbm` for a `KvBlock` is wrong
    /// on exactly the unified-memory nodes where the class is fully resident in `Ddr`.
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
        };
        (Hierarchy::new(mem, Policy::Gdsf), Engine::new(32))
    }

    /// Every read is exact in Phase 1: it must agree with the `Hierarchy`/`Engine` method
    /// it wraps, on the nose, for every input this test exercises. A widened `resident`
    /// in Phase 4 is exactly the edit that would make this test start failing, which is
    /// the point -- it pins "no-op by construction" against the type that will end it.
    #[test]
    #[allow(
        clippy::float_cmp,
        reason = "the claim under test is that Telemetry's f64 reads are bit-identical \
                  passthroughs, not approximately equal, to what Hierarchy/Engine compute"
    )]
    fn every_read_agrees_with_the_hierarchy_and_engine_it_wraps() {
        let (mut h, mut e) = fixture();
        // An idle engine reports 0 for both load and mean batch, so asserting the
        // passthrough against one compares 0 to 0 and would hold for a stub. Drive it
        // first, then assert the readings are non-trivial before comparing them.
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

    /// `TierPool`'s quota reads index by `usize` and reach `Quota`'s per-class accessors
    /// as `BlobKind::ALL[k]`. That rewrite is only value-preserving while the two agree,
    /// and nothing else in the crate would notice if `ALL` were reordered.
    #[test]
    fn blobkind_all_is_indexed_by_its_own_idx() {
        for (k, kind) in BlobKind::ALL.into_iter().enumerate() {
            assert_eq!(kind.idx(), k, "{kind:?}");
        }
    }
}
