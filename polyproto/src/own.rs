//! The ownership predicate `owned-and-observed.md` §1 and §9's Phase 1 ask for, and
//! `phase-1.md` §4.2 designs: which side of the orchestrator/engine boundary answers a
//! question about a class of state, in a given memory pool.
//!
//! This is deliberately not `fn owner(kind: BlobKind) -> Ownership`. §1's table needs a
//! second axis -- *which question is asked* -- because the orchestrator sizes an HBM
//! partition the engine allocates within, and an engine-owned offload tier sits inside a
//! host DDR budget the orchestrator sizes. A one-axis type cannot hold both rows at once;
//! see `phase-1.md` §1.1.
//!
//! The predicate answers; it does not enforce. Calling this changes no code path in Phase
//! 1 -- every existing call site keeps its current behaviour, and this module exists so
//! Phase 3 has one place to change instead of every place ownership was assumed.

use crate::blob::BlobKind;
use crate::tier::Tier;

/// Which side of the boundary holds authority over the answer to a `Question`. `Engine`
/// does not mean "the engine's memory" -- it means the engine is the source of truth for
/// this particular question about this class in this pool.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Authority {
    Orchestrator,
    Engine,
}

/// The two questions `owned-and-observed.md` §1 distinguishes: how big a pool is (a slow,
/// coarse decision) against which bytes occupy it right now (fast, per-request). The same
/// `(kind, tier)` cell can answer them differently -- that asymmetry is the whole point of
/// the type, not an edge case in it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Question {
    Capacity,
    Allocation,
}

/// `owned-and-observed.md` §1's table, transcribed. Total over `(BlobKind, Tier,
/// Question)`: no wildcard arm, so a new `BlobKind` or `Tier` variant is a compile error
/// here until this table says what it owns, rather than silently inheriting a default.
///
/// Capacity is `Orchestrator` in every cell -- §1's "the loss is granularity, not
/// authority", and `phase-1.md`'s P1. Allocation splits by class: `KvBlock` and
/// `WeightShard` are the engine's in every tier they can reach, `Snapshot` and
/// `ServiceHeap` stay the orchestrator's outright. The `Hbm` row for a host class is
/// unreachable under today's `Hierarchy::tier_of` -- spelled rather than `unreachable!()`
/// because Phase 6's heterogeneous fleet is what would make it reachable, and because rule
/// 2 (`phase-1.md` §3) wants the table total rather than partial.
#[must_use]
#[allow(
    clippy::match_same_arms,
    reason = "merging arms with identical bodies would compress this back into the kind-only \
              predicate phase-1.md \u{a7}1.1 rejects; each row stays spelled so it can be \
              reviewed against owned-and-observed.md \u{a7}1's table one line at a time"
)]
pub fn authority(kind: BlobKind, tier: Tier, q: Question) -> Authority {
    use Authority::{Engine, Orchestrator};
    use BlobKind::{KvBlock, ServiceHeap, Snapshot, WeightShard};
    use Question::{Allocation, Capacity};
    use Tier::{Ddr, Hbm, Nvme};

    match (kind, tier, q) {
        // Allocator: vLLM's block manager in HBM, the KV connector (LMCache, NIXL) once
        // offloaded, and by phase-1.md §1.3's rule the connector's disk tier as well.
        (KvBlock, Hbm, Capacity) => Orchestrator,
        (KvBlock, Hbm, Allocation) => Engine,
        (KvBlock, Ddr, Capacity) => Orchestrator,
        (KvBlock, Ddr, Allocation) => Engine,
        (KvBlock, Nvme, Capacity) => Orchestrator,
        (KvBlock, Nvme, Allocation) => Engine,

        // Which models load where is a placement decision (Phase 6), not a per-request
        // allocation one, so it does not move these cells.
        (WeightShard, Hbm, Capacity) => Orchestrator,
        (WeightShard, Hbm, Allocation) => Engine,
        (WeightShard, Ddr, Capacity) => Orchestrator,
        (WeightShard, Ddr, Allocation) => Engine,
        (WeightShard, Nvme, Capacity) => Orchestrator,
        (WeightShard, Nvme, Allocation) => Engine,

        // No engine allocates on a microVM cell's behalf in any pool. The Hbm rows here
        // and below are unreachable under today's `tier_of`, and spelled anyway so the
        // table stays total -- Phase 6's heterogeneous fleet is what reaches them.
        (Snapshot, Hbm, Capacity) => Orchestrator,
        (Snapshot, Hbm, Allocation) => Orchestrator,
        (Snapshot, Ddr, Capacity) => Orchestrator,
        (Snapshot, Ddr, Allocation) => Orchestrator,
        (Snapshot, Nvme, Capacity) => Orchestrator,
        (Snapshot, Nvme, Allocation) => Orchestrator,

        (ServiceHeap, Hbm, Capacity) => Orchestrator,
        (ServiceHeap, Hbm, Allocation) => Orchestrator,
        (ServiceHeap, Ddr, Capacity) => Orchestrator,
        (ServiceHeap, Ddr, Allocation) => Orchestrator,
        (ServiceHeap, Nvme, Capacity) => Orchestrator,
        (ServiceHeap, Nvme, Allocation) => Orchestrator,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use Authority::{Engine, Orchestrator};
    use BlobKind::{KvBlock, ServiceHeap, Snapshot, WeightShard};
    use Question::{Allocation, Capacity};
    use Tier::{Ddr, Hbm, Nvme};

    // One test per row of owned-and-observed.md §1's "Ownership is per class" table, named
    // for the row it encodes, so the doc and the code fail together rather than drifting
    // apart (phase-1.md §4.2).

    #[test]
    fn kvblock_hbm_capacity_is_the_partition_the_orchestrator_sizes() {
        assert_eq!(authority(KvBlock, Hbm, Capacity), Orchestrator);
    }

    #[test]
    fn kvblock_hbm_allocation_is_the_engines_block_manager() {
        assert_eq!(authority(KvBlock, Hbm, Allocation), Engine);
    }

    #[test]
    fn kvblock_ddr_capacity_is_the_orchestrators_ddr_quota() {
        assert_eq!(authority(KvBlock, Ddr, Capacity), Orchestrator);
    }

    #[test]
    fn kvblock_ddr_allocation_follows_the_offload_connector() {
        assert_eq!(authority(KvBlock, Ddr, Allocation), Engine);
    }

    #[test]
    fn kvblock_nvme_capacity_is_the_orchestrators_spill_file() {
        assert_eq!(authority(KvBlock, Nvme, Capacity), Orchestrator);
    }

    #[test]
    fn kvblock_nvme_allocation_follows_the_connector_per_phase1_1_3() {
        assert_eq!(authority(KvBlock, Nvme, Allocation), Engine);
    }

    #[test]
    fn weightshard_hbm_capacity_is_orchestrator() {
        assert_eq!(authority(WeightShard, Hbm, Capacity), Orchestrator);
    }

    #[test]
    fn weightshard_hbm_allocation_is_engine_once_loaded() {
        assert_eq!(authority(WeightShard, Hbm, Allocation), Engine);
    }

    #[test]
    fn weightshard_ddr_capacity_is_orchestrator() {
        assert_eq!(authority(WeightShard, Ddr, Capacity), Orchestrator);
    }

    #[test]
    fn weightshard_ddr_allocation_is_engine() {
        assert_eq!(authority(WeightShard, Ddr, Allocation), Engine);
    }

    #[test]
    fn weightshard_nvme_capacity_is_orchestrator() {
        assert_eq!(authority(WeightShard, Nvme, Capacity), Orchestrator);
    }

    #[test]
    fn weightshard_nvme_allocation_is_engine() {
        assert_eq!(authority(WeightShard, Nvme, Allocation), Engine);
    }

    #[test]
    fn snapshot_is_orchestrator_outright_in_every_tier_and_question() {
        for tier in [Hbm, Ddr, Nvme] {
            assert_eq!(authority(Snapshot, tier, Capacity), Orchestrator);
            assert_eq!(authority(Snapshot, tier, Allocation), Orchestrator);
        }
    }

    #[test]
    fn serviceheap_is_orchestrator_outright_in_every_tier_and_question() {
        for tier in [Hbm, Ddr, Nvme] {
            assert_eq!(authority(ServiceHeap, tier, Capacity), Orchestrator);
            assert_eq!(authority(ServiceHeap, tier, Allocation), Orchestrator);
        }
    }

    /// P1, `phase-1.md` §2: capacity is `Orchestrator` in all twelve `(kind, tier)` cells.
    /// The uniformity is itself a claim worth a standalone assertion rather than only
    /// being implied by the twelve rows above passing individually.
    #[test]
    fn capacity_authority_is_uniformly_orchestrator() {
        for kind in BlobKind::ALL {
            for tier in [Hbm, Ddr, Nvme] {
                assert_eq!(
                    authority(kind, tier, Capacity),
                    Orchestrator,
                    "{kind:?} at {tier:?}"
                );
            }
        }
    }

    /// `Quota`'s census accessors dispatch on `cache::accelerated`, every `Hierarchy`
    /// entry point dispatches on this table, and nothing else holds the two together --
    /// `accelerated` is a `matches!` over two variants that happens to agree with the
    /// table today. This is what makes that agreement checked rather than assumed: a
    /// Phase 6 edit to one allocation cell turns it red, which is the moment `Quota`
    /// would otherwise start censusing a different set of classes than `Hierarchy` does.
    #[test]
    fn accelerated_is_exactly_engine_allocation_authority() {
        for kind in BlobKind::ALL {
            for tier in [Hbm, Ddr, Nvme] {
                assert_eq!(
                    crate::cache::accelerated(kind),
                    authority(kind, tier, Allocation) == Engine,
                    "{kind:?} at {tier:?}"
                );
            }
        }
    }

    /// P1's converse, stated once rather than left implicit: today the allocation
    /// question does not vary by tier, only by class. `phase-1.md` §1.2 keeps the tier
    /// axis anyway because Phase 3 and Phase 6 are expected to make specific cells of it
    /// stop agreeing -- this test is the marker that would go red on that day, which is
    /// the point of writing it now rather than after.
    #[test]
    fn allocation_authority_does_not_yet_vary_by_tier() {
        for kind in BlobKind::ALL {
            let by_tier: Vec<Authority> = [Hbm, Ddr, Nvme]
                .into_iter()
                .map(|tier| authority(kind, tier, Allocation))
                .collect();
            assert!(
                by_tier.windows(2).all(|w| w[0] == w[1]),
                "{kind:?} allocation authority varies by tier: {by_tier:?}"
            );
        }
    }
}
