use crate::blob::BlobKind;
use crate::tier::Tier;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Authority {
    Orchestrator,
    Engine,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Question {
    Capacity,
    Allocation,
}

#[must_use]
#[allow(clippy::match_same_arms)]
pub fn authority(kind: BlobKind, tier: Tier, q: Question) -> Authority {
    use Authority::{Engine, Orchestrator};
    use BlobKind::{KvBlock, ServiceHeap, Snapshot, WeightShard};
    use Question::{Allocation, Capacity};
    use Tier::{Ddr, Hbm, Nvme};

    match (kind, tier, q) {
        (KvBlock, Hbm, Capacity) => Orchestrator,
        (KvBlock, Hbm, Allocation) => Engine,
        (KvBlock, Ddr, Capacity) => Orchestrator,
        (KvBlock, Ddr, Allocation) => Engine,
        (KvBlock, Nvme, Capacity) => Orchestrator,
        (KvBlock, Nvme, Allocation) => Engine,

        (WeightShard, Hbm, Capacity) => Orchestrator,
        (WeightShard, Hbm, Allocation) => Engine,
        (WeightShard, Ddr, Capacity) => Orchestrator,
        (WeightShard, Ddr, Allocation) => Engine,
        (WeightShard, Nvme, Capacity) => Orchestrator,
        (WeightShard, Nvme, Allocation) => Engine,

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
