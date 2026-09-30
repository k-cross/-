use std::collections::HashMap;

use crate::blob::BlobId;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Medium {
    Gpu,
    Cpu,
    Storage,
}

impl Medium {
    pub const ALL: [Self; 3] = [Self::Gpu, Self::Cpu, Self::Storage];

    #[must_use]
    pub fn idx(self) -> usize {
        match self {
            Self::Gpu => 0,
            Self::Cpu => 1,
            Self::Storage => 2,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Rank {
    EvictFirst,
    Retain,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Mark {
    pub rank: Rank,
    pub until_ns: u64,
}

impl Mark {
    #[must_use]
    pub fn live_at(self, now_ns: u64) -> bool {
        self.until_ns > now_ns
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KvEvent {
    Stored {
        id: BlobId,
        bytes: u64,
        medium: Medium,
        mark: Option<Mark>,
    },
    Removed {
        id: BlobId,
        medium: Medium,
    },
    Cleared,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Index {
    tiers: [HashMap<BlobId, u64>; 3],
    bytes: [u64; 3],
    marks: HashMap<BlobId, Mark>,
}

impl Index {
    pub fn apply(&mut self, event: &KvEvent) {
        match *event {
            KvEvent::Stored {
                id,
                bytes,
                medium,
                mark,
            } => {
                let m = medium.idx();
                if let Some(old) = self.tiers[m].insert(id, bytes) {
                    self.bytes[m] -= old;
                }
                self.bytes[m] += bytes;
                if let (Some(mark), Medium::Gpu) = (mark, medium) {
                    self.marks.insert(id, mark);
                }
            }
            KvEvent::Removed { id, medium } => {
                let m = medium.idx();
                if let Some(old) = self.tiers[m].remove(&id) {
                    self.bytes[m] -= old;
                }
                if medium == Medium::Gpu {
                    self.marks.remove(&id);
                }
            }
            KvEvent::Cleared => {
                for tier in &mut self.tiers {
                    tier.clear();
                }
                self.bytes = [0; 3];
                self.marks.clear();
            }
        }
    }

    pub fn ids(&self, medium: Medium) -> impl Iterator<Item = &BlobId> {
        self.tiers[medium.idx()].keys()
    }

    #[must_use]
    pub fn contains(&self, medium: Medium, id: &BlobId) -> bool {
        self.tiers[medium.idx()].contains_key(id)
    }

    #[must_use]
    pub fn len(&self, medium: Medium) -> usize {
        self.tiers[medium.idx()].len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.tiers.iter().all(HashMap::is_empty)
    }

    #[must_use]
    pub fn mark(&self, id: &BlobId) -> Option<Mark> {
        self.marks.get(id).copied()
    }

    pub fn marks(&self) -> impl Iterator<Item = (&BlobId, &Mark)> {
        self.marks.iter()
    }

    #[must_use]
    pub fn live_marks(&self, now_ns: u64) -> Vec<(BlobId, Mark)> {
        let mut live: Vec<(BlobId, Mark)> = self
            .marks
            .iter()
            .filter(|(_, m)| m.live_at(now_ns))
            .map(|(id, m)| (*id, *m))
            .collect();
        live.sort_unstable_by_key(|(id, _)| *id);
        live
    }

    #[must_use]
    pub fn contains_any(&self, id: &BlobId) -> bool {
        self.tiers.iter().any(|t| t.contains_key(id))
    }

    #[must_use]
    pub fn bytes(&self, medium: Medium) -> u64 {
        self.bytes[medium.idx()]
    }

    #[must_use]
    pub fn sorted_ids(&self, medium: Medium) -> Vec<BlobId> {
        let mut ids: Vec<BlobId> = self.tiers[medium.idx()].keys().copied().collect();
        ids.sort_unstable();
        ids
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn events_replay_into_the_state_they_describe() {
        let (a, b) = (BlobId::leaf(b"a"), BlobId::leaf(b"b"));
        let mut index = Index::default();
        index.apply(&KvEvent::Stored {
            id: a,
            bytes: 10,
            medium: Medium::Gpu,
            mark: None,
        });
        index.apply(&KvEvent::Stored {
            id: b,
            bytes: 20,
            medium: Medium::Gpu,
            mark: None,
        });
        index.apply(&KvEvent::Removed {
            id: a,
            medium: Medium::Gpu,
        });
        index.apply(&KvEvent::Stored {
            id: a,
            bytes: 10,
            medium: Medium::Cpu,
            mark: None,
        });
        assert!(!index.contains(Medium::Gpu, &a));
        assert!(index.contains(Medium::Cpu, &a));
        assert_eq!(index.bytes(Medium::Gpu), 20);
        index.apply(&KvEvent::Cleared);
        assert!(index.is_empty());
    }

    #[test]
    fn a_mark_rides_a_gpu_store_until_the_block_leaves_the_gpu() {
        let a = BlobId::leaf(b"a");
        let retain = Mark {
            rank: Rank::Retain,
            until_ns: 100,
        };
        let mut index = Index::default();
        index.apply(&KvEvent::Stored {
            id: a,
            bytes: 10,
            medium: Medium::Gpu,
            mark: None,
        });
        assert_eq!(index.mark(&a), None);
        index.apply(&KvEvent::Stored {
            id: a,
            bytes: 10,
            medium: Medium::Gpu,
            mark: Some(retain),
        });
        assert_eq!(index.mark(&a), Some(retain));
        assert_eq!(index.live_marks(99), vec![(a, retain)]);
        assert!(index.live_marks(100).is_empty());
        index.apply(&KvEvent::Stored {
            id: a,
            bytes: 10,
            medium: Medium::Cpu,
            mark: None,
        });
        assert_eq!(
            index.mark(&a),
            Some(retain),
            "a colder copy leaves the mark"
        );
        index.apply(&KvEvent::Removed {
            id: a,
            medium: Medium::Gpu,
        });
        assert_eq!(index.mark(&a), None);
    }

    #[test]
    fn evict_first_ranks_below_retain() {
        assert!(Rank::EvictFirst < Rank::Retain);
    }
}
