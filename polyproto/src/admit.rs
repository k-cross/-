use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};

use crate::blob::{BlobId, BlobKind, BlobMeta};
use crate::work::{KV_BLOCK_BYTES, Request};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Reserve {
    Bound,
    Perfect,
    Prompt,
}

impl Reserve {
    pub const ALL: [Self; 3] = [Self::Bound, Self::Perfect, Self::Prompt];

    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Bound => "bound",
            Self::Perfect => "perfect",
            Self::Prompt => "none",
        }
    }

    #[must_use]
    pub fn claim(self, req: &Request, tokens_per_block: u64) -> (Vec<(BlobId, BlobMeta)>, u64) {
        let kv = |c: &[(BlobId, BlobMeta)]| {
            c.iter()
                .filter(|(_, m)| m.kind == BlobKind::KvBlock)
                .copied()
                .collect::<Vec<_>>()
        };
        match self {
            Self::Prompt => (kv(&req.chain), 0),
            Self::Perfect => {
                let mut blocks = kv(&req.chain);
                blocks.extend(kv(&req.produces));
                (blocks, 0)
            }
            Self::Bound => (
                kv(&req.chain),
                req.max_tokens.div_ceil(tokens_per_block.max(1)) * KV_BLOCK_BYTES,
            ),
        }
    }
}

#[derive(Debug, Default)]
pub struct Reservations {
    covered: HashMap<BlobId, (u32, u64)>,
    covered_bytes: u64,
    output_bytes: u64,
    inflight: BinaryHeap<Reverse<(u64, u64)>>,
    held: HashMap<u64, (Vec<BlobId>, u64)>,
    next: u64,
}

impl Reservations {
    #[must_use]
    pub fn committed(&self) -> u64 {
        self.covered_bytes + self.output_bytes
    }

    #[must_use]
    pub fn uncovered(
        &self,
        blocks: &[(BlobId, BlobMeta)],
        staged: Option<&HashSet<BlobId>>,
    ) -> u64 {
        let mut seen = HashSet::new();
        blocks
            .iter()
            .filter(|(id, _)| {
                !self.covered.contains_key(id)
                    && staged.is_none_or(|s| !s.contains(id))
                    && seen.insert(*id)
            })
            .map(|(_, m)| m.bytes)
            .sum()
    }

    #[must_use]
    pub fn admits(
        &self,
        capacity: u64,
        req: &Request,
        reserve: Reserve,
        tokens_per_block: u64,
        staged: Option<(&HashSet<BlobId>, u64)>,
    ) -> bool {
        if !req.chain.iter().any(|(_, m)| m.kind == BlobKind::KvBlock) {
            return true;
        }
        let (blocks, extra) = reserve.claim(req, tokens_per_block);
        let (skip, pending) = staged.map_or((None, 0), |(ids, bytes)| (Some(ids), bytes));
        self.committed() + pending + self.uncovered(&blocks, skip) + extra <= capacity
    }

    pub fn commit(&mut self, blocks: &[(BlobId, BlobMeta)], output: u64, until: u64) {
        let mut ids = Vec::with_capacity(blocks.len());
        for &(id, meta) in blocks {
            let e = self.covered.entry(id).or_insert((0, meta.bytes));
            if e.0 == 0 {
                self.covered_bytes += meta.bytes;
            }
            e.0 += 1;
            ids.push(id);
        }
        self.output_bytes += output;
        let seq = self.next;
        self.next += 1;
        self.held.insert(seq, (ids, output));
        self.inflight.push(Reverse((until, seq)));
    }

    pub fn release(&mut self, now_ns: u64) {
        while let Some(&Reverse((end, seq))) = self.inflight.peek() {
            if end > now_ns {
                break;
            }
            self.inflight.pop();
            let Some((ids, output)) = self.held.remove(&seq) else {
                continue;
            };
            self.output_bytes -= output;
            for id in ids {
                if let Some(e) = self.covered.get_mut(&id) {
                    e.0 -= 1;
                    if e.0 == 0 {
                        self.covered_bytes -= e.1;
                        self.covered.remove(&id);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block(tag: &[u8]) -> (BlobId, BlobMeta) {
        (
            BlobId::leaf(tag),
            BlobMeta {
                kind: BlobKind::KvBlock,
                bytes: 10,
                parent: None,
                recompute_ns: 1,
            },
        )
    }

    #[test]
    fn a_shared_block_is_reserved_once_and_released_with_its_last_holder() {
        let (a, b, c) = (block(b"a"), block(b"b"), block(b"c"));
        let mut r = Reservations::default();
        r.commit(&[a, b], 5, 100);
        assert_eq!(r.committed(), 25);
        assert_eq!(r.uncovered(&[a, b, c], None), 10, "only c is new");
        r.commit(&[b, c], 0, 200);
        assert_eq!(r.committed(), 35);
        r.release(100);
        assert_eq!(r.committed(), 20, "b and c are still held by the second");
        r.release(200);
        assert_eq!(r.committed(), 0);
    }
}
