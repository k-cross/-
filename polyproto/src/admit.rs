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

const LENGTH_BINS: usize = 2048;

#[derive(Clone, Debug)]
pub struct Lengths {
    bins: Vec<u64>,
    seen: u64,
}

impl Default for Lengths {
    fn default() -> Self {
        Self {
            bins: vec![0; LENGTH_BINS],
            seen: 0,
        }
    }
}

impl Lengths {
    pub fn record(&mut self, tokens: u64) {
        self.bins[usize::try_from(tokens).map_or(LENGTH_BINS - 1, |t| t.min(LENGTH_BINS - 1))] += 1;
        self.seen += 1;
    }

    #[must_use]
    pub fn bytes(&self) -> u64 {
        self.bins.len() as u64 * 8
    }

    #[must_use]
    pub fn seen(&self) -> u64 {
        self.seen
    }

    #[must_use]
    pub fn quantile(&self, q: f64) -> Option<u64> {
        Self::quantile_of(self.bins.iter().copied(), self.seen, q)
    }

    #[must_use]
    pub fn pooled_quantile(&self, other: &Self, q: f64) -> Option<u64> {
        let bins = self.bins.iter().zip(&other.bins).map(|(a, b)| a + b);
        Self::quantile_of(bins, self.seen + other.seen, q)
    }

    fn quantile_of(bins: impl Iterator<Item = u64>, seen: u64, q: f64) -> Option<u64> {
        if seen == 0 {
            return None;
        }
        let target = ((q * seen as f64).ceil() as u64).max(1);
        let mut cumulative = 0;
        for (tokens, count) in bins.enumerate() {
            cumulative += count;
            if cumulative >= target {
                return Some(tokens as u64);
            }
        }
        Some((LENGTH_BINS - 1) as u64)
    }

    #[must_use]
    pub fn mean(&self) -> Option<u64> {
        if self.seen == 0 {
            return None;
        }
        let sum: u64 = self
            .bins
            .iter()
            .enumerate()
            .map(|(tokens, count)| tokens as u64 * count)
            .sum();
        Some(sum / self.seen)
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
    commits: u64,
    releases: u64,
}

impl Reservations {
    #[must_use]
    pub fn holders(&self) -> usize {
        self.held.len()
    }

    #[must_use]
    pub fn writes(&self) -> (u64, u64) {
        (self.commits, self.releases)
    }

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
        let claim = reserve.claim(req, tokens_per_block);
        self.admits_claim(capacity, req, (&claim.0, claim.1), staged)
    }

    #[must_use]
    pub fn admits_claim(
        &self,
        capacity: u64,
        req: &Request,
        claim: (&[(BlobId, BlobMeta)], u64),
        staged: Option<(&HashSet<BlobId>, u64)>,
    ) -> bool {
        if !req.chain.iter().any(|(_, m)| m.kind == BlobKind::KvBlock) {
            return true;
        }
        let (blocks, extra) = claim;
        let (skip, pending) = staged.map_or((None, 0), |(ids, bytes)| (Some(ids), bytes));
        self.committed() + pending + self.uncovered(blocks, skip) + extra <= capacity
    }

    #[must_use]
    pub fn deficit_claim(
        &self,
        capacity: u64,
        req: &Request,
        claim: (&[(BlobId, BlobMeta)], u64),
    ) -> u64 {
        if !req.chain.iter().any(|(_, m)| m.kind == BlobKind::KvBlock) {
            return 0;
        }
        let (blocks, extra) = claim;
        (self.committed() + self.uncovered(blocks, None) + extra).saturating_sub(capacity)
    }

    #[must_use]
    pub fn exclusive_bytes(&self, seq: u64) -> u64 {
        let Some((ids, output)) = self.held.get(&seq) else {
            return 0;
        };
        output
            + ids
                .iter()
                .filter_map(|id| self.covered.get(id))
                .filter(|(holders, _)| *holders == 1)
                .map(|(_, bytes)| *bytes)
                .sum::<u64>()
    }

    pub fn cancel(&mut self, seq: u64) -> u64 {
        let before = self.committed();
        self.drop_holder(seq);
        before - self.committed()
    }

    fn drop_holder(&mut self, seq: u64) {
        let Some((ids, output)) = self.held.remove(&seq) else {
            return;
        };
        self.releases += 1;
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

    pub fn clear(&mut self) {
        *self = Self {
            next: self.next,
            commits: self.commits,
            releases: self.releases,
            ..Self::default()
        };
    }

    pub fn commit(&mut self, blocks: &[(BlobId, BlobMeta)], output: u64, until: u64) -> u64 {
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
        self.commits += 1;
        self.held.insert(seq, (ids, output));
        self.inflight.push(Reverse((until, seq)));
        seq
    }

    pub fn release(&mut self, now_ns: u64) {
        while let Some(&Reverse((end, seq))) = self.inflight.peek() {
            if end > now_ns {
                break;
            }
            self.inflight.pop();
            self.drop_holder(seq);
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
    fn lengths_report_the_quantile_and_mean_of_what_was_recorded() {
        let mut l = Lengths::default();
        assert_eq!(l.quantile(0.9), None);
        for t in 1..=100 {
            l.record(t);
        }
        assert_eq!(l.quantile(0.9), Some(90));
        assert_eq!(l.quantile(1.0), Some(100));
        assert_eq!(l.mean(), Some(50));
        l.record(1_000_000);
        assert_eq!(
            l.quantile(1.0),
            Some(2047),
            "an outsized length lands in the last bin"
        );
        assert_eq!(l.seen(), 101);
        let empty = Lengths::default();
        assert_eq!(empty.pooled_quantile(&l, 0.9), l.quantile(0.9));
        let mut other = Lengths::default();
        for t in 301..=400 {
            other.record(t);
        }
        assert_eq!(other.pooled_quantile(&l, 0.5), Some(301));
        assert_eq!(other.pooled_quantile(&l, 0.4), Some(81));
    }

    #[test]
    fn a_claim_admits_by_the_bytes_it_names() {
        let (a, b) = (block(b"a"), block(b"b"));
        let r = Reservations::default();
        let req = crate::work::Request {
            phase: 0,
            chain: vec![a, b],
            requires: Vec::new(),
            hint: None,
            completes: None,
            exec_ns: 0,
            tokens: 10,
            gang: None,
            produces: Vec::new(),
            max_tokens: 0,
            slo: crate::work::Slo::Interactive,
            retention: crate::work::Retention::default(),
            concurrent: false,
            tenant: None,
            program: 0,
            pattern: crate::work::Pattern::Plain,
            authority: crate::work::Authority::ReadOnly,
            root: 0,
            tool: false,
        };
        assert!(r.admits_claim(25, &req, (&req.chain, 5), None));
        assert!(!r.admits_claim(24, &req, (&req.chain, 5), None));
    }

    #[test]
    fn a_cancelled_reservation_returns_its_exclusive_bytes_and_leaves_the_shared_ones() {
        let (a, b, c) = (block(b"a"), block(b"b"), block(b"c"));
        let mut r = Reservations::default();
        let first = r.commit(&[a, b], 5, 100);
        let second = r.commit(&[b, c], 0, 200);
        assert_eq!(r.committed(), 35);
        assert_eq!(
            r.exclusive_bytes(first),
            15,
            "a and the output, not the shared b"
        );
        assert_eq!(r.exclusive_bytes(second), 10);
        assert_eq!(r.cancel(first), 15);
        assert_eq!(r.committed(), 20);
        assert_eq!(r.cancel(first), 0, "a cancel is idempotent");
        r.release(100);
        assert_eq!(
            r.committed(),
            20,
            "the cancelled holder is not released twice"
        );
        assert_eq!(r.cancel(second), 20);
        assert_eq!(r.committed(), 0);
    }

    #[test]
    fn a_cleared_ledger_keeps_its_numbering_so_an_old_handle_names_no_new_holder() {
        let (a, b) = (block(b"a"), block(b"b"));
        let mut r = Reservations::default();
        let old = r.commit(&[a], 0, 100);
        r.clear();
        assert_eq!(r.committed(), 0);
        let new = r.commit(&[b], 0, 100);
        assert_ne!(old, new);
        assert_eq!(r.cancel(old), 0);
        assert_eq!(r.committed(), 10);
    }

    #[test]
    fn a_deficit_is_what_a_claim_lacks_and_zero_when_it_fits() {
        let (a, b) = (block(b"a"), block(b"b"));
        let mut r = Reservations::default();
        r.commit(&[a], 0, 100);
        let req = crate::work::Request {
            phase: 0,
            chain: vec![b],
            requires: Vec::new(),
            hint: None,
            completes: None,
            exec_ns: 0,
            tokens: 10,
            gang: None,
            produces: Vec::new(),
            max_tokens: 0,
            slo: crate::work::Slo::Interactive,
            retention: crate::work::Retention::default(),
            concurrent: false,
            tenant: None,
            program: 0,
            pattern: crate::work::Pattern::Plain,
            authority: crate::work::Authority::ReadOnly,
            root: 0,
            tool: false,
        };
        assert_eq!(r.deficit_claim(100, &req, (&req.chain, 0)), 0);
        assert_eq!(r.deficit_claim(15, &req, (&req.chain, 0)), 5);
        assert_eq!(r.deficit_claim(15, &req, (&req.chain, 7)), 12);
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
