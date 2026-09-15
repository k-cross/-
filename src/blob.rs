use std::fmt;

#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BlobId([u8; 32]);

pub const ROOT: BlobId = BlobId([0; 32]);

impl BlobId {
    #[must_use]
    pub fn chain(parent: BlobId, content: &[u8]) -> Self {
        let mut h = blake3::Hasher::new();
        h.update(&parent.0);
        h.update(content);
        Self(*h.finalize().as_bytes())
    }

    #[must_use]
    pub fn leaf(content: &[u8]) -> Self {
        Self::chain(ROOT, content)
    }
}

impl fmt::Debug for BlobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for b in &self.0[..4] {
            write!(f, "{b:02x}")?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum BlobKind {
    KvBlock,
    Snapshot,
    WeightShard,
}

impl BlobKind {
    pub const ALL: [BlobKind; 3] = [BlobKind::KvBlock, BlobKind::Snapshot, BlobKind::WeightShard];

    #[must_use]
    pub fn idx(self) -> usize {
        match self {
            BlobKind::KvBlock => 0,
            BlobKind::Snapshot => 1,
            BlobKind::WeightShard => 2,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct BlobMeta {
    pub kind: BlobKind,
    pub bytes: u64,
    pub parent: Option<BlobId>,
    pub recompute_ns: u64,
}

impl BlobMeta {
    #[must_use]
    pub fn value_per_byte(&self) -> f64 {
        self.recompute_ns as f64 / self.bytes as f64
    }
}
