use crate::blob::{BlobId, BlobMeta};

#[derive(Clone, Debug)]
pub struct FlowHint {
    pub task: u64,
    pub downstream: Vec<(BlobId, BlobMeta)>,
    pub probability: f64,

    #[allow(dead_code)]
    pub lead_ops: u32,

    pub payload_bytes: u64,
}

impl FlowHint {
    #[must_use]
    pub fn missing_bytes(&self, resident: impl Fn(&BlobId) -> bool) -> u64 {
        self.downstream
            .iter()
            .filter(|(id, _)| !resident(id))
            .map(|(_, m)| m.bytes)
            .sum()
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum FlowMode {
    Blind,

    Announce,

    Gate,
}
