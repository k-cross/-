use crate::blob::{BlobId, BlobMeta};

#[derive(Clone, Debug)]
pub struct FlowHint {
    pub task: u64,
    pub template_len: usize,
    pub downstream: Vec<(BlobId, BlobMeta)>,
    pub probability: f64,

    #[allow(dead_code)]
    pub lead_ops: u32,

    pub payload_bytes: u64,
}

impl FlowHint {
    #[must_use]
    pub fn templated(&self) -> Self {
        Self {
            task: self.task,
            template_len: self.template_len,
            downstream: self
                .downstream
                .iter()
                .take(self.template_len)
                .copied()
                .collect(),
            probability: self.probability,
            lead_ops: self.lead_ops,
            payload_bytes: self.payload_bytes,
        }
    }

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
