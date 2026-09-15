use crate::blob::{BlobId, BlobMeta};

/// A declared or learned dependency from one stage of a task to a later stage in a
/// *different* workload. The orchestrator sees the whole task; neither workload does.
#[derive(Clone, Debug)]
pub struct FlowHint {
    pub task: u64,
    pub downstream: Vec<(BlobId, BlobMeta)>,
    pub probability: f64,
    /// Observed delay from upstream to downstream. Currently informational: it is the window
    /// an anticipatory boost should decay over once the ledger is time-aware.
    #[allow(
        dead_code,
        reason = "carried by the hint; consumed when scoring becomes time-aware"
    )]
    pub lead_ops: u32,
    /// Intermediate result handed from the upstream stage to the downstream one. If the two
    /// stages run in different domains this crosses the interconnect, and it is the only
    /// cost co-placement can actually remove.
    pub payload_bytes: u64,
}

impl FlowHint {
    /// Bytes of the downstream working set not yet resident, i.e. what the task will still
    /// have to materialise after this stage completes.
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
    /// Flows are invisible: each workload is scheduled on its own arrivals only.
    Blind,
    /// Downstream state is valued before it is requested (prewarm, in the ledger's currency).
    Announce,
    /// Also refuse an upstream stage whose downstream cannot be made resident.
    Gate,
}
