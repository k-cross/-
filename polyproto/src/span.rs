use crate::blob::BlobKind;
use crate::oracle::{Regime, Regret};

#[derive(Clone, Copy, Debug)]
pub struct Span {
    pub op: u64,

    pub class: BlobKind,

    pub node: usize,

    pub oracle_node: usize,
    pub regret: Regret,
    pub regime: Regime,

    pub decided_by: Option<usize>,

    pub service_ns: u64,
}
