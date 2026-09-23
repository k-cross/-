//! One record per placement decision, `owned-and-observed.md` §3.11 and `phase-2.md` §4.4.
//!
//! §3.11's rule is the rate, not a compromise: one span per *request* is lost in the hundreds
//! of microseconds a request costs at minimum; one span per *candidate* is not, which is why
//! `oracle.rs`'s per-candidate work stays internal to `Machine` and only its conclusion --
//! four numbers and two node ids -- is ever recorded here.
//!
//! Spans are priced at zero. The simulator charges modelled seam costs for control-plane work
//! *on the request path*; a span is instrumentation of the simulator itself, not of the system
//! it models, and charging it would put an observability cost into results that are about
//! something else -- if a later phase wants to model tracing's own cost, `boundary.rs` already
//! has the rung.
//!
//! Collected only when `Machine::set_regret(true)` is on, into `Machine::spans`, which is empty
//! and allocates nothing otherwise.

use crate::blob::BlobKind;
use crate::oracle::{Regime, Regret};

/// One decision's outcome, in the same currency (nanoseconds, node indices) `machine.rs`
/// already reports in aggregate. `phase-2.md` §4.4: `decided_by` is the per-request form of
/// the score's nested ladder, and its one exact reduction is to the *last* rung --
/// `count(decided_by == Some(4)) == Machine::moved_by_congestion`, checked in `machine.rs`'s
/// tests -- because `decided_by` holds only the single last term that changed the pick, while
/// `moved_by_displacement`/`_flow`/`_load` each count how often *their own* comparison flipped
/// regardless of what a later term did afterward. A decision can flip more than one term but
/// is decided by only one, so only the outermost rung's count and this field's count are the
/// same quantity computed twice.
#[derive(Clone, Copy, Debug)]
pub struct Span {
    /// This decision's position in `Machine`'s own op counter, for correlating a span back to
    /// a trace position without re-deriving one.
    pub op: u64,
    /// Ledger class this decision bills against -- `Request::kind_idx`'s class, not a per-blob
    /// breakdown.
    pub class: BlobKind,
    /// The node the policy actually chose -- `p` in `oracle.rs`'s decomposition.
    pub node: usize,
    /// The node the realized-cost oracle would have chosen -- `o`.
    pub oracle_node: usize,
    pub regret: Regret,
    pub regime: Regime,
    /// Index into `machine::TERM_LABELS` of the last term whose inclusion changed the scored
    /// arm's argmin, or `None` if the policy is not `Placement::Scored` or nothing after
    /// `acquire` moved it. Never set by the affinity tie-break: that is `held_by_affinity`'s
    /// own, separately tracked, phenomenon (`phase-2.md` §1.2's P1).
    pub decided_by: Option<usize>,
    /// `Cost::service_ns()` at the chosen node -- `charged(p)`, kept on the span so a reduction
    /// over spans needs no second pass over the trace to recover it.
    pub service_ns: u64,
}
