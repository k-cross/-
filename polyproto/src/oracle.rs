//! The four-gap regret decomposition and the acquisition regime, `owned-and-observed.md` §3.4
//! and §3.5, designed in `phase-2.md`. Pure types and math: the realized-cost computation that
//! feeds them needs `Machine`'s private state and lives in `machine.rs` as `Machine::oracle_pick`
//! and `Machine::finish_regret`, which call `decompose` here once the four picks are known.
//!
//! `Regret` is signed and never clipped (`phase-2.md` §1.2, §3.9): a component summing to a
//! negative number is a real finding -- a heuristic that beat the model's own argmin, or a
//! clairvoyant baseline that lost on cost (§1.7) -- not an error to floor at zero.

use crate::cache::Cost;

/// `charged(p) - R(p) - R(m_b) - R(m_t) - R(o)`, decomposed so each of the four differences
/// stays attributable to one cause rather than blended into a single number. `phase-2.md` §1.2:
///
/// - `execution`  = `charged - R(p)`      -- a plan made on a stale view, executed against truth
/// - `heuristic`  = `R(p) - R(m_b)`       -- the policy is not an argmin over its own belief
/// - `belief`     = `R(m_b) - R(m_t)`     -- the argmin was taken over a stale view
/// - `model`      = `R(m_t) - R(o)`       -- the score's cost function is not the realized charge
///
/// They telescope: `execution + heuristic + belief + model == total`. Under `Control::Unified`
/// and `Control::Query` today, `execution` and `belief` are provably zero -- there is no stale
/// view to diverge from -- and Phase 4's lossy telemetry is what first makes them nonzero.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Regret {
    pub total: i64,
    pub execution: i64,
    pub heuristic: i64,
    pub belief: i64,
    pub model: i64,
    /// `total`, against the oracle argmin when displacement is priced as a realized cost too.
    /// `Terms::displaced` is billed to nobody, so it is excluded from every other field here;
    /// this is the metric's own reported sensitivity to that one term, per `phase-2.md` §1.3 --
    /// never blended into `total`, always read beside it.
    pub total_with_displacement: i64,
}

/// The four realized-cost picks, turned into a `Regret`. `phase-2.md` §4.2's "one function that
/// takes the four picks and returns the decomposition" -- `Machine` computes `r_p`..`r_o_disp`
/// against its own private state and calls this once it has them.
#[must_use]
#[allow(
    clippy::similar_names,
    reason = "r_p/r_mb/r_mt/r_o are phase-2.md §1.2's own notation -- renaming them apart from \
              the design doc's math would cost more clarity than it buys"
)]
pub(crate) fn decompose(
    charged: u64,
    r_p: u64,
    r_mb: u64,
    r_mt: u64,
    r_o: u64,
    r_o_disp: u64,
) -> Regret {
    let i = |x: u64| -> i64 { i64::try_from(x).unwrap_or(i64::MAX) };
    let (charged, r_p, r_mb, r_mt, r_o, r_o_disp) =
        (i(charged), i(r_p), i(r_mb), i(r_mt), i(r_o), i(r_o_disp));
    Regret {
        total: charged - r_o,
        execution: charged - r_p,
        heuristic: r_p - r_mb,
        belief: r_mb - r_mt,
        model: r_mt - r_o,
        total_with_displacement: charged - r_o_disp,
    }
}

/// How a served request's state was actually acquired, exclusive and exhaustive over the same
/// requests the warm/cold split already covers -- `owned-and-observed.md` §3.5, `phase-2.md`
/// §1.10. Kept apart from `Machine::fetches`/`rebuilds`, which count *materialisations* (a
/// request can fetch two blobs and rebuild a third); this counts *requests*, one bucket each,
/// so the four percentages sum to 100 the way the materialisation counters cannot.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Regime {
    /// Nothing had to be acquired and nothing queued: the chain and its dependencies were
    /// already resident, and the engine had a free slot.
    Resident,
    /// The largest cost was time spent waiting for a decode slot.
    Wait,
    /// The largest cost was time spent moving state over a link from a peer.
    Transfer,
    /// The largest cost was time spent rebuilding state locally.
    Recompute,
}

pub const REGIME_COUNT: usize = 4;

impl Regime {
    pub const ALL: [Self; REGIME_COUNT] =
        [Self::Resident, Self::Wait, Self::Transfer, Self::Recompute];

    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Resident => "resident",
            Self::Wait => "wait",
            Self::Transfer => "transfer",
            Self::Recompute => "recompute",
        }
    }

    #[must_use]
    pub fn idx(self) -> usize {
        match self {
            Self::Resident => 0,
            Self::Wait => 1,
            Self::Transfer => 2,
            Self::Recompute => 3,
        }
    }
}

/// Classify one served request's regime from the `Cost` the simulator already charged it --
/// argmax of `(queue_ns, transfer_ns, recompute_ns)`, `Resident` when all three are zero.
/// Ties are broken in that fixed order (queue, then transfer, then recompute), so the result
/// is deterministic and does not depend on iteration order anywhere.
#[must_use]
pub fn classify(cost: &Cost) -> Regime {
    if cost.queue_ns == 0 && cost.transfer_ns == 0 && cost.recompute_ns == 0 {
        return Regime::Resident;
    }
    if cost.queue_ns >= cost.transfer_ns && cost.queue_ns >= cost.recompute_ns {
        Regime::Wait
    } else if cost.transfer_ns >= cost.recompute_ns {
        Regime::Transfer
    } else {
        Regime::Recompute
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decompose_telescopes_to_total() {
        let r = decompose(1_000, 900, 850, 800, 700, 650);
        assert_eq!(
            r.execution + r.heuristic + r.belief + r.model,
            r.total,
            "{r:?}"
        );
        assert_eq!(r.total, 1_000 - 700);
        assert_eq!(r.total_with_displacement, 1_000 - 650);
    }

    #[test]
    fn decompose_allows_negative_components() {
        // A heuristic that beats the model's own belief-argmin: r_p < r_mb.
        let r = decompose(1_000, 700, 900, 900, 900, 900);
        assert!(r.heuristic < 0, "{r:?}");
        assert_eq!(r.execution + r.heuristic + r.belief + r.model, r.total);
    }

    #[test]
    fn classify_resident_when_nothing_acquired_or_queued() {
        let cost = Cost::default();
        assert_eq!(classify(&cost), Regime::Resident);
    }

    #[test]
    fn classify_picks_the_largest_term_with_queue_wait_recompute_priority_on_ties() {
        let mut cost = Cost {
            queue_ns: 5,
            transfer_ns: 5,
            recompute_ns: 5,
            ..Cost::default()
        };
        assert_eq!(classify(&cost), Regime::Wait);
        cost.queue_ns = 0;
        assert_eq!(classify(&cost), Regime::Transfer);
        cost.transfer_ns = 0;
        assert_eq!(classify(&cost), Regime::Recompute);
        cost.recompute_ns = 10;
        cost.transfer_ns = 20;
        cost.queue_ns = 1;
        assert_eq!(classify(&cost), Regime::Transfer);
    }
}
