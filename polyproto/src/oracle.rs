use crate::cache::Cost;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Regret {
    pub total: i64,
    pub execution: i64,
    pub heuristic: i64,
    pub belief: i64,
    pub model: i64,

    pub total_with_displacement: i64,
}

#[must_use]
#[allow(clippy::similar_names)]
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

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Regime {
    Resident,

    Wait,

    Transfer,

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
