#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Observe {
    #[default]
    Dispatch,
    Completion,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fate {
    Shared,
    Held,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Client {
    Restart,
    Continue,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Retry {
    Burst,
    Backoff,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Subscriber {
    Warm,
    Cold,
    Snapshot { after_ns: u64 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Rebuild {
    Now,
    After { after_ns: u64 },
    Never,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Estimators {
    Lost,
    Kept,
    Snapshot,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Restart {
    pub fate: Fate,
    pub client: Client,
    pub outage_ns: u64,
    pub subscriber: Subscriber,
    pub estimators: Estimators,
    pub ledger: Rebuild,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Spill {
    Kept,
    Lost,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EngineCrash {
    pub node: usize,
    pub restart_ns: u64,
    pub spill: Spill,
    pub client: Client,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NodeLoss {
    pub node: usize,
    pub declare_ns: u64,
    pub client: Client,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Degrade {
    pub for_ns: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fault {
    Scheduler(Restart),
    Estimators,
    Engine(EngineCrash),
    Node(NodeLoss),
    Degrade(Degrade),
}

impl Fault {
    #[must_use]
    pub fn outage_ns(self) -> u64 {
        match self {
            Self::Scheduler(r) => r.outage_ns,
            Self::Estimators | Self::Engine(_) | Self::Node(_) | Self::Degrade(_) => 0,
        }
    }
}

pub const BACKOFF_BASE_NS: u64 = 100_000_000;

#[must_use]
pub fn retry_at(
    retry: Retry,
    nominal_ns: u64,
    outage_end_ns: u64,
    jitter: &mut impl FnMut() -> f64,
) -> u64 {
    if nominal_ns >= outage_end_ns {
        return nominal_ns;
    }
    match retry {
        Retry::Burst => outage_end_ns,
        Retry::Backoff => {
            let mut at = nominal_ns;
            let mut wait = BACKOFF_BASE_NS;
            while at < outage_end_ns {
                at += (wait as f64 * (1.0 + jitter())) as u64;
                wait *= 2;
            }
            at
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct FaultStats {
    pub restarts: u64,
    pub streams_aborted: u64,
    pub gangs_aborted: u64,
    pub lost_decode_ns: u64,
    pub kept_tokens: u64,
    pub held_through: u64,
    pub moved_from_engine: u64,
    pub router_held: u64,
    pub estimator_resets: u64,
    pub snapshot_restores: u64,
    pub snapshot_age_ns: u64,
    pub hidden_flights: u64,
    pub hidden_holders: u64,
    pub blind_admissions: u64,
    pub over_admissions: u64,
    pub refused_at_node: u64,
    pub refusal_attempts: u64,
    pub engine_crashes: u64,
    pub nodes_lost: u64,
    pub kv_lost: u64,
    pub host_lost: u64,
    pub durable_lost_with_node: u64,
    pub durable_saved: u64,
    pub durable_copied_bytes: u64,
    pub limbo_requests: u64,
    pub limbo_wait_ns: u64,
    pub degraded: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_burst_retries_at_the_outages_end_and_a_late_arrival_not_at_all() {
        let mut none = || 0.0;
        assert_eq!(retry_at(Retry::Burst, 10, 1_000, &mut none), 1_000);
        assert_eq!(retry_at(Retry::Burst, 1_000, 1_000, &mut none), 1_000);
        assert_eq!(retry_at(Retry::Burst, 2_000, 1_000, &mut none), 2_000);
    }

    #[test]
    fn a_backoff_doubles_from_the_base_until_it_clears_the_outage() {
        let mut none = || 0.0;
        let end = 1_000_000_000;
        let at = retry_at(Retry::Backoff, 0, end, &mut none);
        assert_eq!(at, 100_000_000 + 200_000_000 + 400_000_000 + 800_000_000);
        assert!(at >= end);
        let mut full = || 1.0;
        let jittered = retry_at(Retry::Backoff, 0, end, &mut full);
        assert_ne!(jittered, at);
        assert_eq!(jittered, 200_000_000 + 400_000_000 + 800_000_000);
    }

    #[test]
    fn an_outage_is_a_schedulers_and_an_estimator_reset_has_none() {
        let restart = Restart {
            fate: Fate::Held,
            client: Client::Restart,
            outage_ns: 7,
            subscriber: Subscriber::Warm,
            estimators: Estimators::Kept,
            ledger: Rebuild::Now,
        };
        assert_eq!(Fault::Scheduler(restart).outage_ns(), 7);
        assert_eq!(Fault::Estimators.outage_ns(), 0);
        let crash = EngineCrash {
            node: 0,
            restart_ns: 9,
            spill: Spill::Kept,
            client: Client::Continue,
        };
        assert_eq!(Fault::Engine(crash).outage_ns(), 0);
    }
}
