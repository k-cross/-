use crate::stream::{KvEvent, Medium};

pub const KV_EVENT_KINDS: usize = 7;
pub const KV_EVENT_LABELS: [&str; KV_EVENT_KINDS] = [
    "stored, GPU",
    "stored, CPU offload",
    "stored, NVMe spill",
    "removed, GPU",
    "removed, CPU offload",
    "removed, NVMe spill",
    "cleared",
];

pub const LEASE_RENEW_S: f64 = 10.0;

#[derive(Clone, Copy, Debug, Default)]
pub struct Counted {
    pub flights_opened: u64,
    pub flights_closed: u64,
    pub router_served: u64,
    pub engine_started: u64,
    pub flow_graph: u64,
    pub length_observations: u64,
    pub snapshots: u64,
    pub snapshot_bytes: u64,
    pub kv_events: [u64; KV_EVENT_KINDS],
}

impl Counted {
    pub fn count_events(&mut self, events: &[KvEvent]) {
        for event in events {
            let slot = match *event {
                KvEvent::Stored { medium, .. } => Self::medium_slot(medium),
                KvEvent::Removed { medium, .. } => 3 + Self::medium_slot(medium),
                KvEvent::Cleared => 6,
            };
            self.kv_events[slot] += 1;
        }
    }

    fn medium_slot(medium: Medium) -> usize {
        match medium {
            Medium::Gpu => 0,
            Medium::Cpu => 1,
            Medium::Storage => 2,
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Writes {
    pub decisions: u64,
    pub dispatches: u64,
    pub reservations_committed: u64,
    pub reservations_released: u64,
    pub flights_opened: u64,
    pub flights_closed: u64,
    pub queue_enqueued: u64,
    pub queue_served: u64,
    pub engine_enqueued: u64,
    pub engine_started: u64,
    pub cancels: u64,
    pub refusals: u64,
    pub flow_graph: u64,
    pub fanouts_staged: u64,
    pub host_admissions: u64,
    pub host_evictions: u64,
    pub weight_loads: u64,
    pub length_observations: u64,
    pub planner_moves: u64,
    pub snapshots: u64,
    pub snapshot_bytes: u64,
    pub kv_events: [u64; KV_EVENT_KINDS],
}

impl Writes {
    #[must_use]
    pub fn owned(&self) -> u64 {
        self.decisions
            + self.dispatches
            + self.reservations_committed
            + self.reservations_released
            + self.flights_opened
            + self.flights_closed
            + self.queue_enqueued
            + self.queue_served
            + self.engine_enqueued
            + self.engine_started
            + self.cancels
            + self.refusals
            + self.flow_graph
            + self.fanouts_staged
            + self.host_admissions
            + self.host_evictions
    }

    #[must_use]
    pub fn kv_total(&self) -> u64 {
        self.kv_events.iter().sum()
    }

    #[must_use]
    pub fn kv_removals(&self) -> u64 {
        self.kv_events[3] + self.kv_events[4]
    }
}

#[must_use]
pub fn liveness_writes(nodes: usize, seconds: f64, renew_s: f64) -> f64 {
    nodes as f64 * seconds / renew_s.max(f64::MIN_POSITIVE)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blob::BlobId;

    fn stored(medium: Medium) -> KvEvent {
        KvEvent::Stored {
            id: BlobId::leaf(b"a"),
            bytes: 1,
            medium,
            mark: None,
        }
    }

    fn removed(medium: Medium) -> KvEvent {
        KvEvent::Removed {
            id: BlobId::leaf(b"a"),
            medium,
        }
    }

    #[test]
    fn every_event_lands_in_exactly_one_slot_by_type_and_tier() {
        let mut counted = Counted::default();
        let events = [
            stored(Medium::Gpu),
            stored(Medium::Gpu),
            stored(Medium::Cpu),
            stored(Medium::Storage),
            removed(Medium::Gpu),
            removed(Medium::Cpu),
            removed(Medium::Storage),
            KvEvent::Cleared,
        ];
        counted.count_events(&events);
        assert_eq!(counted.kv_events, [2, 1, 1, 1, 1, 1, 1]);
        assert_eq!(counted.kv_events.iter().sum::<u64>(), events.len() as u64);
    }

    #[test]
    fn owned_changes_exclude_the_inferred_stream_and_the_record() {
        let writes = Writes {
            decisions: 3,
            dispatches: 4,
            reservations_committed: 1,
            reservations_released: 1,
            kv_events: [100; KV_EVENT_KINDS],
            planner_moves: 9,
            length_observations: 50,
            weight_loads: 5,
            ..Writes::default()
        };
        assert_eq!(writes.owned(), 9);
        assert_eq!(writes.kv_total(), 700);
        assert_eq!(writes.kv_removals(), 200);
    }

    #[test]
    fn liveness_is_a_write_per_node_per_renewal() {
        assert!((liveness_writes(8, 100.0, 10.0) - 80.0).abs() < 1e-9);
        assert!((liveness_writes(10_000, 1.0, LEASE_RENEW_S) - 1_000.0).abs() < 1e-9);
    }
}
