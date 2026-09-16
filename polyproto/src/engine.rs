//! Continuous batching, which is why a decode step has no fixed cost.
//!
//! A serving engine runs every resident sequence through one step at a time. The step reads
//! the weights once whatever the batch size, so a second sequence is nearly free and the
//! sixty-fourth is not free at all: per-token latency rises with occupancy while throughput
//! saturates. A scheduler that models decode as a constant cannot see the only tradeoff
//! inference routing exists to make -- send work to the node holding its prefix, or to the
//! node that is not already full.
//!
//! Constants are **modelled**. `STEP_BASE_NS` is the weight-read floor of a step and
//! `STEP_PER_SEQ_NS` the attention and KV-read cost each extra sequence adds; the base is
//! chosen so a batch of one matches the flat 125 tok/s the workload used before, which keeps
//! the unbatched arm comparable. Replace both from a real engine before quoting a result.

use std::cmp::Reverse;
use std::collections::BinaryHeap;

pub const STEP_BASE_NS: u64 = 7_000_000;
pub const STEP_PER_SEQ_NS: u64 = 40_000;
pub const MAX_BATCH: usize = 64;
/// Utilisation at which the congestion toll stops growing. The toll diverges at full
/// occupancy, and a finite cap keeps a saturated node expensive rather than infinite, so an
/// argmin over a cluster where *every* node is full still has an answer.
const UTILISATION_CAP: f64 = 0.95;

/// What one decode admission cost: time spent waiting for a slot, time spent decoding, and
/// the batch it landed in.
#[derive(Clone, Copy, Debug)]
pub struct Decode {
    pub queue_ns: u64,
    pub exec_ns: u64,
    pub batch: usize,
}

#[derive(Debug)]
pub struct Engine {
    max_batch: usize,
    /// Completion times of sequences still occupying a slot, soonest first.
    inflight: BinaryHeap<Reverse<u64>>,
    pub queue_ns: u64,
    pub batch_sum: u64,
    pub admitted: u64,
    pub saturated: u64,
}

impl Engine {
    #[must_use]
    pub fn new(max_batch: usize) -> Self {
        Self {
            max_batch: max_batch.max(1),
            inflight: BinaryHeap::new(),
            queue_ns: 0,
            batch_sum: 0,
            admitted: 0,
            saturated: 0,
        }
    }

    #[must_use]
    pub fn step_ns(batch: usize) -> u64 {
        STEP_BASE_NS + (batch.saturating_sub(1)) as u64 * STEP_PER_SEQ_NS
    }

    fn retire(&mut self, now_ns: u64) {
        while let Some(&Reverse(end)) = self.inflight.peek() {
            if end > now_ns {
                break;
            }
            self.inflight.pop();
        }
    }

    /// Sequences still resident at `now_ns`. Read-only so placement can price every candidate
    /// engine before committing to one.
    #[must_use]
    pub fn load(&self, now_ns: u64) -> usize {
        self.inflight
            .iter()
            .filter(|Reverse(e)| *e > now_ns)
            .count()
    }

    /// What admitting this decode here would cost, without admitting it. Queueing is included
    /// because a saturated engine turns a cache hit into a wait, and that is precisely the
    /// case where the node holding the prefix is the wrong node.
    ///
    /// `reserved` counts sequences already promised to this engine but not yet admitted --
    /// the siblings of a fan-out being placed together, which will join the same batch.
    #[must_use]
    pub fn projected_ns(&self, now_ns: u64, tokens: u64, reserved: usize) -> u64 {
        let live = self.load(now_ns) + reserved;
        let wait = if live < self.max_batch {
            0
        } else {
            self.inflight
                .peek()
                .map_or(0, |&Reverse(e)| e.saturating_sub(now_ns))
        };
        wait + tokens * Self::step_ns(live.min(self.max_batch - 1) + 1)
    }

    /// What admitting this decode here would cost *everyone else already decoding*.
    ///
    /// Joining a batch of `live` widens every one of those sequences' steps by
    /// `STEP_PER_SEQ_NS` for as long as this one overlaps them. `projected_ns` prices only
    /// the private half of that -- what the existing batch does to me -- and a scheduler that
    /// sees only the private half will happily pile work onto the node that is already
    /// deepest, because joining a full batch costs the joiner barely more than joining an
    /// empty one. The social half is convex in occupancy, which is what makes it a gradient
    /// away from hot nodes rather than a tiebreak.
    ///
    /// Overlap is approximated by this request's own length. The exact quantity needs every
    /// in-flight sequence's remaining tokens, which is a step-accurate simulation; the
    /// approximation keeps the term's shape, which is what decides placements.
    ///
    /// The widening alone is linear in occupancy, and a linear toll cannot represent the knee:
    /// the thing that actually hurts near capacity is not wider steps but the wait every
    /// *future* arrival inherits once the batch fills, which grows like `1 / (1 - u)`. That is
    /// the shape of the classical congestion toll on a queue, and it is applied here as a
    /// factor on the widening so that the two agree where the queue is empty -- at low
    /// utilisation this is the linear term, unchanged -- and part company exactly where
    /// placement starts to matter.
    #[must_use]
    pub fn congestion_ns(&self, now_ns: u64, tokens: u64, reserved: usize) -> u64 {
        let live = self.load(now_ns) + reserved;
        let u = (live as f64 / self.max_batch as f64).min(UTILISATION_CAP);
        let widening = live as u64 * STEP_PER_SEQ_NS * tokens;
        (widening as f64 / (1.0 - u)) as u64
    }

    /// Admit one sequence. The batch is sampled once at admission and held for the whole
    /// decode rather than re-evaluated per step: a step-accurate engine is a different
    /// simulation, and the error is second-order next to modelling no batch at all.
    pub fn decode(&mut self, arrival_ns: u64, tokens: u64) -> Decode {
        self.retire(arrival_ns);
        let mut start = arrival_ns;
        if self.inflight.len() >= self.max_batch {
            self.saturated += 1;
            if let Some(&Reverse(end)) = self.inflight.peek() {
                start = start.max(end);
            }
            self.retire(start);
        }
        let batch = self.inflight.len() + 1;
        let exec_ns = tokens * Self::step_ns(batch);
        self.inflight.push(Reverse(start + exec_ns));
        let queue_ns = start - arrival_ns;
        self.queue_ns += queue_ns;
        self.batch_sum += batch as u64;
        self.admitted += 1;
        Decode {
            queue_ns,
            exec_ns,
            batch,
        }
    }

    #[must_use]
    pub fn mean_batch(&self) -> f64 {
        if self.admitted == 0 {
            0.0
        } else {
            self.batch_sum as f64 / self.admitted as f64
        }
    }
}
