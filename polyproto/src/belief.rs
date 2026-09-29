use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashMap, VecDeque};

use crate::blob::{BlobId, BlobMeta};
use crate::engine::STEP_BASE_NS;
use crate::rng::Rng;
use crate::stream::{Index, KvEvent, Medium};

pub const BUFFER_STEPS: usize = 10_000;
pub const SLO_QUANTILE: f64 = 0.9;
const CHANNEL_SEED: u64 = 0x0C4A_77E1_5EED;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Recovery {
    Replay,
    Periodic,
    None,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LoadSource {
    Path,
    Stream,
    StreamPlusDispatch,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Scoring {
    FaceValue,
    Expected,
    Quantile(f64),
    Slo,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Episode {
    pub node: usize,
    pub from_ns: u64,
    pub until_ns: u64,
}

#[derive(Clone, Debug)]
pub struct Conditions {
    pub cadence: bool,
    pub lag_ns: u64,
    pub loss: f64,
    pub recovery: Recovery,
    pub period_ns: u64,
    pub episodes: Vec<Episode>,
    pub seed: u64,
    pub load: LoadSource,
}

impl Conditions {
    #[must_use]
    pub fn exact() -> Self {
        Self {
            cadence: false,
            lag_ns: 0,
            loss: 0.0,
            recovery: Recovery::Replay,
            period_ns: 1_000_000_000,
            episodes: Vec::new(),
            seed: 0,
            load: LoadSource::Path,
        }
    }

    #[must_use]
    pub fn is_exact(&self) -> bool {
        !self.cadence && self.lag_ns == 0 && self.loss == 0.0 && self.episodes.is_empty()
    }
}

#[derive(Clone, Debug)]
struct Batch {
    seq: u64,
    close_ns: u64,
    load: usize,
    events: Vec<KvEvent>,
}

#[derive(Debug)]
enum Delivery {
    Batch(Batch),
    Replay {
        batches: Vec<Batch>,
        floor: u64,
    },
    Snapshot {
        as_of: u64,
        at_ns: u64,
        index: Index,
    },
}

#[derive(Debug, Default)]
struct Publisher {
    mirror: Index,
    open: Vec<KvEvent>,
    next_seq: u64,
    open_close: u64,
    buffer: VecDeque<Batch>,
    next_snapshot: u64,
}

#[derive(Debug, Default)]
struct Subscriber {
    next_expected: u64,
    ahead: BTreeMap<u64, Batch>,
    outstanding_until: u64,
    unapplied: Vec<(u64, u64)>,
    log: BTreeMap<u64, Batch>,
}

#[derive(Clone, Copy, Debug)]
struct Optimistic {
    window_seq: u64,
    bytes: u64,
}

#[derive(Debug, Default)]
pub struct Belief {
    index: Index,
    optimistic: HashMap<BlobId, Optimistic>,
    optimistic_bytes: u64,
    confirmed: HashMap<BlobId, u64>,
    pinned: HashMap<BlobId, u32>,
    holds: BinaryHeap<Reverse<(u64, u64)>>,
    held: HashMap<u64, Vec<BlobId>>,
    next_hold: u64,
    windows_applied: u64,
    gpu_removals: u64,
    cpu_removals: u64,
    unit_price: f64,
    last_price: f64,
    reported_load: usize,
    sent: BTreeMap<u64, usize>,
}

impl Belief {
    #[must_use]
    pub fn believes_gpu(&self, id: &BlobId) -> bool {
        self.index.contains(Medium::Gpu, id) || self.optimistic.contains_key(id)
    }

    #[must_use]
    pub fn believes_held(&self, id: &BlobId) -> bool {
        self.believes_gpu(id) || self.index.contains(Medium::Cpu, id)
    }

    #[must_use]
    pub fn tier(&self, id: &BlobId) -> Option<Medium> {
        if self.believes_gpu(id) {
            Some(Medium::Gpu)
        } else if self.index.contains(Medium::Cpu, id) {
            Some(Medium::Cpu)
        } else if self.index.contains(Medium::Storage, id) {
            Some(Medium::Storage)
        } else {
            None
        }
    }

    pub fn gpu_ids(&self) -> impl Iterator<Item = &BlobId> {
        self.index.ids(Medium::Gpu).chain(self.optimistic.keys())
    }

    #[must_use]
    pub fn gpu_bytes(&self) -> u64 {
        self.index.bytes(Medium::Gpu) + self.optimistic_bytes
    }

    #[must_use]
    pub fn price(&self) -> f64 {
        let resident = self.index.len(Medium::Gpu) + self.optimistic.len();
        if resident > self.pinned.len() {
            self.unit_price
        } else {
            self.last_price
        }
    }

    #[must_use]
    pub fn index(&self) -> &Index {
        &self.index
    }

    fn apply(&mut self, seq: u64, close_ns: u64, load: usize, events: &[KvEvent]) {
        self.windows_applied += 1;
        self.reported_load = load;
        self.sent = self.sent.split_off(&(seq + 1));
        for event in events {
            self.index.apply(event);
            match *event {
                KvEvent::Stored { id, .. } => {
                    if let Some(o) = self.optimistic.remove(&id) {
                        self.optimistic_bytes -= o.bytes;
                    }
                    let seen = self.confirmed.entry(id).or_insert(0);
                    *seen = (*seen).max(close_ns);
                }
                KvEvent::Removed { id, medium } => {
                    match medium {
                        Medium::Gpu => {
                            self.gpu_removals += 1;
                            self.last_price = self.unit_price;
                        }
                        Medium::Cpu => self.cpu_removals += 1,
                        Medium::Storage => {}
                    }
                    if !self.index.contains_any(&id) {
                        self.confirmed.remove(&id);
                    }
                }
                KvEvent::Cleared => {
                    self.optimistic.clear();
                    self.optimistic_bytes = 0;
                    self.confirmed.clear();
                    self.windows_applied = 1;
                    self.gpu_removals = 0;
                    self.cpu_removals = 0;
                }
            }
        }
        self.reconcile(|o| o.window_seq == seq);
    }

    fn reconcile(&mut self, covered: impl Fn(&Optimistic) -> bool) {
        let mut freed = 0;
        self.optimistic.retain(|_, o| {
            let drop = covered(o);
            if drop {
                freed += o.bytes;
            }
            !drop
        });
        self.optimistic_bytes -= freed;
    }

    fn restore(&mut self, as_of: u64, at_ns: u64, index: Index, since: &BTreeMap<u64, Batch>) {
        self.index = index;
        self.reconcile(|o| o.window_seq <= as_of);
        for id in self.index.sorted_ids(Medium::Gpu) {
            let seen = self.confirmed.entry(id).or_insert(0);
            *seen = (*seen).max(at_ns);
        }
        for batch in since.values().filter(|b| b.seq > as_of) {
            for event in &batch.events {
                self.index.apply(event);
            }
        }
    }

    fn dispatched(&mut self, blocks: &[(BlobId, BlobMeta)], now: u64, window_seq: u64, price: f64) {
        if price > 0.0 {
            self.unit_price = price;
        }
        for &(id, meta) in blocks {
            if !self.believes_gpu(&id) {
                self.optimistic.insert(
                    id,
                    Optimistic {
                        window_seq,
                        bytes: meta.bytes,
                    },
                );
                self.optimistic_bytes += meta.bytes;
            }
            let seen = self.confirmed.entry(id).or_insert(0);
            *seen = (*seen).max(now);
        }
    }

    fn pin(&mut self, ids: Vec<BlobId>, until: u64) {
        let seq = self.next_hold;
        self.next_hold += 1;
        for id in &ids {
            *self.pinned.entry(*id).or_insert(0) += 1;
        }
        self.held.insert(seq, ids);
        self.holds.push(Reverse((until, seq)));
    }

    fn release(&mut self, now: u64) {
        while let Some(&Reverse((end, seq))) = self.holds.peek() {
            if end > now {
                break;
            }
            self.holds.pop();
            for id in self.held.remove(&seq).unwrap_or_default() {
                if let Some(n) = self.pinned.get_mut(&id) {
                    *n -= 1;
                    if *n == 0 {
                        self.pinned.remove(&id);
                    }
                }
            }
        }
    }

    fn reset(&mut self) {
        let unit_price = self.unit_price;
        let last_price = self.last_price;
        *self = Self::default();
        self.unit_price = unit_price;
        self.last_price = last_price;
    }
}

#[derive(Debug)]
pub struct Observer {
    cond: Conditions,
    rng: Rng,
    pubs: Vec<Publisher>,
    subs: Vec<Subscriber>,
    beliefs: Vec<Belief>,
    dead: Vec<bool>,
    transit: BTreeMap<(u64, u64), (usize, Delivery)>,
    order: u64,
    clock: u64,
}

impl Observer {
    #[must_use]
    pub fn new(nodes: usize, cond: Conditions) -> Self {
        let rng = Rng::new(CHANNEL_SEED ^ cond.seed);
        Self {
            cond,
            rng,
            pubs: (0..nodes).map(|_| Publisher::default()).collect(),
            subs: (0..nodes).map(|_| Subscriber::default()).collect(),
            beliefs: (0..nodes).map(|_| Belief::default()).collect(),
            dead: vec![false; nodes],
            transit: BTreeMap::new(),
            order: 0,
            clock: 0,
        }
    }

    #[must_use]
    pub fn conditions(&self) -> &Conditions {
        &self.cond
    }

    #[must_use]
    pub fn belief(&self, d: usize) -> &Belief {
        &self.beliefs[d]
    }

    #[must_use]
    pub fn mirror(&self, d: usize) -> &Index {
        &self.pubs[d].mirror
    }

    pub fn pump(&mut self, now: u64, steps: &[u64], loads: &[usize]) {
        self.clock = now;
        if self.cond.cadence {
            for d in 0..self.pubs.len() {
                if self.dead[d] {
                    continue;
                }
                let step = steps.get(d).copied().unwrap_or(STEP_BASE_NS).max(1);
                if self.pubs[d].open_close == 0 {
                    self.pubs[d].open_close = step;
                }
                let load = loads.get(d).copied().unwrap_or(0);
                while self.pubs[d].open_close <= now {
                    let close = self.pubs[d].open_close;
                    self.seal(d, close, load);
                    self.pubs[d].open_close = close + step;
                }
            }
        }
        self.deliver_due(now);
    }

    pub fn emit(&mut self, d: usize, events: Vec<KvEvent>, now: u64, load: usize) {
        self.clock = now;
        if self.dead[d] {
            return;
        }
        self.pubs[d].open.extend(events);
        if !self.cond.cadence {
            self.seal(d, now, load);
            self.deliver_due(now);
        }
    }

    pub fn dispatched(&mut self, d: usize, blocks: &[(BlobId, BlobMeta)], now: u64, price: f64) {
        let window_seq = self.pubs[d].next_seq;
        self.beliefs[d].dispatched(blocks, now, window_seq, price);
    }

    pub fn sent(&mut self, d: usize) {
        let window = self.pubs[d].next_seq;
        *self.beliefs[d].sent.entry(window).or_insert(0) += 1;
    }

    #[must_use]
    pub fn reported_load(&self, d: usize) -> Option<usize> {
        let b = &self.beliefs[d];
        match self.cond.load {
            LoadSource::Path => None,
            LoadSource::Stream => Some(b.reported_load),
            LoadSource::StreamPlusDispatch => {
                Some(b.reported_load + b.sent.values().sum::<usize>())
            }
        }
    }

    pub fn pin(&mut self, d: usize, ids: Vec<BlobId>, until: u64) {
        self.beliefs[d].pin(ids, until);
    }

    pub fn release(&mut self, now: u64) {
        for b in &mut self.beliefs {
            b.release(now);
        }
    }

    pub fn retire(&mut self, d: usize) {
        self.dead[d] = true;
        self.pubs[d].open.clear();
        self.beliefs[d].reset();
    }

    #[must_use]
    pub fn unknown_steps(&self, d: usize, since: u64) -> u64 {
        let sub = &self.subs[d];
        let first = sub.unapplied.partition_point(|&(_, close)| close <= since);
        let mut n = (sub.unapplied.len() - first) as u64;
        if self.cond.cadence && since < self.clock {
            n += 1;
        }
        n
    }

    #[must_use]
    pub fn block_unknown(&self, d: usize, id: &BlobId) -> u64 {
        let since = self.beliefs[d].confirmed.get(id).copied().unwrap_or(0);
        self.unknown_steps(d, since)
    }

    #[must_use]
    pub fn survival(&self, d: usize, id: &BlobId) -> f64 {
        let b = &self.beliefs[d];
        if !b.believes_gpu(id) {
            return 0.0;
        }
        if b.pinned.contains_key(id) {
            return 1.0;
        }
        let since = b.confirmed.get(id).copied().unwrap_or(0);
        let unknown = self.unknown_steps(d, since);
        if unknown == 0 || b.windows_applied == 0 {
            return 1.0;
        }
        let rate = b.gpu_removals as f64 / b.windows_applied as f64;
        let unpinned = (b.index.len(Medium::Gpu) + b.optimistic.len())
            .saturating_sub(b.pinned.len())
            .max(1);
        (1.0 - rate * unknown as f64 / unpinned as f64).clamp(0.0, 1.0)
    }

    #[must_use]
    pub fn held_survival(&self, d: usize, id: &BlobId) -> f64 {
        let b = &self.beliefs[d];
        if b.believes_gpu(id) {
            return 1.0;
        }
        if !b.index.contains(Medium::Cpu, id) {
            return 0.0;
        }
        let since = b.confirmed.get(id).copied().unwrap_or(0);
        let unknown = self.unknown_steps(d, since);
        if unknown == 0 || b.windows_applied == 0 {
            return 1.0;
        }
        let rate = b.cpu_removals as f64 / b.windows_applied as f64;
        let resident = b.index.len(Medium::Cpu).max(1);
        (1.0 - rate * unknown as f64 / resident as f64).clamp(0.0, 1.0)
    }

    fn silenced(&self, d: usize, at: u64) -> bool {
        self.cond
            .episodes
            .iter()
            .any(|e| e.node == d && e.from_ns <= at && at < e.until_ns)
    }

    fn lost(&mut self, d: usize, at: u64) -> bool {
        if self.silenced(d, at) {
            return true;
        }
        self.cond.loss > 0.0 && self.rng.chance(self.cond.loss)
    }

    fn send(&mut self, at: u64, d: usize, delivery: Delivery) {
        self.transit.insert((at, self.order), (d, delivery));
        self.order += 1;
    }

    fn seal(&mut self, d: usize, close: u64, load: usize) {
        let replayable = self.cond.recovery == Recovery::Replay
            && (self.cond.loss > 0.0 || !self.cond.episodes.is_empty());
        let publisher = &mut self.pubs[d];
        let seq = publisher.next_seq;
        publisher.next_seq += 1;
        let events = std::mem::take(&mut publisher.open);
        for event in &events {
            publisher.mirror.apply(event);
        }
        let batch = Batch {
            seq,
            close_ns: close,
            load,
            events,
        };
        if replayable {
            publisher.buffer.push_back(batch.clone());
            if publisher.buffer.len() > BUFFER_STEPS {
                publisher.buffer.pop_front();
            }
        }
        self.subs[d].unapplied.push((seq, close));
        if !self.lost(d, close) {
            self.send(close + self.cond.lag_ns, d, Delivery::Batch(batch));
        }
        if self.cond.recovery == Recovery::Periodic && close >= self.pubs[d].next_snapshot {
            self.pubs[d].next_snapshot = close + self.cond.period_ns;
            if !self.lost(d, close) {
                let index = self.pubs[d].mirror.clone();
                self.send(
                    close + self.cond.lag_ns,
                    d,
                    Delivery::Snapshot {
                        as_of: seq,
                        at_ns: close,
                        index,
                    },
                );
            }
        }
    }

    fn deliver_due(&mut self, now: u64) {
        while let Some(entry) = self.transit.first_entry() {
            if entry.key().0 > now {
                break;
            }
            let ((at, _), (d, delivery)) = entry.remove_entry();
            if self.dead[d] {
                continue;
            }
            match delivery {
                Delivery::Batch(batch) => self.on_batch(d, batch, at),
                Delivery::Replay { batches, floor } => self.on_replay(d, batches, floor, at),
                Delivery::Snapshot {
                    as_of,
                    at_ns,
                    index,
                } => {
                    self.on_snapshot(d, as_of, at_ns, index);
                }
            }
        }
    }

    fn on_batch(&mut self, d: usize, batch: Batch, at: u64) {
        if self.cond.recovery != Recovery::Replay {
            self.apply(d, batch);
            return;
        }
        if batch.seq < self.subs[d].next_expected {
            return;
        }
        self.subs[d].ahead.insert(batch.seq, batch);
        self.drain_ahead(d);
        self.request_replay(d, at);
    }

    fn on_replay(&mut self, d: usize, batches: Vec<Batch>, floor: u64, at: u64) {
        let sub = &mut self.subs[d];
        if floor > sub.next_expected {
            sub.next_expected = floor;
            sub.ahead.retain(|&seq, _| seq >= floor);
        }
        for batch in batches {
            if batch.seq >= self.subs[d].next_expected {
                self.subs[d].ahead.insert(batch.seq, batch);
            }
        }
        self.drain_ahead(d);
        self.request_replay(d, at);
    }

    fn on_snapshot(&mut self, d: usize, as_of: u64, at_ns: u64, index: Index) {
        let sub = &mut self.subs[d];
        sub.unapplied.retain(|&(seq, _)| seq > as_of);
        let since = std::mem::take(&mut sub.log);
        self.beliefs[d].restore(as_of, at_ns, index, &since);
        self.subs[d].log = since.into_iter().filter(|(seq, _)| *seq > as_of).collect();
    }

    fn drain_ahead(&mut self, d: usize) {
        loop {
            let next = self.subs[d].next_expected;
            let Some(batch) = self.subs[d].ahead.remove(&next) else {
                break;
            };
            self.subs[d].next_expected = next + 1;
            self.apply(d, batch);
        }
    }

    fn request_replay(&mut self, d: usize, at: u64) {
        let sub = &self.subs[d];
        let Some(&newest) = sub.ahead.keys().next_back() else {
            return;
        };
        if at < sub.outstanding_until {
            return;
        }
        let missing: Vec<u64> = (sub.next_expected..newest)
            .filter(|seq| !sub.ahead.contains_key(seq))
            .collect();
        if missing.is_empty() {
            return;
        }
        let lag = self.cond.lag_ns;
        self.subs[d].outstanding_until = at + 2 * lag;
        let reply_sent = at + lag;
        let floor = self.pubs[d].buffer.front().map_or(0, |b| b.seq);
        let mut batches = Vec::with_capacity(missing.len());
        for seq in missing {
            let held = seq
                .checked_sub(floor)
                .and_then(|i| {
                    self.pubs[d]
                        .buffer
                        .get(usize::try_from(i).unwrap_or(usize::MAX))
                })
                .cloned();
            let Some(batch) = held else {
                continue;
            };
            if !self.lost(d, reply_sent) {
                batches.push(batch);
            }
        }
        self.send(at + 2 * lag, d, Delivery::Replay { batches, floor });
    }

    fn apply(&mut self, d: usize, batch: Batch) {
        let sub = &mut self.subs[d];
        if let Ok(i) = sub
            .unapplied
            .binary_search_by_key(&batch.seq, |&(seq, _)| seq)
        {
            sub.unapplied.remove(i);
        }
        self.beliefs[d].apply(batch.seq, batch.close_ns, batch.load, &batch.events);
        if self.cond.recovery == Recovery::Periodic {
            self.subs[d].log.insert(batch.seq, batch);
        }
    }
}

#[cfg(test)]
#[allow(clippy::float_cmp)]
mod tests {
    use super::*;

    fn block(tag: &str) -> (BlobId, BlobMeta) {
        (
            BlobId::leaf(tag.as_bytes()),
            BlobMeta {
                kind: crate::blob::BlobKind::KvBlock,
                bytes: 100,
                parent: None,
                recompute_ns: 1_000,
            },
        )
    }

    fn stored(b: &(BlobId, BlobMeta), medium: Medium) -> KvEvent {
        KvEvent::Stored {
            id: b.0,
            bytes: b.1.bytes,
            medium,
        }
    }

    fn removed(b: &(BlobId, BlobMeta), medium: Medium) -> KvEvent {
        KvEvent::Removed { id: b.0, medium }
    }

    fn lagged(loss: f64, recovery: Recovery) -> Observer {
        Observer::new(
            1,
            Conditions {
                cadence: true,
                lag_ns: 100,
                loss,
                recovery,
                period_ns: 1_000,
                episodes: Vec::new(),
                seed: 1,
                load: LoadSource::Path,
            },
        )
    }

    #[test]
    fn an_exact_channel_applies_an_emission_before_returning() {
        let a = block("a");
        let mut o = Observer::new(1, Conditions::exact());
        o.pump(10, &[7], &[0, 0]);
        o.dispatched(0, &[a], 10, 1.0);
        assert!(o.belief(0).believes_gpu(&a.0));
        o.emit(0, vec![stored(&a, Medium::Gpu)], 10, 0);
        assert!(o.belief(0).index().contains(Medium::Gpu, &a.0));
        assert_eq!(o.belief(0).gpu_bytes(), 100);
        o.pump(20, &[7], &[0, 0]);
        o.emit(0, vec![removed(&a, Medium::Gpu)], 20, 0);
        assert!(!o.belief(0).believes_gpu(&a.0));
        assert_eq!(o.belief(0).gpu_bytes(), 0);
        assert_eq!(o.survival(0, &a.0), 0.0);
    }

    #[test]
    fn a_dispatch_that_was_never_stored_stops_being_believed_with_its_window() {
        let a = block("a");
        let mut o = Observer::new(1, Conditions::exact());
        o.pump(10, &[7], &[0, 0]);
        o.dispatched(0, &[a], 10, 1.0);
        assert!(o.belief(0).believes_gpu(&a.0));
        o.emit(0, Vec::new(), 10, 0);
        assert!(!o.belief(0).believes_gpu(&a.0));
        assert_eq!(o.belief(0).gpu_bytes(), 0);
    }

    #[test]
    fn a_lagged_batch_is_invisible_until_its_step_closes_and_its_hop_lands() {
        let a = block("a");
        let mut o = lagged(0.0, Recovery::Replay);
        o.pump(3, &[10], &[0, 0]);
        o.emit(0, vec![stored(&a, Medium::Gpu)], 3, 0);
        o.pump(9, &[10], &[0, 0]);
        assert!(!o.belief(0).index().contains(Medium::Gpu, &a.0));
        o.pump(10, &[10], &[0, 0]);
        assert!(!o.belief(0).index().contains(Medium::Gpu, &a.0));
        o.pump(110, &[10], &[0, 0]);
        assert!(o.belief(0).index().contains(Medium::Gpu, &a.0));
    }

    #[test]
    fn replay_repairs_a_dropped_batch_and_applies_in_sequence_order() {
        let a = block("a");
        let mut o = Observer::new(
            1,
            Conditions {
                cadence: true,
                lag_ns: 5,
                loss: 0.0,
                recovery: Recovery::Replay,
                period_ns: 1_000,
                episodes: vec![Episode {
                    node: 0,
                    from_ns: 10,
                    until_ns: 20,
                }],
                seed: 1,
                load: LoadSource::Path,
            },
        );
        o.pump(9, &[10], &[0, 0]);
        o.emit(0, vec![stored(&a, Medium::Gpu)], 9, 0);
        o.pump(25, &[10], &[0, 0]);
        o.emit(0, vec![removed(&a, Medium::Gpu)], 25, 0);
        o.pump(35, &[10], &[0, 0]);
        let mut t = 35;
        while t < 100 {
            t += 10;
            o.pump(t, &[10], &[0, 0]);
        }
        assert!(!o.belief(0).index().contains(Medium::Gpu, &a.0));
        assert_eq!(o.belief(0).index(), o.mirror(0));
        assert_eq!(o.unknown_steps(0, 0), 1);
    }

    #[test]
    fn without_recovery_a_dropped_store_is_never_learned() {
        let a = block("a");
        let mut o = Observer::new(
            1,
            Conditions {
                cadence: true,
                lag_ns: 5,
                loss: 0.0,
                recovery: Recovery::None,
                period_ns: 1_000,
                episodes: vec![Episode {
                    node: 0,
                    from_ns: 10,
                    until_ns: 30,
                }],
                seed: 1,
                load: LoadSource::Path,
            },
        );
        o.pump(9, &[10], &[0, 0]);
        o.pump(12, &[10], &[0, 0]);
        o.emit(0, vec![stored(&a, Medium::Gpu)], 12, 0);
        for t in (20..200).step_by(10) {
            o.pump(t, &[10], &[0, 0]);
        }
        assert!(o.mirror(0).contains(Medium::Gpu, &a.0));
        assert!(!o.belief(0).index().contains(Medium::Gpu, &a.0));
        assert!(o.unknown_steps(0, 0) >= 1);
    }

    #[test]
    fn a_snapshot_replaces_the_belief_and_keeps_what_arrived_after_it() {
        let (a, b) = (block("a"), block("b"));
        let mut o = Observer::new(
            1,
            Conditions {
                cadence: true,
                lag_ns: 5,
                loss: 0.0,
                recovery: Recovery::Periodic,
                period_ns: 30,
                episodes: vec![Episode {
                    node: 0,
                    from_ns: 10,
                    until_ns: 20,
                }],
                seed: 1,
                load: LoadSource::Path,
            },
        );
        o.pump(9, &[10], &[0, 0]);
        o.emit(0, vec![stored(&a, Medium::Gpu)], 9, 0);
        for t in (10..=60).step_by(5) {
            if t == 45 {
                o.emit(0, vec![stored(&b, Medium::Gpu)], t, 0);
            }
            o.pump(t, &[10], &[0, 0]);
        }
        assert!(o.belief(0).index().contains(Medium::Gpu, &a.0));
        assert!(o.belief(0).index().contains(Medium::Gpu, &b.0));
    }

    #[test]
    fn survival_decays_with_unknown_steps_and_a_pinned_block_is_certain() {
        let (a, b) = (block("a"), block("b"));
        let mut o = Observer::new(
            1,
            Conditions {
                cadence: true,
                lag_ns: 5,
                loss: 0.0,
                recovery: Recovery::Replay,
                period_ns: 1_000,
                episodes: Vec::new(),
                seed: 1,
                load: LoadSource::Path,
            },
        );
        let fillers: Vec<_> = (0..98).map(|i| block(&format!("v{i}"))).collect();
        o.pump(9, &[10], &[0, 0]);
        let mut events = vec![stored(&a, Medium::Gpu), stored(&b, Medium::Gpu)];
        events.extend(fillers.iter().map(|v| stored(v, Medium::Gpu)));
        o.emit(0, events, 9, 0);
        o.pump(15, &[10], &[0, 0]);
        let events: Vec<_> = fillers[..10]
            .iter()
            .map(|v| removed(v, Medium::Gpu))
            .collect();
        o.emit(0, events, 16, 0);
        o.pump(25, &[10], &[0, 0]);
        o.pin(0, vec![b.0], 10_000);
        let expected = 1.0 - 5.0 / 89.0;
        assert!((o.survival(0, &a.0) - expected).abs() < 1e-12);
        assert_eq!(o.survival(0, &b.0), 1.0);
        assert_eq!(o.survival(0, &BlobId::leaf(b"never")), 0.0);
        o.pump(26, &[10], &[0, 0]);
        o.dispatched(0, &[a], 26, 1.0);
        assert_eq!(o.survival(0, &a.0), 1.0);
    }

    #[test]
    fn a_retired_node_stops_believing_and_stops_publishing() {
        let a = block("a");
        let mut o = Observer::new(2, Conditions::exact());
        o.pump(1, &[7, 7], &[0, 0]);
        o.emit(1, vec![stored(&a, Medium::Gpu)], 1, 0);
        assert!(o.belief(1).believes_gpu(&a.0));
        o.retire(1);
        assert!(!o.belief(1).believes_gpu(&a.0));
        o.emit(1, vec![stored(&a, Medium::Gpu)], 2, 0);
        assert!(!o.belief(1).believes_gpu(&a.0));
    }

    #[test]
    fn stream_load_is_the_last_delivered_count_and_dispatches_cover_only_what_it_has_not() {
        let conditions = |load| Conditions {
            cadence: true,
            lag_ns: 5,
            loss: 0.0,
            recovery: Recovery::Replay,
            period_ns: 1_000,
            episodes: Vec::new(),
            seed: 1,
            load,
        };
        let mut path = Observer::new(1, conditions(LoadSource::Path));
        let mut stream = Observer::new(1, conditions(LoadSource::Stream));
        let mut plus = Observer::new(1, conditions(LoadSource::StreamPlusDispatch));
        for o in [&mut path, &mut stream, &mut plus] {
            o.pump(9, &[10], &[3]);
            o.pump(15, &[10], &[3]);
            o.sent(0);
            o.sent(0);
        }
        assert_eq!(path.reported_load(0), None);
        assert_eq!(stream.reported_load(0), Some(3));
        assert_eq!(plus.reported_load(0), Some(5));
        for o in [&mut stream, &mut plus] {
            o.pump(25, &[10], &[7]);
        }
        assert_eq!(stream.reported_load(0), Some(7));
        assert_eq!(plus.reported_load(0), Some(7));
    }
}
