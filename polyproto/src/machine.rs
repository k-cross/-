use crate::admit::{Reservations, Reserve};
use crate::belief::{Conditions, Observer, SLO_QUANTILE, Scoring};
use crate::blob::{BlobId, BlobKind, BlobMeta};
use crate::boundary::Cost as Crossing;
use crate::cache::{Cost, Hierarchy, NodeMemory, Policy};
use crate::engine::{Engine, MAX_BATCH};
use crate::instruments::Instruments;
use crate::oracle;
use crate::span::Span;
use crate::tele::Telemetry;
use crate::tier::TierSpec;
use crate::topo::Topology;
use crate::work::{Agent, Gang, Request, RequestView, Slo, ToolCall};
use std::collections::{HashMap, HashSet};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Placement {
    Blind,

    Sticky,

    Aware,

    Scored,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Control {
    Unified,

    Query,

    Gossip { period: u64 },
}

impl Control {
    #[must_use]
    pub fn label(self) -> String {
        match self {
            Self::Unified => "unified (in-process)".to_string(),
            Self::Query => "query (rpc per decision)".to_string(),
            Self::Gossip { period } => format!("gossip (every {period})"),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum View {
    Belief,
    Truth,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DataPath {
    Integrated,

    Sidecar,

    SidecarPluggable,
}

#[derive(Debug)]
#[allow(clippy::struct_excessive_bools)]
pub struct Machine {
    topo: Topology,
    domains: Vec<Hierarchy>,
    placement: Placement,
    next_unit: usize,
    sticky_unit: usize,

    active: Vec<usize>,
    pub migrated_bytes: u64,

    upstream: HashMap<u64, Vec<(usize, u64)>>,

    cancelled: HashSet<u64>,
    pub handoff_ns: u64,
    pub split_tasks: u64,
    pub joined_tasks: u64,
    pub local: u64,
    pub remote: u64,
    pub cold: u64,
    pub interconnect_ns: u64,
    pub bytes_crossed: u64,
    control: Control,
    crossing: Crossing,
    data_path: DataPath,

    hook: Crossing,

    dispatch: Crossing,

    flow_aware: bool,

    view: Vec<HashSet<BlobId>>,

    staged: Vec<HashSet<BlobId>>,
    staged_seqs: Vec<usize>,
    staged_bytes: Vec<Need>,
    ops: u64,

    pub decide_ns: u64,

    pub control_rpcs: u64,

    pub decisions: u64,

    pub candidates_seen: u64,

    pub dispatches: u64,

    pub stale_decisions: u64,

    pub moved_by_displacement: u64,
    pub moved_by_flow: u64,
    pub moved_by_load: u64,

    pub moved_by_congestion: u64,

    pub term_spread: [f64; TERM_COUNT],
    pub scored_decisions: u64,
    pub held_by_affinity: u64,
    pub flow_requests: u64,
    pub flow_coplaced: u64,

    engines: Vec<Engine>,

    interval_ns: u64,
    arrival_ns: u64,

    state_transfer: bool,
    pub fetched_bytes: u64,
    pub fetches: u64,
    pub rebuilds: u64,

    pub stale_fetches: u64,
    pub moved_by_fetch: u64,

    fanout_atomic: bool,

    tool_anchor: Option<usize>,

    origin: Option<(usize, u64)>,

    pub origin_ns: u64,
    pub origin_hops: u64,
    pub fanouts_admitted: u64,
    pub fanouts_refused: u64,

    pub fanout_wasted_ns: u64,
    pub fanout_wasted_bytes: u64,

    pub fanout_service_ns: u64,
    pub agents_run: u64,

    pub agents_colocated: u64,
    pub tool_calls: u64,

    pub tool_coplaced: u64,
    pub tool_refused: u64,
    pub tool_ns: u64,

    regret: bool,

    pub spans: Vec<Span>,

    pub feasibility_regret: u64,

    pub locality_coupled: u64,
    pub locality_coupled_decisions: u64,
    drain_spill: bool,
    reserve: Reserve,
    tokens_per_block: u64,
    hold_decodes: bool,
    reserved: Vec<Reservations>,
    staged_kv: Vec<u64>,
    displacement: bool,
    shared: Option<crate::engine::EngineCache>,
    pub refused_by_router: [u64; BlobKind::N],
    pub preempted: [u64; BlobKind::N],
    pub shared_reads: [u64; BlobKind::N],
    pub shared_requests: [u64; BlobKind::N],
    kv_sum: [u64; 3],
    kv_samples: u64,
    observer: Option<Observer>,
    scoring: Scoring,
    observables: bool,
    observed: [(u64, u64); 2],
    pub instruments: Instruments,
    instrument: bool,
}

const DIVERGENCE_EVERY: u64 = 16;

impl Machine {
    #[must_use]
    pub fn new(
        topo: Topology,
        memory: impl Fn(usize) -> NodeMemory,
        policy: Policy,
        placement: Placement,
    ) -> Self {
        let n_domains = topo.domains.len();
        let domains = (0..n_domains)
            .map(|d| Hierarchy::new(memory(d), policy))
            .collect();
        Self {
            topo,
            domains,
            placement,
            next_unit: 0,
            sticky_unit: 0,
            active: (0..n_domains).collect(),
            migrated_bytes: 0,
            upstream: HashMap::new(),
            cancelled: HashSet::new(),
            handoff_ns: 0,
            split_tasks: 0,
            joined_tasks: 0,
            local: 0,
            remote: 0,
            cold: 0,
            interconnect_ns: 0,
            bytes_crossed: 0,
            control: Control::Unified,
            crossing: Crossing::default(),
            data_path: DataPath::Integrated,
            hook: Crossing::default(),
            dispatch: Crossing::default(),
            flow_aware: false,
            view: vec![HashSet::new(); n_domains],
            staged: vec![HashSet::new(); n_domains],
            staged_seqs: vec![0; n_domains],
            staged_bytes: vec![[0; BlobKind::N]; n_domains],
            ops: 0,
            decide_ns: 0,
            control_rpcs: 0,
            decisions: 0,
            candidates_seen: 0,
            dispatches: 0,
            stale_decisions: 0,
            moved_by_displacement: 0,
            moved_by_flow: 0,
            moved_by_load: 0,
            moved_by_congestion: 0,
            term_spread: [0.0; TERM_COUNT],
            scored_decisions: 0,
            held_by_affinity: 0,
            flow_requests: 0,
            flow_coplaced: 0,
            engines: (0..n_domains).map(|_| Engine::new(MAX_BATCH)).collect(),
            interval_ns: 0,
            arrival_ns: 0,
            state_transfer: false,
            fetched_bytes: 0,
            fetches: 0,
            rebuilds: 0,
            stale_fetches: 0,
            moved_by_fetch: 0,
            fanout_atomic: false,
            tool_anchor: None,
            origin: None,
            origin_ns: 0,
            origin_hops: 0,
            fanouts_admitted: 0,
            fanouts_refused: 0,
            fanout_wasted_ns: 0,
            fanout_wasted_bytes: 0,
            fanout_service_ns: 0,
            agents_run: 0,
            agents_colocated: 0,
            tool_calls: 0,
            tool_coplaced: 0,
            tool_refused: 0,
            tool_ns: 0,
            regret: false,
            spans: Vec::new(),
            feasibility_regret: 0,
            locality_coupled: 0,
            locality_coupled_decisions: 0,
            drain_spill: false,
            reserve: Reserve::Prompt,
            tokens_per_block: crate::work::TOKENS_PER_KV_BLOCK,
            hold_decodes: false,
            reserved: (0..n_domains).map(|_| Reservations::default()).collect(),
            staged_kv: vec![0; n_domains],
            displacement: true,
            shared: None,
            refused_by_router: [0; BlobKind::N],
            preempted: [0; BlobKind::N],
            shared_reads: [0; BlobKind::N],
            shared_requests: [0; BlobKind::N],
            kv_sum: [0; 3],
            kv_samples: 0,
            observer: None,
            scoring: Scoring::FaceValue,
            observables: false,
            observed: [(0, 0); 2],
            instruments: Instruments::default(),
            instrument: false,
        }
    }

    pub fn set_admission(&mut self, reserve: Reserve, tokens_per_block: u64) {
        self.reserve = reserve;
        self.tokens_per_block = tokens_per_block;
    }

    pub fn set_hold_decodes(&mut self, on: bool) {
        self.hold_decodes = on;
    }

    pub fn set_displacement(&mut self, on: bool) {
        self.displacement = on;
    }

    pub fn set_shared_l2(&mut self, capacity: Option<u64>) {
        self.shared = capacity.map(|c| crate::engine::EngineCache::new(c, false));
    }

    #[must_use]
    pub fn kv_mean(&self) -> [u64; 3] {
        self.kv_sum.map(|b| b / self.kv_samples.max(1))
    }

    #[must_use]
    pub fn preemptions(&self) -> u64 {
        self.domains.iter().map(Hierarchy::preemptions).sum()
    }

    #[must_use]
    pub fn kv_orphans(&self) -> usize {
        self.domains.iter().map(Hierarchy::kv_orphans).sum()
    }

    #[must_use]
    pub fn engine_ops(&self) -> crate::cache::EngineOps {
        let mut total = crate::cache::EngineOps::default();
        for h in &self.domains {
            let o = &h.engine_ops;
            for k in 0..BlobKind::N {
                total.admit[k] += o.admit[k];
                total.touch[k] += o.touch[k];
                total.anticipate[k] += o.anticipate[k];
                total.demote[k] += o.demote[k];
                total.forget_cold[k] += o.forget_cold[k];
                total.superseded[k] += o.superseded[k];
                total.spill[k] += o.spill[k];
                total.drain[k] += o.drain[k];
            }
        }
        total
    }

    fn router_admits(&self, d: usize, req: &Request, staged: bool) -> bool {
        let Some((capacity, _)) = self.telemetry(d).partition() else {
            return true;
        };
        let staged = staged.then(|| (&self.staged[d], self.staged_kv[d]));
        self.reserved[d].admits(capacity, req, self.reserve, self.tokens_per_block, staged)
    }

    fn shared_ns(&self, d: usize, kind: BlobKind, bytes: u64) -> Option<u64> {
        let n = self.domains.len();
        if n < 2 {
            return None;
        }
        let hop = self.topo.fetch_ns(self.unit_in(d), (d + 1) % n, bytes);
        Some(hop + TierSpec::nvme(0).fetch_ns(bytes) + self.domains[d].lift_ns(kind, bytes))
    }

    pub fn set_belief(&mut self, cond: Conditions) {
        self.record_kv_events(true);
        let mut observer = Observer::new(self.domains.len(), cond);
        for (d, h) in self.domains.iter().enumerate() {
            if !h.engine_cache() {
                observer.retire(d);
            }
        }
        self.observer = Some(observer);
    }

    pub fn set_scoring(&mut self, scoring: Scoring) {
        self.scoring = scoring;
    }

    #[must_use]
    pub fn observer(&self) -> Option<&Observer> {
        self.observer.as_ref()
    }

    fn belief_read(&self, d: usize, kind: BlobKind) -> bool {
        self.sees_belief(d, kind) && self.control != Control::Query
    }

    fn rule(&self, slo: Slo) -> Scoring {
        match (self.scoring, slo) {
            (Scoring::Slo, Slo::Interactive) => Scoring::Quantile(SLO_QUANTILE),
            (Scoring::Slo, Slo::Throughput) => Scoring::Expected,
            (other, _) => other,
        }
    }

    pub fn set_instrument(&mut self, on: bool) {
        self.instrument = on;
    }

    pub fn set_observables(&mut self, on: bool) {
        self.observables = on;
    }

    fn seen_tokens(&self, req: &Request) -> u64 {
        if !self.observables || req.tokens == 0 {
            return req.tokens;
        }
        match self.observed[req.slo.idx()] {
            (sum, n) if n > 0 => sum / n,
            _ => req.max_tokens,
        }
    }

    fn view_of<'r>(&self, req: &'r Request) -> RequestView<'r> {
        req.view(self.seen_tokens(req))
    }

    fn survives(&self, d: usize, id: &BlobId, kind: BlobKind, held: bool, rule: Scoring) -> bool {
        let Scoring::Quantile(q) = rule else {
            return true;
        };
        let Some(o) = &self.observer else {
            return true;
        };
        if !self.belief_read(d, kind) || self.staged[d].contains(id) {
            return true;
        }
        let p = if held {
            o.held_survival(d, id)
        } else {
            o.survival(d, id)
        };
        p >= q
    }

    fn block_survival(&self, d: usize, id: &BlobId) -> f64 {
        if self.staged[d].contains(id) {
            return 1.0;
        }
        self.observer.as_ref().map_or(1.0, |o| o.survival(d, id))
    }

    fn expected_chain_ns(
        &self,
        d: usize,
        req: &RequestView<'_>,
        plan: &Plan,
        rule: Scoring,
    ) -> u64 {
        let kind = req
            .chain
            .first()
            .map_or(BlobKind::WeightShard, |(_, m)| m.kind);
        if rule != Scoring::Expected || !self.belief_read(d, kind) {
            return plan.ns;
        }
        let (depth, cut) = (plan.local_depth, plan.chain_cut);
        let mut survival = Vec::with_capacity(depth);
        let mut floor = 1.0f64;
        for (id, _) in &req.chain[..depth] {
            floor = floor.min(self.block_survival(d, id));
            survival.push(floor);
        }
        let peer = match plan.chain_src {
            Some(Source::Peer(p)) if cut > depth && self.belief_read(p, kind) => self
                .observer
                .as_ref()
                .map_or(1.0, |o| o.held_survival(p, &req.chain[cut - 1].0)),
            _ => 1.0,
        };
        if peer >= 1.0 && survival.iter().all(|&s| s >= 1.0) {
            return plan.ns;
        }
        let mut suffix = vec![0u64; req.chain.len() + 1];
        for (k, (id, meta)) in req.chain.iter().enumerate().rev() {
            suffix[k] = suffix[k + 1] + self.local_ns(d, id, meta, View::Belief);
        }
        let fetch = plan.ns.saturating_sub(suffix[cut]);
        let mut expected = 0.0;
        for k in 0..=depth {
            let before = if k == 0 { 1.0 } else { survival[k - 1] };
            let after = if k < depth { survival[k] } else { 0.0 };
            let weight = before - after;
            let rebuilt = suffix[k] as f64;
            let cost = if cut > depth {
                let fetched = fetch as f64
                    + if k == depth {
                        suffix[cut] as f64
                    } else {
                        rebuilt
                    };
                peer * fetched + (1.0 - peer) * rebuilt
            } else {
                rebuilt
            };
            expected += weight * cost;
        }
        expected.round() as u64
    }

    fn sees_belief(&self, d: usize, kind: BlobKind) -> bool {
        kind == BlobKind::KvBlock && self.observer.is_some() && self.domains[d].engine_cache()
    }

    fn observe_dispatch(&mut self, d: usize, req: &Request) {
        if self.observer.is_none() || !self.domains[d].engine_cache() {
            return;
        }
        let blocks: Vec<(BlobId, BlobMeta)> = req
            .chain
            .iter()
            .chain(&req.produces)
            .filter(|(_, m)| m.kind == BlobKind::KvBlock)
            .copied()
            .collect();
        let Some((_, first)) = blocks.first() else {
            return;
        };
        let price = self.domains[d].kv_unit_price(first);
        let now = self.arrival_ns;
        if let Some(o) = self.observer.as_mut() {
            o.dispatched(d, &blocks, now, price);
        }
    }

    fn observe_sent(&mut self, d: usize) {
        if !self.domains[d].engine_cache() {
            return;
        }
        if let Some(o) = self.observer.as_mut() {
            o.sent(d);
        }
    }

    fn observe_pin(&mut self, d: usize, req: &Request, until: u64) {
        let ids: Vec<BlobId> = req
            .chain
            .iter()
            .chain(&req.produces)
            .filter(|(_, m)| m.kind == BlobKind::KvBlock)
            .map(|(id, _)| *id)
            .collect();
        if let Some(o) = self.observer.as_mut() {
            o.pin(d, ids, until);
        }
    }

    fn observe_arrival(&mut self, now: u64) {
        if self.observer.is_none() {
            return;
        }
        let loads: Vec<usize> = self.engines.iter().map(|e| e.load(now)).collect();
        let steps: Vec<u64> = loads.iter().map(|&l| Engine::step_ns(l + 1)).collect();
        if let Some(o) = self.observer.as_mut() {
            o.release(now);
            o.pump(now, &steps, &loads);
        }
    }

    fn observe_emit(&mut self, d: usize) {
        if self.observer.is_none() {
            return;
        }
        let events = self.domains[d].take_kv_events();
        let now = self.arrival_ns;
        let load = self.engines[d].load(now);
        if let Some(o) = self.observer.as_mut() {
            o.emit(d, events, now, load);
        }
    }

    pub fn record_kv_events(&mut self, on: bool) {
        for h in &mut self.domains {
            h.record_kv_events(on);
        }
    }

    pub fn set_drain_spill(&mut self, on: bool) {
        self.drain_spill = on;
    }

    pub fn set_arrival_rate(&mut self, per_sec: f64) {
        self.interval_ns = if per_sec > 0.0 {
            (1e9 / per_sec) as u64
        } else {
            0
        };
    }

    pub fn set_state_transfer(&mut self, on: bool) {
        self.state_transfer = on;
    }

    pub fn set_fanout_atomic(&mut self, on: bool) {
        self.fanout_atomic = on;
    }

    pub fn set_regret(&mut self, on: bool) {
        self.regret = on;
    }

    pub fn set_tool_anchor(&mut self, anchor: Option<usize>) {
        self.tool_anchor = anchor;
    }

    pub fn set_origin(&mut self, origin: Option<(usize, u64)>) {
        self.origin = origin;
    }

    #[must_use]
    pub fn mean_batch(&self) -> f64 {
        let admitted: u64 = self.engines.iter().map(|e| e.admitted).sum();
        let sum: u64 = self.engines.iter().map(|e| e.batch_sum).sum();
        if admitted == 0 {
            0.0
        } else {
            sum as f64 / admitted as f64
        }
    }

    #[must_use]
    pub fn queue_ns(&self) -> u64 {
        self.engines.iter().map(|e| e.queue_ns).sum()
    }

    #[must_use]
    pub fn decodes(&self) -> u64 {
        self.engines.iter().map(|e| e.admitted).sum()
    }

    #[must_use]
    pub fn decodes_on(&self, d: usize) -> u64 {
        self.engines.get(d).map_or(0, |e| e.admitted)
    }

    #[must_use]
    pub fn saturated(&self) -> u64 {
        self.engines.iter().map(|e| e.saturated).sum()
    }

    #[must_use]
    pub fn engine_spread(&self) -> f64 {
        let hi = self.engines.iter().map(|e| e.admitted).max().unwrap_or(0) as f64;
        let lo = self
            .engines
            .iter()
            .map(|e| e.admitted)
            .min()
            .unwrap_or(0)
            .max(1) as f64;
        hi / lo
    }

    pub fn set_flow_aware(&mut self, on: bool) {
        self.flow_aware = on;
    }

    pub fn set_control(&mut self, control: Control, crossing: Crossing) {
        self.control = control;
        self.crossing = crossing;
        if let Control::Gossip { .. } = control {
            self.refresh_view();
        }
    }

    pub fn set_data_path(&mut self, path: DataPath, hook: Crossing, dispatch: Crossing) {
        self.data_path = path;
        self.hook = hook;
        self.dispatch = dispatch;
    }

    fn refresh_view(&mut self) {
        for (d, h) in self.domains.iter().enumerate() {
            let set: HashSet<BlobId> = h.hot_ids().collect();
            self.view[d] = set;
        }
        self.control_rpcs += self.domains.len() as u64;
    }

    fn believes_resident(&self, d: usize, id: &BlobId, kind: BlobKind) -> bool {
        self.staged[d].contains(id)
            || match self.control {
                Control::Gossip { .. } if !self.sees_belief(d, kind) => self.view[d].contains(id),
                Control::Gossip { .. } | Control::Unified | Control::Query => {
                    let seen = self.telemetry(d).resident(id, kind);
                    debug_assert!(
                        self.exact_belief_agrees(d, kind, || {
                            seen == self.domains[d].is_hot(id, kind)
                        }),
                        "an exact belief must read the truth"
                    );
                    seen
                }
            }
    }

    fn exact_belief_agrees(&self, d: usize, kind: BlobKind, check: impl Fn() -> bool) -> bool {
        let exact = self
            .observer
            .as_ref()
            .is_some_and(|o| o.conditions().is_exact());
        !exact || self.control == Control::Query || !self.sees_belief(d, kind) || check()
    }

    fn believes_held(&self, p: usize, id: &BlobId, kind: BlobKind) -> bool {
        match self.control {
            Control::Gossip { .. } if !self.sees_belief(p, kind) => self.view[p].contains(id),
            Control::Gossip { .. } | Control::Unified | Control::Query => {
                let seen = self.telemetry(p).held(id, kind);
                debug_assert!(
                    self.exact_belief_agrees(p, kind, || seen == self.domains[p].holds(id, kind)),
                    "an exact belief must read the truth"
                );
                seen
            }
        }
    }

    fn ground_truth_holds(&self, p: usize, id: &BlobId, kind: BlobKind) -> bool {
        self.domains[p].holds(id, kind)
    }

    fn ground_truth_resident(&self, d: usize, id: &BlobId, kind: BlobKind) -> bool {
        self.domains[d].is_hot(id, kind)
    }

    fn decide(&mut self, chain_len: usize, candidates: usize) -> u64 {
        self.ops += 1;
        self.decisions += 1;
        self.candidates_seen += candidates as u64;
        let control_ns = match self.control {
            Control::Unified => 0,
            Control::Query => {
                self.control_rpcs += self.active.len() as u64;
                self.crossing.ns(QUERY_BYTES_PER_BLOB * chain_len as u64)
            }
            Control::Gossip { period } => {
                if period > 0 && self.ops % period == 0 {
                    self.refresh_view();
                }
                0
            }
        };
        control_ns + self.hook_ns(candidates)
    }

    fn hook_ns(&self, candidates: usize) -> u64 {
        match self.data_path {
            DataPath::Integrated => 0,
            DataPath::Sidecar => self.hook.ns(HOOK_BYTES),
            DataPath::SidecarPluggable => candidates as u64 * self.hook.ns(HOOK_BYTES),
        }
    }

    fn affinity_unit(&self, chain: &[(BlobId, BlobMeta)], candidates: &[usize]) -> usize {
        let root = Self::root_key(chain);

        let d = candidates
            .iter()
            .copied()
            .max_by_key(|&d| Self::rendezvous(root, d))
            .unwrap_or(0);
        self.unit_in(d)
    }

    fn decode_pool_len(&self) -> usize {
        let n = self
            .active
            .iter()
            .filter(|&&d| self.domains[d].can_decode())
            .count();
        if n == 0 { self.active.len() } else { n }
    }

    fn decode_pool(&self) -> Vec<usize> {
        let pool: Vec<usize> = self
            .active
            .iter()
            .copied()
            .filter(|&d| self.domains[d].can_decode())
            .collect();
        if pool.is_empty() {
            self.active.clone()
        } else {
            pool
        }
    }

    fn needs_decode(req: &Request) -> bool {
        req.tokens > 0
            || req
                .chain
                .first()
                .is_some_and(|(_, m)| matches!(m.kind, BlobKind::KvBlock | BlobKind::WeightShard))
    }

    fn root_key(chain: &[(BlobId, BlobMeta)]) -> u64 {
        chain.first().map_or(0, |(id, _)| {
            u64::from_le_bytes(id.as_bytes()[..8].try_into().unwrap_or([0; 8]))
        })
    }

    fn rendezvous(key: u64, domain: usize) -> u64 {
        let mut x = key ^ (domain as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        x ^ (x >> 31)
    }

    fn unit_in(&self, domain: usize) -> usize {
        (0..self.topo.units.len())
            .find(|&u| self.topo.units[u].home as usize == domain)
            .unwrap_or(0)
    }

    pub fn drain(&mut self, victim: usize) {
        let Some(pos) = self.active.iter().position(|&d| d == victim) else {
            return;
        };
        self.active.remove(pos);
        if self.active.is_empty() {
            self.active.push(victim);
            return;
        }
        let (hot, cold) = self.domains[victim].drain_all(self.drain_spill);
        if let Some(o) = self.observer.as_mut() {
            self.domains[victim].take_kv_events();
            o.retire(victim);
        }
        let n = hot.len();
        for (i, (id, meta)) in hot.into_iter().chain(cold).enumerate() {
            let to = self.active[i % self.active.len()];
            self.migrated_bytes += meta.bytes;
            if i < n {
                self.domains[to].reinstate(id, meta);
            } else {
                self.domains[to].respill(id, meta);
            }
        }
    }

    fn policy_target(
        &mut self,
        affinity: usize,
        best: usize,
        value: u64,
        candidates: &[usize],
    ) -> usize {
        match self.placement {
            Placement::Blind => {
                let d = candidates[self.next_unit % candidates.len()];
                self.next_unit += 1;
                d
            }
            Placement::Sticky => affinity,
            Placement::Aware | Placement::Scored => {
                if value > 0 {
                    best
                } else {
                    affinity
                }
            }
        }
    }

    fn resident_value(&self, d: usize, req: &Request) -> u64 {
        let depth = self.prefix_len(&req.chain, |id, kind| self.believes_resident(d, id, kind));
        let chain_bytes: u64 = req.chain[..depth].iter().map(|(_, m)| m.bytes).sum();
        let dep_bytes: u64 = req
            .requires
            .iter()
            .filter(|(id, m)| self.believes_resident(d, id, m.kind))
            .map(|(_, m)| m.bytes)
            .sum();
        chain_bytes + dep_bytes
    }

    fn local_ns(&self, d: usize, id: &BlobId, m: &BlobMeta, view: View) -> u64 {
        self.telemetry_in(d, view).local_ns(id, m)
    }

    fn telemetry_in(&self, d: usize, view: View) -> Telemetry<'_> {
        match view {
            View::Belief => self.telemetry(d),
            View::Truth => Telemetry::new(&self.domains[d], &self.engines[d]),
        }
    }

    fn telemetry(&self, d: usize) -> Telemetry<'_> {
        let t = Telemetry::new(&self.domains[d], &self.engines[d]);
        match self.observed_by(d) {
            Some(o) => t.with_belief(Some(o.belief(d))),
            None => t,
        }
    }

    fn observed_by(&self, d: usize) -> Option<&Observer> {
        self.observer
            .as_ref()
            .filter(|_| self.control != Control::Query && self.domains[d].engine_cache())
    }

    fn local_run_ns(&self, d: usize, blobs: &[(BlobId, BlobMeta)], view: View) -> u64 {
        blobs
            .iter()
            .map(|(id, m)| self.local_ns(d, id, m, view))
            .sum()
    }

    fn prefix_len(
        &self,
        chain: &[(BlobId, BlobMeta)],
        present: impl Fn(&BlobId, BlobKind) -> bool,
    ) -> usize {
        if self.observer.is_some() {
            chain
                .iter()
                .take_while(|(id, m)| present(id, m.kind))
                .count()
        } else {
            chain.partition_point(|(id, m)| present(id, m.kind))
        }
    }

    fn plan(&self, d: usize, req: &RequestView<'_>, view: View) -> Plan {
        let rule = self.rule(req.slo);
        let resident = |dom: usize, id: &BlobId, kind: BlobKind| match view {
            View::Belief => {
                self.believes_resident(dom, id, kind) && self.survives(dom, id, kind, false, rule)
            }
            View::Truth => self.ground_truth_resident(dom, id, kind),
        };
        let held = |dom: usize, id: &BlobId, kind: BlobKind| match view {
            View::Belief => {
                self.believes_held(dom, id, kind) && self.survives(dom, id, kind, true, rule)
            }
            View::Truth => self.ground_truth_holds(dom, id, kind),
        };
        let depth = self.prefix_len(req.chain, |id, kind| resident(d, id, kind));
        let mut need = [0u64; BlobKind::N];
        for (_, m) in &req.chain[depth..] {
            need[m.kind.idx()] += m.bytes;
        }
        let mut plan = Plan {
            ns: self.local_run_ns(d, &req.chain[depth..], view),
            local_depth: depth,
            chain_cut: depth,
            chain_src: None,
            chain_bytes: 0,
            deps: Vec::new(),
            need,
        };
        if let Some(pool) = &self.shared {
            let far = depth
                + req.chain[depth..]
                    .iter()
                    .take_while(|(id, _)| pool.contains(id))
                    .count();
            let bytes: u64 = req.chain[depth..far].iter().map(|(_, m)| m.bytes).sum();
            let kind = req.chain.first().map_or(BlobKind::KvBlock, |(_, m)| m.kind);
            if far > depth
                && let Some(read) = self.shared_ns(d, kind, bytes)
            {
                let cand = read + self.local_run_ns(d, &req.chain[far..], view);
                if cand < plan.ns {
                    plan.ns = cand;
                    plan.chain_cut = far;
                    plan.chain_src = Some(Source::Shared);
                    plan.chain_bytes = bytes;
                }
            }
        }
        if self.state_transfer && depth < req.chain.len() {
            let unit = self.unit_in(d);
            for &p in &self.active {
                if p == d {
                    continue;
                }
                let far = self.prefix_len(req.chain, |id, kind| held(p, id, kind));
                if far <= depth {
                    continue;
                }
                let bytes: u64 = req.chain[depth..far].iter().map(|(_, m)| m.bytes).sum();
                let cand = self.topo.fetch_ns(unit, p, bytes)
                    + self.local_run_ns(d, &req.chain[far..], view);
                if cand < plan.ns {
                    plan.ns = cand;
                    plan.chain_cut = far;
                    plan.chain_src = Some(Source::Peer(p));
                    plan.chain_bytes = bytes;
                }
            }
        }
        if view == View::Belief {
            plan.ns = self.expected_chain_ns(d, req, &plan, rule);
        }
        self.plan_dependencies(d, req, &mut plan, view, &resident, &held);
        plan
    }

    fn plan_dependencies(
        &self,
        d: usize,
        req: &RequestView<'_>,
        plan: &mut Plan,
        view: View,
        resident: &dyn Fn(usize, &BlobId, BlobKind) -> bool,
        held: &dyn Fn(usize, &BlobId, BlobKind) -> bool,
    ) {
        for (i, (id, m)) in req.requires.iter().enumerate() {
            if resident(d, id, m.kind) {
                continue;
            }
            plan.need[m.kind.idx()] += m.bytes;
            let mut best = self.local_ns(d, id, m, view);
            let mut src = None;
            if self.state_transfer {
                let unit = self.unit_in(d);
                for &p in &self.active {
                    if p == d || !held(p, id, m.kind) {
                        continue;
                    }
                    let cand = self.topo.fetch_ns(unit, p, m.bytes);
                    if cand < best {
                        best = cand;
                        src = Some(Source::Peer(p));
                    }
                }
            }
            if self.shared.as_ref().is_some_and(|pool| pool.contains(id))
                && let Some(read) = self.shared_ns(d, m.kind, m.bytes)
                && read < best
            {
                best = read;
                src = Some(Source::Shared);
            }
            plan.ns += best;
            if let Some(p) = src {
                plan.deps.push((i, p));
            }
        }
    }

    fn apply_chain(&mut self, d: usize, req: &Request, plan: &Plan) -> Cost {
        let mut cost = Cost::default();
        if plan.chain_src == Some(Source::Shared) {
            let seg = req.chain[plan.local_depth..plan.chain_cut].to_vec();
            let kind = seg.first().map_or(BlobKind::KvBlock, |(_, m)| m.kind);
            let bytes: u64 = seg.iter().map(|(_, m)| m.bytes).sum();
            cost.transfer_ns += self.shared_ns(d, kind, bytes).unwrap_or(0);
            self.domains[d].supply(&seg);
            self.shared_reads[kind.idx()] += seg.len() as u64;
            if let Some(pool) = self.shared.as_mut() {
                for (id, _) in &seg {
                    pool.touch(*id, false);
                }
            }
            return cost;
        }
        if let Some(Source::Peer(p)) = plan.chain_src {
            let truth = req
                .chain
                .partition_point(|(id, m)| self.ground_truth_holds(p, id, m.kind));
            let cut = plan.chain_cut.min(truth);
            if cut <= plan.local_depth {
                self.stale_fetches += 1;
            } else {
                let seg = req.chain[plan.local_depth..cut].to_vec();
                let bytes: u64 = seg.iter().map(|(_, m)| m.bytes).sum();
                let ns = self.topo.fetch_ns(self.unit_in(d), p, bytes);
                self.domains[d].supply(&seg);
                cost.transfer_ns += ns;
                self.bytes_crossed += bytes;
                self.fetched_bytes += bytes;
                self.fetches += 1;
            }
        } else if plan.local_depth < req.chain.len() {
            self.rebuilds += 1;
        }
        cost
    }

    fn apply_deps(&mut self, d: usize, req: &Request, plan: &Plan) -> Cost {
        let mut cost = Cost::default();
        for &(i, src) in &plan.deps {
            let (id, m) = req.requires[i];
            let Source::Peer(p) = src else {
                cost.transfer_ns += self.shared_ns(d, m.kind, m.bytes).unwrap_or(0);
                self.domains[d].supply(&[(id, m)]);
                self.shared_reads[m.kind.idx()] += 1;
                if let Some(pool) = self.shared.as_mut() {
                    pool.touch(id, false);
                }
                continue;
            };
            if !self.ground_truth_holds(p, &id, m.kind) {
                self.stale_fetches += 1;
                continue;
            }
            let ns = self.topo.fetch_ns(self.unit_in(d), p, m.bytes);
            self.domains[d].supply(&[(id, m)]);
            cost.transfer_ns += ns;
            self.bytes_crossed += m.bytes;
            self.fetched_bytes += m.bytes;
            self.fetches += 1;
        }
        cost
    }

    fn placement_terms(
        &self,
        d: usize,
        req: &RequestView<'_>,
        flow: &[(usize, u64)],
        view: View,
    ) -> Terms {
        let plan = self.plan(d, req, view);
        let tele = match view {
            View::Belief => self
                .telemetry(d)
                .with_load(self.observed_by(d).and_then(|o| o.reported_load(d))),
            View::Truth => self.telemetry_in(d, view),
        };
        let displaced = if self.displacement {
            tele.displacement(&plan.need, &self.staged_bytes[d])
        } else {
            0.0
        };
        let unit = self.unit_in(d);
        let handoff: f64 = flow
            .iter()
            .filter(|(src, _)| *src != d)
            .map(|&(src, payload)| self.topo.fetch_ns(unit, src, payload) as f64)
            .sum();
        let decoding = req.tokens > 0 && self.interval_ns > 0;
        let reserved = self.staged_seqs[d];
        let engine = if decoding {
            tele.projected_ns(self.arrival_ns, req.tokens, reserved) as f64
        } else {
            0.0
        };
        let congestion = if decoding {
            tele.congestion_ns(self.arrival_ns, req.tokens, reserved) as f64
        } else {
            0.0
        };
        Terms {
            domain: d,
            acquire: plan.ns as f64,
            fetched: plan.chain_src.is_some() || !plan.deps.is_empty(),
            displaced,
            handoff,
            engine,
            congestion,
            need: plan.need,
        }
    }

    fn best_scored(
        &mut self,
        req: &RequestView<'_>,
        flow: &[(usize, u64)],
        affinity: usize,
        candidates: &[usize],
    ) -> (usize, Option<usize>) {
        let terms: Vec<Terms> = candidates
            .iter()
            .map(|&d| self.placement_terms(d, req, flow, View::Belief))
            .collect();
        let pick = |f: &dyn Fn(&Terms) -> f64| -> usize {
            terms
                .iter()
                .min_by(|a, b| f(a).total_cmp(&f(b)))
                .map_or(0, |t| t.domain)
        };
        for (i, f) in Terms::EACH.iter().enumerate() {
            let hi = terms.iter().map(f).fold(f64::MIN, f64::max);
            let lo = terms.iter().map(f).fold(f64::MAX, f64::min);
            self.term_spread[i] += hi - lo;
        }
        self.scored_decisions += 1;
        let raw = pick(&Terms::acquire_only);
        let net = pick(&Terms::net);
        let placed = pick(&Terms::placed);
        let loaded = pick(&Terms::loaded);
        let cost = |d: usize| {
            terms
                .iter()
                .find(|t| t.domain == d)
                .map_or(f64::MAX, Terms::full)
        };
        let top = pick(&Terms::full);
        if self.regret {
            self.locality_coupled_decisions += 1;
            let class = BlobKind::ALL[req.class];
            let silo = |t: &Terms| -> f64 {
                let tier = self.domains[t.domain].tier_of(class);
                let disp = self.telemetry(t.domain).displacement_in(
                    tier,
                    &t.need,
                    &self.staged_bytes[t.domain],
                );
                t.acquire + disp + t.engine + t.congestion
            };
            let silo_pick = terms
                .iter()
                .min_by(|a, b| silo(a).total_cmp(&silo(b)))
                .map_or(0, |t| t.domain);
            if silo_pick != top {
                self.locality_coupled += 1;
            }
        }

        let full = if cost(top) < cost(affinity) {
            top
        } else {
            affinity
        };
        let mut decided_by = None;
        if net != raw {
            decided_by = Some(1);
        }
        self.moved_by_displacement += u64::from(net != raw);
        if placed != net {
            decided_by = Some(2);
        }
        self.moved_by_flow += u64::from(placed != net);
        if loaded != placed {
            decided_by = Some(3);
        }
        self.moved_by_load += u64::from(loaded != placed);
        if top != loaded {
            decided_by = Some(4);
        }
        self.moved_by_congestion += u64::from(top != loaded);
        self.held_by_affinity += u64::from(full != top);
        if terms.iter().any(|t| t.domain == full && t.fetched) {
            self.moved_by_fetch += 1;
        }
        if !flow.is_empty() {
            self.flow_requests += 1;
            if flow.iter().any(|&(src, _)| src == full) {
                self.flow_coplaced += 1;
            }
        }
        (full, decided_by)
    }

    fn reach_ns(&self, home: usize, req: &Request, decode_needed: bool) -> u64 {
        let Some((origin, payload)) = self.origin else {
            return 0;
        };
        if origin == home || !decode_needed || req.completes.is_some() {
            return 0;
        }
        2 * self.topo.fetch_ns(self.unit_in(origin), home, payload)
    }

    fn realized_ns(
        &self,
        d: usize,
        req: &Request,
        decode_needed: bool,
        decide_ns: u64,
    ) -> (u64, u64) {
        let plan = self.plan(d, &req.view(req.tokens), View::Truth);
        let unit = self.unit_in(d);
        let mut ns = decide_ns + self.dispatch.ns(DISPATCH_BYTES) + plan.ns;
        if let Some(sources) = req.completes.and_then(|t| self.upstream.get(&t)) {
            for &(src, payload) in sources {
                if src != d {
                    ns += self.topo.fetch_ns(unit, src, payload);
                }
            }
        }
        ns += self.reach_ns(d, req, decode_needed);
        let decoding = req.tokens > 0 && self.interval_ns > 0;
        ns += if decoding {
            self.telemetry_in(d, View::Truth).projected_ns(
                self.arrival_ns,
                req.tokens,
                self.staged_seqs[d],
            )
        } else {
            req.exec_ns
        };
        let displaced = self
            .telemetry_in(d, View::Truth)
            .displacement(&plan.need, &self.staged_bytes[d]);
        (ns, ns + displaced as u64)
    }

    fn score_argmin(
        &self,
        req: &RequestView<'_>,
        flow: &[(usize, u64)],
        candidates: &[usize],
        view: View,
    ) -> usize {
        candidates
            .iter()
            .map(|&d| self.placement_terms(d, req, flow, view))
            .min_by(|a, b| a.full().total_cmp(&b.full()))
            .map_or_else(|| candidates.first().copied().unwrap_or(0), |t| t.domain)
    }

    #[allow(clippy::similar_names)]
    fn oracle_pick(
        &self,
        req: &Request,
        flow: &[(usize, u64)],
        candidates: &[usize],
        decode_needed: bool,
        decide_ns: u64,
        p: usize,
    ) -> OraclePick {
        let seen = self.view_of(req);
        let m_b = self.score_argmin(&seen, flow, candidates, View::Belief);
        let m_t = self.score_argmin(&seen, flow, candidates, View::Truth);
        let (r_p, r_p_disp) = self.realized_ns(p, req, decode_needed, decide_ns);
        let (r_mb, _) = self.realized_ns(m_b, req, decode_needed, decide_ns);
        let (r_mt, _) = self.realized_ns(m_t, req, decode_needed, decide_ns);
        let mut r_o = r_p;

        let mut r_o_disp = r_p_disp;
        let mut oracle_node = p;
        for &d in candidates {
            let (ns, ns_disp) = self.realized_ns(d, req, decode_needed, decide_ns);
            if ns < r_o {
                r_o = ns;
                oracle_node = d;
            }
            if ns_disp < r_o_disp {
                r_o_disp = ns_disp;
            }
        }
        OraclePick {
            m_b,
            m_t,
            oracle_node,
            r_p,
            r_mb,
            r_mt,
            r_o,
            r_o_disp,
        }
    }

    fn feasible_elsewhere(&self, req: &Request, candidates: &[usize], p: usize) -> bool {
        candidates.iter().any(|&d| {
            if d == p {
                return false;
            }
            let plan = self.plan(d, &req.view(req.tokens), View::Truth);
            self.telemetry(d)
                .could_admit(&plan.need, &self.staged_bytes[d])
                && self.router_admits(d, req, false)
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn finish_regret(
        &mut self,
        req: &Request,
        class: BlobKind,
        p: usize,
        candidates: &[usize],
        pick: OraclePick,
        cost: &Cost,
        decided_by: Option<usize>,
    ) {
        if cost.pending {
            if self.feasible_elsewhere(req, candidates, p) {
                self.feasibility_regret += 1;
            }
            return;
        }
        let charged = cost.service_ns();
        let regret = oracle::decompose(
            charged,
            pick.r_p,
            pick.r_mb,
            pick.r_mt,
            pick.r_o,
            pick.r_o_disp,
        );
        if self.instrument && self.observer.is_some() && pick.r_mb != pick.r_mt {
            self.classify_gap(req, pick.m_b, pick.m_t);
        }
        let regime = oracle::classify(cost);
        self.spans.push(Span {
            op: self.ops,
            class,
            node: p,
            oracle_node: pick.oracle_node,
            regret,
            regime,
            decided_by,
            service_ns: charged,
        });
    }

    fn depths(&self, d: usize, chain: &[(BlobId, BlobMeta)]) -> (usize, usize) {
        let believed = chain
            .iter()
            .take_while(|(id, m)| self.believes_resident(d, id, m.kind))
            .count();
        let truth = chain
            .iter()
            .take_while(|(id, m)| self.ground_truth_resident(d, id, m.kind))
            .count();
        (believed, truth)
    }

    fn classify_gap(&mut self, req: &Request, m_b: usize, m_t: usize) {
        let (believed, truth) = self.depths(m_b, &req.chain);
        if believed > truth {
            self.instruments.gap_phantom += 1;
            return;
        }
        let (believed, truth) = self.depths(m_t, &req.chain);
        if truth > believed {
            self.instruments.gap_miss += 1;
        } else {
            self.instruments.gap_discount += 1;
        }
    }

    fn instrument_decision(&mut self, req: &Request, candidates: &[usize], home: usize) {
        let kv = req
            .chain
            .first()
            .is_some_and(|(_, m)| m.kind == BlobKind::KvBlock);
        if !kv {
            return;
        }
        let now = self.arrival_ns;
        self.instruments.place(&req.chain, home);
        if self.observer.is_none() || !self.domains[home].engine_cache() {
            return;
        }
        self.instruments.decisions += 1;
        let mut exposed = false;
        let mut best: Option<(usize, usize, usize)> = None;
        for &d in candidates {
            let (believed, truth) = self.depths(d, &req.chain);
            if believed == 0 {
                continue;
            }
            if best.is_none_or(|(_, b, _)| believed > b) {
                best = Some((d, believed, truth));
            }
            let Some(o) = self.observer.as_ref() else {
                continue;
            };
            let deepest = &req.chain[believed - 1].0;
            let predicted = req.chain[..believed]
                .iter()
                .map(|(id, _)| o.survival(d, id))
                .fold(1.0f64, f64::min);
            let unknown = o.block_unknown(d, deepest);
            let resident = truth >= believed;
            self.instruments.all.record(predicted, unknown, resident);
            if d == home {
                self.instruments.chosen.record(predicted, unknown, resident);
            }
            if believed > truth {
                exposed = true;
                self.instruments.phantom_depth_blocks += (believed - truth) as u64;
                self.instruments.exposed_chosen += u64::from(d == home);
            }
        }
        self.instruments.exposed_any += u64::from(exposed);
        if let Some((node, believed, truth)) = best
            && node != home
        {
            self.instruments.migrations += 1;
            self.instruments.needless_migrations += u64::from(truth >= believed);
        }
        self.observe_silence(home, now);
        if self.instruments.decisions % DIVERGENCE_EVERY == 0 {
            self.sample_divergence();
        }
    }

    fn observe_silence(&mut self, home: usize, now: u64) {
        let Some(o) = self.observer.as_ref() else {
            return;
        };
        for e in &o.conditions().episodes {
            let share = &mut self.instruments.silence;
            if e.from_ns <= now && now < e.until_ns {
                share.inside += 1;
                share.inside_on_node += u64::from(home == e.node);
            } else {
                share.outside += 1;
                share.outside_on_node += u64::from(home == e.node);
            }
        }
    }

    fn sample_divergence(&mut self) {
        let Some(o) = self.observer.as_ref() else {
            return;
        };
        let (mut phantom, mut miss, mut nodes) = (0.0, 0.0, 0u32);
        for &d in &self.active {
            if !self.domains[d].engine_cache() {
                continue;
            }
            let belief = o.belief(d);
            let (mut believed, mut wrong) = (0u64, 0u64);
            for id in belief.gpu_ids() {
                believed += 1;
                wrong += u64::from(!self.domains[d].is_hot(id, BlobKind::KvBlock));
            }
            let (mut actual, mut missed) = (0u64, 0u64);
            for id in self.domains[d].kv_gpu_ids() {
                actual += 1;
                missed += u64::from(!belief.believes_gpu(&id));
            }
            phantom += wrong as f64 / believed.max(1) as f64;
            miss += missed as f64 / actual.max(1) as f64;
            nodes += 1;
        }
        if nodes > 0 {
            self.instruments.divergence_samples += 1;
            self.instruments.phantom_share += phantom / f64::from(nodes);
            self.instruments.miss_share += miss / f64::from(nodes);
        }
    }

    fn greedy_best(&self, req: &Request, candidates: &[usize]) -> usize {
        candidates
            .iter()
            .copied()
            .max_by_key(|&d| self.resident_value(d, req))
            .unwrap_or(0)
    }

    fn affinity_domain(&self, chain: &[(BlobId, BlobMeta)], candidates: &[usize]) -> usize {
        self.topo.units[self.affinity_unit(chain, candidates)].home as usize
    }

    fn truly_resident(&self, d: usize, req: &Request) -> u64 {
        let depth = self.prefix_len(&req.chain, |id, kind| self.domains[d].is_hot(id, kind));
        let chain_bytes: u64 = req.chain[..depth].iter().map(|(_, m)| m.bytes).sum();
        let dep_bytes: u64 = req
            .requires
            .iter()
            .filter(|(id, m)| self.domains[d].is_hot(id, m.kind))
            .map(|(_, m)| m.bytes)
            .sum();
        chain_bytes + dep_bytes
    }

    pub fn serve_request(&mut self, req: &Request) -> Cost {
        self.arrival_ns += self.interval_ns;
        let now = self.arrival_ns;
        for (h, r) in self.domains.iter_mut().zip(&mut self.reserved) {
            h.release(now);
            r.release(now);
        }
        self.observe_arrival(now);
        let engines: Vec<usize> = self
            .active
            .iter()
            .copied()
            .filter(|&d| self.domains[d].can_decode())
            .collect();
        let n = engines.len().max(1) as u64;
        for d in engines {
            for (sum, bytes) in self.kv_sum.iter_mut().zip(self.domains[d].kv_bytes()) {
                *sum += bytes / n;
            }
        }
        self.kv_samples += 1;
        if let Some(task) = req.completes
            && self.cancelled.remove(&task)
        {
            return Cost {
                pending: true,
                ..Cost::default()
            };
        }
        if let Some(gang) = &req.gang {
            return self.serve_gang(req, gang);
        }
        let decode_needed = Self::needs_decode(req);
        let candidates = if decode_needed {
            self.decode_pool()
        } else {
            self.active.clone()
        };
        let decide_ns = self.decide(req.chain.len(), candidates.len());
        self.decide_ns += decide_ns;
        if self.placement != Placement::Blind {
            self.sticky_unit = self.affinity_unit(&req.chain, &candidates);
        }
        let flow: Vec<(usize, u64)> = req
            .completes
            .filter(|_| self.flow_aware)
            .and_then(|t| self.upstream.get(&t).cloned())
            .unwrap_or_default();
        let scored = self.placement == Placement::Scored;
        let affinity = self.topo.units[self.sticky_unit].home as usize;
        let (best, decided_by) = if scored {
            self.best_scored(&self.view_of(req), &flow, affinity, &candidates)
        } else {
            (self.greedy_best(req, &candidates), None)
        };
        let value = self.resident_value(best, req);

        let target = if scored {
            best
        } else {
            match flow.first() {
                Some(&(d, _)) if !decode_needed || self.domains[d].can_decode() => d,
                _ => self.policy_target(affinity, best, value, &candidates),
            }
        };
        let home = self.topo.units[self.unit_in(target)].home as usize;

        if self.placement == Placement::Aware
            && self.resident_value(target, req) > 0
            && self.truly_resident(target, req) == 0
        {
            self.stale_decisions += 1;
        }

        let pick = self
            .regret
            .then(|| self.oracle_pick(req, &flow, &candidates, decode_needed, decide_ns, home));
        if self.instrument {
            self.instrument_decision(req, &candidates, home);
        }

        if let Some(hint) = &req.hint {
            let recorded = self.tool_anchor.unwrap_or(home);
            self.upstream
                .insert(hint.task, vec![(recorded, hint.payload_bytes)]);
        }
        let sources = req
            .completes
            .and_then(|t| self.upstream.remove(&t))
            .unwrap_or_default();
        let handoff = self.collect(home, &sources);
        let arrival = self.reach(home, req, decode_needed);

        let mut cost = self.run_here(home, req);
        cost.decide_ns = decide_ns;
        cost.transfer_ns += handoff + arrival;
        if let Some(pick) = pick {
            let class = BlobKind::ALL[req.kind_idx()];
            self.finish_regret(req, class, home, &candidates, pick, &cost, decided_by);
        }
        cost
    }

    fn reach(&mut self, home: usize, req: &Request, decode_needed: bool) -> u64 {
        let Some((origin, payload)) = self.origin else {
            return 0;
        };
        if origin == home || !decode_needed || req.completes.is_some() {
            return 0;
        }
        let hop = 2 * self.topo.fetch_ns(self.unit_in(origin), home, payload);
        self.origin_ns += hop;
        self.origin_hops += 1;
        hop
    }

    fn collect(&mut self, home: usize, sources: &[(usize, u64)]) -> u64 {
        let unit = self.unit_in(home);
        let mut total = 0;
        for &(src, payload) in sources {
            if src == home {
                self.joined_tasks += 1;
            } else {
                self.split_tasks += 1;
                let hop = self.topo.fetch_ns(unit, src, payload);
                self.handoff_ns += hop;
                total += hop;
            }
        }
        total
    }

    fn run_here(&mut self, home: usize, req: &Request) -> Cost {
        let class = req.kind_idx();
        if !self.router_admits(home, req, false) {
            self.refused_by_router[class] += 1;
            return Cost {
                pending: true,
                ..Cost::default()
            };
        }
        let shared_before = self.shared_reads;
        let ran_with = self.truly_resident(home, req);

        let plan = self.plan(home, &req.view(req.tokens), View::Belief);
        self.observe_dispatch(home, req);
        let fetch = self.apply_chain(home, req, &plan);
        let mut cost = self.domains[home].access(&req.chain);
        let chain_recompute = cost.recompute_ns;
        cost.transfer_ns += fetch.transfer_ns;

        if !cost.pending {
            if fetch.transfer_ns > 0 {
                self.remote += 1;
            } else if ran_with == 0 {
                self.cold += 1;
            } else {
                self.local += 1;
            }
        }

        if !cost.pending && !req.requires.is_empty() {
            let shipped = self.apply_deps(home, req, &plan);
            let dep = self.domains[home].access_set(&req.requires);
            cost.transfer_ns += shipped.transfer_ns + dep.transfer_ns;
            cost.recompute_ns += dep.recompute_ns;
            cost.pending |= dep.pending;
            cost.preempted |= dep.preempted;
        }

        let mut until = None;
        if !cost.pending {
            cost.dispatch_ns += self.dispatch.ns(DISPATCH_BYTES);
            self.dispatches += 1;
            let (exec, queue) = self.execute(home, req);
            if req.tokens > 0 {
                let seen = &mut self.observed[req.slo.idx()];
                seen.0 += req.tokens;
                seen.1 += 1;
            }
            cost.exec_ns = exec;
            cost.queue_ns = queue;
            self.domains[home].decode_output(&req.chain, &req.produces, chain_recompute, &mut cost);
            self.preempted[class] += u64::from(cost.preempted);
            let decoding = req.tokens > 0 && self.interval_ns > 0;
            if decoding {
                self.observe_sent(home);
            }
            if self.hold_decodes && decoding && !cost.preempted {
                until = Some(self.arrival_ns + queue + exec);
            }
            if let Some(pool) = self.shared.as_mut() {
                for &(id, meta) in req.chain.iter().chain(&req.requires).chain(&req.produces) {
                    let _ = pool.admit(id, meta, false);
                }
            }
            for (k, (now, before)) in self.shared_reads.iter().zip(shared_before).enumerate() {
                if *now > before {
                    self.shared_requests[k] += 1;
                }
            }
        }
        if let Some(end) = until
            && self.domains[home].engine_cache()
        {
            let (blocks, extra) = self.reserve.claim(req, self.tokens_per_block);
            self.reserved[home].commit(&blocks, extra, end);
            self.observe_pin(home, req, end);
        }
        self.domains[home].seal(until);
        self.observe_emit(home);
        cost
    }

    fn execute(&mut self, d: usize, req: &Request) -> (u64, u64) {
        if req.tokens == 0 || self.interval_ns == 0 {
            return (req.exec_ns, 0);
        }
        let step = self.engines[d].decode(self.arrival_ns, req.tokens);
        (step.exec_ns, step.queue_ns)
    }

    fn agent_request(agent: &Agent) -> Request {
        Request {
            phase: 0,
            chain: agent.chain.clone(),
            requires: agent.requires.clone(),
            hint: None,
            completes: None,
            exec_ns: agent.tokens * crate::work::DECODE_NS_PER_TOKEN,
            tokens: agent.tokens,
            gang: None,
            produces: agent.produces.clone(),
            max_tokens: agent.max_tokens,
            slo: agent.slo,
        }
    }

    fn tool_request(tool: &ToolCall) -> Request {
        Request {
            phase: 0,
            chain: tool.chain.clone(),
            requires: Vec::new(),
            hint: None,
            completes: None,
            exec_ns: tool.exec_ns,
            tokens: 0,
            gang: None,
            produces: Vec::new(),
            max_tokens: 0,
            slo: crate::work::Slo::Interactive,
        }
    }

    fn serve_gang(&mut self, req: &Request, gang: &Gang) -> Cost {
        let n = gang.agents.len().max(1);
        let blobs: usize = gang.agents.iter().map(|a| a.chain.len()).sum();

        let decide_ns = self.decide(blobs, self.decode_pool_len());
        self.decide_ns += decide_ns;
        let mut cost = Cost {
            decide_ns,
            ..Cost::default()
        };
        let dispatch: Vec<(usize, u64)> = req
            .completes
            .and_then(|t| self.upstream.remove(&t))
            .unwrap_or_default()
            .into_iter()
            .map(|(d, payload)| (d, payload / n as u64))
            .collect();
        let flow: Vec<(usize, u64)> = if self.flow_aware {
            dispatch.clone()
        } else {
            Vec::new()
        };

        let probes: Vec<Request> = gang.agents.iter().map(Self::agent_request).collect();
        let Some(assign) = self.stage_agents(gang, &probes, &flow, &dispatch) else {
            self.fanouts_refused += 1;
            cost.pending = true;
            if let Some(hint) = &req.hint {
                self.cancelled.insert(hint.task);
            }
            return cost;
        };

        let mut per_node: HashMap<usize, u64> = HashMap::new();
        for (d, _) in assign.iter().flatten() {
            *per_node.entry(*d).or_insert(0) += 1;
        }
        let mut worst = Cost::default();
        let mut results = Vec::with_capacity(probes.len());
        let mut spent = (0u64, 0u64);
        for (i, slot) in assign.iter().enumerate() {
            let Some((d, need)) = *slot else { continue };
            let c = self.run_agent(d, &gang.agents[i], &probes[i], &dispatch);
            self.agents_run += 1;
            if per_node.get(&d).copied().unwrap_or(0) > 1 {
                self.agents_colocated += 1;
            }

            cost.pending |= c.pending;
            if !c.pending {
                spent.0 += c.service_ns();
                spent.1 += need.iter().sum::<u64>();
            }
            if c.service_ns() >= worst.service_ns() {
                worst = c;
            }
            results.push((
                d,
                req.hint.as_ref().map_or(0, |h| h.payload_bytes) / n as u64,
            ));
        }

        if cost.pending {
            self.fanouts_refused += 1;
            self.fanout_wasted_ns += spent.0;
            self.fanout_wasted_bytes += spent.1;
        }
        if let Some(hint) = &req.hint {
            if cost.pending {
                self.cancelled.insert(hint.task);
            } else {
                self.upstream.insert(hint.task, results);
            }
        }
        cost.transfer_ns += worst.transfer_ns;
        cost.recompute_ns += worst.recompute_ns;
        cost.queue_ns += worst.queue_ns;
        cost.exec_ns += worst.exec_ns;
        cost.decide_ns += worst.decide_ns;
        cost.dispatch_ns += worst.dispatch_ns;
        if !cost.pending {
            self.fanouts_admitted += 1;
            self.fanout_service_ns += cost.service_ns();
        }
        cost
    }

    fn stage_agents(
        &mut self,
        gang: &Gang,
        probes: &[Request],
        flow: &[(usize, u64)],
        dispatch: &[(usize, u64)],
    ) -> Option<Vec<Option<(usize, Need)>>> {
        let bytes_of = |r: &Request| -> u64 {
            r.chain
                .iter()
                .chain(&r.requires)
                .map(|(_, m)| m.bytes)
                .sum()
        };
        let mut order: Vec<usize> = (0..probes.len()).collect();
        order.sort_by_key(|&i| std::cmp::Reverse(bytes_of(&probes[i])));
        let mut assign: Vec<Option<(usize, Need)>> = vec![None; probes.len()];
        let mut short = false;
        for &i in &order {
            match self.place_agent(&probes[i], flow) {
                Some((d, need)) => {
                    if self.domains[d].engine_cache() {
                        let (blocks, extra) = self.reserve.claim(&probes[i], self.tokens_per_block);
                        self.staged_kv[d] +=
                            self.reserved[d].uncovered(&blocks, Some(&self.staged[d])) + extra;
                    }
                    for (held, add) in self.staged_bytes[d].iter_mut().zip(need) {
                        *held += add;
                    }
                    self.staged_seqs[d] += 1;
                    for (id, _) in probes[i].chain.iter().chain(&probes[i].requires) {
                        self.staged[d].insert(*id);
                    }
                    assign[i] = Some((d, need));
                }
                None => short = true,
            }
        }
        for d in 0..self.staged.len() {
            self.staged[d].clear();
            self.staged_seqs[d] = 0;
            self.staged_bytes[d] = [0; BlobKind::N];
            self.staged_kv[d] = 0;
        }
        if !short {
            return Some(assign);
        }
        if !self.fanout_atomic {
            for (i, slot) in assign.iter().enumerate() {
                let Some((d, need)) = *slot else { continue };
                let c = self.run_agent(d, &gang.agents[i], &probes[i], dispatch);
                if !c.pending {
                    self.fanout_wasted_ns += c.service_ns();
                    self.fanout_wasted_bytes += need.iter().sum::<u64>();
                }
            }
        }
        None
    }

    fn place_agent(&mut self, probe: &Request, flow: &[(usize, u64)]) -> Option<(usize, Need)> {
        let seen = self.view_of(probe);
        let feasible: Vec<(usize, Need)> = self
            .decode_pool()
            .iter()
            .filter_map(|&d| {
                let need = self.plan(d, &seen, View::Belief).need;
                (self.telemetry(d).could_admit(&need, &self.staged_bytes[d])
                    && self.router_admits(d, probe, true))
                .then_some((d, need))
            })
            .collect();
        let need_on = |d: usize| feasible.iter().find(|(f, _)| *f == d).map(|&(_, n)| n);
        let candidates: Vec<usize> = feasible.iter().map(|&(d, _)| d).collect();
        let affinity = self.affinity_domain(&probe.chain, &candidates);
        let target = if self.placement == Placement::Scored {
            let terms: Vec<Terms> = feasible
                .iter()
                .map(|&(d, _)| self.placement_terms(d, &seen, flow, View::Belief))
                .collect();
            let top = terms
                .iter()
                .min_by(|a, b| a.full().total_cmp(&b.full()))
                .map(|t| (t.domain, t.full()))?;
            match terms.iter().find(|t| t.domain == affinity) {
                Some(a) if a.full() <= top.1 => affinity,
                _ => top.0,
            }
        } else {
            self.unscored_among(probe, flow, &candidates)?
        };
        need_on(target).map(|need| (target, need))
    }

    fn unscored_among(
        &mut self,
        probe: &Request,
        flow: &[(usize, u64)],
        ok: &[usize],
    ) -> Option<usize> {
        if let Some(&(d, _)) = flow.first()
            && ok.contains(&d)
        {
            return Some(d);
        }
        let root = Self::root_key(&probe.chain);
        let hashed = ok
            .iter()
            .copied()
            .max_by_key(|&d| Self::rendezvous(root, d))?;
        Some(match self.placement {
            Placement::Blind => {
                let d = ok[self.next_unit % ok.len()];
                self.next_unit += 1;
                d
            }
            Placement::Sticky => hashed,
            Placement::Aware | Placement::Scored => {
                let best = ok
                    .iter()
                    .copied()
                    .max_by_key(|&d| self.resident_value(d, probe))?;
                if self.resident_value(best, probe) > 0 {
                    best
                } else {
                    hashed
                }
            }
        })
    }

    fn run_agent(
        &mut self,
        d: usize,
        agent: &Agent,
        probe: &Request,
        dispatch: &[(usize, u64)],
    ) -> Cost {
        let mut cost = self.run_here(d, probe);
        if cost.pending {
            return cost;
        }
        cost.transfer_ns += self.collect(d, dispatch);
        for tool in &agent.tools {
            let t = self.run_tool(d, tool);
            cost.transfer_ns += t.transfer_ns;
            cost.recompute_ns += t.recompute_ns;
            cost.decide_ns += t.decide_ns;
            cost.exec_ns += t.exec_ns;
            cost.dispatch_ns += t.dispatch_ns;
        }
        cost
    }

    fn run_tool(&mut self, caller: usize, tool: &ToolCall) -> Cost {
        let probe = Self::tool_request(tool);
        let flow = [(caller, tool.payload_bytes), (caller, tool.payload_bytes)];
        let candidates = self.active.clone();

        let scored = if self.placement == Placement::Scored || !self.flow_aware {
            candidates.len()
        } else {
            1
        };
        let decide_ns = self.decide(probe.chain.len(), scored);
        self.decide_ns += decide_ns;
        let affinity = self.affinity_domain(&probe.chain, &candidates);
        let target = if self.placement == Placement::Scored {
            self.best_scored(&self.view_of(&probe), &flow, affinity, &candidates)
                .0
        } else if self.flow_aware {
            caller
        } else {
            let best = self.greedy_best(&probe, &candidates);
            let value = self.resident_value(best, &probe);
            self.policy_target(affinity, best, value, &candidates)
        };
        self.tool_calls += 1;
        let mut cost = self.run_here(target, &probe);
        if cost.pending {
            self.tool_refused += 1;
        }
        if target == caller {
            self.tool_coplaced += 1;
        } else {
            let hop = self
                .topo
                .fetch_ns(self.unit_in(caller), target, tool.payload_bytes);
            self.handoff_ns += 2 * hop;
            cost.transfer_ns += 2 * hop;
        }
        cost.decide_ns = decide_ns;
        self.tool_ns += cost.service_ns();
        cost
    }

    #[must_use]
    pub fn domain_spread(&self) -> f64 {
        let used: Vec<u64> = self
            .active
            .iter()
            .map(|&d| self.telemetry(d).used())
            .collect();
        let hi = used.iter().copied().max().unwrap_or(0) as f64;
        let lo = used.iter().copied().min().unwrap_or(0).max(1) as f64;
        hi / lo
    }

    #[must_use]
    pub fn memory_coupled(&self) -> (u64, u64) {
        self.domains.iter().fold((0, 0), |(c, n), h| {
            let (hc, hn) = h.ddr_memory_coupled();
            (c + hc, n + hn)
        })
    }
}

#[derive(Clone, Copy, Debug)]
struct Terms {
    domain: usize,
    acquire: f64,
    fetched: bool,
    displaced: f64,
    handoff: f64,
    engine: f64,
    congestion: f64,

    need: Need,
}

pub const TERM_COUNT: usize = 5;
pub const TERM_LABELS: [&str; TERM_COUNT] =
    ["acquire", "displaced", "handoff", "engine", "congestion"];

impl Terms {
    const EACH: [fn(&Terms) -> f64; TERM_COUNT] = [
        |t| t.acquire,
        |t| t.displaced,
        |t| t.handoff,
        |t| t.engine,
        |t| t.congestion,
    ];

    fn acquire_only(&self) -> f64 {
        self.acquire
    }
    fn net(&self) -> f64 {
        self.acquire + self.displaced
    }
    fn placed(&self) -> f64 {
        self.acquire + self.displaced + self.handoff
    }
    fn loaded(&self) -> f64 {
        self.placed() + self.engine
    }
    fn full(&self) -> f64 {
        self.loaded() + self.congestion
    }
}

#[derive(Clone, Copy, Debug)]
struct OraclePick {
    m_b: usize,
    m_t: usize,
    oracle_node: usize,
    r_p: u64,
    r_mb: u64,
    r_mt: u64,
    r_o: u64,
    r_o_disp: u64,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Source {
    Peer(usize),
    Shared,
}

#[derive(Clone, Debug)]
struct Plan {
    ns: u64,
    local_depth: usize,
    chain_cut: usize,
    chain_src: Option<Source>,
    chain_bytes: u64,
    deps: Vec<(usize, Source)>,

    need: Need,
}

use crate::tele::Need;

const QUERY_BYTES_PER_BLOB: u64 = 40;

pub const HOOK_BYTES: u64 = 64;

pub const DISPATCH_BYTES: u64 = 1024;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::{NodeMemory, Policy, Quota};
    use crate::topo::{Distance, Topology};
    use crate::work::Workload;

    fn machine(nodes: usize) -> Machine {
        let bands = [0u8; BlobKind::N];
        let mem = NodeMemory {
            hbm: 0,
            ddr: 64 << 30,
            nvme: 0,
            hbm_quota: Quota::open(0, bands),
            ddr_quota: Quota::open(64 << 30, bands),
            can_decode: true,
            kv: None,
        };
        let topo = Topology::cluster(nodes, 1, mem.ddr, Distance::Socket, Crossing::default());
        Machine::new(topo, |_| mem, Policy::Gdsf, Placement::Scored)
    }

    #[test]
    #[allow(clippy::cast_possible_wrap)]
    fn sidecar_tax_matches_closed_form_without_fanout() {
        let hook = Crossing {
            fixed_ns: 36_000.0,
            ns_per_byte: 0.0,
        };
        let dispatch_integrated = Crossing {
            fixed_ns: 6_000.0,
            ns_per_byte: 0.0,
        };
        let dispatch_sidecar = Crossing {
            fixed_ns: 15_000.0,
            ns_per_byte: 0.0,
        };
        let trace: Vec<_> = Workload::new(1, 500, 1.0).collect();

        let mut base = machine(4);
        base.set_data_path(
            DataPath::Integrated,
            Crossing::default(),
            dispatch_integrated,
        );
        let (mut base_total, mut base_served) = (0i64, 0u64);
        for req in &trace {
            let c = base.serve_request(req);
            if !c.pending {
                base_total += c.total_ns() as i64;
                base_served += 1;
            }
        }

        let mut side = machine(4);
        side.set_data_path(DataPath::Sidecar, hook, dispatch_sidecar);
        let (mut side_total, mut side_served) = (0i64, 0u64);
        for req in &trace {
            let c = side.serve_request(req);
            if !c.pending {
                side_total += c.total_ns() as i64;
                side_served += 1;
            }
        }

        assert_eq!(
            base_served, side_served,
            "no fan-out: admission must not depend on the data path"
        );
        assert_eq!(base.decisions, side.decisions);
        assert_eq!(base.dispatches, side.dispatches);
        assert_eq!(
            side.dispatches, side_served,
            "one dispatch per served request without gangs"
        );

        assert_eq!(
            side.decisions,
            trace.len() as u64,
            "fixture must refuse nothing: a refused request is decided but never totalled"
        );

        let dispatch_delta = dispatch_sidecar.ns(DISPATCH_BYTES) as i64
            - dispatch_integrated.ns(DISPATCH_BYTES) as i64;
        let predicted = side.decisions as i64 * hook.ns(HOOK_BYTES) as i64
            + side.dispatches as i64 * dispatch_delta;
        let realized = side_total - base_total;
        assert_eq!(
            predicted, realized,
            "closed-form tax must match the simulator's own total exactly outside gangs"
        );
    }

    fn regret_fixture(seed: u64, ops: u64) -> Vec<Request> {
        Workload::new(seed, ops, 1.0)
            .with_tool_profile(0.0, 0)
            .collect()
    }

    #[test]
    fn execution_gap_is_zero_under_unified_and_non_saturated() {
        let mut mach = machine(4);
        mach.set_regret(true);
        let trace = regret_fixture(2, 600);
        let mut served = 0u64;
        for req in &trace {
            if !mach.serve_request(req).pending {
                served += 1;
            }
        }
        assert!(
            !mach.spans.is_empty(),
            "fixture must serve at least one request"
        );
        assert_eq!(
            mach.spans.len() as u64,
            served,
            "one span per served, non-gang request"
        );
        for span in &mach.spans {
            assert_eq!(
                span.regret.execution, 0,
                "execution gap must be zero under Control::Unified with no gangs: {span:?}"
            );
        }
    }

    #[test]
    fn execution_gap_has_a_bounded_residual_under_a_deliberately_tight_pool() {
        let bands = [0u8; BlobKind::N];
        let mem = NodeMemory {
            hbm: 0,
            ddr: 2 << 30,
            nvme: 64 << 30,
            hbm_quota: Quota::open(0, bands),
            ddr_quota: Quota::open(2 << 30, bands),
            can_decode: true,
            kv: None,
        };
        let topo = Topology::cluster(4, 1, mem.ddr, Distance::Socket, Crossing::default());
        let mut mach = Machine::new(topo, |_| mem, Policy::Gdsf, Placement::Scored);
        mach.set_regret(true);
        mach.set_arrival_rate(250.0);
        for req in &regret_fixture(2, 3000) {
            mach.serve_request(req);
        }
        assert!(!mach.spans.is_empty());
        let nonzero = mach
            .spans
            .iter()
            .filter(|s| s.regret.execution != 0)
            .count();
        let share = nonzero as f64 / mach.spans.len() as f64;
        assert!(
            share < 0.20,
            "residual grew past its measured band ({share:.2} of spans); re-examine \
             whether this is still the same-request ordering effect or a new one"
        );
        for s in &mach.spans {
            if s.regret.execution != 0 {
                assert_eq!(s.regret.heuristic, 0, "{s:?}");
                assert_eq!(s.regret.belief, 0, "{s:?}");
                assert_eq!(s.regret.model, 0, "{s:?}");
            }
        }
    }

    #[test]
    fn regret_decomposition_sums_to_total_on_every_span() {
        let mut mach = machine(4);
        mach.set_regret(true);
        for req in &regret_fixture(3, 600) {
            mach.serve_request(req);
        }
        assert!(!mach.spans.is_empty());
        for span in &mach.spans {
            let r = span.regret;
            assert_eq!(
                r.execution + r.heuristic + r.belief + r.model,
                r.total,
                "{span:?}"
            );
        }
    }

    #[test]
    fn belief_gap_is_zero_under_unified_and_query() {
        let trace = regret_fixture(4, 600);
        for control in [Control::Unified, Control::Query] {
            let mut mach = machine(4);
            mach.set_control(control, Crossing::default());
            mach.set_regret(true);
            for req in &trace {
                mach.serve_request(req);
            }
            assert!(!mach.spans.is_empty(), "{control:?}");
            for span in &mach.spans {
                assert_eq!(
                    span.regret.belief, 0,
                    "{control:?}: belief gap must be zero with no stale view: {span:?}"
                );
            }
        }
    }

    #[test]
    fn decided_by_reduces_to_moved_by_congestion() {
        let mut mach = machine(4);
        mach.set_regret(true);
        for req in &regret_fixture(5, 600) {
            mach.serve_request(req);
        }
        assert!(!mach.spans.is_empty());
        let from_spans = mach
            .spans
            .iter()
            .filter(|s| s.decided_by == Some(4))
            .count() as u64;
        assert_eq!(from_spans, mach.moved_by_congestion);
    }

    #[test]
    fn feasible_elsewhere_requires_a_genuinely_admitting_candidate() {
        let bands = [0u8; BlobKind::N];

        let mem = NodeMemory {
            hbm: 0,
            ddr: 1,
            nvme: 0,
            hbm_quota: Quota::open(0, bands),
            ddr_quota: Quota::open(1, bands),
            can_decode: true,
            kv: None,
        };
        let topo = Topology::cluster(4, 1, mem.ddr, Distance::Socket, Crossing::default());
        let mut mach = Machine::new(topo, |_| mem, Policy::Gdsf, Placement::Scored);
        mach.set_regret(true);
        let mut refused = 0u64;
        for req in &regret_fixture(6, 300) {
            if mach.serve_request(req).pending {
                refused += 1;
            }
        }
        assert!(refused > 0, "fixture must refuse with a 1-byte pool");
        assert_eq!(mach.feasibility_regret, 0);
    }

    #[test]
    fn locality_coupled_counts_match_scored_decisions_and_can_be_nonzero() {
        let mut mach = machine(4);
        mach.set_regret(true);
        mach.set_flow_aware(true);
        mach.set_state_transfer(true);
        for req in &regret_fixture(7, 1500) {
            mach.serve_request(req);
        }
        assert_eq!(mach.locality_coupled_decisions, mach.scored_decisions);
        assert!(mach.locality_coupled_decisions > 0);
        assert!(
            mach.locality_coupled <= mach.locality_coupled_decisions,
            "a count can never exceed its own denominator"
        );
        assert!(
            mach.locality_coupled > 0,
            "a flow-aware scored arm over a multi-node fixture must find at least one decision \
             the handoff term alone moved"
        );
    }

    fn engine_machine(hbm: u64, partition: u64, control: Control) -> Machine {
        let bands = [0u8; BlobKind::N];
        let mem = NodeMemory {
            hbm,
            ddr: 8 << 30,
            nvme: 64 << 30,
            hbm_quota: Quota::open(hbm, bands),
            ddr_quota: Quota::open(8 << 30, bands),
            can_decode: true,
            kv: Some(crate::cache::EngineKv {
                partition,
                offload: if hbm > 0 { 1 << 30 } else { 0 },
                spill: 8 << 30,
                clairvoyant: false,
            }),
        };
        let topo = Topology::cluster(4, 1, mem.ddr, Distance::Rack, Crossing::default());
        let mut mach = Machine::new(topo, |_| mem, Policy::Gdsf, Placement::Scored);
        mach.set_control(control, Crossing::default());
        mach
    }

    fn decode_trace(seed: u64, ops: u64) -> Vec<Request> {
        Workload::with_fanout(seed, ops, 1.0, 0.1)
            .with_decode_kv(crate::work::TOKENS_PER_KV_BLOCK)
            .collect()
    }

    #[test]
    fn census_kvblock_row_is_zero_on_a_cluster_with_the_bit_on() {
        for control in [Control::Unified, Control::Gossip { period: 50 }] {
            let mut mach = engine_machine(4 << 30, 1 << 30, control);
            mach.set_state_transfer(true);
            mach.set_flow_aware(true);
            mach.set_hold_decodes(true);
            mach.set_arrival_rate(250.0);
            let trace = decode_trace(3, 2_000);
            for (i, req) in trace.iter().enumerate() {
                if i == 1_000 {
                    mach.drain(1);
                }
                mach.serve_request(req);
            }
            let ops = mach.engine_ops();
            let k = BlobKind::KvBlock.idx();
            assert_eq!(
                [
                    ops.admit[k],
                    ops.touch[k],
                    ops.anticipate[k],
                    ops.demote[k],
                    ops.forget_cold[k],
                    ops.superseded[k],
                    ops.spill[k],
                    ops.drain[k],
                ],
                [0; 8],
                "{control:?}"
            );
            assert!(ops.total(BlobKind::WeightShard) > 0);
            assert_eq!(mach.kv_orphans(), 0, "{control:?}");
        }
    }

    #[test]
    fn belief_gap_stays_zero_and_regret_still_sums_with_the_bit_on() {
        let trace = regret_fixture(4, 600);
        for control in [Control::Unified, Control::Query] {
            let mut mach = engine_machine(0, 4 << 30, control);
            mach.set_regret(true);
            for req in &trace {
                mach.serve_request(req);
            }
            assert!(!mach.spans.is_empty(), "{control:?}");
            for span in &mach.spans {
                let r = span.regret;
                assert_eq!(r.belief, 0, "{control:?}: {span:?}");
                assert_eq!(r.execution + r.heuristic + r.belief + r.model, r.total);
            }
        }
    }

    #[test]
    fn only_prompt_only_reservations_let_the_engine_preempt() {
        let trace = decode_trace(5, 3_000);
        let mut preempted = Vec::new();
        for reserve in Reserve::ALL {
            let mut mach = engine_machine(4 << 30, 96 << 20, Control::Unified);
            mach.set_state_transfer(true);
            mach.set_fanout_atomic(true);
            mach.set_hold_decodes(true);
            mach.set_admission(reserve, crate::work::TOKENS_PER_KV_BLOCK);
            mach.set_arrival_rate(250.0);
            for req in &trace {
                mach.serve_request(req);
            }
            assert_eq!(mach.kv_orphans(), 0, "{reserve:?}");
            preempted.push((reserve, mach.preemptions(), mach.refused_by_router));
        }
        for &(reserve, n, refused) in &preempted {
            match reserve {
                Reserve::Bound | Reserve::Perfect => {
                    assert_eq!(n, 0, "{reserve:?} covers every byte it pins");
                    assert!(
                        refused.iter().sum::<u64>() > 0,
                        "{reserve:?} must bind here"
                    );
                }
                Reserve::Prompt => assert!(n > 0, "the fixture must overcommit under none"),
            }
        }
    }

    fn stream_machine(partition: u64, offload: u64, spill: u64) -> Machine {
        let bands = [0u8; BlobKind::N];
        let mem = NodeMemory {
            hbm: 4 << 30,
            ddr: 8 << 30,
            nvme: 64 << 30,
            hbm_quota: Quota::open(4 << 30, bands),
            ddr_quota: Quota::open(8 << 30, bands),
            can_decode: true,
            kv: Some(crate::cache::EngineKv {
                partition,
                offload,
                spill,
                clairvoyant: false,
            }),
        };
        let topo = Topology::cluster(4, 1, mem.ddr, Distance::Rack, Crossing::default());
        Machine::new(topo, |_| mem, Policy::Gdsf, Placement::Scored)
    }

    fn replay_matches_every_tier(mach: &mut Machine, replicas: &mut [crate::stream::Index]) {
        use crate::stream::Medium;
        for (d, replica) in replicas.iter_mut().enumerate() {
            for event in mach.domains[d].take_kv_events() {
                replica.apply(&event);
            }
            for medium in Medium::ALL {
                assert_eq!(
                    replica.sorted_ids(medium),
                    mach.domains[d].kv_ids(medium),
                    "domain {d}, {medium:?}"
                );
            }
        }
    }

    #[test]
    fn the_event_stream_reproduces_every_tier_after_every_request() {
        use crate::stream::{Index, Medium};
        let mut mach = stream_machine(64 << 20, 32 << 20, 48 << 20);
        mach.record_kv_events(true);
        mach.set_state_transfer(true);
        mach.set_flow_aware(true);
        mach.set_hold_decodes(true);
        mach.set_shared_l2(Some(256 << 20));
        mach.set_arrival_rate(250.0);
        let mut replicas = vec![Index::default(); 4];
        let mut totals = [0usize; 3];
        for (i, req) in decode_trace(3, 2_500).iter().enumerate() {
            if i == 1_500 {
                mach.drain(1);
            }
            mach.serve_request(req);
            replica_totals(&mach, &mut totals);
            replay_matches_every_tier(&mut mach, &mut replicas);
        }
        assert!(
            totals.iter().all(|&t| t > 0),
            "the fixture must populate every tier, or the invariant is untested: {totals:?}"
        );
        let evicted = mach
            .domains
            .iter()
            .map(|h| h.evicted()[BlobKind::KvBlock.idx()])
            .sum::<u64>();
        assert!(evicted > 0, "the fixture must evict");
        assert_eq!(
            replicas[1].len(Medium::Gpu),
            0,
            "the drained node's stream cleared it"
        );
    }

    fn replica_totals(mach: &Machine, totals: &mut [usize; 3]) {
        use crate::stream::Medium;
        for h in &mach.domains {
            for medium in Medium::ALL {
                totals[medium.idx()] = totals[medium.idx()].max(h.kv_ids(medium).len());
            }
        }
    }

    #[test]
    fn a_node_that_never_records_emits_nothing() {
        let mut mach = stream_machine(64 << 20, 32 << 20, 48 << 20);
        mach.set_arrival_rate(250.0);
        for req in &decode_trace(3, 300) {
            mach.serve_request(req);
        }
        assert!(
            mach.domains
                .iter_mut()
                .all(|h| h.take_kv_events().is_empty())
        );
    }

    fn gate_machine(control: Control, belief: bool) -> Machine {
        let mut mach = stream_machine(64 << 20, 32 << 20, 48 << 20);
        mach.set_control(control, Crossing::default());
        mach.set_state_transfer(true);
        mach.set_flow_aware(true);
        mach.set_hold_decodes(true);
        mach.set_shared_l2(Some(512 << 20));
        mach.set_arrival_rate(250.0);
        if belief {
            mach.set_belief(Conditions::exact());
        }
        mach
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn an_exact_belief_changes_nothing_and_equals_the_truth_after_every_request() {
        use crate::stream::Medium;
        for control in [
            Control::Unified,
            Control::Query,
            Control::Gossip { period: 50 },
        ] {
            let mut plain = gate_machine(control, false);
            let mut seen = gate_machine(control, true);
            for (i, req) in decode_trace(3, 2_500).iter().enumerate() {
                if i == 1_500 {
                    plain.drain(1);
                    seen.drain(1);
                }
                let a = plain.serve_request(req);
                let b = seen.serve_request(req);
                if !matches!(control, Control::Gossip { .. }) {
                    assert_eq!(
                        format!("{a:?}"),
                        format!("{b:?}"),
                        "{control:?} request {i}"
                    );
                }
                let observer = seen.observer().expect("the belief is on");
                for d in 0..4 {
                    let belief = observer.belief(d);
                    for medium in Medium::ALL {
                        assert_eq!(
                            belief.index().sorted_ids(medium),
                            seen.domains[d].kv_ids(medium),
                            "{control:?} request {i} domain {d} {medium:?}"
                        );
                    }
                    assert_eq!(
                        belief.gpu_bytes(),
                        belief.index().bytes(Medium::Gpu),
                        "{control:?} request {i} domain {d}: nothing optimistic outlives its window"
                    );
                    for id in seen.domains[d].kv_ids(Medium::Gpu) {
                        assert_eq!(observer.survival(d, &id), 1.0);
                    }
                }
            }
            if matches!(control, Control::Gossip { .. }) {
                continue;
            }
            for (name, x, y) in [
                ("fetches", plain.fetches, seen.fetches),
                ("rebuilds", plain.rebuilds, seen.rebuilds),
                ("stale_fetches", plain.stale_fetches, seen.stale_fetches),
                ("decisions", plain.decisions, seen.decisions),
                (
                    "held_by_affinity",
                    plain.held_by_affinity,
                    seen.held_by_affinity,
                ),
                (
                    "moved_by_displacement",
                    plain.moved_by_displacement,
                    seen.moved_by_displacement,
                ),
                ("moved_by_flow", plain.moved_by_flow, seen.moved_by_flow),
                ("moved_by_load", plain.moved_by_load, seen.moved_by_load),
                (
                    "moved_by_congestion",
                    plain.moved_by_congestion,
                    seen.moved_by_congestion,
                ),
                ("preemptions", plain.preemptions(), seen.preemptions()),
            ] {
                assert_eq!(x, y, "{control:?} {name}");
            }
            assert!(
                plain.preemptions() > 0 || plain.fetches > 0,
                "{control:?}: the fixture must exercise something"
            );
        }
    }

    #[test]
    fn an_eviction_without_its_event_changes_no_telemetry_read() {
        use crate::stream::Medium;
        use crate::tier::Tier;
        let mut mach = gate_machine(Control::Unified, true);
        for req in &decode_trace(3, 600) {
            mach.serve_request(req);
        }
        let d = (0..4)
            .max_by_key(|&d| mach.domains[d].kv_ids(Medium::Gpu).len())
            .expect("four domains");
        let gpu = mach.domains[d].kv_ids(Medium::Gpu);
        assert!(gpu.len() > 4, "the fixture must leave blocks resident");
        let victim = gpu[gpu.len() / 2];
        let meta = BlobMeta {
            kind: BlobKind::KvBlock,
            bytes: crate::work::KV_BLOCK_BYTES,
            parent: None,
            recompute_ns: crate::work::KV_BLOCK_NS,
        };
        let need = [meta.bytes, 0, 0, 0];
        let none = [0u64; BlobKind::N];
        let reads = |m: &Machine| {
            let t = m.telemetry(d);
            (
                t.resident(&victim, BlobKind::KvBlock),
                t.held(&victim, BlobKind::KvBlock),
                t.resident_bytes(BlobKind::KvBlock),
                t.local_ns(&victim, &meta),
                t.displacement(&need, &none).to_bits(),
                t.displacement_in(Tier::Hbm, &need, &none).to_bits(),
                t.marginal_price(Tier::Hbm).to_bits(),
            )
        };
        let before = reads(&mach);
        assert!(before.0 && before.1);
        assert!(mach.domains[d].evict_unrecorded(&victim));
        assert!(!mach.domains[d].is_hot(&victim, BlobKind::KvBlock));
        assert_eq!(reads(&mach), before);
        let truth_only = Telemetry::new(&mach.domains[d], &mach.engines[d]);
        assert!(!truth_only.resident(&victim, BlobKind::KvBlock));
    }

    #[test]
    fn every_scoring_rule_makes_the_same_decisions_on_an_exact_belief() {
        use crate::belief::Scoring;
        let trace = decode_trace(3, 1_200);
        let mut plain = gate_machine(Control::Unified, false);
        let reference: Vec<String> = trace
            .iter()
            .map(|req| format!("{:?}", plain.serve_request(req)))
            .collect();
        for scoring in [
            Scoring::FaceValue,
            Scoring::Expected,
            Scoring::Quantile(0.5),
            Scoring::Quantile(0.9),
            Scoring::Quantile(0.99),
            Scoring::Slo,
        ] {
            let mut seen = gate_machine(Control::Unified, true);
            seen.set_scoring(scoring);
            for (i, req) in trace.iter().enumerate() {
                assert_eq!(
                    format!("{:?}", seen.serve_request(req)),
                    reference[i],
                    "{scoring:?} request {i}"
                );
            }
            assert_eq!(seen.moved_by_load, plain.moved_by_load, "{scoring:?}");
            assert_eq!(seen.held_by_affinity, plain.held_by_affinity, "{scoring:?}");
        }
    }

    #[test]
    fn the_truth_view_prices_from_the_truth_while_the_belief_view_keeps_believing() {
        use crate::belief::Episode;
        use crate::stream::Medium;
        let mut mach = gate_machine(Control::Unified, false);
        mach.set_shared_l2(None);
        mach.set_state_transfer(false);
        mach.set_belief(Conditions {
            episodes: vec![Episode {
                node: 0,
                from_ns: u64::MAX - 1,
                until_ns: u64::MAX,
            }],
            ..Conditions::exact()
        });
        for req in &decode_trace(3, 600) {
            mach.serve_request(req);
        }
        let d = (0..4)
            .max_by_key(|&d| mach.domains[d].kv_ids(Medium::Gpu).len())
            .expect("four domains");
        let gpu = mach.domains[d].kv_ids(Medium::Gpu);
        let victim = gpu[gpu.len() / 2];
        let meta = BlobMeta {
            kind: BlobKind::KvBlock,
            bytes: crate::work::KV_BLOCK_BYTES,
            parent: None,
            recompute_ns: crate::work::KV_BLOCK_NS,
        };
        let req = Request {
            phase: 0,
            chain: vec![(victim, meta)],
            requires: Vec::new(),
            hint: None,
            completes: None,
            exec_ns: 0,
            tokens: 0,
            gang: None,
            produces: Vec::new(),
            max_tokens: 0,
            slo: crate::work::Slo::Interactive,
        };
        assert!(mach.domains[d].evict_unrecorded(&victim));
        let believed = mach.plan(d, &req.view(0), View::Belief);
        let actual = mach.plan(d, &req.view(0), View::Truth);
        assert_eq!(believed.local_depth, 1);
        assert_eq!(believed.ns, 0);
        assert_eq!(actual.local_depth, 0);
        assert_eq!(actual.ns, mach.domains[d].local_ns(&victim, &meta));
        assert!(actual.ns > 0);
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn on_an_exact_belief_the_instruments_read_zero_and_every_prediction_is_certain() {
        use crate::instruments::CERTAIN;
        let mut mach = gate_machine(Control::Unified, true);
        mach.set_instrument(true);
        mach.set_regret(true);
        for req in &decode_trace(3, 1_500) {
            mach.serve_request(req);
        }
        let i = &mach.instruments;
        assert!(i.decisions > 100, "{}", i.decisions);
        assert_eq!(i.exposed_any, 0);
        assert_eq!(i.gaps(), 0);
        assert_eq!(i.phantom_share, 0.0);
        assert_eq!(i.miss_share, 0.0);
        assert!(i.divergence_samples > 0);
        assert!(i.all.total() > 0);
        assert_eq!(i.all.bins[CERTAIN].n, i.all.total());
        assert_eq!(i.all.bins[CERTAIN].resident, i.all.total());
        assert_eq!(i.chosen.bins[CERTAIN].resident, i.chosen.total());
    }

    #[test]
    fn a_lagged_belief_is_exposed_and_its_predictions_are_not_all_certain() {
        use crate::belief::{LoadSource, Recovery};
        use crate::instruments::CERTAIN;
        let mut mach = gate_machine(Control::Unified, false);
        mach.set_belief(Conditions {
            cadence: true,
            lag_ns: 30_000,
            loss: 0.05,
            recovery: Recovery::None,
            period_ns: 1_000_000_000,
            episodes: Vec::new(),
            seed: 1,
            load: LoadSource::Path,
        });
        mach.set_instrument(true);
        for req in &decode_trace(3, 2_500) {
            mach.serve_request(req);
        }
        let i = &mach.instruments;
        assert!(
            i.exposed_any > 0,
            "loss without recovery must leave phantoms"
        );
        assert!(i.phantom_share > 0.0);
        assert!(i.all.bins[CERTAIN].n < i.all.total());
        assert_eq!(
            i.all.bins[CERTAIN].resident, i.all.bins[CERTAIN].n,
            "a phantom with no unapplied step behind it is a reconciliation defect"
        );
    }

    #[test]
    fn the_score_cannot_see_the_exact_output_length_under_observables() {
        let request = |tokens: u64| Request {
            phase: 0,
            chain: Vec::new(),
            requires: Vec::new(),
            hint: None,
            completes: None,
            exec_ns: 0,
            tokens,
            gang: None,
            produces: Vec::new(),
            max_tokens: 900,
            slo: crate::work::Slo::Interactive,
        };
        let mut mach = gate_machine(Control::Unified, false);
        assert_eq!(mach.view_of(&request(50)).tokens, 50);
        assert_eq!(mach.view_of(&request(200)).tokens, 200);
        mach.set_observables(true);
        assert_eq!(mach.view_of(&request(50)).tokens, 900);
        assert_eq!(mach.view_of(&request(200)).tokens, 900);
        assert_eq!(mach.view_of(&request(0)).tokens, 0);
        for req in &decode_trace(3, 400) {
            mach.serve_request(req);
        }
        assert_eq!(
            mach.view_of(&request(50)).tokens,
            mach.view_of(&request(200)).tokens
        );
        assert_ne!(mach.view_of(&request(50)).tokens, 900);
    }
}
