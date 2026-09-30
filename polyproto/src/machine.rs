use crate::admit::{Reservations, Reserve};
use crate::belief::{Cause, Conditions, Marks, Observer, SLO_QUANTILE, Scoring};
use crate::blob::{BlobId, BlobKind, BlobMeta};
use crate::boundary::Cost as Crossing;
use crate::cache::{Cost, Hierarchy, NodeMemory, Policy};
use crate::engine::{Batching, Engine, MAX_BATCH, MODEL_COUNT, Model, PrefillLoad};
use crate::fleet::{Costs, Fleet, FleetView, PlannerKind, Role, moves_between, retarget};
use crate::flow::FlowHint;
use crate::foresight::Foresight;
use crate::instruments::{Instruments, Reuse};
use crate::oracle;
use crate::span::Span;
use crate::stream::{KvEvent, Mark, Medium, Rank};
use crate::tele::Telemetry;
use crate::tier::TierSpec;
use crate::topo::Topology;
use crate::work::{
    Agent, Gang, Origin, Origins, Request, RequestView, Slo, ToolCall, WEIGHT_BYTES, WEIGHT_NS,
    model_of,
};
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};

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

pub const EVICT_FIRST_LEASE_NS: u64 = 30_000_000_000;

#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum Target {
    #[default]
    Argmin,
    Deepest,
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Emit {
    Declared { retain: bool, evict_first: bool },
    Oracle { horizon_ns: u64 },
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub struct Directives {
    pub emit: Emit,
    pub ignores: bool,
    pub marks: Marks,
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
    directives: Option<Directives>,
    foresight: Option<Foresight>,
    position: u64,
    prefill_ahead: bool,
    prefill_target: Target,
    landing: HashMap<u64, usize>,
    redispatched: Vec<HashSet<BlobId>>,
    origins: Option<Origins>,
    priced_models: bool,
    priced_prefill: bool,
    fleet: Option<Fleet>,
    partition_override: Option<u64>,
    placements: Vec<(u64, Vec<Option<Model>>)>,
    planner: Option<PlannerState>,
    fleet_view: FleetView,
    pairing: Pairing,
    prefill_fetch: bool,
    pair_over_ns: u64,
    prefill_free_at: Vec<u64>,
    list_cursor: usize,
    pub pair_stats: PairStats,
    tenant_set: usize,
    tenant_prefill_ns_per_s: f64,
    tenant_slots: usize,
    buckets: HashMap<u32, (f64, u64)>,
    tenant_flight: HashMap<u32, BinaryHeap<Reverse<u64>>>,
    pub tenant_refused: HashMap<u32, u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Pairing {
    #[default]
    Off,
    List,
    Independent,
    Joint,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct PairStats {
    pub decisions: u64,
    pub paired: u64,
    pub coupled: u64,
    pub failed: u64,
    pub wait_ns: u64,
    pub work_ns: u64,
    pub avoided_ns: u64,
    pub transfer_ns: u64,
}

#[derive(Clone, Copy, Debug)]
struct Quote {
    node: usize,
    wait_ns: u64,
    work_ns: u64,
    transfer_ns: u64,
    toll_ns: f64,
}

impl Quote {
    fn total_ns(&self) -> f64 {
        (self.wait_ns + self.work_ns + self.transfer_ns) as f64 + self.toll_ns
    }
}

#[derive(Clone, Copy, Debug)]
struct PlannerState {
    kind: PlannerKind,
    interval_ns: u64,
    next_at: u64,
    accrued_ns: f64,
    costs: Costs,
    ticks: u64,
    seen: [bool; MODEL_COUNT],
}

const DIVERGENCE_EVERY: u64 = 16;

impl Machine {
    #[must_use]
    #[allow(clippy::too_many_lines)]
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
            directives: None,
            foresight: None,
            position: 0,
            prefill_ahead: false,
            prefill_target: Target::Argmin,
            landing: HashMap::new(),
            redispatched: vec![HashSet::new(); n_domains],
            origins: None,
            priced_models: false,
            priced_prefill: false,
            fleet: None,
            partition_override: None,
            placements: Vec::new(),
            planner: None,
            fleet_view: FleetView::default(),
            pairing: Pairing::Off,
            prefill_fetch: false,
            pair_over_ns: 0,
            prefill_free_at: vec![0; n_domains],
            list_cursor: 0,
            pair_stats: PairStats::default(),
            tenant_set: 0,
            tenant_prefill_ns_per_s: 0.0,
            tenant_slots: 0,
            buckets: HashMap::new(),
            tenant_flight: HashMap::new(),
            tenant_refused: HashMap::new(),
        }
    }

    pub fn set_planner(&mut self, kind: PlannerKind, interval_ns: u64) {
        let interval_ns = interval_ns.max(1);
        let costs = self.fleet.as_ref().map_or_else(
            || Costs::published(interval_ns),
            |f| Costs::of(f.catalogue(), interval_ns),
        );
        self.planner = Some(PlannerState {
            kind,
            interval_ns,
            next_at: interval_ns,
            accrued_ns: 0.0,
            costs,
            ticks: 0,
            seen: [false; MODEL_COUNT],
        });
    }

    pub fn set_tenant_set(&mut self, replicas: usize) {
        self.tenant_set = replicas;
    }

    pub fn set_tenant_quota(&mut self, prefill_ns_per_s: f64, slots: usize) {
        self.tenant_prefill_ns_per_s = prefill_ns_per_s;
        self.tenant_slots = slots;
    }

    pub fn set_tenant_floor(&mut self, bytes: u64) {
        for h in &mut self.domains {
            h.set_tenant_floor(bytes);
        }
    }

    fn home_plan<'p>(&self, home: usize, req: &Request, planned: &'p mut Option<Plan>) -> &'p Plan {
        planned.get_or_insert_with(|| self.plan(home, &req.view(req.tokens), View::Belief))
    }

    fn meters_admit(&mut self, home: usize, req: &Request, planned: &mut Option<Plan>) -> bool {
        let Some(tenant) = req.tenant else {
            return true;
        };
        if req.tokens == 0 {
            return true;
        }
        let now = self.arrival_ns;
        if self.tenant_slots > 0 {
            let flight = self.tenant_flight.entry(tenant).or_default();
            while flight.peek().is_some_and(|&Reverse(end)| end <= now) {
                flight.pop();
            }
            if flight.len() >= self.tenant_slots {
                return false;
            }
        }
        if self.tenant_prefill_ns_per_s > 0.0 && self.priced_prefill {
            let work = self.home_plan(home, req, planned).rebuild_ns as f64;
            let rate = self.tenant_prefill_ns_per_s;
            let (tokens, last) = self.buckets.entry(tenant).or_insert((rate, now));
            let cap = rate.max(work);
            *tokens = (*tokens + rate * now.saturating_sub(*last) as f64 / 1e9).min(cap);
            *last = now;
            if *tokens < work {
                return false;
            }
            *tokens -= work;
        }
        true
    }

    pub fn set_pairing(&mut self, rule: Pairing, over_ns: u64, prefill_fetch: bool) {
        self.pairing = rule;
        self.pair_over_ns = over_ns;
        self.prefill_fetch = prefill_fetch;
    }

    fn quotes(&self, home: usize, req: &Request, plan_d: &Plan) -> Vec<Quote> {
        let (Some(fleet), Some(model)) = (&self.fleet, model_of(&req.requires)) else {
            return Vec::new();
        };
        let now = self.arrival_ns;
        let view = req.view(req.tokens);
        let unit = self.unit_in(home);
        let need = plan_d.need[BlobKind::KvBlock.idx()];
        fleet
            .prefillers(model, now)
            .into_iter()
            .map(|p| {
                let work_ns = self
                    .plan_with(p, &view, View::Belief, self.prefill_fetch)
                    .rebuild_ns;
                Quote {
                    node: p,
                    wait_ns: self.prefill_free_at[p].saturating_sub(now),
                    work_ns,
                    transfer_ns: self.topo.fetch_ns(unit, p, need),
                    toll_ns: self.engines[p].prefill_share(now) * work_ns as f64 / 2.0,
                }
            })
            .collect()
    }

    fn joint_pick(quotes: &[Quote], unpaired_ns: f64) -> Option<usize> {
        quotes
            .iter()
            .min_by(|a, b| {
                a.total_ns()
                    .total_cmp(&b.total_ns())
                    .then(a.node.cmp(&b.node))
            })
            .filter(|q| q.total_ns() < unpaired_ns)
            .map(|q| q.node)
    }

    fn independent_pick(&self, quotes: &[Quote], w_d: u64) -> Option<usize> {
        if w_d <= self.pair_over_ns {
            return None;
        }
        quotes
            .iter()
            .min_by_key(|q| (q.wait_ns + q.work_ns, q.node))
            .map(|q| q.node)
    }

    fn list_pick(&mut self, quotes: &[Quote], w_d: u64) -> Option<usize> {
        if w_d <= self.pair_over_ns || quotes.is_empty() {
            return None;
        }
        let node = quotes[self.list_cursor % quotes.len()].node;
        self.list_cursor += 1;
        Some(node)
    }

    fn decide_pair(
        &mut self,
        home: usize,
        req: &Request,
        planned: &mut Option<Plan>,
    ) -> Option<(usize, u64)> {
        let in_pool = self
            .fleet
            .as_ref()
            .is_some_and(|f| f.role_of(home) == Role::Decode);
        let kv = req
            .chain
            .first()
            .is_some_and(|(_, m)| m.kind == BlobKind::KvBlock);
        if self.pairing == Pairing::Off || !in_pool || !kv || req.tokens == 0 {
            return None;
        }
        let plan_d = self.home_plan(home, req, planned);
        let w_d = plan_d.rebuild_ns;
        if w_d == 0 {
            return None;
        }
        let quotes = self.quotes(home, req, plan_d);
        if quotes.is_empty() {
            return None;
        }
        let in_flight = self.engines[home].load(self.arrival_ns) + self.staged_seqs[home];
        let unpaired_ns = w_d as f64 * (1 + in_flight) as f64;
        let joint = Self::joint_pick(&quotes, unpaired_ns);
        let independent = self.independent_pick(&quotes, w_d);
        self.pair_stats.decisions += 1;
        if joint != independent {
            self.pair_stats.coupled += 1;
        }
        let pick = match self.pairing {
            Pairing::Off => None,
            Pairing::Joint => joint,
            Pairing::Independent => independent,
            Pairing::List => self.list_pick(&quotes, w_d),
        };
        pick.map(|p| (p, w_d))
    }

    fn prefill_on(&mut self, p: usize, req: &Request, avoided_ns: u64) -> Option<(u64, u64, u64)> {
        let now = self.arrival_ns;
        let wait = self.prefill_free_at[p].saturating_sub(now);
        self.domains[p].set_owner(req.tenant);
        let plan = self.plan_with(p, &req.view(req.tokens), View::Belief, self.prefill_fetch);
        let fetch = self.apply_chain(p, req, &plan);
        let cost = self.domains[p].access(&req.chain);
        if cost.pending {
            self.domains[p].set_owner(None);
            self.pair_stats.failed += 1;
            return None;
        }
        self.report_prefill(p, req, cost.recompute_ns);
        self.domains[p].seal(None);
        self.domains[p].set_owner(None);
        self.prefill_free_at[p] = now + wait + cost.recompute_ns;
        self.pair_stats.paired += 1;
        self.pair_stats.avoided_ns += avoided_ns;
        self.pair_stats.wait_ns += wait;
        self.pair_stats.work_ns += cost.recompute_ns;
        let transfer = fetch.transfer_ns + cost.transfer_ns;
        self.pair_stats.transfer_ns += transfer;
        if let (Some(m), true) = (model_of(&req.requires), self.planner.is_some()) {
            let m = usize::from(m) % MODEL_COUNT;
            self.fleet_view.paired_work_ns[m] += cost.recompute_ns;
            self.fleet_view.paired_avoided_ns[m] += avoided_ns;
            self.fleet_view.prefill_work_ns[m] += avoided_ns;
            self.fleet_view.prefills[m] += 1;
        }
        Some((wait, cost.recompute_ns, transfer))
    }

    #[must_use]
    pub fn planner_ticks(&self) -> u64 {
        self.planner.map_or(0, |p| p.ticks)
    }

    fn record_prefill(&mut self, req: &Request, work_ns: u64) {
        let decodes = req.tokens > 0
            && req
                .chain
                .first()
                .is_some_and(|(_, m)| m.kind == BlobKind::KvBlock);
        if let (Some(m), true, true) = (model_of(&req.requires), self.planner.is_some(), decodes)
            && work_ns > 0
        {
            let m = usize::from(m) % MODEL_COUNT;
            self.fleet_view.prefill_work_ns[m] += work_ns;
            self.fleet_view.prefills[m] += 1;
        }
    }

    fn record_demand(&mut self, model: Option<Model>, tokens: u64) {
        if let (Some(m), true) = (model, self.planner.is_some()) {
            self.fleet_view.demand_tokens[usize::from(m) % MODEL_COUNT] += tokens;
        }
    }

    fn keep_cost_ns(&self, d: usize) -> f64 {
        let k = BlobKind::KvBlock.idx();
        let h = &self.domains[d];
        let reads = h.hits[k] + h.misses[k] + h.offload_hits[k] + h.nvme_hits[k];
        let hit_share = if reads == 0 {
            0.0
        } else {
            h.hits[k] as f64 / reads as f64
        };
        let blocks = h.resident_bytes(BlobKind::KvBlock) / crate::work::KV_BLOCK_BYTES;
        blocks as f64 * crate::work::KV_BLOCK_NS as f64 * hit_share
    }

    fn move_cost_ns(
        &self,
        current: &[Option<Model>],
        next: &[Option<Model>],
        demand: &[f64; MODEL_COUNT],
        costs: &Costs,
        keep: &[f64],
    ) -> f64 {
        let Some(fleet) = &self.fleet else {
            return f64::MAX;
        };
        let counts = fleet.counts();
        let mut interim = counts;
        let (mut load_ns, mut rebuild) = (0u64, 0.0);
        for (d, (from, to)) in current.iter().zip(next).enumerate() {
            let (Some(to), true) = (*to, from != to) else {
                continue;
            };
            if let Some(from) = from {
                interim[usize::from(*from) % MODEL_COUNT] -= 1;
            }
            let drain = self.engines[d].drained_by(self.arrival_ns) - self.arrival_ns;
            load_ns = load_ns.max(drain + self.load_ns(d, to));
            rebuild += keep[d];
        }
        let extra = costs.total_rate(demand, &interim) - costs.total_rate(demand, &counts);
        extra.max(0.0) * load_ns as f64 / 1e9 + rebuild
    }

    fn plan_fleet(&mut self) {
        let Some(mut state) = self.planner.take() else {
            return;
        };
        let now = self.arrival_ns;
        if now >= state.next_at
            && let Some(fleet) = &self.fleet
        {
            state.next_at = (now / state.interval_ns + 1) * state.interval_ns;
            state.ticks += 1;
            let seconds = state.interval_ns as f64 / 1e9;
            let view = std::mem::take(&mut self.fleet_view);
            let demand = view.demand_tokens.map(|t| t as f64 / seconds);
            let current = fleet.placement();
            let counts = fleet.counts();
            for ((seen, &tokens), &held) in
                state.seen.iter_mut().zip(&view.demand_tokens).zip(&counts)
            {
                *seen |= tokens > 0 || held > 0;
            }
            let best = state
                .costs
                .best_counts_among(&demand, current.len(), state.seen);
            let keep: Vec<f64> = (0..current.len()).map(|d| self.keep_cost_ns(d)).collect();
            let next = retarget(&current, &best, &keep);
            let go = if best == counts {
                state.accrued_ns = 0.0;
                false
            } else {
                match state.kind {
                    PlannerKind::Once => state.ticks == 1,
                    PlannerKind::Eager => true,
                    PlannerKind::Follow => {
                        let loss = state.costs.total_rate(&demand, &counts)
                            - state.costs.total_rate(&demand, &best);
                        state.accrued_ns += loss.max(0.0) * seconds;
                        state.accrued_ns
                            >= self.move_cost_ns(&current, &next, &demand, &state.costs, &keep)
                    }
                }
            };
            if go && moves_between(&current, &next) > 0 {
                self.apply_placement(&next);
                state.accrued_ns = 0.0;
            }
            self.plan_roles(&state, &view, seconds);
        }
        self.planner = Some(state);
    }

    fn plan_roles(&mut self, state: &PlannerState, view: &FleetView, seconds: f64) {
        if self.pairing == Pairing::Off || (state.kind == PlannerKind::Once && state.ticks != 1) {
            return;
        }
        let now = self.arrival_ns;
        let Some(fleet) = &self.fleet else {
            return;
        };
        let (counts, current) = (fleet.counts(), fleet.prefiller_counts());
        let mut wanted = [0usize; MODEL_COUNT];
        for m in 0..MODEL_COUNT {
            if counts[m] > 0 {
                let demand = view.demand_tokens[m] as f64 / seconds;
                wanted[m] =
                    state
                        .costs
                        .best_prefillers(m, demand, counts[m], view.prefills_of(m, seconds));
            }
        }
        let mut flips: Vec<(usize, Role)> = Vec::new();
        for m in 0..MODEL_COUNT {
            let model = m as Model;
            if wanted[m] > current[m] {
                let mut idle: Vec<usize> = (0..fleet.nodes())
                    .filter(|&d| {
                        fleet.model_on(d) == Some(model) && fleet.role_of(d) == Role::Decode
                    })
                    .collect();
                idle.sort_by_key(|&d| (self.engines[d].drained_by(now), d));
                flips.extend(
                    idle.into_iter()
                        .take(wanted[m] - current[m])
                        .map(|d| (d, Role::Prefill)),
                );
            } else if wanted[m] < current[m] {
                let mut held: Vec<usize> = (0..fleet.nodes())
                    .filter(|&d| {
                        fleet.model_on(d) == Some(model) && fleet.role_of(d) == Role::Prefill
                    })
                    .collect();
                held.sort_by(|a, b| b.cmp(a));
                flips.extend(
                    held.into_iter()
                        .take(current[m] - wanted[m])
                        .map(|d| (d, Role::Decode)),
                );
            }
        }
        if let Some(fleet) = self.fleet.as_mut() {
            for (d, role) in flips {
                fleet.move_role(d, role);
            }
        }
    }

    pub fn set_fleet(&mut self, mut fleet: Fleet, partition_override: Option<u64>) {
        for d in 0..self.domains.len() {
            let Some(model) = fleet.model_on(d) else {
                continue;
            };
            let bytes = fleet.catalogue().get(model).bytes;
            self.domains[d].bind_weights(bytes);
            self.engines[d].set_step_base(fleet.step_base_ns(model));
            if let Some((capacity, _)) = self.domains[d].kv_partition() {
                fleet.bind_partition(d, capacity);
            }
        }
        self.partition_override = partition_override;
        self.fleet = Some(fleet);
    }

    #[must_use]
    pub fn fleet(&self) -> Option<&Fleet> {
        self.fleet.as_ref()
    }

    pub fn schedule_placement(&mut self, at_ns: u64, placement: Vec<Option<Model>>) {
        let at = self.placements.partition_point(|(due, _)| *due <= at_ns);
        self.placements.insert(at, (at_ns, placement));
    }

    fn load_ns(&self, d: usize, model: Model) -> u64 {
        let Some(fleet) = &self.fleet else {
            return 0;
        };
        let spec = fleet.catalogue().get(model);
        let transfer = if fleet.cached(d, model) {
            TierSpec::pcie().fetch_ns(spec.bytes)
        } else {
            let unit = self.unit_in(d);
            fleet
                .holders(model, self.arrival_ns)
                .into_iter()
                .filter(|&p| p != d)
                .map(|p| self.topo.fetch_ns(unit, p, spec.bytes))
                .min()
                .unwrap_or(WEIGHT_NS * spec.bytes.div_ceil(WEIGHT_BYTES))
        };
        spec.start_ns + transfer
    }

    pub fn apply_placement(&mut self, placement: &[Option<Model>]) -> usize {
        let now = self.arrival_ns;
        let mut moved = 0;
        for (d, target) in placement.iter().enumerate() {
            let Some(model) = *target else {
                continue;
            };
            let Some(fleet) = &self.fleet else {
                return moved;
            };
            if fleet.model_on(d) == Some(model) {
                continue;
            }
            let load = self.load_ns(d, model);
            let bytes = fleet.catalogue().get(model).bytes;
            let hbm = self.domains[d].hbm_bytes();
            let partition = self
                .partition_override
                .unwrap_or_else(|| hbm.saturating_sub(bytes))
                .min(hbm.saturating_sub(bytes));
            let base = fleet.step_base_ns(model);
            let begin = self.engines[d].drained_by(now);
            let lost = self.domains[d].reload(bytes, partition);
            self.engines[d].flush();
            self.engines[d].set_step_base(base);
            self.reserved[d] = Reservations::default();
            if let Some(fleet) = self.fleet.as_mut() {
                fleet.assign(now, begin, d, model, load);
                if self.pairing != Pairing::Off {
                    fleet.set_role(d, Role::Decode);
                }
                fleet.bind_partition(d, partition);
                fleet.stats.kv_lost_bytes += lost;
            }
            self.observe_emit(d);
            moved += 1;
        }
        moved
    }

    fn apply_due_placements(&mut self) {
        while self
            .placements
            .first()
            .is_some_and(|(due, _)| *due <= self.arrival_ns)
        {
            let (_, placement) = self.placements.remove(0);
            self.apply_placement(&placement);
        }
    }

    fn eligible(&self, mut pool: Vec<usize>, req: &Request) -> Vec<usize> {
        if self.tenant_set > 0
            && let Some(tenant) = req.tenant
        {
            let burst = tenant == crate::work::NEIGHBOUR_TENANT;
            pool.retain(|&d| (d < self.tenant_set) == burst);
        }
        let (Some(fleet), Some(model)) = (&self.fleet, model_of(&req.requires)) else {
            return pool;
        };
        let blocks = req.chain.len() as u64 + req.max_tokens.div_ceil(self.tokens_per_block.max(1));
        pool.retain(|&d| fleet.serves(d, model, blocks));
        pool
    }

    fn loading_wait_ns(&self, d: usize) -> u64 {
        self.fleet
            .as_ref()
            .map_or(0, |f| f.wait_ns(d, self.arrival_ns))
    }

    pub fn set_model_batches(&mut self, batching: Batching, priced: bool) {
        for e in &mut self.engines {
            e.set_batching(batching);
        }
        self.priced_models = priced;
    }

    pub fn set_prefill_time(&mut self, load: Option<PrefillLoad>, priced: bool) {
        for e in &mut self.engines {
            e.set_prefill_load(load);
        }
        self.priced_prefill = load.is_some() && priced;
    }

    #[must_use]
    pub fn prices_prefill(&self) -> bool {
        self.priced_prefill
    }

    #[must_use]
    pub fn models_in_flight(&self) -> [u64; MODEL_COUNT + 1] {
        let mut total = [0; MODEL_COUNT + 1];
        for e in &self.engines {
            for (sum, n) in total.iter_mut().zip(e.models_in_flight) {
                *sum += n;
            }
        }
        total
    }

    #[must_use]
    pub fn prefill_work_ns(&self) -> u64 {
        self.engines.iter().map(|e| e.prefill_work_ns).sum()
    }

    #[must_use]
    pub fn stretch_ns(&self) -> u64 {
        self.engines.iter().map(|e| e.stretch_ns).sum()
    }

    #[must_use]
    pub fn decode_ns(&self) -> u64 {
        self.engines.iter().map(|e| e.decode_ns).sum()
    }

    #[must_use]
    pub fn decode_share_ns(&self) -> u64 {
        self.engines.iter().map(|e| e.decode_share_ns).sum()
    }

    #[must_use]
    pub fn kv_partition_bytes(&self) -> u64 {
        self.domains
            .first()
            .and_then(Hierarchy::kv_partition)
            .map_or(0, |(capacity, _)| capacity)
    }

    #[must_use]
    pub fn nodes(&self) -> usize {
        self.domains.len()
    }

    fn report_prefill(&mut self, d: usize, req: &Request, work_ns: u64) {
        if !req
            .chain
            .first()
            .is_some_and(|(_, m)| m.kind == BlobKind::KvBlock)
        {
            return;
        }
        self.engines[d].prefill(self.arrival_ns, work_ns);
        if let Some(o) = &self.origins
            && let Some(origin) = req.chain.last().and_then(|(id, _)| o.of(id))
        {
            self.instruments.work_by_origin[origin.idx()].push(work_ns);
        }
    }

    pub fn set_directives(&mut self, directives: Option<Directives>) {
        self.directives = directives;
    }

    pub fn set_foresight(&mut self, foresight: Option<Foresight>) {
        self.foresight = foresight;
    }

    pub fn set_clairvoyant(&mut self, foresight: &Foresight) {
        for h in &mut self.domains {
            h.set_clairvoyant_index(foresight.schedule());
        }
    }

    pub fn set_position(&mut self, position: u64) {
        self.position = position;
        for h in &mut self.domains {
            h.set_clairvoyant_op(position);
        }
    }

    pub fn set_prefill_ahead(&mut self, on: bool) {
        self.prefill_ahead = on;
    }

    pub fn set_prefill_target(&mut self, target: Target) {
        self.prefill_target = target;
    }

    pub fn set_origins(&mut self, origins: Origins) {
        self.record_kv_events(true);
        self.instruments.reuse = Some(Reuse::new(origins.clone(), self.domains.len()));
        self.origins = Some(origins);
    }

    pub fn track_tenants(&mut self) {
        let origins = self.origins.clone().expect("tenants are read by origin");
        self.instruments.tenants = Some(crate::instruments::Tenants::new(origins));
    }

    #[must_use]
    pub fn directive_stats(&self) -> crate::cache::DirectiveStats {
        self.domains.iter().map(Hierarchy::directive_stats).fold(
            crate::cache::DirectiveStats::default(),
            |a, b| crate::cache::DirectiveStats {
                applied: a.applied + b.applied,
                expired: a.expired + b.expired,
                pressure_evictions: a.pressure_evictions + b.pressure_evictions,
                marked_blocks: a.marked_blocks + b.marked_blocks,
                marked_bytes: a.marked_bytes + b.marked_bytes,
            },
        )
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
        self.observe_blocks(d, &blocks);
    }

    fn observe_blocks(&mut self, d: usize, blocks: &[(BlobId, BlobMeta)]) {
        if self.observer.is_none() || !self.domains[d].engine_cache() {
            return;
        }
        let Some((_, first)) = blocks.first() else {
            return;
        };
        let price = self.domains[d].kv_unit_price(first);
        let now = self.arrival_ns;
        if self.instrument
            && let Some(o) = self.observer.as_ref()
        {
            for (id, _) in blocks {
                if o.belief(d).believes_gpu(id) && !self.domains[d].is_hot(id, BlobKind::KvBlock) {
                    self.redispatched[d].insert(*id);
                }
            }
        }
        if let Some(o) = self.observer.as_mut() {
            o.dispatched(d, blocks, now, price);
        }
    }

    fn observe_reuse(&mut self, d: usize, req: &Request) {
        if self.instruments.reuse.is_none() || !self.domains[d].engine_cache() {
            return;
        }
        let now = self.arrival_ns;
        let seen: Vec<_> = req
            .chain
            .iter()
            .filter(|(_, m)| m.kind == BlobKind::KvBlock)
            .map(|(id, meta)| {
                let tier = self.domains[d].kv_tier_of(id);
                (*id, tier, self.domains[d].kv_acquire_ns(tier, meta))
            })
            .collect();
        if seen.is_empty() {
            return;
        }
        if let Some(t) = self.instruments.tenants.as_mut() {
            for (id, tier, _) in &seen {
                t.read(id, *tier == Some(Medium::Gpu));
            }
        }
        if let Some(r) = self.instruments.reuse.as_mut() {
            r.dispatches += 1;
            let mut evicted = false;
            for (id, tier, cost) in seen {
                evicted |= r.classify(d, now, &id, tier, cost);
            }
            r.dispatches_with_evicted_miss += u64::from(evicted);
        }
    }

    fn observe_touched(&mut self, d: usize, req: &Request) {
        if self.instruments.reuse.is_none() || !self.domains[d].engine_cache() {
            return;
        }
        let now = self.arrival_ns;
        let resident: Vec<BlobId> = req
            .chain
            .iter()
            .chain(&req.produces)
            .filter(|(id, m)| m.kind == BlobKind::KvBlock && self.domains[d].is_hot(id, m.kind))
            .map(|(id, _)| *id)
            .collect();
        if let Some(r) = self.instruments.reuse.as_mut() {
            for id in resident {
                r.touched(d, now, id);
            }
        }
    }

    fn declared_marks(&self, req: &Request, scope: (bool, bool)) -> Vec<(BlobId, Mark)> {
        let kv = |(_, m): &&(BlobId, BlobMeta)| m.kind == BlobKind::KvBlock;
        let now = self.arrival_ns;
        let mut marks = Vec::new();
        if let Some(r) = req.retention.retain.filter(|_| scope.0) {
            let until_ns = now + (u64::from(r.lead_ops) + 1) * self.interval_ns;
            marks.extend(req.chain.iter().take(r.upto).filter(kv).map(|(id, _)| {
                (
                    *id,
                    Mark {
                        rank: Rank::Retain,
                        until_ns,
                    },
                )
            }));
        }
        if let Some(from) = req.retention.evict_first_from.filter(|_| scope.1) {
            let until_ns = now + EVICT_FIRST_LEASE_NS;
            marks.extend(
                req.chain
                    .iter()
                    .skip(from)
                    .chain(&req.produces)
                    .filter(kv)
                    .map(|(id, _)| {
                        (
                            *id,
                            Mark {
                                rank: Rank::EvictFirst,
                                until_ns,
                            },
                        )
                    }),
            );
        }
        marks
    }

    fn oracle_marks(&self, req: &Request, horizon_ns: u64) -> Vec<(BlobId, Mark)> {
        let Some(foresight) = &self.foresight else {
            return Vec::new();
        };
        let (now, step, at) = (self.arrival_ns, self.interval_ns, self.position);
        req.chain
            .iter()
            .chain(&req.produces)
            .filter(|(_, m)| m.kind == BlobKind::KvBlock)
            .filter_map(|(id, _)| {
                let wait = (foresight.next_use(id, at)? - at) * step;
                (wait <= horizon_ns).then_some((
                    *id,
                    Mark {
                        rank: Rank::Retain,
                        until_ns: now + wait + step,
                    },
                ))
            })
            .collect()
    }

    fn emit_directives(&mut self, d: usize, req: &Request, cost: &Cost) {
        let Some(cfg) = self.directives else {
            return;
        };
        if cost.pending || self.interval_ns == 0 || !self.domains[d].engine_cache() {
            return;
        }
        let marks = match cfg.emit {
            Emit::Declared {
                retain,
                evict_first,
            } => self.declared_marks(req, (retain, evict_first)),
            Emit::Oracle { horizon_ns } => self.oracle_marks(req, horizon_ns),
        };
        for (id, mark) in marks {
            self.instruments.directives.emitted += 1;
            if !cfg.ignores && self.domains[d].mark_kv(id, mark) {
                self.instruments.directives.honoured += 1;
            }
            if cfg.marks == Marks::Trusted
                && mark.rank == Rank::Retain
                && let Some(o) = self.observer.as_mut()
            {
                o.directed(d, &[id], mark.until_ns);
            }
        }
    }

    fn observe_landing(&mut self, req: &Request, home: usize) {
        if let Some(target) = req.completes.and_then(|t| self.landing.remove(&t)) {
            self.instruments.prefill.landings += 1;
            self.instruments.prefill.landed += u64::from(target == home);
        }
    }

    fn after_dispatch(&mut self, req: &Request, home: usize, cost: &Cost) {
        if cost.pending {
            return;
        }
        if self.tenant_slots > 0
            && let Some(t) = req.tenant
            && req.tokens > 0
        {
            let end = self.arrival_ns + cost.queue_ns + cost.exec_ns;
            self.tenant_flight.entry(t).or_default().push(Reverse(end));
        }
        if self.prefill_ahead
            && let Some(hint) = &req.hint
        {
            self.prefill_for(hint, home);
        }
        let flow_prompt = self.origins.as_ref().is_some_and(|o| {
            req.chain.first().and_then(|(id, _)| o.of(id)) == Some(Origin::FlowPrompt)
        });
        if flow_prompt && req.completes.is_some() {
            self.instruments.flow.record(cost.total_ns());
        }
    }

    fn argmin_target(
        &self,
        blocks: &[(BlobId, BlobMeta)],
        flow: &[(usize, u64)],
        home: usize,
    ) -> usize {
        let tokens = match self.observed[Slo::Interactive.idx()] {
            (sum, n) if n > 0 => sum / n,
            _ => 0,
        };
        let view = RequestView {
            chain: blocks,
            requires: &[],
            model: None,
            tokens,
            class: BlobKind::KvBlock.idx(),
            slo: Slo::Interactive,
            tenant: None,
        };
        self.decode_pool()
            .into_iter()
            .map(|d| (d, self.placement_terms(d, &view, flow, View::Belief).full()))
            .min_by(|a, b| a.1.total_cmp(&b.1))
            .map_or(home, |(d, _)| d)
    }

    fn deepest_target(&self, blocks: &[(BlobId, BlobMeta)], home: usize) -> usize {
        let pool = self.decode_pool();
        let mut best = (
            if pool.contains(&home) {
                home
            } else {
                pool.first().copied().unwrap_or(home)
            },
            0,
        );
        for &d in &pool {
            let depth = blocks
                .iter()
                .take_while(|(id, m)| self.believes_resident(d, id, m.kind))
                .count();
            if depth > best.1 {
                best = (d, depth);
            }
        }
        best.0
    }

    fn prefill_for(&mut self, hint: &FlowHint, home: usize) {
        let blocks = &hint.downstream;
        if blocks.is_empty() || blocks.iter().any(|(_, m)| m.kind != BlobKind::KvBlock) {
            return;
        }
        let flow = if self.flow_aware {
            self.upstream.get(&hint.task).cloned().unwrap_or_default()
        } else {
            Vec::new()
        };
        let target = match self.prefill_target {
            Target::Argmin => self.argmin_target(blocks, &flow, home),
            Target::Deepest => self.deepest_target(blocks, home),
        };
        if !self.domains[target].engine_cache() {
            return;
        }
        let mut placed = Vec::new();
        let mut work = 0;
        for &(id, meta) in blocks {
            match self.domains[target].prefill_block(id, meta) {
                Some(0) => {}
                Some(ns) => {
                    work += ns;
                    placed.push((id, meta));
                }
                None => break,
            }
        }
        self.domains[target].seal(None);
        self.engines[target].prefill(self.arrival_ns, work);
        let stats = &mut self.instruments.prefill;
        stats.calls += 1;
        stats.blocks += placed.len() as u64;
        stats.work_ns += work;
        self.landing.insert(hint.task, target);
        self.observe_blocks(target, &placed);
        self.observe_emit(target);
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
        let steps: Vec<u64> = loads
            .iter()
            .zip(&self.engines)
            .map(|(&l, e)| e.step(l + 1))
            .collect();
        if let Some(o) = self.observer.as_mut() {
            o.release(now);
            o.pump(now, &steps, &loads);
        }
    }

    fn observe_emit(&mut self, d: usize) {
        if self.observer.is_none() && self.instruments.reuse.is_none() {
            return;
        }
        let events: Vec<KvEvent> = self.domains[d].take_kv_events();
        let now = self.arrival_ns;
        if let Some(r) = self.instruments.reuse.as_mut() {
            r.events(d, now, &events);
        }
        if let Some(t) = self.instruments.tenants.as_mut() {
            t.events(&events);
        }
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
        self.redispatched[victim].clear();
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
        self.plan_with(d, req, view, self.state_transfer)
    }

    fn plan_with(&self, d: usize, req: &RequestView<'_>, view: View, peers: bool) -> Plan {
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
            rebuild_ns: 0,
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
        if peers && depth < req.chain.len() {
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
        if self.priced_prefill {
            plan.rebuild_ns = req.chain[plan.chain_cut..]
                .iter()
                .filter(|(id, m)| {
                    m.kind == BlobKind::KvBlock && self.local_ns(d, id, m, view) == m.recompute_ns
                })
                .map(|(_, m)| m.recompute_ns)
                .sum();
        }
        if self.fleet.is_none() {
            self.plan_dependencies(d, req, &mut plan, view, &resident, &held);
        }
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
        let tele = tele.with_model(if self.priced_models || view == View::Truth {
            req.model
        } else {
            None
        });
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
            (tele.projected_ns(self.arrival_ns, req.tokens, reserved) + self.loading_wait_ns(d))
                as f64
        } else {
            0.0
        };
        let congestion = if decoding {
            tele.congestion_ns(self.arrival_ns, req.tokens, reserved) as f64
        } else {
            0.0
        };
        let prefill = if self.priced_prefill {
            tele.prefill_toll_ns(self.arrival_ns, plan.rebuild_ns, reserved)
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
            prefill,
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
            self.loading_wait_ns(d)
                + self
                    .telemetry_in(d, View::Truth)
                    .with_model(model_of(&req.requires))
                    .projected_ns(self.arrival_ns, req.tokens, self.staged_seqs[d])
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
        let mut phantom_cause = [0.0; Cause::N];
        let mut miss_cause = [0.0; Cause::N];
        for &d in &self.active {
            if !self.domains[d].engine_cache() {
                continue;
            }
            let belief = o.belief(d);
            let (mut believed, mut wrong) = (0u64, 0u64);
            let mut by_cause = [0u64; Cause::N];
            for id in belief.gpu_ids() {
                believed += 1;
                if !self.domains[d].is_hot(id, BlobKind::KvBlock) {
                    wrong += 1;
                    by_cause[o.phantom_cause(d, id).idx()] += 1;
                }
            }
            for (c, n) in by_cause.iter().enumerate() {
                phantom_cause[c] += *n as f64 / believed.max(1) as f64;
                self.instruments.phantom_blocks[c] += n;
            }
            self.redispatched[d].retain(|id| !belief.believes_gpu(id));
            let (mut actual, mut missed) = (0u64, 0u64);
            let mut missed_by = [0u64; Cause::N];
            for id in self.domains[d].kv_gpu_ids() {
                actual += 1;
                if !belief.believes_gpu(&id) {
                    missed += 1;
                    let cause = if self.redispatched[d].contains(&id) {
                        Cause::Redispatched
                    } else {
                        o.miss_cause(d, &id)
                    };
                    missed_by[cause.idx()] += 1;
                }
            }
            for (c, n) in missed_by.iter().enumerate() {
                miss_cause[c] += *n as f64 / actual.max(1) as f64;
                self.instruments.miss_blocks[c] += n;
            }
            phantom += wrong as f64 / believed.max(1) as f64;
            miss += missed as f64 / actual.max(1) as f64;
            nodes += 1;
        }
        if nodes > 0 {
            let nodes = f64::from(nodes);
            self.instruments.divergence_samples += 1;
            self.instruments.phantom_share += phantom / nodes;
            self.instruments.miss_share += miss / nodes;
            for c in 0..Cause::N {
                self.instruments.phantom_cause_share[c] += phantom_cause[c] / nodes;
                self.instruments.miss_cause_share[c] += miss_cause[c] / nodes;
            }
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

    fn arrive(&mut self, concurrent: bool) {
        if !concurrent {
            self.arrival_ns += self.interval_ns;
        }
        let now = self.arrival_ns;
        for (h, r) in self.domains.iter_mut().zip(&mut self.reserved) {
            h.release(now);
            r.release(now);
        }
        if let Some(fleet) = self.fleet.as_mut() {
            fleet.settle(now);
        }
        self.apply_due_placements();
        self.plan_fleet();
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
    }

    fn unplaced() -> Cost {
        Cost {
            pending: true,
            ..Cost::default()
        }
    }

    pub fn serve_request(&mut self, req: &Request) -> Cost {
        self.arrive(req.concurrent);
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
        self.record_demand(model_of(&req.requires), req.tokens);
        let candidates = if decode_needed {
            self.eligible(self.decode_pool(), req)
        } else {
            self.active.clone()
        };
        if candidates.is_empty() {
            if let Some(fleet) = self.fleet.as_mut() {
                fleet.stats.unplaced += 1;
            }
            return Self::unplaced();
        }
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

        let mut planned = None;
        if !self.meters_admit(home, req, &mut planned) {
            if let Some(t) = req.tenant {
                *self.tenant_refused.entry(t).or_insert(0) += 1;
            }
            return Self::unplaced();
        }

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

        self.observe_landing(req, home);
        let pair = self.decide_pair(home, req, &mut planned);
        let mut cost = self.run_paired(home, req, pair);
        cost.decide_ns = decide_ns;
        cost.transfer_ns += handoff + arrival;
        self.after_dispatch(req, home, &cost);
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
        self.run_paired(home, req, None)
    }

    fn run_paired(&mut self, home: usize, req: &Request, pair: Option<(usize, u64)>) -> Cost {
        let class = req.kind_idx();
        if !self.router_admits(home, req, false) {
            self.refused_by_router[class] += 1;
            return Cost {
                pending: true,
                ..Cost::default()
            };
        }
        if let Some(t) = self.instruments.tenants.as_mut() {
            t.set_requester(req.tenant);
        }
        self.domains[home].set_owner(req.tenant);
        let prefilled = pair.and_then(|(p, avoided)| self.prefill_on(p, req, avoided));
        let shared_before = self.shared_reads;
        let ran_with = self.truly_resident(home, req);

        let plan = self.plan(home, &req.view(req.tokens), View::Belief);
        self.observe_reuse(home, req);
        self.observe_dispatch(home, req);
        let fetch = self.apply_chain(home, req, &plan);
        let mut cost = self.domains[home].access(&req.chain);
        let chain_recompute = cost.recompute_ns;
        cost.transfer_ns += fetch.transfer_ns;
        if let Some((wait, work, transfer)) = prefilled {
            cost.queue_ns += wait;
            cost.recompute_ns += work;
            cost.transfer_ns += transfer;
        }

        if !cost.pending {
            self.report_prefill(home, req, chain_recompute);
            if prefilled.is_none() {
                self.record_prefill(req, chain_recompute);
            }
            if fetch.transfer_ns > 0 {
                self.remote += 1;
            } else if ran_with == 0 {
                self.cold += 1;
            } else {
                self.local += 1;
            }
        }

        if !cost.pending && !req.requires.is_empty() && self.fleet.is_none() {
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
            cost.queue_ns += queue;
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
        self.emit_directives(home, req, &cost);
        self.observe_emit(home);
        self.domains[home].set_owner(None);
        if !cost.pending {
            self.observe_touched(home, req);
        }
        cost
    }

    fn execute(&mut self, d: usize, req: &Request) -> (u64, u64) {
        if req.tokens == 0 || self.interval_ns == 0 {
            return (req.exec_ns, 0);
        }
        let model = model_of(&req.requires);
        let wait = self.loading_wait_ns(d);
        let step = self.engines[d].decode_for(self.arrival_ns + wait, req.tokens, model);
        if let (Some(fleet), Some(m)) = (self.fleet.as_mut(), model) {
            fleet.record_served(d, m);
        }
        (step.exec_ns, step.queue_ns + wait)
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
            retention: agent.retention,
            concurrent: false,
            tenant: agent.tenant,
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
            retention: crate::work::Retention::default(),
            concurrent: false,
            tenant: None,
        }
    }

    fn serve_gang(&mut self, req: &Request, gang: &Gang) -> Cost {
        let n = gang.agents.len().max(1);
        for a in &gang.agents {
            self.record_demand(model_of(&a.requires), a.tokens);
        }
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
        let eligible = self.eligible(self.decode_pool(), probe);
        if eligible.is_empty()
            && let Some(fleet) = self.fleet.as_mut()
        {
            fleet.stats.unplaced += 1;
        }
        let feasible: Vec<(usize, Need)> = eligible
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
    prefill: f64,

    need: Need,
}

pub const TERM_COUNT: usize = 6;
pub const TERM_LABELS: [&str; TERM_COUNT] = [
    "acquire",
    "displaced",
    "handoff",
    "engine",
    "congestion",
    "prefill",
];

impl Terms {
    const EACH: [fn(&Terms) -> f64; TERM_COUNT] = [
        |t| t.acquire,
        |t| t.displaced,
        |t| t.handoff,
        |t| t.engine,
        |t| t.congestion,
        |t| t.prefill,
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
        self.loaded() + self.congestion + self.prefill
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
    rebuild_ns: u64,
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
        placed_machine(hbm, partition, control, Placement::Scored)
    }

    fn placed_machine(hbm: u64, partition: u64, control: Control, placement: Placement) -> Machine {
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
        let mut mach = Machine::new(topo, |_| mem, Policy::Gdsf, placement);
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
            retention: crate::work::Retention::default(),
            concurrent: false,
            tenant: None,
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
            retention: crate::work::Retention::default(),
            concurrent: false,
            tenant: None,
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

    fn declared(ignores: bool, marks: Marks) -> Directives {
        Directives {
            emit: Emit::Declared {
                retain: true,
                evict_first: true,
            },
            ignores,
            marks,
        }
    }

    #[test]
    fn the_event_stream_reproduces_every_live_mark_after_every_request() {
        use crate::stream::Index;
        let mut mach = stream_machine(64 << 20, 32 << 20, 48 << 20);
        mach.record_kv_events(true);
        mach.set_state_transfer(true);
        mach.set_flow_aware(true);
        mach.set_hold_decodes(true);
        mach.set_arrival_rate(250.0);
        mach.set_directives(Some(declared(false, Marks::Acked)));
        let mut replicas = vec![Index::default(); 4];
        let mut seen = [0usize; 2];
        for (i, req) in decode_trace(3, 2_500).iter().enumerate() {
            if i == 1_500 {
                mach.drain(1);
            }
            mach.serve_request(req);
            let now = mach.arrival_ns;
            for (d, replica) in replicas.iter_mut().enumerate() {
                for event in mach.domains[d].take_kv_events() {
                    replica.apply(&event);
                }
                let live = mach.domains[d].kv_live_marks(now);
                assert_eq!(replica.live_marks(now), live, "request {i} domain {d}");
                for (_, mark) in live {
                    seen[mark.rank as usize] += 1;
                }
            }
        }
        assert!(
            seen.iter().all(|&n| n > 0),
            "the fixture must hold both ranks, or the invariant is untested: {seen:?}"
        );
        let stats = mach.directive_stats();
        assert!(stats.applied > 0 && stats.expired > 0);
        assert_eq!(mach.instruments.directives.honoured, stats.applied);
    }

    fn lossy(recovery: crate::belief::Recovery, loss: f64, episodes: bool) -> Conditions {
        use crate::belief::{Episode, LoadSource};
        Conditions {
            cadence: true,
            lag_ns: 30_000,
            loss,
            recovery,
            period_ns: 1_000_000_000,
            episodes: if episodes {
                (0..4)
                    .map(|node| Episode {
                        node,
                        from_ns: 2_000_000_000 * (node as u64 + 1),
                        until_ns: 2_000_000_000 * (node as u64 + 1) + 1_000_000_000,
                    })
                    .collect()
            } else {
                Vec::new()
            },
            seed: 5,
            load: LoadSource::Path,
        }
    }

    fn run_lossy(conditions: Conditions, directives: Option<Directives>) -> (Machine, Vec<String>) {
        let mut mach = gate_machine(Control::Unified, false);
        mach.set_belief(conditions);
        mach.set_scoring(Scoring::Expected);
        mach.set_instrument(true);
        mach.set_directives(directives);
        mach.set_foresight(None);
        let trace = decode_trace(3, 2_500);
        let mut costs = Vec::new();
        for (i, req) in trace.iter().enumerate() {
            mach.set_position(i as u64);
            costs.push(format!("{:?}", mach.serve_request(req)));
        }
        (mach, costs)
    }

    #[test]
    fn ignored_directives_and_an_acknowledged_belief_change_nothing() {
        use crate::belief::Recovery;
        for recovery in [Recovery::Replay, Recovery::None] {
            let conditions = || lossy(recovery, 0.05, false);
            let (off, a) = run_lossy(conditions(), None);
            let (ignored, b) = run_lossy(conditions(), Some(declared(true, Marks::Acked)));
            assert_eq!(a, b, "{recovery:?}");
            assert!(ignored.instruments.directives.emitted > 0, "{recovery:?}");
            assert_eq!(ignored.instruments.directives.honoured, 0);
            assert_eq!(
                format!("{:?}", off.instruments.phantom_blocks),
                format!("{:?}", ignored.instruments.phantom_blocks)
            );
            assert_eq!(off.directive_stats(), ignored.directive_stats());
        }
    }

    #[test]
    fn honoured_directives_move_the_engine_and_the_belief_hears_them() {
        let (off, _) = run_lossy(lossy(crate::belief::Recovery::Replay, 0.0, false), None);
        let (on, _) = run_lossy(
            lossy(crate::belief::Recovery::Replay, 0.0, false),
            Some(declared(false, Marks::Acked)),
        );
        assert!(on.directive_stats().applied > 0);
        assert!(on.instruments.directives.honoured > 0);
        assert_eq!(off.directive_stats().applied, 0);
        let acked: usize = (0..4)
            .map(|d| {
                on.observer()
                    .map_or(0, |o| o.belief(d).index().marks().count())
            })
            .sum();
        assert!(acked > 0, "an honoured mark is echoed on the stream");
    }

    #[test]
    fn every_phantom_and_miss_has_exactly_one_cause_that_its_condition_allows() {
        use crate::belief::Recovery;
        let cases = [
            ("exact", Conditions::exact(), false, false),
            (
                "loss, replay",
                lossy(Recovery::Replay, 0.2, false),
                true,
                false,
            ),
            ("loss, none", lossy(Recovery::None, 0.2, false), true, false),
            (
                "loss, periodic",
                lossy(Recovery::Periodic, 0.2, false),
                true,
                false,
            ),
            (
                "episodes, replay",
                lossy(Recovery::Replay, 0.0, true),
                false,
                true,
            ),
        ];
        for (name, conditions, loss, episodes) in cases {
            let none = conditions.recovery == Recovery::None && !conditions.is_exact();
            let (mach, _) = run_lossy(conditions, None);
            let i = &mach.instruments;
            let by = |c: Cause| i.phantom_blocks[c.idx()];
            let share: f64 = i.phantom_cause_share.iter().sum();
            assert!((share - i.phantom_share).abs() < 1e-9, "{name}");
            let miss_share: f64 = i.miss_cause_share.iter().sum();
            assert!((miss_share - i.miss_share).abs() < 1e-9, "{name}");
            if name == "exact" {
                assert_eq!(i.phantom_blocks, [0; Cause::N]);
                assert_eq!(i.miss_blocks, [0; Cause::N]);
                continue;
            }
            assert!(i.phantom_blocks.iter().sum::<u64>() > 0, "{name}");
            if !episodes {
                assert_eq!(by(Cause::Silenced), 0, "{name}");
            }
            if !loss {
                assert_eq!(by(Cause::Dropped), 0, "{name}");
            }
            if !none {
                assert_eq!(by(Cause::Stranded), 0, "{name}");
            }
        }
    }

    #[test]
    fn prefill_ahead_is_accounted_block_for_block_and_only_helps_flows() {
        let run = |ahead: bool| {
            let mut mach = gate_machine(Control::Unified, false);
            mach.set_prefill_ahead(ahead);
            let origins = Origins::default();
            mach.set_origins(origins.clone());
            let workload = Workload::with_fanout(3, 2_500, 1.0, 0.1)
                .with_decode_kv(crate::work::TOKENS_PER_KV_BLOCK)
                .with_origins(origins);
            for (i, req) in workload.enumerate() {
                mach.set_position(i as u64);
                mach.serve_request(&req);
            }
            mach
        };
        let (plain, ahead) = (run(false), run(true));
        assert_eq!(plain.instruments.prefill.calls, 0);
        let p = ahead.instruments.prefill;
        assert!(p.calls > 0 && p.blocks > 0 && p.work_ns > 0);
        let placed: u64 = ahead.domains.iter().map(|h| h.prefilled_blocks).sum();
        assert_eq!(p.blocks, placed);
        assert!(p.landings > 0 && p.landed <= p.landings);
        let per_flow =
            |m: &Machine| m.instruments.flow.stall_ns as f64 / m.instruments.flow.n.max(1) as f64;
        assert!(plain.instruments.flow.n > 0 && ahead.instruments.flow.n > 0);
        assert!(per_flow(&ahead) < per_flow(&plain));
    }

    #[test]
    fn the_reuse_table_classifies_every_session_and_flow_access_exactly_once() {
        let mut mach = gate_machine(Control::Unified, false);
        let origins = Origins::default();
        mach.set_origins(origins.clone());
        let workload = Workload::with_fanout(3, 2_500, 1.0, 0.1)
            .with_decode_kv(crate::work::TOKENS_PER_KV_BLOCK)
            .with_origins(origins);
        let mut dispatched = 0u64;
        for (i, req) in workload.enumerate() {
            mach.set_position(i as u64);
            if !mach.serve_request(&req).pending && req.gang.is_none() {
                dispatched += req
                    .chain
                    .iter()
                    .filter(|(_, m)| m.kind == BlobKind::KvBlock)
                    .count() as u64;
            }
        }
        let reuse = mach.instruments.reuse.as_ref().expect("origins turn it on");
        assert!(mach.instruments.tenants.is_none());
        let accesses: u64 = reuse.rows.iter().map(|r| r.accesses).sum();
        assert!(accesses >= dispatched && dispatched > 0);
        for (i, row) in reuse.rows.iter().enumerate() {
            let missed = row.evicted_to_offload + row.evicted_to_spill + row.evicted_gone;
            assert_eq!(row.hit + row.cold + missed, row.accesses, "origin {i}");
        }
        let flow = &reuse.rows[Origin::FlowPrompt.idx()];
        assert!(flow.accesses > 0 && flow.hit < flow.accesses);
        assert!(reuse.rows[Origin::Tenant.idx()].hit > 0);
    }

    #[test]
    fn tracking_origins_and_events_changes_no_cost() {
        let run = |tracked: bool| {
            let mut mach = gate_machine(Control::Unified, false);
            let origins = Origins::default();
            if tracked {
                mach.set_origins(origins.clone());
            }
            let mut workload = Workload::with_fanout(3, 1_500, 1.0, 0.1)
                .with_decode_kv(crate::work::TOKENS_PER_KV_BLOCK);
            if tracked {
                workload = workload.with_origins(origins);
            }
            let mut costs = Vec::new();
            for (i, req) in workload.enumerate() {
                mach.set_position(i as u64);
                costs.push(format!("{:?}", mach.serve_request(&req)));
            }
            costs
        };
        assert_eq!(run(false), run(true));
    }
    fn served_costs(mach: &mut Machine, trace: &[Request]) -> Vec<String> {
        mach.set_arrival_rate(250.0);
        trace
            .iter()
            .map(|r| format!("{:?}", mach.serve_request(r)))
            .collect()
    }

    fn one_model_trace(seed: u64, ops: u64) -> Vec<Request> {
        Workload::with_fanout(seed, ops, 1.0, 0.1)
            .with_decode_kv(crate::work::TOKENS_PER_KV_BLOCK)
            .with_one_model(true)
            .collect()
    }

    fn decode_ns(costs: &[String]) -> u64 {
        costs
            .iter()
            .filter_map(|c| {
                let tail = c.split("exec_ns: ").nth(1)?;
                tail.split(',').next()?.parse::<u64>().ok()
            })
            .sum()
    }

    #[test]
    fn a_batch_per_model_changes_nothing_when_every_request_names_one_model() {
        let trace = one_model_trace(2, 1_500);
        let mut shared = engine_machine(4 << 30, 1 << 30, Control::Unified);
        let base = served_costs(&mut shared, &trace);
        for priced in [false, true] {
            let mut mach = engine_machine(4 << 30, 1 << 30, Control::Unified);
            mach.set_model_batches(Batching::PerModel, priced);
            assert_eq!(base, served_costs(&mut mach, &trace), "priced={priced}");
        }
        assert!(decode_ns(&base) > 0);
    }

    #[test]
    fn a_batch_per_model_is_dearer_where_four_models_decode() {
        let trace = decode_trace(2, 1_500);
        let spread = || placed_machine(4 << 30, 1 << 30, Control::Unified, Placement::Blind);
        let mut shared = spread();
        let base = decode_ns(&served_costs(&mut shared, &trace));
        let mut mach = spread();
        mach.set_model_batches(Batching::PerModel, true);
        let sliced = decode_ns(&served_costs(&mut mach, &trace));
        assert!(sliced > 2 * base, "{sliced} against {base}");
        let in_flight = mach.models_in_flight();
        assert!(in_flight[3] + in_flight[4] > in_flight[1] + in_flight[2]);
    }

    #[test]
    fn an_allowance_no_step_exceeds_makes_prefill_time_change_nothing() {
        let trace = decode_trace(3, 1_500);
        let mut off = engine_machine(4 << 30, 1 << 30, Control::Unified);
        let base = served_costs(&mut off, &trace);
        for priced in [false, true] {
            let mut mach = engine_machine(4 << 30, 1 << 30, Control::Unified);
            mach.set_prefill_time(
                Some(PrefillLoad {
                    window_ns: 1_000_000_000,
                    free_ns: 1_000_000_000,
                }),
                priced,
            );
            assert_eq!(base, served_costs(&mut mach, &trace), "priced={priced}");
            assert_eq!(mach.stretch_ns(), 0);
        }
    }

    #[test]
    fn prefill_time_lengthens_decodes_by_the_work_the_engine_was_handed() {
        let trace = decode_trace(3, 1_500);
        let mut off = engine_machine(4 << 30, 1 << 30, Control::Unified);
        let base = served_costs(&mut off, &trace);
        assert!(off.prefill_work_ns() > 0, "every rebuild reaches an engine");
        assert_eq!(off.stretch_ns(), 0);
        let mut mach = engine_machine(4 << 30, 1 << 30, Control::Unified);
        mach.set_prefill_time(
            Some(PrefillLoad {
                window_ns: 1_000_000_000,
                free_ns: 0,
            }),
            false,
        );
        let stretched = served_costs(&mut mach, &trace);
        assert!(mach.stretch_ns() > 0);
        assert!(decode_ns(&stretched) > decode_ns(&base));
        assert!(!mach.prices_prefill());
    }

    #[test]
    fn a_priced_prefill_term_reaches_the_score_only_when_the_engine_is_loaded() {
        let trace = decode_trace(3, 1_500);
        let load = Some(PrefillLoad {
            window_ns: 1_000_000_000,
            free_ns: 0,
        });
        let mut blind = engine_machine(4 << 30, 1 << 30, Control::Unified);
        blind.set_prefill_time(load, false);
        served_costs(&mut blind, &trace);
        let mut priced = engine_machine(4 << 30, 1 << 30, Control::Unified);
        priced.set_prefill_time(load, true);
        served_costs(&mut priced, &trace);
        assert!(blind.term_spread[5].abs() < f64::EPSILON);
        assert!(priced.term_spread[5] > 0.0);
        assert!(priced.prices_prefill());
    }
    fn fleet_machine(placement: &[Option<Model>], window: Option<u64>) -> Machine {
        let catalogue = crate::fleet::Catalogue::published(8_000_000_000);
        let catalogue = window.map_or(catalogue.clone(), |w| catalogue.with_context(w));
        catalogued_machine(placement, &catalogue, |_| 3 << 30)
    }

    fn catalogued_machine(
        placement: &[Option<Model>],
        catalogue: &crate::fleet::Catalogue,
        partition: impl Fn(usize) -> u64,
    ) -> Machine {
        let bands = [0u8; BlobKind::N];
        let node = |d: usize| NodeMemory {
            hbm: 4 << 30,
            ddr: 8 << 30,
            nvme: 64 << 30,
            hbm_quota: Quota::open(4 << 30, bands),
            ddr_quota: Quota::open(8 << 30, bands),
            can_decode: true,
            kv: Some(crate::cache::EngineKv {
                partition: partition(d),
                offload: 1 << 30,
                spill: 8 << 30,
                clairvoyant: false,
            }),
        };
        let topo = Topology::cluster(
            placement.len(),
            1,
            8 << 30,
            Distance::Rack,
            Crossing::default(),
        );
        let mut mach = Machine::new(topo, node, Policy::Gdsf, Placement::Scored);
        mach.set_flow_aware(true);
        mach.set_state_transfer(true);
        mach.set_model_batches(Batching::PerModel, true);
        mach.set_fleet(crate::fleet::Fleet::new(catalogue.clone(), placement), None);
        mach
    }

    const SIZED: [u64; 4] = [1 << 29, 1 << 30, 1 << 30, 2 << 30];

    fn sized_machine(placement: &[Option<Model>]) -> Machine {
        let catalogue = crate::fleet::Catalogue::published(8_000_000_000).with_sizes(SIZED);
        let held = placement.to_vec();
        catalogued_machine(placement, &catalogue, move |d| {
            (4u64 << 30) - held[d].map_or(0, |m| SIZED[usize::from(m)])
        })
    }

    fn keyed_trace(seed: u64, ops: u64) -> Vec<Request> {
        Workload::with_fanout(seed, ops, 1.0, 0.1)
            .with_decode_kv(crate::work::TOKENS_PER_KV_BLOCK)
            .with_model_keyed(true)
            .collect()
    }

    #[test]
    fn every_decode_lands_on_a_replica_serving_its_model() {
        let placement = [Some(0), Some(1), Some(2), Some(3)];
        let mut mach = fleet_machine(&placement, None);
        served_costs(&mut mach, &keyed_trace(2, 1_500));
        let fleet = mach.fleet().expect("a fleet");
        let mut served = 0;
        for (d, row) in fleet.stats.served.iter().enumerate() {
            for (m, n) in row.iter().enumerate() {
                if Some(m as Model) != placement[d] {
                    assert_eq!(*n, 0, "node {d} decoded model {m}");
                }
                served += n;
            }
        }
        assert!(served > 100);
        assert_eq!(fleet.stats.unplaced, 0);
    }

    #[test]
    fn weights_never_reach_the_ledger_under_a_fleet() {
        let mut mach = fleet_machine(&[Some(0), Some(1), Some(2), Some(3)], None);
        served_costs(&mut mach, &keyed_trace(2, 1_500));
        let w = BlobKind::WeightShard.idx();
        let ops = mach.engine_ops();
        assert_eq!(
            ops.admit[w] + ops.touch[w] + ops.demote[w] + ops.spill[w],
            0
        );
        for d in 0..mach.nodes() {
            let h = &mach.domains[d];
            assert_eq!(
                h.hits[w] + h.misses[w] + h.offload_hits[w] + h.nvme_hits[w],
                0
            );
            assert_eq!(h.weights_bytes(), Some(1 << 30));
        }
    }

    #[test]
    fn a_model_with_no_replica_is_unplaced_and_counted_apart() {
        let mut mach = fleet_machine(&[Some(0), Some(0), Some(1), Some(1)], None);
        let trace = keyed_trace(2, 1_500);
        let costs = served_costs(&mut mach, &trace);
        let unplaced = mach.fleet().map_or(0, |f| f.stats.unplaced);
        assert!(unplaced > 100, "{unplaced}");
        assert!(costs.iter().any(|c| c.contains("pending: true")));
        assert_eq!(mach.refused_by_router.iter().sum::<u64>(), 0);
        assert!(decode_ns(&costs) > 0);
    }

    #[test]
    fn a_context_window_the_partition_cannot_hold_is_not_routed_to() {
        let mut mach = fleet_machine(&[Some(0), Some(1), Some(2), Some(3)], Some(4));
        served_costs(&mut mach, &keyed_trace(2, 600));
        let fleet = mach.fleet().expect("a fleet");
        let served: u64 = fleet.stats.served.iter().flatten().sum();
        assert_eq!(served, 0);
        assert!(fleet.stats.unplaced > 100);
    }

    #[test]
    fn a_reload_empties_the_node_keeps_hbm_whole_and_makes_the_replica_wait() {
        let mut mach = fleet_machine(&[Some(0), Some(0), Some(1), Some(1)], None);
        let trace = keyed_trace(2, 1_500);
        served_costs(&mut mach, &trace[..600]);
        assert!(
            mach.domains[0]
                .kv_partition()
                .is_some_and(|(_, _)| mach.domains[0].resident_bytes(BlobKind::KvBlock) > 0)
        );
        let drain = mach.engines[0].drained_by(mach.arrival_ns) - mach.arrival_ns;
        assert!(drain > 0, "node 0 is decoding when it is told to move");
        let moved = mach.apply_placement(&[Some(2), Some(0), Some(1), Some(1)]);
        assert_eq!(moved, 1);
        let fleet = mach.fleet().expect("a fleet");
        assert_eq!(fleet.model_on(0), Some(2));
        assert_eq!(fleet.stats.loads, 1);
        assert!(fleet.stats.kv_lost_bytes > 0);
        assert_eq!(
            fleet.wait_ns(0, mach.arrival_ns),
            drain + 16_000_000_000,
            "what it is decoding finishes, then the cold load"
        );
        assert_eq!(mach.domains[0].resident_bytes(BlobKind::KvBlock), 0);
        let (capacity, _) = mach.domains[0].kv_partition().expect("a partition");
        assert_eq!(
            capacity + mach.domains[0].weights_bytes().expect("weights"),
            mach.domains[0].hbm_bytes()
        );
        mach.arrival_ns += drain + 16_000_000_000;
        mach.fleet
            .as_mut()
            .expect("a fleet")
            .settle(mach.arrival_ns);
        assert_eq!(mach.fleet().map(|f| f.wait_ns(0, mach.arrival_ns)), Some(0));
    }

    #[test]
    fn a_request_reaching_a_loading_replica_waits_for_the_rest_of_the_load() {
        let mut mach = fleet_machine(&[Some(0), Some(0), Some(1), Some(1)], None);
        let trace = keyed_trace(2, 2_500);
        mach.set_arrival_rate(250.0);
        for req in &trace[..400] {
            mach.serve_request(req);
        }
        mach.apply_placement(&[Some(2), Some(0), Some(1), Some(1)]);
        let ready = mach.arrival_ns + mach.fleet().map_or(0, |f| f.wait_ns(0, mach.arrival_ns));
        let mut waited = None;
        for req in &trace[400..] {
            if model_of(&req.requires) != Some(2) || req.gang.is_some() {
                mach.serve_request(req);
                continue;
            }
            let now = mach.arrival_ns + mach.interval_ns;
            let cost = mach.serve_request(req);
            waited = Some((cost.queue_ns, ready.saturating_sub(now)));
            break;
        }
        let (queue_ns, remaining) = waited.expect("a request for the loading model");
        assert!(
            remaining > 0 && queue_ns >= remaining,
            "{queue_ns} against {remaining}"
        );
    }

    #[test]
    #[should_panic(expected = "exceed")]
    fn weights_that_do_not_fit_beside_the_partition_are_refused() {
        let mut mach = fleet_machine(&[Some(0), Some(1), Some(2), Some(3)], None);
        mach.domains[0].reload(2 << 30, 3 << 30);
    }

    #[test]
    fn a_load_is_priced_from_the_cheapest_copy_of_the_model() {
        let mut mach = fleet_machine(&[Some(0), Some(0), Some(1), Some(1)], None);
        let start = 8_000_000_000;
        let bytes = 2 * WEIGHT_BYTES;
        let peer = mach.topo.fetch_ns(mach.unit_in(0), 2, bytes);
        assert_eq!(mach.load_ns(0, 1), start + peer);
        assert_eq!(mach.load_ns(0, 2), start + 2 * WEIGHT_NS);
        assert_eq!(mach.load_ns(0, 0), start + TierSpec::pcie().fetch_ns(bytes));
        mach.apply_placement(&[Some(1), Some(0), Some(1), Some(1)]);
        assert_eq!(mach.load_ns(0, 0), start + TierSpec::pcie().fetch_ns(bytes));
        assert!(
            mach.fleet()
                .is_some_and(|f| f.cached(0, 0) && f.cached(0, 1))
        );
        mach.apply_placement(&[Some(1), Some(0), Some(3), Some(3)]);
        let cold = start + 2 * WEIGHT_NS;
        let now = mach.arrival_ns;
        assert_eq!(
            mach.fleet().map(|f| (f.wait_ns(2, now), f.wait_ns(3, now))),
            Some((cold, cold)),
            "a copy still in flight is no source"
        );
    }
    #[test]
    fn a_concurrent_request_arrives_at_the_instant_of_the_one_before_it() {
        let trace: Vec<Request> = Workload::with_fanout(2, 800, 1.0, 0.0)
            .with_decode_kv(crate::work::TOKENS_PER_KV_BLOCK)
            .with_fresh(0.3)
            .collect();
        let mut mach = engine_machine(4 << 30, 1 << 30, Control::Unified);
        mach.set_arrival_rate(250.0);
        let (mut base, mut extra, mut last) = (0u64, 0u64, 0u64);
        for req in &trace {
            mach.serve_request(req);
            if req.concurrent {
                assert_eq!(mach.arrival_ns, last);
                extra += 1;
            } else {
                assert_eq!(mach.arrival_ns, last + mach.interval_ns);
                base += 1;
            }
            last = mach.arrival_ns;
        }
        assert!(extra > 50 && base > 500);
    }
    fn mix_trace(seed: u64, ops: u64, head: [f64; MODEL_COUNT]) -> Vec<Request> {
        Workload::with_fanout(seed, ops, 0.0, 0.05)
            .with_decode_kv(crate::work::TOKENS_PER_KV_BLOCK)
            .with_model_keyed(true)
            .with_model_mix(crate::work::rotating_mix(head))
            .collect()
    }

    fn planned(placement: &[Option<Model>], kind: PlannerKind, start_ns: u64) -> Machine {
        let mut mach = fleet_machine(placement, None);
        let catalogue = crate::fleet::Catalogue::published(start_ns);
        mach.set_fleet(crate::fleet::Fleet::new(catalogue, placement), None);
        mach.set_planner(kind, 5_000_000_000);
        mach
    }

    const EVEN: [Option<Model>; 8] = [
        Some(0),
        Some(0),
        Some(1),
        Some(1),
        Some(2),
        Some(2),
        Some(3),
        Some(3),
    ];

    #[test]
    fn a_stationary_mix_makes_every_planner_stop_where_it_started() {
        let trace = mix_trace(2, 8_000, [0.25; 4]);
        for kind in [PlannerKind::Follow, PlannerKind::Eager] {
            let mut mach = planned(&EVEN, kind, 8_000_000_000);
            served_costs(&mut mach, &trace);
            assert!(mach.planner_ticks() >= 5);
            assert_eq!(mach.fleet().map(|f| f.stats.loads), Some(0), "{kind:?}");
        }
    }

    #[test]
    fn a_shifting_mix_moves_replicas_toward_the_model_that_became_hot_and_every_move_is_a_write() {
        let trace = mix_trace(2, 8_000, [0.7, 0.1, 0.1, 0.1]);
        let mut mach = planned(&EVEN, PlannerKind::Eager, 8_000_000_000);
        served_costs(&mut mach, &trace);
        let fleet = mach.fleet().expect("a fleet");
        assert!(fleet.counts()[3] >= 4, "{:?}", fleet.counts());
        assert!(fleet.counts().iter().all(|&n| n >= 1));
        assert_eq!(fleet.stats.moves.len() as u64, fleet.stats.loads);
        assert_eq!(
            fleet
                .stats
                .moves
                .iter()
                .map(|m| m.ready_ns - m.at_ns)
                .sum::<u64>(),
            fleet.stats.downtime_ns
        );
        assert!(fleet.stats.loads >= 2);
    }

    #[test]
    fn follow_waits_for_the_loss_to_pay_for_a_costly_move_and_eager_does_not() {
        let trace = mix_trace(2, 8_000, [0.7, 0.1, 0.1, 0.1]);
        let first_move = |kind| {
            let mut mach = planned(&EVEN, kind, 30_000_000_000);
            served_costs(&mut mach, &trace);
            mach.fleet()
                .and_then(|f| f.stats.moves.first().map(|m| m.at_ns))
        };
        let (eager, follow) = (
            first_move(PlannerKind::Eager),
            first_move(PlannerKind::Follow),
        );
        assert!(eager.is_some());
        assert!(follow.is_none_or(|f| f >= eager.unwrap_or(0)));
    }

    #[test]
    fn once_moves_after_the_first_interval_and_never_again() {
        let trace = mix_trace(2, 8_000, [0.7, 0.1, 0.1, 0.1]);
        let mut mach = planned(&EVEN, PlannerKind::Once, 8_000_000_000);
        served_costs(&mut mach, &trace);
        let fleet = mach.fleet().expect("a fleet");
        assert!(fleet.stats.loads > 0);
        let times: std::collections::HashSet<u64> =
            fleet.stats.moves.iter().map(|m| m.at_ns).collect();
        assert_eq!(times.len(), 1);
        assert!(times.iter().all(|&t| t < 6_000_000_000));
    }

    #[test]
    fn demand_the_fleet_could_not_serve_is_demand_the_planner_sees() {
        let trace = mix_trace(2, 6_000, [0.25; 4]);
        let placement = [
            Some(0),
            Some(0),
            Some(1),
            Some(1),
            Some(0),
            Some(0),
            Some(1),
            Some(1),
        ];
        let mut mach = planned(&placement, PlannerKind::Eager, 8_000_000_000);
        served_costs(&mut mach, &trace);
        let fleet = mach.fleet().expect("a fleet");
        assert!(
            fleet.counts()[2] >= 1 && fleet.counts()[3] >= 1,
            "{:?}",
            fleet.counts()
        );
        assert!(fleet.stats.unplaced > 0);
    }

    #[test]
    fn a_sized_fleet_gives_each_node_the_partition_and_the_step_its_model_leaves() {
        let mut mach = sized_machine(&[Some(0), Some(1), Some(2), Some(3)]);
        let partition = |mach: &Machine, d: usize| mach.domains[d].kv_partition().map(|(c, _)| c);
        for (d, bytes) in SIZED.iter().enumerate() {
            assert_eq!(partition(&mach, d), Some((4 << 30) - bytes));
            assert_eq!(
                mach.engines[d].step_base_ns(),
                crate::engine::STEP_BASE_NS * bytes / (1 << 30)
            );
        }
        served_costs(&mut mach, &keyed_trace(2, 1_500));
        mach.apply_placement(&[Some(3), Some(1), Some(2), Some(3)]);
        assert_eq!(partition(&mach, 0), Some(2 << 30));
        assert_eq!(
            mach.engines[0].step_base_ns(),
            2 * crate::engine::STEP_BASE_NS
        );
        assert_eq!(mach.domains[0].weights_bytes(), Some(2 << 30));
    }

    #[test]
    fn at_equal_demand_a_planner_gives_the_larger_model_more_replicas() {
        let even: [Option<Model>; 8] = [
            Some(0),
            Some(0),
            Some(1),
            Some(1),
            Some(2),
            Some(2),
            Some(3),
            Some(3),
        ];
        let mut mach = sized_machine(&even);
        mach.set_planner(PlannerKind::Eager, 5_000_000_000);
        served_costs(&mut mach, &mix_trace(2, 8_000, [0.25; 4]));
        let counts = mach.fleet().map(Fleet::counts).expect("a fleet");
        assert_eq!(counts, [1, 2, 2, 3]);
    }
    fn pair_machine(prefillers: usize, rule: Pairing, over_ns: u64, fetch: bool) -> Machine {
        let mut mach = fleet_machine(&[Some(0); 8], None);
        for d in 0..8 {
            let lane = if d < prefillers {
                Role::Prefill
            } else {
                Role::Decode
            };
            mach.fleet.as_mut().expect("a fleet").set_role(d, lane);
        }
        mach.set_prefill_time(
            Some(crate::engine::PrefillLoad {
                window_ns: 1_000_000_000,
                free_ns: 0,
            }),
            true,
        );
        mach.set_pairing(rule, over_ns, fetch);
        mach.set_arrival_rate(400.0);
        mach
    }

    fn fresh_trace(seed: u64, ops: u64, fresh: f64) -> Vec<Request> {
        Workload::with_fanout(seed, ops, 1.0, 0.0)
            .with_decode_kv(crate::work::TOKENS_PER_KV_BLOCK)
            .with_model_keyed(true)
            .with_one_model(true)
            .with_fresh(fresh)
            .collect()
    }

    #[test]
    fn a_paired_prefill_runs_on_the_prefiller_and_the_decoder_never_does_it() {
        let trace = fresh_trace(2, 3_000, 0.3);
        let mut aggregated = pair_machine(0, Pairing::Off, 0, false);
        served_costs(&mut aggregated, &trace);
        let mut paired = pair_machine(2, Pairing::List, 0, false);
        served_costs(&mut paired, &trace);
        let stats = paired.pair_stats;
        assert!(stats.paired > 500 && stats.paired == stats.decisions - stats.failed);
        let fleet = paired.fleet().expect("a fleet");
        assert!(fleet.stats.served[..2].iter().flatten().all(|&n| n == 0));
        assert!(fleet.stats.served[2..].iter().flatten().sum::<u64>() > 500);
        let work_at = |m: &Machine, nodes: std::ops::Range<usize>| -> u64 {
            nodes.map(|d| m.engines[d].prefill_work_ns).sum()
        };
        assert!(work_at(&paired, 0..2) > 0);
        assert!(work_at(&paired, 2..8) < work_at(&aggregated, 2..8) / 2);
        assert!(paired.stretch_ns() < aggregated.stretch_ns());
    }

    #[test]
    fn a_prefiller_that_holds_no_history_starts_over_unless_it_may_fetch_the_prefix() {
        let trace = fresh_trace(2, 3_000, 0.0);
        let ratio = |fetch: bool| {
            let mut mach = pair_machine(2, Pairing::List, 0, fetch);
            served_costs(&mut mach, &trace);
            let s = mach.pair_stats;
            s.work_ns as f64 / s.avoided_ns as f64
        };
        assert!(ratio(false) > 1.05, "{}", ratio(false));
        assert!((ratio(true) - 1.0).abs() < 1e-9, "{}", ratio(true));
    }

    #[test]
    fn a_prefiller_is_a_first_come_queue() {
        let trace = fresh_trace(2, 1_500, 1.0);
        let mut mach = pair_machine(1, Pairing::List, 0, false);
        served_costs(&mut mach, &trace);
        let s = mach.pair_stats;
        assert!(s.paired > 1_000 && s.wait_ns > 0);
        assert!(s.wait_ns / s.paired > 1_000_000, "{s:?}");
    }

    #[test]
    fn a_joint_rule_declines_a_saturated_prefiller_where_a_list_does_not() {
        let trace = fresh_trace(2, 3_000, 0.3);
        let share = |rule| {
            let mut mach = pair_machine(1, rule, 0, false);
            served_costs(&mut mach, &trace);
            let s = mach.pair_stats;
            (
                s.paired as f64 / s.decisions as f64,
                s.wait_ns as f64 / s.paired.max(1) as f64,
            )
        };
        let (joint, joint_wait) = share(Pairing::Joint);
        let (list, list_wait) = share(Pairing::List);
        assert!(list > 0.99);
        assert!(joint < 0.6, "{joint}");
        assert!(
            joint_wait < list_wait / 2.0,
            "{joint_wait} against {list_wait}"
        );
    }

    #[test]
    fn an_independent_rule_leaves_short_prefills_at_the_decoder_and_coupling_counts_the_difference()
    {
        let trace = fresh_trace(2, 3_000, 0.0);
        let mut never = pair_machine(2, Pairing::Independent, u64::MAX, false);
        served_costs(&mut never, &trace);
        assert_eq!(never.pair_stats.paired, 0);
        let mut joint = pair_machine(2, Pairing::Joint, 0, false);
        served_costs(&mut joint, &trace);
        let s = joint.pair_stats;
        assert!(s.decisions > 500 && s.coupled <= s.decisions);
        assert!(
            s.coupled > 0,
            "joint and independent agree on every decision"
        );
    }

    #[test]
    fn pairing_needs_a_decode_replica_and_a_prefiller_of_its_model() {
        let trace = fresh_trace(2, 1_000, 0.3);
        let mut mach = pair_machine(0, Pairing::Joint, 0, false);
        served_costs(&mut mach, &trace);
        assert_eq!(mach.pair_stats.decisions, 0);
        let mut both = pair_machine(2, Pairing::Joint, 0, false);
        for d in 0..8 {
            both.fleet
                .as_mut()
                .expect("a fleet")
                .set_role(d, Role::Both);
        }
        served_costs(&mut both, &trace);
        assert_eq!(both.pair_stats.decisions, 0);
    }
    #[test]
    fn a_planner_gives_fresh_prompts_prefillers_and_takes_none_where_there_is_no_prefill() {
        let prefillers = |fresh: f64| {
            let mut mach = pair_machine(0, Pairing::Joint, 0, false);
            mach.set_planner(PlannerKind::Eager, 2_000_000_000);
            served_costs(&mut mach, &fresh_trace(2, 8_000, fresh));
            let fleet = mach.fleet().expect("a fleet");
            (fleet.prefiller_counts()[0], fleet.stats.role_moves)
        };
        let (heavy, moves) = prefillers(0.6);
        assert!(heavy >= 2 && moves >= heavy as u64, "{heavy} {moves}");
        assert!(prefillers(0.0).0 <= 1);
    }

    #[test]
    fn a_replica_that_changes_model_comes_back_as_a_decoder() {
        let mut mach = pair_machine(2, Pairing::Joint, 0, false);
        let mut placement = vec![Some(0); 8];
        placement[0] = Some(1);
        mach.apply_placement(&placement);
        let fleet = mach.fleet().expect("a fleet");
        assert_eq!(fleet.role_of(0), Role::Decode);
        assert_eq!(fleet.role_of(1), Role::Prefill);
    }
    fn tenant_run(shared_prefix: bool) -> Machine {
        let mut mach = gate_machine(Control::Unified, false);
        let origins = Origins::default();
        mach.set_origins(origins.clone());
        mach.track_tenants();
        let workload = Workload::with_fanout(3, 4_000, 1.0, 0.05)
            .with_decode_kv(crate::work::TOKENS_PER_KV_BLOCK)
            .with_shared_prefix(shared_prefix)
            .with_origins(origins);
        for (i, req) in workload.enumerate() {
            mach.set_position(i as u64);
            mach.serve_request(&req);
        }
        mach
    }

    #[test]
    fn no_tenant_touches_a_block_another_tenant_brought_in_until_a_prefix_is_shared() {
        let own = tenant_run(false);
        let t = own.instruments.tenants.as_ref().expect("tracked");
        assert!(t.touches > 1_000);
        assert_eq!(t.cross_touches, 0, "nothing is shared across owners");
        assert!(
            t.evictions > 100
                && t.evictions_by_other * 2 > t.evictions
                && t.evictions_by_other < t.evictions,
            "{} {}",
            t.evictions,
            t.evictions_by_other
        );
        let shared = tenant_run(true);
        let s = shared.instruments.tenants.as_ref().expect("tracked");
        assert!(
            s.cross_touches > t.cross_touches + 500,
            "{} {}",
            s.cross_touches,
            t.cross_touches
        );
    }

    #[test]
    fn hits_by_origin_and_by_tenant_account_for_every_read() {
        let mach = tenant_run(false);
        let t = mach.instruments.tenants.as_ref().expect("tracked");
        let by_origin: (u64, u64) = t
            .by_origin
            .iter()
            .fold((0, 0), |a, r| (a.0 + r.0, a.1 + r.1));
        let by_owner: (u64, u64) = t
            .per_owner
            .values()
            .fold((0, 0), |a, r| (a.0 + r.0, a.1 + r.1));
        assert_eq!(by_origin, by_owner);
        assert_eq!(by_origin.0, t.touches);
        let tenant_row = t.by_origin[Origin::Tenant.idx()];
        assert!(tenant_row.1 as f64 > 0.5 * tenant_row.0 as f64);
        let ranked = t.by_volume(24);
        assert_eq!(ranked.len(), 24);
        assert!(t.hit_rate_of(ranked[..1].iter().copied()) > 0.0);
    }
    fn neighbour_trace(seed: u64, ops: u64, rate: f64) -> Vec<Request> {
        Workload::with_fanout(seed, ops, 1.0, 0.0)
            .with_decode_kv(crate::work::TOKENS_PER_KV_BLOCK)
            .with_model_keyed(true)
            .with_one_model(true)
            .with_neighbour(rate)
            .collect()
    }

    #[test]
    fn a_replica_set_confines_the_neighbour_to_its_replicas_and_the_others_to_the_rest() {
        let trace = neighbour_trace(2, 6_000, 0.3);
        let mut mach = pair_machine(0, Pairing::Off, 0, false);
        mach.set_tenant_set(2);
        let mut on = [0u64; 2];
        for req in &trace {
            let served_before: Vec<u64> = (0..8)
                .map(|d| mach.fleet().map_or(0, |f| f.stats.served[d].iter().sum()))
                .collect();
            let cost = mach.serve_request(req);
            if cost.pending || req.tokens == 0 || req.tenant.is_none() {
                continue;
            }
            let node = (0..8)
                .find(|&d| {
                    mach.fleet()
                        .map_or(0, |f| f.stats.served[d].iter().sum::<u64>())
                        > served_before[d]
                })
                .expect("a decode lands somewhere");
            let burst = req.tenant == Some(crate::work::NEIGHBOUR_TENANT);
            assert_eq!(node < 2, burst, "node {node} for tenant {:?}", req.tenant);
            on[usize::from(burst)] += 1;
        }
        assert!(on[0] > 1_000 && on[1] > 200, "{on:?}");
    }

    #[test]
    fn a_prefill_quota_refuses_the_neighbours_excess_and_leaves_the_others_alone() {
        let trace = neighbour_trace(2, 6_000, 0.3);
        let refused = |rate_ns: f64| {
            let mut mach = pair_machine(0, Pairing::Off, 0, false);
            mach.set_tenant_quota(rate_ns, 0);
            served_costs(&mut mach, &trace);
            let neighbour = mach
                .tenant_refused
                .get(&crate::work::NEIGHBOUR_TENANT)
                .copied()
                .unwrap_or(0);
            let others: u64 = mach
                .tenant_refused
                .iter()
                .filter(|(t, _)| **t != crate::work::NEIGHBOUR_TENANT)
                .map(|(_, n)| n)
                .sum();
            (neighbour, others)
        };
        assert_eq!(refused(0.0), (0, 0));
        let (tight, tight_others) = refused(0.25e9);
        let (loose, _) = refused(1.0e9);
        assert!(tight > 300 && tight_others == 0, "{tight} {tight_others}");
        assert!(loose < tight, "{loose} {tight}");
    }

    #[test]
    fn a_slot_meter_refuses_a_tenant_with_its_slots_full() {
        let trace = neighbour_trace(2, 6_000, 0.5);
        let mut mach = pair_machine(0, Pairing::Off, 0, false);
        mach.set_tenant_quota(0.0, 1);
        let costs = served_costs(&mut mach, &trace);
        let refused: u64 = mach.tenant_refused.values().sum();
        assert!(refused > 50, "{refused}");
        assert!(costs.iter().any(|c| !c.contains("pending: true")));
    }
}
