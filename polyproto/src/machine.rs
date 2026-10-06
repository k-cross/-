use crate::admit::{Lengths, Reservations, Reserve};
use crate::belief::{Cause, Conditions, Marks, Observer, SLO_QUANTILE, Scoring};
use crate::blob::{BlobId, BlobKind, BlobMeta};
use crate::boundary::Cost as Crossing;
use crate::cache::{CellState, Cost, Hierarchy, NodeMemory, Policy};
use crate::engine::{Batching, Engine, MAX_BATCH, MODEL_COUNT, Model, PrefillLoad};
use crate::fault::{
    Client, Degrade, EngineCrash, Estimators, Fate, Fault, FaultStats, NodeLoss, Observe, Rebuild,
    Restart, Spill, Subscriber,
};
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
    Agent, Authority, Gang, Origin, Origins, Pattern, Request, RequestView, Slo, ToolCall,
    WEIGHT_BYTES, WEIGHT_NS, model_of,
};
use crate::writes::{Counted, Writes};
use std::cell::Cell;
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

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RegionMode {
    Global,
    Regional,
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Overflow {
    Off,
    Node,
    RegionMean,
    Threshold { utilisation: f64 },
}

#[derive(Clone, Debug)]
pub struct Shards {
    k: usize,
    report_ns: u64,
    flights: Vec<Vec<Vec<u64>>>,
    reported: Vec<Vec<usize>>,
    next_report_ns: u64,
}

#[derive(Clone, Debug)]
pub struct TenantShares {
    quotas: HashMap<u32, f64>,
    depth_s: f64,
    lease_ns: Option<u64>,
    shares: Vec<f64>,
    observed: Vec<f64>,
    next_refresh_ns: u64,
    buckets: HashMap<(u32, usize), (f64, u64)>,
    pub offered: u64,
    pub refused: u64,
    pub refreshes: u64,
}

impl TenantShares {
    #[must_use]
    pub fn new(
        quotas: HashMap<u32, f64>,
        regions: usize,
        depth_s: f64,
        lease_ns: Option<u64>,
    ) -> Self {
        Self {
            quotas,
            depth_s,
            lease_ns,
            shares: vec![1.0 / regions as f64; regions],
            observed: vec![0.0; regions],
            next_refresh_ns: 0,
            buckets: HashMap::new(),
            offered: 0,
            refused: 0,
            refreshes: 0,
        }
    }

    #[must_use]
    pub fn shares(&self) -> &[f64] {
        &self.shares
    }

    fn refresh(&mut self, now: u64, lease_ns: u64) {
        self.next_refresh_ns = now + lease_ns;
        let total: f64 = self.observed.iter().sum();
        if total > 0.0 {
            let floored: Vec<f64> = self
                .observed
                .iter()
                .map(|o| (o / total).max(LEASE_FLOOR))
                .collect();
            let sum: f64 = floored.iter().sum();
            self.shares = floored.iter().map(|f| f / sum).collect();
        }
        self.observed.iter_mut().for_each(|o| *o *= LEASE_DECAY);
        self.refreshes += 1;
    }

    fn admit(&mut self, tenant: u32, region: usize, tokens: u64, now: u64) -> bool {
        let Some(&quota) = self.quotas.get(&tenant) else {
            return true;
        };
        self.offered += 1;
        if let Some(lease_ns) = self.lease_ns {
            self.observed[region] += tokens as f64;
            if self.next_refresh_ns == 0 {
                self.next_refresh_ns = now + lease_ns;
            } else if now >= self.next_refresh_ns {
                self.refresh(now, lease_ns);
            }
        }
        let tokens = tokens as f64;
        let rate = quota * self.shares[region];
        let cap = (rate * self.depth_s).max(tokens);
        let (level, last) = self.buckets.entry((tenant, region)).or_insert((cap, now));
        *level = (*level + rate * now.saturating_sub(*last) as f64 / 1e9).min(cap);
        *last = now;
        if *level < tokens {
            self.refused += 1;
            return false;
        }
        *level -= tokens;
        true
    }
}

#[derive(Clone, Debug, Default)]
pub struct TableStats {
    pub recomputes: u64,
    pub changes: u64,
}

#[derive(Clone, Debug)]
struct RoutingTable {
    epoch_ns: u64,
    fractions: Vec<Vec<f64>>,
    tokens: Vec<u64>,
    requests: Vec<u64>,
    next_ns: u64,
    epoch: u64,
}

#[derive(Clone, Debug)]
pub enum BudgetRule {
    Static,
    Planned(Vec<(u64, Vec<usize>)>),
    Follow { interval_ns: u64 },
}

#[derive(Clone, Debug)]
pub struct Budgets {
    rule: BudgetRule,
    load_ns: u64,
    provisioned: Vec<bool>,
    ready_at: Vec<u64>,
    next_follow_ns: u64,
    accrued_ns: f64,
    tokens: Vec<u64>,
    pub moves: u64,
    pub loading_ns: u64,
}

impl Budgets {
    #[must_use]
    pub fn new(
        rule: BudgetRule,
        regions: usize,
        slots: usize,
        running: usize,
        load_ns: u64,
    ) -> Self {
        Self {
            rule,
            load_ns,
            provisioned: (0..regions * slots).map(|d| d % slots < running).collect(),
            ready_at: vec![0; regions * slots],
            next_follow_ns: 0,
            accrued_ns: 0.0,
            tokens: vec![0; regions],
            moves: 0,
            loading_ns: 0,
        }
    }

    #[must_use]
    pub fn running(&self, node: usize) -> bool {
        self.provisioned[node]
    }
}

#[derive(Clone, Debug, Default)]
pub struct RegionStats {
    pub facing: [u64; BlobKind::N],
    pub away: [u64; BlobKind::N],
    pub reach_ns: u64,
    pub reach_hops: u64,
    pub dispatched: Vec<u64>,
    pub spilled: u64,
    pub forced: u64,
}

#[derive(Clone, Debug)]
pub struct Regions {
    of: Vec<usize>,
    one_way_ns: Vec<Vec<u64>>,
    pub mode: RegionMode,
    pub price_reach: bool,
    pub client_bytes: u64,
    pub overflow: Overflow,
    pub summary_ns: u64,
    pub own_forwards: bool,
    table: Option<RoutingTable>,
    pub table_stats: TableStats,
    summary: Vec<usize>,
    next_summary_ns: u64,
    forwards: Vec<Vec<Vec<u64>>>,
    at_summary: Vec<Vec<usize>>,
    pub stats: RegionStats,
}

impl Regions {
    #[must_use]
    pub fn new(per_region: usize, one_way_ns: Vec<Vec<u64>>, mode: RegionMode) -> Self {
        let count = one_way_ns.len();
        let nodes = count * per_region;
        Self {
            of: (0..nodes).map(|d| d / per_region).collect(),
            one_way_ns,
            mode,
            price_reach: true,
            client_bytes: CLIENT_BYTES,
            overflow: Overflow::Off,
            summary_ns: 0,
            own_forwards: false,
            table: None,
            table_stats: TableStats::default(),
            summary: vec![0; nodes],
            next_summary_ns: 0,
            forwards: vec![vec![Vec::new(); count]; count],
            at_summary: vec![vec![0; count]; count],
            stats: RegionStats {
                dispatched: vec![0; nodes],
                ..RegionStats::default()
            },
        }
    }

    pub fn set_table(&mut self, epoch_ns: Option<u64>) {
        let count = self.count();
        self.table = epoch_ns.map(|epoch_ns| RoutingTable {
            epoch_ns,
            fractions: (0..count)
                .map(|a| (0..count).map(|b| f64::from(u8::from(a == b))).collect())
                .collect(),
            tokens: vec![0; count],
            requests: vec![0; count],
            next_ns: 0,
            epoch: 0,
        });
    }

    #[must_use]
    pub fn table_fractions(&self) -> Option<&[Vec<f64>]> {
        self.table.as_ref().map(|t| t.fractions.as_slice())
    }

    #[must_use]
    pub fn count(&self) -> usize {
        self.one_way_ns.len()
    }

    #[must_use]
    pub fn region_of(&self, node: usize) -> usize {
        self.of[node]
    }

    #[must_use]
    pub fn round_trip_ns(&self, a: usize, b: usize) -> u64 {
        if a == b {
            return 0;
        }
        2 * self.one_way_ns[a][b]
            + (self.client_bytes as f64 * crate::topo::Distance::Region.ns_per_byte()) as u64
    }
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
    prefill_ready: HashMap<u64, u64>,
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
    engine_wait: EngineWait,
    probe_engine: bool,
    engine_queues: Vec<Vec<Waiting>>,
    next_request: usize,
    closed: Vec<(usize, Cost)>,
    pub engine_waits: EngineWaitStats,
    queue: Queue,
    router_queue: Vec<Waiting>,
    pub queue_waits: QueueStats,
    claims: Claim,
    lengths: [Lengths; 2],
    attained: HashMap<u64, u64>,
    completions: BinaryHeap<Reverse<(u64, u64, u64)>>,
    cancel: CancelMode,
    victim: Victim,
    flights: Vec<Flight>,
    handles: Option<Handles>,
    pub cancel_stats: CancelStats,
    aborted_requests: HashSet<usize>,
    finishing: bool,
    triggers: Triggers,
    departures: Option<Departures>,
    departed: Vec<usize>,
    pub departure_stats: DepartureStats,
    track_stream: bool,
    pub stream: StreamStats,
    submitted: u64,
    deciding: Pattern,
    pub locality_by: [(u64, u64); Pattern::N],
    tool_slots: Option<usize>,
    slot_free: Vec<Vec<u64>>,
    last_slot: Option<ToolSlot>,
    claim_key: ClaimKey,
    root_lengths: HashMap<u32, Lengths>,
    root_observed: HashMap<u32, (u64, u64)>,
    last_home: usize,
    hint_grade: HintGrade,
    learner: FlowLearner,
    counted: Counted,
    count_events: bool,
    armed: bool,
    gang_parts: Vec<Part>,
    gangs: Vec<GangFlight>,
    orphans: Vec<Part>,
    retry_gangs: Vec<GangRetry>,
    down_until: u64,
    known: Option<Known>,
    unknown_reads: Cell<u64>,
    observe: Observe,
    pending_lengths: Vec<PendingLength>,
    shadow: Option<Shadow>,
    node_check: bool,
    snapshot_every_ns: Option<u64>,
    next_snapshot_ns: u64,
    snapshot: Option<EstimatorState>,
    refused_ids: HashSet<usize>,
    decode_out: Vec<u64>,
    lost: Vec<Option<u64>>,
    limbo: Vec<Limbo>,
    copy_durable: bool,
    durable_copies: HashMap<BlobId, u64>,
    lost_durable: Vec<BlobId>,
    degraded: Option<Degraded>,
    pub fault_stats: FaultStats,
    regions: Option<Regions>,
    scope: Option<usize>,
    client_region: Option<usize>,
    budgets: Option<Budgets>,
    shards: Option<Shards>,
    shard: usize,
    tenant_shares: Option<TenantShares>,
    residency: f64,
    confined: bool,
}

#[derive(Clone, Copy, Debug)]
struct Degraded {
    until_ns: u64,
    placement: Placement,
    flow_aware: bool,
    state_transfer: bool,
}

#[derive(Clone, Debug)]
struct Limbo {
    release_ns: u64,
    parked_ns: u64,
    waiting: Waiting,
}

#[derive(Debug)]
struct Shadow {
    until_ns: u64,
    reserved: Vec<Reservations>,
    load: Vec<BinaryHeap<Reverse<u64>>>,
    seqs: HashMap<(usize, u64), u64>,
}

#[derive(Clone, Debug)]
struct EstimatorState {
    lengths: [Lengths; 2],
    observed: [(u64, u64); 2],
    root_lengths: HashMap<u32, Lengths>,
    root_observed: HashMap<u32, (u64, u64)>,
    learner: FlowLearner,
    attained: HashMap<u64, u64>,
    taken_ns: u64,
}

impl EstimatorState {
    fn bytes(&self) -> u64 {
        let histograms: u64 = self
            .lengths
            .iter()
            .chain(self.root_lengths.values())
            .map(Lengths::bytes)
            .sum();
        let templates: usize = self
            .learner
            .template
            .values()
            .map(|chain| size_of::<BlobId>() + chain.len() * size_of::<(BlobId, BlobMeta)>())
            .sum();
        let entries = size_of_val(&self.observed)
            + self.root_lengths.len() * size_of::<u32>()
            + self.root_observed.len() * size_of::<(u32, (u64, u64))>()
            + self.attained.len() * size_of::<(u64, u64)>()
            + self.learner.seen.len() * size_of::<(BlobId, (u64, u64))>()
            + self.learner.task_fn.len() * size_of::<(u64, (BlobId, usize))>()
            + templates;
        histograms + entries as u64
    }
}

#[derive(Clone, Debug)]
struct Part {
    node: usize,
    handles: Handles,
    req: Request,
}

#[derive(Clone, Debug)]
struct GangFlight {
    id: usize,
    seq: u64,
    arrival_ns: u64,
    req: Request,
    cost: Cost,
    end_ns: u64,
    parts: Vec<Part>,
}

#[derive(Clone, Debug)]
struct GangRetry {
    id: usize,
    seq: u64,
    arrival_ns: u64,
    req: Request,
}

#[derive(Clone, Debug)]
struct Known {
    until_ns: u64,
    sets: Vec<HashSet<BlobId>>,
}

#[derive(Clone, Copy, Debug)]
struct PendingLength {
    end_ns: u64,
    node: usize,
    slo: usize,
    tokens: u64,
    root: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ToolSlot {
    pub node: usize,
    pub slot: usize,
    pub start_ns: u64,
    pub end_ns: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum ClaimKey {
    #[default]
    Slo,
    Root,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum HintGrade {
    #[default]
    Declared,
    Template,
    Learned,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct LearnStats {
    pub flows: u64,
    pub known: u64,
    pub fired: u64,
    pub calls: u64,
}

#[derive(Clone, Debug, Default)]
struct FlowLearner {
    seen: HashMap<BlobId, (u64, u64)>,
    task_fn: HashMap<u64, (BlobId, usize)>,
    template: HashMap<BlobId, Vec<(BlobId, BlobMeta)>>,
    gate: f64,
    stats: LearnStats,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum EngineWait {
    #[default]
    Off,
    Fifo,
    FirstFit,
    Priority,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Queue {
    #[default]
    Off,
    Fifo,
    Slo,
    Plas,
}

#[derive(Clone, Copy, Debug, PartialEq, Default)]
pub enum Claim {
    #[default]
    Static,
    Quantile {
        q: f64,
        pooled: bool,
    },
    Tiered {
        q: f64,
    },
    Gate(f64),
}

#[derive(Clone, Copy, Debug)]
struct Arrival {
    id: Option<usize>,
    seq: u64,
    at_ns: u64,
    abort_ns: u64,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Departures {
    pub share: f64,
    pub leak: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Triggers {
    #[default]
    Both,
    Router,
}

pub const STREAM_BYTES_PER_TOKEN: u64 = 200;

const DEPARTURE_POINT_KEY: u64 = 0x5555_5555;

#[derive(Clone, Debug, Default)]
pub struct DepartureStats {
    pub leaving: u64,
    pub aborted: u64,
    pub leaked_ns: u64,
    pub freed_ns: u64,
}

#[derive(Clone, Debug, Default)]
pub struct StreamStats {
    pub samples: u64,
    pub peak_tokens: Vec<u64>,
    pub sum_tokens: Vec<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum CancelMode {
    #[default]
    Off,
    Continue,
    Drop,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Victim {
    #[default]
    Recent,
    Remaining,
    Attained,
}

#[derive(Clone, Copy, Debug)]
struct Handles {
    cache_seq: Option<u64>,
    resv_seq: Option<u64>,
    belief_hold: Option<u64>,
    start_ns: u64,
    end_ns: u64,
}

impl Handles {
    fn progress(self, now: u64) -> f64 {
        if now <= self.start_ns {
            return 0.0;
        }
        ((now - self.start_ns) as f64 / (self.end_ns - self.start_ns).max(1) as f64).min(1.0)
    }
}

#[derive(Clone, Debug)]
struct Flight {
    id: usize,
    seq: u64,
    leaves_at: Option<u64>,
    leaving: bool,
    node: usize,
    handles: Handles,
    dispatched_ns: u64,
    arrival_ns: u64,
    req: Request,
    cost: Cost,
    pre_crash: bool,
}

#[derive(Clone, Debug, Default)]
pub struct CancelStats {
    pub cancels: u64,
    pub engine_cancels: u64,
    pub freed_bytes: u64,
    pub wasted_decode_ns: u64,
    pub reprefill_ns: u64,
}

#[derive(Clone, Debug, Default)]
pub struct QueueStats {
    pub queued: u64,
    pub max_depth: usize,
    pub waited: [u64; 2],
    pub waited_ns: [u64; 2],
}

#[derive(Clone, Debug)]
pub enum Submitted {
    Closed(Cost),
    Open(usize),
}

#[derive(Clone, Debug)]
struct Waiting {
    id: usize,
    seq: u64,
    req: Request,
    arrival_ns: u64,
    queued_ns: u64,
    pre_ns: u64,
    decide_ns: u64,
    requeued: bool,
    not_before: u64,
}

#[derive(Clone, Debug, Default)]
pub struct EngineWaitStats {
    pub queued: u64,
    pub unfittable: u64,
    pub max_depth: usize,
    pub waited: [u64; 2],
    pub waited_ns: [u64; 2],
    pub probe_ns: Vec<u64>,
    pub probe_unplaceable: u64,
}

const FINISH_STEPS: u64 = 10_000_000;

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
            prefill_ready: HashMap::new(),
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
            engine_wait: EngineWait::Off,
            probe_engine: false,
            engine_queues: vec![Vec::new(); n_domains],
            next_request: 0,
            closed: Vec::new(),
            engine_waits: EngineWaitStats::default(),
            queue: Queue::Off,
            router_queue: Vec::new(),
            queue_waits: QueueStats::default(),
            claims: Claim::Static,
            lengths: [Lengths::default(), Lengths::default()],
            attained: HashMap::new(),
            completions: BinaryHeap::new(),
            cancel: CancelMode::Off,
            victim: Victim::Recent,
            flights: Vec::new(),
            handles: None,
            cancel_stats: CancelStats::default(),
            aborted_requests: HashSet::new(),
            finishing: false,
            triggers: Triggers::Both,
            departures: None,
            departed: Vec::new(),
            departure_stats: DepartureStats::default(),
            track_stream: false,
            stream: StreamStats {
                samples: 0,
                peak_tokens: vec![0; n_domains],
                sum_tokens: vec![0; n_domains],
            },
            submitted: 0,
            deciding: Pattern::Plain,
            locality_by: [(0, 0); Pattern::N],
            tool_slots: None,
            slot_free: vec![Vec::new(); n_domains],
            last_slot: None,
            claim_key: ClaimKey::Slo,
            root_lengths: HashMap::new(),
            root_observed: HashMap::new(),
            last_home: 0,
            hint_grade: HintGrade::Declared,
            learner: FlowLearner::default(),
            counted: Counted::default(),
            count_events: false,
            armed: false,
            gang_parts: Vec::new(),
            gangs: Vec::new(),
            orphans: Vec::new(),
            retry_gangs: Vec::new(),
            down_until: 0,
            known: None,
            unknown_reads: Cell::new(0),
            observe: Observe::Dispatch,
            pending_lengths: Vec::new(),
            shadow: None,
            node_check: false,
            snapshot_every_ns: None,
            next_snapshot_ns: 0,
            snapshot: None,
            refused_ids: HashSet::new(),
            decode_out: vec![0; n_domains],
            lost: vec![None; n_domains],
            limbo: Vec::new(),
            copy_durable: false,
            durable_copies: HashMap::new(),
            lost_durable: Vec::new(),
            degraded: None,
            fault_stats: FaultStats::default(),
            regions: None,
            scope: None,
            client_region: None,
            budgets: None,
            shards: None,
            shard: 0,
            tenant_shares: None,
            residency: 0.0,
            confined: false,
        }
    }

    pub fn set_shards(&mut self, schedulers: usize, report_ns: u64) {
        let nodes = self.domains.len();
        self.shards = (schedulers > 1).then(|| Shards {
            k: schedulers,
            report_ns,
            flights: vec![vec![Vec::new(); nodes]; schedulers],
            reported: vec![vec![0; nodes]; schedulers],
            next_report_ns: 0,
        });
    }

    pub fn set_tenant_shares(&mut self, shares: Option<TenantShares>) {
        self.tenant_shares = shares;
    }

    #[must_use]
    pub fn tenant_shares(&self) -> Option<&TenantShares> {
        self.tenant_shares.as_ref()
    }

    pub fn set_residency(&mut self, share: f64) {
        self.residency = share;
    }

    fn restricted(&self, tenant: u32) -> bool {
        self.residency > 0.0
            && ((seq_hint(u64::from(tenant) ^ RESIDENCY_KEY) >> 11) as f64 / (1u64 << 53) as f64)
                < self.residency
    }

    fn assign_shard(&mut self, req: &Request) {
        if let Some(k) = self.shards.as_ref().map(|sh| sh.k) {
            let key = if req.program == 0 {
                self.submitted
            } else {
                req.program
            };
            self.shard = (seq_hint(key) % k as u64) as usize;
        }
    }

    fn shard_view(&self, d: usize) -> Option<usize> {
        let sh = self.shards.as_ref()?;
        let now = self.arrival_ns;
        let own = sh.flights[self.shard][d]
            .iter()
            .filter(|&&end| end > now)
            .count();
        let others: usize = (0..sh.k)
            .filter(|&o| o != self.shard)
            .map(|o| {
                if sh.report_ns == 0 {
                    sh.flights[o][d].iter().filter(|&&end| end > now).count()
                } else {
                    sh.reported[o][d]
                }
            })
            .sum();
        Some((own + others).min(MAX_BATCH))
    }

    fn refresh_shards(&mut self, now: u64) {
        let Some(sh) = self.shards.as_mut() else {
            return;
        };
        for per in &mut sh.flights {
            for ends in per.iter_mut() {
                ends.retain(|&e| e > now);
            }
        }
        if sh.report_ns == 0 || now < sh.next_report_ns {
            return;
        }
        for (reported, flights) in sh.reported.iter_mut().zip(&sh.flights) {
            for (slot, ends) in reported.iter_mut().zip(flights) {
                *slot = ends.len();
            }
        }
        sh.next_report_ns = now + sh.report_ns;
    }

    pub fn set_budgets(&mut self, budgets: Option<Budgets>) {
        self.budgets = budgets;
    }

    #[must_use]
    pub fn budgets(&self) -> Option<&Budgets> {
        self.budgets.as_ref()
    }

    fn available(&self, d: usize) -> bool {
        self.budgets
            .as_ref()
            .is_none_or(|b| b.provisioned[d] && b.ready_at[d] <= self.arrival_ns)
    }

    pub fn set_regions(&mut self, regions: Option<Regions>) {
        self.regions = regions;
    }

    #[must_use]
    pub fn regions(&self) -> Option<&Regions> {
        self.regions.as_ref()
    }

    #[must_use]
    pub fn served_region(&self) -> Option<usize> {
        self.regions.as_ref().map(|r| r.region_of(self.last_home))
    }

    fn enter_region(&mut self, req: &Request) {
        self.scope = None;
        self.client_region = None;
        self.confined = false;
        let Some((mode, overflow)) = self.regions.as_ref().map(|r| (r.mode, r.overflow)) else {
            return;
        };
        let home = usize::from(req.region);
        if req.client_facing() {
            self.client_region = Some(home);
        }
        self.confined = req.tenant.is_some_and(|t| self.restricted(t));
        if mode == RegionMode::Global {
            self.scope = self.confined.then_some(home);
            return;
        }
        let decoding = Self::needs_decode(req);
        let mut target = home;
        if decoding && self.client_region.is_some() && !self.confined {
            target = self.table_target(req, home);
            if target == home && overflow != Overflow::Off {
                target = self.overflow_target(req, home, overflow);
            }
        }
        self.scope = Some(target);
        if let Some(r) = self.regions.as_mut() {
            r.stats.spilled += u64::from(target != home);
        }
        if decoding && req.gang.is_none() && !self.confined {
            self.forward_for_model(req);
        }
    }

    fn table_target(&self, req: &Request, home: usize) -> usize {
        let Some(table) = self.regions.as_ref().and_then(|r| r.table.as_ref()) else {
            return home;
        };
        let key = seq_hint(req.program ^ (table.epoch << 40) ^ TABLE_KEY);
        let u = (key >> 11) as f64 / (1u64 << 53) as f64;
        let mut acc = 0.0;
        for (region, fraction) in table.fractions[home].iter().enumerate() {
            acc += fraction;
            if u < acc {
                return region;
            }
        }
        home
    }

    fn recompute_table(&mut self, now: u64) {
        let Some((epoch_ns, count, next_ns)) = self
            .regions
            .as_ref()
            .and_then(|r| r.table.as_ref().map(|t| (t.epoch_ns, r.count(), t.next_ns)))
        else {
            return;
        };
        if now < next_ns {
            return;
        }
        let nodes: Vec<usize> = (0..count).map(|r| self.region_nodes(r).len()).collect();
        let Some(r) = self.regions.as_mut() else {
            return;
        };
        let round_trips: Vec<Vec<f64>> = (0..count)
            .map(|a| (0..count).map(|b| r.round_trip_ns(a, b) as f64).collect())
            .collect();
        let Some(table) = r.table.as_mut() else {
            return;
        };
        let first = table.next_ns == 0;
        table.next_ns = now + epoch_ns;
        let tokens = std::mem::replace(&mut table.tokens, vec![0; count]);
        let requests = std::mem::replace(&mut table.requests, vec![0; count]);
        if first {
            return;
        }
        let seconds = epoch_ns as f64 / 1e9;
        let demand: Vec<f64> = tokens.iter().map(|&t| t as f64 / seconds).collect();
        let per_request: Vec<f64> = tokens
            .iter()
            .zip(&requests)
            .map(|(&t, &n)| {
                if n == 0 {
                    TABLE_TOKENS_DEFAULT
                } else {
                    t as f64 / n as f64
                }
            })
            .collect();
        let costs = crate::fleet::Costs::published(epoch_ns.max(1_000_000_000));
        let fractions = route_demand(&costs, &demand, &per_request, &nodes, &round_trips);
        let changed = r.table.as_ref().is_some_and(|t| t.fractions != fractions);
        r.table_stats.changes += u64::from(changed);
        r.table_stats.recomputes += 1;
        if let Some(table) = r.table.as_mut() {
            table.fractions = fractions;
            table.epoch += 1;
        }
    }

    fn forward_for_model(&mut self, req: &Request) {
        let Some(home) = self.scope else {
            return;
        };
        let all = self.decode_pool_all();
        if !self.eligible(self.scoped(&all), req).is_empty() {
            return;
        }
        let Some(r) = &self.regions else {
            return;
        };
        let mut others: Vec<usize> = (0..r.count()).filter(|&s| s != home).collect();
        others.sort_by_key(|&s| r.one_way_ns[home][s]);
        for s in others {
            self.scope = Some(s);
            if !self.eligible(self.scoped(&all), req).is_empty() {
                if let Some(r) = self.regions.as_mut() {
                    r.stats.forced += 1;
                }
                return;
            }
        }
        self.scope = Some(home);
    }

    fn refresh_summary(&mut self, now: u64) {
        let Some(r) = self.regions.as_mut() else {
            return;
        };
        if r.summary_ns == 0 || now < r.next_summary_ns {
            return;
        }
        for (d, slot) in r.summary.iter_mut().enumerate() {
            *slot = self.engines[d].load(now);
        }
        for (a, row) in r.forwards.iter_mut().enumerate() {
            for (b, ends) in row.iter_mut().enumerate() {
                ends.retain(|&e| e > now);
                r.at_summary[a][b] = ends.len();
            }
        }
        r.next_summary_ns = now + r.summary_ns;
    }

    fn summarised_load(&self, d: usize) -> usize {
        match &self.regions {
            Some(r) if r.summary_ns > 0 => r.summary[d],
            _ => self.engines[d].load(self.arrival_ns),
        }
    }

    fn forwarded_delta(&self, from: usize, to: usize) -> f64 {
        let Some(r) = &self.regions else {
            return 0.0;
        };
        if !r.own_forwards || r.summary_ns == 0 {
            return 0.0;
        }
        let live = r.forwards[from][to]
            .iter()
            .filter(|&&e| e > self.arrival_ns)
            .count();
        let nodes = self.region_nodes(to).len().max(1);
        (live as f64 - r.at_summary[from][to] as f64) / nodes as f64
    }

    fn region_nodes(&self, region: usize) -> Vec<usize> {
        self.regions.as_ref().map_or_else(Vec::new, |r| {
            (0..r.of.len())
                .filter(|&d| r.of[d] == region && self.domains[d].can_decode() && self.available(d))
                .collect()
        })
    }

    fn region_utilisation(&self, region: usize, live: bool) -> f64 {
        let nodes = self.region_nodes(region);
        if nodes.is_empty() {
            return 1.0;
        }
        let load: usize = nodes
            .iter()
            .map(|&d| {
                if live {
                    self.engines[d].load(self.arrival_ns)
                } else {
                    self.summarised_load(d)
                }
            })
            .sum();
        load as f64 / (nodes.len() * MAX_BATCH) as f64
    }

    fn engine_quote_ns(&self, d: usize, req: &Request, load: usize) -> f64 {
        if req.tokens == 0 || self.interval_ns == 0 {
            return 0.0;
        }
        (self.engines[d].projected_live(self.arrival_ns, req.tokens, load)
            + self.engines[d].congestion_live(req.tokens, load)) as f64
    }

    fn remote_quote_ns(&self, d: usize, req: &Request, flow: &[(usize, u64)], from: usize) -> f64 {
        let delta = self.forwarded_delta(from, self.region_of(d));
        let load = (self.summarised_load(d) as f64 + delta).round().max(0.0) as usize;
        let rebuild: u64 = req.chain.iter().map(|(_, m)| m.recompute_ns).sum();
        let unit = self.unit_in(d);
        let handoff: f64 = flow
            .iter()
            .filter(|(src, _)| *src != d)
            .map(|&(src, payload)| self.topo.fetch_ns(unit, src, payload) as f64)
            .sum();
        self.engine_quote_ns(d, req, load) + rebuild as f64 + handoff
    }

    fn region_mean_quote_ns(&self, region: usize, req: &Request, live: bool, from: usize) -> f64 {
        let nodes: Vec<usize> = self
            .eligible(self.decode_pool_all(), req)
            .into_iter()
            .filter(|&d| self.region_of(d) == region)
            .collect();
        if nodes.is_empty() {
            return f64::MAX;
        }
        let delta = if live {
            0.0
        } else {
            self.forwarded_delta(from, region)
        };
        let sum: f64 = nodes
            .iter()
            .map(|&d| {
                let base = if live {
                    self.engines[d].load(self.arrival_ns)
                } else {
                    self.summarised_load(d)
                };
                self.engine_quote_ns(d, req, (base as f64 + delta).round().max(0.0) as usize)
            })
            .sum();
        sum / nodes.len() as f64
    }

    fn overflow_target(&mut self, req: &Request, home: usize, overflow: Overflow) -> usize {
        match overflow {
            Overflow::Off => home,
            Overflow::Threshold { utilisation } => self.threshold_target(home, utilisation),
            Overflow::Node => self.node_price_target(req, home),
            Overflow::RegionMean => self.region_mean_target(req, home),
        }
    }

    fn threshold_target(&self, home: usize, utilisation: f64) -> usize {
        if self.region_utilisation(home, true) <= utilisation {
            return home;
        }
        let Some(r) = &self.regions else {
            return home;
        };
        let mut others: Vec<usize> = (0..r.count()).filter(|&s| s != home).collect();
        others.sort_by_key(|&s| r.one_way_ns[home][s]);
        others
            .into_iter()
            .find(|&s| {
                let delta = self.forwarded_delta(home, s) / MAX_BATCH as f64;
                self.region_utilisation(s, false) + delta < utilisation
            })
            .unwrap_or(home)
    }

    fn node_price_target(&mut self, req: &Request, home: usize) -> usize {
        let flow: Vec<(usize, u64)> = req
            .completes
            .filter(|_| self.flow_aware)
            .and_then(|t| self.upstream.get(&t).cloned())
            .unwrap_or_default();
        let in_region = |m: &Self, region: usize| -> Vec<usize> {
            m.eligible(m.decode_pool_all(), req)
                .into_iter()
                .filter(|&d| m.region_of(d) == region)
                .collect()
        };
        let saved = self.scope;
        self.scope = Some(home);
        let view = self.view_of(req);
        let local = in_region(self, home)
            .into_iter()
            .map(|d| self.placement_terms(d, &view, &flow, View::Belief).full())
            .fold(f64::MAX, f64::min);
        self.scope = saved;
        let Some(r) = &self.regions else {
            return home;
        };
        let mut best = (home, local);
        for s in (0..r.count()).filter(|&s| s != home) {
            let reach = if r.price_reach {
                r.round_trip_ns(home, s) as f64
            } else {
                0.0
            };
            let remote = in_region(self, s)
                .into_iter()
                .map(|d| self.remote_quote_ns(d, req, &flow, home))
                .fold(f64::MAX, f64::min)
                + reach;
            if remote < best.1 {
                best = (s, remote);
            }
        }
        best.0
    }

    fn region_mean_target(&mut self, req: &Request, home: usize) -> usize {
        let saved = self.scope;
        self.scope = Some(home);
        let view = self.view_of(req);
        let acquire = self
            .eligible(self.decode_pool_all(), req)
            .into_iter()
            .filter(|&d| self.region_of(d) == home)
            .map(|d| self.plan(d, &view, View::Belief).ns as f64)
            .fold(f64::MAX, f64::min);
        self.scope = saved;
        let local = self.region_mean_quote_ns(home, req, true, home) + acquire;
        let rebuild: f64 = req.chain.iter().map(|(_, m)| m.recompute_ns as f64).sum();
        let Some(r) = &self.regions else {
            return home;
        };
        let mut best = (home, local);
        for s in (0..r.count()).filter(|&s| s != home) {
            let remote = self.region_mean_quote_ns(s, req, false, home)
                + rebuild
                + r.round_trip_ns(home, s) as f64;
            if remote < best.1 {
                best = (s, remote);
            }
        }
        best.0
    }

    fn region_of(&self, node: usize) -> usize {
        self.regions.as_ref().map_or(0, |r| r.region_of(node))
    }

    fn scoped(&self, pool: &[usize]) -> Vec<usize> {
        self.scoped_to(pool, self.scope)
    }

    fn scoped_to(&self, pool: &[usize], scope: Option<usize>) -> Vec<usize> {
        pool.iter()
            .copied()
            .filter(|&d| self.available(d))
            .filter(|&d| match (scope, &self.regions) {
                (Some(region), Some(r)) => r.region_of(d) == region,
                _ => true,
            })
            .collect()
    }

    fn reach_term(&self, d: usize) -> f64 {
        match (&self.regions, self.client_region) {
            (Some(r), Some(c)) if r.price_reach => r.round_trip_ns(c, r.region_of(d)) as f64,
            _ => 0.0,
        }
    }

    pub fn set_copy_durable(&mut self, on: bool) {
        self.copy_durable = on;
    }

    pub fn set_node_check(&mut self, on: bool) {
        self.node_check = on;
    }

    pub fn set_snapshot_every(&mut self, every_ns: Option<u64>) {
        self.snapshot_every_ns = every_ns;
        self.next_snapshot_ns = every_ns.unwrap_or(0);
    }

    fn take_snapshot(&mut self) {
        let state = EstimatorState {
            lengths: self.lengths.clone(),
            observed: self.observed,
            root_lengths: self.root_lengths.clone(),
            root_observed: self.root_observed.clone(),
            learner: self.learner.clone(),
            attained: self.attained.clone(),
            taken_ns: self.arrival_ns,
        };
        self.counted.snapshots += 1;
        self.counted.snapshot_bytes += state.bytes();
        self.snapshot = Some(state);
    }

    fn restore_snapshot(&mut self) -> bool {
        let Some(state) = self.snapshot.clone() else {
            return false;
        };
        self.fault_stats.snapshot_restores += 1;
        self.fault_stats.snapshot_age_ns += self.arrival_ns - state.taken_ns;
        self.lengths = state.lengths;
        self.observed = state.observed;
        self.root_lengths = state.root_lengths;
        self.root_observed = state.root_observed;
        self.attained = state.attained;
        self.learner = FlowLearner {
            gate: self.learner.gate,
            stats: self.learner.stats,
            ..state.learner
        };
        true
    }

    fn blind_ledger(&mut self, restart: Restart) {
        let now = self.arrival_ns;
        for flight in &mut self.flights {
            flight.pre_crash = true;
        }
        self.fault_stats.hidden_flights += (self.flights.len() + self.orphans.len()) as u64;
        self.fault_stats.hidden_holders += self
            .reserved
            .iter()
            .map(Reservations::holders)
            .sum::<usize>() as u64;
        let n = self.domains.len();
        self.shadow = Some(Shadow {
            until_ns: match restart.ledger {
                Rebuild::After { after_ns } => now + restart.outage_ns + after_ns,
                Rebuild::Now | Rebuild::Never => u64::MAX,
            },
            reserved: (0..n).map(|_| Reservations::default()).collect(),
            load: vec![BinaryHeap::new(); n],
            seqs: HashMap::new(),
        });
    }

    fn shadow_load(&self, d: usize) -> Option<usize> {
        self.shadow.as_ref().map(|s| s.load[d].len())
    }

    fn note_blind_admission(
        &mut self,
        home: usize,
        req: &Request,
        blocks: &[(BlobId, BlobMeta)],
        extra: u64,
    ) {
        self.fault_stats.blind_admissions += 1;
        let capacity = self.domains[home]
            .kv_partition()
            .map_or(u64::MAX, |(c, _)| c);
        if !self.reserved[home].admits_claim(capacity, req, (blocks, extra), None) {
            self.fault_stats.over_admissions += 1;
        }
    }

    fn node_accepts(&self, d: usize, req: &Request) -> bool {
        !(self.node_check && self.shadow.is_some()) || self.node_admits(d, req)
    }

    fn node_admits(&self, d: usize, req: &Request) -> bool {
        let Some((capacity, _)) = self.domains[d].kv_partition().filter(|_| req.tokens > 0) else {
            return true;
        };
        let (blocks, extra) = self.claim(req);
        self.reserved[d].admits_claim(capacity, req, (&blocks, extra), None)
    }

    fn refuse_at_node(&mut self, req: &Request, arrival: Arrival) -> Submitted {
        let id = arrival.id.unwrap_or_else(|| self.new_request());
        self.fault_stats.refusal_attempts += 1;
        if self.refused_ids.insert(id) {
            self.fault_stats.refused_at_node += 1;
        }
        self.router_queue.push(Waiting {
            id,
            seq: arrival.seq,
            req: req.clone(),
            arrival_ns: arrival.at_ns,
            queued_ns: self.arrival_ns,
            pre_ns: 0,
            decide_ns: 0,
            requeued: true,
            not_before: self.arrival_ns + 1,
        });
        Submitted::Open(id)
    }

    pub fn set_armed(&mut self, on: bool) {
        self.armed = on;
    }

    pub fn set_observe(&mut self, observe: Observe) {
        self.observe = observe;
    }

    #[must_use]
    pub fn unknown_reads(&self) -> u64 {
        self.unknown_reads.get()
    }

    #[must_use]
    pub fn is_down(&self) -> bool {
        self.arrival_ns < self.down_until
    }

    pub fn inject(&mut self, fault: Fault) {
        match fault {
            Fault::Scheduler(restart) => self.restart_scheduler(restart),
            Fault::Estimators => self.reset_estimators(),
            Fault::Engine(crash) => self.crash_engine(crash),
            Fault::Node(loss) => self.lose_node(loss),
            Fault::Degrade(degrade) => self.degrade_routing(degrade),
        }
    }

    #[must_use]
    pub fn lost_durable(&self) -> &[BlobId] {
        &self.lost_durable
    }

    fn degrade_routing(&mut self, degrade: Degrade) {
        if self.degraded.is_none() {
            self.degraded = Some(Degraded {
                until_ns: self.arrival_ns + degrade.for_ns,
                placement: self.placement,
                flow_aware: self.flow_aware,
                state_transfer: self.state_transfer,
            });
        }
        self.fault_stats.degraded += 1;
        self.placement = Placement::Sticky;
        self.flow_aware = false;
        self.state_transfer = false;
    }

    fn crash_engine(&mut self, crash: EngineCrash) {
        let node = crash.node;
        self.fault_stats.engine_crashes += 1;
        self.fail_node_streams(node, crash.client);
        self.fault_stats.kv_lost +=
            self.domains[node].crash_engine(crash.spill == Spill::Kept) as u64;
        self.forget_engine(node);
        self.decode_out[node] = self.arrival_ns + crash.restart_ns;
        self.observe_emit(node);
    }

    fn lose_node(&mut self, loss: NodeLoss) {
        let node = loss.node;
        self.fault_stats.nodes_lost += 1;
        self.fail_node_streams(node, loss.client);
        let gone = self.domains[node].lose_node();
        self.fault_stats.kv_lost += gone.kv_blocks as u64;
        self.fault_stats.host_lost += gone.host_blobs as u64;
        for (id, _) in gone.durable {
            if self.durable_copies.contains_key(&id) {
                self.fault_stats.durable_saved += 1;
            } else {
                self.lost_durable.push(id);
                self.fault_stats.durable_lost_with_node += 1;
            }
        }
        self.forget_engine(node);
        self.lost[node] = Some(self.arrival_ns + loss.declare_ns);
        self.observe_emit(node);
    }

    fn fail_node_streams(&mut self, node: usize, client: Client) {
        let (hit, kept): (Vec<Flight>, Vec<Flight>) = std::mem::take(&mut self.flights)
            .into_iter()
            .partition(|f| f.node == node);
        self.flights = kept;
        for flight in hit {
            self.fail_flight(&flight, client);
        }
        let (hit, kept): (Vec<GangFlight>, Vec<GangFlight>) = std::mem::take(&mut self.gangs)
            .into_iter()
            .partition(|g| g.parts.iter().any(|p| p.node == node));
        self.gangs = kept;
        for gang in hit {
            self.fail_gang(gang);
        }
        let (hit, kept): (Vec<Part>, Vec<Part>) = std::mem::take(&mut self.orphans)
            .into_iter()
            .partition(|p| p.node == node);
        self.orphans = kept;
        for part in &hit {
            self.fail_part(part);
        }
        self.unqueue_engine(node);
    }

    fn forget_engine(&mut self, node: usize) {
        self.engines[node].flush();
        self.reserved[node].clear();
        self.redispatched[node].clear();
        self.pending_lengths.retain(|p| p.node != node);
        if let Some(known) = self.known.as_mut() {
            known.sets[node].clear();
        }
    }

    fn park_for_lease(&mut self, req: &Request, arrival: Arrival, release_ns: u64) -> Submitted {
        let id = arrival.id.unwrap_or_else(|| self.new_request());
        self.fault_stats.limbo_requests += 1;
        self.limbo.push(Limbo {
            release_ns,
            parked_ns: self.arrival_ns,
            waiting: Waiting {
                id,
                seq: arrival.seq,
                req: req.clone(),
                arrival_ns: arrival.at_ns,
                queued_ns: self.arrival_ns,
                pre_ns: 0,
                decide_ns: 0,
                requeued: true,
                not_before: 0,
            },
        });
        Submitted::Open(id)
    }

    fn declare_lost_nodes(&mut self, now: u64) {
        for d in 0..self.lost.len() {
            if self.lost[d].is_some_and(|at| at <= now) && self.active.len() > 1 {
                self.active.retain(|&a| a != d);
            }
        }
        let (due, rest): (Vec<Limbo>, Vec<Limbo>) = std::mem::take(&mut self.limbo)
            .into_iter()
            .partition(|l| l.release_ns <= now);
        self.limbo = rest;
        for parked in due {
            self.fault_stats.limbo_wait_ns += now - parked.parked_ns;
            self.router_queue.push(parked.waiting);
        }
    }

    fn restart_scheduler(&mut self, restart: Restart) {
        let now = self.arrival_ns;
        self.fault_stats.restarts += 1;
        self.fault_stats.router_held += self.router_queue.len() as u64;
        if restart.fate == Fate::Shared {
            for flight in std::mem::take(&mut self.flights) {
                self.fail_flight(&flight, restart.client);
            }
            for gang in std::mem::take(&mut self.gangs) {
                self.fail_gang(gang);
            }
            for part in std::mem::take(&mut self.orphans) {
                self.fail_part(&part);
            }
            self.unqueue_engines();
        } else {
            self.fault_stats.held_through += (self.flights.len()
                + self.orphans.len()
                + self.gangs.iter().map(|g| g.parts.len()).sum::<usize>())
                as u64;
        }
        self.lose_owned_soft();
        match restart.estimators {
            Estimators::Kept => {}
            Estimators::Lost => self.reset_estimators(),
            Estimators::Snapshot => {
                if !self.restore_snapshot() {
                    self.reset_estimators();
                }
            }
        }
        if restart.fate == Fate::Held && restart.ledger != Rebuild::Now {
            self.blind_ledger(restart);
        }
        let blank = || vec![HashSet::new(); self.domains.len()];
        self.known = match restart.subscriber {
            Subscriber::Warm => None,
            Subscriber::Cold => Some(Known {
                until_ns: u64::MAX,
                sets: blank(),
            }),
            Subscriber::Snapshot { after_ns } => Some(Known {
                until_ns: now + restart.outage_ns + after_ns,
                sets: blank(),
            }),
        };
        self.down_until = now + restart.outage_ns;
    }

    fn fail_flight(&mut self, flight: &Flight, client: Client) {
        let (decoded, progress) = self.abort_flight(flight);
        self.fault_stats.streams_aborted += 1;
        let resubmitted = match client {
            Client::Continue => {
                self.fault_stats.kept_tokens += decoded;
                self.continuation(&flight.req, decoded)
            }
            Client::Restart => {
                let span = flight.handles.end_ns - flight.handles.start_ns;
                self.fault_stats.lost_decode_ns += (span as f64 * progress) as u64;
                flight.req.clone()
            }
        };
        self.aborted_requests.insert(flight.id);
        self.router_queue.push(Waiting {
            id: flight.id,
            seq: flight.seq,
            req: resubmitted,
            arrival_ns: flight.arrival_ns,
            queued_ns: self.arrival_ns,
            pre_ns: 0,
            decide_ns: 0,
            requeued: true,
            not_before: 0,
        });
    }

    fn fail_part(&mut self, part: &Part) {
        let (_, progress) = self.abort_held(part.node, part.handles, &part.req);
        let span = part.handles.end_ns - part.handles.start_ns;
        self.fault_stats.streams_aborted += 1;
        self.fault_stats.lost_decode_ns += (span as f64 * progress) as u64;
    }

    fn fail_gang(&mut self, gang: GangFlight) {
        self.fault_stats.gangs_aborted += 1;
        for part in &gang.parts {
            self.fail_part(part);
        }
        self.retry_gangs.push(GangRetry {
            id: gang.id,
            seq: gang.seq,
            arrival_ns: gang.arrival_ns,
            req: gang.req,
        });
    }

    fn unqueue_engines(&mut self) {
        for d in 0..self.engine_queues.len() {
            self.unqueue_engine(d);
        }
    }

    fn unqueue_engine(&mut self, d: usize) {
        for mut waiting in std::mem::take(&mut self.engine_queues[d]) {
            self.fault_stats.moved_from_engine += 1;
            waiting.pre_ns = 0;
            waiting.decide_ns = 0;
            waiting.queued_ns = self.arrival_ns;
            self.router_queue.push(waiting);
        }
    }

    fn lose_owned_soft(&mut self) {
        self.upstream.clear();
        self.landing.clear();
        self.prefill_ready.clear();
        self.buckets.clear();
        self.tenant_flight.clear();
        self.cancelled.clear();
    }

    fn reset_estimators(&mut self) {
        self.fault_stats.estimator_resets += 1;
        self.lengths = [Lengths::default(), Lengths::default()];
        self.observed = [(0, 0); 2];
        self.root_lengths.clear();
        self.root_observed.clear();
        self.learner = FlowLearner {
            gate: self.learner.gate,
            stats: self.learner.stats,
            ..FlowLearner::default()
        };
        self.attained.clear();
        self.completions.clear();
        self.fleet_view = FleetView::default();
        if let Some(planner) = self.planner.as_mut() {
            planner.accrued_ns = 0.0;
        }
    }

    fn fault_tick(&mut self, now: u64) {
        self.declare_lost_nodes(now);
        if let Some(saved) = self.degraded.filter(|d| d.until_ns <= now) {
            self.placement = saved.placement;
            self.flow_aware = saved.flow_aware;
            self.state_transfer = saved.state_transfer;
            self.degraded = None;
        }
        if self.known.as_ref().is_some_and(|k| k.until_ns <= now) {
            self.known = None;
        }
        if let Some(every) = self.snapshot_every_ns
            && now >= self.next_snapshot_ns
            && !self.is_down()
        {
            self.take_snapshot();
            self.next_snapshot_ns = now + every;
        }
        let hidden = self.flights.iter().any(|f| f.pre_crash);
        if let Some(shadow) = self.shadow.as_mut() {
            if shadow.until_ns <= now || !hidden {
                self.shadow = None;
                for flight in &mut self.flights {
                    flight.pre_crash = false;
                }
            } else {
                for ledger in &mut shadow.reserved {
                    ledger.release(now);
                }
                for ends in &mut shadow.load {
                    while ends.peek().is_some_and(|&Reverse(e)| e <= now) {
                        ends.pop();
                    }
                }
            }
        }
    }

    fn known_filter(&self, d: usize, id: &BlobId, kind: BlobKind) -> bool {
        let Some(known) = &self.known else {
            return true;
        };
        if kind != BlobKind::KvBlock {
            return true;
        }
        let seen = known.sets[d].contains(id);
        if !seen && self.domains[d].is_hot(id, kind) {
            self.unknown_reads.set(self.unknown_reads.get() + 1);
        }
        seen
    }

    fn drain_lengths(&mut self) {
        if self.pending_lengths.is_empty() {
            return;
        }
        let now = self.arrival_ns;
        let (due, rest): (Vec<PendingLength>, Vec<PendingLength>) =
            std::mem::take(&mut self.pending_lengths)
                .into_iter()
                .partition(|p| p.end_ns <= now);
        self.pending_lengths = rest;
        for p in due {
            self.record_length(p.slo, p.tokens, p.root);
        }
    }

    fn note_length(&mut self, home: usize, req: &Request, after_ns: u64) {
        match self.observe {
            Observe::Dispatch => self.record_length(req.slo.idx(), req.tokens, req.root),
            Observe::Completion => self.pending_lengths.push(PendingLength {
                end_ns: self.arrival_ns + after_ns,
                node: home,
                slo: req.slo.idx(),
                tokens: req.tokens,
                root: req.root,
            }),
        }
    }

    fn settle_gang(
        &mut self,
        req: &Request,
        cost: Cost,
        seq: u64,
        arrival_ns: u64,
        id: Option<usize>,
    ) -> Submitted {
        let parts = std::mem::take(&mut self.gang_parts);
        if self.armed && cost.pending {
            self.orphans.extend(parts);
            return Submitted::Closed(cost);
        }
        if !self.armed || parts.is_empty() {
            return Submitted::Closed(cost);
        }
        let id = id.unwrap_or_else(|| self.new_request());
        let end_ns = parts
            .iter()
            .map(|p| p.handles.end_ns)
            .max()
            .unwrap_or(arrival_ns);
        self.gangs.push(GangFlight {
            id,
            seq,
            arrival_ns,
            req: req.clone(),
            cost,
            end_ns,
            parts,
        });
        self.counted.flights_opened += 1;
        Submitted::Open(id)
    }

    fn serve_gang_retries(&mut self) {
        for retry in std::mem::take(&mut self.retry_gangs) {
            let Some(gang) = retry.req.gang.as_ref() else {
                continue;
            };
            if self.regions.is_some() || self.shards.is_some() {
                self.assign_shard(&retry.req);
                self.enter_region(&retry.req);
            }
            let mut cost = self.serve_gang(&retry.req, gang);
            cost.queue_ns += self.arrival_ns.saturating_sub(retry.arrival_ns);
            if let Submitted::Closed(done) = self.settle_gang(
                &retry.req,
                cost,
                retry.seq,
                retry.arrival_ns,
                Some(retry.id),
            ) {
                self.closed.push((retry.id, done));
            }
        }
    }

    pub fn set_count_events(&mut self, on: bool) {
        self.count_events = on;
        if on && self.observer.is_none() {
            self.record_kv_events(true);
        }
    }

    #[must_use]
    pub fn writes(&self) -> Writes {
        let (commits, releases) = self
            .reserved
            .iter()
            .map(Reservations::writes)
            .fold((0, 0), |(c, r), (dc, dr)| (c + dc, r + dr));
        let owned = [BlobKind::Snapshot, BlobKind::ServiceHeap];
        let (mut admissions, mut evictions, mut weight_loads) = (0, 0, 0);
        for h in &self.domains {
            let evicted = h.evicted();
            admissions += owned.iter().map(|k| h.misses[k.idx()]).sum::<u64>();
            evictions += owned.iter().map(|k| evicted[k.idx()]).sum::<u64>();
            weight_loads += h.misses[BlobKind::WeightShard.idx()];
        }
        Writes {
            decisions: self.decisions,
            dispatches: self.dispatches,
            reservations_committed: commits,
            reservations_released: releases,
            flights_opened: self.counted.flights_opened,
            flights_closed: self.counted.flights_closed,
            queue_enqueued: self.queue_waits.queued,
            queue_served: self.counted.router_served,
            engine_enqueued: self.engine_waits.queued,
            engine_started: self.counted.engine_started,
            cancels: self.cancel_stats.cancels,
            refusals: self.refused_by_router.iter().sum(),
            flow_graph: self.counted.flow_graph,
            fanouts_staged: self.fanouts_admitted + self.fanouts_refused,
            host_admissions: admissions,
            host_evictions: evictions,
            weight_loads,
            length_observations: self.counted.length_observations,
            snapshots: self.counted.snapshots,
            snapshot_bytes: self.counted.snapshot_bytes,
            planner_moves: self
                .fleet
                .as_ref()
                .map_or(0, |f| f.stats.moves.len() as u64),
            kv_events: self.counted.kv_events,
        }
    }

    fn flow_put(&mut self, task: u64, sources: Vec<(usize, u64)>) {
        self.upstream.insert(task, sources);
        self.counted.flow_graph += 1;
    }

    fn flow_take(&mut self, task: u64) -> Option<Vec<(usize, u64)>> {
        let taken = self.upstream.remove(&task);
        self.counted.flow_graph += u64::from(taken.is_some());
        taken
    }

    fn drain_event_counts(&mut self) {
        if !self.count_events || self.observer.is_some() || self.instruments.reuse.is_some() {
            return;
        }
        for d in 0..self.domains.len() {
            let events = self.domains[d].take_kv_events();
            self.counted.count_events(&events);
        }
    }

    pub fn set_hint_grade(&mut self, grade: HintGrade, gate: f64) {
        self.hint_grade = grade;
        self.learner.gate = gate;
    }

    #[must_use]
    pub fn learn_stats(&self) -> LearnStats {
        self.learner.stats
    }

    pub fn set_claim_key(&mut self, key: ClaimKey) {
        self.claim_key = key;
    }

    pub fn set_tool_slots(&mut self, slots: Option<usize>) {
        self.tool_slots = slots;
        for free in &mut self.slot_free {
            free.clear();
            free.resize(slots.unwrap_or(0), 0);
        }
    }

    fn tool_wait_ns(&self, d: usize) -> u64 {
        if self.tool_slots.is_none() {
            return 0;
        }
        let free = self.slot_free[d].iter().copied().min().unwrap_or(0);
        free.saturating_sub(self.arrival_ns)
    }

    #[must_use]
    pub fn tool_utilisation(&self) -> f64 {
        let total: usize = self.slot_free.iter().map(Vec::len).sum();
        if self.tool_slots.is_none() || total == 0 {
            return 0.0;
        }
        let busy = self
            .slot_free
            .iter()
            .flatten()
            .filter(|&&f| f > self.arrival_ns)
            .count();
        busy as f64 / total as f64
    }

    #[must_use]
    pub fn last_tool_slot(&self) -> Option<ToolSlot> {
        self.last_slot
    }

    pub fn release_tool_slot(&mut self, slot: ToolSlot, at_ns: u64) {
        let now = self.arrival_ns;
        if let Some(cell) = self.slot_free[slot.node].get_mut(slot.slot)
            && *cell == slot.end_ns
        {
            *cell = at_ns.max(now).max(slot.start_ns).min(slot.end_ns);
        }
    }

    pub fn warm_cell(
        &mut self,
        task: Option<u64>,
        fallback: usize,
        cell: (BlobId, BlobMeta),
        pattern: Pattern,
    ) -> u64 {
        if self
            .active
            .iter()
            .any(|&d| self.domains[d].is_hot(&cell.0, cell.1.kind))
        {
            return 0;
        }
        let anchor = task
            .and_then(|t| self.upstream.get(&t))
            .and_then(|sources| sources.first())
            .map(|&(d, _)| d);
        let d = anchor.unwrap_or(fallback);
        let d = if self.lost[d].is_some() {
            self.active
                .iter()
                .copied()
                .find(|&a| self.lost[a].is_none())
                .unwrap_or(d)
        } else {
            d
        };
        self.domains[d].set_pattern(pattern);
        let c = self.domains[d].access(&[cell]);
        c.recompute_ns + c.transfer_ns
    }

    pub fn drop_kv(&mut self, ids: &[BlobId]) -> usize {
        self.domains
            .iter_mut()
            .map(|h| h.kv_drop_unpinned(ids))
            .sum()
    }

    pub fn suspend_cell(&mut self, cell: (BlobId, BlobMeta)) -> bool {
        let mut any = false;
        for h in &mut self.domains {
            any |= h.suspend_cell(cell.0, cell.1.kind);
        }
        any
    }

    fn copy_out(&mut self, id: BlobId, bytes: u64) {
        if self.copy_durable && self.durable_copies.insert(id, bytes).is_none() {
            self.fault_stats.durable_copied_bytes += bytes;
        }
    }

    pub fn mark_durable(&mut self, id: BlobId, bytes: u64) {
        self.copy_out(id, bytes);
        for h in &mut self.domains {
            h.mark_durable(id);
        }
    }

    pub fn forget_flow(&mut self, task: u64) {
        self.flow_take(task);
        self.landing.remove(&task);
        self.prefill_ready.remove(&task);
    }

    pub fn release_durable(&mut self, id: &BlobId) {
        self.durable_copies.remove(id);
        for h in &mut self.domains {
            h.release_durable(id);
        }
    }

    pub fn evict_first_cell(&mut self, cell: (BlobId, BlobMeta)) {
        for h in &mut self.domains {
            h.evict_first_cell(cell.0, cell.1.kind);
        }
    }

    #[must_use]
    pub fn cell_state(&self, cell: &(BlobId, BlobMeta)) -> CellState {
        let states = self
            .domains
            .iter()
            .map(|h| h.cell_state(&cell.0, cell.1.kind));
        let mut best = CellState::Gone;
        for state in states {
            match state {
                CellState::Hot => return CellState::Hot,
                CellState::Cold => best = CellState::Cold,
                CellState::Gone => {}
            }
        }
        best
    }

    #[must_use]
    pub fn host_hold_ns_per_s(&self, bytes: u64) -> f64 {
        let secs = self.arrival_ns as f64 / 1e9;
        if secs <= 0.0 {
            return 0.0;
        }
        let nodes = self.domains.len().max(1) as f64;
        let total: f64 = self
            .domains
            .iter()
            .map(|h| {
                let (evicted, used) = h.ddr_pressure();
                h.ddr_price() * bytes as f64 * (evicted as f64 / secs / used.max(1) as f64)
            })
            .sum();
        total / nodes
    }

    #[must_use]
    pub fn kv_hold_ns_per_s(&self, bytes: u64) -> f64 {
        let secs = self.arrival_ns as f64 / 1e9;
        if secs <= 0.0 {
            return 0.0;
        }
        let mut sum = 0.0;
        let mut n = 0.0;
        for h in &self.domains {
            if let (Some((evictions, used)), Some(price)) = (h.kv_pressure(), h.kv_tail_price()) {
                let turnover = evictions as f64 * crate::work::KV_BLOCK_BYTES as f64
                    / secs
                    / used.max(1) as f64;
                sum += price * bytes as f64 * turnover;
                n += 1.0;
            }
        }
        if n == 0.0 { 0.0 } else { sum / n }
    }

    #[must_use]
    pub fn chain_resident(&self, chain: &[(BlobId, BlobMeta)]) -> (usize, usize) {
        let mut lead = 0;
        let mut any = 0;
        for &d in &self.active {
            let h = &self.domains[d];
            lead = lead.max(
                chain
                    .iter()
                    .take_while(|(id, m)| h.is_hot(id, m.kind))
                    .count(),
            );
            any = any.max(chain.iter().filter(|(id, m)| h.is_hot(id, m.kind)).count());
        }
        (lead, any)
    }

    #[must_use]
    pub fn resident_union(&self, groups: &[Vec<BlobId>]) -> usize {
        self.active
            .iter()
            .map(|&d| {
                groups
                    .iter()
                    .filter(|g| {
                        g.iter()
                            .any(|id| self.domains[d].is_hot(id, BlobKind::KvBlock))
                    })
                    .count()
            })
            .max()
            .unwrap_or(0)
    }

    #[must_use]
    pub fn leases(&self) -> (u64, u64, u64) {
        self.domains
            .iter()
            .fold((0, 0, 0), |(taken, breaks, peak), h| {
                (
                    taken + h.leases_taken,
                    breaks + h.lease_breaks,
                    peak.max(h.leased_peak()),
                )
            })
    }

    #[must_use]
    pub fn durable_lost(&self) -> u64 {
        self.domains.iter().map(|h| h.durable_lost).sum()
    }

    #[must_use]
    pub fn memory_by_pattern(&self) -> [(u64, u64); Pattern::N] {
        let mut out = [(0, 0); Pattern::N];
        for h in &self.domains {
            for (o, c) in out.iter_mut().zip(h.pattern_coupling()) {
                o.0 += c.0;
                o.1 += c.1;
            }
        }
        out
    }

    pub fn set_engine_wait(&mut self, wait: EngineWait) {
        self.engine_wait = wait;
    }

    pub fn set_queue(&mut self, queue: Queue) {
        self.queue = queue;
    }

    pub fn set_cancel_triggers(&mut self, triggers: Triggers) {
        self.triggers = triggers;
    }

    pub fn set_departures(&mut self, departures: Option<Departures>) {
        self.departures = departures;
    }

    pub fn set_track_stream(&mut self, on: bool) {
        self.track_stream = on;
    }

    #[must_use]
    pub fn drain_departed(&mut self) -> Vec<usize> {
        std::mem::take(&mut self.departed)
    }

    fn tracks_flights(&self) -> bool {
        self.cancel != CancelMode::Off
            || self.departures.is_some()
            || self.track_stream
            || self.armed
    }

    pub fn set_cancel(&mut self, cancel: CancelMode, victim: Victim) {
        self.cancel = cancel;
        self.victim = victim;
    }

    #[must_use]
    pub fn cancelled_requests(&self) -> usize {
        self.aborted_requests.len()
    }

    pub fn set_claim(&mut self, claims: Claim) {
        self.claims = claims;
    }

    pub fn set_probe_engine(&mut self, on: bool) {
        self.probe_engine = on;
    }

    #[must_use]
    pub fn drain_closed(&mut self) -> Vec<(usize, Cost)> {
        std::mem::take(&mut self.closed)
    }

    pub fn finish(&mut self) {
        self.finishing = true;
        let mut steps = 0u64;
        while self.engine_queues.iter().any(|q| !q.is_empty())
            || !self.router_queue.is_empty()
            || !self.flights.is_empty()
            || !self.gangs.is_empty()
            || !self.retry_gangs.is_empty()
            || !self.limbo.is_empty()
        {
            steps += 1;
            assert!(
                steps <= FINISH_STEPS,
                "a sequence waits on pins that never release"
            );
            self.arrive(false);
        }
        self.finishing = false;
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
        if req.client_facing()
            && let Some(shares) = self.tenant_shares.as_mut()
            && !shares.admit(
                tenant,
                self.regions.as_ref().map_or(0, |r| r.region_of(home)),
                req.tokens,
                now,
            )
        {
            return false;
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
            self.reserved[d].clear();
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
        let Some((capacity, pinned)) = self.telemetry(d).partition() else {
            return true;
        };
        if let Claim::Gate(theta) = self.claims
            && !staged
            && req.chain.iter().any(|(_, m)| m.kind == BlobKind::KvBlock)
        {
            return (pinned as f64) < theta * capacity as f64;
        }
        let staged = staged.then(|| (&self.staged[d], self.staged_kv[d]));
        let (blocks, extra) = self.claim(req);
        let ledger = self
            .shadow
            .as_ref()
            .map_or(&self.reserved[d], |s| &s.reserved[d]);
        ledger.admits_claim(capacity, req, (&blocks, extra), staged)
    }

    fn router_ever_admits(&self, d: usize, req: &Request) -> bool {
        let Some((capacity, _)) = self.telemetry(d).partition() else {
            return true;
        };
        if matches!(self.claims, Claim::Gate(_)) {
            return true;
        }
        let (blocks, extra) = self.claim(req);
        Reservations::default().admits_claim(capacity, req, (&blocks, extra), None)
    }

    fn claim_tokens(&self, req: &Request) -> Option<u64> {
        let class = |slo: Slo| {
            if self.claim_key == ClaimKey::Root
                && let Some(own) = self.root_lengths.get(&req.root)
            {
                own
            } else {
                &self.lengths[slo.idx()]
            }
        };
        match self.claims {
            Claim::Static | Claim::Gate(_) => None,
            Claim::Quantile { q, pooled: false } => class(req.slo).quantile(q),
            Claim::Quantile { q, pooled: true } => {
                self.lengths[0].pooled_quantile(&self.lengths[1], q)
            }
            Claim::Tiered { q } => match req.slo {
                Slo::Interactive => class(Slo::Interactive).quantile(q),
                Slo::Throughput => class(Slo::Throughput).mean(),
            },
        }
    }

    #[must_use]
    pub fn claimed_tokens(&self, req: &Request) -> Option<u64> {
        match self.claims {
            Claim::Static | Claim::Gate(_) => None,
            _ => Some(self.claim_tokens(req).unwrap_or(req.max_tokens)),
        }
    }

    fn claim(&self, req: &Request) -> (Vec<(BlobId, BlobMeta)>, u64) {
        if matches!(self.claims, Claim::Static | Claim::Gate(_)) {
            return self.reserve.claim(req, self.tokens_per_block);
        }
        let tokens = self.claim_tokens(req);
        let (blocks, _) = Reserve::Prompt.claim(req, self.tokens_per_block);
        let output = if req.tokens == 0 {
            0
        } else {
            tokens
                .unwrap_or(req.max_tokens)
                .div_ceil(self.tokens_per_block.max(1))
                * crate::work::KV_BLOCK_BYTES
        };
        (blocks, output)
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
        if self.claim_key == ClaimKey::Root
            && let Some(&(sum, n)) = self.root_observed.get(&req.root)
            && n > 0
        {
            return sum / n;
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
        if let Some(known) = self.known.as_mut() {
            for (id, meta) in req.chain.iter().chain(&req.produces) {
                if meta.kind == BlobKind::KvBlock {
                    known.sets[d].insert(*id);
                }
            }
        }
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

    fn observe_landing(&mut self, req: &Request, home: usize) -> u64 {
        let Some(task) = req.completes else {
            return 0;
        };
        let Some(target) = self.landing.remove(&task) else {
            return 0;
        };
        self.instruments.prefill.landings += 1;
        self.instruments.prefill.landed += u64::from(target == home);
        let ready = self.prefill_ready.remove(&task);
        if target == home {
            ready.map_or(0, |at| at.saturating_sub(self.arrival_ns))
        } else {
            0
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
        if self.prefill_ahead {
            match self.hint_grade {
                HintGrade::Declared => {
                    if let Some(hint) = &req.hint {
                        self.prefill_for(hint, home);
                    }
                }
                HintGrade::Template => {
                    if let Some(hint) = &req.hint {
                        self.prefill_for(&hint.templated(), home);
                    }
                }
                HintGrade::Learned => self.prefill_learned(req, home),
            }
        }
        let flow_prompt = self.origins.as_ref().is_some_and(|o| {
            req.chain.first().and_then(|(id, _)| o.of(id)) == Some(Origin::FlowPrompt)
        });
        if flow_prompt && req.completes.is_some() {
            self.instruments.flow.record(cost.total_ns());
        }
    }

    fn prefill_learned(&mut self, req: &Request, home: usize) {
        let kv_first = |chain: &[(BlobId, BlobMeta)]| {
            chain
                .first()
                .is_some_and(|(_, m)| m.kind == BlobKind::KvBlock)
        };
        let function = req
            .chain
            .first()
            .filter(|(_, m)| m.kind == BlobKind::Snapshot && req.completes.is_none())
            .map(|(id, _)| *id);
        if let Some(f) = function {
            let flows = req.hint.as_ref().is_some_and(|h| kv_first(&h.downstream));
            let entry = self.learner.seen.entry(f).or_insert((0, 0));
            entry.0 += 1;
            entry.1 += u64::from(flows);
            let probability = entry.1 as f64 / entry.0 as f64;
            if let Some(h) = &req.hint {
                self.learner.task_fn.insert(h.task, (f, h.template_len));
            }
            let template = self.learner.template.get(&f).cloned();
            self.learner.stats.calls += 1;
            if flows {
                self.learner.stats.flows += 1;
                self.learner.stats.known += u64::from(template.is_some());
            }
            if let Some(template) = template
                && probability >= self.learner.gate
            {
                let task = req
                    .hint
                    .as_ref()
                    .map_or(u64::MAX - self.arrival_ns, |h| h.task);
                let predicted = FlowHint {
                    task,
                    template_len: template.len(),
                    downstream: template,
                    probability,
                    lead_ops: crate::work::FLOW_LEAD_OPS,
                    payload_bytes: 0,
                };
                self.learner.stats.fired += 1;
                self.prefill_for(&predicted, home);
                if req.hint.is_none() {
                    self.landing.remove(&task);
                    self.prefill_ready.remove(&task);
                }
            }
        }
        if let Some(task) = req.completes
            && kv_first(&req.chain)
            && let Some((f, n)) = self.learner.task_fn.remove(&task)
        {
            let template: Vec<(BlobId, BlobMeta)> = req.chain.iter().take(n).copied().collect();
            self.learner.template.entry(f).or_insert(template);
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
            root: 0,
            authority: Authority::ReadOnly,
            tool: false,
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
        self.prefill_ready.insert(hint.task, self.arrival_ns + work);
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

    fn hold_claim(
        &mut self,
        home: usize,
        req: &Request,
        until: Option<u64>,
    ) -> (Option<u64>, Option<u64>) {
        match until {
            Some(end) if self.domains[home].engine_cache() => {
                let (blocks, extra) = self.claim(req);
                if self.shadow.is_some() {
                    self.note_blind_admission(home, req, &blocks, extra);
                }
                let seq = self.reserved[home].commit(&blocks, extra, end);
                if let Some(shadow) = self.shadow.as_mut() {
                    let blind = shadow.reserved[home].commit(&blocks, extra, end);
                    shadow.seqs.insert((home, seq), blind);
                    shadow.load[home].push(Reverse(end));
                }
                (Some(seq), self.observe_pin(home, req, end))
            }
            _ => (None, None),
        }
    }

    fn observe_pin(&mut self, d: usize, req: &Request, until: u64) -> Option<u64> {
        let ids: Vec<BlobId> = req
            .chain
            .iter()
            .chain(&req.produces)
            .filter(|(_, m)| m.kind == BlobKind::KvBlock)
            .map(|(id, _)| *id)
            .collect();
        self.observer.as_mut().map(|o| o.pin(d, ids, until))
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
        if self.count_events {
            self.counted.count_events(&events);
        }
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
            || self.known_filter(d, id, kind)
                && match self.control {
                    Control::Gossip { .. } if !self.sees_belief(d, kind) => {
                        self.view[d].contains(id)
                    }
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
        self.known_filter(p, id, kind)
            && match self.control {
                Control::Gossip { .. } if !self.sees_belief(p, kind) => self.view[p].contains(id),
                Control::Gossip { .. } | Control::Unified | Control::Query => {
                    let seen = self.telemetry(p).held(id, kind);
                    debug_assert!(
                        self.exact_belief_agrees(p, kind, || seen
                            == self.domains[p].holds(id, kind)),
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
        self.decode_pool().len()
    }

    fn decode_pool(&self) -> Vec<usize> {
        let all = self.decode_pool_all();
        let scoped = self.scoped(&all);
        if scoped.is_empty() && !self.confined {
            all
        } else {
            scoped
        }
    }

    fn serving_pool(&self) -> Vec<usize> {
        let scoped = self.scoped(&self.active);
        if scoped.is_empty() && !self.confined {
            self.scoped_to(&self.active, None)
        } else {
            scoped
        }
    }

    fn decode_pool_all(&self) -> Vec<usize> {
        let pool: Vec<usize> = self
            .active
            .iter()
            .copied()
            .filter(|&d| self.decodes_now(d))
            .collect();
        if !pool.is_empty() {
            return pool;
        }
        let up: Vec<usize> = self
            .active
            .iter()
            .copied()
            .filter(|&d| self.available(d))
            .collect();
        if up.is_empty() {
            self.active.clone()
        } else {
            up
        }
    }

    fn decodes_now(&self, d: usize) -> bool {
        self.domains[d].can_decode() && self.decode_out[d] <= self.arrival_ns && self.available(d)
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
            View::Belief => self.telemetry(d).with_load(
                self.shadow_load(d)
                    .or_else(|| self.observed_by(d).and_then(|o| o.reported_load(d)))
                    .or_else(|| self.shard_view(d)),
            ),
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
        } else if req.tool {
            self.tool_wait_ns(d) as f64
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
            reach: self.reach_term(d),
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
            let by = &mut self.locality_by[self.deciding.idx()];
            by.1 += 1;
            by.0 += u64::from(silo_pick != top);
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
        self.scope = None;
        self.client_region = None;
        self.confined = false;
        let now = self.arrival_ns;
        self.fault_tick(now);
        self.refresh_summary(now);
        self.refresh_shards(now);
        self.recompute_table(now);
        self.apply_budgets(now);
        self.drain_lengths();
        self.drain_event_counts();
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
            .filter(|&d| self.domains[d].can_decode() && self.available(d))
            .collect();
        let n = engines.len().max(1) as u64;
        if !self.finishing {
            for d in engines {
                for (sum, bytes) in self.kv_sum.iter_mut().zip(self.domains[d].kv_bytes()) {
                    *sum += bytes / n;
                }
            }
            self.kv_samples += 1;
        }
        self.depart_flights();
        self.close_flights();
        self.sample_stream();
        self.serve_engine_queues();
        self.serve_router_queue();
    }

    fn unplaced() -> Cost {
        Cost {
            pending: true,
            ..Cost::default()
        }
    }

    pub fn submit(&mut self, req: &Request) -> Submitted {
        self.submit_with(req, req.concurrent)
    }

    pub fn submit_at(&mut self, at_ns: u64, req: &Request) -> Submitted {
        self.arrival_ns = self.arrival_ns.max(at_ns);
        self.submit_with(req, true)
    }

    pub fn advance_to(&mut self, at_ns: u64) {
        self.arrival_ns = self.arrival_ns.max(at_ns);
        self.arrive(true);
    }

    #[must_use]
    pub fn now_ns(&self) -> u64 {
        self.arrival_ns
    }

    fn submit_with(&mut self, req: &Request, concurrent: bool) -> Submitted {
        self.arrive(concurrent);
        self.last_slot = None;
        self.assign_shard(req);
        self.enter_region(req);
        let seq = self.submitted;
        self.submitted += 1;
        if let Some(task) = req.completes
            && self.cancelled.remove(&task)
        {
            return Submitted::Closed(Cost {
                pending: true,
                ..Cost::default()
            });
        }
        if let Some(gang) = &req.gang {
            let cost = self.serve_gang(req, gang);
            return self.settle_gang(req, cost, seq, self.arrival_ns, None);
        }
        let decode_needed = Self::needs_decode(req);
        self.record_demand(model_of(&req.requires), req.tokens);
        let candidates = if decode_needed {
            self.eligible(self.decode_pool(), req)
        } else {
            self.serving_pool()
        };
        if candidates.is_empty() {
            if let Some(fleet) = self.fleet.as_mut() {
                fleet.stats.unplaced += 1;
            }
            return Submitted::Closed(Self::unplaced());
        }
        let fresh = Arrival {
            id: None,
            seq,
            at_ns: self.arrival_ns,
            abort_ns: 0,
        };
        if self.queue == Queue::Off || !decode_needed {
            return self.place(req, &candidates, fresh);
        }
        let room = self.admitting(&candidates, req);
        if self.router_queue.is_empty() && !room.is_empty() {
            return self.place(req, &room, fresh);
        }
        let id = self.new_request();
        self.router_enqueue(id, seq, req);
        self.serve_router_queue();
        Submitted::Open(id)
    }

    fn new_request(&mut self) -> usize {
        let id = self.next_request;
        self.next_request += 1;
        id
    }

    fn admitting(&self, candidates: &[usize], req: &Request) -> Vec<usize> {
        candidates
            .iter()
            .copied()
            .filter(|&d| self.router_admits(d, req, false))
            .collect()
    }

    fn router_enqueue(&mut self, id: usize, seq: u64, req: &Request) {
        self.queue_waits.queued += 1;
        self.router_queue.push(Waiting {
            id,
            seq,
            req: req.clone(),
            arrival_ns: self.arrival_ns,
            queued_ns: self.arrival_ns,
            pre_ns: 0,
            decide_ns: 0,
            requeued: false,
            not_before: 0,
        });
        self.queue_waits.max_depth = self.queue_waits.max_depth.max(self.router_queue.len());
    }

    fn queue_key(&self, w: &Waiting) -> (u64, u64, usize) {
        match self.queue {
            Queue::Off | Queue::Fifo => (0, w.arrival_ns, w.id),
            Queue::Slo => (w.req.slo.idx() as u64, w.arrival_ns, w.id),
            Queue::Plas => (
                self.attained.get(&w.req.program).copied().unwrap_or(0),
                w.arrival_ns,
                w.id,
            ),
        }
    }

    fn accrue_attained(&mut self) {
        while let Some(&Reverse((end, program, exec))) = self.completions.peek() {
            if end > self.arrival_ns {
                break;
            }
            self.completions.pop();
            *self.attained.entry(program).or_insert(0) += exec;
        }
    }

    fn serve_router_queue(&mut self) {
        let entered = (self.scope, self.client_region, self.confined, self.shard);
        self.serve_router_queue_in_regions();
        (self.scope, self.client_region, self.confined, self.shard) = entered;
    }

    fn serve_router_queue_in_regions(&mut self) {
        if self.is_down() {
            return;
        }
        self.serve_gang_retries();
        self.accrue_attained();
        let now = self.arrival_ns;
        while let Some(head) = (0..self.router_queue.len())
            .filter(|&i| self.router_queue[i].not_before <= now)
            .min_by_key(|&i| self.queue_key(&self.router_queue[i]))
        {
            if self.regions.is_some() || self.shards.is_some() {
                let waiting = self.router_queue[head].req.clone();
                self.assign_shard(&waiting);
                self.enter_region(&waiting);
            }
            let req = &self.router_queue[head].req;
            let candidates = self.eligible(self.decode_pool(), req);
            let mut room = self.admitting(&candidates, req);
            let mut abort_ns = 0;
            let never = room.is_empty()
                && !candidates.is_empty()
                && candidates.iter().all(|&d| !self.router_ever_admits(d, req));
            if !candidates.is_empty() && room.is_empty() && !never {
                if self.cancel == CancelMode::Off {
                    return;
                }
                let blocked = self.router_queue[head].req.clone();
                let Some(node) = self.cancel_for(&blocked, &candidates) else {
                    return;
                };
                room = vec![node];
                abort_ns = self.engines[node].step_base_ns();
            }
            let waiting = self.router_queue.remove(head);
            self.counted.router_served += 1;
            if !waiting.requeued {
                let slo = waiting.req.slo.idx();
                self.queue_waits.waited[slo] += 1;
                self.queue_waits.waited_ns[slo] += self.arrival_ns - waiting.arrival_ns;
            }
            let id = waiting.id;
            let outcome = if candidates.is_empty() {
                if let Some(fleet) = self.fleet.as_mut() {
                    fleet.stats.unplaced += 1;
                }
                Submitted::Closed(Self::unplaced())
            } else if never {
                self.refused_by_router[waiting.req.kind_idx()] += 1;
                Submitted::Closed(Self::unplaced())
            } else {
                let arrival = Arrival {
                    id: Some(id),
                    seq: waiting.seq,
                    at_ns: waiting.arrival_ns,
                    abort_ns,
                };
                self.place(&waiting.req, &room, arrival)
            };
            if let Submitted::Closed(cost) = outcome {
                self.closed.push((id, cost));
            }
        }
    }

    fn place(&mut self, req: &Request, candidates: &[usize], arrival: Arrival) -> Submitted {
        self.deciding = req.pattern;
        let decode_needed = Self::needs_decode(req);
        let decide_ns = self.decide(req.chain.len(), candidates.len());
        self.decide_ns += decide_ns;
        if self.placement != Placement::Blind {
            self.sticky_unit = self.affinity_unit(&req.chain, candidates);
        }
        let flow: Vec<(usize, u64)> = req
            .completes
            .filter(|_| self.flow_aware)
            .and_then(|t| self.upstream.get(&t).cloned())
            .unwrap_or_default();
        let scored = self.placement == Placement::Scored;
        let affinity = self.topo.units[self.sticky_unit].home as usize;
        let (best, decided_by) = if scored {
            self.best_scored(&self.view_of(req), &flow, affinity, candidates)
        } else {
            (self.greedy_best(req, candidates), None)
        };
        let value = self.resident_value(best, req);

        let target = if scored {
            best
        } else {
            match flow.first() {
                Some(&(d, _))
                    if (!decode_needed || self.decodes_now(d))
                        && self.lost[d].is_none_or(|at| at > self.arrival_ns)
                        && (self.queue == Queue::Off || self.router_admits(d, req, false)) =>
                {
                    d
                }
                _ => self.policy_target(affinity, best, value, candidates),
            }
        };
        let home = self.topo.units[self.unit_in(target)].home as usize;
        self.last_home = home;

        let mut planned = None;
        if !self.meters_admit(home, req, &mut planned) {
            if let Some(t) = req.tenant {
                *self.tenant_refused.entry(t).or_insert(0) += 1;
            }
            return Submitted::Closed(Self::unplaced());
        }
        if let Some(declared) = self.lost[home].filter(|&at| at > self.arrival_ns) {
            return self.park_for_lease(req, arrival, declared);
        }
        if !self.node_accepts(home, req) {
            return self.refuse_at_node(req, arrival);
        }

        if self.placement == Placement::Aware
            && self.resident_value(target, req) > 0
            && self.truly_resident(target, req) == 0
        {
            self.stale_decisions += 1;
        }

        let pick = self
            .regret
            .then(|| self.oracle_pick(req, &flow, candidates, decode_needed, decide_ns, home));
        if self.instrument {
            self.instrument_decision(req, candidates, home);
        }

        if let Some(hint) = &req.hint {
            let recorded = self.tool_anchor.unwrap_or(home);
            self.flow_put(hint.task, vec![(recorded, hint.payload_bytes)]);
        }
        let sources = req
            .completes
            .and_then(|t| self.flow_take(t))
            .unwrap_or_default();
        let handoff = self.collect(home, &sources);
        let reached = self.reach(home, req, decode_needed);

        let landing_wait_ns = self.observe_landing(req, home);
        let pair = self.decide_pair(home, req, &mut planned);
        let hops = handoff + reached + arrival.abort_ns;
        let mut cost = match self.run_or_queue(home, req, pair, hops, decide_ns, arrival) {
            Ok(cost) => cost,
            Err(id) => return Submitted::Open(id),
        };
        cost.decide_ns = decide_ns;
        cost.transfer_ns += hops;
        cost.queue_ns += self.arrival_ns - arrival.at_ns + landing_wait_ns;
        self.after_dispatch(req, home, &cost);
        if let Some(pick) = pick {
            let class = BlobKind::ALL[req.kind_idx()];
            self.finish_regret(req, class, home, candidates, pick, &cost, decided_by);
        }
        self.settle_flight(home, req, cost, arrival)
    }

    pub fn serve_request(&mut self, req: &Request) -> Cost {
        match self.submit(req) {
            Submitted::Closed(cost) => cost,
            Submitted::Open(_) => panic!("a request that waits is submitted, not served"),
        }
    }

    fn run_or_queue(
        &mut self,
        home: usize,
        req: &Request,
        pair: Option<(usize, u64)>,
        pre_ns: u64,
        decide_ns: u64,
        arrival: Arrival,
    ) -> Result<Cost, usize> {
        if !self.engine_gated(home, req) {
            return Ok(self.run_paired(home, req, pair, self.resubmitted(arrival.id)));
        }
        if let Some(refused) = self.router_refusal(home, req) {
            return Ok(refused);
        }
        if !self.engine_must_wait(home, req) {
            return Ok(self.dispatch(home, req, pair, self.resubmitted(arrival.id)));
        }
        let id = arrival.id.unwrap_or_else(|| self.new_request());
        self.engine_enqueue(
            home,
            Waiting {
                id,
                seq: arrival.seq,
                req: req.clone(),
                arrival_ns: arrival.at_ns,
                queued_ns: self.arrival_ns,
                pre_ns,
                decide_ns,
                requeued: false,
                not_before: 0,
            },
        );
        Err(id)
    }

    fn engine_gated(&self, home: usize, req: &Request) -> bool {
        (self.engine_wait != EngineWait::Off || self.probe_engine)
            && self.hold_decodes
            && req.tokens > 0
            && self.domains[home].engine_cache()
    }

    fn sequence_blocks(req: &Request) -> Vec<(BlobId, BlobMeta)> {
        let mut blocks = req.chain.clone();
        blocks.extend_from_slice(&req.produces);
        blocks
    }

    fn engine_fits(&self, d: usize, req: &Request) -> bool {
        self.domains[d]
            .kv_fit(&Self::sequence_blocks(req))
            .is_none_or(crate::cache::KvFit::fits)
    }

    fn engine_must_wait(&mut self, home: usize, req: &Request) -> bool {
        let blocks = Self::sequence_blocks(req);
        let Some(fit) = self.domains[home].kv_fit(&blocks) else {
            return false;
        };
        if self.probe_engine && !fit.fits() {
            match self.domains[home].kv_wait_ns(&blocks, self.arrival_ns) {
                Some(ns) => self.engine_waits.probe_ns.push(ns),
                None => self.engine_waits.probe_unplaceable += 1,
            }
        }
        if fit.never() {
            self.engine_waits.unfittable += u64::from(self.engine_wait != EngineWait::Off);
            return false;
        }
        let behind_head = matches!(self.engine_wait, EngineWait::Fifo | EngineWait::Priority)
            && !self.engine_queues[home].is_empty();
        self.engine_wait != EngineWait::Off && (!fit.fits() || behind_head)
    }

    fn engine_enqueue(&mut self, home: usize, waiting: Waiting) {
        self.engine_waits.queued += 1;
        let position = match self.engine_wait {
            EngineWait::Priority => {
                let key = (waiting.req.slo.idx(), waiting.arrival_ns);
                self.engine_queues[home]
                    .iter()
                    .position(|w| (w.req.slo.idx(), w.arrival_ns) > key)
            }
            _ => None,
        };
        let queue = &mut self.engine_queues[home];
        match position {
            Some(at) => queue.insert(at, waiting),
            None => queue.push(waiting),
        }
        self.engine_waits.max_depth = self.engine_waits.max_depth.max(queue.len());
    }

    fn next_startable(&self, d: usize) -> Option<usize> {
        let queue = &self.engine_queues[d];
        match self.engine_wait {
            EngineWait::Off => None,
            EngineWait::FirstFit => queue.iter().position(|w| self.engine_fits(d, &w.req)),
            EngineWait::Fifo | EngineWait::Priority => queue
                .first()
                .filter(|w| self.engine_fits(d, &w.req))
                .map(|_| 0),
        }
    }

    fn serve_engine_queues(&mut self) {
        for d in 0..self.engine_queues.len() {
            loop {
                if let Some(i) = self.next_startable(d) {
                    let waiting = self.engine_queues[d].remove(i);
                    self.start_waiting(d, &waiting);
                } else if self.is_down() || !self.cancel_for_engine(d) {
                    break;
                }
            }
        }
    }

    fn settle_flight(
        &mut self,
        home: usize,
        req: &Request,
        cost: Cost,
        arrival: Arrival,
    ) -> Submitted {
        let handles = self.handles.take();
        let Some(handles) =
            handles.filter(|_| self.tracks_flights() && !cost.pending && req.tokens > 0)
        else {
            return Submitted::Closed(cost);
        };
        let id = arrival.id.unwrap_or_else(|| self.new_request());
        let resubmitted = self.aborted_requests.contains(&id);
        if resubmitted {
            self.cancel_stats.reprefill_ns += cost.recompute_ns;
        }
        let leaves_at = self.departures.and_then(|d| {
            (crate::rng::Rng::hashed_unit(arrival.seq) < d.share).then(|| {
                let span = (handles.end_ns - handles.start_ns) as f64;
                handles.start_ns
                    + (span * crate::rng::Rng::hashed_unit(arrival.seq ^ DEPARTURE_POINT_KEY))
                        as u64
            })
        });
        self.departure_stats.leaving += u64::from(leaves_at.is_some() && !resubmitted);
        self.flights.push(Flight {
            id,
            seq: arrival.seq,
            leaves_at,
            leaving: leaves_at.is_some(),
            node: home,
            handles,
            dispatched_ns: self.arrival_ns,
            arrival_ns: arrival.at_ns,
            req: req.clone(),
            cost,
            pre_crash: false,
        });
        self.counted.flights_opened += 1;
        Submitted::Open(id)
    }

    fn close_flights(&mut self) {
        let now = self.arrival_ns;
        let mut at = 0;
        while at < self.flights.len() {
            if self.flights[at].handles.end_ns > now {
                at += 1;
                continue;
            }
            let flight = self.flights.swap_remove(at);
            self.counted.flights_closed += 1;
            if self.queue == Queue::Plas && self.cancel != CancelMode::Off {
                *self.attained.entry(flight.req.program).or_insert(0) += flight.cost.exec_ns;
            }
            if flight.leaving {
                self.departed.push(flight.id);
            } else {
                self.closed.push((flight.id, flight.cost));
            }
        }
        let mut at = 0;
        while at < self.gangs.len() {
            if self.gangs[at].end_ns > now {
                at += 1;
                continue;
            }
            let gang = self.gangs.swap_remove(at);
            self.counted.flights_closed += 1;
            self.closed.push((gang.id, gang.cost));
        }
        self.orphans.retain(|p| p.handles.end_ns > now);
    }

    fn depart_flights(&mut self) {
        let Some(departures) = self.departures else {
            return;
        };
        let now = self.arrival_ns;
        let mut at = 0;
        while at < self.flights.len() {
            let flight = &self.flights[at];
            let due = flight.leaves_at.filter(|&gone| gone <= now);
            let Some(gone) = due.filter(|_| flight.handles.end_ns > now) else {
                at += 1;
                continue;
            };
            if departures.leak {
                self.departure_stats.leaked_ns += flight.handles.end_ns - gone;
                self.flights[at].leaves_at = None;
                at += 1;
                continue;
            }
            let flight = self.flights.swap_remove(at);
            self.counted.flights_closed += 1;
            self.departure_stats.aborted += 1;
            self.departure_stats.freed_ns += flight.handles.end_ns - now;
            self.abort_flight(&flight);
            self.departed.push(flight.id);
        }
    }

    fn sample_stream(&mut self) {
        if !self.track_stream || self.finishing {
            return;
        }
        let now = self.arrival_ns;
        let mut emitted = vec![0u64; self.domains.len()];
        for f in &self.flights {
            emitted[f.node] += (f.req.tokens as f64 * f.handles.progress(now)) as u64;
        }
        self.stream.samples += 1;
        for (d, tokens) in emitted.into_iter().enumerate() {
            self.stream.sum_tokens[d] += tokens;
            self.stream.peak_tokens[d] = self.stream.peak_tokens[d].max(tokens);
        }
    }

    fn attained_of(&self, program: u64) -> u64 {
        self.attained.get(&program).copied().unwrap_or(0)
    }

    fn is_victim_for(&self, head: &Request, flight: &Flight) -> bool {
        match self.victim {
            Victim::Attained => {
                self.attained_of(flight.req.program) > self.attained_of(head.program)
            }
            Victim::Recent | Victim::Remaining => {
                head.slo == Slo::Interactive && flight.req.slo == Slo::Throughput
            }
        }
    }

    fn victims_at(&self, node: usize, head: &Request) -> Vec<usize> {
        let now = self.arrival_ns;
        let mut victims: Vec<usize> = (0..self.flights.len())
            .filter(|&i| {
                let f = &self.flights[i];
                f.node == node
                    && f.handles.end_ns > now
                    && !f.pre_crash
                    && self.is_victim_for(head, f)
            })
            .collect();
        match self.victim {
            Victim::Recent => victims.sort_by_key(|&i| Reverse(self.flights[i].dispatched_ns)),
            Victim::Remaining => {
                victims.sort_by_key(|&i| Reverse(self.flights[i].handles.end_ns));
            }
            Victim::Attained => {
                victims.sort_by_key(|&i| Reverse(self.attained_of(self.flights[i].req.program)));
            }
        }
        victims
    }

    fn freed_by(&self, flight: &Flight) -> u64 {
        flight
            .handles
            .resv_seq
            .map_or(0, |seq| self.reserved[flight.node].exclusive_bytes(seq))
    }

    fn abort_flight(&mut self, flight: &Flight) -> (u64, f64) {
        self.abort_held(flight.node, flight.handles, &flight.req)
    }

    fn abort_held(&mut self, node: usize, handles: Handles, req: &Request) -> (u64, f64) {
        if let Some(seq) = handles.resv_seq {
            self.cancel_stats.freed_bytes += self.reserved[node].cancel(seq);
            if let Some(shadow) = self.shadow.as_mut() {
                if let Some(blind) = shadow.seqs.remove(&(node, seq)) {
                    shadow.reserved[node].cancel(blind);
                }
                let mut ends = std::mem::take(&mut shadow.load[node]).into_vec();
                if let Some(i) = ends.iter().position(|Reverse(e)| *e == handles.end_ns) {
                    ends.swap_remove(i);
                }
                shadow.load[node] = BinaryHeap::from(ends);
            }
        }
        if let Some(seq) = handles.cache_seq {
            self.domains[node].abort_seq(seq);
        }
        if let Some(seq) = handles.belief_hold
            && let Some(o) = self.observer.as_mut()
        {
            o.unpin(node, seq);
        }
        if let Some(i) = self
            .pending_lengths
            .iter()
            .position(|p| p.node == node && p.end_ns == handles.end_ns && p.tokens == req.tokens)
        {
            self.pending_lengths.remove(i);
        }
        self.engines[node].cancel_inflight(handles.end_ns, model_of(&req.requires));
        let progress = handles.progress(self.arrival_ns);
        let decoded = (req.tokens as f64 * progress) as u64;
        let written = self.written_blocks(req, decoded);
        let unwritten: Vec<BlobId> = req.produces[written..].iter().map(|(id, _)| *id).collect();
        self.domains[node].kv_drop_unpinned(&unwritten);
        (decoded, progress)
    }

    fn written_blocks(&self, req: &Request, decoded: u64) -> usize {
        usize::try_from(decoded / self.tokens_per_block.max(1))
            .unwrap_or(usize::MAX)
            .min(req.produces.len())
    }

    fn continuation(&self, req: &Request, decoded: u64) -> Request {
        let written = self.written_blocks(req, decoded);
        let mut chain = req.chain.clone();
        chain.extend_from_slice(&req.produces[..written]);
        let tokens = req.tokens.saturating_sub(decoded).max(1);
        Request {
            chain,
            produces: req.produces[written..].to_vec(),
            tokens,
            exec_ns: tokens * crate::work::DECODE_NS_PER_TOKEN,
            max_tokens: req.max_tokens.saturating_sub(decoded).max(1),
            hint: None,
            completes: None,
            ..req.clone()
        }
    }

    fn cancel_flight(&mut self, at: usize, by_engine: bool) {
        let flight = self.flights.swap_remove(at);
        self.cancel_stats.cancels += 1;
        self.cancel_stats.engine_cancels += u64::from(by_engine);
        let (decoded, progress) = self.abort_flight(&flight);
        let resubmitted = if self.cancel == CancelMode::Drop {
            self.cancel_stats.wasted_decode_ns += (flight.cost.exec_ns as f64 * progress) as u64;
            flight.req.clone()
        } else {
            self.continuation(&flight.req, decoded)
        };
        self.aborted_requests.insert(flight.id);
        self.router_queue.push(Waiting {
            id: flight.id,
            seq: flight.seq,
            req: resubmitted,
            arrival_ns: flight.arrival_ns,
            queued_ns: self.arrival_ns,
            pre_ns: 0,
            decide_ns: 0,
            requeued: true,
            not_before: 0,
        });
        self.queue_waits.max_depth = self.queue_waits.max_depth.max(self.router_queue.len());
    }

    fn cancel_for(&mut self, head: &Request, candidates: &[usize]) -> Option<usize> {
        if matches!(self.claims, Claim::Gate(_)) {
            return self.cancel_for_gate(head, candidates);
        }
        let claim = self.claim(head);
        let mut best: Option<(usize, Vec<usize>)> = None;
        for &node in candidates {
            let Some((capacity, _)) = self.telemetry(node).partition() else {
                continue;
            };
            let deficit = self.reserved[node].deficit_claim(capacity, head, (&claim.0, claim.1));
            let mut freed = 0;
            let mut chosen = Vec::new();
            for i in self.victims_at(node, head) {
                if freed >= deficit {
                    break;
                }
                freed += self.freed_by(&self.flights[i]);
                chosen.push(i);
            }
            if freed >= deficit && best.as_ref().is_none_or(|(_, c)| chosen.len() < c.len()) {
                best = Some((node, chosen));
            }
        }
        let (node, mut chosen) = best?;
        chosen.sort_unstable_by(|a, b| b.cmp(a));
        for i in chosen {
            self.cancel_flight(i, false);
        }
        Some(node)
    }

    fn cancel_for_gate(&mut self, head: &Request, candidates: &[usize]) -> Option<usize> {
        let node = candidates
            .iter()
            .copied()
            .map(|d| (d, self.victims_at(d, head).len()))
            .filter(|&(_, n)| n > 0)
            .max_by_key(|&(_, n)| n)?
            .0;
        loop {
            if self.router_admits(node, head, false) {
                return Some(node);
            }
            let at = *self.victims_at(node, head).first()?;
            self.cancel_flight(at, false);
        }
    }

    fn cancel_for_engine(&mut self, node: usize) -> bool {
        if self.cancel == CancelMode::Off
            || self.engine_wait == EngineWait::Off
            || self.triggers == Triggers::Router
        {
            return false;
        }
        let Some(head) = self.engine_queues[node].first().map(|w| w.req.clone()) else {
            return false;
        };
        let Some(&at) = self.victims_at(node, &head).first() else {
            return false;
        };
        self.cancel_flight(at, true);
        true
    }

    fn start_waiting(&mut self, home: usize, waiting: &Waiting) {
        self.counted.engine_started += 1;
        let waited = self.arrival_ns - waiting.arrival_ns;
        let slo = waiting.req.slo.idx();
        self.engine_waits.waited[slo] += 1;
        self.engine_waits.waited_ns[slo] += self.arrival_ns - waiting.queued_ns;
        if self.regions.is_some() || self.shards.is_some() {
            self.assign_shard(&waiting.req);
            self.client_region = self.regions.as_ref().and(
                waiting
                    .req
                    .client_facing()
                    .then_some(usize::from(waiting.req.region)),
            );
        }
        let mut cost = self.dispatch(home, &waiting.req, None, self.resubmitted(Some(waiting.id)));
        cost.decide_ns = waiting.decide_ns;
        cost.transfer_ns += waiting.pre_ns;
        cost.queue_ns += waited;
        self.after_dispatch(&waiting.req, home, &cost);
        let arrival = Arrival {
            id: Some(waiting.id),
            seq: waiting.seq,
            at_ns: waiting.arrival_ns,
            abort_ns: 0,
        };
        if let Submitted::Closed(cost) = self.settle_flight(home, &waiting.req, cost, arrival) {
            self.closed.push((waiting.id, cost));
        }
    }

    fn reach(&mut self, home: usize, req: &Request, decode_needed: bool) -> u64 {
        if let (Some(client), Some(r)) = (self.client_region, self.regions.as_mut()) {
            assert!(
                req.client_facing() && client == usize::from(req.region),
                "a request is placed in its own client's region"
            );
            let class = req.kind_idx();
            let here = r.region_of(home);
            r.stats.facing[class] += 1;
            r.stats.dispatched[home] += 1;
            if client == here {
                return 0;
            }
            let hop = r.round_trip_ns(client, here);
            r.stats.away[class] += 1;
            r.stats.reach_ns += hop;
            r.stats.reach_hops += 1;
            return hop;
        }
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
        self.run_paired(home, req, None, false)
    }

    fn resubmitted(&self, id: Option<usize>) -> bool {
        id.is_some_and(|id| self.aborted_requests.contains(&id))
    }

    fn router_refusal(&mut self, home: usize, req: &Request) -> Option<Cost> {
        if self.router_admits(home, req, false) {
            return None;
        }
        self.refused_by_router[req.kind_idx()] += 1;
        Some(Cost {
            pending: true,
            ..Cost::default()
        })
    }

    fn run_paired(
        &mut self,
        home: usize,
        req: &Request,
        pair: Option<(usize, u64)>,
        resubmitted: bool,
    ) -> Cost {
        if let Some(refused) = self.router_refusal(home, req) {
            return refused;
        }
        self.dispatch(home, req, pair, resubmitted)
    }

    fn dispatch(
        &mut self,
        home: usize,
        req: &Request,
        pair: Option<(usize, u64)>,
        resubmitted: bool,
    ) -> Cost {
        let class = req.kind_idx();
        if let Some(t) = self.instruments.tenants.as_mut() {
            t.set_requester(req.tenant);
        }
        self.domains[home].set_owner(req.tenant);
        self.domains[home].set_pattern(req.pattern);
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
            self.note_decode(home, req, queue + exec);
            if req.tokens > 0 && !resubmitted {
                self.note_length(home, req, queue + exec);
            }
            if req.tokens > 0 && self.queue == Queue::Plas && self.cancel == CancelMode::Off {
                let end = self.arrival_ns + queue + exec;
                self.completions.push(Reverse((end, req.program, exec)));
            }
            cost.exec_ns = exec;
            cost.queue_ns += queue;
            self.lease_side_effects(home, req, queue + exec);
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
        let (resv_seq, belief_hold) = self.hold_claim(home, req, until);
        let cache_seq = self.domains[home].seal(until);
        self.handles = until.map(|end| Handles {
            cache_seq,
            resv_seq,
            belief_hold,
            start_ns: end - cost.exec_ns,
            end_ns: end,
        });
        self.emit_directives(home, req, &cost);
        self.observe_emit(home);
        self.domains[home].set_owner(None);
        if !cost.pending {
            self.observe_touched(home, req);
        }
        cost
    }

    fn note_decode(&mut self, home: usize, req: &Request, held_ns: u64) {
        if req.tokens == 0 {
            return;
        }
        let from = usize::from(req.region);
        if let Some(sh) = self.shards.as_mut() {
            sh.flights[self.shard][home].push(self.arrival_ns + held_ns);
        }
        if let Some(b) = self.budgets.as_mut() {
            b.tokens[from] += req.tokens;
        }
        let Some(r) = self.regions.as_mut() else {
            return;
        };
        if let Some(t) = r.table.as_mut() {
            t.tokens[from] += req.tokens;
            t.requests[from] += 1;
        }
        let here = r.region_of(home);
        if r.summary_ns > 0 && from != here && self.client_region == Some(from) {
            r.forwards[from][here].push(self.arrival_ns + held_ns);
        }
    }

    fn running_counts(&self) -> Vec<usize> {
        let (Some(r), Some(b)) = (self.regions.as_ref(), self.budgets.as_ref()) else {
            return Vec::new();
        };
        let mut counts = vec![0; r.count()];
        for (d, up) in b.provisioned.iter().enumerate() {
            counts[r.region_of(d)] += usize::from(*up);
        }
        counts
    }

    fn apply_budgets(&mut self, now: u64) {
        let interval_ns = match self.budgets.as_ref().map(|b| &b.rule) {
            None | Some(BudgetRule::Static) => return,
            Some(BudgetRule::Planned(_)) => None,
            Some(BudgetRule::Follow { interval_ns }) => Some(*interval_ns),
        };
        match interval_ns {
            None => self.apply_plan(now),
            Some(interval_ns) => self.follow_demand(now, interval_ns),
        }
    }

    fn apply_plan(&mut self, now: u64) {
        loop {
            let due = match self.budgets.as_mut().map(|b| &mut b.rule) {
                Some(BudgetRule::Planned(plan))
                    if plan.first().is_some_and(|(at, _)| *at <= now) =>
                {
                    plan.remove(0).1
                }
                _ => return,
            };
            self.set_node_counts(&due, now);
        }
    }

    fn follow_demand(&mut self, now: u64, interval_ns: u64) {
        let Some(b) = self.budgets.as_mut() else {
            return;
        };
        if now < b.next_follow_ns {
            return;
        }
        let first = b.next_follow_ns == 0;
        b.next_follow_ns = (now / interval_ns + 1) * interval_ns;
        let regions = b.tokens.len();
        let tokens = std::mem::replace(&mut b.tokens, vec![0; regions]);
        if first {
            return;
        }
        if let Some(target) = self.follow_target(&tokens, interval_ns) {
            self.set_node_counts(&target, now);
        }
    }

    fn follow_target(&mut self, tokens: &[u64], interval_ns: u64) -> Option<Vec<usize>> {
        let current = self.running_counts();
        let slots = self
            .regions
            .as_ref()?
            .of
            .iter()
            .filter(|&&x| x == 0)
            .count();
        let seconds = interval_ns as f64 / 1e9;
        let demand: Vec<f64> = tokens.iter().map(|&t| t as f64 / seconds).collect();
        let costs = crate::fleet::Costs::published(interval_ns);
        let best = costs.best_region_counts(&demand, current.iter().sum(), slots);
        if best == current {
            self.budgets.as_mut()?.accrued_ns = 0.0;
            return None;
        }
        let rate = |counts: &[usize]| -> f64 {
            demand
                .iter()
                .zip(counts)
                .map(|(&d, &n)| costs.cost_rate(0, d, n))
                .sum()
        };
        let loss = (rate(&current) - rate(&best)).max(0.0) * seconds;
        let interim: Vec<usize> = current.iter().zip(&best).map(|(&c, &b)| c.min(b)).collect();
        let extra = (rate(&interim) - rate(&current)).max(0.0);
        let rebuild: f64 = self
            .release_order(&current, &best)
            .iter()
            .map(|&d| self.keep_cost_ns(d))
            .sum();
        let b = self.budgets.as_mut()?;
        let cost = extra * b.load_ns as f64 / 1e9 + rebuild;
        b.accrued_ns += loss;
        if b.accrued_ns < cost {
            return None;
        }
        b.accrued_ns = 0.0;
        Some(best)
    }

    fn release_order(&self, current: &[usize], target: &[usize]) -> Vec<usize> {
        let (Some(r), Some(b)) = (self.regions.as_ref(), self.budgets.as_ref()) else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for (region, (&have, &want)) in current.iter().zip(target).enumerate() {
            let mut held: Vec<usize> = (0..r.of.len())
                .filter(|&d| r.of[d] == region && b.provisioned[d])
                .collect();
            held.sort_by(|&x, &y| {
                self.keep_cost_ns(x)
                    .total_cmp(&self.keep_cost_ns(y))
                    .then(y.cmp(&x))
            });
            out.extend(held.into_iter().take(have.saturating_sub(want)));
        }
        out
    }

    fn set_node_counts(&mut self, target: &[usize], now: u64) {
        let current = self.running_counts();
        let released = self.release_order(&current, target);
        let Some(of) = self.regions.as_ref().map(|r| r.of.clone()) else {
            return;
        };
        let Some(b) = self.budgets.as_mut() else {
            return;
        };
        for &d in &released {
            b.provisioned[d] = false;
        }
        for (region, (&have, &want)) in current.iter().zip(target).enumerate() {
            let free: Vec<usize> = (0..of.len())
                .filter(|&d| of[d] == region && !b.provisioned[d])
                .take(want.saturating_sub(have))
                .collect();
            for d in free {
                b.provisioned[d] = true;
                b.ready_at[d] = now + b.load_ns;
                b.moves += 1;
                b.loading_ns += b.load_ns;
            }
        }
        for d in released {
            let _ = self.domains[d].lose_node();
            self.forget_engine(d);
        }
    }

    fn record_length(&mut self, slo: usize, tokens: u64, root: u32) {
        self.counted.length_observations += 1;
        let seen = &mut self.observed[slo];
        seen.0 += tokens;
        seen.1 += 1;
        self.lengths[slo].record(tokens);
        if self.claim_key == ClaimKey::Root {
            let by_root = self.root_observed.entry(root).or_insert((0, 0));
            by_root.0 += tokens;
            by_root.1 += 1;
            self.root_lengths.entry(root).or_default().record(tokens);
        }
    }

    fn lease_side_effects(&mut self, home: usize, req: &Request, duration_ns: u64) {
        if !req.tool || req.authority != Authority::SideEffecting {
            return;
        }
        let until = self.arrival_ns + duration_ns;
        for &(id, meta) in &req.chain {
            self.domains[home].lease(id, meta.kind, until);
            self.copy_out(id, meta.bytes);
            self.domains[home].mark_durable(id);
        }
    }

    fn execute(&mut self, d: usize, req: &Request) -> (u64, u64) {
        if req.tool
            && req.tokens == 0
            && let Some(slots) = self.tool_slots
        {
            if self.slot_free[d].len() != slots {
                self.slot_free[d].resize(slots, 0);
            }
            let now = self.arrival_ns;
            let (slot, free) = self.slot_free[d]
                .iter()
                .copied()
                .enumerate()
                .min_by_key(|&(_, f)| f)
                .unwrap_or((0, 0));
            let wait = free.saturating_sub(now);
            let start = now + wait;
            let end = start + req.exec_ns;
            if let Some(cell) = self.slot_free[d].get_mut(slot) {
                *cell = end;
            }
            self.last_slot = Some(ToolSlot {
                node: d,
                slot,
                start_ns: start,
                end_ns: end,
            });
            return (req.exec_ns, wait);
        }
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

    #[must_use]
    pub fn last_home(&self) -> usize {
        self.last_home
    }

    #[must_use]
    pub fn anchor_of(&self, task: u64) -> Option<usize> {
        self.upstream
            .get(&task)
            .and_then(|sources| sources.first())
            .map(|&(d, _)| d)
    }

    #[must_use]
    pub fn warm_price(&self, task: Option<u64>, fallback: usize, cell: &(BlobId, BlobMeta)) -> f64 {
        let d = task.and_then(|t| self.anchor_of(t)).unwrap_or(fallback);
        let mut need = [0u64; BlobKind::N];
        need[cell.1.kind.idx()] = cell.1.bytes;
        self.domains[d].displacement(&need, &[0; BlobKind::N])
    }

    #[must_use]
    pub fn agent_request(agent: &Agent) -> Request {
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
            program: 0,
            pattern: Pattern::MultiAgent,
            authority: Authority::ReadOnly,
            root: agent.root,
            tool: false,
            region: 0,
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
            program: 0,
            pattern: Pattern::MultiAgent,
            authority: Authority::ReadOnly,
            root: 0,
            tool: false,
            region: 0,
        }
    }

    fn serve_gang(&mut self, req: &Request, gang: &Gang) -> Cost {
        self.gang_parts.clear();
        self.deciding = Pattern::MultiAgent;
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
            .and_then(|t| self.flow_take(t))
            .unwrap_or_default()
            .into_iter()
            .map(|(d, payload)| (d, payload / n as u64))
            .collect();
        let flow: Vec<(usize, u64)> = if self.flow_aware {
            dispatch.clone()
        } else {
            Vec::new()
        };

        let probes: Vec<Request> = gang
            .agents
            .iter()
            .map(|agent| Request {
                region: req.region,
                ..Self::agent_request(agent)
            })
            .collect();
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
                self.flow_put(hint.task, results);
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
                        let (blocks, extra) = self.claim(&probes[i]);
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
        let mut eligible = self.eligible(self.decode_pool(), probe);
        if eligible.is_empty() && self.scope.is_some() && !self.confined {
            eligible = self.eligible(self.decode_pool_all(), probe);
        }
        eligible.retain(|&d| self.lost[d].is_none());
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
                    && self.router_admits(d, probe, true)
                    && self.node_accepts(d, probe))
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
        if self.armed
            && let Some(handles) = self.handles.take()
        {
            self.gang_parts.push(Part {
                node: d,
                handles,
                req: probe.clone(),
            });
        }
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
        let caller_region = self
            .regions
            .as_ref()
            .filter(|r| r.mode == RegionMode::Regional)
            .map(|r| r.region_of(caller));
        let candidates: Vec<usize> = self
            .active
            .iter()
            .copied()
            .filter(|&d| self.lost[d].is_none() && self.available(d))
            .filter(|&d| {
                caller_region
                    .is_none_or(|c| self.regions.as_ref().is_some_and(|r| r.region_of(d) == c))
            })
            .collect();

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
    reach: f64,

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
        self.loaded() + self.congestion + self.prefill + self.reach
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

pub const CLIENT_BYTES: u64 = 16 * 1024;

const TABLE_KEY: u64 = 0x7ab1e;

const RESIDENCY_KEY: u64 = 0x5e51_de4c;

const LEASE_FLOOR: f64 = 0.02;

const LEASE_DECAY: f64 = 0.5;

fn seq_hint(x: u64) -> u64 {
    let mut z = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

const TABLE_TOKENS_DEFAULT: f64 = 124.0;

const TABLE_STEP: f64 = 0.005;

fn route_demand(
    costs: &crate::fleet::Costs,
    demand: &[f64],
    per_request: &[f64],
    nodes: &[usize],
    round_trips: &[Vec<f64>],
) -> Vec<Vec<f64>> {
    let count = demand.len();
    let mut moved: Vec<Vec<f64>> = (0..count)
        .map(|a| {
            (0..count)
                .map(|b| if a == b { demand[a] } else { 0.0 })
                .collect()
        })
        .collect();
    let served = |m: &[Vec<f64>], to: usize| -> f64 { (0..count).map(|a| m[a][to]).sum() };
    let step = (demand.iter().sum::<f64>() * TABLE_STEP).max(1.0);
    loop {
        let mut best: Option<(usize, usize, f64)> = None;
        for a in (0..count).filter(|&a| moved[a][a] >= step) {
            for b in (0..count).filter(|&b| b != a) {
                let (from, to) = (served(&moved, a), served(&moved, b));
                let saved =
                    costs.cost_rate(0, from, nodes[a]) - costs.cost_rate(0, from - step, nodes[a]);
                let added =
                    costs.cost_rate(0, to + step, nodes[b]) - costs.cost_rate(0, to, nodes[b]);
                let trip = step / per_request[a] * round_trips[a][b];
                let delta = added + trip - saved;
                if delta < best.map_or(-1e-6, |(_, _, d)| d) {
                    best = Some((a, b, delta));
                }
            }
        }
        let Some((a, b, _)) = best else {
            break;
        };
        moved[a][a] -= step;
        moved[a][b] += step;
    }
    (0..count)
        .map(|a| {
            (0..count)
                .map(|b| {
                    if demand[a] > 0.0 {
                        moved[a][b] / demand[a]
                    } else {
                        f64::from(u8::from(a == b))
                    }
                })
                .collect()
        })
        .collect()
}

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

    fn single_trace(seed: u64, ops: u64, throughput: f64) -> Vec<Request> {
        Workload::with_fanout(seed, ops, 1.0, 0.0)
            .with_decode_kv(crate::work::TOKENS_PER_KV_BLOCK)
            .with_throughput(throughput)
            .collect()
    }

    fn waiting_machine(partition: u64, wait: EngineWait) -> Machine {
        let mut mach = engine_machine(4 << 30, partition, Control::Unified);
        mach.set_hold_decodes(true);
        mach.set_arrival_rate(250.0);
        mach.set_engine_wait(wait);
        mach
    }

    fn submit_all(mach: &mut Machine, trace: &[Request]) -> (Vec<Cost>, usize) {
        let mut costs = Vec::new();
        let mut opened = 0;
        for req in trace {
            match mach.submit(req) {
                Submitted::Closed(cost) => costs.push(cost),
                Submitted::Open(_) => opened += 1,
            }
            costs.extend(mach.drain_closed().into_iter().map(|(_, cost)| cost));
        }
        mach.finish();
        costs.extend(mach.drain_closed().into_iter().map(|(_, cost)| cost));
        (costs, opened)
    }

    const TIGHT: u64 = 160 << 20;
    const MILD: u64 = 320 << 20;

    #[test]
    fn an_engine_that_waits_preempts_no_single_request() {
        let trace = single_trace(3, 3_000, 0.0);
        let mut runs = waiting_machine(TIGHT, EngineWait::Off);
        let (_, none_opened) = submit_all(&mut runs, &trace);
        assert_eq!(none_opened, 0);
        assert!(
            runs.preempted.iter().sum::<u64>() > 0,
            "the fixture must overcommit the partition under today's engine"
        );
        let mut waits = waiting_machine(TIGHT, EngineWait::Fifo);
        let (costs, opened) = submit_all(&mut waits, &trace);
        assert!(
            opened > 0,
            "an overcommitted partition must queue something"
        );
        assert_eq!(waits.preempted.iter().sum::<u64>(), 0);
        assert!(costs.iter().all(|c| !c.preempted));
    }

    #[test]
    fn every_request_that_waits_closes_once_and_carries_its_wait() {
        let trace = single_trace(4, 3_000, 0.0);
        let mut mach = waiting_machine(TIGHT, EngineWait::Fifo);
        let (costs, opened) = submit_all(&mut mach, &trace);
        assert_eq!(
            costs.len(),
            trace.len(),
            "no request is lost or closed twice"
        );
        assert_eq!(mach.engine_waits.queued as usize, opened);
        let waited: u64 = mach.engine_waits.waited.iter().sum();
        assert_eq!(waited as usize, opened);
        let carried: u64 = costs.iter().map(|c| c.queue_ns).sum();
        let mut base = waiting_machine(TIGHT, EngineWait::Off);
        let (plain, _) = submit_all(&mut base, &trace);
        let engine_queue: u64 = plain.iter().map(|c| c.queue_ns).sum();
        assert!(
            carried >= mach.engine_waits.waited_ns.iter().sum::<u64>() + engine_queue / 2,
            "the wait is charged to the request as queue time"
        );
        assert!(mach.engine_queues.iter().all(Vec::is_empty));
    }

    #[test]
    fn a_sequence_larger_than_the_partition_runs_at_once_and_is_preempted() {
        let partition = 64 * crate::work::KV_BLOCK_BYTES;
        let mut req = single_trace(5, 400, 0.0)
            .into_iter()
            .find(|r| r.tokens > 0 && r.chain.len() <= 20)
            .expect("a short chain among the first requests");
        let mut parent = req.chain.last().expect("a chain").0;
        req.produces = (0..80)
            .map(|i| {
                let id = BlobId::chain(parent, format!("oversize:{i}").as_bytes());
                let meta = BlobMeta {
                    kind: BlobKind::KvBlock,
                    bytes: crate::work::KV_BLOCK_BYTES,
                    parent: Some(parent),
                    recompute_ns: crate::work::KV_BLOCK_NS,
                };
                parent = id;
                (id, meta)
            })
            .collect();
        let mut mach = waiting_machine(partition, EngineWait::Fifo);
        let (costs, opened) = submit_all(&mut mach, &[req]);
        assert_eq!(opened, 0, "a sequence that can never fit does not wait");
        assert_eq!(mach.engine_waits.unfittable, 1);
        assert_eq!(costs.len(), 1);
        assert!(costs[0].preempted);
    }

    #[test]
    fn first_fit_starts_sooner_than_arrival_order_and_priority_serves_interactive_first() {
        let trace = single_trace(6, 4_000, 0.3);
        let mean_wait = |mach: &Machine, slo: usize| {
            mach.engine_waits.waited_ns[slo] as f64 / mach.engine_waits.waited[slo].max(1) as f64
        };
        let mut fifo = waiting_machine(TIGHT, EngineWait::Fifo);
        let mut first_fit = waiting_machine(TIGHT, EngineWait::FirstFit);
        let mut priority = waiting_machine(TIGHT, EngineWait::Priority);
        for mach in [&mut fifo, &mut first_fit, &mut priority] {
            submit_all(mach, &trace);
        }
        let all = |m: &Machine| m.engine_waits.waited_ns.iter().sum::<u64>() as f64;
        assert!(all(&first_fit) < all(&fifo));
        assert!(mean_wait(&priority, 0) < mean_wait(&fifo, 0));
        assert!(mean_wait(&priority, 0) < mean_wait(&priority, 1));
    }

    #[test]
    fn the_probe_reads_what_the_old_engine_hides_and_changes_nothing() {
        let trace = single_trace(7, 3_000, 0.0);
        let mut plain = waiting_machine(TIGHT, EngineWait::Off);
        let (before, _) = submit_all(&mut plain, &trace);
        let mut probed = waiting_machine(TIGHT, EngineWait::Off);
        probed.set_probe_engine(true);
        let (after, _) = submit_all(&mut probed, &trace);
        assert_eq!(format!("{before:?}"), format!("{after:?}"));
        let seen =
            probed.engine_waits.probe_ns.len() as u64 + probed.engine_waits.probe_unplaceable;
        assert!(seen > 0);
        assert!(seen <= probed.preempted.iter().sum::<u64>());
    }

    fn queued_machine(partition: u64, queue: Queue) -> Machine {
        let mut mach = waiting_machine(partition, EngineWait::Off);
        mach.set_admission(Reserve::Perfect, crate::work::TOKENS_PER_KV_BLOCK);
        mach.set_queue(queue);
        mach
    }

    fn waiting_for(req: &Request, id: usize, arrival_ns: u64) -> Waiting {
        Waiting {
            id,
            seq: 0,
            req: req.clone(),
            arrival_ns,
            queued_ns: arrival_ns,
            pre_ns: 0,
            decide_ns: 0,
            requeued: false,
            not_before: 0,
        }
    }

    #[test]
    fn a_queue_holds_what_the_router_would_refuse_and_closes_every_request_once() {
        let trace = single_trace(9, 3_000, 0.3);
        let mut refusing = queued_machine(TIGHT, Queue::Off);
        let (served, opened) = submit_all(&mut refusing, &trace);
        assert_eq!(opened, 0);
        assert!(refusing.refused_by_router.iter().sum::<u64>() > 0);
        assert_eq!(served.len(), trace.len());
        let mut queued = queued_machine(TIGHT, Queue::Fifo);
        let (costs, opened) = submit_all(&mut queued, &trace);
        assert!(
            opened > 0,
            "an overcommitted partition must queue something"
        );
        assert_eq!(
            costs.len(),
            trace.len(),
            "no request is lost or closed twice"
        );
        assert_eq!(queued.refused_by_router.iter().sum::<u64>(), 0);
        assert!(queued.router_queue.is_empty());
        assert_eq!(
            queued.queue_waits.waited.iter().sum::<u64>(),
            queued.queue_waits.queued
        );
        assert!(costs.iter().all(|c| !c.pending));
    }

    #[test]
    fn a_claim_no_partition_could_hold_is_refused_and_blocks_no_queue() {
        let trace = single_trace(9, 600, 0.3);
        let mut queued = queued_machine(4 * crate::work::KV_BLOCK_BYTES, Queue::Fifo);
        let (costs, _) = submit_all(&mut queued, &trace);
        assert_eq!(
            costs.len(),
            trace.len(),
            "no request is lost or closed twice"
        );
        assert!(queued.refused_by_router.iter().sum::<u64>() > 0);
        assert!(queued.router_queue.is_empty());
        assert_eq!(
            queued.queue_waits.waited.iter().sum::<u64>(),
            queued.queue_waits.queued
        );
    }

    #[test]
    fn a_queued_request_carries_its_wait_as_queue_time() {
        let trace = single_trace(10, 3_000, 0.0);
        let mut plain = queued_machine(TIGHT, Queue::Off);
        let (before, _) = submit_all(&mut plain, &trace);
        let mut queued = queued_machine(TIGHT, Queue::Fifo);
        let (after, _) = submit_all(&mut queued, &trace);
        let queue_ns = |costs: &[Cost]| costs.iter().map(|c| c.queue_ns).sum::<u64>();
        assert!(
            queue_ns(&after)
                >= queue_ns(&before) + queued.queue_waits.waited_ns.iter().sum::<u64>() / 2,
            "the router's wait is charged to the request"
        );
    }

    #[test]
    fn a_queue_changes_nothing_where_every_check_passes() {
        let trace = single_trace(11, 2_000, 0.3);
        let mut off = queued_machine(3 << 30, Queue::Off);
        let (a, _) = submit_all(&mut off, &trace);
        for queue in [Queue::Fifo, Queue::Slo, Queue::Plas] {
            let mut on = queued_machine(3 << 30, queue);
            let (b, opened) = submit_all(&mut on, &trace);
            assert_eq!(opened, 0, "{queue:?}");
            assert_eq!(format!("{a:?}"), format!("{b:?}"), "{queue:?}");
        }
    }

    #[test]
    fn declared_class_order_serves_interactive_requests_first() {
        let trace = single_trace(12, 4_000, 0.3);
        let mean = |m: &Machine, slo: usize| {
            m.queue_waits.waited_ns[slo] as f64 / m.queue_waits.waited[slo].max(1) as f64
        };
        let mut fifo = queued_machine(TIGHT, Queue::Fifo);
        let mut slo = queued_machine(TIGHT, Queue::Slo);
        submit_all(&mut fifo, &trace);
        submit_all(&mut slo, &trace);
        assert!(mean(&slo, 0) < mean(&fifo, 0));
        assert!(mean(&slo, 0) < mean(&slo, 1));
    }

    #[test]
    fn the_queue_orders_by_arrival_class_or_attained_service() {
        let trace = single_trace(13, 400, 0.0);
        let mut reqs = trace.iter().filter(|r| r.tokens > 0);
        let (mut a, mut b) = (
            reqs.next().expect("a decode").clone(),
            reqs.next().expect("a decode").clone(),
        );
        a.program = 1;
        b.program = 2;
        b.slo = Slo::Throughput;
        let (first, second) = (waiting_for(&a, 0, 10), waiting_for(&b, 1, 20));
        let mut mach = queued_machine(TIGHT, Queue::Fifo);
        assert!(mach.queue_key(&first) < mach.queue_key(&second));
        mach.set_queue(Queue::Slo);
        assert!(mach.queue_key(&first) < mach.queue_key(&second));
        let (later_interactive, earlier_throughput) =
            (waiting_for(&a, 2, 30), waiting_for(&b, 3, 5));
        assert!(mach.queue_key(&later_interactive) < mach.queue_key(&earlier_throughput));
        mach.set_queue(Queue::Plas);
        mach.attained.insert(1, 900);
        mach.attained.insert(2, 5);
        assert!(mach.queue_key(&second) < mach.queue_key(&first));
    }

    #[test]
    fn attained_service_accrues_when_a_call_completes_and_not_before() {
        let trace = single_trace(14, 600, 0.0);
        let mut mach = queued_machine(3 << 30, Queue::Plas);
        let first = trace.iter().find(|r| r.tokens > 0).expect("a decode");
        mach.submit(first);
        assert!(mach.attained.is_empty(), "the call is still decoding");
        let (_, exec) = (0, first.tokens * crate::work::DECODE_NS_PER_TOKEN);
        mach.finish();
        for _ in 0..(exec / mach.interval_ns + 200) {
            mach.arrive(false);
        }
        assert!(mach.attained.get(&first.program).copied().unwrap_or(0) > 0);
    }

    #[test]
    fn a_claim_names_the_bytes_of_an_observed_quantile_mean_or_the_declared_bound() {
        let trace = single_trace(15, 200, 0.0);
        let mut req = trace
            .iter()
            .find(|r| r.tokens > 0)
            .expect("a decode")
            .clone();
        req.max_tokens = 700;
        let block = crate::work::KV_BLOCK_BYTES;
        let tpb = crate::work::TOKENS_PER_KV_BLOCK;
        let mut mach = queued_machine(3 << 30, Queue::Off);
        mach.set_claim(Claim::Quantile {
            q: 0.9,
            pooled: false,
        });
        assert_eq!(
            mach.claim(&req).1,
            700u64.div_ceil(tpb) * block,
            "unobserved: the bound"
        );
        for t in 1..=100 {
            mach.lengths[0].record(t);
        }
        for t in 301..=400 {
            mach.lengths[1].record(t);
        }
        assert_eq!(mach.claim(&req).1, 90u64.div_ceil(tpb) * block);
        mach.set_claim(Claim::Quantile {
            q: 0.9,
            pooled: true,
        });
        assert_eq!(mach.claim(&req).1, 370u64.div_ceil(tpb) * block);
        mach.set_claim(Claim::Tiered { q: 0.9 });
        assert_eq!(mach.claim(&req).1, 90u64.div_ceil(tpb) * block);
        req.slo = Slo::Throughput;
        assert_eq!(mach.claim(&req).1, 350u64.div_ceil(tpb) * block);
        mach.set_claim(Claim::Static);
        let (blocks, extra) = Reserve::Perfect.claim(&req, tpb);
        let (claimed, claimed_extra) = mach.claim(&req);
        assert_eq!(claimed_extra, extra);
        assert_eq!(
            claimed.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            blocks.iter().map(|(id, _)| *id).collect::<Vec<_>>()
        );
    }

    #[test]
    fn a_gate_admits_below_its_threshold_and_queues_above_it() {
        let trace = single_trace(16, 3_000, 0.3);
        let run = |theta: f64| {
            let mut mach = waiting_machine(TIGHT, EngineWait::Off);
            mach.set_queue(Queue::Fifo);
            mach.set_claim(Claim::Gate(theta));
            let (costs, _) = submit_all(&mut mach, &trace);
            assert_eq!(costs.len(), trace.len());
            mach.queue_waits.queued
        };
        assert!(run(0.2) > run(0.95));
        assert_eq!(run(1.0), 0, "a gate no node can exceed queues nothing");
    }

    fn cancelling_machine(
        partition: u64,
        queue: Queue,
        mode: CancelMode,
        wait: EngineWait,
    ) -> Machine {
        let mut mach = queued_machine(partition, queue);
        mach.set_engine_wait(wait);
        mach.set_cancel(mode, Victim::Recent);
        mach
    }

    #[test]
    fn an_abort_releases_the_ledgers_a_flight_held_and_only_those() {
        let trace = single_trace(17, 600, 0.5);
        let mut mach =
            cancelling_machine(3 << 30, Queue::Slo, CancelMode::Continue, EngineWait::Off);
        let mut held = None;
        for req in &trace {
            mach.submit(req);
            held = (0..mach.flights.len()).find(|&i| mach.flights[i].req.slo == Slo::Throughput);
            if held.is_some() {
                break;
            }
        }
        let at = held.expect("a throughput decode in flight");
        let node = mach.flights[at].node;
        let now = mach.arrival_ns;
        let committed = mach.reserved[node].committed();
        let exclusive = mach.freed_by(&mach.flights[at]);
        let free = mach.domains[node]
            .kv_fit(&[])
            .expect("an engine cache")
            .free;
        let load = mach.engines[node].load(now);
        let flights = mach.flights.len();
        mach.cancel_flight(at, false);
        assert_eq!(mach.reserved[node].committed(), committed - exclusive);
        assert!(
            mach.domains[node]
                .kv_fit(&[])
                .expect("an engine cache")
                .free
                >= free
        );
        assert_eq!(mach.engines[node].load(now), load - 1);
        assert_eq!(mach.flights.len(), flights - 1);
        assert_eq!(mach.router_queue.len(), 1);
        assert_eq!(mach.cancelled_requests(), 1);
        assert_eq!(mach.cancel_stats.cancels, 1);
    }

    #[test]
    fn only_a_lower_class_sequence_is_a_victim() {
        let trace = single_trace(18, 500, 0.5);
        let mut mach =
            cancelling_machine(3 << 30, Queue::Slo, CancelMode::Continue, EngineWait::Off);
        for req in &trace {
            mach.submit(req);
        }
        let head = |slo: Slo| {
            let mut r = trace
                .iter()
                .find(|r| r.tokens > 0)
                .expect("a decode")
                .clone();
            r.slo = slo;
            r
        };
        let mut seen = 0;
        for node in 0..mach.nodes() {
            for i in mach.victims_at(node, &head(Slo::Interactive)) {
                assert_eq!(mach.flights[i].req.slo, Slo::Throughput);
                seen += 1;
            }
            assert_eq!(mach.victims_at(node, &head(Slo::Throughput)).len(), 0);
        }
        assert!(
            seen > 0,
            "the fixture must have a throughput decode in flight"
        );
    }

    #[test]
    fn a_continuation_is_the_prompt_plus_the_whole_blocks_decoded() {
        let mach = cancelling_machine(3 << 30, Queue::Slo, CancelMode::Continue, EngineWait::Off);
        let tpb = crate::work::TOKENS_PER_KV_BLOCK;
        let mut req = single_trace(19, 200, 0.0)
            .into_iter()
            .find(|r| r.tokens > 100 && r.produces.len() >= 3)
            .expect("a long decode");
        req.hint = None;
        let kept = mach.continuation(&req, 0);
        assert_eq!(kept.chain.len(), req.chain.len());
        assert_eq!(kept.tokens, req.tokens);
        let decoded = 2 * tpb + 5;
        let resumed = mach.continuation(&req, decoded);
        assert_eq!(resumed.chain.len(), req.chain.len() + 2);
        assert_eq!(resumed.produces.len(), req.produces.len() - 2);
        assert_eq!(resumed.tokens, req.tokens - decoded);
        assert_eq!(
            resumed.exec_ns,
            resumed.tokens * crate::work::DECODE_NS_PER_TOKEN
        );
        assert!(resumed.hint.is_none() && resumed.completes.is_none());
        assert_eq!(mach.continuation(&req, req.tokens + 50).tokens, 1);
    }

    #[test]
    fn a_cancelled_request_is_resubmitted_and_every_request_closes_once() {
        let trace = single_trace(20, 3_000, 0.5);
        let mut off = cancelling_machine(MILD, Queue::Slo, CancelMode::Off, EngineWait::Off);
        submit_all(&mut off, &trace);
        let mut on = cancelling_machine(MILD, Queue::Slo, CancelMode::Continue, EngineWait::Off);
        let (costs, _) = submit_all(&mut on, &trace);
        assert_eq!(
            costs.len(),
            trace.len(),
            "no request is lost or closed twice"
        );
        assert!(on.cancel_stats.cancels > 0);
        assert!(on.cancelled_requests() > 0);
        assert!(on.flights.is_empty() && on.router_queue.is_empty());
        assert!(
            on.queue_waits.waited_ns[0] < off.queue_waits.waited_ns[0],
            "a cancel shortens the wait of the class it protects"
        );
    }

    #[test]
    fn restarting_throws_decode_away_and_continuing_does_not() {
        let trace = single_trace(21, 3_000, 0.3);
        let mut resumed =
            cancelling_machine(TIGHT, Queue::Slo, CancelMode::Continue, EngineWait::Off);
        let mut restarted =
            cancelling_machine(TIGHT, Queue::Slo, CancelMode::Drop, EngineWait::Off);
        let (a, _) = submit_all(&mut resumed, &trace);
        let (b, _) = submit_all(&mut restarted, &trace);
        assert_eq!(a.len(), trace.len());
        assert_eq!(b.len(), trace.len());
        assert_eq!(resumed.cancel_stats.wasted_decode_ns, 0);
        assert!(restarted.cancel_stats.wasted_decode_ns > 0);
    }

    #[test]
    fn an_interactive_request_waiting_at_an_engine_cancels_a_lower_class_sequence_there() {
        let trace = single_trace(22, 3_000, 0.5);
        let run = |mode: CancelMode| {
            let mut mach = cancelling_machine(TIGHT, Queue::Slo, mode, EngineWait::Priority);
            mach.set_claim(Claim::Quantile {
                q: 0.1,
                pooled: false,
            });
            let (costs, _) = submit_all(&mut mach, &trace);
            assert_eq!(costs.len(), trace.len());
            mach
        };
        let on = run(CancelMode::Continue);
        assert!(on.cancel_stats.engine_cancels > 0);
        assert!(on.cancel_stats.engine_cancels <= on.cancel_stats.cancels);
        assert_eq!(run(CancelMode::Off).cancel_stats.cancels, 0);
    }

    #[test]
    fn a_cancel_changes_no_request_where_nothing_is_overcommitted() {
        let trace = single_trace(23, 2_000, 0.3);
        let mut off = cancelling_machine(3 << 30, Queue::Slo, CancelMode::Off, EngineWait::Off);
        let mut on = cancelling_machine(3 << 30, Queue::Slo, CancelMode::Continue, EngineWait::Off);
        let (a, _) = submit_all(&mut off, &trace);
        let (b, _) = submit_all(&mut on, &trace);
        assert_eq!(on.cancel_stats.cancels, 0);
        let sorted = |costs: &[Cost]| {
            let mut v: Vec<String> = costs.iter().map(|c| format!("{c:?}")).collect();
            v.sort();
            v
        };
        assert_eq!(sorted(&a), sorted(&b));
    }

    #[test]
    fn a_gate_that_evicts_resubmits_what_it_drops() {
        let trace = single_trace(24, 3_000, 0.5);
        let mut mach = cancelling_machine(TIGHT, Queue::Slo, CancelMode::Drop, EngineWait::Off);
        mach.set_claim(Claim::Gate(0.5));
        let (costs, _) = submit_all(&mut mach, &trace);
        assert_eq!(costs.len(), trace.len());
        assert!(mach.cancel_stats.cancels > 0);
        assert!(mach.flights.is_empty() && mach.router_queue.is_empty());
    }

    #[test]
    fn finishing_a_trace_samples_no_occupancy_and_so_leaves_the_grants_alone() {
        let trace = single_trace(25, 1_500, 0.5);
        let mut mach = cancelling_machine(MILD, Queue::Slo, CancelMode::Continue, EngineWait::Off);
        for req in &trace {
            mach.submit(req);
        }
        let (samples, mean) = (mach.kv_samples, mach.kv_mean());
        assert!(
            !mach.flights.is_empty(),
            "the trace ends with decodes in flight"
        );
        mach.finish();
        assert_eq!(mach.kv_samples, samples);
        assert_eq!(mach.kv_mean(), mean);
    }

    fn leaving_machine(queue: Queue, share: f64, leak: bool) -> Machine {
        let mut mach = queued_machine(MILD, queue);
        mach.set_departures(Some(Departures { share, leak }));
        mach
    }

    fn submit_counting_departures(mach: &mut Machine, trace: &[Request]) -> (usize, usize) {
        let (costs, _) = submit_all(mach, trace);
        let mut departed = mach.drain_departed().len();
        mach.finish();
        departed += mach.drain_departed().len();
        (costs.len(), departed)
    }

    #[test]
    fn a_departing_client_has_its_sequence_aborted_and_leaves_every_tally() {
        let trace = single_trace(26, 3_000, 0.3);
        let mut mach = leaving_machine(Queue::Fifo, 0.2, false);
        let mut served = 0;
        let mut departed = 0;
        for req in &trace {
            if let Submitted::Closed(_) = mach.submit(req) {
                served += 1;
            }
            served += mach.drain_closed().len();
            departed += mach.drain_departed().len();
        }
        mach.finish();
        served += mach.drain_closed().len();
        departed += mach.drain_departed().len();
        assert!(departed > 0);
        assert_eq!(
            served + departed,
            trace.len(),
            "every request ends served or departed"
        );
        assert!(mach.departure_stats.aborted > 0);
        assert!(mach.departure_stats.freed_ns > 0);
        assert_eq!(mach.departure_stats.leaked_ns, 0);
        assert!(mach.flights.is_empty());
    }

    #[test]
    fn a_leaked_departure_runs_to_its_end_and_still_leaves_the_tally() {
        let trace = single_trace(27, 3_000, 0.3);
        let mut aborting = leaving_machine(Queue::Fifo, 0.2, false);
        let mut leaking = leaving_machine(Queue::Fifo, 0.2, true);
        let (served_a, departed_a) = submit_counting_departures(&mut aborting, &trace);
        let (served_l, departed_l) = submit_counting_departures(&mut leaking, &trace);
        assert_eq!(served_a + departed_a, trace.len());
        assert_eq!(served_l + departed_l, trace.len());
        assert_eq!(leaking.departure_stats.aborted, 0);
        assert!(leaking.departure_stats.leaked_ns > 0);
        assert_eq!(departed_l as u64, leaking.departure_stats.leaving);
    }

    #[test]
    fn which_clients_leave_does_not_depend_on_the_arm() {
        let trace = single_trace(28, 2_000, 0.3);
        let leaving = |queue: Queue| {
            let mut mach = leaving_machine(queue, 0.25, true);
            submit_counting_departures(&mut mach, &trace);
            mach.departure_stats.leaving
        };
        assert_eq!(leaving(Queue::Fifo), leaving(Queue::Slo));
    }

    #[test]
    fn registering_flights_without_a_cancel_accrues_attained_service_once() {
        let trace = single_trace(29, 1_500, 0.3);
        let mut plain = queued_machine(MILD, Queue::Plas);
        let mut sampled = queued_machine(MILD, Queue::Plas);
        sampled.set_track_stream(true);
        for mach in [&mut plain, &mut sampled] {
            submit_all(mach, &trace);
            while !mach.completions.is_empty() {
                mach.arrive(false);
            }
        }
        assert!(!plain.attained.is_empty());
        assert_eq!(plain.attained, sampled.attained);
    }

    #[test]
    fn the_stream_buffer_counts_tokens_emitted_so_far_and_changes_no_request() {
        let trace = single_trace(29, 2_000, 0.3);
        let mut plain = queued_machine(MILD, Queue::Slo);
        let mut sampled = queued_machine(MILD, Queue::Slo);
        sampled.set_track_stream(true);
        let (a, _) = submit_all(&mut plain, &trace);
        let (b, _) = submit_all(&mut sampled, &trace);
        let sorted = |costs: &[Cost]| {
            let mut v: Vec<String> = costs.iter().map(|c| format!("{c:?}")).collect();
            v.sort();
            v
        };
        assert_eq!(sorted(&a), sorted(&b));
        assert!(sampled.stream.samples > 0);
        let peak = sampled
            .stream
            .peak_tokens
            .iter()
            .copied()
            .max()
            .unwrap_or(0);
        assert!(peak > 0);
        let ceiling = (MAX_BATCH as u64) * 224;
        assert!(
            peak <= ceiling,
            "a node cannot have emitted more than its batch can hold"
        );
        for (sum, top) in sampled
            .stream
            .sum_tokens
            .iter()
            .zip(&sampled.stream.peak_tokens)
        {
            assert!(*sum <= top * sampled.stream.samples);
        }
    }

    #[test]
    fn a_router_only_cancel_never_fires_at_an_engine() {
        let trace = single_trace(30, 3_000, 0.5);
        let run = |triggers: Triggers| {
            let mut mach = cancelling_machine(
                TIGHT,
                Queue::Slo,
                CancelMode::Continue,
                EngineWait::Priority,
            );
            mach.set_claim(Claim::Quantile {
                q: 0.1,
                pooled: false,
            });
            mach.set_cancel_triggers(triggers);
            let (costs, _) = submit_all(&mut mach, &trace);
            assert_eq!(costs.len(), trace.len());
            mach
        };
        assert_eq!(run(Triggers::Router).cancel_stats.engine_cancels, 0);
        assert!(run(Triggers::Both).cancel_stats.engine_cancels > 0);
    }

    #[test]
    fn a_resubmitted_request_is_observed_once() {
        let trace = single_trace(32, 3_000, 0.5);
        let decodes = trace.iter().filter(|r| r.tokens > 0).count() as u64;
        for mode in [CancelMode::Continue, CancelMode::Drop] {
            let mut mach = cancelling_machine(MILD, Queue::Slo, mode, EngineWait::Off);
            let (costs, _) = submit_all(&mut mach, &trace);
            assert_eq!(costs.len(), trace.len());
            assert!(mach.cancel_stats.cancels > 0);
            assert_eq!(mach.lengths[0].seen() + mach.lengths[1].seen(), decodes);
            assert_eq!(mach.observed[0].1 + mach.observed[1].1, decodes);
        }
    }

    #[test]
    fn a_resubmitted_request_charges_its_recompute_to_the_cancel() {
        let trace = single_trace(31, 3_000, 0.5);
        let mut restarted = cancelling_machine(MILD, Queue::Slo, CancelMode::Drop, EngineWait::Off);
        submit_all(&mut restarted, &trace);
        assert!(restarted.cancel_stats.cancels > 0);
        assert!(restarted.cancel_stats.reprefill_ns > 0);
        let mut quiet = cancelling_machine(3 << 30, Queue::Slo, CancelMode::Drop, EngineWait::Off);
        submit_all(&mut quiet, &trace);
        assert_eq!(quiet.cancel_stats.reprefill_ns, 0);
    }

    #[test]
    fn an_engine_that_waits_changes_nothing_where_every_sequence_fits() {
        let trace = single_trace(8, 2_000, 0.3);
        let mut off = waiting_machine(3 << 30, EngineWait::Off);
        let mut fifo = waiting_machine(3 << 30, EngineWait::Fifo);
        let (a, _) = submit_all(&mut off, &trace);
        let (b, opened) = submit_all(&mut fifo, &trace);
        assert_eq!(opened, 0);
        assert_eq!(format!("{a:?}"), format!("{b:?}"));
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
            program: 0,
            pattern: crate::work::Pattern::Plain,
            authority: crate::work::Authority::ReadOnly,
            root: 0,
            tool: false,
            region: 0,
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
            program: 0,
            pattern: crate::work::Pattern::Plain,
            authority: crate::work::Authority::ReadOnly,
            root: 0,
            tool: false,
            region: 0,
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

    fn bare_request() -> Request {
        Request {
            phase: 0,
            chain: Vec::new(),
            requires: Vec::new(),
            hint: None,
            completes: None,
            exec_ns: 0,
            tokens: 0,
            gang: None,
            produces: Vec::new(),
            max_tokens: 0,
            slo: Slo::Interactive,
            retention: crate::work::Retention::default(),
            concurrent: true,
            tenant: None,
            program: 0,
            pattern: Pattern::Plain,
            authority: Authority::ReadOnly,
            root: 0,
            tool: false,
            region: 0,
        }
    }

    fn flow_pair(downstream_blocks: u64) -> (Request, Request) {
        let upstream_cell = (
            BlobId::leaf(b"fn:wait"),
            BlobMeta {
                kind: BlobKind::Snapshot,
                bytes: 1 << 20,
                parent: None,
                recompute_ns: 1_000,
            },
        );
        let mut parent = crate::blob::ROOT;
        let downstream: Vec<(BlobId, BlobMeta)> = (0..downstream_blocks)
            .map(|d| {
                let block = crate::programs::kv_block(parent, &format!("wait:{d}"));
                parent = block.0;
                block
            })
            .collect();
        let mut up = bare_request();
        up.chain = vec![upstream_cell];
        up.requires = Vec::new();
        up.tokens = 0;
        up.exec_ns = 1_000;
        up.pattern = Pattern::Pipeline;
        up.hint = Some(FlowHint {
            task: 7,
            template_len: downstream.len(),
            downstream: downstream.clone(),
            probability: 1.0,
            lead_ops: 6,
            payload_bytes: 0,
        });
        let mut down = bare_request();
        down.chain = downstream;
        down.completes = Some(7);
        (up, down)
    }

    fn landing_run(downstream_blocks: u64, arrives_ns: u64) -> (u64, u64) {
        let mut mach = gate_machine(Control::Unified, false);
        mach.set_prefill_ahead(true);
        let (up, down) = flow_pair(downstream_blocks);
        let Submitted::Closed(_) = mach.submit_at(1_000_000, &up) else {
            panic!("closed");
        };
        let Submitted::Closed(cost) = mach.submit_at(1_000_000 + arrives_ns, &down) else {
            panic!("closed");
        };
        (cost.queue_ns, mach.instruments.prefill.work_ns)
    }

    #[test]
    fn a_downstream_that_arrives_before_its_prefill_ends_waits_for_the_rest_of_it() {
        let work = landing_run(10, u64::MAX / 4).1;
        assert!(work > 0);
        let (early, _) = landing_run(10, work / 4);
        let (late, _) = landing_run(10, 2 * work);
        assert_eq!(late, 0);
        assert!(early > 0 && early <= work, "{early} of {work}");
        let (instant, _) = landing_run(10, 1_000);
        assert!(instant > early);
    }

    #[test]
    fn a_tool_waits_for_an_executor_slot_and_only_when_slots_are_bounded() {
        let tool_request = |at: u64| {
            let mut req = bare_request();
            req.chain = vec![(
                BlobId::leaf(format!("sbx:{at}").as_bytes()),
                BlobMeta {
                    kind: BlobKind::Snapshot,
                    bytes: 1 << 20,
                    parent: None,
                    recompute_ns: 1_000,
                },
            )];
            req.requires = Vec::new();
            req.tokens = 0;
            req.exec_ns = 50_000_000;
            req.tool = true;
            req
        };
        let waits = |slots: Option<usize>| -> Vec<u64> {
            let mut mach = gate_machine(Control::Unified, false);
            mach.set_tool_slots(slots);
            (0..16)
                .map(|i| {
                    let req = tool_request(i);
                    let Submitted::Closed(cost) = mach.submit_at(1_000_000 + i * 1_000, &req)
                    else {
                        panic!("closed");
                    };
                    cost.queue_ns
                })
                .collect()
        };
        assert!(waits(None).iter().all(|&w| w == 0));
        assert!(waits(Some(1000)).iter().all(|&w| w == 0));
        assert!(waits(Some(1)).iter().any(|&w| w > 0));
    }

    #[test]
    fn coupling_by_pattern_sums_to_the_machines_own_counters() {
        let mut mach = gate_machine(Control::Unified, false);
        mach.set_regret(true);
        for req in &decode_trace(3, 1_500) {
            mach.serve_request(req);
        }
        let (coupled, decisions) = mach
            .locality_by
            .iter()
            .fold((0, 0), |a, b| (a.0 + b.0, a.1 + b.1));
        assert_eq!(decisions, mach.locality_coupled_decisions);
        assert_eq!(coupled, mach.locality_coupled);
        let memory: u64 = mach.memory_by_pattern().iter().map(|c| c.1).sum();
        let (_, evictions) = mach.memory_coupled();
        assert_eq!(memory, evictions);
    }

    #[test]
    fn a_resubmission_at_the_trace_instants_is_the_trace_submitted_in_order() {
        let trace = decode_trace(3, 800);
        let mut plain = gate_machine(Control::Unified, false);
        let mut timed = gate_machine(Control::Unified, false);
        let interval = (1e9 / 250.0) as u64;
        let mut slot = 0;
        for req in &trace {
            let a = plain.serve_request(req);
            let at = if req.concurrent {
                slot * interval
            } else {
                slot += 1;
                slot * interval
            };
            let Submitted::Closed(b) = timed.submit_at(at, req) else {
                panic!("closed");
            };
            assert_eq!(format!("{a:?}"), format!("{b:?}"));
        }
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

    fn flush_counts(mach: &mut Machine) {
        let now = mach.now_ns();
        mach.advance_to(now + 1_000_000_000_000);
    }

    #[test]
    fn counting_kv_events_changes_no_cost_and_balances_against_what_each_tier_holds() {
        let trace = single_trace(21, 3_000, 0.5);
        let mut plain = cancelling_machine(MILD, Queue::Slo, CancelMode::Continue, EngineWait::Off);
        let (plain_costs, _) = submit_all(&mut plain, &trace);
        let mut counting =
            cancelling_machine(MILD, Queue::Slo, CancelMode::Continue, EngineWait::Off);
        counting.set_count_events(true);
        let (counted_costs, _) = submit_all(&mut counting, &trace);
        flush_counts(&mut counting);
        assert_eq!(format!("{plain_costs:?}"), format!("{counted_costs:?}"));
        assert_eq!(plain.writes().kv_total(), 0);
        let writes = counting.writes();
        assert!(writes.kv_total() > 0);
        for (slot, medium) in [Medium::Gpu, Medium::Cpu, Medium::Storage]
            .into_iter()
            .enumerate()
        {
            let held: usize = counting
                .domains
                .iter()
                .map(|h| h.kv_ids(medium).len())
                .sum();
            assert_eq!(
                writes.kv_events[slot] - writes.kv_events[3 + slot],
                held as u64,
                "{medium:?}: stores less removals is what the tier holds"
            );
        }
    }

    #[test]
    fn every_reservation_is_released_and_every_flight_ends_closed_or_cancelled() {
        let trace = single_trace(20, 3_000, 0.5);
        let mut mach = cancelling_machine(MILD, Queue::Slo, CancelMode::Continue, EngineWait::Off);
        submit_all(&mut mach, &trace);
        flush_counts(&mut mach);
        let writes = mach.writes();
        assert!(writes.reservations_committed > 0);
        assert_eq!(writes.reservations_committed, writes.reservations_released);
        assert!(writes.cancels > 0);
        assert_eq!(
            writes.flights_opened,
            writes.flights_closed + writes.cancels
        );
        assert!(writes.queue_served >= writes.queue_enqueued);
    }

    #[test]
    fn a_machine_that_enforces_nothing_writes_no_reservation_flight_or_queue_entry() {
        let trace = decode_trace(3, 1_500);
        let mut mach = engine_machine(4 << 30, MILD, Control::Unified);
        mach.set_arrival_rate(250.0);
        let _ = submit_all(&mut mach, &trace);
        let writes = mach.writes();
        assert_eq!(writes.reservations_committed + writes.flights_opened, 0);
        assert_eq!(
            writes.queue_enqueued + writes.engine_enqueued + writes.cancels,
            0
        );
        assert_eq!(writes.decisions, mach.decisions);
        assert!(writes.flow_graph > 0 && writes.length_observations > 0);
        assert!(writes.owned() >= writes.decisions + writes.dispatches);
    }

    #[test]
    fn the_flow_graph_counts_each_insert_and_each_removal_that_finds_something() {
        let mut mach = engine_machine(4 << 30, MILD, Control::Unified);
        mach.flow_put(7, vec![(0, 10)]);
        mach.flow_put(8, vec![(1, 10)]);
        assert!(mach.flow_take(7).is_some());
        assert!(mach.flow_take(7).is_none());
        mach.forget_flow(8);
        mach.forget_flow(9);
        assert_eq!(mach.writes().flow_graph, 4);
    }

    fn armed_machine() -> Machine {
        let mut mach = queued_machine(MILD, Queue::Slo);
        mach.set_armed(true);
        mach
    }

    fn restart(fate: Fate, client: Client, outage_ns: u64, subscriber: Subscriber) -> Fault {
        Fault::Scheduler(Restart {
            fate,
            client,
            outage_ns,
            subscriber,
            estimators: Estimators::Kept,
            ledger: Rebuild::Now,
        })
    }

    fn submit_until_busy(mach: &mut Machine, trace: &[Request]) -> (Vec<Cost>, usize) {
        let mut costs = Vec::new();
        for (i, req) in trace.iter().enumerate() {
            if let Submitted::Closed(cost) = mach.submit(req) {
                costs.push(cost);
            }
            costs.extend(mach.drain_closed().into_iter().map(|(_, cost)| cost));
            if i > 200 && !mach.flights.is_empty() && !mach.gangs.is_empty() {
                return (costs, i + 1);
            }
        }
        panic!("no moment with a stream and a gang in flight");
    }

    fn submit_until(mach: &mut Machine, trace: &[Request], stop: usize) -> Vec<Cost> {
        let mut costs = Vec::new();
        for req in &trace[..stop] {
            if let Submitted::Closed(cost) = mach.submit(req) {
                costs.push(cost);
            }
            costs.extend(mach.drain_closed().into_iter().map(|(_, cost)| cost));
        }
        costs
    }

    fn drain_rest(mach: &mut Machine, trace: &[Request], from: usize) -> Vec<Cost> {
        let mut costs = Vec::new();
        for req in &trace[from..] {
            if let Submitted::Closed(cost) = mach.submit(req) {
                costs.push(cost);
            }
            costs.extend(mach.drain_closed().into_iter().map(|(_, cost)| cost));
        }
        mach.finish();
        costs.extend(mach.drain_closed().into_iter().map(|(_, cost)| cost));
        costs
    }

    fn sorted_debug(costs: &[Cost]) -> Vec<String> {
        let mut out: Vec<String> = costs.iter().map(|c| format!("{c:?}")).collect();
        out.sort();
        out
    }

    #[test]
    fn registering_every_decode_as_a_flight_changes_no_request() {
        let trace = decode_trace(5, 2_500);
        let mut plain = queued_machine(MILD, Queue::Slo);
        let (plain_costs, _) = submit_all(&mut plain, &trace);
        let mut armed = armed_machine();
        let (armed_costs, opened) = submit_all(&mut armed, &trace);
        assert!(opened > 0, "decodes stay open until they end");
        assert_eq!(sorted_debug(&plain_costs), sorted_debug(&armed_costs));
        assert!(armed.flights.is_empty() && armed.gangs.is_empty());
        let writes = armed.writes();
        assert_eq!(writes.flights_opened, writes.flights_closed);
        assert!(writes.flights_opened > plain.writes().flights_opened);
    }

    #[test]
    fn a_shared_restart_aborts_every_stream_and_gang_and_every_request_still_closes_once() {
        let trace = decode_trace(5, 2_500);
        let mut mach = armed_machine();
        let (mut costs, stop) = submit_until_busy(&mut mach, &trace);
        mach.inject(restart(Fate::Shared, Client::Restart, 0, Subscriber::Warm));
        assert!(mach.flights.is_empty() && mach.gangs.is_empty());
        let stats = mach.fault_stats.clone();
        assert!(stats.streams_aborted > 0 && stats.gangs_aborted > 0);
        assert!(stats.lost_decode_ns > 0);
        let holders: usize = mach.reserved.iter().map(Reservations::holders).sum();
        assert_eq!(holders, 0, "an aborted stream holds no reservation");
        let pinned: u64 = mach
            .domains
            .iter()
            .filter_map(crate::cache::Hierarchy::kv_partition)
            .map(|(_, pinned)| pinned)
            .sum();
        assert_eq!(pinned, 0, "an aborted stream pins no block");
        assert_eq!(mach.engines[0].load(mach.now_ns()), 0);
        costs.extend(drain_rest(&mut mach, &trace, stop));
        assert_eq!(costs.len(), trace.len());
        flush_counts(&mut mach);
        let writes = mach.writes();
        assert_eq!(writes.reservations_committed, writes.reservations_released);
    }

    #[test]
    fn a_held_restart_aborts_nothing_and_a_continuation_keeps_the_tokens_decoded() {
        let trace = decode_trace(5, 2_500);
        let mut held = armed_machine();
        let _ = submit_until(&mut held, &trace, 1_200);
        held.inject(restart(Fate::Held, Client::Restart, 0, Subscriber::Warm));
        assert_eq!(held.fault_stats.streams_aborted, 0);
        assert!(held.fault_stats.held_through > 0);
        assert!(!held.flights.is_empty());

        let mut cont = armed_machine();
        let _ = submit_until(&mut cont, &trace, 1_200);
        cont.inject(restart(Fate::Shared, Client::Continue, 0, Subscriber::Warm));
        assert!(cont.fault_stats.kept_tokens > 0);
    }

    #[test]
    fn the_router_serves_nothing_while_down_and_everything_when_it_is_back() {
        let trace = decode_trace(5, 2_500);
        let mut mach = armed_machine();
        let _ = submit_until(&mut mach, &trace, 1_200);
        let down = 2_000_000_000;
        mach.inject(restart(
            Fate::Shared,
            Client::Restart,
            down,
            Subscriber::Warm,
        ));
        let held = mach.router_queue.len() + mach.retry_gangs.len();
        assert!(held > 0);
        let start = mach.now_ns();
        mach.advance_to(start + down / 2);
        assert!(mach.is_down());
        assert_eq!(mach.router_queue.len() + mach.retry_gangs.len(), held);
        assert!(mach.drain_closed().is_empty());
        mach.advance_to(start + down + 1);
        assert!(!mach.is_down());
        assert!(mach.router_queue.len() + mach.retry_gangs.len() < held);
    }

    #[test]
    fn an_aborted_decode_releases_the_beliefs_pin_on_its_blocks() {
        let trace = single_trace(21, 2_000, 0.5);
        let mut mach = armed_machine();
        mach.set_belief(crate::belief::Conditions::exact());
        let _ = submit_until(&mut mach, &trace, 800);
        let pinned = |m: &Machine| {
            m.observer().map_or(0, |o| {
                (0..4).map(|d| o.belief(d).pinned_blocks()).sum::<usize>()
            })
        };
        assert!(pinned(&mach) > 0);
        mach.inject(restart(Fate::Shared, Client::Restart, 0, Subscriber::Warm));
        assert_eq!(pinned(&mach), 0);
    }

    #[test]
    fn an_estimator_reset_sends_every_claim_back_to_max_tokens() {
        let trace = single_trace(21, 1_500, 0.5);
        let mut mach = armed_machine();
        mach.set_claim(Claim::Quantile {
            q: 0.9,
            pooled: false,
        });
        let _ = submit_until(&mut mach, &trace, 800);
        let probe = trace
            .iter()
            .find(|r| r.tokens > 0)
            .expect("a request that decodes");
        let learned = mach.claimed_tokens(probe).expect("a claim");
        assert!(learned < probe.max_tokens, "{learned} {}", probe.max_tokens);
        mach.inject(Fault::Estimators);
        assert_eq!(mach.claimed_tokens(probe), Some(probe.max_tokens));
        assert_eq!(mach.fault_stats.estimator_resets, 1);
    }

    #[test]
    fn a_length_is_observed_when_its_decode_ends_and_never_if_it_is_aborted() {
        let trace = single_trace(22, 1_500, 0.5);
        let mut dispatch = armed_machine();
        let _ = submit_until(&mut dispatch, &trace, 400);
        let mut completion = armed_machine();
        completion.set_observe(Observe::Completion);
        let _ = submit_until(&mut completion, &trace, 400);
        assert!(
            completion.writes().length_observations < dispatch.writes().length_observations,
            "decodes in flight are not yet observed"
        );
        for mach in [&mut completion, &mut dispatch] {
            flush_counts(mach);
            flush_counts(mach);
        }
        assert_eq!(
            completion.writes().length_observations,
            dispatch.writes().length_observations
        );

        let mut aborted = armed_machine();
        aborted.set_observe(Observe::Completion);
        let _ = submit_until(&mut aborted, &trace, 400);
        let before = aborted.writes().length_observations;
        let in_flight = aborted.flights.len() as u64;
        assert!(in_flight > 0);
        aborted.inject(restart(Fate::Shared, Client::Continue, 0, Subscriber::Warm));
        flush_counts(&mut aborted);
        assert!(aborted.writes().length_observations - before < in_flight);
    }

    #[test]
    fn a_cold_scheduler_does_not_know_residency_it_did_not_dispatch_and_a_warm_one_does() {
        let trace = single_trace(23, 2_000, 0.5);
        let reads = |subscriber: Subscriber| {
            let mut mach = armed_machine();
            let _ = submit_until(&mut mach, &trace, 1_000);
            mach.inject(restart(Fate::Held, Client::Restart, 0, subscriber));
            let _ = drain_rest(&mut mach, &trace, 1_000);
            mach.unknown_reads()
        };
        assert_eq!(reads(Subscriber::Warm), 0);
        assert!(reads(Subscriber::Cold) > 0);
        assert!(
            reads(Subscriber::Snapshot {
                after_ns: 1_000_000_000
            }) > 0
        );
    }

    fn blind_restart(ledger: Rebuild) -> Fault {
        Fault::Scheduler(Restart {
            fate: Fate::Held,
            client: Client::Restart,
            outage_ns: 0,
            subscriber: Subscriber::Warm,
            estimators: Estimators::Kept,
            ledger,
        })
    }

    const BLIND_PARTITION: u64 = 200 << 20;

    fn tight_machine(node_check: bool) -> Machine {
        let mut mach = queued_machine(BLIND_PARTITION, Queue::Slo);
        mach.set_armed(true);
        mach.set_node_check(node_check);
        mach
    }

    fn fill_real_ledgers(mach: &mut Machine, until_ns: u64) {
        for d in 0..mach.domains.len() {
            if let Some((capacity, _)) = mach.domains[d].kv_partition() {
                mach.reserved[d].commit(&[], capacity, until_ns);
            }
        }
    }

    fn first_decode(trace: &[Request]) -> &Request {
        trace
            .iter()
            .find(|r| r.tokens > 0 && r.chain.iter().any(|(_, m)| m.kind == BlobKind::KvBlock))
            .expect("a decode with a KV chain")
    }

    #[test]
    fn a_blind_ledger_admits_what_the_real_one_would_refuse_and_a_node_check_refuses_it() {
        let trace = decode_trace(7, 3_000);
        let mut mach = tight_machine(true);
        let _ = submit_until(&mut mach, &trace, 300);
        mach.inject(blind_restart(Rebuild::Never));
        assert!(mach.fault_stats.hidden_holders > 0);
        fill_real_ledgers(&mut mach, u64::MAX);
        let req = first_decode(&trace);
        assert!(
            mach.router_admits(0, req, false),
            "the blind ledger has room"
        );
        assert!(!mach.node_admits(0, req), "the node's own ledger has none");
        let end = mach.now_ns() + 1_000_000;
        let _ = mach.hold_claim(0, req, Some(end));
        assert_eq!(mach.fault_stats.blind_admissions, 1);
        assert_eq!(mach.fault_stats.over_admissions, 1);

        let mut rebuilt = tight_machine(true);
        let _ = submit_until(&mut rebuilt, &trace, 300);
        rebuilt.inject(blind_restart(Rebuild::Now));
        fill_real_ledgers(&mut rebuilt, u64::MAX);
        assert!(
            !rebuilt.router_admits(0, req, false),
            "a rebuilt ledger sees it"
        );
    }

    #[test]
    fn a_refused_dispatch_returns_to_the_router_and_every_request_still_closes_once() {
        let trace = decode_trace(7, 3_000);
        let mut mach = tight_machine(true);
        let mut costs = submit_until(&mut mach, &trace, 300);
        mach.inject(blind_restart(Rebuild::Never));
        let until = mach.now_ns() + 200_000_000;
        fill_real_ledgers(&mut mach, until);
        costs.extend(drain_rest(&mut mach, &trace, 300));
        assert!(mach.fault_stats.refused_at_node > 0);
        assert_eq!(costs.len(), trace.len());
        assert!(mach.router_queue.is_empty() && mach.flights.is_empty());
    }

    #[test]
    fn the_blind_view_ends_when_the_pre_restart_decodes_have_ended_or_at_its_deadline() {
        let trace = decode_trace(7, 3_000);
        let mut mach = tight_machine(false);
        let _ = submit_until(&mut mach, &trace, 1_000);
        mach.inject(blind_restart(Rebuild::Never));
        assert!(mach.shadow.is_some());
        flush_counts(&mut mach);
        flush_counts(&mut mach);
        assert!(mach.shadow.is_none());
        assert!(mach.flights.iter().all(|f| !f.pre_crash));

        let mut timed = tight_machine(false);
        let _ = submit_until(&mut timed, &trace, 1_000);
        timed.inject(blind_restart(Rebuild::After { after_ns: 1 }));
        let now = timed.now_ns();
        timed.advance_to(now + 10);
        assert!(timed.shadow.is_none());
    }

    #[test]
    fn a_snapshot_is_written_on_its_cadence_and_a_restart_restores_what_it_held() {
        let trace = single_trace(21, 1_500, 0.5);
        let mut mach = armed_machine();
        mach.set_claim(Claim::Quantile {
            q: 0.9,
            pooled: false,
        });
        mach.set_snapshot_every(Some(500_000_000));
        let _ = submit_until(&mut mach, &trace, 800);
        let written = mach.writes();
        assert!(written.snapshots >= 2 && written.snapshot_bytes > 0);
        let probe = trace.iter().find(|r| r.tokens > 0).expect("a decode");
        let learned = mach.claimed_tokens(probe).expect("a claim");
        mach.inject(Fault::Scheduler(Restart {
            fate: Fate::Held,
            client: Client::Restart,
            outage_ns: 0,
            subscriber: Subscriber::Warm,
            estimators: Estimators::Snapshot,
            ledger: Rebuild::Now,
        }));
        assert_eq!(mach.fault_stats.snapshot_restores, 1);
        assert!(mach.fault_stats.snapshot_age_ns <= 500_000_000 + 4_000_000);
        let restored = mach.claimed_tokens(probe).expect("a claim");
        assert!(restored < probe.max_tokens && restored.abs_diff(learned) <= learned / 2);
        mach.inject(Fault::Scheduler(Restart {
            fate: Fate::Held,
            client: Client::Restart,
            outage_ns: 0,
            subscriber: Subscriber::Warm,
            estimators: Estimators::Lost,
            ledger: Rebuild::Now,
        }));
        assert_eq!(mach.claimed_tokens(probe), Some(probe.max_tokens));
    }

    #[test]
    fn taking_snapshots_changes_no_cost() {
        let trace = single_trace(21, 1_500, 0.5);
        let mut plain = armed_machine();
        let (plain_costs, _) = submit_all(&mut plain, &trace);
        let mut snapped = armed_machine();
        snapped.set_snapshot_every(Some(250_000_000));
        let (snapped_costs, _) = submit_all(&mut snapped, &trace);
        assert_eq!(sorted_debug(&plain_costs), sorted_debug(&snapped_costs));
        assert_eq!(plain.writes().snapshots, 0);
        assert!(snapped.writes().snapshots > 0);
    }

    #[test]
    fn a_snapshot_counts_the_bytes_of_everything_a_restart_restores() {
        let mut mach = armed_machine();
        mach.take_snapshot();
        let bare = mach.writes().snapshot_bytes;
        assert!(bare > 0);
        mach.attained.insert(1, 900);
        mach.attained.insert(2, 5);
        mach.learner.seen.insert(BlobId::leaf(b"f"), (3, 1));
        mach.take_snapshot();
        let grown = (2 * size_of::<(u64, u64)>() + size_of::<(BlobId, (u64, u64))>()) as u64;
        assert_eq!(mach.writes().snapshot_bytes, 2 * bare + grown);
    }

    fn crash(node: usize, restart_ns: u64, spill: Spill, client: Client) -> Fault {
        Fault::Engine(EngineCrash {
            node,
            restart_ns,
            spill,
            client,
        })
    }

    fn lose(node: usize, declare_ns: u64, client: Client) -> Fault {
        Fault::Node(NodeLoss {
            node,
            declare_ns,
            client,
        })
    }

    #[test]
    fn an_engine_crash_takes_one_nodes_streams_and_kv_and_its_decoding_until_it_restarts() {
        let trace = decode_trace(5, 2_500);
        let mut mach = armed_machine();
        let (mut costs, stop) = submit_until_busy(&mut mach, &trace);
        let node = mach.flights.first().map(|f| f.node).expect("a stream");
        let elsewhere = mach.flights.iter().filter(|f| f.node != node).count();
        let spilled = mach.domains[node].kv_ids(Medium::Storage);
        let restart_ns = 5_000_000_000;
        mach.inject(crash(node, restart_ns, Spill::Kept, Client::Continue));
        assert_eq!(mach.flights.len(), elsewhere);
        assert!(mach.flights.iter().all(|f| f.node != node));
        assert!(
            mach.gangs
                .iter()
                .all(|g| g.parts.iter().all(|p| p.node != node))
        );
        assert_eq!(mach.reserved[node].holders(), 0);
        assert_eq!(mach.domains[node].kv_ids(Medium::Gpu).len(), 0);
        assert_eq!(mach.domains[node].kv_ids(Medium::Cpu).len(), 0);
        assert_eq!(mach.domains[node].kv_ids(Medium::Storage), spilled);
        assert!(mach.fault_stats.kv_lost > 0);
        assert!(!mach.decodes_now(node) && !mach.decode_pool().contains(&node));
        assert!(mach.active.contains(&node), "its host work runs on");
        let now = mach.now_ns();
        mach.advance_to(now + restart_ns + 1);
        assert!(mach.decodes_now(node));
        costs.extend(drain_rest(&mut mach, &trace, stop));
        assert_eq!(costs.len(), trace.len());
    }

    #[test]
    fn a_crash_that_loses_the_spill_loses_every_tier_and_one_that_keeps_it_loses_two() {
        let trace = decode_trace(5, 2_500);
        let lost = |spill: Spill| {
            let mut mach = armed_machine();
            let _ = submit_until(&mut mach, &trace, 1_800);
            let node = 0;
            let before = mach.domains[node].kv_ids(Medium::Storage).len();
            mach.inject(crash(node, 1, spill, Client::Restart));
            (
                before,
                mach.domains[node].kv_ids(Medium::Storage).len(),
                mach.fault_stats.kv_lost,
            )
        };
        let (before, kept_after, kept_lost) = lost(Spill::Kept);
        let (_, lost_after, lost_lost) = lost(Spill::Lost);
        assert_eq!(kept_after, before);
        assert_eq!(lost_after, 0);
        assert_eq!(lost_lost, kept_lost + before as u64);
    }

    #[test]
    fn a_lost_node_parks_what_is_placed_on_it_until_it_is_declared_and_then_leaves_placement() {
        let trace = decode_trace(5, 3_000);
        let mut mach = armed_machine();
        let mut costs = submit_until(&mut mach, &trace, 600);
        let declare_ns = 3_000_000_000;
        mach.inject(lose(0, declare_ns, Client::Continue));
        assert!(
            mach.active.contains(&0),
            "still placed on until it is declared"
        );
        assert_eq!(mach.domains[0].kv_ids(Medium::Gpu).len(), 0);
        costs.extend(drain_rest(&mut mach, &trace, 600));
        assert_eq!(costs.len(), trace.len(), "every request closes once");
        assert!(
            mach.fault_stats.limbo_requests > 0,
            "{:?}",
            mach.fault_stats
        );
        assert!(mach.fault_stats.limbo_wait_ns > 0);
        assert!(!mach.active.contains(&0));
        assert!(mach.limbo.is_empty());
    }

    #[test]
    fn a_node_declared_at_once_is_never_parked_on() {
        let trace = decode_trace(5, 3_000);
        let mut mach = armed_machine();
        let mut costs = submit_until(&mut mach, &trace, 600);
        mach.inject(lose(0, 0, Client::Continue));
        costs.extend(drain_rest(&mut mach, &trace, 600));
        assert_eq!(costs.len(), trace.len());
        assert_eq!(mach.fault_stats.limbo_requests, 0);
        assert!(!mach.active.contains(&0));
    }

    #[test]
    fn losing_a_node_counts_its_durable_cells_and_a_copy_made_when_they_were_marked_saves_them() {
        let cell = (
            BlobId::leaf(b"sandbox"),
            BlobMeta {
                kind: BlobKind::Snapshot,
                bytes: 32 << 20,
                parent: None,
                recompute_ns: 1,
            },
        );
        for copy in [false, true] {
            let mut mach = armed_machine();
            mach.set_copy_durable(copy);
            let _ = mach.domains[1].access(&[cell]);
            mach.mark_durable(cell.0, cell.1.bytes);
            mach.inject(lose(1, 0, Client::Restart));
            let stats = &mach.fault_stats;
            assert_eq!(stats.durable_lost_with_node, u64::from(!copy));
            assert_eq!(stats.durable_saved, u64::from(copy));
            assert_eq!(stats.durable_copied_bytes, if copy { 32 << 20 } else { 0 });
            assert_eq!(mach.durable_lost(), 0, "a lost node is not a dropped cell");
        }
    }

    #[test]
    fn a_degraded_router_places_by_hash_for_its_window_and_aborts_nothing() {
        let trace = decode_trace(5, 2_500);
        let mut mach = armed_machine();
        let before = (mach.placement, mach.flow_aware, mach.state_transfer);
        let mut costs = submit_until(&mut mach, &trace, 800);
        let streams = mach.flights.len();
        mach.inject(Fault::Degrade(Degrade {
            for_ns: 2_000_000_000,
        }));
        assert_eq!(mach.placement, Placement::Sticky);
        assert!(!mach.flow_aware && !mach.state_transfer);
        assert_eq!(mach.flights.len(), streams);
        assert_eq!(mach.fault_stats.streams_aborted, 0);
        let now = mach.now_ns();
        mach.advance_to(now + 2_000_000_001);
        assert_eq!(
            (mach.placement, mach.flow_aware, mach.state_transfer),
            before
        );
        costs.extend(drain_rest(&mut mach, &trace, 800));
        assert_eq!(costs.len(), trace.len());
    }

    fn region_trace(regions: usize, ops: u64) -> Vec<Request> {
        Workload::with_fanout(2, ops, 0.0, 0.1)
            .with_regions(Some(crate::work::RegionDemand {
                count: regions,
                shape: crate::work::RegionShape::Even,
            }))
            .collect()
    }

    fn regional_machine(per_region: usize, regions: usize, mode: Option<RegionMode>) -> Machine {
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
        let one_way: Vec<Vec<u64>> = (0..regions)
            .map(|a| {
                (0..regions)
                    .map(|b| if a == b { 0 } else { 30_000_000 })
                    .collect()
            })
            .collect();
        let topo = Topology::regions(
            per_region,
            1,
            mem.ddr,
            Distance::Rack,
            &one_way,
            Crossing::default(),
        );
        let mut m = Machine::new(topo, |_| mem, Policy::Gdsf, Placement::Scored);
        m.set_flow_aware(true);
        m.set_state_transfer(true);
        m.set_fanout_atomic(true);
        m.set_regions(mode.map(|mode| Regions::new(per_region, one_way, mode)));
        m
    }

    fn serve_all(m: &mut Machine, trace: &[Request]) -> u64 {
        trace
            .iter()
            .map(|req| match m.submit(req) {
                Submitted::Closed(c) => c.service_ns(),
                Submitted::Open(_) => 0,
            })
            .sum()
    }

    fn away(m: &Machine) -> u64 {
        m.regions().map_or(0, |r| r.stats.away.iter().sum())
    }

    fn facing(m: &Machine) -> u64 {
        m.regions().map_or(0, |r| r.stats.facing.iter().sum())
    }

    #[test]
    fn a_regional_scheduler_serves_every_client_facing_request_in_its_clients_region() {
        let trace = region_trace(3, 3_000);
        let mut m = regional_machine(2, 3, Some(RegionMode::Regional));
        serve_all(&mut m, &trace);
        assert!(facing(&m) > 1_000);
        assert_eq!(away(&m), 0);
        let stats = &m.regions().expect("regions are on").stats;
        for region in 0..3 {
            let placed: u64 = (region * 2..region * 2 + 2)
                .map(|d| stats.dispatched[d])
                .sum();
            assert!(placed > 200, "region {region} placed {placed}");
        }
    }

    #[test]
    fn the_global_argmin_charges_the_round_trip_once_for_each_request_it_serves_away() {
        let trace = region_trace(3, 3_000);
        let mut unpriced = regional_machine(2, 3, Some(RegionMode::Global));
        unpriced
            .regions
            .as_mut()
            .expect("regions are on")
            .price_reach = false;
        let mut priced = regional_machine(2, 3, Some(RegionMode::Global));
        serve_all(&mut unpriced, &trace);
        serve_all(&mut priced, &trace);
        let stats = &unpriced.regions().expect("regions are on").stats;
        assert!(stats.reach_hops > 0 && stats.reach_hops == away(&unpriced));
        let hop = unpriced
            .regions()
            .expect("regions are on")
            .round_trip_ns(0, 1);
        assert_eq!(stats.reach_ns, stats.reach_hops * hop);
        assert!(
            away(&priced) < away(&unpriced),
            "{} {}",
            away(&priced),
            away(&unpriced)
        );
    }

    #[test]
    fn one_region_with_every_region_bit_on_changes_no_request() {
        let trace: Vec<Request> = Workload::with_fanout(2, 2_000, 0.0, 0.1).collect();
        let mut off = regional_machine(4, 1, None);
        let reference = serve_all(&mut off, &trace);
        for mode in [RegionMode::Global, RegionMode::Regional] {
            let mut on = regional_machine(4, 1, Some(mode));
            assert_eq!(serve_all(&mut on, &trace), reference);
            assert_eq!(away(&on), 0);
            assert_eq!(on.decisions, off.decisions);
        }
    }

    #[test]
    fn a_region_with_no_node_for_a_request_falls_back_to_every_node() {
        let trace = region_trace(2, 500);
        let mut m = regional_machine(1, 2, Some(RegionMode::Regional));
        m.drain(1);
        let unplaced = trace
            .iter()
            .filter(|req| matches!(m.submit(req), Submitted::Closed(c) if c.pending))
            .count();
        assert_eq!(unplaced, 0);
        assert!(facing(&m) > 100);
        assert!(
            trace
                .iter()
                .any(|r| r.region == 1 && !Machine::needs_decode(r))
        );
    }

    #[test]
    fn a_lease_moves_no_share_until_it_has_seen_a_lease_of_demand() {
        let mut shares =
            TenantShares::new(HashMap::from([(1, 1_000.0)]), 3, 2.0, Some(500_000_000));
        assert!(shares.admit(1, 0, 100, 1_000));
        assert_eq!(shares.shares(), &[1.0 / 3.0; 3]);
        assert!(shares.admit(1, 1, 100, 600_000_000));
        assert_eq!(shares.refreshes, 1);
        assert!((shares.shares()[0] - shares.shares()[1]).abs() < 1e-9);
        assert!(shares.shares()[2] < shares.shares()[0]);
    }

    fn hot_trace(share: f64, ops: u64) -> Vec<Request> {
        Workload::with_fanout(2, ops, 0.0, 0.0)
            .with_regions(Some(crate::work::RegionDemand {
                count: 2,
                shape: crate::work::RegionShape::Skew {
                    region: 0,
                    share,
                    from: 0.0,
                    to: 1.0,
                },
            }))
            .collect()
    }

    fn hot_machine(overflow: Overflow, summary_ns: u64, own_forwards: bool) -> Machine {
        hot_machine_at(1_500.0, overflow, summary_ns, own_forwards)
    }

    fn hot_machine_at(
        rate: f64,
        overflow: Overflow,
        summary_ns: u64,
        own_forwards: bool,
    ) -> Machine {
        let mut m = regional_machine(2, 2, Some(RegionMode::Regional));
        m.set_arrival_rate(rate);
        let regions = m.regions.as_mut().expect("regions are on");
        regions.overflow = overflow;
        regions.summary_ns = summary_ns;
        regions.own_forwards = own_forwards;
        m
    }

    #[test]
    fn a_region_that_cannot_serve_its_demand_forwards_it_and_the_whole_run_is_faster() {
        let trace = hot_trace(0.9, 4_000);
        let mut alone = hot_machine(Overflow::Off, 0, false);
        let alone_ns = serve_all(&mut alone, &trace);
        assert_eq!(away(&alone), 0);
        for overflow in [Overflow::Node, Overflow::RegionMean] {
            let mut m = hot_machine(overflow, 0, false);
            let ns = serve_all(&mut m, &trace);
            assert!(away(&m) > 100, "{overflow:?} forwarded {}", away(&m));
            assert!(ns < alone_ns, "{overflow:?}: {ns} against {alone_ns}");
        }
    }

    #[test]
    fn overflow_forwards_only_what_a_client_sent_and_a_decode_needs() {
        let trace = hot_trace(0.9, 4_000);
        let mut m = hot_machine(Overflow::Node, 0, false);
        serve_all(&mut m, &trace);
        let stats = &m.regions().expect("regions are on").stats;
        assert_eq!(stats.away[BlobKind::Snapshot.idx()], 0);
        assert_eq!(stats.away[BlobKind::ServiceHeap.idx()], 0);
        assert!(stats.away[BlobKind::KvBlock.idx()] > 0);
    }

    #[test]
    fn a_threshold_forwards_only_above_its_utilisation() {
        let trace = hot_trace(0.9, 4_000);
        let at = |utilisation| hot_machine_at(600.0, Overflow::Threshold { utilisation }, 0, false);
        let mut never = at(2.0);
        serve_all(&mut never, &trace);
        assert_eq!(away(&never), 0);
        let mut low = at(0.6);
        serve_all(&mut low, &trace);
        assert!(away(&low) > 100, "{}", away(&low));
    }

    #[test]
    fn a_sender_counts_what_it_forwarded_since_the_summary_it_holds() {
        let trace = hot_trace(0.9, 4_000);
        let mut m = hot_machine(Overflow::RegionMean, 5_000_000_000, true);
        serve_all(&mut m, &trace);
        let regions = m.regions.as_ref().expect("regions are on");
        assert_ne!(regions.forwards[0][1].len(), 0);
        assert!(m.forwarded_delta(0, 1) > 0.0);
        assert!(m.forwarded_delta(0, 1) > m.forwarded_delta(1, 0));
        let mut blind = hot_machine(Overflow::RegionMean, 5_000_000_000, false);
        serve_all(&mut blind, &trace);
        assert!(blind.forwarded_delta(0, 1).abs() < f64::EPSILON);
    }

    #[test]
    fn a_summary_is_a_snapshot_taken_on_its_clock() {
        let mut m = hot_machine(Overflow::Node, 1_000_000_000, false);
        serve_all(&mut m, &hot_trace(0.9, 3_000));
        let regions = m.regions.as_ref().expect("regions are on");
        assert!(regions.next_summary_ns > 0);
        assert!(regions.summary.iter().any(|&n| n > 0));
        assert!(regions.next_summary_ns <= m.now_ns() + regions.summary_ns);
    }

    fn regional_fleet(placement: &[Option<Model>]) -> Machine {
        let bands = [0u8; BlobKind::N];
        let node = |_: usize| NodeMemory {
            hbm: 4 << 30,
            ddr: 8 << 30,
            nvme: 64 << 30,
            hbm_quota: Quota::open(4 << 30, bands),
            ddr_quota: Quota::open(8 << 30, bands),
            can_decode: true,
            kv: Some(crate::cache::EngineKv {
                partition: 3 << 30,
                offload: 1 << 30,
                spill: 8 << 30,
                clairvoyant: false,
            }),
        };
        let one_way = vec![vec![0, 30_000_000], vec![30_000_000, 0]];
        let topo = Topology::regions(2, 1, 8 << 30, Distance::Rack, &one_way, Crossing::default());
        let mut m = Machine::new(topo, node, Policy::Gdsf, Placement::Scored);
        m.set_flow_aware(true);
        m.set_state_transfer(true);
        m.set_model_batches(Batching::PerModel, true);
        m.set_fleet(
            crate::fleet::Fleet::new(crate::fleet::Catalogue::published(8_000_000_000), placement),
            None,
        );
        m.set_regions(Some(Regions::new(2, one_way, RegionMode::Regional)));
        m
    }

    #[test]
    fn a_request_for_a_model_its_region_lacks_is_forwarded_to_the_nearest_region_with_one() {
        let trace: Vec<Request> = Workload::with_fanout(2, 1_500, 1.0, 0.1)
            .with_decode_kv(crate::work::TOKENS_PER_KV_BLOCK)
            .with_model_keyed(true)
            .with_regions(Some(crate::work::RegionDemand {
                count: 2,
                shape: crate::work::RegionShape::Even,
            }))
            .collect();
        let placement = [Some(0), Some(1), Some(2), Some(3)];
        let mut m = regional_fleet(&placement);
        m.set_arrival_rate(250.0);
        served_costs(&mut m, &trace);
        let stats = &m.regions().expect("regions are on").stats;
        assert!(stats.forced > 100, "{}", stats.forced);
        assert!(away(&m) > 0);
        let fleet = m.fleet().expect("a fleet");
        assert_eq!(fleet.stats.unplaced, 0);
        for (d, row) in fleet.stats.served.iter().enumerate() {
            for (model, n) in row.iter().enumerate() {
                if Some(model as Model) != placement[d] {
                    assert_eq!(*n, 0, "node {d} decoded model {model}");
                }
            }
        }
    }

    #[test]
    fn a_request_that_waits_at_the_router_is_served_in_its_own_region() {
        let bands = [0u8; BlobKind::N];
        let mem = NodeMemory {
            hbm: 4 << 30,
            ddr: 8 << 30,
            nvme: 64 << 30,
            hbm_quota: Quota::open(4 << 30, bands),
            ddr_quota: Quota::open(8 << 30, bands),
            can_decode: true,
            kv: Some(crate::cache::EngineKv {
                partition: TIGHT,
                offload: 1 << 30,
                spill: 8 << 30,
                clairvoyant: false,
            }),
        };
        let one_way = vec![vec![0, 30_000_000], vec![30_000_000, 0]];
        let topo = Topology::regions(2, 1, mem.ddr, Distance::Rack, &one_way, Crossing::default());
        let mut m = Machine::new(topo, |_| mem, Policy::Gdsf, Placement::Scored);
        m.set_hold_decodes(true);
        m.set_arrival_rate(250.0);
        m.set_admission(Reserve::Perfect, crate::work::TOKENS_PER_KV_BLOCK);
        m.set_queue(Queue::Fifo);
        m.set_regions(Some(Regions::new(2, one_way, RegionMode::Regional)));
        let trace: Vec<Request> = Workload::with_fanout(9, 3_000, 1.0, 0.0)
            .with_decode_kv(crate::work::TOKENS_PER_KV_BLOCK)
            .with_throughput(0.3)
            .with_regions(Some(crate::work::RegionDemand {
                count: 2,
                shape: crate::work::RegionShape::Even,
            }))
            .collect();
        let (costs, opened) = submit_all(&mut m, &trace);
        assert!(
            opened > 0,
            "an overcommitted partition must queue something"
        );
        assert_eq!(costs.len(), trace.len());
        assert!(facing(&m) > 1_000);
        assert_eq!(away(&m), 0);
    }

    #[test]
    fn a_fan_outs_agents_are_never_counted_as_forwarded_clients() {
        let trace: Vec<Request> = Workload::with_fanout(2, 3_000, 0.0, 0.3)
            .with_regions(Some(crate::work::RegionDemand {
                count: 2,
                shape: crate::work::RegionShape::Even,
            }))
            .collect();
        assert!(trace.iter().any(|r| r.gang.is_some()));
        let mut m = hot_machine_at(1_500.0, Overflow::Off, 1_000_000_000, true);
        serve_all(&mut m, &trace);
        let regions = m.regions.as_ref().expect("regions are on");
        assert_eq!(away(&m), 0);
        assert!(regions.forwards.iter().flatten().all(Vec::is_empty));
    }

    fn table_machine(epoch_ns: u64) -> Machine {
        let mut m = hot_machine_at(600.0, Overflow::Off, 0, false);
        m.regions
            .as_mut()
            .expect("regions are on")
            .set_table(Some(epoch_ns));
        m
    }

    #[test]
    fn a_table_moves_almost_nothing_when_no_region_is_worse_than_another() {
        let trace = region_trace(2, 4_000);
        let mut m = table_machine(1_000_000_000);
        m.set_arrival_rate(400.0);
        serve_all(&mut m, &trace);
        let regions = m.regions().expect("regions are on");
        assert!(regions.table_stats.recomputes > 2);
        assert!(away(&m) * 50 < facing(&m), "{} of {}", away(&m), facing(&m));
        for (a, row) in regions
            .table_fractions()
            .expect("a table")
            .iter()
            .enumerate()
        {
            assert!(row[a] > 0.8, "{row:?}");
        }
    }

    #[test]
    fn a_table_forwards_a_hot_regions_demand_by_session_and_each_row_sums_to_one() {
        let trace = hot_trace(0.9, 4_000);
        let mut m = table_machine(1_000_000_000);
        serve_all(&mut m, &trace);
        let regions = m.regions().expect("regions are on");
        let table = regions.table_fractions().expect("a table");
        assert!(table[0][1] > 0.1, "{table:?}");
        for row in table {
            assert!((row.iter().sum::<f64>() - 1.0).abs() < 1e-9);
        }
        assert!(away(&m) > 100);
        assert!(regions.table_stats.changes > 0);
    }

    #[test]
    fn routing_demand_keeps_a_balanced_fleet_home_and_moves_a_hot_regions_excess() {
        let costs = crate::fleet::Costs::published(1_000_000_000);
        let trips = vec![vec![0.0, 6e7], vec![6e7, 0.0]];
        let even = route_demand(&costs, &[2_000.0, 2_000.0], &[124.0; 2], &[4, 4], &trips);
        assert_eq!(even, vec![vec![1.0, 0.0], vec![0.0, 1.0]]);
        let hot = route_demand(&costs, &[40_000.0, 1_000.0], &[124.0; 2], &[4, 4], &trips);
        assert!(hot[0][1] > 0.2 && hot[1][1] > 0.99, "{hot:?}");
    }

    fn budget_machine(rule: BudgetRule, running: usize, load_ns: u64) -> Machine {
        let mut m = regional_machine(3, 2, Some(RegionMode::Regional));
        m.set_arrival_rate(600.0);
        m.set_budgets(Some(Budgets::new(rule, 2, 3, running, load_ns)));
        m
    }

    #[test]
    fn a_node_that_is_not_running_takes_no_work() {
        let mut m = budget_machine(BudgetRule::Static, 2, 0);
        serve_all(&mut m, &region_trace(2, 3_000));
        let placed = &m.regions().expect("regions are on").stats.dispatched;
        assert_eq!(placed[2], 0);
        assert_eq!(placed[5], 0);
        assert!(placed[0] > 100 && placed[1] > 100 && placed[3] > 100 && placed[4] > 100);
        assert_eq!(m.running_counts(), vec![2, 2]);
    }

    #[test]
    fn a_planned_move_releases_a_node_and_the_new_one_serves_only_after_its_load() {
        let plan = BudgetRule::Planned(vec![(1_000_000_000, vec![3, 1])]);
        let mut m = budget_machine(plan, 2, 500_000_000);
        m.advance_to(900_000_000);
        assert_eq!(m.running_counts(), vec![2, 2]);
        m.advance_to(1_100_000_000);
        assert_eq!(m.running_counts(), vec![3, 1]);
        assert!(!m.available(2), "the acquired node is still loading");
        assert!(
            !m.available(4) && !m.available(5),
            "the released node and the idle slot are out"
        );
        m.advance_to(1_600_000_000);
        assert!(m.available(2));
        let budgets = m.budgets().expect("budgets are on");
        assert_eq!(budgets.moves, 1);
        assert_eq!(budgets.loading_ns, 500_000_000);
    }

    #[test]
    fn budgets_that_follow_demand_move_nodes_to_the_hot_region_and_not_when_demand_is_even() {
        let follow = || BudgetRule::Follow {
            interval_ns: 1_000_000_000,
        };
        let mut hot = budget_machine(follow(), 2, 100_000_000);
        serve_all(&mut hot, &hot_trace(0.9, 4_000));
        let counts = hot.running_counts();
        assert_eq!(counts.iter().sum::<usize>(), 4);
        assert!(counts[0] > counts[1], "{counts:?}");
        assert!(hot.budgets().expect("budgets are on").moves > 0);
        let mut even = budget_machine(follow(), 2, 100_000_000);
        serve_all(&mut even, &region_trace(2, 4_000));
        assert_eq!(even.budgets().expect("budgets are on").moves, 0);
        assert_eq!(even.running_counts(), vec![2, 2]);
    }

    fn sharded(k: usize, report_ns: u64) -> Machine {
        let mut m = regional_machine(4, 1, Some(RegionMode::Regional));
        m.set_arrival_rate(500.0);
        m.set_shards(k, report_ns);
        m
    }

    #[test]
    fn schedulers_that_see_each_others_decodes_exactly_change_no_request() {
        let trace = region_trace(1, 4_000);
        let one = serve_all(&mut sharded(1, 0), &trace);
        for k in [2, 4] {
            assert_eq!(serve_all(&mut sharded(k, 0), &trace), one, "{k} schedulers");
        }
    }

    #[test]
    fn schedulers_that_report_seconds_late_herd_and_more_schedulers_herd_more() {
        let trace = region_trace(1, 6_000);
        let one = serve_all(&mut sharded(1, 0), &trace);
        let two = serve_all(&mut sharded(2, 5_000_000_000), &trace);
        let four = serve_all(&mut sharded(4, 5_000_000_000), &trace);
        assert!(four > one && four >= two, "{one} {two} {four}");
        let fast = serve_all(&mut sharded(4, 25_000_000), &trace);
        assert!(fast < four);
    }

    fn day_trace(ops: u64) -> Vec<Request> {
        Workload::with_fanout(2, ops, 0.0, 0.1)
            .with_regions(Some(crate::work::RegionDemand {
                count: 2,
                shape: crate::work::RegionShape::Sun {
                    amplitude: 0.75,
                    peaks: vec![0.75, 0.25],
                    days: 1.0,
                },
            }))
            .collect()
    }

    fn tenant_quotas(trace: &[Request], seconds: f64, headroom: f64) -> HashMap<u32, f64> {
        let mut quotas: HashMap<u32, f64> = HashMap::new();
        for r in trace.iter().filter(|r| r.client_facing() && r.tokens > 0) {
            if let Some(t) = r.tenant {
                *quotas.entry(t).or_insert(0.0) += r.tokens as f64 / seconds;
            }
        }
        for quota in quotas.values_mut() {
            *quota *= 1.0 + headroom;
        }
        quotas
    }

    fn shared_tenants(trace: &[Request], headroom: f64, lease_ns: Option<u64>) -> Machine {
        let mut m = regional_machine(2, 2, Some(RegionMode::Regional));
        m.set_arrival_rate(400.0);
        let quotas = tenant_quotas(trace, trace.len() as f64 / 400.0, headroom);
        m.set_tenant_shares(Some(TenantShares::new(quotas, 2, 2.0, lease_ns)));
        m
    }

    #[test]
    fn a_leased_share_follows_the_day_and_a_static_split_refuses_at_the_peaks() {
        let day = day_trace(8_000);
        let even = region_trace(2, 8_000);
        let refused = |trace: &[Request], headroom, lease| {
            let mut m = shared_tenants(trace, headroom, lease);
            serve_all(&mut m, trace);
            let t = m.tenant_shares().expect("shares are on");
            (t.refused, t.offered, t.refreshes)
        };
        let (generous, offered, _) = refused(&day, 20.0, None);
        assert!(offered > 1_000);
        assert_eq!(generous, 0);
        let lease = Some(500_000_000);
        let (fixed_day, _, _) = refused(&day, 0.1, None);
        let (fixed_even, _, _) = refused(&even, 0.1, None);
        let (leased_day, _, refreshes) = refused(&day, 0.1, lease);
        let (leased_even, _, _) = refused(&even, 0.1, lease);
        assert!(refreshes > 5);
        let by_the_day = |day: u64, even: u64| day.saturating_sub(even);
        assert!(
            by_the_day(fixed_day, fixed_even) > 2 * by_the_day(leased_day, leased_even).max(1),
            "static {fixed_day} against {fixed_even}, leased {leased_day} against {leased_even}"
        );
    }

    #[test]
    fn a_lease_moves_a_regions_share_toward_its_demand() {
        let trace = hot_trace(0.9, 6_000);
        let mut m = shared_tenants(&trace, 1.0, Some(500_000_000));
        serve_all(&mut m, &trace);
        let shares = m.tenant_shares().expect("shares are on").shares();
        assert!(
            shares[0] > 0.7 && (shares.iter().sum::<f64>() - 1.0).abs() < 1e-9,
            "{shares:?}"
        );
    }

    fn confined_machine(residency: f64, overflow: Overflow) -> Machine {
        let mut m = hot_machine(overflow, 0, false);
        m.set_residency(residency);
        m
    }

    #[test]
    fn full_residency_makes_every_overflow_rule_regional() {
        let trace = hot_trace(0.9, 4_000);
        let regional = serve_all(&mut hot_machine(Overflow::Off, 0, false), &trace);
        for overflow in [Overflow::Node, Overflow::RegionMean] {
            let mut m = confined_machine(1.0, overflow);
            assert_eq!(serve_all(&mut m, &trace), regional);
            assert_eq!(away(&m), 0);
        }
        let mut table = confined_machine(1.0, Overflow::Off);
        table
            .regions
            .as_mut()
            .expect("regions are on")
            .set_table(Some(1_000_000_000));
        assert_eq!(serve_all(&mut table, &trace), regional);
        assert_eq!(away(&table), 0);
    }

    #[test]
    fn residency_keeps_a_share_of_tenants_home_and_the_rest_may_leave() {
        let trace = hot_trace(0.9, 4_000);
        let open = {
            let mut m = confined_machine(0.0, Overflow::Node);
            serve_all(&mut m, &trace);
            away(&m)
        };
        let half = {
            let mut m = confined_machine(0.5, Overflow::Node);
            serve_all(&mut m, &trace);
            away(&m)
        };
        assert!(half > 0 && half < open, "{half} {open}");
        let tenants: Vec<u32> = (0..24).collect();
        let m = confined_machine(0.5, Overflow::Off);
        let kept = tenants.iter().filter(|&&t| m.restricted(t)).count();
        assert!((6..=18).contains(&kept), "{kept}");
    }

    #[test]
    fn a_confined_request_whose_model_is_not_in_its_region_goes_unplaced_and_never_leaves() {
        let trace: Vec<Request> = Workload::with_fanout(2, 1_500, 1.0, 0.0)
            .with_decode_kv(crate::work::TOKENS_PER_KV_BLOCK)
            .with_model_keyed(true)
            .with_regions(Some(crate::work::RegionDemand {
                count: 2,
                shape: crate::work::RegionShape::Even,
            }))
            .collect();
        let placement = [Some(0), Some(1), Some(2), Some(3)];
        let mut m = regional_fleet(&placement);
        m.set_arrival_rate(250.0);
        m.set_residency(1.0);
        served_costs(&mut m, &trace);
        let confined = m.regions().expect("regions are on").stats.forced;
        assert_eq!(away(&m), 0);
        let unplaced = m.fleet().expect("a fleet").stats.unplaced;
        assert!(unplaced > 100, "{unplaced}");
        let mut open = regional_fleet(&placement);
        open.set_arrival_rate(250.0);
        served_costs(&mut open, &trace);
        let forced = open.regions().expect("regions are on").stats.forced;
        assert!(confined * 3 < forced, "{confined} against {forced}");
    }

    #[test]
    fn a_restricted_tenants_request_is_never_served_outside_its_clients_region() {
        let trace = hot_trace(0.9, 4_000);
        let mut m = confined_machine(0.5, Overflow::Node);
        let (mut kept, mut left) = (0, 0);
        for req in &trace {
            let Submitted::Closed(_) = m.submit(req) else {
                continue;
            };
            let (Some(tenant), true) = (req.tenant, req.client_facing() && req.tokens > 0) else {
                continue;
            };
            let served = m.served_region();
            if m.restricted(tenant) {
                assert_eq!(served, Some(usize::from(req.region)));
                kept += 1;
            } else if served != Some(usize::from(req.region)) {
                left += 1;
            }
        }
        assert!(kept > 100 && left > 0, "{kept} {left}");
    }
}
