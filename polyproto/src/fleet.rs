use crate::engine::{MAX_BATCH, MODEL_COUNT, Model, STEP_BASE_NS, STEP_PER_SEQ_NS};
use crate::work::{KV_BLOCK_BYTES, PHASES, Request, WEIGHT_BYTES};

pub const GIB: u64 = 1 << 30;
pub const PUBLISHED_START_NS: u64 = 8_000_000_000;
pub const PUBLISHED_CONTEXT_BLOCKS: u64 = 4096;
const PREFILL_SHARE_CEILING: f64 = 0.95;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ModelSpec {
    pub bytes: u64,
    pub start_ns: u64,
    pub context_blocks: u64,
}

#[derive(Clone, Debug)]
pub struct Catalogue {
    models: [ModelSpec; MODEL_COUNT],
}

impl Catalogue {
    #[must_use]
    pub fn published(start_ns: u64) -> Self {
        Self {
            models: [ModelSpec {
                bytes: 2 * WEIGHT_BYTES,
                start_ns,
                context_blocks: PUBLISHED_CONTEXT_BLOCKS,
            }; MODEL_COUNT],
        }
    }

    #[must_use]
    pub fn with_sizes(mut self, bytes: [u64; MODEL_COUNT]) -> Self {
        for (m, b) in self.models.iter_mut().zip(bytes) {
            m.bytes = b;
        }
        self
    }

    #[must_use]
    pub fn step_base_ns(&self, model: Model) -> f64 {
        STEP_BASE_NS as f64 * self.get(model).bytes as f64 / GIB as f64
    }

    #[must_use]
    pub fn with_context(mut self, context_blocks: u64) -> Self {
        for m in &mut self.models {
            m.context_blocks = context_blocks;
        }
        self
    }

    #[must_use]
    pub fn get(&self, model: Model) -> ModelSpec {
        self.models[usize::from(model) % MODEL_COUNT]
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    Serving,
    Loading { ready_ns: u64 },
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Role {
    #[default]
    Both,
    Prefill,
    Decode,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Replica {
    pub model: Model,
    pub state: State,
    pub context_blocks: u64,
    pub role: Role,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Move {
    pub at_ns: u64,
    pub node: usize,
    pub from: Option<Model>,
    pub to: Model,
    pub ready_ns: u64,
}

#[derive(Clone, Debug, Default)]
pub struct FleetStats {
    pub loads: u64,
    pub role_moves: u64,
    pub downtime_ns: u64,
    pub kv_lost_bytes: u64,
    pub unplaced: u64,
    pub served: Vec<[u64; MODEL_COUNT]>,
    pub moves: Vec<Move>,
}

#[derive(Clone, Debug)]
pub struct Fleet {
    catalogue: Catalogue,
    replicas: Vec<Option<Replica>>,
    cached: Vec<u8>,
    landing: Vec<Option<(Model, u64)>>,
    pub stats: FleetStats,
}

impl Fleet {
    #[must_use]
    pub fn new(catalogue: Catalogue, placement: &[Option<Model>]) -> Self {
        let replicas = placement
            .iter()
            .map(|slot| {
                slot.map(|model| Replica {
                    model,
                    state: State::Serving,
                    context_blocks: catalogue.get(model).context_blocks,
                    role: Role::Both,
                })
            })
            .collect();
        let cached = placement
            .iter()
            .map(|slot| slot.map_or(0, |m| 1 << m))
            .collect();
        Self {
            catalogue,
            replicas,
            cached,
            landing: vec![None; placement.len()],
            stats: FleetStats {
                served: vec![[0; MODEL_COUNT]; placement.len()],
                ..FleetStats::default()
            },
        }
    }

    #[must_use]
    pub fn catalogue(&self) -> &Catalogue {
        &self.catalogue
    }

    #[must_use]
    pub fn nodes(&self) -> usize {
        self.replicas.len()
    }

    #[must_use]
    pub fn replica(&self, node: usize) -> Option<Replica> {
        self.replicas.get(node).copied().flatten()
    }

    #[must_use]
    pub fn model_on(&self, node: usize) -> Option<Model> {
        self.replica(node).map(|r| r.model)
    }

    #[must_use]
    pub fn placement(&self) -> Vec<Option<Model>> {
        self.replicas.iter().map(|r| r.map(|r| r.model)).collect()
    }

    #[must_use]
    pub fn counts(&self) -> [usize; MODEL_COUNT] {
        let mut counts = [0; MODEL_COUNT];
        for r in self.replicas.iter().flatten() {
            counts[usize::from(r.model) % MODEL_COUNT] += 1;
        }
        counts
    }

    #[must_use]
    pub fn decode_counts(&self) -> [usize; MODEL_COUNT] {
        let mut counts = [0; MODEL_COUNT];
        for r in self
            .replicas
            .iter()
            .flatten()
            .filter(|r| r.role != Role::Prefill)
        {
            counts[usize::from(r.model) % MODEL_COUNT] += 1;
        }
        counts
    }

    #[must_use]
    pub fn serves(&self, node: usize, model: Model, blocks: u64) -> bool {
        self.replica(node).is_some_and(|r| {
            r.model == model && r.role != Role::Prefill && blocks <= r.context_blocks
        })
    }

    pub fn set_role(&mut self, node: usize, role: Role) {
        if let Some(r) = self.replicas[node].as_mut() {
            r.role = role;
        }
    }

    pub fn move_role(&mut self, node: usize, role: Role) {
        if self.role_of(node) != role {
            self.set_role(node, role);
            self.stats.role_moves += 1;
        }
    }

    #[must_use]
    pub fn prefiller_counts(&self) -> [usize; MODEL_COUNT] {
        let (all, decode) = (self.counts(), self.decode_counts());
        std::array::from_fn(|m| all[m] - decode[m])
    }

    #[must_use]
    pub fn role_of(&self, node: usize) -> Role {
        self.replica(node).map_or(Role::Both, |r| r.role)
    }

    #[must_use]
    pub fn prefillers(&self, model: Model, now_ns: u64) -> Vec<usize> {
        (0..self.nodes())
            .filter(|&d| {
                self.replica(d)
                    .is_some_and(|r| r.model == model && r.role == Role::Prefill)
                    && self.wait_ns(d, now_ns) == 0
            })
            .collect()
    }

    #[must_use]
    pub fn wait_ns(&self, node: usize, now_ns: u64) -> u64 {
        match self.replica(node).map(|r| r.state) {
            Some(State::Loading { ready_ns }) => ready_ns.saturating_sub(now_ns),
            _ => 0,
        }
    }

    pub fn settle(&mut self, now_ns: u64) {
        for r in self.replicas.iter_mut().flatten() {
            if let State::Loading { ready_ns } = r.state
                && ready_ns <= now_ns
            {
                r.state = State::Serving;
            }
        }
    }

    #[must_use]
    pub fn cached(&self, node: usize, model: Model) -> bool {
        self.cached
            .get(node)
            .is_some_and(|mask| mask & (1 << model) != 0)
    }

    #[must_use]
    pub fn holders(&self, model: Model, now_ns: u64) -> Vec<usize> {
        (0..self.nodes())
            .filter(|&d| self.model_on(d) == Some(model) || self.cached(d, model))
            .filter(|&d| !matches!(self.landing[d], Some((m, at)) if m == model && at > now_ns))
            .collect()
    }

    #[must_use]
    pub fn step_base_ns(&self, model: Model) -> u64 {
        self.catalogue.step_base_ns(model) as u64
    }

    #[must_use]
    pub fn partition_bytes(&self, model: Model, hbm: u64) -> u64 {
        hbm.saturating_sub(self.catalogue.get(model).bytes)
    }

    pub fn assign(
        &mut self,
        now_ns: u64,
        begin_ns: u64,
        node: usize,
        model: Model,
        load_ns: u64,
    ) -> Move {
        let from = self.model_on(node);
        let role = self.role_of(node);
        if let Some((copying, lands_ns)) = self.landing[node]
            && lands_ns > begin_ns
        {
            self.cached[node] &= !(1 << copying);
        }
        let held = self.cached(node, model);
        let ready_ns = begin_ns + load_ns;
        self.replicas[node] = Some(Replica {
            model,
            state: if ready_ns == now_ns {
                State::Serving
            } else {
                State::Loading { ready_ns }
            },
            context_blocks: self.catalogue.get(model).context_blocks,
            role,
        });
        self.cached[node] |= 1 << model;
        let copy_ns = load_ns.saturating_sub(self.catalogue.get(model).start_ns);
        self.landing[node] = (!held).then_some((model, begin_ns + copy_ns));
        self.stats.loads += 1;
        self.stats.downtime_ns += ready_ns - now_ns;
        let mv = Move {
            at_ns: now_ns,
            node,
            from,
            to: model,
            ready_ns,
        };
        self.stats.moves.push(mv);
        mv
    }

    pub fn bind_partition(&mut self, node: usize, partition_bytes: u64) {
        if let Some(r) = self.replicas[node].as_mut() {
            r.context_blocks =
                Self::context_blocks(partition_bytes, self.catalogue.get(r.model).context_blocks);
        }
    }

    pub fn record_served(&mut self, node: usize, model: Model) {
        self.stats.served[node][usize::from(model) % MODEL_COUNT] += 1;
    }

    #[must_use]
    pub fn context_blocks(partition: u64, model_window: u64) -> u64 {
        model_window.min(partition / KV_BLOCK_BYTES)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PlannerKind {
    Once,
    Follow,
    Eager,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct FleetView {
    pub demand_tokens: [u64; MODEL_COUNT],
    pub prefill_work_ns: [u64; MODEL_COUNT],
    pub prefills: [u64; MODEL_COUNT],
    pub paired_work_ns: [u64; MODEL_COUNT],
    pub paired_avoided_ns: [u64; MODEL_COUNT],
}

#[derive(Clone, Copy, Debug)]
pub struct Prefills {
    pub work_ns_per_s: f64,
    pub per_s: f64,
    pub duplication: f64,
}

impl FleetView {
    #[must_use]
    pub fn prefills_of(&self, model: usize, seconds: f64) -> Prefills {
        let duplication = if self.paired_avoided_ns[model] == 0 {
            1.0
        } else {
            (self.paired_work_ns[model] as f64 / self.paired_avoided_ns[model] as f64).max(1.0)
        };
        Prefills {
            work_ns_per_s: self.prefill_work_ns[model] as f64 / seconds,
            per_s: self.prefills[model] as f64 / seconds,
            duplication,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub struct Costs {
    pub base_ns: [f64; MODEL_COUNT],
    pub per_ns: f64,
    pub max_batch: f64,
    pub overload_ns: f64,
}

impl Costs {
    #[must_use]
    pub fn published(overload_ns: u64) -> Self {
        Self::of(&Catalogue::published(PUBLISHED_START_NS), overload_ns)
    }

    #[must_use]
    pub fn of(catalogue: &Catalogue, overload_ns: u64) -> Self {
        Self {
            base_ns: std::array::from_fn(|m| catalogue.step_base_ns(m as Model)),
            per_ns: STEP_PER_SEQ_NS as f64,
            max_batch: MAX_BATCH as f64,
            overload_ns: overload_ns as f64,
        }
    }

    #[must_use]
    pub fn cost_rate(&self, model: usize, tokens_per_s: f64, replicas: usize) -> f64 {
        if tokens_per_s <= 0.0 {
            return 0.0;
        }
        if replicas == 0 {
            return tokens_per_s * self.overload_ns;
        }
        let base = self.base_ns[model % MODEL_COUNT];
        let step_full = base + (self.max_batch - 1.0) * self.per_ns;
        let capacity = self.max_batch / step_full;
        let rate = tokens_per_s / replicas as f64 / 1e9;
        if rate >= capacity {
            let served = capacity * replicas as f64 * 1e9;
            return served * step_full + (tokens_per_s - served) * self.overload_ns;
        }
        let in_flight = (rate * (base - self.per_ns) / (1.0 - rate * self.per_ns)).max(1.0);
        tokens_per_s * (base + (in_flight - 1.0) * self.per_ns)
    }

    #[must_use]
    pub fn role_rate(
        &self,
        model: usize,
        tokens_per_s: f64,
        replicas: usize,
        prefillers: usize,
        prefills: Prefills,
    ) -> f64 {
        let decoders = replicas.saturating_sub(prefillers).max(1);
        let decode = self.cost_rate(model, tokens_per_s, decoders);
        let work = prefills.work_ns_per_s;
        if prefillers == 0 {
            let share = (work / replicas.max(1) as f64 / 1e9).min(PREFILL_SHARE_CEILING);
            return decode / (1.0 - share) + work;
        }
        let paired = work * prefills.duplication;
        let utilisation = paired / (prefillers as f64 * 1e9);
        let waiting = if utilisation >= 1.0 {
            prefills.per_s * self.overload_ns * (1.0 - 1.0 / utilisation)
        } else {
            utilisation / (1.0 - utilisation) * paired
        };
        decode + paired + waiting
    }

    #[must_use]
    pub fn best_prefillers(
        &self,
        model: usize,
        tokens_per_s: f64,
        replicas: usize,
        prefills: Prefills,
    ) -> usize {
        (0..replicas.max(1))
            .min_by(|&a, &b| {
                let cost = |p| self.role_rate(model, tokens_per_s, replicas, p, prefills);
                cost(a).total_cmp(&cost(b)).then(a.cmp(&b))
            })
            .unwrap_or(0)
    }

    #[must_use]
    pub fn total_rate(&self, demand: &[f64; MODEL_COUNT], counts: &[usize; MODEL_COUNT]) -> f64 {
        demand
            .iter()
            .zip(counts)
            .enumerate()
            .map(|(m, (&d, &r))| self.cost_rate(m, d, r))
            .sum()
    }

    #[must_use]
    pub fn best_counts(&self, demand: &[f64; MODEL_COUNT], nodes: usize) -> [usize; MODEL_COUNT] {
        self.best_counts_among(demand, nodes, [true; MODEL_COUNT])
    }

    #[must_use]
    pub fn best_counts_among(
        &self,
        demand: &[f64; MODEL_COUNT],
        nodes: usize,
        open: [bool; MODEL_COUNT],
    ) -> [usize; MODEL_COUNT] {
        let mut counts = [0usize; MODEL_COUNT];
        let mut by_demand: Vec<usize> = (0..MODEL_COUNT).filter(|&m| open[m]).collect();
        if by_demand.is_empty() {
            return counts;
        }
        by_demand.sort_by(|&a, &b| demand[b].total_cmp(&demand[a]).then(a.cmp(&b)));
        let floor = nodes.min(by_demand.len());
        for &m in by_demand.iter().take(floor) {
            counts[m] = 1;
        }
        for _ in floor..nodes {
            let gain = |m: usize| {
                self.cost_rate(m, demand[m], counts[m])
                    - self.cost_rate(m, demand[m], counts[m] + 1)
            };
            let best = by_demand
                .iter()
                .copied()
                .max_by(|&a, &b| gain(a).total_cmp(&gain(b)).then(b.cmp(&a)))
                .unwrap_or(0);
            counts[best] += 1;
        }
        counts
    }
}

#[must_use]
pub fn layout(counts: &[usize; MODEL_COUNT], nodes: usize) -> Vec<Option<Model>> {
    let mut placement: Vec<Option<Model>> = counts
        .iter()
        .enumerate()
        .flat_map(|(m, &n)| std::iter::repeat_n(Some(m as Model), n))
        .collect();
    placement.resize(nodes, None);
    placement
}

#[must_use]
pub fn retarget(
    current: &[Option<Model>],
    best: &[usize; MODEL_COUNT],
    keep_cost: &[f64],
) -> Vec<Option<Model>> {
    let mut next = current.to_vec();
    let mut held = [0usize; MODEL_COUNT];
    for m in current.iter().flatten() {
        held[usize::from(*m) % MODEL_COUNT] += 1;
    }
    let mut released: Vec<usize> = (0..current.len())
        .filter(|&d| current[d].is_none())
        .collect();
    for m in 0..MODEL_COUNT {
        let surplus = held[m].saturating_sub(best[m]);
        let mut nodes: Vec<usize> = (0..current.len())
            .filter(|&d| current[d].is_some_and(|x| usize::from(x) == m))
            .collect();
        nodes.sort_by(|&a, &b| keep_cost[a].total_cmp(&keep_cost[b]).then(a.cmp(&b)));
        released.extend(nodes.into_iter().take(surplus));
    }
    let mut spare = released.into_iter();
    for m in 0..MODEL_COUNT {
        for _ in held[m]..best[m] {
            if let Some(d) = spare.next() {
                next[d] = Some(m as Model);
            }
        }
    }
    next
}

#[must_use]
pub fn moves_between(current: &[Option<Model>], next: &[Option<Model>]) -> usize {
    current.iter().zip(next).filter(|(a, b)| a != b).count()
}

#[derive(Clone, Debug)]
pub struct OraclePlan {
    pub initial: Vec<Option<Model>>,
    pub shifts: Vec<(u64, Vec<Option<Model>>)>,
}

#[derive(Clone, Copy, Debug)]
pub struct PhaseDemand {
    pub per_phase: [[f64; MODEL_COUNT]; PHASES],
    pub overall: [f64; MODEL_COUNT],
    pub starts_ns: [u64; PHASES],
}

#[must_use]
pub fn phase_demand(trace: &[Request], interval_ns: u64) -> PhaseDemand {
    let mut tokens = [[0u64; MODEL_COUNT]; PHASES];
    let mut requests = [0u64; PHASES];
    let mut starts = [None; PHASES];
    let mut seen = 0u64;
    for req in trace {
        if !req.concurrent {
            seen += 1;
            requests[req.phase] += 1;
        }
        starts[req.phase].get_or_insert(seen * interval_ns);
        if let Some(m) = crate::work::model_of(&req.requires) {
            tokens[req.phase][usize::from(m)] += req.tokens;
        }
        for a in req.gang.iter().flat_map(|g| &g.agents) {
            if let Some(m) = crate::work::model_of(&a.requires) {
                tokens[req.phase][usize::from(m)] += a.tokens;
            }
        }
    }
    let per_second =
        |t: u64, n: u64| t as f64 / ((n * interval_ns) as f64 / 1e9).max(f64::MIN_POSITIVE);
    PhaseDemand {
        per_phase: std::array::from_fn(|p| tokens[p].map(|t| per_second(t, requests[p]))),
        overall: std::array::from_fn(|m| {
            per_second(tokens.iter().map(|row| row[m]).sum(), requests.iter().sum())
        }),
        starts_ns: starts.map(|s| s.unwrap_or(0)),
    }
}

#[must_use]
pub fn oracle_plan(
    costs: &Costs,
    trace: &[Request],
    nodes: usize,
    interval_ns: u64,
    late_ns: u64,
) -> OraclePlan {
    let demand = phase_demand(trace, interval_ns);
    let counts: Vec<[usize; MODEL_COUNT]> = demand
        .per_phase
        .iter()
        .map(|d| costs.best_counts(d, nodes))
        .collect();
    let initial = layout(&counts[0], nodes);
    let free = vec![0.0; nodes];
    let mut current = initial.clone();
    let mut shifts = Vec::new();
    for (p, target) in counts.iter().enumerate().skip(1) {
        let next = retarget(&current, target, &free);
        if next != current {
            shifts.push((demand.starts_ns[p] + late_ns, next.clone()));
        }
        current = next;
    }
    OraclePlan { initial, shifts }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fleet() -> Fleet {
        Fleet::new(
            Catalogue::published(PUBLISHED_START_NS),
            &[Some(0), Some(0), Some(1), None],
        )
    }

    #[test]
    fn a_replica_serves_its_own_model_and_nothing_else() {
        let f = fleet();
        assert!(f.serves(0, 0, 10) && f.serves(1, 0, 10));
        assert!(!f.serves(0, 1, 10));
        assert!(f.serves(2, 1, 10));
        assert!(!f.serves(3, 0, 10), "a node with no replica serves nothing");
        assert_eq!(f.counts(), [2, 1, 0, 0]);
    }

    #[test]
    fn the_context_window_is_the_smaller_of_the_model_and_the_partition() {
        assert_eq!(Fleet::context_blocks(3 * GIB, 4096), 4096);
        assert_eq!(Fleet::context_blocks(GIB, 4096), 2048);
        let f = Fleet::new(Catalogue::published(0).with_context(8), &[Some(0)]);
        assert!(f.serves(0, 0, 8) && !f.serves(0, 0, 9));
    }

    #[test]
    fn a_load_serves_nothing_until_it_is_ready_and_leaves_the_model_cached() {
        let mut f = fleet();
        assert!(!f.cached(0, 2));
        let mv = f.assign(1_000, 1_000, 0, 2, 5_000);
        assert_eq!(mv.from, Some(0));
        assert_eq!(mv.ready_ns, 6_000);
        assert_eq!(f.wait_ns(0, 2_000), 4_000);
        assert_eq!(f.wait_ns(0, 7_000), 0);
        assert_eq!(
            f.replica(0).map(|r| r.state),
            Some(State::Loading { ready_ns: 6_000 })
        );
        f.settle(6_000);
        assert_eq!(f.replica(0).map(|r| r.state), Some(State::Serving));
        assert!(f.cached(0, 2) && f.cached(0, 0));
        assert_eq!((f.stats.loads, f.stats.downtime_ns), (1, 5_000));
        assert_eq!(f.holders(2, 6_000), vec![0]);
    }

    #[test]
    fn the_partition_is_what_the_weights_leave() {
        let f = fleet();
        assert_eq!(f.partition_bytes(0, 4 * GIB), 3 * GIB);
        assert_eq!(f.partition_bytes(0, GIB / 2), 0);
    }
    fn costs() -> Costs {
        Costs::published(5_000_000_000)
    }

    #[test]
    fn a_replica_is_cheaper_the_more_of_them_share_a_models_demand_and_saturation_costs_a_queue() {
        let c = costs();
        let demand = 6_000.0;
        let rates: Vec<f64> = (1..=6).map(|r| c.cost_rate(0, demand, r)).collect();
        assert!(rates.windows(2).all(|w| w[1] < w[0]), "{rates:?}");
        let gains: Vec<f64> = rates.windows(2).map(|w| w[0] - w[1]).collect();
        assert!(gains.windows(2).all(|w| w[1] <= w[0]), "convex: {gains:?}");
        assert!(c.cost_rate(0, 9_000.0, 1) > 100.0 * c.cost_rate(0, 9_000.0, 2));
        assert!((c.cost_rate(0, 0.0, 3)).abs() < f64::EPSILON);
        assert!((c.cost_rate(0, 100.0, 0) - 100.0 * 5e9).abs() < 1.0);
    }

    #[test]
    fn the_allocation_follows_demand_and_keeps_every_model_available() {
        let c = costs();
        let nodes = 8;
        let counts = c.best_counts(&[4_000.0, 2_000.0, 1_000.0, 500.0], nodes);
        assert_eq!(counts.iter().sum::<usize>(), nodes);
        assert!(counts.iter().all(|&n| n >= 1));
        assert!(counts[0] > counts[1] && counts[1] >= counts[2] && counts[2] >= counts[3]);
        let even = c.best_counts(&[1_000.0; 4], nodes);
        assert_eq!(even, [2, 2, 2, 2]);
        let few = c.best_counts(&[900.0, 10.0, 500.0, 0.0], 2);
        assert_eq!(few, [1, 0, 1, 0], "two nodes go to the two hottest models");
    }

    #[test]
    fn a_greedy_allocation_is_optimal_for_separable_convex_costs() {
        let c = costs();
        let demand = [3_000.0, 1_800.0, 900.0, 300.0];
        let best = c.best_counts(&demand, 6);
        let best_cost = c.total_rate(&demand, &best);
        for a in 1..=3usize {
            for b in 1..=3usize {
                for d in 1..=3usize {
                    let rest = 6usize.saturating_sub(a + b + d);
                    if rest == 0 || a + b + d > 6 {
                        continue;
                    }
                    let counts = [a, b, d, rest];
                    assert!(c.total_rate(&demand, &counts) >= best_cost - 1e-3);
                }
            }
        }
    }

    #[test]
    fn a_retarget_moves_only_the_surplus_and_spares_the_nodes_that_cost_most_to_empty() {
        let current: Vec<Option<Model>> =
            vec![Some(0), Some(0), Some(0), Some(1), Some(1), Some(2)];
        let keep = [5.0, 1.0, 3.0, 0.0, 0.0, 0.0];
        let next = retarget(&current, &[1, 2, 2, 1], &keep);
        assert_eq!(moves_between(&current, &next), 2);
        assert_eq!(next[0], Some(0), "the node with the most KV to lose stays");
        let mut counts = [0usize; MODEL_COUNT];
        for m in next.iter().flatten() {
            counts[usize::from(*m)] += 1;
        }
        assert_eq!(counts, [1, 2, 2, 1]);
        assert_eq!(retarget(&current, &[3, 2, 1, 0], &keep), current);
        assert_eq!(
            retarget(&[Some(0), None], &[1, 1, 0, 0], &[0.0, 0.0]),
            vec![Some(0), Some(1)],
            "an empty node is the first spare"
        );
    }

    #[test]
    fn the_oracle_places_for_the_first_phase_and_shifts_at_each_boundary_it_reads() {
        let trace: Vec<Request> = crate::work::Workload::with_fanout(2, 8_000, 0.0, 0.05)
            .with_model_mix(crate::work::rotating_mix([0.55, 0.25, 0.12, 0.08]))
            .collect();
        let plan = oracle_plan(&costs(), &trace, 8, 4_000_000, 0);
        let mut first = [0usize; MODEL_COUNT];
        for m in plan.initial.iter().flatten() {
            first[usize::from(*m)] += 1;
        }
        assert!(first[0] > first[3], "{first:?}");
        assert_eq!(plan.shifts.len(), PHASES - 1);
        assert!(plan.shifts.windows(2).all(|w| w[0].0 < w[1].0));
        let late = oracle_plan(&costs(), &trace, 8, 4_000_000, 10_000_000_000);
        assert_eq!(late.shifts[0].0, plan.shifts[0].0 + 10_000_000_000);
        assert_eq!(late.initial, plan.initial);
    }

    #[test]
    fn a_load_that_waits_for_a_drain_is_down_from_the_decision_to_the_ready() {
        let mut f = Fleet::new(Catalogue::published(1_000), &[Some(0), Some(1)]);
        let mv = f.assign(1_000, 3_000, 0, 2, 5_000);
        assert_eq!((mv.at_ns, mv.ready_ns), (1_000, 8_000));
        assert_eq!(f.wait_ns(0, 1_000), 7_000);
        assert_eq!(f.stats.downtime_ns, 7_000);
        assert!(
            f.holders(2, 6_999).is_empty(),
            "the copy starts when the drain ends"
        );
        assert_eq!(f.holders(2, 7_000), vec![0]);
    }

    #[test]
    fn a_copy_interrupted_by_another_move_leaves_nothing_behind() {
        let mut f = Fleet::new(Catalogue::published(1_000), &[Some(0), Some(1)]);
        f.assign(0, 0, 0, 2, 9_000);
        f.assign(2_000, 2_000, 0, 3, 9_000);
        assert!(!f.cached(0, 2), "model 2's copy never landed");
        assert!(f.cached(0, 3) && f.cached(0, 0));
        f.assign(20_000, 20_000, 0, 0, 1_500);
        assert!(
            f.cached(0, 3),
            "model 3's copy landed before the node moved on"
        );
        assert_eq!(
            f.holders(0, 20_000),
            vec![0],
            "a model already held is no copy in flight"
        );
    }

    const SIZED: [u64; MODEL_COUNT] = [GIB / 2, GIB, GIB, 2 * GIB];

    #[test]
    fn a_sized_catalogue_scales_the_step_the_partition_and_the_cold_pull() {
        let catalogue = Catalogue::published(0).with_sizes(SIZED);
        let f = Fleet::new(catalogue.clone(), &[Some(0), Some(3)]);
        assert_eq!(f.step_base_ns(0) * 4, f.step_base_ns(3));
        assert_eq!(f.step_base_ns(1), STEP_BASE_NS);
        assert_eq!(f.partition_bytes(3, 4 * GIB), 2 * GIB);
        assert_eq!(f.partition_bytes(0, 4 * GIB), 7 * GIB / 2);
        assert_eq!(Catalogue::published(0).get(2).bytes, GIB);
    }

    #[test]
    fn at_equal_token_demand_the_larger_model_gets_the_replicas() {
        let c = Costs::of(&Catalogue::published(0).with_sizes(SIZED), 5_000_000_000);
        let even = Costs::published(5_000_000_000);
        for tokens in [2_000.0, 6_000.0, 12_000.0] {
            let demand = [tokens; MODEL_COUNT];
            let counts = c.best_counts(&demand, 8);
            assert_eq!(counts.iter().sum::<usize>(), 8);
            assert!(counts[0] <= counts[1] && counts[1] == counts[2] && counts[2] <= counts[3]);
            assert!(counts[3] > counts[0], "{tokens}: {counts:?}");
            assert_eq!(even.best_counts(&demand, 8), [2; MODEL_COUNT]);
        }
    }

    #[test]
    fn a_prefill_replica_serves_no_decode_and_keeps_its_role_through_a_load() {
        let mut f = Fleet::new(Catalogue::published(0), &[Some(0), Some(0), Some(1)]);
        f.set_role(0, Role::Prefill);
        f.set_role(1, Role::Decode);
        assert!(!f.serves(0, 0, 1) && f.serves(1, 0, 1) && !f.serves(2, 0, 0));
        assert_eq!(f.prefillers(0, 0), vec![0]);
        assert!(f.prefillers(1, 0).is_empty());
        f.assign(0, 0, 0, 1, 0);
        assert_eq!(f.role_of(0), Role::Prefill);
        assert_eq!(f.prefillers(1, 0), vec![0]);
        assert_eq!(f.role_of(2), Role::Both);
    }

    fn prefills(work_share: f64, per_s: f64, duplication: f64) -> Prefills {
        Prefills {
            work_ns_per_s: work_share * 8.0 * 1e9,
            per_s,
            duplication,
        }
    }

    #[test]
    fn with_no_prefill_work_the_best_number_of_prefillers_is_zero() {
        let c = costs();
        assert_eq!(c.best_prefillers(0, 6_000.0, 8, prefills(0.0, 0.0, 1.0)), 0);
    }

    #[test]
    fn a_prefiller_pays_when_the_work_it_takes_is_large_and_the_duplicate_is_small() {
        let c = costs();
        let heavy = c.best_prefillers(0, 6_000.0, 8, prefills(0.25, 300.0, 1.0));
        let light = c.best_prefillers(0, 6_000.0, 8, prefills(0.03, 300.0, 1.0));
        assert!(heavy >= 2, "{heavy}");
        assert!(light <= heavy);
        let duplicated = c.best_prefillers(0, 6_000.0, 8, prefills(0.03, 300.0, 5.0));
        assert!(duplicated <= light, "{duplicated} against {light}");
    }

    #[test]
    fn a_saturated_prefiller_costs_a_queue_and_more_prefillers_take_it_off() {
        let c = costs();
        let p = prefills(0.40, 300.0, 1.0);
        let rates: Vec<f64> = (1..5).map(|n| c.role_rate(0, 6_000.0, 8, n, p)).collect();
        assert!(rates[0] > 3.0 * rates[2], "{rates:?}");
    }

    #[test]
    fn a_model_no_request_names_keeps_no_replica() {
        let c = costs();
        let demand = [6_000.0, 0.0, 0.0, 0.0];
        let open = [true, false, false, false];
        assert_eq!(c.best_counts_among(&demand, 8, open), [8, 0, 0, 0]);
        assert_eq!(
            c.best_counts_among(&demand, 8, [false; MODEL_COUNT]),
            [0; MODEL_COUNT]
        );
        assert_eq!(
            c.best_counts_among(&demand, 8, [true; MODEL_COUNT])[1..],
            [1, 1, 1]
        );
    }
}
