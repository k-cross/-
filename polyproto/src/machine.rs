//! A machine of several memory domains, and the placement decision over them.
//!
//! The ledger in each domain decides what stays resident. This layer decides *which compute
//! unit runs the work*, and therefore what the state costs to reach. Placing by load and
//! routing by residency are the same decision; a scheduler that cannot see both makes it
//! twice, badly.

use crate::blob::{BlobId, BlobKind, BlobMeta};
use crate::boundary::Cost as Crossing;
use crate::cache::{Cost, Hierarchy, NodeMemory, Policy};
use crate::engine::{Engine, MAX_BATCH};
use crate::topo::Topology;
use crate::work::{Agent, Gang, Request, ToolCall};
use std::collections::{HashMap, HashSet};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Placement {
    /// Spread by load, blind to where the state already is. A floor, not a fair baseline:
    /// no real load balancer re-scatters continuations of the same session.
    Blind,
    /// Consistent hash on the chain root, so a session always lands on the same domain.
    /// What a load balancer in front of a stateless fleet actually does -- stable placement,
    /// but still blind to *content*: it cannot see two sessions sharing a tenant prefix.
    Sticky,
    /// Run the work where its state already lives.
    Aware,
    /// Run the work where it is worth the most: state already resident, minus what admitting
    /// the rest would displace, minus the handoff it avoids -- all in the ledger's own
    /// currency of recompute nanoseconds. `Aware` scores only the numerator of this.
    Scored,
}

/// Where residency knowledge lives, and what it costs to consult.
///
/// This is the architectural question the prototype exists to answer. A scheduler that shares
/// a process with the ledger reads residency off a field. One that does not must either ask
/// -- and pay a boundary crossing on the request's critical path -- or work from a view that
/// was true a moment ago. Both alternatives are what a Kubernetes scheduler extender and an
/// informer cache respectively are.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Control {
    /// Scheduler and ledger in one address space. A decision is a function call.
    Unified,
    /// Scheduler asks every candidate node before placing. Always correct, and pays the
    /// measured crossing on every request.
    Query,
    /// Scheduler places from a view refreshed every `period` requests. Free at decision time;
    /// wrong in proportion to how fast residency moves.
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

#[derive(Debug)]
pub struct Machine {
    topo: Topology,
    domains: Vec<Hierarchy>,
    placement: Placement,
    next_unit: usize,
    sticky_unit: usize,
    /// Domains still accepting work. Draining one leaves its state migrated elsewhere and
    /// every content hash that pointed at it stale.
    active: Vec<usize>,
    pub migrated_bytes: u64,
    /// Domains each in-flight task's upstream stage ran in and the bytes each will hand over,
    /// so the downstream stage can be co-placed with them -- or charged for the handoff when
    /// it is not. A fan-out's results come back from as many nodes as it had agents, so a
    /// task can have several sources. An agent's tool result and a function's prompt bundle
    /// differ by an order of magnitude, so the payload travels with the task rather than
    /// being assumed.
    upstream: HashMap<u64, Vec<(usize, u64)>>,
    /// Downstream stages whose upstream was refused. They arrive anyway -- the workload does
    /// not know -- and must be refused too, or the saving from refusing is imaginary.
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
    /// Co-place a task's downstream stage with its upstream. Orthogonal to residency
    /// placement: the scheduler learns the upstream's location by having placed it, so this
    /// needs no cross-node knowledge and pays no crossing to use.
    flow_aware: bool,
    /// Residency as the scheduler believes it to be. Identical to the truth under `Unified`
    /// and `Query`; a snapshot under `Gossip`.
    view: Vec<HashSet<BlobId>>,
    /// State and decode slots promised to a fan-out's agents while the rest are still being
    /// placed. Siblings share a context prefix, so an agent placed after another on the same
    /// node will find that prefix resident; pretending otherwise prices co-location as if it
    /// bought nothing.
    staged: Vec<HashSet<BlobId>>,
    staged_seqs: Vec<usize>,
    staged_bytes: Vec<Need>,
    ops: u64,
    /// Latency charged to requests for deciding where to run them.
    pub decide_ns: u64,
    /// Crossings spent on control traffic, whether or not they sit on the critical path.
    pub control_rpcs: u64,
    /// Decisions where the scheduler's view of the chosen node disagreed with the truth.
    pub stale_decisions: u64,
    /// How many placements each term of the score actually changed. A term that never moves
    /// a decision is not a policy, it is a comment.
    pub moved_by_displacement: u64,
    pub moved_by_flow: u64,
    pub moved_by_load: u64,
    /// Placements changed by pricing what joining a batch costs the sequences already in it.
    pub moved_by_congestion: u64,
    /// Mean spread (max - min across candidate nodes) of each score term, in nanoseconds.
    /// Only spread decides an argmin -- a term that is large everywhere is a constant, and a
    /// term whose spread is an order of magnitude under another's cannot outvote it. This is
    /// the number that says whether a term is a policy or a comment.
    pub term_spread: [f64; TERM_COUNT],
    pub scored_decisions: u64,
    pub held_by_affinity: u64,
    pub flow_requests: u64,
    pub flow_coplaced: u64,
    /// One serving engine per domain. Decode cost is theirs to decide, not the workload's.
    engines: Vec<Engine>,
    /// Spacing between arrivals, or zero to leave the engine model out and charge the
    /// workload's own unbatched execution time.
    interval_ns: u64,
    arrival_ns: u64,
    /// May a node obtain missing state from a peer that holds it, instead of rebuilding it?
    /// Off is the older behaviour and the baseline the transfer arm is measured against.
    state_transfer: bool,
    pub fetched_bytes: u64,
    pub fetches: u64,
    pub rebuilds: u64,
    /// Fetches planned against a view that turned out to be wrong, and fell back to a rebuild.
    pub stale_fetches: u64,
    pub moved_by_fetch: u64,
    /// All-or-nothing fan-out admission. Off admits whichever agents fit and lets the
    /// orchestrator stall anyway, which is the baseline a per-request scheduler provides.
    fanout_atomic: bool,
    /// Fixed origin used for a downstream flow's recorded location, in place of wherever its
    /// upstream actually ran. See `set_tool_anchor`.
    tool_anchor: Option<usize>,
    /// Where requests enter the cluster, and the bytes that make the round trip when the work
    /// runs somewhere else. See `set_origin`.
    origin: Option<(usize, u64)>,
    /// Link time spent carrying requests to the node that can serve them and results back.
    pub origin_ns: u64,
    pub origin_hops: u64,
    pub fanouts_admitted: u64,
    pub fanouts_refused: u64,
    /// Decode and tool time spent by agents of fan-outs that could never resume. Zero under
    /// atomic admission by construction; under the baseline it is the cost of lacking it.
    pub fanout_wasted_ns: u64,
    pub fanout_wasted_bytes: u64,
    /// Wall time of admitted fan-outs, which is their slowest agent's.
    pub fanout_service_ns: u64,
    pub agents_run: u64,
    /// Agents that shared a node with at least one sibling.
    pub agents_colocated: u64,
    pub tool_calls: u64,
    /// Tool calls that ran on their agent's own node.
    pub tool_coplaced: u64,
    pub tool_refused: u64,
    pub tool_ns: u64,
}

impl Machine {
    #[must_use]
    /// `memory` is a function of domain index rather than one shared value, so a cluster can be
    /// heterogeneous: a domain with `hbm: 0, can_decode: false` is a host-only node that can
    /// run `FaaS`/service work but never decode. Every existing experiment passes `|_| mem`.
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
            flow_aware: false,
            view: vec![HashSet::new(); n_domains],
            staged: vec![HashSet::new(); n_domains],
            staged_seqs: vec![0; n_domains],
            staged_bytes: vec![[0; BlobKind::N]; n_domains],
            ops: 0,
            decide_ns: 0,
            control_rpcs: 0,
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
        }
    }

    /// Requests per second arriving at the cluster. Setting it turns on the engine model:
    /// decode cost stops being a constant and starts depending on how many sequences are
    /// already resident on the chosen node. Zero leaves it off.
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

    /// Anchor the recorded origin of every downstream flow to a fixed domain, rather than
    /// wherever its upstream stage actually ran.
    ///
    /// Real agent frameworks dispatch tool calls from the orchestrator's own process, not from
    /// wherever the model happened to run a decode step -- the two are on different hosts by
    /// construction on split-memory hardware. Without an anchor, a tool call's flow-affinity
    /// pulls it toward the decode node, which is only correct when the orchestrator and the
    /// engine are the same place. `None` (the default) keeps the original behaviour: downstream
    /// work follows its upstream's actual location.
    pub fn set_tool_anchor(&mut self, anchor: Option<usize>) {
        self.tool_anchor = anchor;
    }

    /// Where requests enter the cluster and where their results must return.
    ///
    /// An agent host with no engine cannot decode, so each reasoning request crosses to a node
    /// that can and its result crosses back. No placement policy can remove that round trip --
    /// it is the price of running the orchestrator off the accelerator, and the one cost here
    /// that grows with distance. `payload` is what makes the trip: the context delta going in,
    /// which is dominated by whatever the last tool call returned, and the generated tokens
    /// coming back.
    ///
    /// Charged only to decode-bearing work that starts here. A downstream stage already pays
    /// its own handoff from the origin recorded for it, and charging both would count the same
    /// transfer twice.
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

    /// Sequences admitted by one domain's engine. In a heterogeneous cluster this is the check
    /// that the decode constraint held: a host-only node must report zero.
    #[must_use]
    pub fn decodes_on(&self, d: usize) -> u64 {
        self.engines.get(d).map_or(0, |e| e.admitted)
    }

    #[must_use]
    pub fn saturated(&self) -> u64 {
        self.engines.iter().map(|e| e.saturated).sum()
    }

    /// Ratio of busiest to least-busy engine by sequences admitted. Residency-greedy routing
    /// buys cache hits by concentrating decode, and this is what it pays with.
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

    /// Install the control-plane model. `crossing` should come from `boundary::measure` on
    /// the host being modelled, not from a constant.
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

    fn refresh_view(&mut self) {
        for (d, h) in self.domains.iter().enumerate() {
            let set: HashSet<BlobId> = h.hot_ids().collect();
            self.view[d] = set;
        }
        self.control_rpcs += self.domains.len() as u64;
    }

    /// Residency as the scheduler sees it, which is not always residency as it is.
    fn believes_resident(&self, d: usize, id: &BlobId, kind: BlobKind) -> bool {
        self.staged[d].contains(id)
            || match self.control {
                Control::Gossip { .. } => self.view[d].contains(id),
                Control::Unified | Control::Query => self.domains[d].is_hot(id, kind),
            }
    }

    /// Whether a peer can supply this blob at all. Offloaded state in a peer's host memory is
    /// as readable over RDMA as its accelerator's, so it counts -- except to a gossiped view,
    /// which only ever advertised what was hot.
    fn believes_held(&self, p: usize, id: &BlobId, kind: BlobKind) -> bool {
        match self.control {
            Control::Gossip { .. } => self.view[p].contains(id),
            Control::Unified | Control::Query => self.domains[p].holds(id, kind),
        }
    }

    /// What one placement decision costs, and what it costs the cluster. A fan-out query is
    /// charged one crossing of *latency* because the asks go out in parallel, but N crossings
    /// of *work*, which is what caps the decision rate.
    fn decide(&mut self, chain_len: usize) -> u64 {
        self.ops += 1;
        match self.control {
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
        }
    }

    /// Domain a chain belongs to by content, independent of what is currently resident:
    /// a consistent hash of the chain root, which identifies the tenant or session.
    fn affinity_unit(&self, chain: &[(BlobId, BlobMeta)], candidates: &[usize]) -> usize {
        let root = Self::root_key(chain);
        // Rendezvous hashing, not modulo: removing a domain remaps only the keys that lived
        // on it, which is what a real balancer achieves. Modulo would remap nearly every key
        // on resize and make the residency-aware arm look good for the wrong reason.
        let d = candidates
            .iter()
            .copied()
            .max_by_key(|&d| Self::rendezvous(root, d))
            .unwrap_or(0);
        self.unit_in(d)
    }

    /// Domains that can serve a decode-bearing request, falling back to every active domain
    /// if none can -- a misconfigured cluster should behave as it always did, not panic.
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

    /// Whether this request's chain needs a domain that can actually decode: a `KvBlock` or
    /// `WeightShard` chain, or anything charged tokens. `Snapshot`/`ServiceHeap` work has no
    /// such requirement and may run on a host-only node.
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

    /// Take a domain out of service, migrating its resident state to the remaining domains.
    /// The bytes survive; every hash that pointed at the drained domain does not.
    pub fn drain(&mut self, victim: usize) {
        let Some(pos) = self.active.iter().position(|&d| d == victim) else {
            return;
        };
        self.active.remove(pos);
        if self.active.is_empty() {
            self.active.push(victim);
            return;
        }
        let moving: Vec<(BlobId, BlobMeta)> = self.domains[victim].drain_all();
        for (i, (id, meta)) in moving.into_iter().enumerate() {
            let to = self.active[i % self.active.len()];
            self.migrated_bytes += meta.bytes;
            self.domains[to].reinstate(id, meta);
        }
    }

    /// The domain this policy would pick knowing nothing about flows: round-robin for
    /// `Blind`, content hash for `Sticky`, deepest resident prefix for `Aware`.
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

    /// Bytes of everything this request needs that domain `d` already holds: the resident
    /// prefix of its chain, plus any shared dependencies. Weight shards dwarf a KV prefix, so
    /// scoring by bytes is what lets sharing outvote caller identity.
    fn resident_value(&self, d: usize, req: &Request) -> u64 {
        let depth = req
            .chain
            .partition_point(|(id, m)| self.believes_resident(d, id, m.kind));
        let chain_bytes: u64 = req.chain[..depth].iter().map(|(_, m)| m.bytes).sum();
        let dep_bytes: u64 = req
            .requires
            .iter()
            .filter(|(id, m)| self.believes_resident(d, id, m.kind))
            .map(|(_, m)| m.bytes)
            .sum();
        chain_bytes + dep_bytes
    }

    /// What it costs domain `d` to make one blob hot without leaving the node: promote it
    /// from host memory, read it off the spill tier, or rebuild it.
    fn local_ns(&self, d: usize, id: &BlobId, m: &BlobMeta) -> u64 {
        self.domains[d].local_ns(id, m)
    }

    fn local_run_ns(&self, d: usize, blobs: &[(BlobId, BlobMeta)]) -> u64 {
        blobs.iter().map(|(id, m)| self.local_ns(d, id, m)).sum()
    }

    /// The cheapest way for `d` to get everything this request needs.
    ///
    /// Three options, priced against each other every time rather than settled once by
    /// policy: run where the state already is, ship the state to where the work is, or
    /// rebuild it locally. Which one wins is not a property of the system, it is a property
    /// of the moment -- a 512 KiB KV block is worth shipping across a rack and worth
    /// rebuilding across a region, and the crossover moves with the link, the block size and
    /// what the spill tier happens to be holding.
    ///
    /// Peers are scanned rather than assumed, because the deepest peer is only the cheapest
    /// peer when every link is identical, and the whole point of the topology is that they
    /// are not.
    fn plan(&self, d: usize, req: &Request) -> Plan {
        let depth = req
            .chain
            .partition_point(|(id, m)| self.believes_resident(d, id, m.kind));
        let mut need = [0u64; BlobKind::N];
        for (_, m) in &req.chain[depth..] {
            need[m.kind.idx()] += m.bytes;
        }
        let mut plan = Plan {
            ns: self.local_run_ns(d, &req.chain[depth..]),
            local_depth: depth,
            chain_cut: depth,
            chain_src: None,
            chain_bytes: 0,
            deps: Vec::new(),
            need,
        };
        if self.state_transfer && depth < req.chain.len() {
            let unit = self.unit_in(d);
            for &p in &self.active {
                if p == d {
                    continue;
                }
                let far = req
                    .chain
                    .partition_point(|(id, m)| self.believes_held(p, id, m.kind));
                if far <= depth {
                    continue;
                }
                let bytes: u64 = req.chain[depth..far].iter().map(|(_, m)| m.bytes).sum();
                let cand =
                    self.topo.fetch_ns(unit, p, bytes) + self.local_run_ns(d, &req.chain[far..]);
                if cand < plan.ns {
                    plan.ns = cand;
                    plan.chain_cut = far;
                    plan.chain_src = Some(p);
                    plan.chain_bytes = bytes;
                }
            }
        }
        for (i, (id, m)) in req.requires.iter().enumerate() {
            if self.believes_resident(d, id, m.kind) {
                continue;
            }
            plan.need[m.kind.idx()] += m.bytes;
            let mut best = self.local_ns(d, id, m);
            let mut src = None;
            if self.state_transfer {
                let unit = self.unit_in(d);
                for &p in &self.active {
                    if p == d || !self.believes_held(p, id, m.kind) {
                        continue;
                    }
                    let cand = self.topo.fetch_ns(unit, p, m.bytes);
                    if cand < best {
                        best = cand;
                        src = Some(p);
                    }
                }
            }
            plan.ns += best;
            if let Some(p) = src {
                plan.deps.push((i, p));
            }
        }
        plan
    }

    /// Carry out a plan's chain transfer, charging the link and installing what arrives.
    ///
    /// The plan was made against the scheduler's view; the peer is asked against the truth.
    /// A source that has since evicted the state supplies a shorter prefix or none at all,
    /// and the rest falls back to a rebuild -- which is what makes a stale view cost
    /// something here rather than silently succeed.
    fn apply_chain(&mut self, d: usize, req: &Request, plan: &Plan) -> Cost {
        let mut cost = Cost::default();
        if let Some(p) = plan.chain_src {
            let truth = req
                .chain
                .partition_point(|(id, m)| self.domains[p].holds(id, m.kind));
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

    /// The same for the request's shared dependencies. Separate from the chain because a
    /// request whose chain was refused must not have its weight shards shipped anyway:
    /// admitting state for work that will not run is exactly what refusal exists to prevent.
    fn apply_deps(&mut self, d: usize, req: &Request, plan: &Plan) -> Cost {
        let mut cost = Cost::default();
        for &(i, p) in &plan.deps {
            let (id, m) = req.requires[i];
            if !self.domains[p].holds(&id, m.kind) {
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

    /// What running this request on `d` would cost, in the ledger's own nanoseconds.
    ///
    /// Four terms, each of which has to be able to overrule the others: acquiring the state
    /// by whichever of the three routes is cheapest here, the debt the next request pays for
    /// bytes this one evicts, the handoff avoided by co-placing with an upstream stage, and
    /// the wait an already-full engine imposes. The last is why a cache hit is not
    /// automatically the right answer -- a node holding the prefix but running a full batch
    /// can be the slower node, and nothing that scores residency alone can see it.
    fn placement_terms(&self, d: usize, req: &Request, flow: &[(usize, u64)]) -> Terms {
        let plan = self.plan(d, req);
        let displaced = self.domains[d].displacement(&plan.need, &self.staged_bytes[d]);
        let unit = self.unit_in(d);
        let handoff: f64 = flow
            .iter()
            .filter(|(src, _)| *src != d)
            .map(|&(src, payload)| self.topo.fetch_ns(unit, src, payload) as f64)
            .sum();
        let decoding = req.tokens > 0 && self.interval_ns > 0;
        let reserved = self.staged_seqs[d];
        let engine = if decoding {
            self.engines[d].projected_ns(self.arrival_ns, req.tokens, reserved) as f64
        } else {
            0.0
        };
        let congestion = if decoding {
            self.engines[d].congestion_ns(self.arrival_ns, req.tokens, reserved) as f64
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
        }
    }

    /// Argmin of the cost, plus what each term changed. Reported rather than assumed: a term
    /// worth four orders of magnitude less than another one cannot move an argmin, and saying
    /// so is more useful than shipping it and believing otherwise.
    fn best_scored(
        &mut self,
        req: &Request,
        flow: &[(usize, u64)],
        affinity: usize,
        candidates: &[usize],
    ) -> usize {
        let terms: Vec<Terms> = candidates
            .iter()
            .map(|&d| self.placement_terms(d, req, flow))
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
        // Never move without a reason. With nothing resident anywhere every cost is equal,
        // and an argmin over ties would send every cold request to the same node; falling
        // back to content affinity spreads them the way a hash does.
        let full = if cost(top) < cost(affinity) {
            top
        } else {
            affinity
        };
        self.moved_by_displacement += u64::from(net != raw);
        self.moved_by_flow += u64::from(placed != net);
        self.moved_by_load += u64::from(loaded != placed);
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
        full
    }

    /// Where a residency-greedy policy would run this: the node holding the most of it.
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

    /// Bytes domain `d` really holds for this request, regardless of what the scheduler
    /// thinks. Must span exactly what `resident_value` scores, dependencies included, or a
    /// decision correctly made on weight residency is reported as stale.
    fn truly_resident(&self, d: usize, req: &Request) -> u64 {
        let depth = req
            .chain
            .partition_point(|(id, m)| self.domains[d].is_hot(id, m.kind));
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
        let decide_ns = self.decide(req.chain.len());
        self.decide_ns += decide_ns;
        let decode_needed = Self::needs_decode(req);
        let candidates = if decode_needed {
            self.decode_pool()
        } else {
            self.active.clone()
        };
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
        let best = if scored {
            self.best_scored(req, &flow, affinity, &candidates)
        } else {
            self.greedy_best(req, &candidates)
        };
        let value = self.resident_value(best, req);

        // A task's downstream stage belongs where its upstream ran: neither workload's own
        // identity hashes to the other's domain, so only a scheduler that sees the flow can
        // put them together. This overrides the placement policy for every policy, which is
        // what makes it separable from residency routing. With several upstream nodes, as
        // after a fan-out, the unscored policy follows the first of them that can actually
        // serve this request -- a flow recorded from a host-only anchor must not force a
        // decode-bearing downstream onto a node that cannot decode.
        // Under `Scored` the score is the whole decision: the flow's pull is already inside
        // it, priced against what co-placing would evict, and gating on `resident_value` as
        // well would discard the scored choice using a metric the score never consulted.
        let target = if scored {
            best
        } else {
            match flow.first() {
                Some(&(d, _)) if !decode_needed || self.domains[d].can_decode() => d,
                _ => self.policy_target(affinity, best, value, &candidates),
            }
        };
        let home = self.topo.units[self.unit_in(target)].home as usize;
        // Only meaningful for a policy that acted on residency: a hash placement did not
        // consult a view, so it cannot have been misled by one.
        if self.placement == Placement::Aware
            && self.resident_value(target, req) > 0
            && self.truly_resident(target, req) == 0
        {
            self.stale_decisions += 1;
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
        cost
    }

    /// Charge a request's round trip from wherever it entered the cluster to the node that can
    /// actually serve it. Zero when the two are the same place, or when no origin is set.
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

    /// Charge the handoffs from a task's upstream nodes into `home`, counting joined and
    /// split stages as it goes.
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

    /// Materialise a request's state on `home` by the cheapest route and run it there.
    fn run_here(&mut self, home: usize, req: &Request) -> Cost {
        let ran_with = self.truly_resident(home, req);
        // Ship first, then read. Whatever a peer supplied is resident by the time `access`
        // walks the chain, so it costs the link once and never a rebuild; whatever no peer
        // could supply falls through to the ledger's own spill-or-rebuild choice.
        let plan = self.plan(home, req);
        let fetch = self.apply_chain(home, req, &plan);
        let mut cost = self.domains[home].access(&req.chain);
        cost.transfer_ns += fetch.transfer_ns;
        // Counted after the fact: a refused request never ran, so charging it a placement
        // outcome would inflate every rate by the refusal rate.
        if !cost.pending {
            if fetch.transfer_ns > 0 {
                self.remote += 1;
            } else if ran_with == 0 {
                self.cold += 1;
            } else {
                self.local += 1;
            }
        }
        // A refused request consumes nothing. Admitting its dependencies anyway would evict
        // live state to make room for work that never runs, which is the opposite of what
        // refusal is for.
        if !cost.pending && !req.requires.is_empty() {
            let shipped = self.apply_deps(home, req, &plan);
            let dep = self.domains[home].access_set(&req.requires);
            cost.transfer_ns += shipped.transfer_ns + dep.transfer_ns;
            cost.recompute_ns += dep.recompute_ns;
            cost.pending |= dep.pending;
        }
        // Charged only on a request that actually runs: a refusal does no work.
        if !cost.pending {
            let (exec, queue) = self.execute(home, req);
            cost.exec_ns = exec;
            cost.queue_ns = queue;
        }
        cost
    }

    /// What the work itself costs once its state is here. For a decode that is the engine's
    /// answer, not the request's: the same hundred tokens cost one thing in a batch of two
    /// and another in a batch of sixty.
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
        }
    }

    /// Admit a fan-out's agents, all of them or none, run them, and record where their
    /// results will come back from.
    ///
    /// Placement is sequential over agents, hardest first, and each placement is staged --
    /// its state marked as about to be resident, its bytes and decode slot reserved -- so the
    /// next sibling is priced against the node as it *will* be. That is what lets the score
    /// see both halves of co-location: the shared prefix a sibling already paid for, and the
    /// batch that sibling is about to widen. Nothing is admitted until every agent has a
    /// feasible node.
    fn serve_gang(&mut self, req: &Request, gang: &Gang) -> Cost {
        let n = gang.agents.len().max(1);
        let blobs: usize = gang.agents.iter().map(|a| a.chain.len()).sum();
        let decide_ns = self.decide(blobs);
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
            // The orchestrator waits for its slowest agent, and a refused agent never returns.
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
        // Feasibility is checked against a conservative estimate, so an admitted fan-out can
        // still lose an agent at admission. That is the same waste, reached the other way.
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
        if !cost.pending {
            self.fanouts_admitted += 1;
            self.fanout_service_ns += cost.service_ns();
        }
        cost
    }

    /// Place every agent, hardest first, staging each so its siblings see it. Returns the
    /// assignment only if every agent found a node.
    ///
    /// Without atomic admission, the agents that did fit are run anyway before returning
    /// `None`: the orchestrator waits on one that never comes, and their work is the number
    /// the baseline exists to produce.
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

    /// Where one agent goes, and the bytes it will claim there, or `None` if no node can take
    /// it given what its siblings have already reserved.
    fn place_agent(&mut self, probe: &Request, flow: &[(usize, u64)]) -> Option<(usize, Need)> {
        let feasible: Vec<(usize, Need)> = self
            .decode_pool()
            .iter()
            .filter_map(|&d| {
                let need = self.plan(d, probe).need;
                self.domains[d]
                    .could_admit(&need, &self.staged_bytes[d])
                    .then_some((d, need))
            })
            .collect();
        let need_on = |d: usize| feasible.iter().find(|(f, _)| *f == d).map(|&(_, n)| n);
        let candidates: Vec<usize> = feasible.iter().map(|&(d, _)| d).collect();
        let affinity = self.affinity_domain(&probe.chain, &candidates);
        let target = if self.placement == Placement::Scored {
            let terms: Vec<Terms> = feasible
                .iter()
                .map(|&(d, _)| self.placement_terms(d, probe, flow))
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

    /// What an unscored policy picks when some nodes cannot take the work: the same rule,
    /// applied after filtering those nodes out. That is what a Kubernetes scheduler's filter
    /// phase does and what an inference router does when it only considers endpoints able to
    /// serve the model. Without the filter, a policy that sends every sibling to one node is
    /// refused whenever they do not fit there, and it loses for a reason no real system has.
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

    /// One agent's whole turn: its context and weights, its decode, the dispatch handed to
    /// it, and each tool call it makes along the way. Tool time is added to the agent's wall
    /// time as if the sequence paused for it; the decode slot is held from admission, which
    /// overstates occupancy for an engine that parks sequences during tool calls.
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
        }
        cost
    }

    /// A function call made by an agent on `caller`. Placed like any other request, with the
    /// agent as its flow: arguments go out and the result comes back, so a remote call pays
    /// the link twice. Whether that beats restoring the function's snapshot next to the agent
    /// is exactly the trade the score exists to make.
    fn run_tool(&mut self, caller: usize, tool: &ToolCall) -> Cost {
        let probe = Self::tool_request(tool);
        let decide_ns = self.decide(probe.chain.len());
        self.decide_ns += decide_ns;
        let flow = [(caller, tool.payload_bytes), (caller, tool.payload_bytes)];
        let candidates = self.active.clone();
        let affinity = self.affinity_domain(&probe.chain, &candidates);
        let target = if self.placement == Placement::Scored {
            self.best_scored(&probe, &flow, affinity, &candidates)
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

    /// Ratio of busiest to least-busy domain occupancy. Locality-greedy placement buys hops
    /// by concentrating state, and this is what it pays with.
    #[must_use]
    pub fn domain_spread(&self) -> f64 {
        let used: Vec<u64> = self
            .active
            .iter()
            .map(|&d| self.domains[d].used())
            .collect();
        let hi = used.iter().copied().max().unwrap_or(0) as f64;
        let lo = used.iter().copied().min().unwrap_or(0).max(1) as f64;
        hi / lo
    }
}

/// The terms of a placement cost, all in nanoseconds: getting the state here by the cheapest
/// available route, what claiming the room costs whatever gets evicted, what not co-placing
/// costs in handoff, and what this node's engine occupancy costs in queueing and a wider
/// batch. Kept apart so each one's contribution can be counted rather than assumed.
#[derive(Clone, Copy, Debug)]
struct Terms {
    domain: usize,
    acquire: f64,
    fetched: bool,
    displaced: f64,
    handoff: f64,
    engine: f64,
    congestion: f64,
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

/// The cheapest route to everything a request needs at one node: how deep its chain already
/// goes here, how much of the rest a peer can supply, and which dependencies come over a
/// link rather than off the spill tier or out of a rebuild.
#[derive(Clone, Debug)]
struct Plan {
    ns: u64,
    local_depth: usize,
    chain_cut: usize,
    chain_src: Option<usize>,
    chain_bytes: u64,
    deps: Vec<(usize, usize)>,
    /// Bytes this node would have to admit, per class, which is what the displacement term
    /// prices -- per class because the classes land in different pools.
    need: Need,
}

type Need = [u64; BlobKind::N];

/// A residency question names a blob by its 32-byte id and its size.
const QUERY_BYTES_PER_BLOB: u64 = 40;
