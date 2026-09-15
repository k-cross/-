//! A machine of several memory domains, and the placement decision over them.
//!
//! The ledger in each domain decides what stays resident. This layer decides *which compute
//! unit runs the work*, and therefore what the state costs to reach. Placing by load and
//! routing by residency are the same decision; a scheduler that cannot see both makes it
//! twice, badly.

use crate::blob::{BlobId, BlobMeta};
use crate::boundary::Cost as Crossing;
use crate::cache::{Cost, Hierarchy, Policy, Quota};
use crate::tier::TierSpec;
use crate::topo::Topology;
use crate::work::Request;
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
    kv_transfer: bool,
    next_unit: usize,
    sticky_unit: usize,
    /// Domains still accepting work. Draining one leaves its state migrated elsewhere and
    /// every content hash that pointed at it stale.
    active: Vec<usize>,
    pub migrated_bytes: u64,
    /// Domain each in-flight task's upstream stage ran in, so the downstream stage can be
    /// co-placed with it -- or charged for the handoff when it is not.
    upstream: HashMap<u64, usize>,
    pub handoff_ns: u64,
    pub split_tasks: u64,
    pub joined_tasks: u64,
    last_handoff: u64,
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
    ops: u64,
    /// Latency charged to requests for deciding where to run them.
    pub decide_ns: u64,
    /// Crossings spent on control traffic, whether or not they sit on the critical path.
    pub control_rpcs: u64,
    /// Decisions where the scheduler's view of the chosen node disagreed with the truth.
    pub stale_decisions: u64,
}

impl Machine {
    #[must_use]
    pub fn new(
        topo: Topology,
        nvme_per_domain: u64,
        policy: Policy,
        quota: impl Fn(u64) -> Quota,
        placement: Placement,
    ) -> Self {
        let n_domains = topo.domains.len();
        let domains = topo
            .domains
            .iter()
            .map(|d| {
                Hierarchy::new(
                    TierSpec::dram(d.capacity),
                    TierSpec::nvme(nvme_per_domain),
                    policy,
                    quota(d.capacity),
                )
            })
            .collect();
        Self {
            topo,
            domains,
            placement,
            kv_transfer: false,
            next_unit: 0,
            sticky_unit: 0,
            active: (0..n_domains).collect(),
            migrated_bytes: 0,
            upstream: HashMap::new(),
            handoff_ns: 0,
            split_tasks: 0,
            joined_tasks: 0,
            last_handoff: 0,
            local: 0,
            remote: 0,
            cold: 0,
            interconnect_ns: 0,
            bytes_crossed: 0,
            control: Control::Unified,
            crossing: Crossing::default(),
            flow_aware: false,
            view: vec![HashSet::new(); n_domains],
            ops: 0,
            decide_ns: 0,
            control_rpcs: 0,
            stale_decisions: 0,
        }
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
            let set: HashSet<BlobId> = h.dram.resident_ids().collect();
            self.view[d] = set;
        }
        self.control_rpcs += self.domains.len() as u64;
    }

    /// Residency as the scheduler sees it, which is not always residency as it is.
    fn believes_resident(&self, d: usize, id: &BlobId) -> bool {
        match self.control {
            Control::Gossip { .. } => self.view[d].contains(id),
            Control::Unified | Control::Query => self.domains[d].dram.contains(id),
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
                if period > 0 && self.ops.is_multiple_of(period) {
                    self.refresh_view();
                }
                0
            }
        }
    }

    /// Domain holding the deepest resident prefix of this chain, and how deep.
    fn deepest(&self, chain: &[(BlobId, BlobMeta)]) -> (usize, usize) {
        let mut best = (0usize, 0usize);
        for d in 0..self.domains.len() {
            let depth = chain.partition_point(|(id, _)| self.believes_resident(d, id));
            if depth > best.1 {
                best = (d, depth);
            }
        }
        best
    }

    /// Domain a chain belongs to by content, independent of what is currently resident:
    /// a consistent hash of the chain root, which identifies the tenant or session.
    fn affinity_unit(&self, chain: &[(BlobId, BlobMeta)]) -> usize {
        let root = chain.first().map_or(0, |(id, _)| {
            u64::from_le_bytes(id.as_bytes()[..8].try_into().unwrap_or([0; 8]))
        });
        // Rendezvous hashing, not modulo: removing a domain remaps only the keys that lived
        // on it, which is what a real balancer achieves. Modulo would remap nearly every key
        // on resize and make the residency-aware arm look good for the wrong reason.
        let d = self
            .active
            .iter()
            .copied()
            .max_by_key(|&d| Self::rendezvous(root, d))
            .unwrap_or(0);
        self.unit_in(d)
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
        let moving: Vec<(BlobId, BlobMeta)> = self.domains[victim].dram.drain_all();
        for (i, (id, meta)) in moving.into_iter().enumerate() {
            let to = self.active[i % self.active.len()];
            self.migrated_bytes += meta.bytes;
            self.domains[to].reinstate(id, meta);
        }
    }

    /// The domain this policy would pick knowing nothing about flows: round-robin for
    /// `Blind`, content hash for `Sticky`, deepest resident prefix for `Aware`.
    fn policy_target(&mut self, affinity: usize, best: usize, value: u64) -> usize {
        match self.placement {
            Placement::Blind => {
                let d = self.active[self.next_unit % self.active.len()];
                self.next_unit += 1;
                d
            }
            Placement::Sticky => affinity,
            Placement::Aware => {
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
            .partition_point(|(id, _)| self.believes_resident(d, id));
        let chain_bytes: u64 = req.chain[..depth].iter().map(|(_, m)| m.bytes).sum();
        let dep_bytes: u64 = req
            .requires
            .iter()
            .filter(|(id, _)| self.believes_resident(d, id))
            .map(|(_, m)| m.bytes)
            .sum();
        chain_bytes + dep_bytes
    }

    /// Bytes domain `d` really holds for this chain, regardless of what the scheduler thinks.
    fn truly_resident(&self, d: usize, chain: &[(BlobId, BlobMeta)]) -> u64 {
        let depth = chain.partition_point(|(id, _)| self.domains[d].dram.contains(id));
        chain[..depth].iter().map(|(_, m)| m.bytes).sum()
    }

    pub fn serve_request(&mut self, req: &Request) -> Cost {
        let decide_ns = self.decide(req.chain.len());
        self.decide_ns += decide_ns;
        if self.placement != Placement::Blind {
            self.sticky_unit = self.affinity_unit(&req.chain);
        }
        let best = self
            .active
            .iter()
            .copied()
            .max_by_key(|&d| self.resident_value(d, req))
            .unwrap_or(0);
        let value = self.resident_value(best, req);

        // A task's downstream stage belongs where its upstream ran: neither workload's own
        // identity hashes to the other's domain, so only a scheduler that sees the flow can
        // put them together. This overrides the placement policy for every policy, which is
        // what makes it separable from residency routing.
        let flow_home = req
            .completes
            .filter(|_| self.flow_aware)
            .and_then(|t| self.upstream.get(&t).copied());
        let affinity = self.topo.units[self.sticky_unit].home as usize;
        let target = match flow_home {
            Some(d) => d,
            None => self.policy_target(affinity, best, value),
        };
        let unit = self.unit_in(target);
        let home = self.topo.units[unit].home as usize;
        if value > 0 && self.truly_resident(target, &req.chain) == 0 {
            self.stale_decisions += 1;
        }
        let serving = if self.kv_transfer && value > 0 {
            best
        } else {
            home
        };

        let bytes: u64 = req.chain.iter().map(|(_, m)| m.bytes).sum();
        let link_ns = self.topo.fetch_ns(unit, serving, bytes);
        self.interconnect_ns += link_ns;

        self.last_handoff = 0;
        if let Some(hint) = &req.hint {
            self.upstream.insert(hint.task, home);
        }
        if let Some(src) = req.completes.and_then(|t| self.upstream.remove(&t)) {
            if src == home {
                self.joined_tasks += 1;
            } else {
                self.split_tasks += 1;
                let hop = self
                    .topo
                    .fetch_ns(unit, src, crate::work::FLOW_PAYLOAD_BYTES);
                self.handoff_ns += hop;
                self.last_handoff = hop;
            }
        }

        let mut cost = self.domains[serving].access(&req.chain);
        cost.decide_ns = decide_ns;
        // Counted after the fact: a refused request never ran, so charging it a placement
        // outcome would inflate every rate by the refusal rate.
        if !cost.pending {
            if value == 0 {
                self.cold += 1;
            } else if serving == home {
                self.local += 1;
            } else {
                self.remote += 1;
            }
        }
        if !req.requires.is_empty() {
            let dep = self.domains[serving].access_set(&req.requires);
            cost.transfer_ns += dep.transfer_ns;
            cost.recompute_ns += dep.recompute_ns;
            cost.pending |= dep.pending;
        }
        cost.transfer_ns += link_ns + self.last_handoff;
        cost
    }

    pub fn serve(&mut self, chain: &[(BlobId, BlobMeta)]) -> Cost {
        let decide_ns = self.decide(chain.len());
        self.decide_ns += decide_ns;
        if self.placement != Placement::Blind {
            self.sticky_unit = self.affinity_unit(chain);
        }
        let (held, depth) = self.deepest(chain);
        // With nothing resident anywhere the placement policy picks freely; with state on the
        // floor, `held` is where the work wants to run.
        // Content affinity, not load balance: spreading cold chains by bytes scatters a
        // tenant's sessions and destroys the locality this policy exists to capture.
        let affinity = self.topo.units[self.sticky_unit].home as usize;
        let target = self.policy_target(affinity, held, depth as u64);
        let unit = self.unit_in(target);
        let home = self.topo.units[unit].home as usize;
        if depth > 0 && self.truly_resident(target, chain) == 0 {
            self.stale_decisions += 1;
        }

        // A prefix cache is node-local process state, not shared memory: a replica on one
        // domain cannot read another's KV blocks even over a coherent link. Work therefore
        // runs against its *own* domain's ledger, and landing in the wrong place means
        // recomputing, not fetching remotely. Enable `kv_transfer` to model an explicit
        // cross-domain KV move instead.
        let serving = if self.kv_transfer && depth > 0 {
            held
        } else {
            home
        };
        let bytes: u64 = chain.iter().map(|(_, m)| m.bytes).sum();
        let link_ns = self.topo.fetch_ns(unit, serving, bytes);

        if depth == 0 {
            self.cold += 1;
        } else if serving == home {
            self.local += 1;
        } else {
            self.remote += 1;
            self.bytes_crossed += bytes;
        }
        self.interconnect_ns += link_ns;

        let mut cost = self.domains[serving].access(chain);
        cost.transfer_ns += link_ns;
        cost.decide_ns = decide_ns;
        cost
    }

    /// Ratio of busiest to least-busy domain occupancy. Locality-greedy placement buys hops
    /// by concentrating state, and this is what it pays with.
    #[must_use]
    pub fn domain_spread(&self) -> f64 {
        let used: Vec<u64> = self
            .active
            .iter()
            .map(|&d| self.domains[d].dram.used())
            .collect();
        let hi = used.iter().copied().max().unwrap_or(0) as f64;
        let lo = used.iter().copied().min().unwrap_or(0).max(1) as f64;
        hi / lo
    }

    /// Allow a chain to be served from a remote domain over a coherent link, modelling an
    /// explicit cross-domain KV transfer rather than a local recompute.
    pub fn set_kv_transfer(&mut self, on: bool) {
        self.kv_transfer = on;
    }

    #[must_use]
    pub fn local_fraction(&self) -> f64 {
        let n = self.local + self.remote;
        if n == 0 {
            0.0
        } else {
            self.local as f64 / n as f64
        }
    }
}

/// A residency question names a blob by its 32-byte id and its size.
const QUERY_BYTES_PER_BLOB: u64 = 40;
