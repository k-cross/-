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
    /// Domain each in-flight task's upstream stage ran in and the bytes it will hand over, so
    /// the downstream stage can be co-placed with it -- or charged for the handoff when it is
    /// not. An agent's tool result and a function's prompt bundle differ by an order of
    /// magnitude, so the payload travels with the task rather than being assumed.
    upstream: HashMap<u64, (usize, u64)>,
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
    /// How many placements each term of the score actually changed. A term that never moves
    /// a decision is not a policy, it is a comment.
    pub moved_by_displacement: u64,
    pub moved_by_flow: u64,
    pub held_by_affinity: u64,
    pub flow_requests: u64,
    pub flow_coplaced: u64,
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
            moved_by_displacement: 0,
            moved_by_flow: 0,
            held_by_affinity: 0,
            flow_requests: 0,
            flow_coplaced: 0,
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

    /// Recompute nanoseconds this request would avoid by running on `d`, and the bytes it
    /// would have to admit there. The first is what residency is worth; the second is what
    /// claiming it costs someone else.
    fn gain_and_need(&self, d: usize, req: &Request) -> (f64, u64) {
        let depth = req
            .chain
            .partition_point(|(id, _)| self.believes_resident(d, id));
        let mut gain = 0.0;
        let mut need = 0;
        for (i, (_, m)) in req.chain.iter().enumerate() {
            if i < depth {
                gain += m.recompute_ns as f64;
            } else {
                need += m.bytes;
            }
        }
        for (id, m) in &req.requires {
            if self.believes_resident(d, id) {
                gain += m.recompute_ns as f64;
            } else {
                need += m.bytes;
            }
        }
        (gain, need)
    }

    /// What placing this request on `d` is worth, net.
    ///
    /// Every previous placement policy scored only what it stood to gain, which is why each
    /// one eventually concentrated load onto whichever node already held the most bytes and
    /// made things worse. Admitting bytes a node does not have room for evicts something, and
    /// that eviction is a debt the next request pays. Pricing it is the whole difference.
    fn placement_terms(&self, d: usize, req: &Request, flow: Option<(usize, u64)>) -> Terms {
        let (gain, need) = self.gain_and_need(d, req);
        let h = &self.domains[d];
        let short = need.saturating_sub(h.dram.free_bytes());
        let displaced = short as f64 * h.dram.marginal_price();
        let handoff = match flow {
            Some((src, payload)) if src != d => {
                self.topo.fetch_ns(self.unit_in(d), src, payload) as f64
            }
            _ => 0.0,
        };
        Terms {
            domain: d,
            gain,
            displaced,
            handoff,
        }
    }

    /// Argmax of the score, plus what each term changed. Reported rather than assumed: a term
    /// worth four orders of magnitude less than another one cannot move an argmax, and saying
    /// so is more useful than shipping it and believing otherwise.
    fn best_scored(&mut self, req: &Request, flow: Option<(usize, u64)>, affinity: usize) -> usize {
        let terms: Vec<Terms> = self
            .active
            .iter()
            .map(|&d| self.placement_terms(d, req, flow))
            .collect();
        let pick = |f: &dyn Fn(&Terms) -> f64| -> usize {
            terms
                .iter()
                .max_by(|a, b| f(a).total_cmp(&f(b)))
                .map_or(0, |t| t.domain)
        };
        let raw = pick(&Terms::gain_only);
        let net = pick(&Terms::net);
        let score = |d: usize| {
            terms
                .iter()
                .find(|t| t.domain == d)
                .map_or(f64::MIN, Terms::full)
        };
        let top = pick(&Terms::full);
        // Never move without a reason. With nothing resident anywhere every score is zero,
        // and an argmax over ties would send every cold request to the same node; falling
        // back to content affinity spreads them the way a hash does.
        let full = if score(top) > score(affinity) {
            top
        } else {
            affinity
        };
        self.moved_by_displacement += u64::from(net != raw);
        self.moved_by_flow += u64::from(top != net);
        self.held_by_affinity += u64::from(full != top);
        if let Some((src, _)) = flow {
            self.flow_requests += 1;
            if full == src {
                self.flow_coplaced += 1;
            }
        }
        full
    }

    /// Bytes domain `d` really holds for this request, regardless of what the scheduler
    /// thinks. Must span exactly what `resident_value` scores, dependencies included, or a
    /// decision correctly made on weight residency is reported as stale.
    fn truly_resident(&self, d: usize, req: &Request) -> u64 {
        let depth = req
            .chain
            .partition_point(|(id, _)| self.domains[d].dram.contains(id));
        let chain_bytes: u64 = req.chain[..depth].iter().map(|(_, m)| m.bytes).sum();
        let dep_bytes: u64 = req
            .requires
            .iter()
            .filter(|(id, _)| self.domains[d].dram.contains(id))
            .map(|(_, m)| m.bytes)
            .sum();
        chain_bytes + dep_bytes
    }

    pub fn serve_request(&mut self, req: &Request) -> Cost {
        let decide_ns = self.decide(req.chain.len());
        self.decide_ns += decide_ns;
        if self.placement != Placement::Blind {
            self.sticky_unit = self.affinity_unit(&req.chain);
        }
        let flow_pair = req
            .completes
            .filter(|_| self.flow_aware)
            .and_then(|t| self.upstream.get(&t).copied());
        let scored = self.placement == Placement::Scored;
        let affinity = self.topo.units[self.sticky_unit].home as usize;
        let best = if scored {
            self.best_scored(req, flow_pair, affinity)
        } else {
            self.active
                .iter()
                .copied()
                .max_by_key(|&d| self.resident_value(d, req))
                .unwrap_or(0)
        };
        let value = self.resident_value(best, req);

        // A task's downstream stage belongs where its upstream ran: neither workload's own
        // identity hashes to the other's domain, so only a scheduler that sees the flow can
        // put them together. This overrides the placement policy for every policy, which is
        // what makes it separable from residency routing.
        // Under `Scored` the score is the whole decision: the flow's pull is already inside
        // it, priced against what co-placing would evict, and gating on `resident_value` as
        // well would discard the scored choice using a metric the score never consulted.
        let target = if scored {
            best
        } else {
            match flow_pair {
                Some((d, _)) => d,
                None => self.policy_target(affinity, best, value),
            }
        };
        let unit = self.unit_in(target);
        let home = self.topo.units[unit].home as usize;
        // Only meaningful for a policy that acted on residency: a hash placement did not
        // consult a view, so it cannot have been misled by one.
        if self.placement == Placement::Aware
            && self.resident_value(target, req) > 0
            && self.truly_resident(target, req) == 0
        {
            self.stale_decisions += 1;
        }
        // A prefix cache is node-local process state, not shared memory: a replica on one
        // node cannot read another's KV blocks. Work runs against its own node's ledger, so
        // landing in the wrong place means recomputing, not fetching remotely.
        let serving = home;

        // Only bytes that actually cross a link are charged one. State in the running node's
        // own memory is mapped, not copied: charging a local fetch made every invocation look
        // cold and drove the warm rate to zero.
        let link_ns = if serving == home {
            0
        } else {
            let bytes: u64 = req.chain.iter().map(|(_, m)| m.bytes).sum();
            self.bytes_crossed += bytes;
            self.topo.fetch_ns(unit, serving, bytes)
        };
        self.interconnect_ns += link_ns;

        self.last_handoff = 0;
        if let Some(hint) = &req.hint {
            self.upstream.insert(hint.task, (home, hint.payload_bytes));
        }
        if let Some((src, payload)) = req.completes.and_then(|t| self.upstream.remove(&t)) {
            if src == home {
                self.joined_tasks += 1;
            } else {
                self.split_tasks += 1;
                let hop = self.topo.fetch_ns(unit, src, payload);
                self.handoff_ns += hop;
                self.last_handoff = hop;
            }
        }

        let ran_with = self.truly_resident(serving, req);
        let mut cost = self.domains[serving].access(&req.chain);
        cost.decide_ns = decide_ns;
        // Counted after the fact: a refused request never ran, so charging it a placement
        // outcome would inflate every rate by the refusal rate.
        // Measured at the node that ran the work, against the truth. Scoring `best` instead
        // reported what the scheduler looked at rather than what the request found, and under
        // a stale view reported belief rather than residency.
        if !cost.pending {
            if ran_with == 0 {
                self.cold += 1;
            } else if serving == home {
                self.local += 1;
            } else {
                self.remote += 1;
            }
        }
        // A refused request consumes nothing. Admitting its dependencies anyway would evict
        // live state to make room for work that never runs, which is the opposite of what
        // refusal is for.
        if !cost.pending && !req.requires.is_empty() {
            let dep = self.domains[serving].access_set(&req.requires);
            cost.transfer_ns += dep.transfer_ns;
            cost.recompute_ns += dep.recompute_ns;
            cost.pending |= dep.pending;
        }
        cost.transfer_ns += link_ns + self.last_handoff;
        // Charged only on a request that actually runs: a refusal does no work.
        if !cost.pending {
            cost.exec_ns = req.exec_ns;
        }
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
}

/// The three terms of a placement score, in recompute nanoseconds: what running here saves,
/// what claiming the room costs whatever gets evicted, and what not co-placing costs in
/// handoff. Kept apart so each one's contribution can be counted rather than assumed.
#[derive(Clone, Copy, Debug)]
struct Terms {
    domain: usize,
    gain: f64,
    displaced: f64,
    handoff: f64,
}

impl Terms {
    fn gain_only(&self) -> f64 {
        self.gain
    }
    fn net(&self) -> f64 {
        self.gain - self.displaced
    }
    fn full(&self) -> f64 {
        self.gain - self.displaced - self.handoff
    }
}

/// A residency question names a blob by its 32-byte id and its size.
const QUERY_BYTES_PER_BLOB: u64 = 40;
