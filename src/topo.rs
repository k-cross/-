//! Compute units, memory domains, and the links between them.
//!
//! The ledger decides *what* stays resident; the topology decides *where* it is and what it
//! costs to reach from a given compute unit. Placement and routing are one decision only if
//! both are visible to the same scheduler.

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum UnitKind {
    Performance,
    Efficiency,
    Accelerator,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DomainKind {
    /// Directly attached, load/store reachable.
    Dram,
    /// Device-attached; reachable only by DMA.
    Vram,
    /// Another socket or host: coherent over a fabric, or not at all.
    Remote,
}

#[derive(Clone, Copy, Debug)]
pub struct ComputeUnit {
    pub id: u8,
    pub kind: UnitKind,
    pub cluster: u8,
    /// Domain this unit reaches with no interconnect hop.
    pub home: u8,
}

#[derive(Clone, Copy, Debug)]
pub struct MemoryDomain {
    pub id: u8,
    pub kind: DomainKind,
    pub capacity: u64,
}

/// A path from a compute unit to a memory domain.
#[derive(Clone, Copy, Debug)]
pub struct Link {
    pub latency_ns: u64,
    pub ns_per_byte: f64,
    /// Coherent links are traversed by reference; non-coherent ones require a copy. This is
    /// the single most consequential bit in the graph -- it decides whether co-placement
    /// saves a pointer dereference or a full materialisation.
    pub coherent: bool,
}

impl Link {
    #[must_use]
    pub fn cost_ns(&self, bytes: u64) -> u64 {
        self.latency_ns + (bytes as f64 * self.ns_per_byte) as u64
    }

    #[must_use]
    pub fn local(ns_per_byte: f64) -> Self {
        Self {
            latency_ns: 0,
            ns_per_byte,
            coherent: true,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Topology {
    pub units: Vec<ComputeUnit>,
    pub domains: Vec<MemoryDomain>,
    links: Vec<Link>,
}

impl Topology {
    /// # Panics
    /// Panics if `links` is not exactly `units.len() * domains.len()` entries.
    #[must_use]
    pub fn new(units: Vec<ComputeUnit>, domains: Vec<MemoryDomain>, links: Vec<Link>) -> Self {
        assert_eq!(
            links.len(),
            units.len() * domains.len(),
            "link matrix shape"
        );
        Self {
            units,
            domains,
            links,
        }
    }

    #[must_use]
    pub fn link(&self, unit: usize, domain: usize) -> &Link {
        &self.links[unit * self.domains.len() + domain]
    }

    #[must_use]
    pub fn fetch_ns(&self, unit: usize, domain: usize, bytes: u64) -> u64 {
        self.link(unit, domain).cost_ns(bytes)
    }

    /// Unit that can reach this blob set most cheaply given where it already lives.
    #[must_use]
    pub fn best_unit(&self, placed: &[(usize, u64)]) -> usize {
        (0..self.units.len())
            .min_by_key(|&u| {
                placed
                    .iter()
                    .map(|&(d, bytes)| self.fetch_ns(u, d, bytes))
                    .sum::<u64>()
            })
            .unwrap_or(0)
    }

    /// A machine with `sockets` memory domains and `per_socket` units each, plus an optional
    /// non-coherent accelerator domain. Link constants are **modelled**, not measured --
    /// re-derive them per host before trusting any result that depends on them.
    #[must_use]
    pub fn synthetic(sockets: usize, per_socket: usize, dram_per_socket: u64) -> Self {
        let mut units = Vec::new();
        let mut domains = Vec::new();
        for s in 0..sockets {
            domains.push(MemoryDomain {
                id: s as u8,
                kind: DomainKind::Dram,
                capacity: dram_per_socket,
            });
            for i in 0..per_socket {
                units.push(ComputeUnit {
                    id: (s * per_socket + i) as u8,
                    kind: UnitKind::Performance,
                    cluster: s as u8,
                    home: s as u8,
                });
            }
        }
        let mut links = Vec::with_capacity(units.len() * domains.len());
        for u in &units {
            for d in &domains {
                links.push(if u.home == d.id {
                    Link::local(1.0 / 28.0)
                } else {
                    // Cross-socket: UPI/Infinity-Fabric class -- coherent, but ~2x the
                    // per-byte cost and a real hop latency.
                    Link {
                        latency_ns: 120,
                        ns_per_byte: 1.0 / 14.0,
                        coherent: true,
                    }
                });
            }
        }
        Self::new(units, domains, links)
    }
}

impl Topology {
    /// Probe the host. Reports what the machine actually exposes -- which on a unified-memory
    /// laptop is one memory domain and two compute clusters, and on a multi-socket server is
    /// the kernel's NUMA node set with its own distance matrix.
    #[must_use]
    pub fn discover() -> Self {
        use crate::plat::{numa_nodes, sysctl_u64};

        let nodes = numa_nodes();
        let memsize = sysctl_u64("hw.memsize").unwrap_or(0);
        let domains: Vec<MemoryDomain> = nodes
            .iter()
            .map(|(i, _)| MemoryDomain {
                id: *i,
                kind: DomainKind::Dram,
                capacity: if nodes.len() == 1 {
                    memsize
                } else {
                    memsize / nodes.len() as u64
                },
            })
            .collect();

        let perf = sysctl_u64("hw.perflevel0.physicalcpu").unwrap_or(0);
        let eff = sysctl_u64("hw.perflevel1.physicalcpu").unwrap_or(0);
        let mut units = Vec::new();
        if perf > 0 || eff > 0 {
            for i in 0..perf {
                units.push(ComputeUnit {
                    id: i as u8,
                    kind: UnitKind::Performance,
                    cluster: 0,
                    home: 0,
                });
            }
            for i in 0..eff {
                units.push(ComputeUnit {
                    id: (perf + i) as u8,
                    kind: UnitKind::Efficiency,
                    cluster: 1,
                    home: 0,
                });
            }
        } else {
            let n = std::thread::available_parallelism().map_or(1, std::num::NonZero::get);
            for i in 0..n {
                let home = (i * domains.len() / n) as u8;
                units.push(ComputeUnit {
                    id: i as u8,
                    kind: UnitKind::Performance,
                    cluster: home,
                    home,
                });
            }
        }

        // Kernel distances are relative (10 == local); scale them against a local link until
        // the real per-byte costs are measured on the host.
        let mut links = Vec::with_capacity(units.len() * domains.len());
        for u in &units {
            for d in &domains {
                let rel = nodes
                    .get(u.home as usize)
                    .and_then(|(_, dist)| dist.get(d.id as usize).copied())
                    .unwrap_or(if u.home == d.id { 10 } else { 20 });
                let scale = f64::from(rel) / 10.0;
                links.push(Link {
                    latency_ns: if u.home == d.id { 0 } else { 120 },
                    ns_per_byte: scale / 28.0,
                    coherent: true,
                });
            }
        }
        Self::new(units, domains, links)
    }
}
