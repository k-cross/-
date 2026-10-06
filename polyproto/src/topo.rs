#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum UnitKind {
    Performance,
    Efficiency,
    Accelerator,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DomainKind {
    Dram,

    Vram,

    Remote,
}

#[derive(Clone, Copy, Debug)]
pub struct ComputeUnit {
    pub id: u8,
    pub kind: UnitKind,
    pub cluster: u8,

    pub home: u8,
}

#[derive(Clone, Copy, Debug)]
pub struct MemoryDomain {
    pub id: u8,
    pub kind: DomainKind,
    pub capacity: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct Link {
    pub latency_ns: u64,
    pub ns_per_byte: f64,

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

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Distance {
    Socket,

    Rack,

    Zone,

    Region,
}

impl Distance {
    #[must_use]
    pub fn one_way_ns(self) -> u64 {
        match self {
            Self::Socket => 120,
            Self::Rack => 30_000,
            Self::Zone => 400_000,
            Self::Region => 30_000_000,
        }
    }

    #[must_use]
    pub fn ns_per_byte(self) -> f64 {
        match self {
            Self::Socket => 1.0 / 14.0,
            Self::Rack => 0.32,
            Self::Zone => 0.80,
            Self::Region => 1.00,
        }
    }

    #[must_use]
    pub fn coherent(self) -> bool {
        self == Self::Socket
    }

    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Socket => "socket",
            Self::Rack => "rack",
            Self::Zone => "zone",
            Self::Region => "region",
        }
    }

    #[must_use]
    pub fn all() -> [Self; 4] {
        [Self::Socket, Self::Rack, Self::Zone, Self::Region]
    }
}

impl std::str::FromStr for Distance {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "socket" => Ok(Self::Socket),
            "rack" => Ok(Self::Rack),
            "zone" => Ok(Self::Zone),
            "region" => Ok(Self::Region),
            _ => Err(format!(
                "unknown distance {s}; want socket|rack|zone|region"
            )),
        }
    }
}

impl Topology {
    #[must_use]
    pub fn regions(
        per_region: usize,
        units_per_node: usize,
        dram_per_node: u64,
        within: Distance,
        one_way_ns: &[Vec<u64>],
        crossing: crate::boundary::Cost,
    ) -> Self {
        let nodes = one_way_ns.len() * per_region;
        assert!(nodes * units_per_node <= 256, "unit ids are a byte");
        let region_of = |n: usize| n / per_region;
        let mut units = Vec::new();
        let mut domains = Vec::new();
        for n in 0..nodes {
            domains.push(MemoryDomain {
                id: n as u8,
                kind: if within.coherent() {
                    DomainKind::Dram
                } else {
                    DomainKind::Remote
                },
                capacity: dram_per_node,
            });
            for i in 0..units_per_node {
                units.push(ComputeUnit {
                    id: (n * units_per_node + i) as u8,
                    kind: UnitKind::Performance,
                    cluster: n as u8,
                    home: n as u8,
                });
            }
        }
        let mut links = Vec::with_capacity(units.len() * domains.len());
        for u in &units {
            for dom in &domains {
                let (from, to) = (
                    region_of(usize::from(u.home)),
                    region_of(usize::from(dom.id)),
                );
                links.push(if u.home == dom.id {
                    Link::local(1.0 / 28.0)
                } else if from == to {
                    Link {
                        latency_ns: within.one_way_ns() + crossing.fixed_ns as u64,
                        ns_per_byte: within.ns_per_byte() + crossing.ns_per_byte,
                        coherent: within.coherent(),
                    }
                } else {
                    Link {
                        latency_ns: one_way_ns[from][to] + crossing.fixed_ns as u64,
                        ns_per_byte: Distance::Region.ns_per_byte() + crossing.ns_per_byte,
                        coherent: false,
                    }
                });
            }
        }
        Self::new(units, domains, links)
    }

    #[must_use]
    pub fn cluster(
        nodes: usize,
        units_per_node: usize,
        dram_per_node: u64,
        d: Distance,
        crossing: crate::boundary::Cost,
    ) -> Self {
        let mut units = Vec::new();
        let mut domains = Vec::new();
        for n in 0..nodes {
            domains.push(MemoryDomain {
                id: n as u8,
                kind: if d.coherent() {
                    DomainKind::Dram
                } else {
                    DomainKind::Remote
                },
                capacity: dram_per_node,
            });
            for i in 0..units_per_node {
                units.push(ComputeUnit {
                    id: (n * units_per_node + i) as u8,
                    kind: UnitKind::Performance,
                    cluster: n as u8,
                    home: n as u8,
                });
            }
        }
        let mut links = Vec::with_capacity(units.len() * domains.len());
        for u in &units {
            for dom in &domains {
                links.push(if u.home == dom.id {
                    Link::local(1.0 / 28.0)
                } else {
                    Link {
                        latency_ns: d.one_way_ns() + crossing.fixed_ns as u64,
                        ns_per_byte: d.ns_per_byte() + crossing.ns_per_byte,
                        coherent: d.coherent(),
                    }
                });
            }
        }
        Self::new(units, domains, links)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::boundary::Cost;

    fn latency(topo: &Topology, from: usize, to: usize) -> u64 {
        topo.link(from, to).latency_ns
    }

    #[test]
    fn a_link_costs_the_distance_of_the_regions_it_joins() {
        let wan = vec![vec![0, 30_000_000], vec![30_000_000, 0]];
        let topo = Topology::regions(2, 1, 1 << 30, Distance::Rack, &wan, Cost::default());
        assert_eq!(latency(&topo, 0, 0), 0);
        assert_eq!(latency(&topo, 0, 1), Distance::Rack.one_way_ns());
        assert_eq!(latency(&topo, 0, 2), 30_000_000);
        assert_eq!(latency(&topo, 3, 1), 30_000_000);
        assert!(!topo.link(0, 2).coherent);
    }

    #[test]
    fn one_region_is_the_cluster_at_that_distance() {
        let regions = Topology::regions(4, 2, 1 << 30, Distance::Rack, &[vec![0]], Cost::default());
        let cluster = Topology::cluster(4, 2, 1 << 30, Distance::Rack, Cost::default());
        assert_eq!(regions.units.len(), cluster.units.len());
        for u in 0..regions.units.len() {
            for d in 0..regions.domains.len() {
                let (a, b) = (regions.link(u, d), cluster.link(u, d));
                assert_eq!(a.latency_ns, b.latency_ns);
                assert!((a.ns_per_byte - b.ns_per_byte).abs() < f64::EPSILON);
                assert_eq!(a.coherent, b.coherent);
            }
        }
    }
}
