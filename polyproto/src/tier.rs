/// Declared in eviction order, so `own.rs` can key its authority table on `(kind, tier)`
/// without that pair also having to name a node.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Tier {
    Hbm,
    Ddr,
    Nvme,
}

// Constants fitted from `polyphonic calibrate` on darwin/arm64 with direct I/O; re-derive per host.
#[derive(Clone, Copy, Debug)]
pub struct TierSpec {
    pub capacity: u64,
    pub fixed_ns: u64,
    pub ns_per_byte: f64,
}

impl TierSpec {
    #[must_use]
    pub fn dram(capacity: u64) -> Self {
        Self {
            capacity,
            fixed_ns: 0,
            ns_per_byte: 1.0 / 28.0,
        }
    }

    #[must_use]
    pub fn nvme(capacity: u64) -> Self {
        Self {
            capacity,
            fixed_ns: 113_000,
            ns_per_byte: 1.0 / 7.8,
        }
    }

    /// Accelerator memory. Nothing is ever read *from* it by the ledger -- state there is hot
    /// by definition -- so only its capacity matters.
    #[must_use]
    pub fn hbm(capacity: u64) -> Self {
        Self {
            capacity,
            fixed_ns: 0,
            ns_per_byte: 0.0,
        }
    }

    /// Moving bytes between host DDR and accelerator HBM: a pinned-memory copy over `PCIe`
    /// Gen4 x16, about 25 `GiB/s` in practice, plus the fixed cost of launching and
    /// synchronising the copy. **Modelled**; nothing on this host has the link to measure.
    #[must_use]
    pub fn pcie() -> Self {
        Self {
            capacity: 0,
            fixed_ns: 50_000,
            ns_per_byte: 0.04,
        }
    }

    #[must_use]
    pub fn fetch_ns(&self, bytes: u64) -> u64 {
        self.fixed_ns + (bytes as f64 * self.ns_per_byte) as u64
    }
}
