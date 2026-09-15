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

    #[must_use]
    pub fn fetch_ns(&self, bytes: u64) -> u64 {
        self.fixed_ns + (bytes as f64 * self.ns_per_byte) as u64
    }
}
