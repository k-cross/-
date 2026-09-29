use std::collections::HashMap;

use crate::blob::{BlobId, BlobMeta};

pub const BINS: usize = 12;
pub const CERTAIN: usize = 11;
pub const FULL: usize = 10;

#[derive(Clone, Copy, Debug, Default)]
pub struct Bin {
    pub n: u64,
    pub resident: u64,
    pub predicted: f64,
}

#[derive(Clone, Debug, Default)]
pub struct Calibration {
    pub bins: [Bin; BINS],
}

impl Calibration {
    pub fn record(&mut self, predicted: f64, unknown: u64, resident: bool) {
        let bin = if unknown == 0 {
            CERTAIN
        } else if predicted >= 1.0 {
            FULL
        } else {
            ((predicted * 10.0) as usize).min(9)
        };
        let b = &mut self.bins[bin];
        b.n += 1;
        b.resident += u64::from(resident);
        b.predicted += predicted;
    }

    #[must_use]
    pub fn label(bin: usize) -> String {
        match bin {
            CERTAIN => "certain".to_string(),
            FULL => "1.0".to_string(),
            b => format!("{:.1}", b as f64 / 10.0),
        }
    }

    #[must_use]
    pub fn total(&self) -> u64 {
        self.bins.iter().map(|b| b.n).sum()
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Share {
    pub inside: u64,
    pub inside_on_node: u64,
    pub outside: u64,
    pub outside_on_node: u64,
}

impl Share {
    #[must_use]
    pub fn inside_share(&self) -> f64 {
        self.inside_on_node as f64 / self.inside.max(1) as f64
    }

    #[must_use]
    pub fn outside_share(&self) -> f64 {
        self.outside_on_node as f64 / self.outside.max(1) as f64
    }
}

#[derive(Clone, Debug, Default)]
pub struct Instruments {
    pub decisions: u64,
    pub exposed_any: u64,
    pub exposed_chosen: u64,
    pub phantom_depth_blocks: u64,
    pub all: Calibration,
    pub chosen: Calibration,
    pub divergence_samples: u64,
    pub phantom_share: f64,
    pub miss_share: f64,
    pub gap_phantom: u64,
    pub gap_miss: u64,
    pub gap_discount: u64,
    pub migrations: u64,
    pub needless_migrations: u64,
    pub churn: u64,
    pub placed: u64,
    pub silence: Share,
    last_node: HashMap<BlobId, usize>,
}

impl Instruments {
    pub fn place(&mut self, chain: &[(BlobId, BlobMeta)], node: usize) {
        self.placed += 1;
        let Some(&(tip, _)) = chain.last() else {
            return;
        };
        if let Some(&before) = chain
            .iter()
            .rev()
            .find_map(|(id, _)| self.last_node.get(id))
        {
            self.churn += u64::from(before != node);
        }
        self.last_node.insert(tip, node);
    }

    #[must_use]
    pub fn gaps(&self) -> u64 {
        self.gap_phantom + self.gap_miss + self.gap_discount
    }
}
