use std::collections::{HashMap, VecDeque};

use crate::blob::BlobId;
use crate::work::Request;

#[derive(Clone, Debug, Default)]
pub struct Foresight {
    uses: HashMap<BlobId, Vec<u64>>,
}

impl Foresight {
    #[must_use]
    pub fn of(trace: &[Request]) -> Self {
        let mut uses: HashMap<BlobId, Vec<u64>> = HashMap::new();
        for (pos, req) in trace.iter().enumerate() {
            let pos = pos as u64;
            let agents = req
                .gang
                .iter()
                .flat_map(|g| g.agents.iter().map(|a| &a.chain));
            for chain in std::iter::once(&req.chain).chain(agents) {
                for (id, _) in chain {
                    let seen = uses.entry(*id).or_default();
                    if seen.last() != Some(&pos) {
                        seen.push(pos);
                    }
                }
            }
        }
        Self { uses }
    }

    #[must_use]
    pub fn next_use(&self, id: &BlobId, after: u64) -> Option<u64> {
        let uses = self.uses.get(id)?;
        uses.get(uses.partition_point(|&p| p <= after)).copied()
    }

    #[must_use]
    pub fn schedule(&self) -> HashMap<BlobId, VecDeque<u64>> {
        self.uses
            .iter()
            .map(|(id, uses)| (*id, uses.iter().copied().collect()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::work::Workload;

    #[test]
    fn next_use_is_the_first_position_strictly_after() {
        let trace: Vec<Request> = Workload::with_fanout(1, 400, 1.0, 0.2).collect();
        let f = Foresight::of(&trace);
        let (id, _) = trace
            .iter()
            .find_map(|r| r.chain.first().copied())
            .expect("a chain");
        let uses: Vec<u64> = trace
            .iter()
            .enumerate()
            .filter(|(_, r)| {
                r.chain.iter().any(|(b, _)| *b == id)
                    || r.gang.iter().any(|g| {
                        g.agents
                            .iter()
                            .any(|a| a.chain.iter().any(|(b, _)| *b == id))
                    })
            })
            .map(|(p, _)| p as u64)
            .collect();
        assert!(uses.len() > 1);
        assert_eq!(f.next_use(&id, uses[0]), Some(uses[1]));
        assert_eq!(f.next_use(&id, *uses.last().unwrap_or(&0)), None);
        assert_eq!(f.schedule()[&id].iter().copied().collect::<Vec<_>>(), uses);
    }
}
