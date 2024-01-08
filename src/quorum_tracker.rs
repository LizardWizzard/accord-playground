use std::collections::HashMap;

use crate::{
    topology::{KeyRange, Topology},
    NodeId,
};

#[derive(Debug, Clone, Copy)]
pub struct QuorumTracker {
    successes: u8,
    failures: u8,
    pending: u8,
}

impl QuorumTracker {
    pub fn new(pending: u8) -> Self {
        QuorumTracker {
            successes: 0,
            failures: 0,
            pending,
        }
    }

    fn has_quorum(&self) -> bool {
        self.successes > (self.successes + self.pending) / 2
    }

    pub fn record_outcome(&mut self, outcome: Outcome) -> bool {
        match outcome {
            Outcome::Success => self.successes += 1,
            Outcome::Failure => self.failures += 1,
        }
        self.pending -= 1;
        self.has_quorum()
    }

    fn reset(&mut self) {
        self.pending = self.pending + self.successes + self.failures;
        self.successes = 0;
        self.failures = 0;
    }
}

#[derive(Debug, Default, Clone)]
pub struct ShardQuorumTracker {
    // TODO use shard_id?
    pub shards: HashMap<KeyRange, QuorumTracker>,
}

impl ShardQuorumTracker {
    pub fn reset(&mut self) {
        self.shards.values_mut().for_each(|s| s.reset());
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Outcome {
    Success,
    Failure,
}

impl ShardQuorumTracker {
    pub fn record_outcome(
        &mut self,
        node_id: NodeId,
        topology: &Topology,
        outcome: Outcome,
    ) -> bool {
        // for each shard containing a node count the response
        let mut all_shards_made_quorum = true;

        for shard in topology.shards_for_node(node_id) {
            if let Some(quorum_tracker) = self.shards.get_mut(&shard.range) {
                all_shards_made_quorum =
                    all_shards_made_quorum && quorum_tracker.record_outcome(outcome);
            }
        }
        all_shards_made_quorum
    }
}
