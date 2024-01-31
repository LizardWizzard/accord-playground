use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    ops::Deref,
};

use crate::{transaction::Key, NodeId};

#[derive(Debug, Clone)]
pub struct Shard {
    pub range: KeyRange,
    pub node_ids: HashSet<NodeId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ShardId(pub(crate) usize);

/// Represents a key range interval where in form of [lo, hi).
/// In other words hi key is not included in the range.
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct KeyRange {
    pub lo: Key,
    pub hi: Key,
}

#[derive(Clone)]
pub struct Topology {
    // contains sorted non intersecting ranges
    shards: Vec<Shard>,
    // Here Vec contains indexes into shard Vec above
    node_to_shards: HashMap<NodeId, Vec<ShardId>>,
}

pub struct ShardRef<'a> {
    shard: &'a Shard,
    id: ShardId,
}

impl<'a> ShardRef<'a> {
    pub fn id(&self) -> ShardId {
        self.id
    }
}

impl<'a> Deref for ShardRef<'a> {
    type Target = Shard;

    fn deref(&self) -> &Self::Target {
        self.shard
    }
}

impl Topology {
    pub fn new(shards: Vec<Shard>, node_to_shards: HashMap<NodeId, Vec<ShardId>>) -> Self {
        Self {
            shards,
            node_to_shards,
        }
    }

    pub fn shard_for_key(&self, key: &Key) -> ShardRef {
        let idx = self
            .shards
            .binary_search_by(|shard| {
                let o = if &shard.range.lo <= key && key < &shard.range.hi {
                    Ordering::Equal
                } else if key < &shard.range.lo {
                    Ordering::Greater
                } else {
                    // key >= range.hi
                    Ordering::Less
                };

                // dbg!(&shard.range.lo, key, &shard.range.hi, o);
                o
            })
            .expect("we cover whole key range from min to max");

        ShardRef {
            shard: &self.shards[idx],
            id: ShardId(idx),
        }
    }

    pub fn shards_for_node(&self, node_id: NodeId) -> Vec<&Shard> {
        // TODO: with capacity can help if we now how many shards each node contains
        // TODO: we can reuse single allocattion if vec is passed as an argument
        self.node_to_shards[&node_id]
            .iter()
            .map(|shard_idx| &self.shards[shard_idx.0])
            .collect()
    }

    /// Shard id is given to outside world when actual shard exists
    /// so it serves as a proof that such a shard exists, so there
    /// is no need to return option
    pub fn shard_by_id(&self, id: ShardId) -> &Shard {
        &self.shards[id.0]
    }
}

// Reconsider when going through next phases of the protocol:
// TODO: should we have shard ids as well? the question is how to efficiently translate
//       NodeId to Shards that belong to them and vice versa. Just two hashmaps or?
// TODO: How original Topology and ShardQuorumTracker solve this is rather unclear.
//       supersetIndexes in Topology represents the same thing but it is quite entangled

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use crate::{transaction::Key, NodeId};

    use super::{KeyRange, Shard, ShardId, Topology};

    #[test]
    fn key_to_shard() {
        let shards = vec![
            Shard {
                range: KeyRange {
                    lo: Key(0),
                    hi: Key(10),
                },
                node_ids: HashSet::from([NodeId(1), NodeId(2), NodeId(3)]),
            },
            Shard {
                range: KeyRange {
                    lo: Key(10),
                    hi: Key(20),
                },
                node_ids: HashSet::from([NodeId(4), NodeId(5), NodeId(6)]),
            },
        ];

        let topology = Topology::new(
            shards,
            HashMap::from([
                (NodeId(1), vec![ShardId(0)]),
                (NodeId(2), vec![ShardId(0)]),
                (NodeId(3), vec![ShardId(0)]),
                (NodeId(4), vec![ShardId(1)]),
                (NodeId(5), vec![ShardId(1)]),
                (NodeId(6), vec![ShardId(1)]),
            ]),
        );

        topology.shard_for_key(&Key(0));
    }
}
