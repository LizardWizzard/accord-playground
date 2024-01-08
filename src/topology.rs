use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    ops::Deref,
};

use crate::{transaction::Key, NodeId};

pub struct Shard {
    pub range: KeyRange,
    pub node_ids: HashSet<NodeId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ShardId(usize);

/// Represents a key range interval where in form of [lo, hi).
/// In other words hi key is not included in the range.
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct KeyRange {
    lo: Key,
    hi: Key,
}

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
    pub fn shard_for_key(&self, key: &Key) -> ShardRef {
        let idx = self
            .shards
            .binary_search_by(|shard| {
                if &shard.range.lo <= key && key < &shard.range.hi {
                    Ordering::Equal
                } else if key < &shard.range.lo {
                    Ordering::Less
                } else {
                    // key >= range.hi
                    Ordering::Greater
                }
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
