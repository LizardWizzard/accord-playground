use std::collections::HashSet;
use std::hash::{BuildHasher, Hash};
use std::marker::PhantomData;

use hashbrown::raw::Bucket;
use hashbrown::{Equivalent, HashMap};

mod coordinator;
pub mod messages;
pub mod node;
pub mod quorum_tracker;
mod replica;
pub mod timestamp;
pub mod topology;
pub mod transaction;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(u16);

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("N{}", self.0))
    }
}

// The problem this object aims to solve is that we cant mutably borrow different hashmap entities at the same time
// because rustc cant see that they are different thus it guards us from mutably borrowing one entity several times.
// Since we can guarantee that we wont borrow the same entity twice we can safely mutate different ones at the same time.
struct Lens<'a, 'b, K, V> {
    root_key: K,
    other_keys: &'a HashSet<K>,
    source: &'b mut HashMap<K, V>,
}

impl<'a, 'b, K, V> Lens<'a, 'b, K, V>
where
    K: Eq + Hash,
{
    // TODO there can be a trait that provides a method that returns id and a unique sequence that are guaranteed to be unique together
    // I e Apply message can easily implement it, because transaction cant depend on itself thus Apply.txn_id
    // and Apply.dependencies are guaranteed to be unique together
    // TODO clarify that guards are needed because if these methods exist on Lens itself we must have shared reference as self parameter
    // which is wrong because this way we'd be able to invoke several for_each_mut iterators inside of each other which would end
    // up in multiple mutable references existing for the same memory location which violates safety
    fn new(
        root_key: K,
        other_keys: &'a HashSet<K>,
        source: &'b mut HashMap<K, V>,
    ) -> Option<(LensRootGuard<'b, K, V>, LensIterGuard<'a, 'b, K, V>)> {
        if other_keys.contains(&root_key) {
            return None;
        }

        let hash = source.hasher().hash_one(&root_key);
        let root_bucket = source
            .raw_table()
            .find(hash, |(k, _)| k.equivalent(&root_key))
            .expect("TODO");

        Some((
            LensRootGuard {
                bucket: root_bucket,
                _phantom: PhantomData,
            },
            LensIterGuard {
                other_keys,
                source,
                root_key,
            },
        ))
    }
}

struct LensRootGuard<'a, K, V> {
    bucket: Bucket<(K, V)>,
    _phantom: PhantomData<&'a ()>,
}

impl<'b, K, V> LensRootGuard<'b, K, V>
where
    K: Eq + Hash,
{
    fn with_mut<U>(&mut self, f: impl FnOnce(&mut V) -> U) -> U {
        unsafe {
            let root = self.bucket.as_mut();
            f(&mut root.1)
        }
    }
}

struct LensIterGuard<'a, 'b, K, V> {
    root_key: K,
    other_keys: &'a HashSet<K>,
    source: &'b mut HashMap<K, V>,
}
impl<'a, 'b, K, V> LensIterGuard<'a, 'b, K, V>
where
    K: Eq + Hash,
{
    fn for_each_mut(&self, mut f: impl FnMut(&K, &mut V)) {
        for key in self.other_keys {
            let hash = self.source.hasher().hash_one(key);
            let entry = self
                .source
                .raw_table()
                .find(hash, |(k, _)| k.equivalent(key))
                .expect("TODO");

            unsafe {
                let (k, v) = entry.as_mut();
                f(k, v)
            }
        }
    }

    fn exchange(self, other_keys: &'a HashSet<K>) -> Option<Self> {
        if other_keys.contains(&self.root_key) {
            return None;
        }

        Some(Self {
            root_key: self.root_key,
            other_keys: other_keys,
            source: self.source,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use tracing::info;
    use tracing_subscriber;

    use crate::{
        coordinator::Coordinator,
        messages::{
            Accept, AcceptOk, Apply, Commit, CommitAndRead, EitherCommitOrAccept, NewTransaction,
            PreAccept, PreAcceptOk, Read, ReadOk,
        },
        node::{DataStore, Node},
        replica::Replica,
        timestamp::TimestampProvider,
        topology::{KeyRange, Shard, ShardId, Topology},
        transaction::{Key, TransactionBody, Value},
        NodeId,
    };

    #[derive(Debug)]
    enum Message {
        NewTransaction(NewTransaction),
        PreAccept(PreAccept),
        PreAcceptOk(PreAcceptOk),
        Commit(Commit),
        Accept(Accept),
        AcceptOk(AcceptOk),
        Read(Read),
        ReadOk(ReadOk),
        Apply(Apply),
    }

    struct Harness {
        pub nodes: HashMap<NodeId, Node>,
        pub topology: Topology,
    }

    impl Harness {
        fn new() -> Self {
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

            let mut nodes = HashMap::new();
            for id in 1..=6 {
                let timestamp_provider = TimestampProvider::new(NodeId(id));

                let node = Node::new(
                    NodeId(id),
                    topology.clone(),
                    timestamp_provider,
                    Coordinator::default(),
                    Replica::default(),
                    DataStore::default(),
                );
                nodes.insert(NodeId(id), node);
            }

            Harness { nodes, topology }
        }

        fn decompose_commit_and_read(
            messages: &mut Vec<(NodeId, NodeId, Message)>,
            mut commit_and_read: CommitAndRead,
            src_node: NodeId,
        ) {
            for (node_id, commit) in commit_and_read.commits.drain() {
                messages.push((src_node, node_id, Message::Commit(commit)))
            }

            for (node_id, read) in commit_and_read.reads.drain(..) {
                messages.push((src_node, node_id, Message::Read(read)))
            }
        }

        fn run(&mut self, initial_messages: Vec<(NodeId, NodeId, Message)>) {
            let mut messages = initial_messages;

            while !messages.is_empty() {
                let mut new_messages = vec![];

                // let push = || new_messages.push(value);

                for (src_node, dst_node, message) in messages.drain(..) {
                    info!("{:?} --> {:?}: {:?}", src_node, dst_node, message);
                    let node = self.nodes.get_mut(&dst_node).expect("cant be missing");
                    match message {
                        Message::NewTransaction(new_transaction) => {
                            for (node_id, message) in node.receive_new_transaction(new_transaction)
                            {
                                new_messages.push((dst_node, node_id, Message::PreAccept(message)))
                            }
                        }
                        Message::PreAccept(pre_accept) => {
                            new_messages.push((
                                dst_node,
                                src_node,
                                Message::PreAcceptOk(node.receive_pre_accept(pre_accept)),
                            ));
                        }
                        Message::PreAcceptOk(pre_accept_ok) => {
                            if let Some(reply) = node.receive_pre_accept_ok(src_node, pre_accept_ok)
                            {
                                match reply {
                                    EitherCommitOrAccept::Commit(commit_and_read) => {
                                        Self::decompose_commit_and_read(
                                            &mut new_messages,
                                            commit_and_read,
                                            dst_node,
                                        )
                                    }
                                    EitherCommitOrAccept::Accept(mut accept) => {
                                        for (node_id, commit) in accept.drain() {
                                            new_messages.push((
                                                dst_node,
                                                node_id,
                                                Message::Accept(commit),
                                            ))
                                        }
                                    }
                                }
                            }
                        }
                        Message::Commit(commit) => node.receive_commit(commit),
                        Message::Accept(accept) => new_messages.push((
                            dst_node,
                            src_node,
                            Message::AcceptOk(node.receive_accept(accept)),
                        )),
                        Message::AcceptOk(accept_ok) => {
                            if let Some(commit_and_read) =
                                node.receive_accept_ok(src_node, accept_ok)
                            {
                                Self::decompose_commit_and_read(
                                    &mut new_messages,
                                    commit_and_read,
                                    dst_node,
                                )
                            }
                        }
                        Message::Read(read) => {
                            if let Some(read_ok) = node.receive_read(src_node, read) {
                                new_messages.push((dst_node, src_node, Message::ReadOk(read_ok)))
                            }
                        }
                        Message::ReadOk(read_ok) => {
                            if let Some(mut apply) = node.receive_read_ok(src_node, read_ok) {
                                {
                                    let (_, apply) = apply.first().unwrap();
                                    info!(
                                        "Responding to client: {:?}: {:?}",
                                        apply.txn_id, apply.result
                                    );
                                }
                                for (node_id, apply) in apply.drain(..) {
                                    new_messages.push((dst_node, node_id, Message::Apply(apply)))
                                }
                            }
                        }
                        Message::Apply(apply) => {
                            for (node_id, read_ok) in node.receive_apply(src_node, apply) {
                                new_messages.push((dst_node, node_id, Message::ReadOk(read_ok)))
                            }
                        }
                    }
                }

                messages = new_messages;
            }
        }
    }

    #[test]
    fn one_transaction() {
        tracing_subscriber::fmt::init();

        let mut harness = Harness::new();
        for node_id in &harness.topology.shard_for_key(&Key(1)).node_ids {
            harness
                .nodes
                .get_mut(node_id)
                .unwrap()
                .data_store_mut()
                .insert(Key(1), Value(1));
        }

        for node_id in &harness.topology.shard_for_key(&Key(11)).node_ids {
            harness
                .nodes
                .get_mut(node_id)
                .unwrap()
                .data_store_mut()
                .insert(Key(11), Value(11));
        }

        harness.run(vec![(
            NodeId(0),
            NodeId(1),
            Message::NewTransaction(NewTransaction {
                body: TransactionBody {
                    keys: HashSet::from([Key(1), Key(11)]),
                },
            }),
        )]);
    }
}
