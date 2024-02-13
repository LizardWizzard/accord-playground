mod coordinator;
pub mod messages;
pub mod node;
pub mod quorum_tracker;
mod replica;
pub mod timestamp;
pub mod topology;
pub mod transaction;

use std::hash::Hash;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(u16);

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("N{}", self.0))
    }
}

#[cfg(any(test, kani))]
mod harness {
    use std::hash::Hash;

    use tracing::info;

    use crate::collections::{Map, Set};

    pub use super::{
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

    #[cfg(not(kani))]
    mod log {
        use once_cell::sync::OnceCell;
        pub static LOG_HANDLE: OnceCell<()> = OnceCell::new();
    }

    #[derive(Debug)]
    pub enum Message {
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

    pub struct Harness {
        pub nodes: Map<NodeId, Node>,
        pub topology: Topology,
    }

    // vector map doesnt have corresponding from impl :(
    pub fn set_from_iter<T: Hash + Eq, const N: usize>(a: [T; N]) -> Set<T> {
        let mut s = Set::new();
        for t in a {
            s.insert(t);
        }
        s
    }

    impl Harness {
        pub fn new() -> Self {
            let shards = vec![
                Shard {
                    range: KeyRange {
                        lo: Key(0),
                        hi: Key(10),
                    },
                    node_ids: set_from_iter([NodeId(1), NodeId(2), NodeId(3)]),
                },
                Shard {
                    range: KeyRange {
                        lo: Key(10),
                        hi: Key(20),
                    },
                    node_ids: set_from_iter([NodeId(4), NodeId(5), NodeId(6)]),
                },
            ];

            let mut node_to_shards = Map::new();
            for (node, shard) in [
                (NodeId(1), vec![ShardId(0)]),
                (NodeId(2), vec![ShardId(0)]),
                (NodeId(3), vec![ShardId(0)]),
                (NodeId(4), vec![ShardId(1)]),
                (NodeId(5), vec![ShardId(1)]),
                (NodeId(6), vec![ShardId(1)]),
            ] {
                node_to_shards.insert(node, shard);
            }

            let topology = Topology::new(shards, node_to_shards);

            let mut nodes = Map::new();
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

            #[cfg(not(kani))]
            log::LOG_HANDLE.get_or_init(|| {
                tracing_subscriber::fmt::init();
            });

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

        pub fn run(&mut self, initial_messages: Vec<(NodeId, NodeId, Message)>) {
            let mut messages = initial_messages;

            while !messages.is_empty() {
                let mut new_messages = vec![];

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
}

#[cfg(test)]
mod tests {
    use crate::collections::Set;

    use super::harness::*;

    #[test]
    fn one_transaction() {
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
                    keys: Set::from([Key(1), Key(11)]),
                },
            }),
        )]);
    }

    #[test]
    fn two_transactions() {
        let mut harness = Harness::new();
        for node_id in &harness.topology.shard_for_key(&Key(1)).node_ids {
            let data_store = harness.nodes.get_mut(node_id).unwrap().data_store_mut();
            data_store.insert(Key(1), Value(1));
            data_store.insert(Key(2), Value(2));
            data_store.insert(Key(3), Value(3));
        }

        for node_id in &harness.topology.shard_for_key(&Key(11)).node_ids {
            harness
                .nodes
                .get_mut(node_id)
                .unwrap()
                .data_store_mut()
                .insert(Key(11), Value(11));
        }

        harness.run(vec![
            (
                NodeId(0),
                NodeId(1),
                Message::NewTransaction(NewTransaction {
                    body: TransactionBody {
                        keys: Set::from([Key(1), Key(11)]),
                    },
                }),
            ),
            (
                NodeId(0),
                NodeId(4),
                Message::NewTransaction(NewTransaction {
                    body: TransactionBody {
                        keys: Set::from([Key(2), Key(11)]),
                    },
                }),
            ),
        ]);
    }
}

#[cfg(kani)]
mod verification {
    use crate::collections::Map;

    use super::harness::*;

    #[kani::proof]
    #[kani::unwind(7)]
    fn one_transaction() {
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
                    keys: set_from_iter([Key(1), Key(11)]),
                },
            }),
        )]);
    }

    #[kani::proof]
    // #[kani::unwind(100)]
    fn new_transaction() {
        let node_id = NodeId(1);

        let shards = vec![Shard {
            range: KeyRange {
                lo: Key(0),
                hi: Key(10),
            },
            node_ids: set_from_iter([node_id]),
        }];

        let mut nodes = Map::new();
        nodes.insert(node_id, vec![ShardId(0)]);

        let topology = Topology::new(shards, nodes);

        let timestamp_provider = TimestampProvider::new(node_id);

        let node = Node::new(
            node_id,
            topology.clone(),
            timestamp_provider,
            Coordinator::default(),
            Replica::default(),
            DataStore::default(),
        );
    }
}
