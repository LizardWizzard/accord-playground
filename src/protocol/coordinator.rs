use std::cmp;

use crate::{
    collections::{Map, Set},
    protocol::{
        messages::{
            Accept, AcceptOk, Apply, Commit, CommitAndRead, EitherCommitOrAccept, NewTransaction,
            PreAccept, PreAcceptOk, Read, ReadOk,
        },
        quorum_tracker::{Outcome, QuorumTracker, ShardQuorumTracker},
        timestamp::{Timestamp, TimestampProvider, TxnId},
        topology::{ShardId, Topology},
        transaction::{execute, Key, TransactionBody, Value},
        NodeId,
    },
};

#[derive(Debug, Clone)]
struct StageConsensus {
    // Defaults to TxnId, bumped according to replica responses in PreAccept and Accept rounds
    execute_at: Timestamp,
    // tracks progress across participating shards in PreAccept and AcceptRounds
    quorum_tracker: ShardQuorumTracker,
    // Transactions this one depends from, gathered from participating shards
    dependencies: Set<TxnId>,
    // Participating shards, keep them to avoid recalculation each time we need
    // to send a message to nodes from every shard
    participating_shards: Set<ShardId>,
    // The logical core of the transaction. For now this is just the keys it accesses
    body: TransactionBody,
}

impl StageConsensus {
    fn combine_dependencies(&mut self, dependencies: Set<TxnId>) {
        // TODO (perf): does extend(foo.iter()) invoke .reserve?
        self.dependencies.reserve(dependencies.len());
        for dep in dependencies {
            self.dependencies.insert(dep);
        }
    }
}

struct StageRead {
    // TODO: execution result that is broadcasted to replicas, etc
    // reads gathered from participating shards
    execute_at: Timestamp,
    // Transactions this one depends from, gathered from participating shards
    dependencies: Set<TxnId>,
    // KV pairs fetched as part of transaction Read interest
    reads: Vec<(Key, Value)>,
    // number of pending read requests
    pending_reads: u8,

    participating_shards: Set<ShardId>,

    body: TransactionBody,
}

impl StageRead {
    fn from_stage_consensus_and_commit(
        stage_consensus: &mut StageConsensus,
        commit_and_read: &CommitAndRead,
    ) -> Self {
        StageRead {
            execute_at: stage_consensus.execute_at,
            reads: vec![],
            pending_reads: commit_and_read.reads.len().try_into().expect("TODO"),
            participating_shards: std::mem::take(&mut stage_consensus.participating_shards),
            body: std::mem::take(&mut stage_consensus.body),
            dependencies: std::mem::take(&mut stage_consensus.dependencies),
        }
    }
}

enum TransactionProgress {
    PreAccept(StageConsensus),
    Accept(StageConsensus),
    // Commit is combined with Read because there are no intermediate steps between them
    // Commits are sent to all nodes and Reads are sent to one node from each replica set
    // on single step
    // TODO Read is part of execution, consider going forward with this enum
    Read(StageRead),
}

impl TransactionProgress {
    pub const fn name(&self) -> &'static str {
        match self {
            TransactionProgress::PreAccept(_) => "PreAccept",
            TransactionProgress::Accept(_) => "Accept",
            TransactionProgress::Read(_) => "Read",
        }
    }

    fn as_mut_pre_accept(&mut self) -> Option<&mut StageConsensus> {
        match self {
            Self::PreAccept(pa) => Some(pa),
            _ => None,
        }
    }

    fn as_mut_accept(&mut self) -> Option<&mut StageConsensus> {
        match self {
            Self::Accept(a) => Some(a),
            _ => None,
        }
    }

    fn as_mut_read(&mut self) -> Option<&mut StageRead> {
        match self {
            Self::Read(r) => Some(r),
            _ => None,
        }
    }
}

#[derive(Default)]
pub struct Coordinator {
    transactions: Map<TxnId, TransactionProgress>,
}

impl Coordinator {
    /// Executed on a coordinator node, handles newly appeared transaction
    pub fn receive_new_transaction(
        &mut self,
        txn: NewTransaction,
        timestamp_provider: &mut TimestampProvider,
        topology: &Topology,
    ) -> Vec<(NodeId, PreAccept)> {
        let txn_id = TxnId::from(timestamp_provider.tick_next());
        // TODO (feature): in the paper it is union of fast path electorates for all participating shards, simplify with simple quorum for now
        // TODO (clarity): more comments
        let mut participating_nodes: Set<NodeId> = Set::new();
        let mut participating_shards: Set<ShardId> = Set::new();

        let mut quorum_tracker = ShardQuorumTracker::default();
        for key in &txn.body.keys {
            let shard = topology.shard_for_key(key);
            participating_nodes.extend(&shard.node_ids);
            participating_shards.insert(shard.id());

            quorum_tracker
                .shards
                .entry(shard.range.clone())
                .or_insert_with(|| {
                    QuorumTracker::new(
                        shard
                            .node_ids
                            .len()
                            .try_into()
                            // TODO (type-safety): special type for node ids that implements the limit
                            .expect("cant be more than u8 nodes for shard"),
                    )
                });
        }

        let mut messages = vec![];
        for participating_node in participating_nodes {
            messages.push((
                participating_node,
                PreAccept {
                    txn_id,
                    body: txn.body.clone(), // TODO (perf): avoid this clone
                },
            ))
        }

        self.transactions.insert(
            txn_id,
            TransactionProgress::PreAccept(StageConsensus {
                execute_at: Timestamp::from(txn_id),
                quorum_tracker,
                dependencies: Set::new(),
                body: txn.body,
                participating_shards,
            }),
        );

        messages
    }

    fn make_commit_and_read(
        progress: &mut StageConsensus,
        txn_id: TxnId,
        topology: &Topology,
    ) -> CommitAndRead {
        let mut commits = Map::new();

        let mut reads = vec![];

        for shard_id in &progress.participating_shards {
            let shard = topology.shard_by_id(*shard_id);

            // Issue commits to every participating node
            for node_id in &shard.node_ids {
                commits.entry(*node_id).or_insert_with(|| {
                    Commit {
                        txn_id,
                        execute_at: progress.execute_at,
                        // TODO (perf) avoid clone
                        dependencies: progress.dependencies.clone(),
                        // Note: we dont send body because it was sent during pre accept and body couldnt have changed since then
                        // TODO: verify for recovery
                    }
                });
            }

            // Issue reads to one node from each shard
            // TODO (feature) pick closest node instead of random one
            // TODO (type-safety) nonempty hashset
            let node = shard
                .node_ids
                .iter()
                .next()
                .expect("at least one node in each shard");

            // TODO send only dependencies relevant to node, how to track that?
            reads.push((
                *node,
                Read {
                    txn_id,
                    execute_at: progress.execute_at,
                    dependencies: progress.dependencies.clone(),
                    keys: progress.body.keys.clone(),
                },
            ))
        }

        CommitAndRead { commits, reads }
    }

    /// Executed on coordinator, represents handling of PreAcceptOk message
    pub fn receive_pre_accept_ok(
        &mut self,
        src_node: NodeId,
        mut pre_accept_ok: PreAcceptOk,
        topology: &Topology,
    ) -> Option<EitherCommitOrAccept> {
        let txn_id = pre_accept_ok.txn_id;
        let progress = self
            .transactions
            .get_mut(&pre_accept_ok.txn_id)
            .expect("TODO (correctness):");

        let pre_accept_stage = match progress.as_mut_pre_accept() {
            Some(pa) => pa,
            None => {
                // After receiving a quorum we've made progress but continued to receive responses from other nodes, ignore?
                panic!("TODO Expected PreAccept got {}", progress.name())
            }
        };

        pre_accept_stage.execute_at =
            cmp::max(pre_accept_stage.execute_at, pre_accept_ok.execute_at);

        pre_accept_stage.combine_dependencies(std::mem::take(&mut pre_accept_ok.dependencies));

        // TODO (feature): we dont have a notion of separate fast path quorum just yet.
        // for now we always use simple quorum (slow path quorum)
        let have_quorum =
            pre_accept_stage
                .quorum_tracker
                .record_outcome(src_node, topology, Outcome::Success);

        if !have_quorum {
            return None;
        }

        if pre_accept_stage.execute_at == Timestamp::from(txn_id) {
            // we've reached fast path decision there were no conflicts so our initial
            // timestamp becomes the transaction execution timestamp
            let commit_and_read = Self::make_commit_and_read(pre_accept_stage, txn_id, topology);

            *progress = TransactionProgress::Read(StageRead::from_stage_consensus_and_commit(
                pre_accept_stage,
                &commit_and_read,
            ));

            Some(EitherCommitOrAccept::Commit(commit_and_read))
        } else {
            // we didnt reach fast path decision, we now need to broadcast new timestamp
            // for a transaction that is a maximum among all ones we've received from all participating nodes
            let mut accepts = Map::new();

            for key in &pre_accept_stage.body.keys {
                let shard = &topology.shard_for_key(key);
                for node_id in &shard.node_ids {
                    accepts.entry(*node_id).or_insert_with(|| {
                        Accept {
                            txn_id,
                            execute_at: pre_accept_stage.execute_at,
                            // TODO (perf) avoid clone
                            dependencies: pre_accept_stage.dependencies.clone(),
                            // TODO (perf) avoid clone and maybe avoid sending body here at all because we did send it in pre accept
                            body: pre_accept_stage.body.clone(),
                        }
                    });
                }
            }

            // TODO make it an impl from?
            let mut quorum_tracker = std::mem::take(&mut pre_accept_stage.quorum_tracker);
            quorum_tracker.reset(); // TODO find a way to mutate state without clone

            let body = std::mem::take(&mut pre_accept_stage.body);
            let participating_shards = std::mem::take(&mut pre_accept_stage.participating_shards);
            *progress = TransactionProgress::Accept(StageConsensus {
                execute_at: pre_accept_stage.execute_at,
                quorum_tracker,
                dependencies: Set::new(),
                participating_shards,
                body,
            });

            Some(EitherCommitOrAccept::Accept(accepts))
        }
    }

    pub fn receive_accept_ok(
        &mut self,
        src_node: NodeId,
        accept_ok: AcceptOk,
        topology: &Topology,
    ) -> Option<CommitAndRead> {
        let progress = self
            .transactions
            .get_mut(&accept_ok.txn_id)
            .expect("TODO (correctness)");

        let accept_stage = progress.as_mut_accept().expect("TODO");

        let have_quorum =
            accept_stage
                .quorum_tracker
                .record_outcome(src_node, topology, Outcome::Success);

        if !have_quorum {
            return None;
        }

        let commit_and_read = Self::make_commit_and_read(accept_stage, accept_ok.txn_id, topology);

        *progress = TransactionProgress::Read(StageRead::from_stage_consensus_and_commit(
            accept_stage,
            &commit_and_read,
        ));

        Some(commit_and_read)
    }

    pub fn receive_read_ok(
        &mut self,
        _src_node: NodeId,
        read_ok: ReadOk,
        topology: &Topology,
    ) -> Option<Vec<(NodeId, Apply)>> {
        // TODO (idea) make quorum tracker parametrized by type that implements some sort of Reduce trait so it can accumulate responses

        let progress = self
            .transactions
            .get_mut(&read_ok.txn_id)
            .expect("TODO (correctness)");

        let read_stage = progress.as_mut_read().expect("TODO");

        read_stage.reads.extend_from_slice(&read_ok.reads);
        read_stage.pending_reads -= 1;

        if read_stage.pending_reads != 0 {
            return None;
        }

        let result = execute(std::mem::take(&mut read_stage.reads));

        let mut applies = vec![];

        for shard_id in &read_stage.participating_shards {
            for node_id in &topology.shard_by_id(*shard_id).node_ids {
                applies.push((
                    *node_id,
                    Apply {
                        txn_id: read_ok.txn_id,
                        execute_at: read_stage.execute_at,
                        // TODO slice deps for shard? (perf): avoid clone
                        dependencies: read_stage.dependencies.clone(),
                        result: result.clone(),
                    },
                ))
            }
        }

        Some(applies)
    }
}
