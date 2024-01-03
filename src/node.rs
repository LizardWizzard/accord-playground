use std::{
    cmp,
    collections::{HashMap, HashSet},
};

use crate::{
    timestamp::{Timestamp, TimestampProvider, TxnId},
    topology::{KeyRange, Topology},
    transaction::{
        Key, NewTransaction, ReplicaTransactionProgress, TransactionBody, Value, WaitingOn,
    },
    NodeId,
};

#[derive(Debug)]
struct QuorumTracker {
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

#[derive(Debug, Default)]
struct ShardQuorumTracker {
    shards: HashMap<KeyRange, QuorumTracker>,
}

impl ShardQuorumTracker {
    fn reset(&mut self) {
        self.shards.values_mut().for_each(|s| s.reset());
    }
}

#[derive(Clone, Copy, Debug)]
enum Outcome {
    Success,
    Failure,
}

impl ShardQuorumTracker {
    fn record_outcome(&mut self, node_id: NodeId, topology: &Topology, outcome: Outcome) -> bool {
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

#[derive(Debug, Default, PartialEq, Eq)]
enum Stage {
    #[default]
    PreAccept,
    Accept,
    Commit,
}

struct StageConsensus {
    // currently only used for asserts, consider making separate variants for each enum
    stage: Stage,
    // tracks progress across participating shards in PreAccept and AcceptRounds
    quorum_tracker: ShardQuorumTracker,
}

struct StageExecution {
    // TODO: reads gathered from participating shards
    //       execution result that is broadcasted to replicas, etc
}

enum CoordinatorTransactionStage {
    Consensus(StageConsensus),
    Execution(StageExecution),
}

struct CoordinatorTransactionProgress {
    // Defaults to TxnId, bumped according to replica responses in PreAccept and Accept rounds
    execute_at: Timestamp,
    // Transactions this one depends from, union of dependencies calculated on each participating shard
    dependencies: HashSet<TxnId>,
    // The logical core of the transaction. For now this is just the keys it accesses
    body: TransactionBody,

    stage: CoordinatorTransactionStage,
}

impl CoordinatorTransactionProgress {
    fn combine_dependencies(&mut self, dependencies: HashSet<TxnId>) {
        // TODO (perf): does extend(foo.iter()) invoke .reserve?
        self.dependencies.reserve(dependencies.len());
        for dep in dependencies {
            self.dependencies.insert(dep);
        }
    }
}

pub struct PreAccept {
    pub txn_id: TxnId,
    pub body: TransactionBody,
}

pub struct PreAcceptOk {
    txn_id: TxnId,
    execute_at: Timestamp,
    dependencies: HashSet<TxnId>,
}

#[derive(Debug)]
pub struct Commit {
    txn_id: TxnId,
    execute_at: Timestamp,
    dependencies: HashSet<TxnId>,
    body: TransactionBody,
}

pub struct Accept {
    txn_id: TxnId,
    execute_at: Timestamp,
    dependencies: HashSet<TxnId>,
    // TODO (correctness): at this point we've already sent body in PreAccept, why send it second time? Check with java version
    body: TransactionBody,
}

pub enum EitherCommitOrAccept {
    Commit(HashMap<NodeId, Commit>),
    Accept(HashMap<NodeId, Accept>),
}

pub struct AcceptOk {
    txn_id: TxnId,
    deps: HashSet<TxnId>,
}

pub struct Read {
    txn_id: TxnId,
    execute_at: Timestamp,
    deps: HashSet<TxnId>,
    keys: HashSet<Key>,
}

// TODO (perf): coalesce Commit and Read coming to one node into one network packet
pub struct CommitAndRead {
    commits: HashMap<NodeId, Commit>,
    reads: Vec<(NodeId, Read)>,
}

pub struct ReadOk {
    reads: Vec<(Key, Value)>,
}

pub struct Apply {
    txn_id: TxnId,
    execute_at: Timestamp,
    deps: HashSet<TxnId>,
    // Result of the computation for transaction. For now we just take sum of the keys that were read
    result: u64,
}

pub enum Message {
    DispatchTransaction,
    PreAccept(PreAccept),
    PreAcceptOk(PreAcceptOk),
}

struct MessageBus {
    messages: Vec<Message>,
}

pub struct DataStore {
    data: HashMap<Key, Value>,
}

impl DataStore {
    pub fn read_keys_if_present<'a>(
        &self,
        keys: impl IntoIterator<Item = &'a Key>,
    ) -> Vec<(Key, Value)> {
        // TODO (perf): inefficient
        let mut ret = vec![];

        for key in keys.into_iter() {
            if let Some(value) = self.data.get(key) {
                // TODO (perf): avoid clone
                ret.push((key.to_owned(), value.to_owned()))
            }
        }
        ret
    }
}

/// One node takes responsibility for one or more shards
pub struct Node {
    id: NodeId,
    topology: Topology,
    timestamp_provider: TimestampProvider,
    // Indexmap is used because in receive_read we need to access several entries mutably
    // at the same time which is not possible with HashMap without RefCell.
    // Anyway there needs to be proper storage abstraction hiding these aspects (TODO (clarity):)
    replica_transactions: HashMap<TxnId, ReplicaTransactionProgress>,
    coordinator_transactions: HashMap<TxnId, CoordinatorTransactionProgress>,

    data_store: DataStore,
    // Shadow field with links to other nodes to write asserts?
}

impl Node {
    /// Executed on a coordinator node, handles newly appeared transaction
    pub fn receive_new_transaction(&mut self, txn: NewTransaction) -> Vec<(NodeId, PreAccept)> {
        let txn_id = TxnId::from(self.timestamp_provider.next());
        // TODO (feature): in the paper it is union of fast path electorates for all participating shards, simplify with simple quorum for now
        // TODO (clarity): more comments
        let mut participants: HashSet<NodeId> = HashSet::new();
        let mut quorum_tracker = ShardQuorumTracker::default();
        for key in &txn.body.keys {
            let shard = self.topology.shard_for_key(key);
            participants.extend(&shard.node_ids);

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
        for participating_node in participants {
            messages.push((
                participating_node,
                PreAccept {
                    txn_id,
                    body: txn.body.clone(), // TODO (perf): avoid this clone
                },
            ))
        }

        self.coordinator_transactions.insert(
            txn_id,
            CoordinatorTransactionProgress {
                stage: Stage::PreAccept,
                quorum_tracker,
                execute_at: Timestamp::from(txn_id),
                dependencies: HashSet::new(),
                body: txn.body,
            },
        );

        messages
    }

    /// Executed on a replica, represents handling of PreAccept message
    /// ref preacceptOrRecover in java code
    pub fn receive_pre_accept(&mut self, req: PreAccept) -> PreAcceptOk {
        let initial_timestamp = Timestamp::from(req.txn_id); // t0 in the paper
        let mut max_conflicting_timestamp = initial_timestamp;
        let mut dependencies = HashSet::new();

        for transaction in self
            .replica_transactions
            .values()
            // per the paper we need to only respond with with ids < req.txn.id : {γ | γ ∼ τ∧t0 γ < t0 τ}
            .filter(|t| t.execute_at < initial_timestamp)
        {
            if transaction
                .body
                .keys
                .intersection(&req.body.keys)
                .next()
                .is_some()
            {
                // TODO (correctness) do we need to include keys? probably no
                dependencies.insert(transaction.id);
                max_conflicting_timestamp =
                    cmp::max(transaction.execute_at, max_conflicting_timestamp);
            }
        }

        let execute_at = if max_conflicting_timestamp == initial_timestamp {
            // there are no conflicting transactions
            // we're good to go with initially proposed timestamp
            initial_timestamp
        } else {
            // there are conflicting transactions
            Timestamp::new(
                max_conflicting_timestamp.time,
                max_conflicting_timestamp.seq + 1,
                self.id,
            )
        };

        // TODO (feature) handle case when transaction is already known
        assert!(self
            .replica_transactions
            .insert(
                req.txn_id,
                ReplicaTransactionProgress {
                    id: req.txn_id,
                    pre_accepted: true,
                    accepted: false,
                    committed: false,
                    applied: false,
                    execute_at,
                    max_witnessed_at: execute_at,
                    body: req.body,
                    waiting_for_dependencies: HashMap::new(),
                    dependencies_waiting: HashSet::new(),
                    pending_reads: HashSet::new()
                },
            )
            .is_none());

        PreAcceptOk {
            txn_id: req.txn_id,
            execute_at,
            dependencies,
        }
    }

    /// Executed on coordinator, represents handling of PreAcceptOk message
    fn receive_pre_accept_ok(
        &mut self,
        src_node: NodeId,
        mut pre_accept_ok: PreAcceptOk,
    ) -> Option<EitherCommitOrAccept> {
        let txn_id = pre_accept_ok.txn_id;
        let progress = self
            .coordinator_transactions
            .get_mut(&pre_accept_ok.txn_id)
            .expect("TODO (correctness):");

        assert_eq!(progress.stage, Stage::PreAccept);

        progress.execute_at = cmp::max(progress.execute_at, pre_accept_ok.execute_at);

        progress.combine_dependencies(std::mem::take(&mut pre_accept_ok.dependencies));

        // TODO (feature): we dont have a notion of separate fast path quorum just yet.
        // for now we always use simple quorum (slow path quorum)
        let have_quorum =
            progress
                .quorum_tracker
                .record_outcome(src_node, &self.topology, Outcome::Success);

        if !have_quorum {
            return None;
        }

        if progress.execute_at == Timestamp::from(txn_id) {
            // we've reached fast path decision there were no conflicts so our initial
            // timestamp becomes the transaction execution timestamp
            let mut commits = HashMap::new();

            for key in &progress.body.keys {
                let shard = self.topology.shard_for_key(key);
                for node_id in &shard.node_ids {
                    commits.entry(*node_id).or_insert_with(|| {
                        Commit {
                            txn_id,
                            execute_at: progress.execute_at,
                            // TODO (perf): avoid clone
                            dependencies: progress.dependencies.clone(),
                            // TODO (perf): avoid clone and maybe avoid sending body here at all because we did send it in pre accept
                            body: progress.body.clone(),
                        }
                    });
                }
            }
            progress.stage = Stage::Commit;

            Some(EitherCommitOrAccept::Commit(commits))
        } else {
            // we didnt reach fast path decision, we now need to broadcast new timestamp
            // for a transaction that is a maximum among all ones we've received from all participating nodes
            let mut accepts = HashMap::new();

            for key in &progress.body.keys {
                let shard = self.topology.shard_for_key(key);
                for node_id in &shard.node_ids {
                    accepts.entry(*node_id).or_insert_with(|| {
                        Accept {
                            txn_id,
                            execute_at: progress.execute_at,
                            // TODO (perf) avoid clone
                            dependencies: progress.dependencies.clone(),
                            // TODO (perf) avoid clone and maybe avoid sending body here at all because we did send it in pre accept
                            body: progress.body.clone(),
                        }
                    });
                }
            }

            progress.stage = Stage::Accept;
            progress.dependencies.clear();
            progress.quorum_tracker.reset();
            Some(EitherCommitOrAccept::Accept(accepts))
        }
    }

    /// Executed on a replica, represents handling of Accept message
    fn receive_accept(&mut self, accept: Accept) -> AcceptOk {
        let transaction = self
            .replica_transactions
            .get_mut(&accept.txn_id)
            .expect("TODO (correctness)");

        transaction.max_witnessed_at = cmp::max(transaction.max_witnessed_at, accept.execute_at);
        transaction.accepted = true;

        // TODO (clarity) extract into function.
        let mut dependencies = HashSet::new();
        for transaction in self
            .replica_transactions
            .values()
            // per the paper we need to only respond with with ids < req.txn.id : {γ | γ ∼ τ∧t0 γ < t0 τ}
            .filter(|t| Timestamp::from(t.id) < accept.execute_at)
        {
            if transaction
                .body
                .keys
                .intersection(&accept.body.keys)
                .next()
                .is_some()
            {
                dependencies.insert(transaction.id);
            }
        }

        AcceptOk {
            txn_id: accept.txn_id,
            deps: dependencies,
        }
    }

    /// Executed on coordinator, represents handling of AcceptOk message
    pub fn receive_accept_ok(
        &mut self,
        src_node: NodeId,
        accept_ok: AcceptOk,
    ) -> Option<CommitAndRead> {
        let progress = self
            .coordinator_transactions
            .get_mut(&accept_ok.txn_id)
            .expect("TODO (correctness)");

        assert_eq!(progress.stage, Stage::Accept);

        let have_quorum =
            progress
                .quorum_tracker
                .record_outcome(src_node, &self.topology, Outcome::Success);

        if !have_quorum {
            return None;
        }

        let mut commits = HashMap::new();

        let mut shard_ids = HashSet::new();

        for key in &progress.body.keys {
            let shard = self.topology.shard_for_key(key);
            shard_ids.insert(shard.id());
            for node_id in &shard.node_ids {
                commits.entry(*node_id).or_insert_with(|| {
                    Commit {
                        txn_id: accept_ok.txn_id,
                        execute_at: progress.execute_at,
                        // TODO (perf) avoid clone
                        dependencies: progress.dependencies.clone(),
                        // TODO (perf) avoid clone and maybe avoid sending body here at all because we did send it in pre accept
                        body: progress.body.clone(),
                    }
                });
            }
        }
        progress.stage = Stage::Commit;

        let mut reads = vec![];

        for shard_id in shard_ids {
            let shard = self.topology.shard_by_id(shard_id);

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
                    txn_id: accept_ok.txn_id,
                    execute_at: progress.execute_at,
                    deps: progress.dependencies.clone(),
                    keys: progress.body.keys.clone(),
                },
            ))
        }

        Some(CommitAndRead { commits, reads })
    }

    /// Executed on replica, represents handling of Commit message
    pub fn receive_commit(&mut self, commit: Commit) {
        // TODO (correctness) store full dependency set, transaction body etc
        // TODO (feature) arm recovery timer
        let transaction = self
            .replica_transactions
            .get_mut(&commit.txn_id)
            .expect("TODO (correctness)");

        transaction.committed = true;

        // TODO (perf): get rid of this clone. Not easy
        let dependencies_to_advance = transaction.dependencies_waiting.clone();
        for dep_id in dependencies_to_advance {
            let dep = self
                .replica_transactions
                .get_mut(&dep_id)
                .expect("TODO (type-safety): can we prove that dependency id is always valid?");

            dep.mark_dependency_committed(dep_id);
        }

        // TODO (correctness) why execute_at/max_witnessed_at are not updated? check with java version
    }

    pub fn receive_read(&mut self, src_node: NodeId, read: Read) -> Option<ReadOk> {
        let transaction = self
            .replica_transactions
            .get(&read.txn_id)
            .expect("TODO (correctness)");

        // TODO (type-safety): transaction struct should contain an enum representing valid states.
        assert!(transaction.waiting_for_dependencies.is_empty());

        // TODO идея мультилинзы, когда мы собираем пачку ключей и из-за того что ключи разные мы можем на каждый иметь мут.
        // можно сделать через RawEntry

        let mut waiting_for_dependencies = HashMap::new();

        for dep_id in read.deps {
            let dep = self
                .replica_transactions
                .get_mut(&dep_id)
                .expect("TODO (type-safety)");

            let waiting_on = match (dep.committed, dep.applied) {
                (true, true) => None,
                (true, false) => Some(WaitingOn::Apply),
                (false, true) => unreachable!("uncommitted tx cant be applied"),
                (false, false) => Some(WaitingOn::Commit),
            };
            if let Some(waiting_on) = waiting_on {
                waiting_for_dependencies.insert(dep_id, waiting_on);
                dep.dependencies_waiting.insert(read.txn_id);
            }
        }

        let transaction = self
            .replica_transactions
            .get_mut(&read.txn_id)
            .expect("index cannot be invalid because we got it in the beginning of the function");

        if waiting_for_dependencies.is_empty() {
            // we can proceed straight away
            // TODO (clarity): it is a simplification, transaction.body.keys contains all keys, not only
            //      ones specific to this shard (this is the same for Read struct though). Sort out which keys are passed in Read if any are needed.
            //      On Commit we can store two sets of keys, home keys and foreign ones so we quickly iterate over needed ones
            return Some(ReadOk {
                reads: self
                    .data_store
                    .read_keys_if_present(transaction.body.keys.iter()),
            });
        }

        transaction.waiting_for_dependencies = waiting_for_dependencies;

        // TODO Can we rely on timestamp process identifier for registerering src_id in transaction?
        let was_there = transaction.pending_reads.insert(src_node);
        assert!(was_there);

        None
    }

    pub fn receive_read_ok(&mut self, src_node: NodeId, read_ok: ReadOk) -> Option<Apply> {
        // TODO make quorum tracker to be parametrized by type that implements Reduce trait so it can accumulate responses
        //      consider making CoordinatorTransactionProgress an enum (or store enum inside) to store received read responses
        todo!()
    }
}

// TODO idea: HashSet or Vec for dependencies? What is more efficient, union of hashsets or merge sort-like union operation?
//    vec deque to store transactions in order of their id (if that is needed)? search will be not that efficient

// TODO sometthing like?
enum ReplicaTransactionProgress2 {
    PreAccepted,
    Accepted,
    Committed,
    Applied,
}
