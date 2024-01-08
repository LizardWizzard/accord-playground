use std::{
    cmp,
    collections::{HashMap, HashSet},
};

use crate::{
    messages::{Accept, AcceptOk, Apply, Commit, PreAccept, PreAcceptOk, Read, ReadOk},
    node::DataStore,
    timestamp::{Timestamp, TxnId},
    transaction::TransactionBody,
    NodeId,
};

#[derive(Debug, Clone, Copy)]
pub enum WaitingOn {
    Commit,
    Apply,
}

/// TODO (feature): for recovery we need a way to include full tx state
#[derive(Debug, Clone)]
pub struct ReplicaTransactionProgress {
    pub id: TxnId,
    pub pre_accepted: bool,
    pub accepted: bool,
    pub committed: bool,
    pub applied: bool,
    pub execute_at: Timestamp,
    pub max_witnessed_at: Timestamp,
    pub body: TransactionBody,
    // TODO (perf): this is inefficient, in java version in Command.java there is WaitingOn class that uses bitsets to represent dependency status
    //              additionally it compresses txids into ints by maintaining a separate mapping in a vec. Consider doing that too
    //              I havent seen the reverse mapping on java side though.
    //              Also look at updateDependencyAndMaybeExecute
    // dependencies we wait on None indicates
    pub waiting_for_dependencies: HashMap<TxnId, WaitingOn>,
    // dependencies waiting on us
    pub dependencies_waiting: HashSet<TxnId>,
    // Record ids of nodes which we received Read request from.
    // Once transaction is committed and applied we resolve the read request by
    // by sending requested keys. TODO (can we calculate keys at this point withoustoring them?).
    pub pending_reads: HashSet<NodeId>,
}

impl ReplicaTransactionProgress {
    pub fn mark_dependency_committed(&mut self, dep_id: TxnId) {
        let dep = self
            .waiting_for_dependencies
            .get_mut(&dep_id)
            .expect("TODO (type-safety)");

        // TODO:
        // replicas wait to answer this message until every such dependency has
        // either been witnessed as committed with a higher execution timestamp tγ > tτ, or its result has been applied locally.
        //
        // In other words if execution timestamp jumped ahead of ours we dont need to wait for it to commit,
        // figure out how exactly this happens, probably in case of recovery
        match dep {
            WaitingOn::Commit => *dep = WaitingOn::Apply,
            WaitingOn::Apply => unreachable!("cant be applied before committed"),
        }
    }
}

pub struct Replica {
    replica_transactions: HashMap<TxnId, ReplicaTransactionProgress>,
}

impl Replica {
    /// Executed on a replica, represents handling of PreAccept message
    /// ref preacceptOrRecover in java code
    pub fn receive_pre_accept(&mut self, pre_accept: PreAccept, node_id: NodeId) -> PreAcceptOk {
        let initial_timestamp = Timestamp::from(pre_accept.txn_id); // t0 in the paper
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
                .intersection(&pre_accept.body.keys)
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
                node_id,
            )
        };

        // TODO (feature) handle case when transaction is already known
        assert!(self
            .replica_transactions
            .insert(
                pre_accept.txn_id,
                ReplicaTransactionProgress {
                    id: pre_accept.txn_id,
                    pre_accepted: true,
                    accepted: false,
                    committed: false,
                    applied: false,
                    execute_at,
                    max_witnessed_at: execute_at,
                    body: pre_accept.body,
                    waiting_for_dependencies: HashMap::new(),
                    dependencies_waiting: HashSet::new(),
                    pending_reads: HashSet::new()
                },
            )
            .is_none());

        PreAcceptOk {
            txn_id: pre_accept.txn_id,
            execute_at,
            dependencies,
        }
    }

    pub fn receive_accept(&mut self, accept: Accept) -> AcceptOk {
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
            dependencies,
        }
    }

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

    pub fn receive_read(
        &mut self,
        src_node: NodeId,
        read: Read,
        data_store: &DataStore,
    ) -> Option<ReadOk> {
        let transaction = self
            .replica_transactions
            .get(&read.txn_id)
            .expect("TODO (correctness)");

        // TODO (type-safety): transaction struct should contain an enum representing valid states.
        assert!(transaction.waiting_for_dependencies.is_empty());

        // TODO идея мультилинзы, когда мы собираем пачку ключей и из-за того что ключи разные мы можем на каждый иметь мут.
        // можно сделать через RawEntry

        let mut waiting_for_dependencies = HashMap::new();

        for dep_id in read.dependencies {
            let dep = self
                .replica_transactions
                // TODO it is actually OK that it is missing because we send full set of dependencies
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
                txn_id: read.txn_id,
                reads: data_store.read_keys_if_present(transaction.body.keys.iter()),
            });
        }

        transaction.waiting_for_dependencies = waiting_for_dependencies;

        // TODO Can we rely on timestamp process identifier for registerering src_id in transaction?
        let was_there = transaction.pending_reads.insert(src_node);
        assert!(was_there);

        None
    }

    pub fn receive_apply(&mut self, src_node: NodeId, apply: Apply) -> Vec<(NodeId, ReadOk)> {
        let progress = self
            .replica_transactions
            .get_mut(&apply.txn_id)
            .expect("TODO");

        todo!()
    }
}

// TODO sometthing like?
enum ReplicaTransactionProgress2 {
    PreAccepted,
    Accepted,
    Committed,
    Applied,
}
