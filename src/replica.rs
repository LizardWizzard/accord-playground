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

struct StageConsensus {
    execute_at: Timestamp,
    max_witnessed_at: Timestamp,

    // dependencies waiting on this transaction to become committed / applied
    // TODO describe how this can get populated during consensus stage
    dependencies_waiting: HashSet<TxnId>,

    body: TransactionBody,
}

struct StageExecution {
    execute_at: Timestamp,
    max_witnessed_at: Timestamp,

    // dependencies we wait to commit / apply
    // TODO currently used only in CommittedAndReadPending, decide if neds to be placed out of StageExecution
    // TODO (perf): this is inefficient, in java version in Command.java there is WaitingOn class that uses bitsets to represent dependency status
    //              additionally it compresses txids into ints by maintaining a separate mapping in a vec. Consider doing that too
    //              I havent seen the reverse mapping on java side though.
    //              Also look at updateDependencyAndMaybeExecute
    // dependencies we wait on None indicates
    waiting_for_dependencies: HashMap<TxnId, WaitingOn>,
    // dependencies waiting on this transaction to become committed / applied
    dependencies_waiting: HashSet<TxnId>,

    body: TransactionBody,
}

impl From<&mut StageConsensus> for StageExecution {
    fn from(stage_consensus: &mut StageConsensus) -> Self {
        StageExecution {
            execute_at: stage_consensus.execute_at,
            max_witnessed_at: stage_consensus.max_witnessed_at,
            waiting_for_dependencies: HashMap::new(),
            dependencies_waiting: std::mem::take(&mut stage_consensus.dependencies_waiting),
            body: std::mem::take(&mut stage_consensus.body),
        }
    }
}

impl From<&mut StageExecution> for StageExecution {
    fn from(stage_execution: &mut StageExecution) -> Self {
        StageExecution {
            execute_at: stage_execution.execute_at,
            max_witnessed_at: stage_execution.max_witnessed_at,
            waiting_for_dependencies: std::mem::take(&mut stage_execution.waiting_for_dependencies),
            dependencies_waiting: std::mem::take(&mut stage_execution.dependencies_waiting),
            body: std::mem::take(&mut stage_execution.body),
        }
    }
}

struct ReadInterest {
    // Record ids of nodes which we received Read requests from.
    // Once transaction is committed and applied we resolve the read request by
    // by sending requested kv pairs. TODO (can we calculate keys at this point withou storing them?).
    // TODO can it be more than one node? in case of a recovery coordinator taking over?
    pub send_to: NodeId,
}

enum ReplicaTransactionProgress {
    // Initial stage when replica is notified about transaction in PreAccept round
    PreAccepted(StageConsensus),
    // In case PreAccept round was not sufficient to determine execution timestamp we move on to Accept round
    Accepted(StageConsensus),
    // After either of PreAccept or Accept transaction becomes committed
    Committed(StageExecution),
    // The only difference with Committed is supplementary info about pending Read requests
    CommittedAndReadPending((StageExecution, ReadInterest)),
    Applied(StageExecution),
}

impl ReplicaTransactionProgress {
    fn execute_at(&self) -> Timestamp {
        use ReplicaTransactionProgress::*;
        match self {
            PreAccepted(pa) => pa.execute_at,
            Accepted(a) => a.execute_at,
            Committed(c) => c.execute_at,
            CommittedAndReadPending((c, _)) => c.execute_at,
            Applied(a) => a.execute_at,
        }
    }

    fn body(&self) -> TransactionBody {
        use ReplicaTransactionProgress::*;
        match self {
            PreAccepted(pa) => pa.body,
            Accepted(a) => a.body,
            Committed(c) => c.body,
            CommittedAndReadPending((c, _)) => c.body,
            Applied(a) => a.body,
        }
    }

    fn as_mut_pre_accepted(&mut self) -> Option<&mut StageConsensus> {
        match self {
            ReplicaTransactionProgress::PreAccepted(pa) => Some(pa),
            _ => None,
        }
    }

    fn as_mut_pre_accepted_or_accepted(&mut self) -> Option<&mut StageConsensus> {
        match self {
            ReplicaTransactionProgress::PreAccepted(pa) => Some(pa),
            ReplicaTransactionProgress::Accepted(a) => Some(a),
            _ => None,
        }
    }

    fn as_mut_committed(&mut self) -> Option<&mut StageExecution> {
        match self {
            ReplicaTransactionProgress::Committed(c) => Some(c),
            _ => None,
        }
    }

    fn get_awaited_dependency(&mut self, dep_id: TxnId) -> &mut WaitingOn {
        use ReplicaTransactionProgress::*;

        let mut deps = match self {
            Committed(se) => se.waiting_for_dependencies,
            CommittedAndReadPending((se, _)) => se.waiting_for_dependencies,
            Applied(se) => se.waiting_for_dependencies,
            _ => unreachable!(
                "transactions in PreAccepted/Accepted stages do not have register wait interest"
            ),
        };

        deps.get_mut(&dep_id).expect("TODO")
    }

    fn mark_dependency_committed(&mut self, dep_id: TxnId) {
        let dep = self.get_awaited_dependency(dep_id);

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

    fn mark_dependency_applied(&mut self, dep_id: TxnId) {
        let dep = self.get_awaited_dependency(dep_id);
        match dep {
            WaitingOn::Commit => unreachable!("cant be waiting for commit when applied"),
            WaitingOn::Apply => *dep = WaitingOn::Apply,
        }
    }
}

pub struct Replica {
    transactions: HashMap<TxnId, ReplicaTransactionProgress>,
}

impl Replica {
    /// Executed on a replica, represents handling of PreAccept message
    /// ref preacceptOrRecover in java code
    pub fn receive_pre_accept(&mut self, pre_accept: PreAccept, node_id: NodeId) -> PreAcceptOk {
        let initial_timestamp = Timestamp::from(pre_accept.txn_id); // t0 in the paper
        let mut max_conflicting_timestamp = initial_timestamp;
        let mut dependencies = HashSet::new();

        for (txn_id, transaction) in self
            .transactions
            .iter()
            // per the paper we need to only respond with with ids < req.txn.id : {γ | γ ∼ τ∧t0 γ < t0 τ}
            .filter(|(_, t)| t.execute_at() < initial_timestamp)
        {
            if transaction
                .body()
                .keys
                .intersection(&pre_accept.body.keys)
                .next()
                .is_some()
            {
                // TODO (correctness) do we need to include keys? probably no
                dependencies.insert(*txn_id);
                max_conflicting_timestamp =
                    cmp::max(transaction.execute_at(), max_conflicting_timestamp);
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
        let inserted_new = self
            .transactions
            .insert(
                pre_accept.txn_id,
                ReplicaTransactionProgress::PreAccepted(StageConsensus {
                    execute_at,
                    max_witnessed_at: execute_at,
                    dependencies_waiting: HashSet::new(),
                    body: pre_accept.body,
                }),
            )
            .is_none();
        assert!(inserted_new);

        PreAcceptOk {
            txn_id: pre_accept.txn_id,
            execute_at,
            dependencies,
        }
    }

    pub fn receive_accept(&mut self, accept: Accept) -> AcceptOk {
        let transaction = self
            .transactions
            .get_mut(&accept.txn_id)
            .expect("TODO (correctness)");

        let mut pre_accepted = transaction.as_mut_pre_accepted().expect("TODO");
        pre_accepted.max_witnessed_at = cmp::max(pre_accepted.max_witnessed_at, accept.execute_at);

        *transaction = ReplicaTransactionProgress::Accepted(StageConsensus {
            execute_at: pre_accepted.execute_at,
            max_witnessed_at: pre_accepted.max_witnessed_at,
            dependencies_waiting: std::mem::take(&mut pre_accepted.dependencies_waiting),
            body: std::mem::take(&mut pre_accepted.body),
        });

        // TODO (clarity) extract into function.
        let mut dependencies = HashSet::new();
        for (txn_id, transaction) in self
            .transactions
            .iter()
            // per the paper we need to only respond with with ids < req.txn.id : {γ | γ ∼ τ∧t0 γ < t0 τ}
            // TODO filter out ourselves?
            .filter(|(t_id, _)| Timestamp::from(**t_id) < accept.execute_at)
        {
            if transaction
                .body()
                .keys
                .intersection(&accept.body.keys)
                .next()
                .is_some()
            {
                dependencies.insert(*txn_id);
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
            .transactions
            .get_mut(&commit.txn_id)
            .expect("TODO (correctness)");

        let stage_consensus = transaction.as_mut_pre_accepted_or_accepted().expect("TODO");
        let stage_execution = StageExecution::from(stage_consensus);

        // TODO (perf): get rid of this clone. Not easy
        let dependencies_to_advance = stage_execution.dependencies_waiting.clone();
        for dep_id in dependencies_to_advance {
            let dep = self
                .transactions
                .get_mut(&dep_id)
                .expect("TODO (type-safety): can we prove that dependency id is always valid?");

            dep.mark_dependency_committed(dep_id);
        }

        *transaction = ReplicaTransactionProgress::Committed(stage_execution);
        // TODO (correctness) why execute_at/max_witnessed_at are not updated? check with java version
    }

    pub fn receive_read(
        &mut self,
        src_node: NodeId,
        read: Read,
        data_store: &DataStore,
    ) -> Option<ReadOk> {
        let transaction = self
            .transactions
            .get(&read.txn_id)
            .expect("TODO (correctness)");

        let stage_committed = transaction.as_mut_committed().expect("TODO");
        assert!(stage_committed.waiting_for_dependencies.is_empty());

        // TODO идея мультилинзы, когда мы собираем пачку ключей и из-за того что ключи разные мы можем на каждый иметь мут.
        // можно сделать через RawEntry

        // Register this transaction in each of its dependencies so once they're
        // committed/applied this transaction can move forward too
        let mut waiting_for_dependencies = HashMap::new();

        for dep_id in read.dependencies {
            let dep = self
                .transactions
                // TODO it is actually OK that it is missing because we send full set of dependencies
                .get_mut(&dep_id)
                .expect("TODO (type-safety)");

            use ReplicaTransactionProgress::*;

            // Register transaction as awaiting progress of this one (Commit/Apply interest)
            let waiting_on = match dep {
                PreAccepted(sc) | Accepted(sc) => {
                    assert!(!sc.dependencies_waiting.insert(read.txn_id));
                    Some(WaitingOn::Commit)
                }
                Committed(se) | CommittedAndReadPending((se, _)) => {
                    assert!(!se.dependencies_waiting.insert(read.txn_id));
                    Some(WaitingOn::Apply)
                }
                Applied(_) => None,
            };

            if let Some(waiting_on) = waiting_on {
                waiting_for_dependencies.insert(dep_id, waiting_on);
            }
        }

        let transaction = self
            .transactions
            .get_mut(&read.txn_id)
            .expect("index cannot be invalid because we got it in the beginning of the function");

        if waiting_for_dependencies.is_empty() {
            // we can proceed straight away
            // TODO (clarity): it is a simplification, transaction.body.keys contains all keys, not only
            //      ones specific to this shard (this is the same for Read struct though). Sort out which keys are passed in Read if any are needed.
            //      On Commit we can store two sets of keys, home keys and foreign ones so we quickly iterate over needed ones
            return Some(ReadOk {
                txn_id: read.txn_id,
                reads: data_store.read_keys_if_present(stage_committed.body.keys.iter()),
            });
        }

        stage_committed.waiting_for_dependencies = waiting_for_dependencies;

        // TODO Can we rely on timestamp process identifier for registerering src_id in transaction?
        *transaction = ReplicaTransactionProgress::CommittedAndReadPending((
            StageExecution::from(stage_committed),
            ReadInterest { send_to: src_node },
        ));

        None
    }

    pub fn receive_apply(&mut self, src_node: NodeId, apply: Apply) -> Vec<(NodeId, ReadOk)> {
        let progress = self.transactions.get_mut(&apply.txn_id).expect("TODO");

        todo!()
    }
}
