use std::cmp;

use crate::{
    collections::{split, IterGuard, Map, MapEntry, Set},
    protocol::{
        messages::{Accept, AcceptOk, Apply, Commit, PreAccept, PreAcceptOk, Read, ReadOk},
        node::DataStore,
        timestamp::{Timestamp, TxnId},
        transaction::{self, Key, TransactionBody, Value},
        NodeId,
    },
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
    dependencies_waiting: Set<TxnId>,

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
    pending_dependencies: Map<TxnId, WaitingOn>,
    // dependencies waiting on this transaction to become committed / applied
    dependencies_waiting: Set<TxnId>,

    body: TransactionBody,
}

impl From<&mut StageConsensus> for StageExecution {
    fn from(stage_consensus: &mut StageConsensus) -> Self {
        StageExecution {
            execute_at: stage_consensus.execute_at,
            max_witnessed_at: stage_consensus.max_witnessed_at,
            pending_dependencies: Map::new(),
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
            pending_dependencies: std::mem::take(&mut stage_execution.pending_dependencies),
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

struct Committed {
    stage_execution: StageExecution,
}

impl Committed {
    // TODO make this more typesafe, &mut is not ideal
    fn apply(
        &mut self,
        key: Key,
        value: Value,
        data_store: &mut DataStore,
    ) -> ReplicaTransactionProgress {
        // TODO assert all dependencies are applied

        transaction::apply(key, value, data_store);

        ReplicaTransactionProgress::Applied
    }

    #[allow(clippy::wrong_self_convention)]
    fn into_pending_apply(&mut self, result: (Key, Value)) -> ReplicaTransactionProgress {
        ReplicaTransactionProgress::CommittedApplyPending(CommittedApplyPending {
            stage_execution: StageExecution::from(&mut self.stage_execution),
            pending_apply: Some(result),
        })
    }
}

// In case we received Apply but we are not able to perform it imediately (pending deps)
// we need to store received result of the execution to be able to apply it when our dependencies become applied
struct CommittedApplyPending {
    stage_execution: StageExecution,
    // Option because we want to Option::take it when we only have &mut
    pending_apply: Option<(Key, Value)>,
}

impl CommittedApplyPending {
    // TODO make this more typesafe, &mut is not ideal
    fn apply(&mut self, data_store: &mut DataStore) -> ReplicaTransactionProgress {
        // TODO assert all dependencies are applied
        let (key, value) = self.pending_apply.take().expect("cant be empty");
        transaction::apply(key, value, data_store);

        ReplicaTransactionProgress::Applied
    }
}

enum ReplicaTransactionProgress {
    /// Initial stage when replica is notified about transaction in PreAccept round
    PreAccepted(StageConsensus),
    /// In case PreAccept round was not sufficient to determine execution timestamp we move on to Accept round
    Accepted(StageConsensus),
    /// After either of PreAccept or Accept transaction becomes committed
    Committed(Committed),
    /// The only difference with Committed is supplementary info about pending Read requests.
    /// Note that after read request is satisfied we're  
    CommittedReadPending((Committed, ReadInterest)),

    CommittedApplyPending(CommittedApplyPending),

    Applied,
}

impl ReplicaTransactionProgress {
    fn execute_at(&self) -> Timestamp {
        use ReplicaTransactionProgress::*;
        match self {
            PreAccepted(pa) => pa.execute_at,
            Accepted(a) => a.execute_at,
            Committed(c) => c.stage_execution.execute_at,
            CommittedReadPending((c, _)) => c.stage_execution.execute_at,
            CommittedApplyPending(c) => c.stage_execution.execute_at,
            Applied => todo!(),
        }
    }

    fn body(&self) -> &TransactionBody {
        use ReplicaTransactionProgress::*;
        match self {
            PreAccepted(pa) => &pa.body,
            Accepted(a) => &a.body,
            Committed(c) => &c.stage_execution.body,
            CommittedReadPending((c, _)) => &c.stage_execution.body,
            CommittedApplyPending(c) => &c.stage_execution.body,
            Applied => todo!(),
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
            ReplicaTransactionProgress::Committed(c) => Some(&mut c.stage_execution),
            _ => None,
        }
    }

    fn pending_dependencies(&mut self) -> &mut Map<TxnId, WaitingOn> {
        use ReplicaTransactionProgress::*;

        match self {
            Committed(c) => &mut c.stage_execution.pending_dependencies,
            CommittedReadPending((c, _)) => &mut c.stage_execution.pending_dependencies,
            Applied => todo!(),
            _ => unreachable!(
                "transactions in PreAccepted/Accepted stages do not have registered wait interest"
            ),
        }
    }

    fn mark_dependency_committed(&mut self, dep_id: TxnId) {
        match self.pending_dependencies().entry(dep_id) {
            MapEntry::Occupied(mut o) => {
                let dep = o.get_mut();
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
            MapEntry::Vacant(_) => panic!("TODO: dependency must exist"),
        };
    }

    fn mark_dependency_applied(&mut self, dep_id: TxnId) -> bool {
        let pending_dependencies = self.pending_dependencies();

        let dep = match pending_dependencies.entry(dep_id) {
            MapEntry::Occupied(o) => o,
            MapEntry::Vacant(_) => panic!("TODO: dependency must exist"),
        };

        match dep.get() {
            WaitingOn::Commit => unreachable!("cant be waiting for commit when applied"),
            WaitingOn::Apply => {
                dep.remove();
                pending_dependencies.is_empty()
            }
        }
    }
}

#[derive(Default)]
pub struct Replica {
    transactions: Map<TxnId, ReplicaTransactionProgress>,
}

impl Replica {
    /// Executed on a replica, represents handling of PreAccept message
    /// ref preacceptOrRecover in java code
    pub fn receive_pre_accept(&mut self, pre_accept: PreAccept, node_id: NodeId) -> PreAcceptOk {
        let initial_timestamp = Timestamp::from(pre_accept.txn_id); // t0 in the paper
        let mut max_conflicting_timestamp = initial_timestamp;
        let mut dependencies = Set::new();

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
                    dependencies_waiting: Set::new(),
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

        let pre_accepted = transaction.as_mut_pre_accepted().expect("TODO");
        pre_accepted.max_witnessed_at = cmp::max(pre_accepted.max_witnessed_at, accept.execute_at);

        *transaction = ReplicaTransactionProgress::Accepted(StageConsensus {
            execute_at: pre_accepted.execute_at,
            max_witnessed_at: pre_accepted.max_witnessed_at,
            dependencies_waiting: std::mem::take(&mut pre_accepted.dependencies_waiting),
            body: std::mem::take(&mut pre_accepted.body),
        });

        // TODO (clarity) extract into function.
        let mut dependencies = Set::new();
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
        let dummy = Set::new();
        let (mut root_guard, dummy_iter_guard) =
            split(commit.txn_id, &dummy, &mut self.transactions).expect("TODO");

        root_guard.with_mut(|progress| {
            let stage_consensus = progress.as_mut_pre_accepted_or_accepted().expect("TODO");
            let stage_execution = StageExecution::from(stage_consensus);

            let mut deps_waiting_guard = dummy_iter_guard
                .exchange(&stage_execution.dependencies_waiting)
                .expect("TODO");

            deps_waiting_guard
                .for_each_mut(|_dep_id, dep| dep.mark_dependency_committed(commit.txn_id));

            *progress = ReplicaTransactionProgress::Committed(Committed { stage_execution });
        });

        // TODO (correctness) why execute_at/max_witnessed_at are not updated? check with java version
    }

    fn register_pending_dependencies(
        txn_id: TxnId,
        deps_iter_guard: &mut IterGuard<TxnId, ReplicaTransactionProgress>,
    ) -> Map<TxnId, WaitingOn> {
        // Register this transaction in each of its dependencies so once they're
        // committed/applied this transaction can move forward too
        let mut pending_dependencies = Map::new();

        // TODO it is actually OK that dependency is not found because we send full set of dependencies
        deps_iter_guard.for_each_mut(|dep_id, dep| {
            use ReplicaTransactionProgress::*;

            // Register transaction as awaiting progress of this one (Commit/Apply interest)
            let waiting_on = match dep {
                PreAccepted(sc) | Accepted(sc) => {
                    tracing::info!(txn_id = ?txn_id, dep_id = ?dep_id, "pre_accepted/accepted");
                    assert!(sc.dependencies_waiting.insert(txn_id));
                    Some(WaitingOn::Commit)
                }
                Committed(c) => {
                    tracing::info!(txn_id = ?txn_id, dep_id = ?dep_id, deps = ?c.stage_execution.dependencies_waiting, "committed");
                    assert!(c.stage_execution.dependencies_waiting.insert(txn_id));
                    Some(WaitingOn::Apply)
                }
                CommittedReadPending((c, _)) => {
                    tracing::info!(txn_id = ?txn_id, dep_id = ?dep_id, "committed_read_pending");
                    assert!(c.stage_execution.dependencies_waiting.insert(txn_id));
                    Some(WaitingOn::Apply)
                }
                CommittedApplyPending(c) => {
                    tracing::info!(txn_id = ?txn_id, dep_id = ?dep_id, "committed_apply_pending");
                    // TODO validate this is correct for CommittedApplyPending
                    assert!(c.stage_execution.dependencies_waiting.insert(txn_id));
                    Some(WaitingOn::Apply)
                }
                Applied => {
                    tracing::info!(txn_id = ?txn_id, dep_id = ?dep_id, "applied");
                    None},
            };

            if let Some(waiting_on) = waiting_on {
                pending_dependencies.insert(*dep_id, waiting_on);
            }
        });

        pending_dependencies
    }

    pub fn receive_read(
        &mut self,
        src_node: NodeId,
        read: Read,
        data_store: &DataStore,
    ) -> Option<ReadOk> {
        let (mut root_guard, mut deps_iter_guard) =
            split(read.txn_id, &read.dependencies, &mut self.transactions).expect("TODO");

        root_guard.with_mut(|progress| {
            let stage_committed = progress.as_mut_committed().expect("TODO");
            assert!(stage_committed.pending_dependencies.is_empty());

            let pending_dependencies =
                Self::register_pending_dependencies(read.txn_id, &mut deps_iter_guard);

            if pending_dependencies.is_empty() {
                // we can proceed straight away
                // TODO (clarity): it is a simplification, transaction.body.keys contains all keys, not only
                //      ones specific to this shard (this is the same for Read struct though). Sort out which keys are passed in Read if any are needed.
                //      On Commit we can store two sets of keys, home keys and foreign ones so we quickly iterate over needed ones
                return Some(ReadOk {
                    txn_id: read.txn_id,
                    reads: data_store.read_keys_if_present(stage_committed.body.keys.iter()),
                });
            }
            tracing::info!(txn_id = ?read.txn_id, pending = ?pending_dependencies, "CommittedReadPending");

            stage_committed.pending_dependencies = pending_dependencies;

            *progress = ReplicaTransactionProgress::CommittedReadPending((
                Committed {
                    stage_execution: StageExecution::from(stage_committed),
                },
                ReadInterest { send_to: src_node },
            ));

            None
        })
    }

    fn propagate_apply_to_waiting_dependencies(
        txn_id: TxnId,
        mut dependencies_waiting_iter_guard: IterGuard<'_, '_, TxnId, ReplicaTransactionProgress>,
        res: &mut Vec<(NodeId, ReadOk)>,
        data_store: &mut DataStore,
    ) {
        use ReplicaTransactionProgress::*;

        // At this point there are no pending dependencies, go ahead with apply phase
        dependencies_waiting_iter_guard.for_each_mut(|dep_id, dep| {
            if !dep.mark_dependency_applied(txn_id) {
                return;
            }

            // We were the last dependency the dep was waiting on.
            // Now it can make progress.
            match dep {
                Committed(_) => {
                    // Here our dependency is no longer waiting on its dependencies
                    // but on Apply message to be received with execution result
                    // In this case when apply is received it will be immediately resolved
                }
                CommittedReadPending((c, read_interest)) => {
                    res.push((
                        read_interest.send_to,
                        ReadOk {
                            txn_id: *dep_id,
                            reads: data_store
                                .read_keys_if_present(c.stage_execution.body.keys.iter()),
                        },
                    ));

                    // TODO Should we transition into ApplyPending?
                    // We also need to be able to respond to read request multiple times
                }
                CommittedApplyPending(c) => *dep = c.apply(data_store),
                _ => unreachable!(),
            }
        });
    }

    /// If there are dependencies we're waiting on -> register ourselves as waiting
    /// If there are no such dependencies update dependencies waiting on us so they can move forward
    pub fn receive_apply(
        &mut self,
        _src_node: NodeId,
        apply: Apply,
        data_store: &mut DataStore,
    ) -> Vec<(NodeId, ReadOk)> {
        use ReplicaTransactionProgress::*;

        let (mut root_guard, mut pending_deps_iter_guard) =
            split(apply.txn_id, &apply.dependencies, &mut self.transactions).expect("TODO");

        let mut res = vec![];

        root_guard.with_mut(|progress| {
            match progress {
                Committed(committed) 
                // This could mean is that read request was satisfied by other replica and we become out of date.
                // So we should go forward with apply if we're ok dependency-wise.
                // Also since we cant transition to ApplyPending because we dont have result yet when
                // ReadOk is satisfied this can happen in normal flow
                | CommittedReadPending((committed, _)) => {
                    let pending_dependencies = Self::register_pending_dependencies(
                        apply.txn_id,
                        &mut pending_deps_iter_guard,
                    );

                    // There are pending dependencies, save them for later, progress will be made when they go forward
                    if !pending_dependencies.is_empty() {
                        committed.stage_execution.pending_dependencies = pending_dependencies;
                        *progress = committed.into_pending_apply(apply.result);
                        return;
                    }

                    // At this point there are no pending dependencies, go ahead with apply phase
                    Self::propagate_apply_to_waiting_dependencies(
                        apply.txn_id,
                        pending_deps_iter_guard
                            .exchange(&committed.stage_execution.dependencies_waiting)
                            // This cant fail because set of dependencies we wait on and set of dependencies
                            // waiting on us are disjoint. In other words there cant be a cycle.
                            .expect("failed to exchange for deps_waiting_guard"),
                        &mut res,
                        data_store,
                    );

                    let (key, value) = apply.result;
                    *progress = committed.apply(key, value, data_store);
                }
                CommittedApplyPending(_) => {
                    // means we received Apply second time, we transitioned into CommittedApplyPending
                    // and this is the state where transaction waits for its dependencies to apply after receiving Apply
                    unreachable!();
                }
                _ => panic!("TODO"),
            };
        });

        res
    }
}
