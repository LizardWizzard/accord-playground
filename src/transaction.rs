use std::collections::{HashMap, HashSet};

use crate::{
    timestamp::{Timestamp, TxnId},
    NodeId,
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Key(usize);

#[derive(Debug, Clone)]
pub struct Value(usize);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionBody {
    pub keys: HashSet<Key>,
}

#[derive(Debug, Clone)]
pub struct NewTransaction {
    pub body: TransactionBody,
}

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

        match dep {
            WaitingOn::Commit => *dep = WaitingOn::Apply,
            WaitingOn::Apply => unreachable!("cant be applied before committed"),
        }
    }
}
