use std::collections::{HashMap, HashSet};

use crate::{
    timestamp::{Timestamp, TxnId},
    transaction::{Key, TransactionBody, Value},
    NodeId,
};

#[derive(Debug, Clone)]
pub struct NewTransaction {
    pub body: TransactionBody,
}

pub struct PreAccept {
    pub txn_id: TxnId,
    pub body: TransactionBody,
}

pub struct PreAcceptOk {
    pub txn_id: TxnId,
    pub execute_at: Timestamp,
    pub dependencies: HashSet<TxnId>,
}

#[derive(Debug)]
pub struct Commit {
    pub txn_id: TxnId,
    pub execute_at: Timestamp,
    pub dependencies: HashSet<TxnId>,
}

pub struct Accept {
    pub txn_id: TxnId,
    pub execute_at: Timestamp,
    pub dependencies: HashSet<TxnId>,
    // TODO (correctness): at this point we've already sent body in PreAccept, why send it second time? Check with java version
    pub body: TransactionBody,
}

pub enum EitherCommitOrAccept {
    Commit(CommitAndRead),
    Accept(HashMap<NodeId, Accept>),
}

pub struct AcceptOk {
    pub txn_id: TxnId,
    pub dependencies: HashSet<TxnId>,
}

pub struct Read {
    pub txn_id: TxnId,
    pub execute_at: Timestamp,
    pub dependencies: HashSet<TxnId>,
    pub keys: HashSet<Key>,
}

// TODO (perf): coalesce Commit and Read coming to one node into one network packet
pub struct CommitAndRead {
    pub commits: HashMap<NodeId, Commit>,
    pub reads: Vec<(NodeId, Read)>,
}

pub struct ReadOk {
    pub txn_id: TxnId,
    pub reads: Vec<(Key, Value)>,
}

pub struct Apply {
    pub txn_id: TxnId,
    pub execute_at: Timestamp,
    pub dependencies: HashSet<TxnId>,
    // Result of the computation for transaction. For now we just take sum
    // of the keys that were read and add it to one of the keys value
    pub result: (Key, Value),
}
