use std::collections::HashMap;

use crate::{
    coordinator::Coordinator,
    messages::{
        Accept, AcceptOk, Apply, Commit, CommitAndRead, EitherCommitOrAccept, NewTransaction,
        PreAccept, PreAcceptOk, Read, ReadOk,
    },
    replica::Replica,
    timestamp::TimestampProvider,
    topology::Topology,
    transaction::{Key, Value},
    NodeId,
};

#[derive(Default)]
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

    pub fn insert(&mut self, key: Key, value: Value) -> Option<Value> {
        self.data.insert(key, value)
    }
}

/// One node takes responsibility for one or more shards
pub struct Node {
    id: NodeId,
    topology: Topology,
    timestamp_provider: TimestampProvider,

    coordinator: Coordinator,
    replica: Replica,

    data_store: DataStore,
    // Shadow field with links to other nodes to write asserts?
}

impl Node {
    pub fn new(
        id: NodeId,
        topology: Topology,
        timestamp_provider: TimestampProvider,
        coordinator: Coordinator,
        replica: Replica,
        data_store: DataStore,
    ) -> Self {
        Self {
            id,
            topology,
            timestamp_provider,
            coordinator,
            replica,
            data_store,
        }
    }

    #[cfg(any(test, kani))]
    pub(crate) fn data_store_mut(&mut self) -> &mut DataStore {
        &mut self.data_store
    }

    pub fn receive_new_transaction(&mut self, txn: NewTransaction) -> Vec<(NodeId, PreAccept)> {
        self.coordinator
            .receive_new_transaction(txn, &mut self.timestamp_provider, &self.topology)
    }

    pub fn receive_pre_accept(&mut self, pre_accept: PreAccept) -> PreAcceptOk {
        self.replica.receive_pre_accept(pre_accept, self.id)
    }

    pub fn receive_pre_accept_ok(
        &mut self,
        src_node: NodeId,
        pre_accept_ok: PreAcceptOk,
    ) -> Option<EitherCommitOrAccept> {
        self.coordinator
            .receive_pre_accept_ok(src_node, pre_accept_ok, &self.topology)
    }

    pub fn receive_accept(&mut self, accept: Accept) -> AcceptOk {
        self.replica.receive_accept(accept)
    }

    pub fn receive_accept_ok(
        &mut self,
        src_node: NodeId,
        accept_ok: AcceptOk,
    ) -> Option<CommitAndRead> {
        self.coordinator
            .receive_accept_ok(src_node, accept_ok, &self.topology)
    }

    /// Executed on replica, represents handling of Commit message
    pub fn receive_commit(&mut self, commit: Commit) {
        self.replica.receive_commit(commit)
    }

    pub fn receive_read(&mut self, src_node: NodeId, read: Read) -> Option<ReadOk> {
        self.replica.receive_read(src_node, read, &self.data_store)
    }

    pub fn receive_read_ok(
        &mut self,
        src_node: NodeId,
        read_ok: ReadOk,
    ) -> Option<Vec<(NodeId, Apply)>> {
        self.coordinator
            .receive_read_ok(src_node, read_ok, &self.topology)
    }

    pub fn receive_apply(&mut self, src_node: NodeId, apply: Apply) -> Vec<(NodeId, ReadOk)> {
        self.replica
            .receive_apply(src_node, apply, &mut self.data_store)
    }
}

// TODO idea: HashSet or Vec for dependencies? What is more efficient, union of hashsets or merge sort-like union operation?
//    vec deque to store transactions in order of their id (if that is needed)? search will be not that efficient
