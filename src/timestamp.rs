use crate::NodeId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp {
    // assigned from a per-process monotonically increasing value
    // that is loosely synchronised with the wall clock
    pub time: u64,
    // logical time component
    pub seq: u64,
    // unique identifier of the process (node) that created the timestamp
    pub id: NodeId,
}

impl Timestamp {
    // TODO revisit abstractions so this is not exposed in such a raw form
    pub fn new(time: u64, seq: u64, id: NodeId) -> Self {
        Timestamp { time, seq, id }
    }
}

impl From<TxnId> for Timestamp {
    fn from(t: TxnId) -> Self {
        t.0
    }
}

/// Transaction id is a separate flavour of a Timestamp. It represents initial
/// timestamp that the transaction gets assigned on the coordinator.
/// At a later stages transaction is refferred to by its initial timestamp
/// thus the name - transaction id
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TxnId(Timestamp);

impl From<Timestamp> for TxnId {
    fn from(t: Timestamp) -> Self {
        TxnId(t)
    }
}

impl AsRef<Timestamp> for TxnId {
    fn as_ref(&self) -> &Timestamp {
        &self.0
    }
}

#[derive(Debug)]
pub struct TimestampProvider {
    node_id: NodeId,
    counter: u64,
}

impl TimestampProvider {
    pub fn new(node_id: NodeId) -> Self {
        TimestampProvider {
            node_id,
            counter: 0,
        }
    }

    pub fn next(&mut self) -> Timestamp {
        self.counter += 1;
        Timestamp {
            time: self.counter,
            seq: 0,
            id: self.node_id,
        }
    }
}
