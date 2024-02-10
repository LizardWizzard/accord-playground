use crate::protocol::NodeId;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
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

impl std::fmt::Debug for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("Ts({}.{}.{})", self.time, self.seq, self.id))
    }
}

/// Transaction id is a separate flavour of a Timestamp. It represents initial
/// timestamp that the transaction gets assigned on the coordinator.
/// At a later stages transaction is refferred to by its initial timestamp
/// thus the name - transaction id
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
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

impl std::fmt::Debug for TxnId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "TxnId({}.{}.{})",
            self.0.time, self.0.seq, self.0.id
        ))
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

    pub fn tick_next(&mut self) -> Timestamp {
        self.counter += 1;
        Timestamp {
            time: self.counter,
            seq: 0,
            id: self.node_id,
        }
    }
}
