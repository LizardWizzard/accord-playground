mod coordinator;
mod messages;
pub mod node;
mod quorum_tracker;
mod replica;
mod timestamp;
mod topology;
mod transaction;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(u16);
