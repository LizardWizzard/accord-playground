use std::collections::{HashMap, HashSet};

use crate::{
    timestamp::{Timestamp, TxnId},
    NodeId,
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Key(usize);

#[derive(Debug, Clone)]
pub struct Value(pub usize);

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TransactionBody {
    pub keys: HashSet<Key>,
}

// Dummy execute function, take all keys except the last one, add their sum to last key's Value
pub fn execute(mut reads: Vec<(Key, Value)>) -> (Key, Value) {
    // sort_by_key doesnt work as is because of issue with lifetimes
    reads.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

    let all_but_one = reads.len() - 1;

    let sum: usize = reads.iter().take(all_but_one).map(|(_k, v)| v.0).sum();

    let (last_k, last_v) = reads.pop().unwrap();

    (last_k, Value(last_v.0 + sum))
}
