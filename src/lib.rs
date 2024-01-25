use std::collections::HashSet;
use std::hash::{BuildHasher, Hash};
use std::marker::PhantomData;

use hashbrown::raw::Bucket;
use hashbrown::{Equivalent, HashMap};

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

// The problem this object aims to solve is that we cant mutably borrow different hashmap entities at the same time
// because rustc cant see that they are different thus it guards us from mutably borrowing one entity several times.
// Since we can guarantee that we wont borrow the same entity twice we can safely mutate different ones at the same time.
struct Lens<'a, 'b, K, V> {
    root_key: K,
    other_keys: &'a HashSet<K>,
    source: &'b mut HashMap<K, V>,
}

impl<'a, 'b, K, V> Lens<'a, 'b, K, V>
where
    K: Eq + Hash,
{
    // TODO there can be a trait that provides a method that returns id and a unique sequence that are guaranteed to be unique together
    // I e Apply message can easily implement it, because transaction cant depend on itself thus Apply.txn_id
    // and Apply.dependencies are guaranteed to be unique together
    // TODO clarify that guards are needed because if these methods exist on Lens itself we must have shared reference as self parameter
    // which is wrong because this way we'd be able to invoke several for_each_mut iterators inside of each other which would end
    // up in multiple mutable references existing for the same memory location which violates safety
    fn new(
        root_key: K,
        other_keys: &'a HashSet<K>,
        source: &'b mut HashMap<K, V>,
    ) -> Option<(LensRootGuard<'b, K, V>, LensIterGuard<'a, 'b, K, V>)> {
        if other_keys.contains(&root_key) {
            return None;
        }

        let hash = source.hasher().hash_one(&root_key);
        let root_bucket = source
            .raw_table()
            .find(hash, |(k, _)| k.equivalent(&root_key))
            .expect("TODO");

        Some((
            LensRootGuard {
                bucket: root_bucket,
                _phantom: PhantomData,
            },
            LensIterGuard {
                other_keys,
                source,
                root_key,
            },
        ))
    }
}

struct LensRootGuard<'a, K, V> {
    bucket: Bucket<(K, V)>,
    _phantom: PhantomData<&'a ()>,
}

impl<'b, K, V> LensRootGuard<'b, K, V>
where
    K: Eq + Hash,
{
    fn with_mut<U>(&mut self, f: impl FnOnce(&mut V) -> U) -> U {
        unsafe {
            let root = self.bucket.as_mut();
            f(&mut root.1)
        }
    }
}

struct LensIterGuard<'a, 'b, K, V> {
    root_key: K,
    other_keys: &'a HashSet<K>,
    source: &'b mut HashMap<K, V>,
}
impl<'a, 'b, K, V> LensIterGuard<'a, 'b, K, V>
where
    K: Eq + Hash,
{
    fn for_each_mut(&self, mut f: impl FnMut(&K, &mut V)) {
        for key in self.other_keys {
            let hash = self.source.hasher().hash_one(key);
            let entry = self
                .source
                .raw_table()
                .find(hash, |(k, _)| k.equivalent(key))
                .expect("TODO");

            unsafe {
                let (k, v) = entry.as_mut();
                f(k, v)
            }
        }
    }

    fn exchange(self, other_keys: &'a HashSet<K>) -> Option<Self> {
        if other_keys.contains(&self.root_key) {
            return None;
        }

        Some(Self {
            root_key: self.root_key,
            other_keys: other_keys,
            source: self.source,
        })
    }
}
