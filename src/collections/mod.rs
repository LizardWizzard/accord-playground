#[allow(clippy::disallowed_types)]
#[cfg(not(kani))]
mod default {
    use hashbrown::hash_map::Entry;
    use hashbrown::raw::Bucket;
    use hashbrown::{Equivalent, HashMap, HashSet};
    use std::hash::{BuildHasher, Hash};
    use std::marker::PhantomData;

    pub type Set<T> = HashSet<T>;
    pub type Map<K, V> = HashMap<K, V>;
    pub type MapEntry<'a, K, V, S, A> = Entry<'a, K, V, S, A>;

    // The problem this object aims to solve is that we cant mutably borrow different hashmap entities at the same time
    // because rustc cant see that they are different thus it guards us from mutably borrowing one entity several times.
    // Since we can guarantee that we wont borrow the same entity twice we can safely mutate different ones at the same time.
    // TODO: HashMap during lookup can touch keys other than the passed one, thus it can dereference root key while searching
    // for other keys, thus creating a shared borrow in addition to mutable one
    // TODO
    pub struct Lens<'a, 'b, K, V> {
        root_key: K,
        other_keys: &'a Set<K>,
        source: &'b mut Map<K, V>,
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
        pub fn zoom(
            root_key: K,
            other_keys: &'a Set<K>,
            source: &'b mut Map<K, V>,
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

    pub struct LensRootGuard<'a, K, V> {
        bucket: Bucket<(K, V)>,
        _phantom: PhantomData<&'a ()>,
    }

    impl<'b, K, V> LensRootGuard<'b, K, V>
    where
        K: Eq + Hash,
    {
        pub fn with_mut<U>(&mut self, f: impl FnOnce(&mut V) -> U) -> U {
            unsafe {
                let root = self.bucket.as_mut();
                f(&mut root.1)
            }
        }
    }

    pub struct LensIterGuard<'a, 'b, K, V> {
        root_key: K,
        other_keys: &'a Set<K>,
        source: &'b mut Map<K, V>,
    }
    impl<'a, 'b, K, V> LensIterGuard<'a, 'b, K, V>
    where
        K: Eq + Hash,
    {
        pub fn for_each_mut(&self, mut f: impl FnMut(&K, &mut V)) {
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

        pub fn exchange(self, other_keys: &'a Set<K>) -> Option<Self> {
            if other_keys.contains(&self.root_key) {
                return None;
            }

            Some(Self {
                root_key: self.root_key,
                other_keys,
                source: self.source,
            })
        }
    }
}

// #[cfg(test)]
#[cfg(kani)]
mod verification {
    use std::marker::PhantomData;

    use vector_map::{set::VecSet, Entry, VecMap};

    pub type Set<T> = VecSet<T>;
    pub type Map<K, V> = VecMap<K, V>;
    pub type MapEntry<'a, K, V> = Entry<'a, K, V>;

    pub struct Lens<'a, 'b, K, V> {
        root_key: K,
        other_keys: &'a Set<K>,
        source: &'b mut Map<K, V>,
    }

    impl<'a, 'b, K, V> Lens<'a, 'b, K, V>
    where
        K: Eq,
    {
        pub fn zoom(
            root_key: K,
            other_keys: &'a Set<K>,
            source: &'b mut Map<K, V>,
        ) -> Option<(LensRootGuard<'b, K, V>, LensIterGuard<'a, 'b, K, V>)> {
            if other_keys.contains(&root_key) {
                return None;
            }

            let root_position = source.position(&root_key).unwrap();
            let root_ptr = unsafe { source.as_values_ptr().add(root_position) };

            Some((
                LensRootGuard {
                    root_ptr,
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

    pub struct LensRootGuard<'a, K, V> {
        root_ptr: *const V,
        _phantom: PhantomData<&'a (K, V)>,
    }

    impl<'b, K, V> LensRootGuard<'b, K, V>
    where
        K: Eq,
    {
        pub fn with_mut<U>(&mut self, f: impl FnOnce(&mut V) -> U) -> U {
            unsafe {
                // This cant be unsound: https://github.com/rust-lang/rust/issues/66136
                let mut root = &mut *(self.root_ptr as *mut V);
                f(&mut root)
            }
        }
    }

    pub struct LensIterGuard<'a, 'b, K, V> {
        root_key: K,
        other_keys: &'a Set<K>,
        source: &'b mut Map<K, V>,
    }
    impl<'a, 'b, K, V> LensIterGuard<'a, 'b, K, V>
    where
        K: Eq,
    {
        pub fn for_each_mut(&self, mut f: impl FnMut(&K, &mut V)) {
            for key in self.other_keys {
                let position = self.source.position(key).unwrap();
                unsafe {
                    let ptr = self.source.as_values_ptr().add(position);
                    let v = &mut *(ptr as *mut V);
                    f(key, v)
                }
            }
        }

        pub fn exchange(self, other_keys: &'a Set<K>) -> Option<Self> {
            if other_keys.contains(&self.root_key) {
                return None;
            }

            Some(Self {
                root_key: self.root_key,
                other_keys,
                source: self.source,
            })
        }
    }
}

#[cfg(not(kani))]
pub use default::*;

#[cfg(kani)]
pub use verification::*;
