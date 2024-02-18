#![cfg(kani)]

use std::{borrow::Borrow, ops::Index};

use arrayvec::{ArrayVec, IntoIter};

const CAPACITY: usize = 10;

#[derive(Debug, Clone)]
pub struct ArrayMap<K, V> {
    keys: ArrayVec<K, CAPACITY>,
    values: ArrayVec<V, CAPACITY>,
}

impl<K, V> ArrayMap<K, V> {
    pub fn new() -> Self {
        ArrayMap {
            keys: ArrayVec::new(),
            values: ArrayVec::new(),
        }
    }
}

impl<K: Eq, V> ArrayMap<K, V> {
    pub fn insert(&mut self, k: K, v: V) -> Option<V> {
        let existing_position = match self.position(&k) {
            Some(p) => p,
            None => {
                self.keys.push(k);
                self.values.push(v);
                return None;
            }
        };

        self.keys.remove(existing_position);
        let prev_value = self.values.remove(existing_position);

        self.keys.push(k);
        self.values.push(v);

        Some(prev_value)
    }

    pub fn contains_key<Q: ?Sized>(&self, search_key: &Q) -> bool
    where
        Q: Borrow<K>,
    {
        for key in &self.keys {
            if key == search_key.borrow() {
                return true;
            }
        }
        false
    }

    pub fn get<Q: ?Sized>(&self, k: &Q) -> Option<&V>
    where
        Q: Borrow<K>,
    {
        let position = self.position(k.borrow())?;

        Some(self.values.get(position).unwrap())
    }

    pub fn get_mut<Q: ?Sized>(&mut self, k: &Q) -> Option<&mut V>
    where
        Q: Borrow<K>,
    {
        let position = self.position(k.borrow())?;

        Some(self.values.get_mut(position).unwrap())
    }

    pub fn iter(&self) -> Iter<'_, K, V> {
        Iter {
            keys: self.keys.iter(),
            values: self.values.iter(),
        }
    }

    pub fn values_mut(&mut self) -> ValuesMut<'_, V> {
        ValuesMut {
            inner: self.values.iter_mut(),
        }
    }

    pub fn drain(&mut self) -> Drain<'_, K, V> {
        Drain {
            keys: self.keys.drain(..),
            values: self.values.drain(..),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    pub fn reserve(&self, _additional: usize) {}

    pub fn entry(&mut self, key: K) -> Entry<'_, K, V> {
        match self.position(&key) {
            Some(pos) => Entry::Occupied(OccupiedEntry { pos, map: self }),
            None => Entry::Vacant(VacantEntry { key, map: self }),
        }
    }

    pub(crate) fn position(&self, k: &K) -> Option<usize> {
        for (idx, key) in self.keys.iter().enumerate() {
            if key == k {
                return Some(idx);
            }
        }
        None
    }

    pub(crate) fn as_values_ptr(&self) -> *const V {
        self.values.as_ptr()
    }
}

pub struct Drain<'a, K, V> {
    keys: arrayvec::Drain<'a, K, CAPACITY>,
    values: arrayvec::Drain<'a, V, CAPACITY>,
}

impl<'a, K, V> Iterator for Drain<'a, K, V> {
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        let next_k = self.keys.next()?;
        let next_v = self.values.next().unwrap();
        Some((next_k, next_v))
    }
}

pub struct Iter<'a, K, V> {
    keys: std::slice::Iter<'a, K>,
    values: std::slice::Iter<'a, V>,
}

impl<'a, K, V> Iterator for Iter<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        let next_key = self.keys.next()?;
        let next_value = self.values.next().unwrap();
        Some((next_key, next_value))
    }
}

pub struct ValuesMut<'a, V> {
    inner: core::slice::IterMut<'a, V>,
}

impl<'a, V> Iterator for ValuesMut<'a, V> {
    type Item = &'a mut V;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

impl<K, V> Default for ArrayMap<K, V> {
    fn default() -> Self {
        Self {
            keys: Default::default(),
            values: Default::default(),
        }
    }
}

impl<K: Eq, V> Index<&K> for ArrayMap<K, V> {
    type Output = V;

    fn index(&self, key: &K) -> &Self::Output {
        let position = self.position(&key).expect("no such key");
        &self.values[position]
    }
}

pub struct OccupiedEntry<'a, K, V> {
    pos: usize,
    map: &'a mut ArrayMap<K, V>,
}

impl<'a, K, V> OccupiedEntry<'a, K, V> {
    pub fn get(&self) -> &V {
        &self.map.values[self.pos]
    }

    pub fn get_mut(&mut self) -> &mut V {
        &mut self.map.values[self.pos]
    }

    pub fn remove(self) -> V {
        self.map.keys.remove(self.pos);
        self.map.values.remove(self.pos)
    }
}

pub struct VacantEntry<'a, K, V> {
    key: K,
    map: &'a mut ArrayMap<K, V>,
}

impl<'a, K: Eq, V> VacantEntry<'a, K, V> {
    pub fn insert(self, value: V) -> &'a mut V {
        let pos = self.map.keys.len();
        self.map.insert(self.key, value);
        self.map.values.get_mut(pos).unwrap()
    }
}

pub enum Entry<'a, K, V> {
    Occupied(OccupiedEntry<'a, K, V>),
    Vacant(VacantEntry<'a, K, V>),
}

impl<'a, K: Eq, V> Entry<'a, K, V> {
    pub fn or_insert_with<F: FnOnce() -> V>(self, default: F) -> &'a mut V {
        match self {
            Entry::Occupied(entry) => &mut entry.map.values[entry.pos],
            Entry::Vacant(entry) => entry.insert(default()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ArraySet<T> {
    map: ArrayMap<T, ()>,
}

impl<T> ArraySet<T> {
    pub fn new() -> Self {
        Self {
            map: ArrayMap::new(),
        }
    }
}

impl<T: Eq> ArraySet<T> {
    pub fn contains<Q: ?Sized>(&self, value: &Q) -> bool
    where
        Q: Borrow<T>,
    {
        self.map.contains_key(value)
    }

    pub fn insert(&mut self, value: T) -> bool {
        self.map.insert(value, ()).is_none()
    }

    pub fn reserve(&self, _additional: usize) {}

    pub fn iter(&self) -> std::slice::Iter<'_, T> {
        self.map.keys.iter()
    }

    pub fn intersection<'a>(&'a self, other: &'a ArraySet<T>) -> Intersection<'a, T> {
        let (smaller, larger) = if self.map.keys.len() <= other.map.keys.len() {
            (self, other)
        } else {
            (other, self)
        };

        Intersection {
            iter: smaller.map.keys.iter(),
            other: larger,
        }
    }

    pub fn len(&self) -> usize {
        self.map.keys.len()
    }
}

impl<'a, T: Eq + Copy + 'a> ArraySet<T> {
    pub fn extend(&mut self, other: &ArraySet<T>) {
        for k in &other.map.keys {
            self.insert(*k);
        }
    }
}

impl<'a, T> IntoIterator for &'a ArraySet<T> {
    type Item = &'a T;
    type IntoIter = core::slice::Iter<'a, T>;

    fn into_iter(self) -> core::slice::Iter<'a, T> {
        self.map.keys.iter()
    }
}

impl<T> IntoIterator for ArraySet<T> {
    type Item = T;
    type IntoIter = IntoIter<T, CAPACITY>;

    fn into_iter(self) -> Self::IntoIter {
        self.map.keys.into_iter()
    }
}

pub struct Intersection<'a, T> {
    // iterator of the first set
    iter: std::slice::Iter<'a, T>,
    // the second set
    other: &'a ArraySet<T>,
}

impl<'a, T: Eq> Iterator for Intersection<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(next) = self.iter.next() {
            if self.other.contains(&next) {
                return Some(next);
            }
        }
        None
    }
}

impl<T> Default for ArraySet<T> {
    fn default() -> Self {
        Self {
            map: Default::default(),
        }
    }
}
