#![cfg(verification)]

use arrayvec;

struct ArrayMap<K, V> {
    keys: arrayvec,
}
