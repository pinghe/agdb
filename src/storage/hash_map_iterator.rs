use super::hash_map_data::HashMapData;
use super::hash_map_meta_value::HashMapMetaValue;
use super::StableHash;
use agdb_serialize::Serialize;
use std::hash::Hash;
use std::marker::PhantomData;

pub(crate) struct HashMapIterator<'a, K, T, Data>
where
    K: Clone + Default + Eq + Hash + PartialEq + StableHash + Serialize,
    T: Clone + Default + Serialize,
    Data: HashMapData<K, T>,
{
    pub(super) pos: u64,
    pub(super) data: &'a Data,
    pub(super) phantom_data: PhantomData<(K, T)>,
}

impl<'a, K, T, Data> HashMapIterator<'a, K, T, Data>
where
    K: Clone + Default + Eq + Hash + PartialEq + StableHash + Serialize,
    T: Clone + Default + Serialize,
    Data: HashMapData<K, T>,
{
}

impl<'a, K, T, Data> Iterator for HashMapIterator<'a, K, T, Data>
where
    K: Clone + Default + Eq + Hash + PartialEq + StableHash + Serialize,
    T: Clone + Default + Serialize,
    Data: HashMapData<K, T>,
{
    type Item = (K, T);

    fn next(&mut self) -> Option<Self::Item> {
        while self.pos < self.data.capacity() {
            let value = self.data.record(self.pos).unwrap_or_default();

            self.pos += 1;

            if value.meta_value == HashMapMetaValue::Valid {
                return Some((value.key, value.value));
            }
        }

        None
    }
}
