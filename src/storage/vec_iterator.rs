use super::StorageVec;
use agdb_serialize::Serialize;
use agdb_storage::Storage;

pub(crate) struct VecIterator<'a, T, Data>
where
    T: Serialize,
    Data: Storage,
{
    pub(super) index: u64,
    pub(super) vec: &'a StorageVec<T, Data>,
    pub(super) phantom_data: std::marker::PhantomData<T>,
}

impl<'a, T, Data> Iterator for VecIterator<'a, T, Data>
where
    T: Serialize,
    Data: Storage,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let value = self.vec.value(self.index).ok();
        self.index += 1;

        value
    }
}
