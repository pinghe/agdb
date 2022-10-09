use crate::dictionary_data::DictionaryData;
use crate::dictionary_index::DictionaryIndex;
use crate::dictionary_value::DictionaryValue;
use agdb_db_error::DbError;
use agdb_multi_map::StorageMultiMap;
use agdb_serialize::Serialize;
use agdb_storage::Storage;
use agdb_storage::StorageFile;
use agdb_storage::StorageIndex;
use agdb_storage_vec::StorageVec;
use agdb_utilities::StableHash;
use std::cell::RefCell;
use std::rc::Rc;

pub struct DictionaryDataStorage<T, Data = StorageFile>
where
    T: Clone + Default + Eq + PartialEq + StableHash + Serialize,
    Data: Storage,
{
    pub(crate) storage: Rc<RefCell<Data>>,
    pub(crate) storage_index: StorageIndex,
    pub(crate) index: StorageMultiMap<u64, DictionaryIndex, Data>,
    pub(crate) values: StorageVec<DictionaryValue<T>, Data>,
}

impl<T, Data> DictionaryData<T> for DictionaryDataStorage<T, Data>
where
    T: Clone + Default + Eq + PartialEq + StableHash + Serialize,
    Data: Storage,
{
    fn capacity(&self) -> u64 {
        self.values.len() as u64
    }

    fn commit(&mut self) -> Result<(), DbError> {
        self.storage.borrow_mut().commit()
    }

    fn indexes(&self, hash: u64) -> Result<Vec<DictionaryIndex>, DbError> {
        self.index.values(&hash)
    }

    fn insert(&mut self, hash: u64, index: &DictionaryIndex) -> Result<(), DbError> {
        self.index.insert(hash, index.clone())
    }

    fn hash(&self, index: &DictionaryIndex) -> Result<u64, DbError> {
        let values_index = self.values.storage_index();
        self.storage.borrow_mut().value_at::<u64>(
            &values_index,
            StorageVec::<DictionaryValue<T>>::value_offset(index.as_u64()) + i64::serialized_size(),
        )
    }

    fn meta(&self, index: &DictionaryIndex) -> Result<i64, DbError> {
        let values_index = self.values.storage_index();
        self.storage.borrow_mut().value_at::<i64>(
            &values_index,
            StorageVec::<DictionaryValue<T>>::value_offset(index.as_u64()),
        )
    }

    fn remove(&mut self, hash: u64, index: &DictionaryIndex) -> Result<(), DbError> {
        self.index.remove_value(&hash, &index)
    }

    fn set_hash(&mut self, index: &DictionaryIndex, hash: u64) -> Result<(), DbError> {
        let values_index = self.values.storage_index();
        self.storage.borrow_mut().insert_at(
            &values_index,
            StorageVec::<DictionaryValue<T>>::value_offset(index.as_u64()) + u64::serialized_size(),
            &hash,
        )
    }

    fn set_meta(&mut self, index: &DictionaryIndex, meta: i64) -> Result<(), DbError> {
        let values_index = self.values.storage_index();
        self.storage.borrow_mut().insert_at(
            &values_index,
            StorageVec::<DictionaryValue<T>>::value_offset(index.as_u64()),
            &meta,
        )
    }

    fn set_value(
        &mut self,
        index: &DictionaryIndex,
        value: DictionaryValue<T>,
    ) -> Result<(), DbError> {
        if self.capacity() == index.as_u64() {
            self.values.push(&value)
        } else {
            self.values.set_value(index.as_u64(), &value)
        }
    }

    fn transaction(&mut self) {
        self.storage.borrow_mut().transaction()
    }

    fn value(&self, index: &DictionaryIndex) -> Result<DictionaryValue<T>, DbError> {
        self.values.value(index.as_u64())
    }
}