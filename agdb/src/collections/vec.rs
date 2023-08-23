use crate::db::db_error::DbError;
use crate::storage::file_storage::FileStorage;
use crate::storage::Storage;
use crate::storage::StorageIndex;
use crate::utilities::serialize::Serialize;
use crate::utilities::serialize::SerializeStatic;
use std::marker::PhantomData;

pub trait VecData<T, S, E> {
    fn capacity(&self) -> u64;
    fn len(&self) -> u64;
    fn reallocate(&mut self, storage: &mut S, capacity: u64) -> Result<(), E>;
    fn remove(&mut self, storage: &mut S, index: u64) -> Result<T, E>;
    fn replace(&mut self, storage: &mut S, index: u64, value: &T) -> Result<T, E>;
    fn resize(&mut self, storage: &mut S, new_len: u64, value: &T) -> Result<(), E>;
    fn swap(&mut self, storage: &mut S, index: u64, other: u64) -> Result<(), E>;
    fn value(&self, index: u64) -> Result<T, E>;
}

pub trait VecValue: Sized {
    fn store<S: Storage>(&self, storage: &mut S) -> Result<Vec<u8>, DbError>;
    fn load<S: Storage>(storage: &mut S, bytes: &[u8]) -> Result<Self, DbError>;
    fn remove<S: Storage>(storage: &mut S, _bytes: &[u8]) -> Result<(), DbError>;
    fn storage_len() -> u64;
}

impl VecValue for u64 {
    fn store<S: Storage>(&self, _storage: &mut S) -> Result<Vec<u8>, DbError> {
        Ok(self.serialize())
    }

    fn load<S: Storage>(_storage: &mut S, bytes: &[u8]) -> Result<Self, DbError> {
        Self::deserialize(bytes)
    }

    fn remove<S: Storage>(_storage: &mut S, _bytes: &[u8]) -> Result<(), DbError> {
        Ok(())
    }

    fn storage_len() -> u64 {
        Self::serialized_size_static()
    }
}

impl VecValue for i64 {
    fn store<S: Storage>(&self, _storage: &mut S) -> Result<Vec<u8>, DbError> {
        Ok(self.serialize())
    }

    fn load<S: Storage>(_storage: &mut S, bytes: &[u8]) -> Result<Self, DbError> {
        Self::deserialize(bytes)
    }

    fn remove<S: Storage>(_storage: &mut S, _bytes: &[u8]) -> Result<(), DbError> {
        Ok(())
    }

    fn storage_len() -> u64 {
        Self::serialized_size_static()
    }
}

impl VecValue for String {
    fn store<S: Storage>(&self, storage: &mut S) -> Result<Vec<u8>, DbError> {
        let index = storage.insert(self)?;
        Ok(index.serialize())
    }

    fn load<S: Storage>(storage: &mut S, bytes: &[u8]) -> Result<Self, DbError> {
        let index = StorageIndex::deserialize(bytes)?;
        storage.value(index)
    }

    fn remove<S: Storage>(storage: &mut S, bytes: &[u8]) -> Result<(), DbError> {
        let index = StorageIndex::deserialize(bytes)?;
        storage.remove(index)
    }

    fn storage_len() -> u64 {
        StorageIndex::serialized_size_static()
    }
}

pub struct DbVecData<T, S, E>
where
    T: Clone + VecValue,
    S: Storage,
    E: From<DbError>,
{
    phantom_data: PhantomData<(S, E)>,
    capacity: u64,
    storage_index: StorageIndex,
    data: Vec<T>,
}

impl<T, S, E> DbVecData<T, S, E>
where
    T: Clone + VecValue,
    S: Storage,
    E: From<DbError>,
{
    fn offset(index: u64) -> u64 {
        u64::serialized_size_static() + T::storage_len() * index
    }
}

impl<T, S, E> VecData<T, S, E> for DbVecData<T, S, E>
where
    T: Clone + VecValue,
    S: Storage,
    E: From<DbError>,
{
    fn capacity(&self) -> u64 {
        self.capacity
    }

    fn len(&self) -> u64 {
        self.data.len() as u64
    }

    fn reallocate(&mut self, storage: &mut S, capacity: u64) -> Result<(), E> {
        self.capacity = capacity;

        storage.resize_value(
            self.storage_index,
            self.len().serialized_size() + T::storage_len() * capacity,
        )?;

        let current_capacity = self.data.capacity();

        if capacity < current_capacity as u64 {
            self.data.shrink_to(capacity as usize);
        } else {
            self.data.reserve(capacity as usize - current_capacity);
        }

        Ok(())
    }

    fn remove(&mut self, storage: &mut S, index: u64) -> Result<T, E> {
        let offset_from = Self::offset(index + 1);
        let offset_to = Self::offset(index);
        let move_len = T::storage_len() * (self.len() - index);
        let bytes = storage.value_as_bytes_at_size(
            self.storage_index,
            Self::offset(index),
            T::storage_len(),
        )?;
        let id = storage.transaction();
        T::remove(storage, &bytes)?;
        storage.move_at(self.storage_index, offset_from, offset_to, move_len)?;
        storage.insert_at(self.storage_index, 0, &(self.len() - 1))?;
        storage.commit(id)?;
        Ok(self.data.remove(index as usize))
    }

    fn replace(&mut self, storage: &mut S, index: u64, value: &T) -> Result<T, E> {
        let old_bytes = storage.value_as_bytes_at_size(
            self.storage_index,
            Self::offset(index),
            T::storage_len(),
        )?;
        let id = storage.transaction();
        T::remove(storage, &old_bytes)?;
        let bytes = value.store(storage)?;
        storage.insert_bytes_at(self.storage_index, Self::offset(index), &bytes)?;
        storage.commit(id)?;
        let old_value = self.data[index as usize].clone();
        self.data[index as usize] = value.clone();

        Ok(old_value)
    }

    fn resize(&mut self, storage: &mut S, new_len: u64, value: &T) -> Result<(), E> {
        let id = storage.transaction();

        for index in self.len()..new_len {
            let bytes = value.store(storage)?;
            storage.insert_bytes_at(self.storage_index, Self::offset(index), &bytes)?;
        }

        for index in new_len..self.len() {
            let old_bytes = storage.value_as_bytes_at_size(
                self.storage_index,
                Self::offset(index),
                T::storage_len(),
            )?;
            T::remove(storage, &old_bytes)?;
        }

        storage.insert_at(self.storage_index, 0, &new_len)?;
        storage.commit(id)?;
        self.data.resize_with(new_len as usize, || value.clone());
        Ok(())
    }

    fn swap(&mut self, storage: &mut S, index: u64, other: u64) -> Result<(), E> {
        let offset_from = Self::offset(other);
        let offset_to = Self::offset(index);
        let size = T::storage_len();
        let bytes = storage.value_as_bytes_at_size(
            self.storage_index,
            Self::offset(index),
            T::storage_len(),
        )?;
        let id = storage.transaction();
        storage.move_at(self.storage_index, offset_from, offset_to, size)?;
        storage.insert_bytes_at(self.storage_index, Self::offset(other), &bytes)?;
        storage.commit(id)?;
        self.data.swap(index as usize, other as usize);
        Ok(())
    }

    fn value(&self, index: u64) -> Result<T, E> {
        Ok(self.data[index as usize].clone())
    }
}

pub struct VecImpl<T, S, D, E>
where
    T: VecValue,
    D: VecData<T, S, E>,
    E: From<DbError> + From<String>,
{
    phantom_data: PhantomData<(T, S, E)>,
    data: D,
}

pub struct VecIterator<'a, T, S, D, E>
where
    T: VecValue,
    D: VecData<T, S, E>,
    E: From<DbError> + From<String>,
{
    pub index: u64,
    pub vec: &'a VecImpl<T, S, D, E>,
}

pub type DbVec<T, S = FileStorage> = VecImpl<T, S, DbVecData<T, S, DbError>, DbError>;

impl<'a, T, S, D, E> Iterator for VecIterator<'a, T, S, D, E>
where
    T: VecValue,
    D: VecData<T, S, E>,
    E: From<DbError> + From<String>,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let value = self.vec.value(self.index).ok();
        self.index += 1;

        value
    }
}

impl<T, S, D, E> VecImpl<T, S, D, E>
where
    T: VecValue,
    D: VecData<T, S, E>,
    E: From<DbError> + From<String>,
{
    pub fn capacity(&self) -> u64 {
        self.data.capacity()
    }

    pub fn len(&self) -> u64 {
        self.data.len()
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[allow(dead_code)]
    pub fn iter(&self) -> VecIterator<T, S, D, E> {
        VecIterator {
            index: 0,
            vec: self,
        }
    }

    pub fn push(&mut self, storage: &mut S, value: &T) -> Result<(), E> {
        if self.data.len() == self.data.capacity() {
            self.data.reallocate(
                storage,
                std::cmp::max(64, self.capacity() + self.capacity() / 2),
            )?;
        }

        self.data.resize(storage, self.data.len() + 1, value)
    }

    #[allow(dead_code)]
    pub fn remove(&mut self, storage: &mut S, index: u64) -> Result<T, E> {
        self.validate_index(index)?;
        self.data.remove(storage, index)
    }

    pub fn replace(&mut self, storage: &mut S, index: u64, value: &T) -> Result<T, E> {
        self.validate_index(index)?;
        self.data.replace(storage, index, value)
    }

    #[allow(dead_code)]
    pub fn reserve(&mut self, storage: &mut S, capacity: u64) -> Result<(), E> {
        if self.capacity() < capacity {
            self.data.reallocate(storage, capacity)?;
        }

        Ok(())
    }

    pub fn resize(&mut self, storage: &mut S, new_len: u64, value: &T) -> Result<(), E> {
        if self.capacity() < new_len {
            self.data.reallocate(storage, new_len)?;
        }

        self.data.resize(storage, new_len, value)
    }

    #[allow(dead_code)]
    pub fn shrink_to_fit(&mut self, storage: &mut S) -> Result<(), E> {
        self.data.reallocate(storage, self.len())
    }

    pub fn swap(&mut self, storage: &mut S, index: u64, other: u64) -> Result<(), E> {
        if index == other {
            return Ok(());
        }

        self.validate_index(index)?;
        self.validate_index(other)?;
        self.data.swap(storage, index, other)
    }

    pub fn value(&self, index: u64) -> Result<T, E> {
        self.validate_index(index)?;
        self.data.value(index)
    }

    fn validate_index(&self, index: u64) -> Result<(), E> {
        if self.len() <= index {
            return Err(E::from(format!(
                "Index ({index}) out of bounds ({})",
                self.len()
            )));
        }

        Ok(())
    }
}

impl<T, S> DbVec<T, S>
where
    T: Clone + VecValue,
    S: Storage,
{
    pub fn new(storage: &mut S) -> Result<Self, DbError> {
        let storage_index = storage.insert(&0_u64)?;

        Ok(Self {
            phantom_data: PhantomData,
            data: DbVecData {
                phantom_data: PhantomData,
                capacity: 0,
                storage_index,
                data: vec![],
            },
        })
    }

    pub fn from_storage(storage: &mut S, storage_index: StorageIndex) -> Result<Self, DbError> {
        let mut data = Vec::<T>::new();
        let capacity;

        {
            let len = storage.value::<u64>(storage_index)?;
            let data_len = storage.value_size(storage_index)?;
            capacity = data_len / T::storage_len();

            data.reserve(capacity as usize);

            for index in 0..len {
                let bytes = storage.value_as_bytes_at_size(
                    storage_index,
                    DbVecData::<T, S, DbError>::offset(index),
                    T::storage_len(),
                )?;
                data.push(T::load(storage, &bytes)?);
            }
        }

        Ok(DbVec {
            phantom_data: PhantomData,
            data: DbVecData {
                phantom_data: PhantomData,
                capacity,
                storage_index,
                data,
            },
        })
    }

    pub fn storage_index(&self) -> StorageIndex {
        self.data.storage_index
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::file_storage::FileStorage;
    use crate::test_utilities::test_file::TestFile;

    #[test]
    fn from_storage_index() {
        let test_file = TestFile::new();
        let mut storage = FileStorage::new(test_file.file_name()).unwrap();

        let index;

        {
            let mut vec = DbVec::<String>::new(&mut storage).unwrap();
            vec.push(&mut storage, &"Hello".to_string()).unwrap();
            vec.push(&mut storage, &", ".to_string()).unwrap();
            vec.push(&mut storage, &"World".to_string()).unwrap();
            vec.push(&mut storage, &"!".to_string()).unwrap();
            index = vec.storage_index();
        }

        let vec = DbVec::<String>::from_storage(&mut storage, index).unwrap();

        assert_eq!(
            vec.iter().collect::<Vec<String>>(),
            vec![
                "Hello".to_string(),
                ", ".to_string(),
                "World".to_string(),
                "!".to_string()
            ]
        );
    }

    #[test]
    fn from_storage_missing_index() {
        let test_file = TestFile::new();
        let mut storage = FileStorage::new(test_file.file_name()).unwrap();

        assert_eq!(
            DbVec::<String>::from_storage(&mut storage, StorageIndex::from(1_u64))
                .err()
                .unwrap(),
            DbError::from("FileStorage error: index (1) not found")
        );
    }

    #[test]
    fn iter() {
        let test_file = TestFile::new();
        let mut storage = FileStorage::new(test_file.file_name()).unwrap();

        let mut vec = DbVec::<String>::new(&mut storage).unwrap();
        vec.push(&mut storage, &"Hello".to_string()).unwrap();
        vec.push(&mut storage, &", ".to_string()).unwrap();
        vec.push(&mut storage, &"World".to_string()).unwrap();
        vec.push(&mut storage, &"!".to_string()).unwrap();

        assert_eq!(
            vec.iter().collect::<Vec<String>>(),
            vec!["Hello", ", ", "World", "!"]
        );
    }

    #[test]
    fn is_empty() {
        let test_file = TestFile::new();
        let mut storage = FileStorage::new(test_file.file_name()).unwrap();

        let mut vec = DbVec::<String>::new(&mut storage).unwrap();

        assert!(vec.is_empty());

        vec.push(&mut storage, &"Hello, World!".to_string())
            .unwrap();

        assert!(!vec.is_empty());
    }

    #[test]
    fn len() {
        let test_file = TestFile::new();
        let mut storage = FileStorage::new(test_file.file_name()).unwrap();

        let mut vec = DbVec::<String>::new(&mut storage).unwrap();

        assert_eq!(vec.len(), 0);

        vec.push(&mut storage, &"Hello".to_string()).unwrap();
        vec.push(&mut storage, &", ".to_string()).unwrap();
        vec.push(&mut storage, &"World".to_string()).unwrap();
        vec.push(&mut storage, &"!".to_string()).unwrap();

        assert_eq!(vec.len(), 4)
    }

    #[test]
    fn min_capacity() {
        let test_file = TestFile::new();
        let mut storage = FileStorage::new(test_file.file_name()).unwrap();

        let mut vec = DbVec::<String>::new(&mut storage).unwrap();

        assert_eq!(vec.capacity(), 0);

        vec.push(&mut storage, &"Hello".to_string()).unwrap();
        vec.push(&mut storage, &", ".to_string()).unwrap();
        vec.push(&mut storage, &"World".to_string()).unwrap();
        vec.push(&mut storage, &"!".to_string()).unwrap();

        assert_eq!(vec.capacity(), 64);
    }

    #[test]
    fn push() {
        let test_file = TestFile::new();
        let mut storage = FileStorage::new(test_file.file_name()).unwrap();

        let mut vec = DbVec::<String>::new(&mut storage).unwrap();
        vec.push(&mut storage, &"Hello".to_string()).unwrap();
        vec.push(&mut storage, &", ".to_string()).unwrap();
        vec.push(&mut storage, &"World".to_string()).unwrap();
        vec.push(&mut storage, &"!".to_string()).unwrap();

        let indexes = storage
            .value::<Vec<StorageIndex>>(vec.storage_index())
            .unwrap();

        let mut values = Vec::<String>::new();

        for index in indexes {
            values.push(storage.value::<String>(index).unwrap());
        }

        assert_eq!(
            values,
            vec![
                "Hello".to_string(),
                ", ".to_string(),
                "World".to_string(),
                "!".to_string()
            ]
        );
    }

    #[test]
    fn remove() {
        let test_file = TestFile::new();
        let mut storage = FileStorage::new(test_file.file_name()).unwrap();

        let mut vec = DbVec::<String>::new(&mut storage).unwrap();
        vec.push(&mut storage, &"Hello".to_string()).unwrap();
        vec.push(&mut storage, &", ".to_string()).unwrap();
        vec.push(&mut storage, &"World".to_string()).unwrap();
        vec.push(&mut storage, &"!".to_string()).unwrap();

        vec.remove(&mut storage, 1).unwrap();

        assert_eq!(
            vec.iter().collect::<Vec<String>>(),
            vec!["Hello".to_string(), "World".to_string(), "!".to_string()]
        );
    }

    #[test]
    fn remove_at_end() {
        let test_file = TestFile::new();
        let mut storage = FileStorage::new(test_file.file_name()).unwrap();

        let mut vec = DbVec::<String>::new(&mut storage).unwrap();
        vec.push(&mut storage, &"Hello".to_string()).unwrap();
        vec.push(&mut storage, &", ".to_string()).unwrap();
        vec.push(&mut storage, &"World".to_string()).unwrap();
        vec.push(&mut storage, &"!".to_string()).unwrap();

        vec.remove(&mut storage, 2).unwrap();

        assert_eq!(
            vec.iter().collect::<Vec<String>>(),
            vec!["Hello".to_string(), ", ".to_string(), "!".to_string(),]
        );
    }

    #[test]
    fn remove_index_out_of_bounds() {
        let test_file = TestFile::new();
        let mut storage = FileStorage::new(test_file.file_name()).unwrap();

        let mut vec = DbVec::<String>::new(&mut storage).unwrap();

        assert_eq!(
            vec.remove(&mut storage, 0),
            Err(DbError::from("Index (0) out of bounds (0)"))
        );
    }

    #[test]
    fn reserve_larger() {
        let test_file = TestFile::new();
        let mut storage = FileStorage::new(test_file.file_name()).unwrap();

        let mut vec = DbVec::<String>::new(&mut storage).unwrap();
        assert_eq!(vec.capacity(), 0);

        vec.reserve(&mut storage, 20).unwrap();

        assert_eq!(vec.capacity(), 20);
    }

    #[test]
    fn reserve_smaller() {
        let test_file = TestFile::new();
        let mut storage = FileStorage::new(test_file.file_name()).unwrap();

        let mut vec = DbVec::<String>::new(&mut storage).unwrap();
        vec.reserve(&mut storage, 20).unwrap();
        vec.reserve(&mut storage, 10).unwrap();

        assert_eq!(vec.capacity(), 20);
    }

    #[test]
    fn resize_larger() {
        let test_file = TestFile::new();
        let mut storage = FileStorage::new(test_file.file_name()).unwrap();

        let mut vec = DbVec::<String>::new(&mut storage).unwrap();
        vec.push(&mut storage, &"Hello".to_string()).unwrap();
        vec.push(&mut storage, &", ".to_string()).unwrap();
        vec.push(&mut storage, &"World".to_string()).unwrap();
        vec.push(&mut storage, &"!".to_string()).unwrap();

        vec.resize(&mut storage, 6, &" ".to_string()).unwrap();

        assert_eq!(
            vec.iter().collect::<Vec<String>>(),
            vec![
                "Hello".to_string(),
                ", ".to_string(),
                "World".to_string(),
                "!".to_string(),
                " ".to_string(),
                " ".to_string(),
            ]
        );
    }

    #[test]
    fn resize_over_capacity() {
        let test_file = TestFile::new();
        let mut storage = FileStorage::new(test_file.file_name()).unwrap();

        let mut vec = DbVec::<String>::new(&mut storage).unwrap();
        vec.push(&mut storage, &"Hello".to_string()).unwrap();
        vec.push(&mut storage, &", ".to_string()).unwrap();
        vec.push(&mut storage, &"World".to_string()).unwrap();
        vec.push(&mut storage, &"!".to_string()).unwrap();

        vec.resize(&mut storage, 100, &" ".to_string()).unwrap();

        let mut expected = Vec::<String>::new();
        expected.resize(100, " ".to_string());
        expected[0] = "Hello".to_string();
        expected[1] = ", ".to_string();
        expected[2] = "World".to_string();
        expected[3] = "!".to_string();

        assert_eq!(vec.len(), 100);
        assert_eq!(vec.capacity(), 100);

        assert_eq!(vec.iter().collect::<Vec<String>>(), expected);
    }

    #[test]
    fn resize_same() {
        let test_file = TestFile::new();
        let mut storage = FileStorage::new(test_file.file_name()).unwrap();

        let mut vec = DbVec::<String>::new(&mut storage).unwrap();
        vec.push(&mut storage, &"Hello".to_string()).unwrap();
        vec.push(&mut storage, &", ".to_string()).unwrap();
        vec.push(&mut storage, &"World".to_string()).unwrap();
        vec.push(&mut storage, &"!".to_string()).unwrap();

        vec.resize(&mut storage, 4, &String::default()).unwrap();

        assert_eq!(
            vec.iter().collect::<Vec<String>>(),
            vec![
                "Hello".to_string(),
                ", ".to_string(),
                "World".to_string(),
                "!".to_string()
            ]
        );
    }

    #[test]
    fn resize_smaller() {
        let test_file = TestFile::new();
        let mut storage = FileStorage::new(test_file.file_name()).unwrap();

        let mut vec = DbVec::<String>::new(&mut storage).unwrap();
        vec.push(&mut storage, &"Hello".to_string()).unwrap();
        vec.push(&mut storage, &", ".to_string()).unwrap();
        vec.push(&mut storage, &"World".to_string()).unwrap();
        vec.push(&mut storage, &"!".to_string()).unwrap();

        vec.resize(&mut storage, 3, &String::default()).unwrap();

        assert_eq!(
            vec.iter().collect::<Vec<String>>(),
            vec!["Hello".to_string(), ", ".to_string(), "World".to_string()]
        );
    }

    #[test]
    fn replace() {
        let test_file = TestFile::new();
        let mut storage = FileStorage::new(test_file.file_name()).unwrap();

        let mut vec = DbVec::<String>::new(&mut storage).unwrap();
        vec.push(&mut storage, &"Hello".to_string()).unwrap();
        vec.push(&mut storage, &", ".to_string()).unwrap();
        vec.push(&mut storage, &"World".to_string()).unwrap();
        vec.push(&mut storage, &"!".to_string()).unwrap();

        vec.replace(&mut storage, 1, &" ".to_string()).unwrap();

        assert_eq!(vec.value(0), Ok("Hello".to_string()));
        assert_eq!(vec.value(1), Ok(" ".to_string()));
        assert_eq!(vec.value(2), Ok("World".to_string()));
        assert_eq!(vec.value(3), Ok("!".to_string()));
    }

    #[test]
    fn replace_out_of_bounds() {
        let test_file = TestFile::new();
        let mut storage = FileStorage::new(test_file.file_name()).unwrap();

        let mut vec = DbVec::<String>::new(&mut storage).unwrap();

        assert_eq!(
            vec.replace(&mut storage, 0, &"".to_string()),
            Err(DbError::from("Index (0) out of bounds (0)"))
        );
    }

    #[test]
    fn shrink_to_fit() {
        let test_file = TestFile::new();
        let mut storage = FileStorage::new(test_file.file_name()).unwrap();

        let mut vec = DbVec::<String>::new(&mut storage).unwrap();
        vec.push(&mut storage, &"Hello".to_string()).unwrap();
        vec.push(&mut storage, &", ".to_string()).unwrap();
        vec.push(&mut storage, &"World".to_string()).unwrap();
        vec.push(&mut storage, &"!".to_string()).unwrap();

        assert_eq!(vec.capacity(), 64);

        vec.shrink_to_fit(&mut storage).unwrap();

        assert_eq!(vec.capacity(), 4);

        vec.shrink_to_fit(&mut storage).unwrap();

        assert_eq!(vec.capacity(), 4);
    }

    #[test]
    fn shrink_to_fit_empty() {
        let test_file = TestFile::new();
        let mut storage = FileStorage::new(test_file.file_name()).unwrap();

        let mut vec = DbVec::<String>::new(&mut storage).unwrap();

        assert_eq!(vec.capacity(), 0);

        vec.shrink_to_fit(&mut storage).unwrap();

        assert_eq!(vec.capacity(), 0);
    }

    #[test]
    fn swap() {
        let test_file = TestFile::new();
        let mut storage = FileStorage::new(test_file.file_name()).unwrap();
        let mut vec = DbVec::<String>::new(&mut storage).unwrap();
        vec.push(&mut storage, &"Hello".to_string()).unwrap();
        vec.push(&mut storage, &", ".to_string()).unwrap();
        vec.push(&mut storage, &"World".to_string()).unwrap();
        vec.push(&mut storage, &"!".to_string()).unwrap();
        vec.swap(&mut storage, 0, 2).unwrap();
        assert_eq!(
            vec.iter().collect::<Vec<String>>(),
            vec![
                "World".to_string(),
                ", ".to_string(),
                "Hello".to_string(),
                "!".to_string()
            ]
        );
    }
    #[test]
    fn swap_self() {
        let test_file = TestFile::new();
        let mut storage = FileStorage::new(test_file.file_name()).unwrap();
        let mut vec = DbVec::<String>::new(&mut storage).unwrap();
        vec.push(&mut storage, &"Hello".to_string()).unwrap();
        vec.push(&mut storage, &", ".to_string()).unwrap();
        vec.push(&mut storage, &"World".to_string()).unwrap();
        vec.push(&mut storage, &"!".to_string()).unwrap();
        vec.swap(&mut storage, 1, 1).unwrap();
        assert_eq!(
            vec.iter().collect::<Vec<String>>(),
            vec![
                "Hello".to_string(),
                ", ".to_string(),
                "World".to_string(),
                "!".to_string()
            ]
        );
    }
    #[test]
    fn swap_invalid() {
        let test_file = TestFile::new();
        let mut storage = FileStorage::new(test_file.file_name()).unwrap();
        let mut vec = DbVec::<String>::new(&mut storage).unwrap();
        vec.push(&mut storage, &"Hello".to_string()).unwrap();
        vec.push(&mut storage, &", ".to_string()).unwrap();
        vec.push(&mut storage, &"World".to_string()).unwrap();
        vec.push(&mut storage, &"!".to_string()).unwrap();
        assert_eq!(
            vec.swap(&mut storage, 1, 10),
            Err(DbError::from("Index (10) out of bounds (4)"))
        );
        assert_eq!(
            vec.swap(&mut storage, 10, 1),
            Err(DbError::from("Index (10) out of bounds (4)"))
        );
    }

    #[test]
    fn value() {
        let test_file = TestFile::new();
        let mut storage = FileStorage::new(test_file.file_name()).unwrap();

        let mut vec = DbVec::<String>::new(&mut storage).unwrap();
        vec.push(&mut storage, &"Hello".to_string()).unwrap();
        vec.push(&mut storage, &", ".to_string()).unwrap();
        vec.push(&mut storage, &"World".to_string()).unwrap();
        vec.push(&mut storage, &"!".to_string()).unwrap();

        assert_eq!(vec.value(0), Ok("Hello".to_string()));
        assert_eq!(vec.value(1), Ok(", ".to_string()));
        assert_eq!(vec.value(2), Ok("World".to_string()));
        assert_eq!(vec.value(3), Ok("!".to_string()));
    }

    #[test]
    fn value_out_of_bounds() {
        let test_file = TestFile::new();
        let mut storage = FileStorage::new(test_file.file_name()).unwrap();

        let vec = DbVec::<String>::new(&mut storage).unwrap();

        assert_eq!(
            vec.value(0),
            Err(DbError::from("Index (0) out of bounds (0)"))
        );
    }
}