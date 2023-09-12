use crate::collections::vec::VecValue;
use crate::storage::Storage;
use crate::utilities::serialize::Serialize;
use crate::utilities::serialize::SerializeStatic;
use crate::utilities::stable_hash::StableHash;
use crate::DbError;
use crate::StorageData;

#[derive(Clone, Debug, Eq, PartialEq)]
struct CollisionValue<T> {
    pub value: T,
}

impl<T> CollisionValue<T> {
    pub fn new(value: T) -> Self {
        CollisionValue { value }
    }
}

impl<T> StableHash for CollisionValue<T> {
    fn stable_hash(&self) -> u64 {
        1
    }
}

impl<T> Serialize for CollisionValue<T>
where
    T: Serialize,
{
    fn serialize(&self) -> Vec<u8> {
        self.value.serialize()
    }

    fn deserialize(bytes: &[u8]) -> Result<Self, DbError> {
        Ok(Self {
            value: T::deserialize(bytes)?,
        })
    }

    fn serialized_size(&self) -> u64 {
        self.value.serialized_size()
    }
}

impl<T> SerializeStatic for CollisionValue<T> where T: SerializeStatic {}

impl<T> VecValue for CollisionValue<T>
where
    T: VecValue,
{
    fn store<D: StorageData>(&self, storage: &mut Storage<D>) -> Result<Vec<u8>, DbError> {
        self.value.store(storage)
    }

    fn load<D: StorageData>(storage: &Storage<D>, bytes: &[u8]) -> Result<Self, DbError> {
        Ok(Self {
            value: T::load(storage, bytes)?,
        })
    }

    fn remove<D: StorageData>(storage: &mut Storage<D>, bytes: &[u8]) -> Result<(), DbError> {
        T::remove(storage, bytes)
    }

    fn storage_len() -> u64 {
        T::storage_len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{storage::file_storage::FileStorage, test_utilities::test_file::TestFile};

    #[test]
    #[allow(clippy::redundant_clone)]
    fn derived_from_clone() {
        let value = CollisionValue::new(1_i64);
        let other = value.clone();
        assert_eq!(value, other);
    }

    #[test]
    fn derived_from_debug() {
        let value = CollisionValue::new(1_i64);
        format!("{value:?}");
    }

    #[test]
    fn serialize() {
        let value = CollisionValue::new(1_i64);
        let bytes = value.serialize();

        assert_eq!(bytes.len() as u64, value.serialized_size());

        let other = CollisionValue::deserialize(&bytes).unwrap();

        assert_eq!(value, other);
    }

    #[test]
    fn stable_hash() {
        let value = CollisionValue::new(1_i64);

        assert_eq!(value.stable_hash(), 1_u64);
    }

    #[test]
    fn storage_value() {
        let test_file = TestFile::new();
        let mut storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();
        let value = CollisionValue::<i64>::new(1);
        let bytes = value.store(&mut storage).unwrap();
        CollisionValue::<i64>::remove(&mut storage, &bytes).unwrap();
        let other = CollisionValue::<i64>::load(&storage, &bytes).unwrap();

        assert_eq!(value, other);

        assert_eq!(
            CollisionValue::<i64>::storage_len(),
            i64::serialized_size_static()
        );
    }
}
