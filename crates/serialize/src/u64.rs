use crate::Serialize;
use db_error::DbError;

impl Serialize for u64 {
    fn deserialize(bytes: &[u8]) -> Result<Self, DbError> {
        let buffer: [u8; std::mem::size_of::<Self>()] = bytes
            .get(0..std::mem::size_of::<Self>())
            .ok_or_else(|| DbError::from("u64 deserialization error: out of bounds"))?
            .try_into()
            .unwrap();
        Ok(Self::from_le_bytes(buffer))
    }

    fn serialize(&self) -> Vec<u8> {
        self.to_le_bytes().into()
    }
}