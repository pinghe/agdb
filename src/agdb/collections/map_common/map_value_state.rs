use crate::db::db_error::DbError;
use crate::utilities::serialize::Serialize;
use std::mem::size_of;

#[derive(Clone, Default, Debug, Eq, PartialEq)]
pub enum MapValueState {
    #[default]
    Empty,
    Deleted,
    Valid,
}

impl Serialize for MapValueState {
    fn deserialize(bytes: &[u8]) -> Result<Self, DbError> {
        match bytes.first() {
            Some(0) => Ok(MapValueState::Empty),
            Some(1) => Ok(MapValueState::Valid),
            Some(2) => Ok(MapValueState::Deleted),
            _ => Err(DbError::from("value out of bounds")),
        }
    }

    fn serialize(&self) -> Vec<u8> {
        match self {
            MapValueState::Empty => vec![0_u8],
            MapValueState::Deleted => vec![2_u8],
            MapValueState::Valid => vec![1_u8],
        }
    }

    fn serialized_size() -> u64 {
        size_of::<u8>() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bad_deserialization() {
        assert_eq!(
            MapValueState::deserialize(&[10_u8]),
            Err(DbError::from("value out of bounds"))
        );
    }
    #[test]
    fn derived_from_default() {
        assert_eq!(MapValueState::default(), MapValueState::Empty);
    }
    #[test]
    fn derived_from_debug() {
        let value = MapValueState::Deleted;
        format!("{:?}", value);
    }
    #[test]
    fn serialize() {
        let data = vec![
            MapValueState::Valid,
            MapValueState::Empty,
            MapValueState::Deleted,
        ];
        let bytes = data.serialize();
        let other = Vec::<MapValueState>::deserialize(&bytes).unwrap();
        assert_eq!(data, other);
    }
    #[test]
    fn serialized_size() {
        assert_eq!(MapValueState::serialized_size(), 1);
    }
}