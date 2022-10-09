use std::io::SeekFrom;

use crate::storage::Storage;
use crate::storage_data::StorageData;
use crate::storage_impl::StorageImpl;
use agdb_db_error::DbError;
use agdb_serialize::Serialize;
use agdb_storage_index::StorageIndex;

impl<Data: StorageData> Storage for StorageImpl<Data> {
    fn commit(&mut self) -> Result<(), DbError> {
        if self.data.end_transaction() {
            self.data.clear_wal()?;
        }

        Ok(())
    }

    fn insert<V: Serialize>(&mut self, value: &V) -> Result<StorageIndex, DbError> {
        self.transaction();
        let position = self.size()?;
        let bytes = value.serialize();
        let record = self.data.create_record(position, bytes.len() as u64);

        self.append(&record.serialize())?;
        self.append(&bytes)?;
        self.commit()?;

        Ok(record.index)
    }

    fn insert_at<V: Serialize>(
        &mut self,
        index: &StorageIndex,
        offset: u64,
        value: &V,
    ) -> Result<(), DbError> {
        self.transaction();
        let mut record = self.data.record(index)?;
        let bytes = V::serialize(value);
        self.ensure_record_size(&mut record, index, offset, bytes.len() as u64)?;
        self.write(Self::value_position(record.position, offset), &bytes)?;
        self.commit()
    }

    fn move_at(
        &mut self,
        index: &StorageIndex,
        offset_from: u64,
        offset_to: u64,
        size: u64,
    ) -> Result<(), DbError> {
        if offset_from == offset_to || size == 0 {
            return Ok(());
        }

        let mut record = self.data.record(index)?;
        Self::validate_move_size(offset_from, size, record.size)?;
        self.transaction();
        self.ensure_record_size(&mut record, index, offset_to, size)?;
        self.move_bytes(
            Self::value_position_u64(record.position, offset_from),
            Self::value_position_u64(record.position, offset_to),
            size,
        )?;
        self.commit()?;

        Ok(())
    }

    fn remove(&mut self, index: &StorageIndex) -> Result<(), DbError> {
        self.transaction();
        let position = self.data.record(index)?.position;
        self.invalidate_record(position)?;
        self.data.remove_index(index);
        self.commit()
    }

    fn resize_value(&mut self, index: &StorageIndex, new_size: u64) -> Result<(), DbError> {
        if new_size == 0 {
            return Err(DbError::from("value size cannot be 0"));
        }

        let mut record = self.data.record(index)?;

        if record.size != new_size {
            self.transaction();
            self.resize_record(index, new_size, new_size, &mut record)?;
            self.commit()?;
        }

        Ok(())
    }

    fn shrink_to_fit(&mut self) -> Result<(), DbError> {
        self.transaction();
        let indexes = self.data.indexes_by_position();
        let size = self.shrink_indexes(indexes)?;
        self.truncate(size)?;
        self.commit()
    }

    fn size(&mut self) -> Result<u64, DbError> {
        self.data.seek(SeekFrom::End(0))
    }

    fn transaction(&mut self) {
        self.data.begin_transaction();
    }

    fn value<V: Serialize>(&mut self, index: &StorageIndex) -> Result<V, DbError> {
        let record = self.data.record(index)?;
        V::deserialize(&self.read(Self::value_position(record.position, 0), record.size)?)
    }

    fn value_at<V: Serialize>(&mut self, index: &StorageIndex, offset: u64) -> Result<V, DbError> {
        let record = self.data.record(index)?;
        let bytes = self.read(
            Self::value_position(record.position, offset),
            Self::value_read_size::<V>(record.size, offset)?,
        );

        V::deserialize(&bytes?)
    }

    fn value_size(&self, index: &StorageIndex) -> Result<u64, DbError> {
        Ok(self.data.record(index)?.size)
    }
}