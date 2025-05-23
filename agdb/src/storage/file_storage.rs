use super::StorageData;
use super::StorageSlice;
use super::write_ahead_log::WriteAheadLog;
use super::write_ahead_log::WriteAheadLogRecord;
use crate::DbError;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::sync::Mutex;

/// Single file based storage with write ahead log (WAL) for resiliency
/// implementing [`StorageData`]. It uses the storage name as the file
/// name (`.{name}` for WAL). It allows multiple readers from the file
/// by opening additional temporary file handles if the single member file
/// handle is being used (read). The [`StorageData::read()`] always returns
/// owning buffer.
///
/// The [`StorageData::backup()`] is implemented so that it copes the file
/// to the new name.
///
/// The [`StorageData::flush()`] merely clears the WAL. This implementation
/// relies on the OS to flush the content to the disk. It is specifically not
/// using manual calls to `File::sync_data()` / `File::sync_all()` because they
/// result in extreme slowdown.
#[derive(Debug)]
pub struct FileStorage {
    file: File,
    filename: String,
    len: u64,
    lock: Mutex<()>,
    wal: WriteAheadLog,
}

impl FileStorage {
    fn apply_wal_record(file: &mut File, record: WriteAheadLogRecord) -> Result<(), DbError> {
        if record.value.is_empty() {
            file.set_len(record.pos)?;
        } else {
            file.seek(SeekFrom::Start(record.pos))?;
            file.write_all(&record.value)?;
        }

        Ok(())
    }

    fn apply_wal(file: &mut File, wal: &mut WriteAheadLog) -> Result<(), DbError> {
        for record in wal.records()? {
            Self::apply_wal_record(file, record)?;
        }

        wal.clear()
    }

    fn open_file(&self) -> Result<File, DbError> {
        Ok(File::open(&self.filename)?)
    }

    fn read_impl(mut file: &File, pos: u64, buffer: &mut [u8]) -> Result<(), DbError> {
        file.seek(SeekFrom::Start(pos))?;
        file.read_exact(buffer)?;
        Ok(())
    }
}

impl StorageData for FileStorage {
    fn backup(&self, name: &str) -> Result<(), DbError> {
        std::fs::copy(&self.filename, name)?;
        Ok(())
    }

    fn copy(&self, name: &str) -> Result<Self, DbError> {
        self.backup(name)?;
        Self::new(name)
    }

    fn flush(&mut self) -> Result<(), DbError> {
        self.wal.clear()
    }

    fn len(&self) -> u64 {
        self.len
    }

    fn name(&self) -> &str {
        &self.filename
    }

    fn new(name: &str) -> Result<Self, DbError> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(false)
            .create(true)
            .open(name)?;
        let mut wal: WriteAheadLog = WriteAheadLog::new(name)?;

        Self::apply_wal(&mut file, &mut wal)?;

        let len = file.seek(SeekFrom::End(0))?;

        Ok(Self {
            file,
            filename: name.to_string(),
            len,
            lock: Mutex::new(()),
            wal,
        })
    }

    fn read(&self, pos: u64, value_len: u64) -> Result<StorageSlice, DbError> {
        let mut buffer = vec![0_u8; value_len as usize];

        if let Ok(_guard) = self.lock.try_lock() {
            Self::read_impl(&self.file, pos, &mut buffer)?;
        } else {
            Self::read_impl(&self.open_file()?, pos, &mut buffer)?;
        }

        Ok(StorageSlice::Owned(buffer))
    }

    fn rename(&mut self, new_name: &str) -> Result<(), DbError> {
        std::fs::rename(&self.filename, new_name)?;
        self.file = OpenOptions::new().read(true).write(true).open(new_name)?;
        self.wal = WriteAheadLog::new(new_name)?;
        std::fs::remove_file(WriteAheadLog::wal_filename(&self.filename))?;
        self.filename = new_name.to_string();
        Ok(())
    }

    fn resize(&mut self, new_len: u64) -> Result<(), DbError> {
        let current_len = self.len();

        if new_len < current_len {
            let mut buffer = vec![0_u8; (current_len - new_len) as usize];
            Self::read_impl(&self.file, new_len, &mut buffer)?;
            self.wal.insert(new_len, &buffer)?;
        } else {
            self.wal.insert(new_len, &[])?;
        }

        self.file.set_len(new_len)?;
        self.len = new_len;
        Ok(())
    }

    fn write(&mut self, pos: u64, bytes: &[u8]) -> Result<(), DbError> {
        let current_len = self.len();
        let end = pos + bytes.len() as u64;
        let mut buffer = vec![0_u8; (std::cmp::min(current_len, end) - pos) as usize];
        Self::read_impl(&self.file, pos, &mut buffer)?;
        self.wal.insert(pos, &buffer)?;
        self.file.seek(SeekFrom::Start(pos))?;
        self.file.write_all(bytes)?;
        self.len = std::cmp::max(current_len, end);
        Ok(())
    }
}

impl Drop for FileStorage {
    fn drop(&mut self) {
        if Self::apply_wal(&mut self.file, &mut self.wal).is_ok() {
            let _ = self.flush();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Storage;
    use crate::storage::StorageIndex;
    use crate::test_utilities::test_file::TestFile;
    use crate::utilities::serialize::Serialize;
    use crate::utilities::serialize::SerializeStatic;

    #[test]
    fn bad_file() {
        assert!(Storage::<FileStorage>::new("/a/").is_err());
    }

    #[test]
    fn index_reuse() {
        let test_file = TestFile::new();
        let mut storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();

        let _index1 = storage.insert(&"Hello, World!".to_string()).unwrap();
        let index2 = storage.insert(&10_i64).unwrap();
        let _index3 = storage.insert(&vec![1_u64, 2_u64, 3_u64]).unwrap();

        storage.remove(index2).unwrap();

        let index4 = storage
            .insert(&vec!["Hello".to_string(), "World".to_string()])
            .unwrap();

        assert_eq!(index2, index4);
    }

    #[test]
    fn index_reuse_after_restore() {
        let test_file = TestFile::new();

        let index2;

        {
            let mut storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();

            let _index1 = storage.insert(&"Hello, World!".to_string()).unwrap();
            index2 = storage.insert(&10_i64).unwrap();
            let _index3 = storage.insert(&vec![1_u64, 2_u64, 3_u64]).unwrap();

            storage.remove(index2).unwrap();
        }

        let mut storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();

        let index4 = storage
            .insert(&vec!["Hello".to_string(), "World".to_string()])
            .unwrap();

        assert_eq!(index2, index4);
    }

    #[test]
    fn index_reuse_chain_after_restore() {
        let test_file = TestFile::new();

        let index1;
        let index2;

        {
            let mut storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();

            index1 = storage.insert(&"Hello, World!".to_string()).unwrap();
            index2 = storage.insert(&10_i64).unwrap();
            let _index3 = storage.insert(&vec![1_u64, 2_u64, 3_u64]).unwrap();

            storage.remove(index1).unwrap();
            storage.remove(index2).unwrap();
        }

        let mut storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();

        let index4 = storage
            .insert(&vec!["Hello".to_string(), "World".to_string()])
            .unwrap();
        let index5 = storage.insert(&1_u64).unwrap();
        let index6 = storage.insert(&vec![0_u8; 0]).unwrap();

        assert_eq!(index2, index4);
        assert_eq!(index1, index5);
        assert_eq!(index6.0, 4);
    }

    #[test]
    fn insert() {
        let test_file = TestFile::new();
        let mut storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();

        let value1 = "Hello, World!".to_string();
        let index1 = storage.insert(&value1).unwrap();
        assert_eq!(storage.value_size(index1), Ok(value1.serialized_size()));
        assert_eq!(storage.value_size(index1), Ok(value1.serialized_size()));
        assert_eq!(storage.value(index1), Ok(value1));

        let value2 = 10_i64;
        let index2 = storage.insert(&value2).unwrap();
        assert_eq!(
            storage.value_size(index2),
            Ok(i64::serialized_size_static())
        );
        assert_eq!(
            storage.value_size(index2),
            Ok(i64::serialized_size_static())
        );
        assert_eq!(storage.value(index2), Ok(value2));

        let value3 = vec![1_u64, 2_u64, 3_u64];
        let index3 = storage.insert(&value3).unwrap();
        assert_eq!(storage.value_size(index3), Ok(value3.serialized_size()));
        assert_eq!(storage.value_size(index3), Ok(value3.serialized_size()));
        assert_eq!(storage.value(index3), Ok(value3));

        let value4 = vec!["Hello".to_string(), "World".to_string()];
        let index4 = storage.insert(&value4).unwrap();
        assert_eq!(storage.value_size(index4), Ok(value4.serialized_size()));
        assert_eq!(storage.value_size(index4), Ok(value4.serialized_size()));
        assert_eq!(storage.value(index4), Ok(value4));
    }

    #[test]
    fn insert_at() {
        let test_file = TestFile::new();
        let mut storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();

        let index = storage.insert(&vec![1_i64, 2_i64, 3_i64]).unwrap();
        let offset = u64::serialized_size_static() + i64::serialized_size_static();

        storage.insert_at(index, offset, &10_i64).unwrap();

        assert_eq!(
            storage.value::<Vec<i64>>(index).unwrap(),
            vec![1_i64, 10_i64, 3_i64]
        );
    }

    #[test]
    fn insert_at_value_end() {
        let test_file = TestFile::new();
        let mut storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();

        let index = storage.insert(&vec![1_i64, 2_i64, 3_i64]).unwrap();
        let offset = u64::serialized_size_static() + i64::serialized_size_static() * 3;

        storage.insert_at(index, 0, &4_u64).unwrap();
        storage.insert_at(index, offset, &10_i64).unwrap();

        assert_eq!(
            storage.value::<Vec<i64>>(index).unwrap(),
            vec![1_i64, 2_i64, 3_i64, 10_i64]
        );
    }

    #[test]
    fn insert_at_value_end_multiple_values() {
        let test_file = TestFile::new();
        let mut storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();

        let index = storage.insert(&vec![1_i64, 2_i64, 3_i64]).unwrap();
        storage.insert(&"Hello, World!".to_string()).unwrap();
        let offset = u64::serialized_size_static() + i64::serialized_size_static() * 3;

        storage.insert_at(index, 0, &4_u64).unwrap();
        storage.insert_at(index, offset, &10_i64).unwrap();

        assert_eq!(
            storage.value::<Vec<i64>>(index).unwrap(),
            vec![1_i64, 2_i64, 3_i64, 10_i64]
        );
    }

    #[test]
    fn insert_at_beyond_end() {
        let test_file = TestFile::new();
        let mut storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();

        let index = storage.insert(&vec![1_i64, 2_i64, 3_i64]).unwrap();
        let offset = u64::serialized_size_static() + i64::serialized_size_static() * 4;

        storage.insert_at(index, 0, &5_u64).unwrap();
        storage.insert_at(index, offset, &10_i64).unwrap();

        assert_eq!(
            storage.value::<Vec<i64>>(index).unwrap(),
            vec![1_i64, 2_i64, 3_i64, 0_i64, 10_i64]
        );
    }

    #[test]
    fn insert_at_beyond_end_multiple_values() {
        let test_file = TestFile::new();
        let mut storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();

        let index = storage.insert(&vec![1_i64, 2_i64, 3_i64]).unwrap();
        storage.insert(&"Hello, World!".to_string()).unwrap();
        let offset = u64::serialized_size_static() + i64::serialized_size_static() * 4;

        storage.insert_at(index, 0, &5_u64).unwrap();
        storage.insert_at(index, offset, &10_i64).unwrap();

        assert_eq!(
            storage.value::<Vec<i64>>(index).unwrap(),
            vec![1_i64, 2_i64, 3_i64, 0_i64, 10_i64]
        );
    }

    #[test]
    fn insert_at_missing_index() {
        let test_file = TestFile::new();
        let mut storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();

        assert_eq!(
            storage.insert_at(StorageIndex::from(1_u64), 8, &1_i64),
            Err(DbError::from("Storage error: index (1) not found"))
        );
    }

    #[test]
    fn move_at_left() {
        let test_file = TestFile::new();
        let mut storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();

        let index = storage.insert(&vec![1_i64, 2_i64, 3_i64]).unwrap();
        let offset_from = u64::serialized_size_static() + i64::serialized_size_static() * 2;
        let offset_to = u64::serialized_size_static() + i64::serialized_size_static();
        let size = i64::serialized_size_static();

        storage
            .move_at(index, offset_from, offset_to, size)
            .unwrap();

        assert_eq!(
            storage.value::<Vec<i64>>(index).unwrap(),
            vec![1_i64, 3_i64, 0_i64]
        )
    }

    #[test]
    fn move_at_left_overlapping() {
        let test_file = TestFile::new();
        let mut storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();

        let index = storage.insert(&vec![1_i64, 2_i64, 3_i64]).unwrap();
        let offset_from = u64::serialized_size_static() + i64::serialized_size_static();
        let offset_to = u64::serialized_size_static();
        let size = u64::serialized_size_static() * 2;

        storage
            .move_at(index, offset_from, offset_to, size)
            .unwrap();

        assert_eq!(
            storage.value::<Vec<i64>>(index).unwrap(),
            vec![2_i64, 3_i64, 0_i64]
        )
    }

    #[test]
    fn move_at_right() {
        let test_file = TestFile::new();
        let mut storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();

        let index = storage.insert(&vec![1_i64, 2_i64, 3_i64]).unwrap();
        let offset_from = u64::serialized_size_static() + i64::serialized_size_static();
        let offset_to = u64::serialized_size_static() + i64::serialized_size_static() * 2;
        let size = u64::serialized_size_static();

        storage
            .move_at(index, offset_from, offset_to, size)
            .unwrap();

        assert_eq!(
            storage.value::<Vec<i64>>(index).unwrap(),
            vec![1_i64, 0_i64, 2_i64]
        )
    }

    #[test]
    fn move_at_right_overlapping() {
        let test_file = TestFile::new();
        let mut storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();

        let index = storage.insert(&vec![1_i64, 2_i64, 3_i64]).unwrap();
        let offset_from = u64::serialized_size_static();
        let offset_to = u64::serialized_size_static() + i64::serialized_size_static();
        let size = u64::serialized_size_static() * 2;

        storage
            .move_at(index, offset_from, offset_to, size)
            .unwrap();

        assert_eq!(
            storage.value::<Vec<i64>>(index).unwrap(),
            vec![0_i64, 1_i64, 2_i64]
        )
    }

    #[test]
    fn move_at_beyond_end() {
        let test_file = TestFile::new();
        let mut storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();

        let index = storage.insert(&vec![1_i64, 2_i64, 3_i64]).unwrap();
        let offset_from = u64::serialized_size_static() + i64::serialized_size_static();
        let offset_to = u64::serialized_size_static() + i64::serialized_size_static() * 4;
        let size = u64::serialized_size_static();

        storage
            .move_at(index, offset_from, offset_to, size)
            .unwrap();

        storage.insert_at(index, 0, &5_u64).unwrap();

        assert_eq!(
            storage.value::<Vec<i64>>(index).unwrap(),
            vec![1_i64, 0_i64, 3_i64, 0_i64, 2_i64]
        )
    }

    #[test]
    fn move_at_size_out_of_bounds() {
        let test_file = TestFile::new();
        let mut storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();

        let index = storage.insert(&vec![1_i64, 2_i64, 3_i64]).unwrap();

        assert_eq!(
            storage.move_at(index, 8, 16, 1000),
            Err(DbError::from(
                "Storage error: value size (1008) out of bounds (32)"
            ))
        )
    }

    #[test]
    fn move_at_same_offset() {
        let test_file = TestFile::new();
        let mut storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();

        let index = storage.insert(&vec![1_i64, 2_i64, 3_i64]).unwrap();
        let offset_from = u64::serialized_size_static();
        let offset_to = u64::serialized_size_static();
        let size = u64::serialized_size_static();

        storage
            .move_at(index, offset_from, offset_to, size)
            .unwrap();

        assert_eq!(
            storage.value::<Vec<i64>>(index).unwrap(),
            vec![1_i64, 2_i64, 3_i64]
        )
    }

    #[test]
    fn move_at_zero_size() {
        let test_file = TestFile::new();
        let mut storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();

        let value = vec![1_i64, 2_i64, 3_i64];
        let index = storage.insert(&value).unwrap();

        storage.move_at(index, 0, 1, 0).unwrap();

        assert_eq!(
            storage.value::<Vec<i64>>(index).unwrap(),
            vec![1_i64, 2_i64, 3_i64]
        );
    }

    #[test]
    fn remove() {
        let test_file = TestFile::new();
        let mut storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();

        let index = storage.insert(&1_i64).unwrap();

        assert_eq!(storage.value::<i64>(index).unwrap(), 1_i64);

        storage.remove(index).unwrap();

        assert_eq!(
            storage.value::<i64>(index),
            Err(DbError::from("Storage error: index (1) not found"))
        );
    }

    #[test]
    fn remove_missing_index() {
        let test_file = TestFile::new();
        let mut storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();

        assert_eq!(
            storage.remove(StorageIndex::from(1_u64)),
            Err(DbError::from("Storage error: index (1) not found"))
        );
    }

    #[test]
    fn replace_larger() {
        let test_file = TestFile::new();
        let mut storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();

        let index = storage.insert(&1_i64).unwrap();
        let value = "Hello, World!".to_string();
        let expected_size = value.serialized_size();

        storage.replace(index, &value).unwrap();

        assert_eq!(storage.value_size(index).unwrap(), expected_size);
    }

    #[test]
    fn replace_missing_index() {
        let test_file = TestFile::new();
        let mut storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();

        assert_eq!(
            storage.replace(StorageIndex::from(1_u64), &10_i64),
            Err(DbError::from("Storage error: index (1) not found"))
        );
    }

    #[test]
    fn replace_same_size() {
        let test_file = TestFile::new();
        let mut storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();

        let index = storage.insert(&1_i64).unwrap();
        let size = storage.value_size(index).unwrap();

        storage.replace(index, &10_i64).unwrap();

        assert_eq!(storage.value_size(index).unwrap(), size);
    }

    #[test]
    fn replace_smaller() {
        let test_file = TestFile::new();
        let mut storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();

        let index = storage.insert(&"Hello, World!".to_string()).unwrap();
        let value = 1_i64;
        let expected_size = i64::serialized_size_static();

        storage.replace(index, &value).unwrap();

        assert_eq!(storage.value_size(index).unwrap(), expected_size);
    }

    #[test]
    fn resize_at_end_does_not_move() {
        let test_file = TestFile::new();
        let mut storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();

        let index = storage.insert(&1_i64).unwrap();
        let size = storage.len();
        let value_size = storage.value_size(index).unwrap();

        storage.resize_value(index, value_size + 8).unwrap();

        assert_eq!(storage.len(), size + 8);
    }

    #[test]
    fn resize_value_greater() {
        let test_file = TestFile::new();
        let mut storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();

        let index = storage.insert(&10_i64).unwrap();
        let expected_size = i64::serialized_size_static();

        assert_eq!(storage.value_size(index), Ok(expected_size));

        storage.resize_value(index, expected_size * 2).unwrap();

        assert_eq!(storage.value_size(index), Ok(expected_size * 2));
    }

    #[test]
    fn resize_value_missing_index() {
        let test_file = TestFile::new();
        let mut storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();

        assert_eq!(
            storage.resize_value(StorageIndex::from(1_u64), 1),
            Err(DbError::from("Storage error: index (1) not found"))
        );
    }

    #[test]
    fn resize_value_same() {
        let test_file = TestFile::new();
        let mut storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();

        let index = storage.insert(&10_i64).unwrap();
        let expected_size = i64::serialized_size_static();

        assert_eq!(storage.value_size(index), Ok(expected_size));

        storage.resize_value(index, expected_size).unwrap();

        assert_eq!(storage.value_size(index), Ok(expected_size));
    }

    #[test]
    fn resize_value_smaller() {
        let test_file = TestFile::new();
        let mut storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();

        let index = storage.insert(&10_i64).unwrap();
        let expected_size = i64::serialized_size_static();

        assert_eq!(storage.value_size(index), Ok(expected_size));

        storage.resize_value(index, expected_size / 2).unwrap();

        assert_eq!(storage.value_size(index), Ok(expected_size / 2));
    }

    #[test]
    fn resize_value_zero() {
        let test_file = TestFile::new();
        let mut storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();

        let index = storage.insert(&10_i64).unwrap();
        let expected_size = i64::serialized_size_static();

        assert_eq!(storage.value_size(index), Ok(expected_size));

        storage.resize_value(index, 0).unwrap();

        assert_eq!(storage.value_size(index), Ok(0));
    }

    #[test]
    fn resize_value_resizes_file() {
        let test_file = TestFile::new();
        let mut storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();

        let index = storage.insert(&3_i64).unwrap();
        let len = storage.len();
        let size = u64::serialized_size_static() + i64::serialized_size_static() * 3;
        let expected_len = len + i64::serialized_size_static() * 3;

        storage.resize_value(index, size).unwrap();

        assert_eq!(storage.value::<Vec<i64>>(index).unwrap(), vec![0_i64; 3]);
        assert_eq!(storage.len(), expected_len);
    }

    #[test]
    fn resize_value_invalidates_original_position() {
        let test_file = TestFile::new();

        let index;

        {
            let mut storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();
            index = storage.insert(&10_i64).unwrap();
            storage.insert(&5_i64).unwrap();
            storage.resize_value(index, 1).unwrap();
            storage.remove(index).unwrap();
        }

        let storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();

        assert_eq!(
            storage.value::<i64>(index),
            Err(DbError::from("Storage error: index (1) not found"))
        );
    }

    #[test]
    fn restore_from_file() {
        let test_file = TestFile::new();
        let value1 = vec![1_i64, 2_i64, 3_i64];
        let value2 = 64_u64;
        let value3 = vec![4_i64, 5_i64, 6_i64, 7_i64, 8_i64, 9_i64, 10_i64];
        let index1;
        let index2;
        let index3;

        {
            let mut storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();
            index1 = storage.insert(&value1).unwrap();
            index2 = storage.insert(&value2).unwrap();
            index3 = storage.insert(&value3).unwrap();
        }

        let storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();

        assert_eq!(storage.value::<Vec<i64>>(index1), Ok(value1));
        assert_eq!(storage.value::<u64>(index2), Ok(value2));
        assert_eq!(storage.value::<Vec<i64>>(index3), Ok(value3));
    }

    #[test]
    fn restore_from_file_with_removed_index() {
        let test_file = TestFile::new();
        let value1 = vec![1_i64, 2_i64, 3_i64];
        let value2 = 64_u64;
        let value3 = vec![4_i64, 5_i64, 6_i64, 7_i64, 8_i64, 9_i64, 10_i64];
        let index1;
        let index2;
        let index3;

        {
            let mut storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();
            index1 = storage.insert(&value1).unwrap();
            index2 = storage.insert(&value2).unwrap();
            index3 = storage.insert(&value3).unwrap();
            storage.remove(index2).unwrap();
        }

        let storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();

        assert_eq!(storage.value::<Vec<i64>>(index1), Ok(value1));
        assert_eq!(
            storage.value::<u64>(StorageIndex::default()),
            Err(DbError::from("Storage error: index (0) not found"))
        );
        assert_eq!(
            storage.value::<u64>(index2),
            Err(DbError::from(format!(
                "Storage error: index ({}) not found",
                index2.0
            )))
        );
        assert_eq!(storage.value::<Vec<i64>>(index3), Ok(value3));
    }

    #[test]
    fn restore_from_file_with_all_indexes_removed() {
        let test_file = TestFile::new();
        let value1 = vec![1_i64, 2_i64, 3_i64];
        let value2 = 64_u64;
        let value3 = vec![4_i64, 5_i64, 6_i64, 7_i64, 8_i64, 9_i64, 10_i64];
        let index1;
        let index2;
        let index3;

        {
            let mut storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();
            index1 = storage.insert(&value1).unwrap();
            index2 = storage.insert(&value2).unwrap();
            index3 = storage.insert(&value3).unwrap();
            storage.remove(index1).unwrap();
            storage.remove(index2).unwrap();
            storage.remove(index3).unwrap();
        }

        let storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();

        assert_eq!(
            storage.value::<u64>(StorageIndex::default()),
            Err(DbError::from("Storage error: index (0) not found"))
        );
        assert_eq!(
            storage.value::<Vec<i64>>(index1),
            Err(DbError::from(format!(
                "Storage error: index ({}) not found",
                index1.0
            )))
        );
        assert_eq!(
            storage.value::<u64>(index2),
            Err(DbError::from(format!(
                "Storage error: index ({}) not found",
                index2.0
            )))
        );
        assert_eq!(
            storage.value::<Vec<i64>>(index3),
            Err(DbError::from(format!(
                "Storage error: index ({}) not found",
                index3.0
            )))
        );
    }

    #[test]
    fn restore_from_file_with_wal() {
        let test_file = TestFile::new();
        let value1 = vec![1_i64, 2_i64, 3_i64];
        let value2 = 64_u64;
        let value3 = vec![4_i64, 5_i64, 6_i64, 7_i64, 8_i64, 9_i64, 10_i64];
        let index1;
        let index2;
        let index3;

        {
            let mut storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();
            index1 = storage.insert(&value1).unwrap();
            index2 = storage.insert(&value2).unwrap();
            index3 = storage.insert(&value3).unwrap();
        }

        let mut wal = WriteAheadLog::new(test_file.file_name()).unwrap();
        wal.insert(u64::serialized_size_static() * 5, &2_u64.serialize())
            .unwrap();

        let storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();

        assert_eq!(storage.value::<Vec<i64>>(index1), Ok(vec![1_i64, 2_i64]));
        assert_eq!(storage.value::<u64>(index2), Ok(value2));
        assert_eq!(storage.value::<Vec<i64>>(index3), Ok(value3));
    }

    #[test]
    fn shrink_to_fit() {
        let test_file = TestFile::new();
        let mut storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();

        let index1 = storage.insert(&1_i64).unwrap();
        let index2 = storage.insert(&2_i64).unwrap();
        let index3 = storage.insert(&3_i64).unwrap();
        storage.remove(index2).unwrap();
        storage.shrink_to_fit().unwrap();

        let actual_size = std::fs::metadata(test_file.file_name()).unwrap().len();
        let expected_size =
            (u64::serialized_size_static() * 2) * 3 + i64::serialized_size_static() * 3;

        assert_eq!(actual_size, expected_size);
        assert_eq!(storage.value(index1), Ok(1_i64));
        assert_eq!(storage.value(index3), Ok(3_i64));
    }

    #[test]
    fn shrink_to_fit_no_change() {
        let test_file = TestFile::new();
        let mut storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();

        let index1 = storage.insert(&1_i64).unwrap();
        let index2 = storage.insert(&2_i64).unwrap();
        let index3 = storage.insert(&3_i64).unwrap();

        let actual_size = std::fs::metadata(test_file.file_name()).unwrap().len();

        storage.shrink_to_fit().unwrap();

        assert_eq!(
            actual_size,
            std::fs::metadata(test_file.file_name()).unwrap().len()
        );
        assert_eq!(storage.value(index1), Ok(1_i64));
        assert_eq!(storage.value(index2), Ok(2_i64));
        assert_eq!(storage.value(index3), Ok(3_i64));
    }

    #[test]
    fn shrink_to_fit_uncommitted() {
        let test_file = TestFile::new();

        let expected_size;
        let index1;
        let index2;
        let index3;

        {
            let mut storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();
            index1 = storage.insert(&1_i64).unwrap();
            index2 = storage.insert(&2_i64).unwrap();
            index3 = storage.insert(&3_i64).unwrap();
            storage.remove(index2).unwrap();

            expected_size = std::fs::metadata(test_file.file_name()).unwrap().len();

            storage.transaction();
            storage.shrink_to_fit().unwrap();
        }

        let actual_size = std::fs::metadata(test_file.file_name()).unwrap().len();
        assert_eq!(actual_size, expected_size);

        let storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();
        assert_eq!(storage.value(index1), Ok(1_i64));
        assert_eq!(
            storage.value::<i64>(index2),
            Err(DbError::from(format!(
                "Storage error: index ({}) not found",
                index2.0
            )))
        );
        assert_eq!(storage.value(index3), Ok(3_i64));
    }

    #[test]
    fn transaction_commit() {
        let test_file = TestFile::new();
        let index;

        {
            let mut storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();
            let id = storage.transaction();
            index = storage.insert(&1_i64).unwrap();
            storage.commit(id).unwrap();
            assert_eq!(storage.value::<i64>(index), Ok(1_i64));
        }

        let storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();
        assert_eq!(storage.value::<i64>(index), Ok(1_i64));
    }

    #[test]
    fn transaction_commit_no_transaction() {
        let test_file = TestFile::new();
        let mut storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();
        assert_eq!(storage.commit(0), Ok(()));
    }

    #[test]
    fn transaction_unfinished() {
        let test_file = TestFile::new();
        let index;

        {
            let mut storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();
            storage.transaction();
            index = storage.insert(&1_i64).unwrap();
            assert_eq!(storage.value::<i64>(index), Ok(1_i64));
        }

        let storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();
        assert_eq!(
            storage.value::<i64>(index),
            Err(DbError::from(format!(
                "Storage error: index ({}) not found",
                index.0
            )))
        );
    }

    #[test]
    fn transaction_nested_unfinished() {
        let test_file = TestFile::new();
        let index;

        {
            let mut storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();
            let _ = storage.transaction();
            let id2 = storage.transaction();
            index = storage.insert(&1_i64).unwrap();
            assert_eq!(storage.value::<i64>(index), Ok(1_i64));
            storage.commit(id2).unwrap();
        }

        let storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();
        assert_eq!(
            storage.value::<i64>(index),
            Err(DbError::from(format!(
                "Storage error: index ({}) not found",
                index.0
            )))
        );
    }

    #[test]
    fn transaction_commit_mismatch() {
        let test_file = TestFile::new();
        let mut storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();
        let id1 = storage.transaction();
        let id2 = storage.transaction();
        let index = storage.insert(&1_i64).unwrap();
        assert_eq!(storage.value::<i64>(index), Ok(1_i64));

        assert_eq!(
            storage.commit(id1),
            Err(DbError::from(format!(
                "Cannot end transaction '{id1}'. Transaction '{id2}' in progress."
            )))
        );
    }

    #[test]
    fn value() {
        let test_file = TestFile::new();
        let mut storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();
        let index = storage.insert(&10_i64).unwrap();

        assert_eq!(storage.value::<i64>(index), Ok(10_i64));
    }

    #[test]
    fn value_at() {
        let test_file = TestFile::new();

        let mut storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();
        let data = vec![1_i64, 2_i64, 3_i64];

        let index = storage.insert(&data).unwrap();
        let offset = u64::serialized_size_static() + i64::serialized_size_static();

        assert_eq!(storage.value_at::<i64>(index, offset), Ok(2_i64));
    }

    #[test]
    fn value_at_dynamic_size() {
        let test_file = TestFile::new();

        let mut storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();
        let data = vec![2_i64, 1_i64, 2_i64];

        let index = storage.insert(&data).unwrap();
        let offset = u64::serialized_size_static();

        assert_eq!(
            storage.value_at::<Vec<i64>>(index, offset),
            Ok(vec![1_i64, 2_i64])
        );
    }

    #[test]
    fn value_at_of_missing_index() {
        let test_file = TestFile::new();
        let storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();

        assert_eq!(
            storage.value_at::<i64>(StorageIndex::from(1_u64), 8),
            Err(DbError::from("Storage error: index (1) not found"))
        );
    }

    #[test]
    fn value_at_out_of_bounds() {
        let test_file = TestFile::new();
        let mut storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();

        let data = vec![1_i64, 2_i64];
        let index = storage.insert(&data).unwrap();
        let offset = u64::serialized_size_static() + i64::serialized_size_static() * 2;

        assert_eq!(
            storage.value_at::<i64>(index, offset),
            Err(DbError::from("i64 deserialization error: out of bounds"))
        );
    }

    #[test]
    fn value_at_offset_overflow() {
        let test_file = TestFile::new();
        let mut storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();

        let data = vec![1_i64, 2_i64];
        let index = storage.insert(&data).unwrap();
        let offset = u64::serialized_size_static() + i64::serialized_size_static() * 3;

        assert_eq!(
            storage.value_at::<i64>(index, offset),
            Err(DbError::from(
                "Storage error: offset (32) out of bounds (24)"
            ))
        );
    }

    #[test]
    fn value_of_missing_index() {
        let test_file = TestFile::new();
        let storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();

        assert_eq!(
            storage.value::<i64>(StorageIndex::from(1_u64)),
            Err(DbError::from("Storage error: index (1) not found"))
        );
    }

    #[test]
    fn value_out_of_bounds() {
        let test_file = TestFile::new();
        let mut storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();

        let index = storage.insert(&10_i64).unwrap();

        assert_eq!(
            storage.value::<Vec<i64>>(index),
            Err(DbError::from(
                "Vec<i64> deserialization error: out of bounds"
            ))
        );
    }

    #[test]
    fn value_size() {
        let test_file = TestFile::new();
        let mut storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();

        let index = storage.insert(&10_i64).unwrap();
        let expected_size = i64::serialized_size_static();

        assert_eq!(storage.value_size(index), Ok(expected_size));
    }

    #[test]
    fn value_size_of_missing_index() {
        let test_file = TestFile::new();
        let storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();

        assert_eq!(
            storage.value_size(StorageIndex::from(1_u64)),
            Err(DbError::from("Storage error: index (1) not found"))
        );
    }

    #[test]
    fn file_lock() {
        let test_file = TestFile::new();
        let mut file = FileStorage::new(test_file.file_name()).unwrap();
        file.write(0, "Hello, World".as_bytes()).unwrap();

        assert_eq!(file.read(0, 12).unwrap(), "Hello, World".as_bytes());
        assert_eq!(file.len(), 12);

        let _guard = file.lock.lock();

        assert_eq!(file.read(0, 12).unwrap(), "Hello, World".as_bytes());
        assert_eq!(file.len(), 12);
    }

    #[test]
    fn resize() {
        let test_file = TestFile::new();
        let mut file = FileStorage::new(test_file.file_name()).unwrap();
        file.write(0, "Hello, World".as_bytes()).unwrap();
        assert_eq!(file.len(), 12);
        file.resize(20).unwrap();
        assert_eq!(file.len(), 20);
        file.resize(10).unwrap();
        assert_eq!(file.read(0, 10).unwrap(), "Hello, Wor".as_bytes());
    }

    #[test]
    fn corrupted_wal_is_truncated() {
        let test_file = TestFile::new();

        //crate wal
        {
            let mut wal = WriteAheadLog::new(test_file.file_name()).unwrap();
            wal.insert(0, "Hello".as_bytes()).unwrap(); //len 5
            wal.insert(5, "World".as_bytes()).unwrap();
        }

        // truncate wal simulating cutoff during write
        // of the value causing the wal record to overrun
        // the file length
        {
            let file = File::options()
                .write(true)
                .open(TestFile::hidden_filename(test_file.file_name()))
                .unwrap();
            file.set_len((std::mem::size_of::<u64>() * 4 + 6) as u64)
                .unwrap();
        }

        let storage = FileStorage::new(test_file.file_name()).unwrap();
        assert_eq!(storage.len(), 5);
    }

    #[test]
    fn corrupted_wal_is_truncated_validation_read_error() {
        let test_file = TestFile::new();

        //crate wal
        {
            let mut wal = WriteAheadLog::new(test_file.file_name()).unwrap();
            wal.insert(0, "Hello".as_bytes()).unwrap(); //len 5
            wal.insert(5, "World".as_bytes()).unwrap();
        }

        // truncate wal simulating cutoff during write
        // of value meta-data (i.e. pos or value len)
        // causing the record to be unreadable
        {
            let file = File::options()
                .write(true)
                .open(TestFile::hidden_filename(test_file.file_name()))
                .unwrap();
            file.set_len((std::mem::size_of::<u64>() * 3 + 5) as u64)
                .unwrap();
        }

        let storage = FileStorage::new(test_file.file_name()).unwrap();
        assert_eq!(storage.len(), 5);
    }

    #[test]
    fn load_with_version_info() {
        let test_file = TestFile::new();

        let mut with_version_record = 0_u64.serialize();
        with_version_record.extend(8_u64.serialize());
        with_version_record.extend(1_u64.serialize());
        let value1 = vec![1_u64, 2, 3, 4, 5];
        with_version_record.extend(1_u64.serialize());
        with_version_record.extend(value1.serialized_size().serialize());
        with_version_record.extend(value1.serialize());
        let value2 = "Hello, world!".to_string();
        with_version_record.extend(0_u64.serialize());
        with_version_record.extend(value2.serialized_size().serialize());
        with_version_record.extend(value2.serialize());
        let value3 = -20_i64;
        with_version_record.extend(3_u64.serialize());
        with_version_record.extend(value3.serialized_size().serialize());
        with_version_record.extend(value3.serialize());
        std::fs::write(test_file.file_name(), with_version_record).unwrap();

        let storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();
        let vec = storage.value::<Vec<u64>>(StorageIndex(1)).unwrap();
        let val = storage.value::<i64>(StorageIndex(3)).unwrap();

        assert_eq!(vec, vec![1, 2, 3, 4, 5]);
        assert_eq!(val, -20);
        assert_eq!(storage.len(), 149);
        assert_eq!(storage.version(), 1);
    }

    #[test]
    fn load_with_version_info_optimize() {
        let test_file = TestFile::new();

        let mut with_version_record = 0_u64.serialize();
        with_version_record.extend(8_u64.serialize());
        with_version_record.extend(1_u64.serialize());
        let value1 = vec![1_u64, 2, 3, 4, 5];
        with_version_record.extend(1_u64.serialize());
        with_version_record.extend(value1.serialized_size().serialize());
        with_version_record.extend(value1.serialize());
        let value2 = "Hello, world!".to_string();
        with_version_record.extend(0_u64.serialize());
        with_version_record.extend(value2.serialized_size().serialize());
        with_version_record.extend(value2.serialize());
        let value3 = -20_i64;
        with_version_record.extend(3_u64.serialize());
        with_version_record.extend(value3.serialized_size().serialize());
        with_version_record.extend(value3.serialize());
        std::fs::write(test_file.file_name(), with_version_record).unwrap();

        let mut storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();
        let vec = storage.value::<Vec<u64>>(StorageIndex(1)).unwrap();
        let val = storage.value::<i64>(StorageIndex(3)).unwrap();

        storage.shrink_to_fit().unwrap();

        assert_eq!(vec, vec![1, 2, 3, 4, 5]);
        assert_eq!(val, -20);
        assert_eq!(storage.len(), 112);
        assert_eq!(storage.version(), 1);
    }

    #[test]
    fn load_without_version_info() {
        let test_file = TestFile::new();

        let mut buf = Vec::new();
        let value1 = vec![1_u64, 2, 3, 4, 5];
        buf.extend(1_u64.serialize());
        buf.extend(value1.serialized_size().serialize());
        buf.extend(value1.serialize());
        let value2 = "Hello, world!".to_string();
        buf.extend(0_u64.serialize());
        buf.extend(value2.serialized_size().serialize());
        buf.extend(value2.serialize());
        let value3 = -20_i64;
        buf.extend(3_u64.serialize());
        buf.extend(value3.serialized_size().serialize());
        buf.extend(value3.serialize());
        std::fs::write(test_file.file_name(), buf).unwrap();

        let mut storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();
        let vec = storage.value::<Vec<u64>>(StorageIndex(1)).unwrap();
        let val = storage.value::<i64>(StorageIndex(3)).unwrap();

        storage.shrink_to_fit().unwrap();

        assert_eq!(vec, vec![1, 2, 3, 4, 5]);
        assert_eq!(val, -20);
        assert_eq!(storage.len(), 112);
        assert_eq!(storage.version(), 1);
    }

    #[test]
    fn load_with_too_high_version_info() {
        let test_file = TestFile::new();

        let mut with_version_record = 0_u64.serialize();
        with_version_record.extend(8_u64.serialize());
        with_version_record.extend(2_u64.serialize());
        std::fs::write(test_file.file_name(), with_version_record).unwrap();

        assert_eq!(
            Storage::<FileStorage>::new(test_file.file_name()).unwrap_err(),
            DbError::from("Storage error: db version '2' is higher than the current version '1'")
        );
    }

    #[test]
    fn load_with_corrupted_version_info() {
        let test_file = TestFile::new();

        let mut with_version_record = 0_u64.serialize();
        with_version_record.extend(4_u64.serialize());
        std::fs::write(test_file.file_name(), with_version_record).unwrap();

        assert_eq!(
            Storage::<FileStorage>::new(test_file.file_name()).unwrap_err(),
            DbError::from("Storage error: invalid version record size (4 < 8)")
        );
    }

    #[test]
    fn load_without_version_info_large_data() {
        let test_file = TestFile::new();

        let mut buf = Vec::new();
        let value1 = vec![1_u64; 256000];
        buf.extend(1_u64.serialize());
        buf.extend(value1.serialized_size().serialize());
        buf.extend(value1.serialize());
        std::fs::write(test_file.file_name(), buf).unwrap();

        let storage = Storage::<FileStorage>::new(test_file.file_name()).unwrap();
        let vec = storage.value::<Vec<u64>>(StorageIndex(1)).unwrap();

        assert_eq!(vec, value1);
        assert_eq!(storage.version(), 1);
    }
}
