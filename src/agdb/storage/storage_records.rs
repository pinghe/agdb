use crate::storage::storage_index::StorageIndex;
use crate::storage::storage_record::StorageRecord;

pub struct StorageRecords {
    pub(crate) records: Vec<StorageRecord>,
}

impl StorageRecords {
    pub fn create(&mut self, position: u64, size: u64) -> StorageRecord {
        let index;

        if let Some(free_index) = self.free_index() {
            index = free_index.as_usize();
            self.records[index] = StorageRecord {
                index: free_index,
                position,
                size,
            };
        } else {
            index = self.records.len();
            self.records.push(StorageRecord {
                index: StorageIndex::from(index),
                position,
                size,
            });
        }

        self.records[index].clone()
    }

    pub fn get(&self, index: &StorageIndex) -> Option<&StorageRecord> {
        if let Some(record) = self.records.get(index.as_usize()) {
            if record.size != 0 {
                return Some(record);
            }
        }

        None
    }

    pub fn get_mut(&mut self, index: &StorageIndex) -> Option<&mut StorageRecord> {
        if let Some(record) = self.records.get_mut(index.as_usize()) {
            if record.size != 0 {
                return Some(record);
            }
        }

        None
    }

    pub fn indexes_by_position(&self) -> Vec<StorageIndex> {
        let mut indexes = Vec::<StorageIndex>::new();

        for index in 1..self.records.len() {
            if self.records[index].size != 0 {
                indexes.push(StorageIndex::from(index));
            }
        }

        indexes.sort_by(|left, right| {
            self.records[left.as_usize()]
                .position
                .cmp(&self.records[right.as_usize()].position)
        });

        indexes
    }

    pub fn remove(&mut self, index: &StorageIndex) {
        if let Some(_record) = self.get_mut(index) {
            self.add_free_index(index);
        }
    }

    pub(crate) fn add_free_index(&mut self, index: &StorageIndex) {
        self.records[index.as_usize()].position = self.records[0].position;
        self.records[index.as_usize()].size = 0;
        self.records[0].position = index.as_u64();
    }

    fn free_index(&mut self) -> Option<StorageIndex> {
        let free = self.records[0].position;

        if free != 0 {
            self.records[0].position = self.records[free as usize].position;
            return Some(StorageIndex::from(free));
        }

        None
    }
}

impl Default for StorageRecords {
    fn default() -> Self {
        Self {
            records: vec![StorageRecord::default()],
        }
    }
}

impl From<Vec<StorageRecord>> for StorageRecords {
    fn from(mut records: Vec<StorageRecord>) -> Self {
        records.sort();

        let mut file_records = StorageRecords::default();

        if let Some(last) = records.last() {
            if last.index.is_valid() {
                file_records = StorageRecords {
                    records: vec![StorageRecord::default(); last.index.as_usize() + 1],
                };
            }
        }

        let mut last_index = 0;

        for record in records {
            if !record.index.is_valid() {
                continue;
            }

            for index in (last_index + 1)..record.index.value() {
                file_records.add_free_index(&StorageIndex::from(index));
            }

            file_records.records[record.index.as_usize()].position = record.position;
            file_records.records[record.index.as_usize()].size = record.size;
            last_index = record.index.value();
        }

        file_records
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create() {
        let mut file_records = StorageRecords::default();

        let record = file_records.create(1, 4);

        assert_eq!(
            record,
            StorageRecord {
                index: StorageIndex::from(1_i64),
                position: 1,
                size: 4
            }
        );
    }

    #[test]
    fn default_constructed() {
        let _records = StorageRecords::default();
    }

    #[test]
    fn from_records() {
        let index1 = StorageIndex::from(2_i64);
        let index2 = StorageIndex::from(1_i64);
        let index3 = StorageIndex::from(3_i64);

        let file_records = StorageRecords::from(vec![
            StorageRecord {
                index: index1.clone(),
                position: 8,
                size: 16,
            },
            StorageRecord {
                index: index2.clone(),
                position: 24,
                size: 16,
            },
            StorageRecord {
                index: index3.clone(),
                position: 40,
                size: 16,
            },
        ]);

        assert_eq!(
            file_records.get(&index1),
            Some(&StorageRecord {
                index: StorageIndex::default(),
                position: 8,
                size: 16
            })
        );
        assert_eq!(
            file_records.get(&index2),
            Some(&StorageRecord {
                index: StorageIndex::default(),
                position: 24,
                size: 16
            })
        );
        assert_eq!(
            file_records.get(&index3),
            Some(&StorageRecord {
                index: StorageIndex::default(),
                position: 40,
                size: 16
            })
        );
    }

    #[test]
    fn from_records_with_index_gaps() {
        let record1 = StorageRecord {
            index: StorageIndex::from(5_i64),
            position: 24,
            size: 16,
        };
        let record2 = StorageRecord {
            index: StorageIndex::from(1_i64),
            position: 40,
            size: 16,
        };
        let record3 = StorageRecord {
            index: StorageIndex::from(2_i64),
            position: 40,
            size: 16,
        };

        let mut file_records = StorageRecords::from(vec![record1, record2, record3]);

        let record1 = file_records.create(2, 2);
        let record2 = file_records.create(4, 4);
        let record3 = file_records.create(6, 6);

        assert_eq!(
            record1,
            StorageRecord {
                index: StorageIndex::from(4_i64),
                position: 2,
                size: 2
            }
        );
        assert_eq!(
            record2,
            StorageRecord {
                index: StorageIndex::from(3_i64),
                position: 4,
                size: 4
            }
        );
        assert_eq!(
            record3,
            StorageRecord {
                index: StorageIndex::from(6_i64),
                position: 6,
                size: 6
            }
        );
    }

    #[test]
    fn from_records_with_removed_index() {
        let record1 = StorageRecord {
            index: StorageIndex::from(1_i64),
            position: 24,
            size: 16,
        };
        let record2 = StorageRecord {
            index: StorageIndex::from(-2_i64),
            position: 40,
            size: 16,
        };
        let record3 = StorageRecord {
            index: StorageIndex::from(3_i64),
            position: 40,
            size: 16,
        };

        let file_records = StorageRecords::from(vec![record1, record2, record3]);

        assert_eq!(file_records.get(&StorageIndex::default()), None);
    }

    #[test]
    fn get() {
        let mut file_records = StorageRecords::default();
        let position = 32_u64;
        let size = 64_u64;

        let record = file_records.create(position, size);
        let expected_record = StorageRecord {
            index: StorageIndex::from(1_i64),
            position,
            size,
        };

        assert_eq!(file_records.get(&record.index), Some(&expected_record));
    }

    #[test]
    fn get_mut() {
        let mut file_records = StorageRecords::default();
        let position = 32_u64;
        let size = 64_u64;

        let record = file_records.create(position, size);
        let mut expected_record = StorageRecord {
            index: StorageIndex::from(1_i64),
            position,
            size,
        };

        assert_eq!(
            file_records.get_mut(&record.index),
            Some(&mut expected_record)
        );
    }

    #[test]
    fn get_mut_invalid_index() {
        let mut file_records = StorageRecords::default();

        assert_eq!(file_records.get_mut(&StorageIndex::from(-1_i64)), None);
    }

    #[test]
    fn get_mut_zero_index() {
        let mut file_records = StorageRecords::default();

        assert_eq!(file_records.get_mut(&StorageIndex::default()), None);
    }

    #[test]
    fn get_zero_index() {
        let file_records = StorageRecords::default();

        assert_eq!(file_records.get(&StorageIndex::default()), None);
    }

    #[test]
    fn indexes_by_position() {
        let mut file_records = StorageRecords::default();
        let index1 = file_records.create(30, 8).index;
        let index2 = file_records.create(20, 8).index;
        let index3 = file_records.create(10, 8).index;
        file_records.remove(&index2);

        assert_eq!(file_records.indexes_by_position(), vec![index3, index1]);
    }

    #[test]
    fn remove() {
        let mut file_records = StorageRecords::default();
        let record = file_records.create(8u64, 16u64);

        file_records.remove(&record.index);

        assert_eq!(file_records.get(&record.index), None);
    }

    #[test]
    fn remove_invalid_index() {
        let mut file_records = StorageRecords::default();
        let record = file_records.create(16_u64, 48_u64);

        file_records.remove(&StorageIndex::from(-1_i64));

        assert_eq!(file_records.get(&record.index), Some(&record));
    }

    #[test]
    fn reuse_indexes() {
        let mut file_records = StorageRecords::default();
        let record = file_records.create(8u64, 16u64);
        file_records.remove(&record.index);
        let other = file_records.create(16u64, 32u64);

        assert_eq!(record.index, other.index);
    }
}