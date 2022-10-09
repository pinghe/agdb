use agdb_dictionary::StorageDictionary;
use agdb_storage::StorageFile;
use agdb_test_utilities::TestFile;
use std::cell::RefCell;
use std::rc::Rc;

#[test]
fn insert() {
    let test_file = TestFile::new();
    let storage = Rc::new(RefCell::new(
        StorageFile::try_from(test_file.file_name().clone()).unwrap(),
    ));
    let mut dictionary = StorageDictionary::<i64>::try_from(storage).unwrap();

    let index = dictionary.insert(&10).unwrap();

    assert_eq!(dictionary.len(), Ok(1));
    assert_eq!(dictionary.value(&index), Ok(Some(10_i64)));
    assert_eq!(dictionary.count(&index), Ok(Some(1)));
}

#[test]
fn insert_multiple() {
    let test_file = TestFile::new();
    let storage = Rc::new(RefCell::new(
        StorageFile::try_from(test_file.file_name().clone()).unwrap(),
    ));
    let mut dictionary = StorageDictionary::<i64>::try_from(storage).unwrap();

    let index1 = dictionary.insert(&10).unwrap();
    let index2 = dictionary.insert(&15).unwrap();
    let index3 = dictionary.insert(&20).unwrap();

    assert_eq!(dictionary.len(), Ok(3));

    assert_eq!(dictionary.value(&index1).unwrap(), Some(10_i64));
    assert_eq!(dictionary.count(&index1), Ok(Some(1)));

    assert_eq!(dictionary.value(&index2).unwrap(), Some(15_i64));
    assert_eq!(dictionary.count(&index2), Ok(Some(1)));

    assert_eq!(dictionary.value(&index3).unwrap(), Some(20_i64));
    assert_eq!(dictionary.count(&index3), Ok(Some(1)));
}

#[test]
fn insert_same() {
    let test_file = TestFile::new();
    let storage = Rc::new(RefCell::new(
        StorageFile::try_from(test_file.file_name().clone()).unwrap(),
    ));
    let mut dictionary = StorageDictionary::<i64>::try_from(storage).unwrap();

    dictionary.insert(&10).unwrap();

    let index2 = dictionary.insert(&15).unwrap();

    assert_eq!(dictionary.insert(&15).unwrap(), index2);
    assert_eq!(dictionary.insert(&15).unwrap(), index2);

    dictionary.insert(&20).unwrap();

    assert_eq!(dictionary.len(), Ok(3));
    assert_eq!(dictionary.count(&index2), Ok(Some(3)));
}