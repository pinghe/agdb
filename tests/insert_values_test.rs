mod test_db;

use agdb::DbElement;
use agdb::DbId;
use agdb::QueryBuilder;
use agdb::QueryError;
use test_db::TestDb;

#[test]
fn insert_values_ids_rollback() {
    let mut db = TestDb::new();
    db.exec_mut_ids(QueryBuilder::insert().nodes().count(1).query(), &[1]);
    db.transaction_mut_error(
        |t| -> Result<(), QueryError> {
            assert_eq!(
                t.exec_mut(
                    &QueryBuilder::insert()
                        .values(&[&[
                            ("key", vec![1.1, 2.1]).into(),
                            (vec!["a".to_string(), "b".to_string()], vec![1, 2]).into(),
                            ("numbers", vec![1_u64, 2_u64, 3_u64]).into(),
                            (1_u64, 10_u64).into(),
                            ("bytes", vec![0_u8, 1_u8, 2_u8, 3_u8, 4_u8]).into(),
                            ("really large bytes", vec![1_u8; 32]).into()
                        ]])
                        .ids(&[1.into()])
                        .query()
                )?
                .result,
                6
            );
            assert_eq!(
                t.exec(&QueryBuilder::select().ids(&[1.into()]).query())?
                    .elements,
                vec![DbElement {
                    id: DbId(1),
                    values: vec![
                        ("key", vec![1.1, 2.1]).into(),
                        (vec!["a".to_string(), "b".to_string()], vec![1, 2]).into(),
                        ("numbers", vec![1_u64, 2_u64, 3_u64]).into(),
                        (1_u64, 10_u64).into(),
                        ("bytes", vec![0_u8, 1_u8, 2_u8, 3_u8, 4_u8]).into(),
                        ("really large bytes", vec![1_u8; 32]).into()
                    ],
                }]
            );
            Err(QueryError::from("error"))
        },
        QueryError::from("error"),
    );
    db.exec_elements(
        QueryBuilder::select().ids(&[1.into()]).query(),
        &[DbElement {
            id: DbId(1),
            values: vec![],
        }],
    );
}

#[test]
fn insert_values_ids() {
    let mut db = TestDb::new();
    db.exec_mut_ids(QueryBuilder::insert().nodes().count(2).query(), &[1, 2]);
    db.exec_mut(
        QueryBuilder::insert()
            .values(&[
                &[("some really long key", 1000).into()],
                &[(10, 1.1).into()],
            ])
            .ids(&[1.into(), 2.into()])
            .query(),
        2,
    );
    db.exec_elements(
        QueryBuilder::select().ids(&[1.into(), 2.into()]).query(),
        &[
            DbElement {
                id: DbId(1),
                values: vec![("some really long key", 1000).into()],
            },
            DbElement {
                id: DbId(2),
                values: vec![(10, 1.1).into()],
            },
        ],
    );
}

#[test]
fn insert_values_invalid_length() {
    let mut db = TestDb::new();
    db.exec_mut_error(
        QueryBuilder::insert()
            .values(&[&[("key", "value").into()]])
            .ids(&[])
            .query(),
        "Ids and values length do not match",
    )
}

#[test]
fn insert_values_uniform_ids() {
    let mut db = TestDb::new();
    db.exec_mut_ids(
        QueryBuilder::insert()
            .nodes()
            .aliases(&["alias".into(), "alias2".into()])
            .query(),
        &[1, 2],
    );
    db.exec_mut(
        QueryBuilder::insert()
            .values_uniform(&[("key", "value").into()])
            .ids(&["alias".into(), "alias2".into()])
            .query(),
        2,
    );
    db.exec_elements(
        QueryBuilder::select()
            .ids(&["alias".into(), "alias2".into()])
            .query(),
        &[
            DbElement {
                id: DbId(1),
                values: vec![("key", "value").into()],
            },
            DbElement {
                id: DbId(2),
                values: vec![("key", "value").into()],
            },
        ],
    );
}

#[test]
fn insert_values_uniform_search() {
    let mut db = TestDb::new();
    db.exec_mut(QueryBuilder::insert().nodes().count(3).query(), 3);
    db.exec_mut(
        QueryBuilder::insert()
            .edges()
            .from(&[1.into(), 2.into()])
            .to(&[2.into(), 3.into()])
            .query(),
        2,
    );
    db.exec_mut(
        QueryBuilder::insert()
            .values_uniform(&[("key", "value").into()])
            .search(QueryBuilder::search().from(1).query())
            .query(),
        5,
    );
    db.exec_elements(
        QueryBuilder::select()
            .search(QueryBuilder::search().from(1).query())
            .query(),
        &[
            DbElement {
                id: DbId(1),
                values: vec![("key", "value").into()],
            },
            DbElement {
                id: DbId(-4),
                values: vec![("key", "value").into()],
            },
            DbElement {
                id: DbId(2),
                values: vec![("key", "value").into()],
            },
            DbElement {
                id: DbId(-5),
                values: vec![("key", "value").into()],
            },
            DbElement {
                id: DbId(3),
                values: vec![("key", "value").into()],
            },
        ],
    );
}

#[test]
fn insert_values_search() {
    let mut db = TestDb::new();
    db.exec_mut(QueryBuilder::insert().nodes().count(3).query(), 3);
    db.exec_mut(
        QueryBuilder::insert()
            .edges()
            .from(&[1.into()])
            .to(&[3.into()])
            .query(),
        1,
    );
    db.exec_mut(
        QueryBuilder::insert()
            .values(&[
                &[("key1", "value1").into()],
                &[("key2", "value2").into()],
                &[("key3", "value3").into()],
            ])
            .search(QueryBuilder::search().from(1).query())
            .query(),
        3,
    );
    db.exec_elements(
        QueryBuilder::select()
            .search(QueryBuilder::search().from(1).query())
            .query(),
        &[
            DbElement {
                id: DbId(1),
                values: vec![("key1", "value1").into()],
            },
            DbElement {
                id: DbId(-4),
                values: vec![("key2", "value2").into()],
            },
            DbElement {
                id: DbId(3),
                values: vec![("key3", "value3").into()],
            },
        ],
    );
}

#[test]
fn insert_values_search_invalid_length() {
    let mut db = TestDb::new();
    db.exec_mut(QueryBuilder::insert().nodes().count(3).query(), 3);
    db.exec_mut(
        QueryBuilder::insert()
            .edges()
            .from(&[1.into()])
            .to(&[3.into()])
            .query(),
        1,
    );
    db.exec_mut_error(
        QueryBuilder::insert()
            .values(&[&[("key1", "value1").into()], &[("key2", "value2").into()]])
            .search(QueryBuilder::search().from(1).query())
            .query(),
        "Ids and values length do not match",
    );
}