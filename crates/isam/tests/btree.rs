mod common;
use common::make_db;
use highlandcows_isam::Isam;

#[test]
fn test_btree_forces_page_split() {
    let (_dir, db): (_, Isam<u32, u32>) = make_db();
    let mut txn = db.begin_transaction().unwrap();
    for i in 0..300u32 {
        db.insert(&mut txn, i, &i).unwrap();
    }
    for i in 0..300u32 {
        assert_eq!(db.get(&mut txn, &i).unwrap(), Some(i));
    }
    txn.commit().unwrap();
}

#[test]
fn test_btree_forces_merge_after_delete() {
    let (_dir, db): (_, Isam<u32, u32>) = make_db();
    let mut txn = db.begin_transaction().unwrap();
    for i in 0..200u32 {
        db.insert(&mut txn, i, &i).unwrap();
    }
    for i in 10..200u32 {
        db.delete(&mut txn, &i).unwrap();
    }
    for i in 0..10u32 {
        assert_eq!(db.get(&mut txn, &i).unwrap(), Some(i));
    }
    for i in 10..200u32 {
        assert_eq!(db.get(&mut txn, &i).unwrap(), None);
    }
    txn.commit().unwrap();
}
