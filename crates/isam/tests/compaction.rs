mod common;
use common::make_db;
use highlandcows_isam::Isam;

#[test]
fn test_compact_removes_tombstones() {
    let (dir, db): (_, Isam<u32, String>) = make_db();

    {
        let mut txn = db.begin_transaction().unwrap();
        for i in 0..20u32 {
            db.insert(&mut txn, i, &format!("v{i}")).unwrap();
        }
        txn.commit().unwrap();
    }
    {
        let mut txn = db.begin_transaction().unwrap();
        for i in 0..10u32 {
            db.delete(&mut txn, &i).unwrap();
        }
        txn.commit().unwrap();
    }

    let idb_before = std::fs::metadata(dir.path().join("test.idb"))
        .unwrap()
        .len();

    db.compact().unwrap();

    let idb_after = std::fs::metadata(dir.path().join("test.idb"))
        .unwrap()
        .len();

    assert!(
        idb_after < idb_before,
        "expected idb_after ({idb_after}) < idb_before ({idb_before})"
    );
}

#[test]
fn test_compact_preserves_all_alive_records() {
    let (_dir, db): (_, Isam<u32, String>) = make_db();

    {
        let mut txn = db.begin_transaction().unwrap();
        for i in 0..50u32 {
            db.insert(&mut txn, i, &format!("value_{i}")).unwrap();
        }
        txn.commit().unwrap();
    }
    {
        let mut txn = db.begin_transaction().unwrap();
        for i in (0..50u32).step_by(2) {
            db.delete(&mut txn, &i).unwrap();
        }
        txn.commit().unwrap();
    }

    db.compact().unwrap();

    let mut txn = db.begin_transaction().unwrap();
    for i in 0..50u32 {
        let got = db.get(&mut txn, &i).unwrap();
        if i % 2 == 0 {
            assert_eq!(got, None, "key {i} should be gone");
        } else {
            assert_eq!(got, Some(format!("value_{i}")), "key {i} should survive");
        }
    }
    txn.commit().unwrap();
}
