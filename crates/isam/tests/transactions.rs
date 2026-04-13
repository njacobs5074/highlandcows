mod common;
use common::make_db;
use highlandcows_isam::{IsamError, Isam};
use tempfile::TempDir;

// ── Transaction semantics ─────────────────────────────────────────────── //

#[test]
fn test_transaction_commit() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("txn_commit");

    let db: Isam<u32, String> = Isam::create(&path).unwrap();
    {
        let mut txn = db.begin_transaction().unwrap();
        db.insert(&mut txn, 1, &"committed".to_string()).unwrap();
        txn.commit().unwrap();
    }

    let db2: Isam<u32, String> = Isam::open(&path).unwrap();
    let mut txn = db2.begin_transaction().unwrap();
    assert_eq!(db2.get(&mut txn, &1).unwrap(), Some("committed".to_string()));
    txn.commit().unwrap();
}

#[test]
fn test_transaction_auto_rollback_on_drop() {
    let (_dir, db): (_, Isam<u32, String>) = make_db();

    {
        let mut txn = db.begin_transaction().unwrap();
        db.insert(&mut txn, 42, &"should_disappear".to_string()).unwrap();
        // drop without commit → auto rollback
    }

    let mut txn = db.begin_transaction().unwrap();
    assert_eq!(db.get(&mut txn, &42).unwrap(), None);
    txn.commit().unwrap();
}

#[test]
fn test_transaction_explicit_rollback() {
    let (_dir, db): (_, Isam<u32, String>) = make_db();

    let mut txn = db.begin_transaction().unwrap();
    db.insert(&mut txn, 10, &"gone".to_string()).unwrap();
    txn.rollback().unwrap();

    let mut txn2 = db.begin_transaction().unwrap();
    assert_eq!(db.get(&mut txn2, &10).unwrap(), None);
    txn2.commit().unwrap();
}

#[test]
fn test_transaction_partial_failure_rolls_back_all() {
    let (_dir, db): (_, Isam<u32, String>) = make_db();

    {
        let mut txn = db.begin_transaction().unwrap();
        db.insert(&mut txn, 2, &"existing".to_string()).unwrap();
        txn.commit().unwrap();
    }

    {
        let mut txn = db.begin_transaction().unwrap();
        db.insert(&mut txn, 1, &"a".to_string()).unwrap();
        let err = db.insert(&mut txn, 2, &"b".to_string()).unwrap_err();
        assert!(matches!(err, IsamError::DuplicateKey));
        // drop → auto rollback of key 1
    }

    let mut txn = db.begin_transaction().unwrap();
    assert_eq!(db.get(&mut txn, &1).unwrap(), None, "key 1 should be absent");
    assert_eq!(db.get(&mut txn, &2).unwrap(), Some("existing".to_string()));
    txn.commit().unwrap();
}

#[test]
fn test_transaction_rollback_update() {
    let (_dir, db): (_, Isam<u32, String>) = make_db();

    {
        let mut txn = db.begin_transaction().unwrap();
        db.insert(&mut txn, 5, &"original".to_string()).unwrap();
        txn.commit().unwrap();
    }

    {
        let mut txn = db.begin_transaction().unwrap();
        db.update(&mut txn, 5, &"modified".to_string()).unwrap();
        // drop → rollback restores original RecordRef in index
    }

    let mut txn = db.begin_transaction().unwrap();
    assert_eq!(db.get(&mut txn, &5).unwrap(), Some("original".to_string()));
    txn.commit().unwrap();
}

#[test]
fn test_transaction_rollback_delete() {
    let (_dir, db): (_, Isam<u32, String>) = make_db();

    {
        let mut txn = db.begin_transaction().unwrap();
        db.insert(&mut txn, 99, &"keeper".to_string()).unwrap();
        txn.commit().unwrap();
    }

    {
        let mut txn = db.begin_transaction().unwrap();
        db.delete(&mut txn, &99).unwrap();
        // drop → rollback re-inserts key
    }

    let mut txn = db.begin_transaction().unwrap();
    assert_eq!(db.get(&mut txn, &99).unwrap(), Some("keeper".to_string()));
    txn.commit().unwrap();
}

#[test]
fn test_transaction_read_within_txn() {
    let (_dir, db): (_, Isam<u32, String>) = make_db();
    let mut txn = db.begin_transaction().unwrap();
    db.insert(&mut txn, 7, &"visible".to_string()).unwrap();
    // Read within same txn should see the just-inserted value.
    assert_eq!(db.get(&mut txn, &7).unwrap(), Some("visible".to_string()));
    txn.commit().unwrap();
}

#[test]
fn test_isam_clone_concurrent_writes() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("clone_concurrent");

    let db: Isam<u32, String> = Isam::create(&path).unwrap();

    {
        let mut txn = db.begin_transaction().unwrap();
        db.insert(&mut txn, 1, &"from_original".to_string()).unwrap();
        txn.commit().unwrap();
    }

    let db2 = db.clone();
    {
        let mut txn = db2.begin_transaction().unwrap();
        db2.insert(&mut txn, 2, &"from_clone".to_string()).unwrap();
        txn.commit().unwrap();
    }

    let mut txn = db.begin_transaction().unwrap();
    assert_eq!(db.get(&mut txn, &1).unwrap(), Some("from_original".to_string()));
    assert_eq!(db.get(&mut txn, &2).unwrap(), Some("from_clone".to_string()));
    txn.commit().unwrap();
}

#[test]
fn test_transaction_commit_is_durable() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("durable");

    {
        let db: Isam<u32, String> = Isam::create(&path).unwrap();
        let mut txn = db.begin_transaction().unwrap();
        db.insert(&mut txn, 42, &"durable".to_string()).unwrap();
        txn.commit().unwrap();
    }

    let db2: Isam<u32, String> = Isam::open(&path).unwrap();
    let mut txn = db2.begin_transaction().unwrap();
    assert_eq!(db2.get(&mut txn, &42).unwrap(), Some("durable".to_string()));
    txn.commit().unwrap();
}

// ── write() / read() helpers ───────────────────────────────────────────── //

#[test]
fn test_write_insert_and_read_get() {
    let (_dir, db): (_, Isam<u32, String>) = make_db();

    db.write(|txn| db.insert(txn, 1, &"hello".to_string())).unwrap();

    let val = db.read(|txn| db.get(txn, &1)).unwrap();
    assert_eq!(val, Some("hello".to_string()));
}

#[test]
fn test_write_update_and_delete() {
    let (_dir, db): (_, Isam<u32, String>) = make_db();

    db.write(|txn| db.insert(txn, 1, &"hello".to_string())).unwrap();
    db.write(|txn| db.update(txn, 1, &"world".to_string())).unwrap();

    let val = db.read(|txn| db.get(txn, &1)).unwrap();
    assert_eq!(val, Some("world".to_string()));

    db.write(|txn| db.delete(txn, &1)).unwrap();

    let val = db.read(|txn| db.get(txn, &1)).unwrap();
    assert_eq!(val, None);
}

#[test]
fn test_write_multi_step() {
    let (_dir, db): (_, Isam<u32, String>) = make_db();

    db.write(|txn| {
        db.insert(txn, 1, &"one".to_string())?;
        db.insert(txn, 2, &"two".to_string())?;
        db.insert(txn, 3, &"three".to_string())?;
        Ok(())
    }).unwrap();

    assert_eq!(db.read(|txn| db.get(txn, &1)).unwrap(), Some("one".to_string()));
    assert_eq!(db.read(|txn| db.get(txn, &2)).unwrap(), Some("two".to_string()));
    assert_eq!(db.read(|txn| db.get(txn, &3)).unwrap(), Some("three".to_string()));
}

#[test]
fn test_write_rolls_back_on_error() {
    let (_dir, db): (_, Isam<u32, String>) = make_db();

    db.write(|txn| db.insert(txn, 1, &"one".to_string())).unwrap();

    let result = db.write(|txn| {
        db.insert(txn, 2, &"two".to_string())?;
        db.insert(txn, 1, &"duplicate".to_string())?; // errors here
        Ok(())
    });

    assert!(matches!(result, Err(IsamError::DuplicateKey)));
    assert_eq!(db.read(|txn| db.get(txn, &2)).unwrap(), None);
}

#[test]
fn test_write_returns_closure_value() {
    let (_dir, db): (_, Isam<u32, String>) = make_db();

    let inserted = db.write(|txn| {
        db.insert(txn, 42, &"answer".to_string())?;
        Ok(42u32)
    }).unwrap();

    assert_eq!(inserted, 42u32);
}

#[test]
fn test_read_returns_closure_value() {
    let (_dir, db): (_, Isam<u32, String>) = make_db();

    db.write(|txn| db.insert(txn, 1, &"hello".to_string())).unwrap();

    let count = db.read(|txn| {
        let mut n = 0u32;
        for item in db.iter(txn)? {
            item?;
            n += 1;
        }
        Ok(n)
    }).unwrap();

    assert_eq!(count, 1);
}
