mod common;
use common::make_db;
use highlandcows_isam::{IsamError, Isam};

// ── Basic CRUD ─────────────────────────────────────────────────────────── //

#[test]
fn test_insert_and_get() {
    let (_dir, db): (_, Isam<u32, String>) = make_db();
    let mut txn = db.begin_transaction().unwrap();
    db.insert(&mut txn, 1, &"hello".to_string()).unwrap();
    assert_eq!(db.get(&mut txn, &1).unwrap(), Some("hello".to_string()));
    txn.commit().unwrap();
}

#[test]
fn test_update() {
    let (_dir, db): (_, Isam<u32, String>) = make_db();
    let mut txn = db.begin_transaction().unwrap();
    db.insert(&mut txn, 42, &"old".to_string()).unwrap();
    db.update(&mut txn, 42, &"new".to_string()).unwrap();
    assert_eq!(db.get(&mut txn, &42).unwrap(), Some("new".to_string()));
    txn.commit().unwrap();
}

#[test]
fn test_delete() {
    let (_dir, db): (_, Isam<u32, String>) = make_db();
    let mut txn = db.begin_transaction().unwrap();
    db.insert(&mut txn, 7, &"bye".to_string()).unwrap();
    db.delete(&mut txn, &7).unwrap();
    assert_eq!(db.get(&mut txn, &7).unwrap(), None);
    txn.commit().unwrap();
}

// ── Edge cases ─────────────────────────────────────────────────────────── //

#[test]
fn test_get_missing_key_returns_none() {
    let (_dir, db): (_, Isam<u32, String>) = make_db();
    let mut txn = db.begin_transaction().unwrap();
    db.insert(&mut txn, 1, &"one".to_string()).unwrap();
    db.insert(&mut txn, 2, &"two".to_string()).unwrap();
    db.insert(&mut txn, 3, &"three".to_string()).unwrap();
    assert_eq!(db.get(&mut txn, &999).unwrap(), None);
    txn.commit().unwrap();
}

#[test]
fn test_update_missing_key_returns_err() {
    let (_dir, db): (_, Isam<u32, String>) = make_db();
    let mut txn = db.begin_transaction().unwrap();
    db.insert(&mut txn, 1, &"one".to_string()).unwrap();
    db.insert(&mut txn, 2, &"two".to_string()).unwrap();
    db.insert(&mut txn, 3, &"three".to_string()).unwrap();
    let err = db.update(&mut txn, 999, &"x".to_string()).unwrap_err();
    assert!(matches!(err, IsamError::KeyNotFound));
    txn.commit().unwrap();
}

#[test]
fn test_insert_duplicate_key_returns_err() {
    let (_dir, db): (_, Isam<u32, String>) = make_db();
    let mut txn = db.begin_transaction().unwrap();
    db.insert(&mut txn, 1, &"first".to_string()).unwrap();
    let err = db.insert(&mut txn, 1, &"second".to_string()).unwrap_err();
    assert!(matches!(err, IsamError::DuplicateKey));
    txn.commit().unwrap();
}
