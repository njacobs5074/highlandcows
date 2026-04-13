/// Integration tests for highlandcows-isam.
///
/// All tests use the transactional API:
///   let db = Isam::create(path)?;
///   let mut txn = db.begin_transaction()?;
///   db.insert(&mut txn, key, &val)?;
///   txn.commit()?;
use highlandcows_isam::{DeriveKey, IsamError, Isam};
use serde::{Deserialize, Serialize};
use tempfile::TempDir;

/// Helper: open a fresh database in a temp dir.
fn make_db<K, V>() -> (TempDir, Isam<K, V>)
where
    K: serde::Serialize + serde::de::DeserializeOwned + Ord + Clone + 'static,
    V: serde::Serialize + serde::de::DeserializeOwned + Clone + 'static,
{
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test");
    let db = Isam::create(&path).unwrap();
    (dir, db)
}

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

// ── Sequential access ──────────────────────────────────────────────────── //

#[test]
fn test_iter_returns_keys_in_sorted_order() {
    let (_dir, db): (_, Isam<u32, String>) = make_db();
    let mut txn = db.begin_transaction().unwrap();
    for k in [5u32, 2, 8, 1, 9, 3, 7, 4, 6] {
        db.insert(&mut txn, k, &k.to_string()).unwrap();
    }

    let pairs: Vec<(u32, String)> = db
        .iter(&mut txn)
        .unwrap()
        .map(|r| r.unwrap())
        .collect();

    let keys: Vec<u32> = pairs.iter().map(|(k, _)| *k).collect();
    let mut sorted = keys.clone();
    sorted.sort();
    assert_eq!(keys, sorted);
    assert_eq!(keys, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
    txn.commit().unwrap();
}

// ── B-tree structure ───────────────────────────────────────────────────── //

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

// ── Compaction ─────────────────────────────────────────────────────────── //

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

// ── Byte array keys and values ─────────────────────────────────────────── //

#[test]
fn test_byte_array_insert_and_get() {
    let (_dir, db): (_, Isam<Vec<u8>, Vec<u8>>) = make_db();
    let mut txn = db.begin_transaction().unwrap();
    let key = b"hello".to_vec();
    let val = b"world".to_vec();
    db.insert(&mut txn, key.clone(), &val).unwrap();
    assert_eq!(db.get(&mut txn, &key).unwrap(), Some(val));
    txn.commit().unwrap();
}

#[test]
fn test_byte_array_update() {
    let (_dir, db): (_, Isam<Vec<u8>, Vec<u8>>) = make_db();
    let mut txn = db.begin_transaction().unwrap();
    let key = b"key".to_vec();
    db.insert(&mut txn, key.clone(), &b"old".to_vec()).unwrap();
    db.update(&mut txn, key.clone(), &b"new".to_vec()).unwrap();
    assert_eq!(db.get(&mut txn, &key).unwrap(), Some(b"new".to_vec()));
    txn.commit().unwrap();
}

#[test]
fn test_byte_array_delete() {
    let (_dir, db): (_, Isam<Vec<u8>, Vec<u8>>) = make_db();
    let mut txn = db.begin_transaction().unwrap();
    let key = b"gone".to_vec();
    db.insert(&mut txn, key.clone(), &b"value".to_vec()).unwrap();
    db.delete(&mut txn, &key).unwrap();
    assert_eq!(db.get(&mut txn, &key).unwrap(), None);
    txn.commit().unwrap();
}

#[test]
fn test_byte_array_iter_sorted_order() {
    let (_dir, db): (_, Isam<Vec<u8>, Vec<u8>>) = make_db();
    let mut txn = db.begin_transaction().unwrap();
    let entries: Vec<(&[u8], &[u8])> = vec![
        (b"banana", b"2"),
        (b"apple", b"1"),
        (b"cherry", b"3"),
    ];
    for (k, v) in &entries {
        db.insert(&mut txn, k.to_vec(), &v.to_vec()).unwrap();
    }

    let pairs: Vec<(Vec<u8>, Vec<u8>)> = db
        .iter(&mut txn)
        .unwrap()
        .map(|r| r.unwrap())
        .collect();

    let keys: Vec<Vec<u8>> = pairs.into_iter().map(|(k, _)| k).collect();
    assert_eq!(
        keys,
        vec![b"apple".to_vec(), b"banana".to_vec(), b"cherry".to_vec()]
    );
    txn.commit().unwrap();
}

// ── Min / max key ──────────────────────────────────────────────────────── //

#[test]
fn test_min_max_empty_db() {
    let (_dir, db): (_, Isam<u32, u32>) = make_db();
    let mut txn = db.begin_transaction().unwrap();
    assert_eq!(db.min_key(&mut txn).unwrap(), None);
    assert_eq!(db.max_key(&mut txn).unwrap(), None);
    txn.commit().unwrap();
}

#[test]
fn test_min_max_single_entry() {
    let (_dir, db): (_, Isam<u32, u32>) = make_db();
    let mut txn = db.begin_transaction().unwrap();
    db.insert(&mut txn, 42, &42).unwrap();
    assert_eq!(db.min_key(&mut txn).unwrap(), Some(42));
    assert_eq!(db.max_key(&mut txn).unwrap(), Some(42));
    txn.commit().unwrap();
}

#[test]
fn test_min_max_basic() {
    let (_dir, db): (_, Isam<u32, u32>) = make_db();
    let mut txn = db.begin_transaction().unwrap();
    for k in [5u32, 2, 8, 1, 9, 3] {
        db.insert(&mut txn, k, &k).unwrap();
    }
    assert_eq!(db.min_key(&mut txn).unwrap(), Some(1));
    assert_eq!(db.max_key(&mut txn).unwrap(), Some(9));
    txn.commit().unwrap();
}

#[test]
fn test_min_max_after_delete() {
    let (_dir, db): (_, Isam<u32, u32>) = make_db();
    let mut txn = db.begin_transaction().unwrap();
    for k in 1..=5u32 {
        db.insert(&mut txn, k, &k).unwrap();
    }
    db.delete(&mut txn, &1).unwrap();
    db.delete(&mut txn, &5).unwrap();
    assert_eq!(db.min_key(&mut txn).unwrap(), Some(2));
    assert_eq!(db.max_key(&mut txn).unwrap(), Some(4));
    txn.commit().unwrap();
}

#[test]
fn test_min_max_across_page_boundary() {
    let (_dir, db): (_, Isam<u32, u32>) = make_db();
    let mut txn = db.begin_transaction().unwrap();
    for k in 0..300u32 {
        db.insert(&mut txn, k, &k).unwrap();
    }
    assert_eq!(db.min_key(&mut txn).unwrap(), Some(0));
    assert_eq!(db.max_key(&mut txn).unwrap(), Some(299));
    txn.commit().unwrap();
}

// ── Range search ───────────────────────────────────────────────────────── //

fn range_keys(db: &Isam<u32, u32>, txn: &mut highlandcows_isam::Transaction<'_, u32, u32>, bounds: impl std::ops::RangeBounds<u32>) -> Vec<u32> {
    db.range(txn, bounds)
        .unwrap()
        .map(|r| r.unwrap().0)
        .collect()
}

#[test]
fn test_range_inclusive() {
    let (_dir, db): (_, Isam<u32, u32>) = make_db();
    let mut txn = db.begin_transaction().unwrap();
    for i in 1..=20u32 {
        db.insert(&mut txn, i, &i).unwrap();
    }
    assert_eq!(range_keys(&db, &mut txn, 5..=10), vec![5, 6, 7, 8, 9, 10]);
    txn.commit().unwrap();
}

#[test]
fn test_range_exclusive_end() {
    let (_dir, db): (_, Isam<u32, u32>) = make_db();
    let mut txn = db.begin_transaction().unwrap();
    for i in 1..=20u32 {
        db.insert(&mut txn, i, &i).unwrap();
    }
    assert_eq!(range_keys(&db, &mut txn, 5..10), vec![5, 6, 7, 8, 9]);
    txn.commit().unwrap();
}

#[test]
fn test_range_unbounded_start() {
    let (_dir, db): (_, Isam<u32, u32>) = make_db();
    let mut txn = db.begin_transaction().unwrap();
    for i in 1..=20u32 {
        db.insert(&mut txn, i, &i).unwrap();
    }
    assert_eq!(range_keys(&db, &mut txn, ..=5), vec![1, 2, 3, 4, 5]);
    txn.commit().unwrap();
}

#[test]
fn test_range_unbounded_end() {
    let (_dir, db): (_, Isam<u32, u32>) = make_db();
    let mut txn = db.begin_transaction().unwrap();
    for i in 1..=20u32 {
        db.insert(&mut txn, i, &i).unwrap();
    }
    assert_eq!(range_keys(&db, &mut txn, 17..), vec![17, 18, 19, 20]);
    txn.commit().unwrap();
}

#[test]
fn test_range_full() {
    let (_dir, db): (_, Isam<u32, u32>) = make_db();
    let mut txn = db.begin_transaction().unwrap();
    for i in 1..=10u32 {
        db.insert(&mut txn, i, &i).unwrap();
    }
    assert_eq!(range_keys(&db, &mut txn, ..), (1..=10).collect::<Vec<_>>());
    txn.commit().unwrap();
}

#[test]
fn test_range_empty_result() {
    let (_dir, db): (_, Isam<u32, u32>) = make_db();
    let mut txn = db.begin_transaction().unwrap();
    for i in 1..=20u32 {
        db.insert(&mut txn, i, &i).unwrap();
    }
    assert_eq!(range_keys(&db, &mut txn, 10..=5), vec![]);
    txn.commit().unwrap();
}

#[test]
fn test_range_across_page_boundary() {
    let (_dir, db): (_, Isam<u32, u32>) = make_db();
    let mut txn = db.begin_transaction().unwrap();
    for i in 0..300u32 {
        db.insert(&mut txn, i, &i).unwrap();
    }
    let keys = range_keys(&db, &mut txn, 100..=200);
    assert_eq!(keys, (100..=200).collect::<Vec<_>>());
    txn.commit().unwrap();
}

// ── Schema versioning ──────────────────────────────────────────────────── //

#[test]
fn test_schema_version_defaults_to_zero() {
    let (_dir, db): (_, Isam<u32, String>) = make_db();
    assert_eq!(db.key_schema_version().unwrap(), 0);
    assert_eq!(db.val_schema_version().unwrap(), 0);
}

#[test]
fn test_schema_versions_persist_across_reopen() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("schema_test");

    {
        let db: Isam<u32, String> = Isam::create(&path).unwrap();
        let mut txn = db.begin_transaction().unwrap();
        db.insert(&mut txn, 1u32, &"42".to_string()).unwrap();
        txn.commit().unwrap();
        // migrate_values consumes db and returns the new typed db.
        let db2: Isam<u32, u64> = db
            .migrate_values(1, |s: String| Ok(s.parse::<u64>().unwrap_or(0)))
            .unwrap();
        drop(db2);
    }

    let db3: Isam<u32, u64> = Isam::open(&path).unwrap();
    assert_eq!(db3.key_schema_version().unwrap(), 0);
    assert_eq!(db3.val_schema_version().unwrap(), 1);
}

#[test]
fn test_migrate_values() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("migrate_values");

    let db: Isam<u32, String> = Isam::create(&path).unwrap();
    {
        let mut txn = db.begin_transaction().unwrap();
        db.insert(&mut txn, 1u32, &"3".to_string()).unwrap();
        db.insert(&mut txn, 2u32, &"7".to_string()).unwrap();
        db.insert(&mut txn, 3u32, &"11".to_string()).unwrap();
        txn.commit().unwrap();
    }

    let db2: Isam<u32, u64> = db
        .migrate_values(1, |s: String| Ok(s.len() as u64))
        .unwrap();

    assert_eq!(db2.val_schema_version().unwrap(), 1);
    assert_eq!(db2.key_schema_version().unwrap(), 0);
    let mut txn = db2.begin_transaction().unwrap();
    assert_eq!(db2.get(&mut txn, &1).unwrap(), Some(1u64));
    assert_eq!(db2.get(&mut txn, &2).unwrap(), Some(1u64));
    assert_eq!(db2.get(&mut txn, &3).unwrap(), Some(2u64));
    txn.commit().unwrap();
}

#[test]
fn test_migrate_keys() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("migrate_keys");

    let db: Isam<u32, String> = Isam::create(&path).unwrap();
    {
        let mut txn = db.begin_transaction().unwrap();
        db.insert(&mut txn, 1u32, &"one".to_string()).unwrap();
        db.insert(&mut txn, 2u32, &"two".to_string()).unwrap();
        db.insert(&mut txn, 3u32, &"three".to_string()).unwrap();
        txn.commit().unwrap();
    }

    let db2: Isam<String, String> = db
        .migrate_keys(1, |k: u32| Ok(format!("{k}")))
        .unwrap();

    assert_eq!(db2.key_schema_version().unwrap(), 1);
    assert_eq!(db2.val_schema_version().unwrap(), 0);
    let mut txn = db2.begin_transaction().unwrap();
    assert_eq!(db2.get(&mut txn, &"1".to_string()).unwrap(), Some("one".to_string()));
    assert_eq!(db2.get(&mut txn, &"2".to_string()).unwrap(), Some("two".to_string()));
    assert_eq!(db2.get(&mut txn, &"3".to_string()).unwrap(), Some("three".to_string()));

    let keys: Vec<String> = db2.iter(&mut txn).unwrap().map(|r| r.unwrap().0).collect();
    assert_eq!(keys, vec!["1".to_string(), "2".to_string(), "3".to_string()]);
    txn.commit().unwrap();
}

#[test]
fn test_migrate_keys_reorders_correctly() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("migrate_keys_reorder");

    let db: Isam<u32, String> = Isam::create(&path).unwrap();
    {
        let mut txn = db.begin_transaction().unwrap();
        for i in [1u32, 5, 3, 2, 4] {
            db.insert(&mut txn, i, &format!("v{i}")).unwrap();
        }
        txn.commit().unwrap();
    }

    let db2: Isam<String, String> = db
        .migrate_keys(2, |k: u32| Ok(format!("{k:02}")))
        .unwrap();

    // Check version before beginning the transaction (would deadlock inside txn).
    assert_eq!(db2.key_schema_version().unwrap(), 2);

    let mut txn = db2.begin_transaction().unwrap();
    let pairs: Vec<(String, String)> = db2.iter(&mut txn).unwrap().map(|r| r.unwrap()).collect();
    assert_eq!(pairs[0], ("01".to_string(), "v1".to_string()));
    assert_eq!(pairs[1], ("02".to_string(), "v2".to_string()));
    assert_eq!(pairs[4], ("05".to_string(), "v5".to_string()));
    txn.commit().unwrap();
}

#[test]
fn test_migrate_values_then_keys() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("migrate_chain");

    let db: Isam<u32, String> = Isam::create(&path).unwrap();
    {
        let mut txn = db.begin_transaction().unwrap();
        db.insert(&mut txn, 10u32, &"hello".to_string()).unwrap();
        db.insert(&mut txn, 20u32, &"world".to_string()).unwrap();
        txn.commit().unwrap();
    }

    let db2: Isam<u32, usize> = db
        .migrate_values(1, |s: String| Ok(s.len()))
        .unwrap();

    let db3: Isam<String, usize> = db2
        .migrate_keys(1, |k: u32| Ok(format!("{k:04}")))
        .unwrap();

    assert_eq!(db3.key_schema_version().unwrap(), 1);
    assert_eq!(db3.val_schema_version().unwrap(), 1);
    let mut txn = db3.begin_transaction().unwrap();
    assert_eq!(db3.get(&mut txn, &"0010".to_string()).unwrap(), Some(5usize));
    assert_eq!(db3.get(&mut txn, &"0020".to_string()).unwrap(), Some(5usize));
    txn.commit().unwrap();
}

// ── Persistence ────────────────────────────────────────────────────────── //

#[test]
fn test_data_survives_reopen() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("persist");

    {
        let db: Isam<String, u64> = Isam::create(&path).unwrap();
        let mut txn = db.begin_transaction().unwrap();
        db.insert(&mut txn, "alpha".to_string(), &100).unwrap();
        db.insert(&mut txn, "beta".to_string(), &200).unwrap();
        db.insert(&mut txn, "gamma".to_string(), &300).unwrap();
        txn.commit().unwrap();
    }

    {
        let db: Isam<String, u64> = Isam::open(&path).unwrap();
        let mut txn = db.begin_transaction().unwrap();
        assert_eq!(db.get(&mut txn, &"alpha".to_string()).unwrap(), Some(100));
        assert_eq!(db.get(&mut txn, &"beta".to_string()).unwrap(), Some(200));
        assert_eq!(db.get(&mut txn, &"gamma".to_string()).unwrap(), Some(300));
        assert_eq!(db.get(&mut txn, &"delta".to_string()).unwrap(), None);
        txn.commit().unwrap();
    }
}

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

    // Reopen and verify persistence.
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

    // Pre-insert key 2 so duplicate insert inside txn fails.
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

    // Write from original handle.
    {
        let mut txn = db.begin_transaction().unwrap();
        db.insert(&mut txn, 1, &"from_original".to_string()).unwrap();
        txn.commit().unwrap();
    }

    // Write from a clone (sequentially here; the lock serializes concurrent access).
    let db2 = db.clone();
    {
        let mut txn = db2.begin_transaction().unwrap();
        db2.insert(&mut txn, 2, &"from_clone".to_string()).unwrap();
        txn.commit().unwrap();
    }

    // Both keys visible via either handle.
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
        // db dropped here
    }

    // Reopen and verify the data is still there.
    let db2: Isam<u32, String> = Isam::open(&path).unwrap();
    let mut txn = db2.begin_transaction().unwrap();
    assert_eq!(db2.get(&mut txn, &42).unwrap(), Some("durable".to_string()));
    txn.commit().unwrap();
}

// ── Secondary index tests ──────────────────────────────────────────────────── //

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Person {
    name: String,
    city: String,
}

struct CityIndex;
impl DeriveKey<Person> for CityIndex {
    type Key = String;
    fn derive(p: &Person) -> String {
        p.city.clone()
    }
}

struct NameIndex;
impl DeriveKey<Person> for NameIndex {
    type Key = String;
    fn derive(p: &Person) -> String {
        p.name.clone()
    }
}

fn make_person_db() -> (TempDir, Isam<u32, Person>) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("people");
    let db = Isam::<u32, Person>::builder()
        .with_index("city", CityIndex)
        .with_index("name", NameIndex)
        .create(&path)
        .unwrap();
    (dir, db)
}

#[test]
fn test_secondary_index_basic_lookup() {
    let (_dir, db) = make_person_db();
    let city_idx = db.index::<CityIndex>("city");

    let mut txn = db.begin_transaction().unwrap();
    db.insert(&mut txn, 1, &Person { name: "Alice".into(), city: "London".into() }).unwrap();
    db.insert(&mut txn, 2, &Person { name: "Bob".into(), city: "Paris".into() }).unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin_transaction().unwrap();
    let results = city_idx.lookup(&mut txn, &"London".to_string()).unwrap();
    txn.commit().unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, 1);
    assert_eq!(results[0].1.name, "Alice");
}

#[test]
fn test_secondary_index_non_unique() {
    let (_dir, db) = make_person_db();
    let city_idx = db.index::<CityIndex>("city");

    let mut txn = db.begin_transaction().unwrap();
    db.insert(&mut txn, 1, &Person { name: "Alice".into(), city: "London".into() }).unwrap();
    db.insert(&mut txn, 2, &Person { name: "Bob".into(), city: "London".into() }).unwrap();
    db.insert(&mut txn, 3, &Person { name: "Carol".into(), city: "Paris".into() }).unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin_transaction().unwrap();
    let mut results = city_idx.lookup(&mut txn, &"London".to_string()).unwrap();
    txn.commit().unwrap();

    results.sort_by_key(|(k, _)| *k);
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].0, 1);
    assert_eq!(results[1].0, 2);
}

#[test]
fn test_secondary_index_no_match_returns_empty() {
    let (_dir, db) = make_person_db();
    let city_idx = db.index::<CityIndex>("city");

    let mut txn = db.begin_transaction().unwrap();
    db.insert(&mut txn, 1, &Person { name: "Alice".into(), city: "London".into() }).unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin_transaction().unwrap();
    let results = city_idx.lookup(&mut txn, &"Tokyo".to_string()).unwrap();
    txn.commit().unwrap();

    assert!(results.is_empty());
}

#[test]
fn test_secondary_index_update_same_sk() {
    // Updating a record without changing the secondary key — bucket stays intact.
    let (_dir, db) = make_person_db();
    let city_idx = db.index::<CityIndex>("city");

    let mut txn = db.begin_transaction().unwrap();
    db.insert(&mut txn, 1, &Person { name: "Alice".into(), city: "London".into() }).unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin_transaction().unwrap();
    db.update(&mut txn, 1, &Person { name: "Alice Smith".into(), city: "London".into() }).unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin_transaction().unwrap();
    let results = city_idx.lookup(&mut txn, &"London".to_string()).unwrap();
    txn.commit().unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].1.name, "Alice Smith");
}

#[test]
fn test_secondary_index_update_changes_sk() {
    // Updating a record so its secondary key changes — old bucket loses the PK,
    // new bucket gains it.
    let (_dir, db) = make_person_db();
    let city_idx = db.index::<CityIndex>("city");

    let mut txn = db.begin_transaction().unwrap();
    db.insert(&mut txn, 1, &Person { name: "Alice".into(), city: "London".into() }).unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin_transaction().unwrap();
    db.update(&mut txn, 1, &Person { name: "Alice".into(), city: "Paris".into() }).unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin_transaction().unwrap();
    let london = city_idx.lookup(&mut txn, &"London".to_string()).unwrap();
    let paris = city_idx.lookup(&mut txn, &"Paris".to_string()).unwrap();
    txn.commit().unwrap();

    assert!(london.is_empty());
    assert_eq!(paris.len(), 1);
    assert_eq!(paris[0].0, 1);
}

#[test]
fn test_secondary_index_delete_removes_from_bucket() {
    let (_dir, db) = make_person_db();
    let city_idx = db.index::<CityIndex>("city");

    let mut txn = db.begin_transaction().unwrap();
    db.insert(&mut txn, 1, &Person { name: "Alice".into(), city: "London".into() }).unwrap();
    db.insert(&mut txn, 2, &Person { name: "Bob".into(), city: "London".into() }).unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin_transaction().unwrap();
    db.delete(&mut txn, &1).unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin_transaction().unwrap();
    let results = city_idx.lookup(&mut txn, &"London".to_string()).unwrap();
    txn.commit().unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, 2);
}

#[test]
fn test_secondary_index_rollback_insert() {
    // Rolling back an insert must also remove the PK from the secondary index.
    let (_dir, db) = make_person_db();
    let city_idx = db.index::<CityIndex>("city");

    let mut txn = db.begin_transaction().unwrap();
    db.insert(&mut txn, 1, &Person { name: "Alice".into(), city: "London".into() }).unwrap();
    txn.rollback().unwrap();

    let mut txn = db.begin_transaction().unwrap();
    let results = city_idx.lookup(&mut txn, &"London".to_string()).unwrap();
    txn.commit().unwrap();

    assert!(results.is_empty());
}

#[test]
fn test_secondary_index_rollback_update() {
    // Rolling back an update must restore the secondary index to its pre-update state.
    let (_dir, db) = make_person_db();
    let city_idx = db.index::<CityIndex>("city");

    let mut txn = db.begin_transaction().unwrap();
    db.insert(&mut txn, 1, &Person { name: "Alice".into(), city: "London".into() }).unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin_transaction().unwrap();
    db.update(&mut txn, 1, &Person { name: "Alice".into(), city: "Paris".into() }).unwrap();
    txn.rollback().unwrap();

    let mut txn = db.begin_transaction().unwrap();
    let london = city_idx.lookup(&mut txn, &"London".to_string()).unwrap();
    let paris = city_idx.lookup(&mut txn, &"Paris".to_string()).unwrap();
    txn.commit().unwrap();

    assert_eq!(london.len(), 1);
    assert!(paris.is_empty());
}

#[test]
fn test_secondary_index_rollback_delete() {
    // Rolling back a delete must re-add the PK to the secondary index.
    let (_dir, db) = make_person_db();
    let city_idx = db.index::<CityIndex>("city");

    let mut txn = db.begin_transaction().unwrap();
    db.insert(&mut txn, 1, &Person { name: "Alice".into(), city: "London".into() }).unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin_transaction().unwrap();
    db.delete(&mut txn, &1).unwrap();
    txn.rollback().unwrap();

    let mut txn = db.begin_transaction().unwrap();
    let results = city_idx.lookup(&mut txn, &"London".to_string()).unwrap();
    txn.commit().unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, 1);
}

#[test]
fn test_secondary_index_reopen() {
    // Secondary index survives a database close and reopen.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("people");

    {
        let db = Isam::<u32, Person>::builder()
            .with_index("city", CityIndex)
            .create(&path)
            .unwrap();
        let mut txn = db.begin_transaction().unwrap();
        db.insert(&mut txn, 1, &Person { name: "Alice".into(), city: "London".into() }).unwrap();
        txn.commit().unwrap();
    }

    let db = Isam::<u32, Person>::builder()
        .with_index("city", CityIndex)
        .open(&path)
        .unwrap();
    let city_idx = db.index::<CityIndex>("city");

    let mut txn = db.begin_transaction().unwrap();
    let results = city_idx.lookup(&mut txn, &"London".to_string()).unwrap();
    txn.commit().unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, 1);
}

#[test]
fn test_multiple_secondary_indices() {
    let (_dir, db) = make_person_db();
    let city_idx = db.index::<CityIndex>("city");
    let name_idx = db.index::<NameIndex>("name");

    let mut txn = db.begin_transaction().unwrap();
    db.insert(&mut txn, 1, &Person { name: "Alice".into(), city: "London".into() }).unwrap();
    db.insert(&mut txn, 2, &Person { name: "Bob".into(), city: "London".into() }).unwrap();
    txn.commit().unwrap();

    let mut txn = db.begin_transaction().unwrap();
    let by_city = city_idx.lookup(&mut txn, &"London".to_string()).unwrap();
    let by_name = name_idx.lookup(&mut txn, &"Alice".to_string()).unwrap();
    txn.commit().unwrap();

    assert_eq!(by_city.len(), 2);
    assert_eq!(by_name.len(), 1);
    assert_eq!(by_name[0].0, 1);
}

// ── write() / read() helpers ───────────────────────────────────────────────── //

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

    // Second insert of key 1 is a duplicate — should roll back the whole closure.
    let result = db.write(|txn| {
        db.insert(txn, 2, &"two".to_string())?;
        db.insert(txn, 1, &"duplicate".to_string())?; // errors here
        Ok(())
    });

    assert!(matches!(result, Err(IsamError::DuplicateKey)));
    // Key 2 must not have been committed.
    assert_eq!(db.read(|txn| db.get(txn, &2)).unwrap(), None);
}

#[test]
fn test_write_returns_closure_value() {
    let (_dir, db): (_, Isam<u32, String>) = make_db();

    // write() forwards the closure's return value to the caller.
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

    // read() forwards the closure's return value to the caller.
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

// ── secondary_indices() listing ────────────────────────────────────────────── //

#[test]
fn test_secondary_indices_listing() {
    let (_dir, db) = make_person_db();

    let info = db.secondary_indices().unwrap();
    assert_eq!(info.len(), 2);

    let names: Vec<&str> = info.iter().map(|i| i.name.as_str()).collect();
    assert!(names.contains(&"city"));
    assert!(names.contains(&"name"));

    let city = info.iter().find(|i| i.name == "city").unwrap();
    assert!(city.extractor_type.contains("CityIndex"));

    let name_idx = info.iter().find(|i| i.name == "name").unwrap();
    assert!(name_idx.extractor_type.contains("NameIndex"));
}

#[test]
fn test_secondary_indices_listing_empty() {
    let (_dir, db): (_, Isam<u32, String>) = make_db();
    let info = db.secondary_indices().unwrap();
    assert!(info.is_empty());
}

// ── rebuild_index ─────────────────────────────────────────────────────────── //

#[test]
fn test_rebuild_index_restores_correct_results() {
    // Create a database, insert records, then reopen with rebuild_index and
    // verify the index produces correct lookup results.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("people");

    {
        let db = Isam::<u32, Person>::builder()
            .with_index("city", CityIndex)
            .create(&path)
            .unwrap();
        db.write(|txn| db.insert(txn, 1, &Person { name: "Alice".into(), city: "London".into() })).unwrap();
        db.write(|txn| db.insert(txn, 2, &Person { name: "Bob".into(),   city: "London".into() })).unwrap();
        db.write(|txn| db.insert(txn, 3, &Person { name: "Carol".into(), city: "Paris".into()  })).unwrap();
    }

    let db = Isam::<u32, Person>::builder()
        .with_index("city", CityIndex)
        .rebuild_index("city")
        .open(&path)
        .unwrap();
    let city_idx = db.index::<CityIndex>("city");

    let mut results = db.read(|txn| city_idx.lookup(txn, &"London".to_string())).unwrap();
    results.sort_by_key(|(k, _)| *k);
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].0, 1);
    assert_eq!(results[1].0, 2);

    let paris = db.read(|txn| city_idx.lookup(txn, &"Paris".to_string())).unwrap();
    assert_eq!(paris.len(), 1);
    assert_eq!(paris[0].0, 3);
}

#[test]
fn test_rebuild_index_selective() {
    // Only the city index is rebuilt; the name index is reopened normally.
    // Both should produce correct results afterward.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("people");

    {
        let db = Isam::<u32, Person>::builder()
            .with_index("city", CityIndex)
            .with_index("name", NameIndex)
            .create(&path)
            .unwrap();
        db.write(|txn| db.insert(txn, 1, &Person { name: "Alice".into(), city: "London".into() })).unwrap();
        db.write(|txn| db.insert(txn, 2, &Person { name: "Bob".into(),   city: "Paris".into()  })).unwrap();
    }

    let db = Isam::<u32, Person>::builder()
        .with_index("city", CityIndex)
        .with_index("name", NameIndex)
        .rebuild_index("city")   // only rebuild city; name reopened as-is
        .open(&path)
        .unwrap();

    let city_idx = db.index::<CityIndex>("city");
    let name_idx = db.index::<NameIndex>("name");

    let london = db.read(|txn| city_idx.lookup(txn, &"London".to_string())).unwrap();
    assert_eq!(london.len(), 1);
    assert_eq!(london[0].0, 1);

    let alice = db.read(|txn| name_idx.lookup(txn, &"Alice".to_string())).unwrap();
    assert_eq!(alice.len(), 1);
    assert_eq!(alice[0].0, 1);
}

#[test]
fn test_rebuild_index_empty_database() {
    // Rebuild on an empty database should succeed and produce an empty index.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("people");

    {
        Isam::<u32, Person>::builder()
            .with_index("city", CityIndex)
            .create(&path)
            .unwrap();
    }

    let db = Isam::<u32, Person>::builder()
        .with_index("city", CityIndex)
        .rebuild_index("city")
        .open(&path)
        .unwrap();
    let city_idx = db.index::<CityIndex>("city");

    let results = db.read(|txn| city_idx.lookup(txn, &"London".to_string())).unwrap();
    assert!(results.is_empty());
}
