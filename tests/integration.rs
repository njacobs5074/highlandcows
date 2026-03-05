/// Integration tests for rust-isam.
///
/// `tempfile::TempDir` creates a temporary directory that is automatically
/// deleted when the `TempDir` value is dropped (goes out of scope).
/// This keeps tests hermetic — no leftover files between runs.
use rust_isam::{IsamError, Isam};
use tempfile::TempDir;

/// Helper: open a fresh database in a temp dir.
fn make_db<K, V>() -> (TempDir, Isam<K, V>)
where
    K: serde::Serialize + serde::de::DeserializeOwned + Ord + Clone,
    V: serde::Serialize + serde::de::DeserializeOwned,
{
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test");
    let db = Isam::create(&path).unwrap();
    (dir, db) // return dir so it isn't dropped (and deleted) early
}

// ── Basic CRUD ─────────────────────────────────────────────────────────── //

#[test]
fn test_insert_and_get() {
    let (_dir, mut db): (_, Isam<u32, String>) = make_db();
    db.insert(1, &"hello".to_string()).unwrap();
    assert_eq!(db.get(&1).unwrap(), Some("hello".to_string()));
}

#[test]
fn test_update() {
    let (_dir, mut db): (_, Isam<u32, String>) = make_db();
    db.insert(42, &"old".to_string()).unwrap();
    db.update(42, &"new".to_string()).unwrap();
    assert_eq!(db.get(&42).unwrap(), Some("new".to_string()));
}

#[test]
fn test_delete() {
    let (_dir, mut db): (_, Isam<u32, String>) = make_db();
    db.insert(7, &"bye".to_string()).unwrap();
    db.delete(&7).unwrap();
    assert_eq!(db.get(&7).unwrap(), None);
}

// ── Edge cases ─────────────────────────────────────────────────────────── //

#[test]
fn test_get_missing_key_returns_none() {
    let (_dir, mut db): (_, Isam<u32, String>) = make_db();
    db.insert(1, &"one".to_string()).unwrap();
    db.insert(2, &"two".to_string()).unwrap();
    db.insert(3, &"three".to_string()).unwrap();
    assert_eq!(db.get(&999).unwrap(), None);
}

#[test]
fn test_update_missing_key_returns_err() {
    let (_dir, mut db): (_, Isam<u32, String>) = make_db();
    db.insert(1, &"one".to_string()).unwrap();
    db.insert(2, &"two".to_string()).unwrap();
    db.insert(3, &"three".to_string()).unwrap();
    let err = db.update(999, &"x".to_string()).unwrap_err();
    assert!(matches!(err, IsamError::KeyNotFound));
}

#[test]
fn test_insert_duplicate_key_returns_err() {
    let (_dir, mut db): (_, Isam<u32, String>) = make_db();
    db.insert(1, &"first".to_string()).unwrap();
    let err = db.insert(1, &"second".to_string()).unwrap_err();
    assert!(matches!(err, IsamError::DuplicateKey));
}

// ── Sequential access ──────────────────────────────────────────────────── //

#[test]
fn test_iter_returns_keys_in_sorted_order() {
    let (_dir, mut db): (_, Isam<u32, String>) = make_db();
    // Insert out of order intentionally.
    for k in [5u32, 2, 8, 1, 9, 3, 7, 4, 6] {
        db.insert(k, &k.to_string()).unwrap();
    }

    let pairs: Vec<(u32, String)> = db
        .iter()
        .unwrap()
        .map(|r| r.unwrap())
        .collect();

    let keys: Vec<u32> = pairs.iter().map(|(k, _)| *k).collect();
    let mut sorted = keys.clone();
    sorted.sort();
    assert_eq!(keys, sorted);
    assert_eq!(keys, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
}

// ── B-tree structure ───────────────────────────────────────────────────── //

/// Force a page split by inserting more entries than fit in one 4096-byte page.
///
/// Each entry with a u32 key takes roughly 2 + 4 + 8 + 4 = 18 bytes in the
/// leaf page, so ~200 entries should be enough to exceed the page and trigger
/// a split.
#[test]
fn test_btree_forces_page_split() {
    let (_dir, mut db): (_, Isam<u32, u32>) = make_db();
    for i in 0..300u32 {
        db.insert(i, &i).unwrap();
    }
    // Verify all records are still readable after splits.
    for i in 0..300u32 {
        assert_eq!(db.get(&i).unwrap(), Some(i));
    }
}

/// Insert many entries then delete most of them to trigger leaf merges.
#[test]
fn test_btree_forces_merge_after_delete() {
    let (_dir, mut db): (_, Isam<u32, u32>) = make_db();
    for i in 0..200u32 {
        db.insert(i, &i).unwrap();
    }
    // Delete all but a few.
    for i in 10..200u32 {
        db.delete(&i).unwrap();
    }
    // Remaining keys must still be accessible.
    for i in 0..10u32 {
        assert_eq!(db.get(&i).unwrap(), Some(i));
    }
    // Deleted keys must be gone.
    for i in 10..200u32 {
        assert_eq!(db.get(&i).unwrap(), None);
    }
}

// ── Compaction ─────────────────────────────────────────────────────────── //

#[test]
fn test_compact_removes_tombstones() {
    let (dir, mut db): (_, Isam<u32, String>) = make_db();

    for i in 0..20u32 {
        db.insert(i, &format!("v{i}")).unwrap();
    }
    for i in 0..10u32 {
        db.delete(&i).unwrap();
    }

    let idb_before = std::fs::metadata(dir.path().join("test.idb"))
        .unwrap()
        .len();

    db.compact().unwrap();

    let idb_after = std::fs::metadata(dir.path().join("test.idb"))
        .unwrap()
        .len();

    // The compacted file should be smaller (tombstones removed, stale copies gone).
    assert!(
        idb_after < idb_before,
        "expected idb_after ({idb_after}) < idb_before ({idb_before})"
    );
}

#[test]
fn test_compact_preserves_all_alive_records() {
    let (_dir, mut db): (_, Isam<u32, String>) = make_db();

    for i in 0..50u32 {
        db.insert(i, &format!("value_{i}")).unwrap();
    }
    for i in (0..50u32).step_by(2) {
        db.delete(&i).unwrap();
    }

    db.compact().unwrap();

    // Odd keys alive, even keys gone.
    for i in 0..50u32 {
        let got = db.get(&i).unwrap();
        if i % 2 == 0 {
            assert_eq!(got, None, "key {i} should be gone");
        } else {
            assert_eq!(got, Some(format!("value_{i}")), "key {i} should survive");
        }
    }
}

// ── Byte array keys and values ─────────────────────────────────────────── //

#[test]
fn test_byte_array_insert_and_get() {
    let (_dir, mut db): (_, Isam<Vec<u8>, Vec<u8>>) = make_db();
    let key = b"hello".to_vec();
    let val = b"world".to_vec();
    db.insert(key.clone(), &val).unwrap();
    assert_eq!(db.get(&key).unwrap(), Some(val));
}

#[test]
fn test_byte_array_update() {
    let (_dir, mut db): (_, Isam<Vec<u8>, Vec<u8>>) = make_db();
    let key = b"key".to_vec();
    db.insert(key.clone(), &b"old".to_vec()).unwrap();
    db.update(key.clone(), &b"new".to_vec()).unwrap();
    assert_eq!(db.get(&key).unwrap(), Some(b"new".to_vec()));
}

#[test]
fn test_byte_array_delete() {
    let (_dir, mut db): (_, Isam<Vec<u8>, Vec<u8>>) = make_db();
    let key = b"gone".to_vec();
    db.insert(key.clone(), &b"value".to_vec()).unwrap();
    db.delete(&key).unwrap();
    assert_eq!(db.get(&key).unwrap(), None);
}

#[test]
fn test_byte_array_iter_sorted_order() {
    let (_dir, mut db): (_, Isam<Vec<u8>, Vec<u8>>) = make_db();
    // Insert several byte-array keys out of lexicographic order.
    let entries: Vec<(&[u8], &[u8])> = vec![
        (b"banana", b"2"),
        (b"apple", b"1"),
        (b"cherry", b"3"),
    ];
    for (k, v) in &entries {
        db.insert(k.to_vec(), &v.to_vec()).unwrap();
    }

    let pairs: Vec<(Vec<u8>, Vec<u8>)> = db
        .iter()
        .unwrap()
        .map(|r| r.unwrap())
        .collect();

    let keys: Vec<Vec<u8>> = pairs.into_iter().map(|(k, _)| k).collect();
    assert_eq!(
        keys,
        vec![b"apple".to_vec(), b"banana".to_vec(), b"cherry".to_vec()]
    );
}

// ── Persistence ────────────────────────────────────────────────────────── //

#[test]
fn test_data_survives_reopen() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("persist");

    {
        let mut db: Isam<String, u64> = Isam::create(&path).unwrap();
        db.insert("alpha".to_string(), &100).unwrap();
        db.insert("beta".to_string(), &200).unwrap();
        db.insert("gamma".to_string(), &300).unwrap();
        // db is dropped here, closing the files.
    }

    {
        let mut db: Isam<String, u64> = Isam::open(&path).unwrap();
        assert_eq!(db.get(&"alpha".to_string()).unwrap(), Some(100));
        assert_eq!(db.get(&"beta".to_string()).unwrap(), Some(200));
        assert_eq!(db.get(&"gamma".to_string()).unwrap(), Some(300));
        assert_eq!(db.get(&"delta".to_string()).unwrap(), None);
    }
}
