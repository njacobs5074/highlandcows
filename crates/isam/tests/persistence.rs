mod common;
use highlandcows_isam::Isam;
use tempfile::TempDir;

// ── Byte array keys and values ─────────────────────────────────────────── //

#[test]
fn test_byte_array_insert_and_get() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test");
    let db: Isam<Vec<u8>, Vec<u8>> = Isam::create(&path).unwrap();
    let mut txn = db.begin_transaction().unwrap();
    let key = b"hello".to_vec();
    let val = b"world".to_vec();
    db.insert(&mut txn, key.clone(), &val).unwrap();
    assert_eq!(db.get(&mut txn, &key).unwrap(), Some(val));
    txn.commit().unwrap();
}

#[test]
fn test_byte_array_update() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test");
    let db: Isam<Vec<u8>, Vec<u8>> = Isam::create(&path).unwrap();
    let mut txn = db.begin_transaction().unwrap();
    let key = b"key".to_vec();
    db.insert(&mut txn, key.clone(), &b"old".to_vec()).unwrap();
    db.update(&mut txn, key.clone(), &b"new".to_vec()).unwrap();
    assert_eq!(db.get(&mut txn, &key).unwrap(), Some(b"new".to_vec()));
    txn.commit().unwrap();
}

#[test]
fn test_byte_array_delete() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test");
    let db: Isam<Vec<u8>, Vec<u8>> = Isam::create(&path).unwrap();
    let mut txn = db.begin_transaction().unwrap();
    let key = b"gone".to_vec();
    db.insert(&mut txn, key.clone(), &b"value".to_vec()).unwrap();
    db.delete(&mut txn, &key).unwrap();
    assert_eq!(db.get(&mut txn, &key).unwrap(), None);
    txn.commit().unwrap();
}

#[test]
fn test_byte_array_iter_sorted_order() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test");
    let db: Isam<Vec<u8>, Vec<u8>> = Isam::create(&path).unwrap();
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

// ── Schema versioning / migration ─────────────────────────────────────── //

#[test]
fn test_schema_version_defaults_to_zero() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test");
    let db: Isam<u32, String> = Isam::create(&path).unwrap();
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
