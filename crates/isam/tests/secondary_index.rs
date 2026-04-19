mod common;
use common::{make_person_db, CityIndex, NameIndex, Person};
use highlandcows_isam::Isam;
use tempfile::TempDir;

// ── Basic lookup ───────────────────────────────────────────────────────── //

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

// ── Update / delete ────────────────────────────────────────────────────── //

#[test]
fn test_secondary_index_update_same_sk() {
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

// ── Rollback ───────────────────────────────────────────────────────────── //

#[test]
fn test_secondary_index_rollback_insert() {
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

// ── Reopen / multiple indices ──────────────────────────────────────────── //

#[test]
fn test_secondary_index_reopen() {
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

// ── secondary_indices() listing ────────────────────────────────────────── //

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
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test");
    let db: Isam<u32, String> = Isam::create(&path).unwrap();
    let info = db.secondary_indices().unwrap();
    assert!(info.is_empty());
}

// ── rebuild_index ─────────────────────────────────────────────────────── //

#[test]
fn test_rebuild_index_restores_correct_results() {
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
        .rebuild_index("city")
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

// ── migrate_index ─────────────────────────────────────────────────────── //

#[test]
fn test_migrate_index_identity_preserves_results() {
    let (_dir, db) = make_person_db();
    let city_idx = db.index::<CityIndex>("city");

    db.write(|txn| db.insert(txn, 1, &Person { name: "Alice".into(), city: "London".into() })).unwrap();
    db.write(|txn| db.insert(txn, 2, &Person { name: "Bob".into(),   city: "Paris".into()  })).unwrap();

    // Identity migration: f = |v| v — just a rebuild with a version bump.
    db.migrate_index("city", 1, |v| Ok(v)).unwrap();

    let mut london = db.read(|txn| city_idx.lookup(txn, &"London".to_string())).unwrap();
    london.sort_by_key(|(k, _)| *k);
    assert_eq!(london.len(), 1);
    assert_eq!(london[0].0, 1);

    let paris = db.read(|txn| city_idx.lookup(txn, &"Paris".to_string())).unwrap();
    assert_eq!(paris.len(), 1);
    assert_eq!(paris[0].0, 2);
}

#[test]
fn test_migrate_index_updates_schema_version() {
    let (_dir, db) = make_person_db();

    // Initial version should be 0.
    let info = db.secondary_indices().unwrap();
    let city = info.iter().find(|i| i.name == "city").unwrap();
    assert_eq!(city.schema_version, 0);

    db.migrate_index("city", 3, |v| Ok(v)).unwrap();

    let info = db.secondary_indices().unwrap();
    let city = info.iter().find(|i| i.name == "city").unwrap();
    assert_eq!(city.schema_version, 3);
}

#[test]
fn test_migrate_index_with_value_transformation() {
    // CityIndex::derive returns city as-is.
    // We migrate by lowercasing each person's city field so the index
    // entries match the updated derivation logic (which now lowercases).
    let (_dir, db) = make_person_db();
    let city_idx = db.index::<CityIndex>("city");

    db.write(|txn| db.insert(txn, 1, &Person { name: "Alice".into(), city: "London".into() })).unwrap();
    db.write(|txn| db.insert(txn, 2, &Person { name: "Bob".into(),   city: "Paris".into()  })).unwrap();

    // Transform: lowercase the city before DeriveKey::derive runs.
    db.migrate_index("city", 1, |mut p: Person| {
        p.city = p.city.to_lowercase();
        Ok(p)
    }).unwrap();

    // Old keys no longer match.
    let london_old = db.read(|txn| city_idx.lookup(txn, &"London".to_string())).unwrap();
    assert!(london_old.is_empty(), "old mixed-case key should be gone");

    // New lowercase keys should be present.
    let london_new = db.read(|txn| city_idx.lookup(txn, &"london".to_string())).unwrap();
    assert_eq!(london_new.len(), 1);
    assert_eq!(london_new[0].0, 1);
}

#[test]
fn test_migrate_index_does_not_affect_other_index() {
    let (_dir, db) = make_person_db();
    let name_idx = db.index::<NameIndex>("name");

    db.write(|txn| db.insert(txn, 1, &Person { name: "Alice".into(), city: "London".into() })).unwrap();

    // Migrate only "city"; "name" index should be untouched.
    db.migrate_index("city", 1, |v| Ok(v)).unwrap();

    let alice = db.read(|txn| name_idx.lookup(txn, &"Alice".to_string())).unwrap();
    assert_eq!(alice.len(), 1);
    assert_eq!(alice[0].0, 1);
}

#[test]
fn test_migrate_index_does_not_modify_primary_records() {
    let (_dir, db) = make_person_db();

    db.write(|txn| db.insert(txn, 1, &Person { name: "Alice".into(), city: "London".into() })).unwrap();

    // Transform city to lowercase for index derivation only.
    db.migrate_index("city", 1, |mut p: Person| {
        p.city = p.city.to_lowercase();
        Ok(p)
    }).unwrap();

    // Primary record should still have original city casing.
    let person = db.read(|txn| db.get(txn, &1u32)).unwrap().unwrap();
    assert_eq!(person.city, "London", "primary record must not be modified");
}

#[test]
fn test_migrate_index_unknown_name_returns_error() {
    let (_dir, db) = make_person_db();
    let result = db.migrate_index("nonexistent", 1, |v: Person| Ok(v));
    assert!(result.is_err());
}

#[test]
fn test_migrate_index_schema_version_persists_across_reopen() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("people");

    {
        let db = Isam::<u32, Person>::builder()
            .with_index("city", CityIndex)
            .create(&path)
            .unwrap();
        db.write(|txn| db.insert(txn, 1, &Person { name: "Alice".into(), city: "London".into() })).unwrap();
        db.migrate_index("city", 2, |v| Ok(v)).unwrap();
    }

    let db = Isam::<u32, Person>::builder()
        .with_index("city", CityIndex)
        .open(&path)
        .unwrap();

    let info = db.secondary_indices().unwrap();
    let city = info.iter().find(|i| i.name == "city").unwrap();
    assert_eq!(city.schema_version, 2, "schema version must survive reopen");
}
