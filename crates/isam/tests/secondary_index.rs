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
