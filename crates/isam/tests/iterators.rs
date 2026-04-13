mod common;
use common::make_db;
use highlandcows_isam::{Isam, Transaction};

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

fn range_keys(
    db: &Isam<u32, u32>,
    txn: &mut Transaction<'_, u32, u32>,
    bounds: impl std::ops::RangeBounds<u32>,
) -> Vec<u32> {
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
