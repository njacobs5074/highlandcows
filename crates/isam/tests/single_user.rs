/// Integration tests for single-user mode (`Isam::as_single_user`).
mod common;

use std::assert_matches;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use highlandcows_isam::{IsamError, IsamResult};

const TIMEOUT: Duration = Duration::from_secs(5);

// ── as_single_user basics ────────────────────────────────────────────────── //

#[test]
fn test_single_user_closure_runs_and_returns_value() {
    let (_dir, db) = common::make_db::<u32, String>();
    let result: IsamResult<u32> = db.as_single_user(TIMEOUT, |_token, _db| Ok(42));
    assert_eq!(result.unwrap(), 42);
}

#[test]
fn test_single_user_closure_can_write_and_read() {
    let (_dir, db) = common::make_db::<u32, String>();

    db.as_single_user(TIMEOUT, |_token, db| {
        db.write(|txn| db.insert(txn, 1u32, &"hello".to_string()))?;
        let val = db.read(|txn| db.get(txn, &1u32))?;
        assert_eq!(val, Some("hello".to_string()));
        Ok(db)
    })
    .unwrap();
}

#[test]
fn test_single_user_closure_propagates_error() {
    let (_dir, db) = common::make_db::<u32, String>();

    let result = db.as_single_user(TIMEOUT, |_token, _db| -> IsamResult<()> {
        Err(IsamError::KeyNotFound)
    });

    assert_matches!(result, Err(IsamError::KeyNotFound));
}

#[test]
fn test_single_user_mode_released_after_closure() {
    let (_dir, db) = common::make_db::<u32, String>();

    // Enter and exit single-user mode via closure; rebind to keep the handle.
    let db = db.as_single_user(TIMEOUT, |_token, db| Ok(db)).unwrap();

    // Database is fully usable afterward — we can enter again.
    let db = db
        .as_single_user(TIMEOUT, |_token, db| {
            db.write(|txn| db.insert(txn, 1u32, &"after".to_string()))?;
            Ok(db)
        })
        .unwrap();

    let val = db.read(|txn| db.get(txn, &1u32)).unwrap();
    assert_eq!(val, Some("after".to_string()));
}

#[test]
fn test_single_user_wraps_compact() {
    let (_dir, db) = common::make_db::<u32, String>();

    db.write(|txn| {
        for i in 0u32..5 {
            db.insert(txn, i, &i.to_string())?;
        }
        Ok(())
    })
    .unwrap();
    db.write(|txn| {
        for i in 0u32..3 {
            db.delete(txn, &i)?;
        }
        Ok(())
    })
    .unwrap();

    let db = db
        .as_single_user(TIMEOUT, |token, db| {
            db.compact(token)?;
            Ok(db)
        })
        .unwrap();

    // Records 3 and 4 should still be present.
    let val = db.read(|txn| db.get(txn, &3u32)).unwrap();
    assert_eq!(val, Some("3".to_string()));
}

#[test]
fn test_default_timeout_constant_is_accessible() {
    use highlandcows_isam::DEFAULT_SINGLE_USER_TIMEOUT;
    // Smoke-test that the public constant compiles and has the expected value.
    assert_eq!(DEFAULT_SINGLE_USER_TIMEOUT, Duration::from_secs(30));
}

// ── cross-thread exclusion ────────────────────────────────────────────────── //

#[test]
fn test_other_thread_blocked_during_single_user_mode() {
    let (_dir, db) = common::make_db::<u32, String>();
    let db2 = db.clone();

    // Barrier: owner enters single-user mode, then signals the other thread.
    let barrier = Arc::new(Barrier::new(2));
    let barrier2 = Arc::clone(&barrier);

    let handle = thread::spawn(move || {
        // Wait until the main thread has entered single-user mode.
        barrier2.wait();
        // This must fail with SingleUserMode.
        db2.write(|txn| db2.insert(txn, 99u32, &"blocked".to_string()))
    });

    let result = db.as_single_user(TIMEOUT, |_token, _db| {
        // Signal the other thread to attempt its write.
        barrier.wait();
        // Give the other thread time to attempt the operation.
        thread::sleep(Duration::from_millis(50));
        Ok(())
    });
    assert!(result.is_ok());

    let thread_result = handle.join().unwrap();
    assert_matches!(thread_result, Err(IsamError::SingleUserMode));
}

#[test]
fn test_other_thread_can_operate_after_single_user_released() {
    let (_dir, db) = common::make_db::<u32, String>();
    let db2 = db.clone();

    let barrier_enter = Arc::new(Barrier::new(2));
    let barrier_exit = Arc::new(Barrier::new(2));
    // Third barrier: main thread signals after the guard is dropped.
    let barrier_released = Arc::new(Barrier::new(2));
    let barrier_enter2 = Arc::clone(&barrier_enter);
    let barrier_exit2 = Arc::clone(&barrier_exit);
    let barrier_released2 = Arc::clone(&barrier_released);

    let handle = thread::spawn(move || {
        // Wait for single-user mode to be active.
        barrier_enter2.wait();
        // Confirm we are blocked.
        let blocked = db2.write(|txn| db2.insert(txn, 1u32, &"from thread".to_string()));
        assert_matches!(blocked, Err(IsamError::SingleUserMode));
        // Signal main thread that we have confirmed blockage.
        barrier_exit2.wait();
        // Wait until the main thread confirms the guard has been dropped.
        barrier_released2.wait();
        // Now single-user mode is definitely released — this must succeed.
        db2.write(|txn| db2.insert(txn, 1u32, &"from thread".to_string()))
    });

    let db = db
        .as_single_user(TIMEOUT, |_token, db| {
            barrier_enter.wait();
            barrier_exit.wait();
            Ok(db)
        })
        .unwrap();
    // Guard is dropped here — signal the other thread.
    barrier_released.wait();

    handle.join().unwrap().unwrap();

    let val = db.read(|txn| db.get(txn, &1u32)).unwrap();
    assert_eq!(val, Some("from thread".to_string()));
}

// ── timeout ───────────────────────────────────────────────────────────────── //

#[test]
fn test_single_user_timeout_if_transaction_held() {
    let (_dir, db) = common::make_db::<u32, String>();
    let db2 = db.clone();

    // barrier_txn_held: other thread signals when its transaction is live.
    let barrier_txn_held = Arc::new(Barrier::new(2));
    // barrier_release: main thread signals when it's done asserting the timeout.
    let barrier_release = Arc::new(Barrier::new(2));
    let barrier_txn_held2 = Arc::clone(&barrier_txn_held);
    let barrier_release2 = Arc::clone(&barrier_release);

    let handle = thread::spawn(move || {
        // Begin a transaction and hold it open.
        let txn = db2.begin_transaction().unwrap();
        // Signal: transaction is live.
        barrier_txn_held2.wait();
        // Hold until the main thread says it's done.
        barrier_release2.wait();
        txn.commit().unwrap();
    });

    // Wait for the other thread's transaction to be live.
    barrier_txn_held.wait();

    // Clone before the failing call so we have a handle for the retry.
    let db_retry = db.clone();

    // Try to enter single-user mode with a short timeout — must fail.
    let result = db.as_single_user(Duration::from_millis(50), |_token, _db| Ok(()));
    assert_matches!(result, Err(IsamError::Timeout));

    // Release the other thread's transaction.
    barrier_release.wait();
    handle.join().unwrap();

    // After the transaction is gone, single-user mode should be acquirable again.
    db_retry.as_single_user(TIMEOUT, |_token, _db| Ok(())).unwrap();
}

// ── re-entry ─────────────────────────────────────────────────────────────── //

#[test]
fn test_single_user_mode_not_reentrant() {
    let (_dir, db) = common::make_db::<u32, String>();

    let result = db.as_single_user(TIMEOUT, |_token, db| {
        // Attempting to enter single-user mode again from the same thread
        // (the owner) returns SingleUserMode — re-entry is not supported.
        db.as_single_user(TIMEOUT, |_token, _db| Ok(()))
    });

    assert_matches!(result, Err(IsamError::SingleUserMode));
}

#[test]
fn test_single_user_mode_error_takes_priority_over_would_deadlock() {
    // If a thread both holds its own open Transaction AND a different thread
    // already owns single-user mode, SingleUserMode must win: releasing the
    // caller's own transaction wouldn't be enough to make the call succeed,
    // since someone else's exclusive hold is the actual blocker.
    let (_dir, db) = common::make_db::<u32, String>();
    let db_a = db.clone();

    // Main thread opens (and holds) its own transaction first, so thread A's
    // single-user claim below can succeed (claiming the flag doesn't need
    // the storage lock), but its subsequent spin-wait can't — our txn is
    // still holding the real lock.
    let txn = db.begin_transaction().unwrap();

    let handle = thread::spawn(move || db_a.as_single_user(TIMEOUT, |_token, _db| Ok(())));

    // Wait until thread A has claimed single-user mode.  Until it does, our
    // own open transaction makes this call return `WouldDeadlock`.
    let deadline = Instant::now() + Duration::from_secs(2);
    while !matches!(db.key_schema_version(), Err(IsamError::SingleUserMode)) {
        assert!(Instant::now() < deadline, "thread A never claimed single-user mode");
        thread::yield_now();
    }

    // Even though the main thread holds its own open transaction, thread A
    // already owns single-user mode — that must be reported, not WouldDeadlock.
    let result = db.clone().as_single_user(TIMEOUT, |_token, _db| Ok(()));
    assert_matches!(result, Err(IsamError::SingleUserMode));

    // Release our transaction so thread A's spin succeeds.
    txn.commit().unwrap();
    handle.join().unwrap().unwrap();
}

// ── same-thread reentrancy / deadlock detection ──────────────────────────── //

#[test]
fn test_schema_version_while_txn_open_returns_would_deadlock() {
    let (_dir, db) = common::make_db::<u32, String>();

    // Run on a separate thread and hand results back over a channel with a
    // bounded `recv_timeout` — if the fix regresses, this call would block
    // forever on the same thread's own lock, and we want the test to fail
    // fast rather than hang the suite.
    let (tx, rx) = std::sync::mpsc::channel();
    thread::spawn(move || {
        let _txn = db.begin_transaction().unwrap();
        let key_result = db.key_schema_version();
        let val_result = db.val_schema_version();
        tx.send((key_result, val_result)).unwrap();
    });

    let (key_result, val_result) = rx
        .recv_timeout(Duration::from_secs(2))
        .expect("call hung instead of failing fast with WouldDeadlock");
    assert_matches!(key_result, Err(IsamError::WouldDeadlock));
    assert_matches!(val_result, Err(IsamError::WouldDeadlock));
}

#[test]
fn test_nested_begin_transaction_returns_would_deadlock() {
    let (_dir, db) = common::make_db::<u32, String>();

    let (tx, rx) = std::sync::mpsc::channel();
    thread::spawn(move || {
        let _txn = db.begin_transaction().unwrap();
        // Second begin_transaction on the same thread, before the first
        // commits or drops, would deadlock on the bare Mutex without the fix.
        let result = db.begin_transaction().map(|_| ());
        tx.send(result).unwrap();
    });

    let result = rx
        .recv_timeout(Duration::from_secs(2))
        .expect("nested begin_transaction hung instead of failing fast");
    assert_matches!(result, Err(IsamError::WouldDeadlock));
}

#[test]
fn test_admin_method_while_txn_open_returns_would_deadlock() {
    let (_dir, db) = common::make_db::<u32, String>();

    let (tx, rx) = std::sync::mpsc::channel();
    thread::spawn(move || {
        let result = db.as_single_user(TIMEOUT, |token, db| {
            let _txn = db.begin_transaction()?;
            db.compact(token)
        });
        tx.send(result).unwrap();
    });

    let result = rx
        .recv_timeout(Duration::from_secs(2))
        .expect("compact() hung instead of failing fast with WouldDeadlock");
    assert_matches!(result, Err(IsamError::WouldDeadlock));
}

#[test]
fn test_reentrancy_from_inside_migration_closure_returns_would_deadlock() {
    // The previous three tests cover "a Transaction is open, then an admin
    // method is called" — this covers the opposite direction: an admin
    // method (migrate_index) is itself holding the lock, and a closure it
    // invokes re-enters the database on the same thread via a captured
    // clone. Both directions must be caught, or this is only half a fix.
    let (_dir, db) = common::make_person_db();

    db.write(|txn| db.insert(txn, 1u32, &common::Person { name: "Alice".into(), city: "London".into() }))
        .unwrap();

    let db2 = db.clone();

    let (tx, rx) = std::sync::mpsc::channel();
    thread::spawn(move || {
        let result = db.as_single_user(TIMEOUT, |token, db| {
            db.migrate_index(
                "city",
                1,
                move |v| {
                    let reentrant_result = db2.key_schema_version();
                    tx.send(reentrant_result).unwrap();
                    Ok(v)
                },
                token,
            )
        });
        result.unwrap();
    });

    let reentrant_result = rx
        .recv_timeout(Duration::from_secs(2))
        .expect("reentrant call from inside migrate_index's closure hung instead of failing fast");
    assert_matches!(reentrant_result, Err(IsamError::WouldDeadlock));
}

#[test]
fn test_owner_cleared_after_commit() {
    let (_dir, db) = common::make_db::<u32, String>();

    let txn = db.begin_transaction().unwrap();
    txn.commit().unwrap();

    // If `commit` forgot to clear the reentrancy flag, this would wrongly
    // fail with WouldDeadlock even though the first transaction is gone.
    let txn2 = db.begin_transaction().unwrap();
    txn2.commit().unwrap();
}

#[test]
fn test_owner_cleared_after_rollback() {
    let (_dir, db) = common::make_db::<u32, String>();

    let txn = db.begin_transaction().unwrap();
    txn.rollback().unwrap();

    let txn2 = db.begin_transaction().unwrap();
    txn2.commit().unwrap();
}

#[test]
fn test_owner_cleared_after_drop() {
    let (_dir, db) = common::make_db::<u32, String>();

    {
        let _txn = db.begin_transaction().unwrap();
        // Dropped here without commit/rollback — exercises the Drop path.
    }

    let txn2 = db.begin_transaction().unwrap();
    txn2.commit().unwrap();
}

#[test]
fn test_owner_check_is_per_thread_not_global() {
    let (_dir, db) = common::make_db::<u32, String>();
    let db2 = db.clone();

    let barrier = Arc::new(Barrier::new(2));
    let barrier2 = Arc::clone(&barrier);

    let handle = thread::spawn(move || {
        let txn = db2.begin_transaction().unwrap();
        // Signal: thread A's transaction is live.
        barrier2.wait();
        thread::sleep(Duration::from_millis(50));
        txn.commit().unwrap();
    });

    // Wait for thread A's transaction to be live.
    barrier.wait();

    // Thread B is not the owner, so this is ordinary cross-thread lock
    // contention, not a self-deadlock — it must block and then succeed once
    // thread A commits, rather than being wrongly rejected as WouldDeadlock.
    let result = db.key_schema_version();
    assert_matches!(result, Ok(0));

    handle.join().unwrap();
}

#[test]
fn test_as_single_user_while_txn_open_returns_would_deadlock() {
    let (_dir, db) = common::make_db::<u32, String>();

    let (tx, rx) = std::sync::mpsc::channel();
    thread::spawn(move || {
        let txn = db.begin_transaction().unwrap();
        let result = db.clone().as_single_user(TIMEOUT, |_token, _db| Ok(()));
        txn.commit().unwrap();

        // The failed call must release its single-user claim: a different
        // thread can use the database again.
        let other = db.clone();
        let after = thread::spawn(move || other.key_schema_version()).join().unwrap();
        tx.send((result, after)).unwrap();
    });

    let (result, after) = rx
        .recv_timeout(Duration::from_secs(2))
        .expect("as_single_user spun instead of failing fast");
    assert_matches!(result, Err(IsamError::WouldDeadlock));
    assert_matches!(after, Ok(0));
}

#[test]
fn test_nested_read_and_write_return_would_deadlock() {
    let (_dir, db) = common::make_db::<u32, String>();

    let (tx, rx) = std::sync::mpsc::channel();
    thread::spawn(move || {
        let nested = db
            .write(|_txn| {
                let read = db.read(|inner| db.get(inner, &1));
                let write = db.write(|inner| db.insert(inner, 2, &"nested".to_string()));
                Ok((read, write))
            })
            .unwrap();
        tx.send(nested).unwrap();
    });

    let (read, write) = rx
        .recv_timeout(Duration::from_secs(2))
        .expect("nested call hung instead of failing fast");
    assert_matches!(read, Err(IsamError::WouldDeadlock));
    assert_matches!(write, Err(IsamError::WouldDeadlock));
}
