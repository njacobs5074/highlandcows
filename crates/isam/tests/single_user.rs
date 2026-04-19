/// Integration tests for single-user mode (`Isam::as_single_user`).
mod common;

use std::sync::{Arc, Barrier};
use std::thread;

use highlandcows_isam::{IsamError, IsamResult};

// ── as_single_user basics ────────────────────────────────────────────────── //

#[test]
fn test_single_user_closure_runs_and_returns_value() {
    let (_dir, db) = common::make_db::<u32, String>();
    let result: IsamResult<u32> = db.as_single_user(|| Ok(42));
    assert_eq!(result.unwrap(), 42);
}

#[test]
fn test_single_user_closure_can_write_and_read() {
    let (_dir, db) = common::make_db::<u32, String>();

    db.as_single_user(|| {
        db.write(|txn| db.insert(txn, 1u32, &"hello".to_string()))?;
        let val = db.read(|txn| db.get(txn, &1u32))?;
        assert_eq!(val, Some("hello".to_string()));
        Ok(())
    })
    .unwrap();
}

#[test]
fn test_single_user_closure_propagates_error() {
    let (_dir, db) = common::make_db::<u32, String>();

    let result = db.as_single_user(|| -> IsamResult<()> {
        Err(IsamError::KeyNotFound)
    });

    assert!(matches!(result, Err(IsamError::KeyNotFound)));
}

#[test]
fn test_single_user_mode_released_after_closure() {
    let (_dir, db) = common::make_db::<u32, String>();

    // Enter and exit single-user mode via closure.
    db.as_single_user(|| Ok(())).unwrap();

    // Database is fully usable afterward — we can enter again.
    db.as_single_user(|| {
        db.write(|txn| db.insert(txn, 1u32, &"after".to_string()))
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

    db.as_single_user(|| db.compact()).unwrap();

    // Records 3 and 4 should still be present.
    let val = db.read(|txn| db.get(txn, &3u32)).unwrap();
    assert_eq!(val, Some("3".to_string()));
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

    let result = db.as_single_user(|| {
        // Signal the other thread to attempt its write.
        barrier.wait();
        // Give the other thread time to attempt the operation.
        thread::sleep(std::time::Duration::from_millis(50));
        Ok(())
    });
    assert!(result.is_ok());

    let thread_result = handle.join().unwrap();
    assert!(
        matches!(thread_result, Err(IsamError::SingleUserMode)),
        "expected SingleUserMode, got: {:?}",
        thread_result
    );
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
        assert!(matches!(blocked, Err(IsamError::SingleUserMode)));
        // Signal main thread that we have confirmed blockage.
        barrier_exit2.wait();
        // Wait until the main thread confirms the guard has been dropped.
        barrier_released2.wait();
        // Now single-user mode is definitely released — this must succeed.
        db2.write(|txn| db2.insert(txn, 1u32, &"from thread".to_string()))
    });

    db.as_single_user(|| {
        barrier_enter.wait();
        barrier_exit.wait();
        Ok(())
    })
    .unwrap();
    // Guard is dropped here — signal the other thread.
    barrier_released.wait();

    handle.join().unwrap().unwrap();

    let val = db.read(|txn| db.get(txn, &1u32)).unwrap();
    assert_eq!(val, Some("from thread".to_string()));
}

// ── re-entry ─────────────────────────────────────────────────────────────── //

#[test]
fn test_single_user_mode_not_reentrant() {
    let (_dir, db) = common::make_db::<u32, String>();

    let result = db.as_single_user(|| {
        // Attempting to enter single-user mode again from the same thread
        // (the owner) returns SingleUserMode — re-entry is not supported.
        db.as_single_user(|| Ok(()))
    });

    assert!(
        matches!(result, Err(IsamError::SingleUserMode)),
        "expected SingleUserMode on re-entry, got: {:?}",
        result
    );
}
