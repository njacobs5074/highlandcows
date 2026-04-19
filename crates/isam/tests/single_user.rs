/// Integration tests for single-user mode (`Isam::as_single_user`).
mod common;

use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use highlandcows_isam::{IsamError, IsamResult};

const TIMEOUT: Duration = Duration::from_secs(5);

// ── as_single_user basics ────────────────────────────────────────────────── //

#[test]
fn test_single_user_closure_runs_and_returns_value() {
    let (_dir, db) = common::make_db::<u32, String>();
    let result: IsamResult<u32> = db.as_single_user(TIMEOUT, || Ok(42));
    assert_eq!(result.unwrap(), 42);
}

#[test]
fn test_single_user_closure_can_write_and_read() {
    let (_dir, db) = common::make_db::<u32, String>();

    db.as_single_user(TIMEOUT, || {
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

    let result = db.as_single_user(TIMEOUT, || -> IsamResult<()> {
        Err(IsamError::KeyNotFound)
    });

    assert!(matches!(result, Err(IsamError::KeyNotFound)));
}

#[test]
fn test_single_user_mode_released_after_closure() {
    let (_dir, db) = common::make_db::<u32, String>();

    // Enter and exit single-user mode via closure.
    db.as_single_user(TIMEOUT, || Ok(())).unwrap();

    // Database is fully usable afterward — we can enter again.
    db.as_single_user(TIMEOUT, || {
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

    db.as_single_user(TIMEOUT, || db.compact()).unwrap();

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

    let result = db.as_single_user(TIMEOUT, || {
        // Signal the other thread to attempt its write.
        barrier.wait();
        // Give the other thread time to attempt the operation.
        thread::sleep(Duration::from_millis(50));
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

    db.as_single_user(TIMEOUT, || {
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

    // Try to enter single-user mode with a short timeout — must fail.
    let result = db.as_single_user(Duration::from_millis(50), || Ok(()));
    assert!(
        matches!(result, Err(IsamError::Timeout)),
        "expected Timeout, got: {:?}",
        result
    );

    // Release the other thread's transaction.
    barrier_release.wait();
    handle.join().unwrap();

    // After the transaction is gone, single-user mode should be acquirable again.
    db.as_single_user(TIMEOUT, || Ok(())).unwrap();
}

// ── re-entry ─────────────────────────────────────────────────────────────── //

#[test]
fn test_single_user_mode_not_reentrant() {
    let (_dir, db) = common::make_db::<u32, String>();

    let result = db.as_single_user(TIMEOUT, || {
        // Attempting to enter single-user mode again from the same thread
        // (the owner) returns SingleUserMode — re-entry is not supported.
        db.as_single_user(TIMEOUT, || Ok(()))
    });

    assert!(
        matches!(result, Err(IsamError::SingleUserMode)),
        "expected SingleUserMode on re-entry, got: {:?}",
        result
    );
}
