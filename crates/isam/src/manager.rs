/// `TransactionManager<K, V>` — owns shared storage and creates transactions.
///
/// This type is `pub(crate)` — callers interact with it only through `Isam`.
/// It is `Clone` (via `Arc::clone`) so that `Isam` can be cloned freely.
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::ThreadId;
use std::time::{Duration, Instant};

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::error::{IsamError, IsamResult};
use crate::storage::IsamStorage;
use crate::transaction::Transaction;

pub(crate) struct TransactionManager<K, V> {
    pub(crate) storage: Arc<Mutex<IsamStorage<K, V>>>,
    /// `true` while single-user mode is active.
    pub(crate) single_user_active: Arc<AtomicBool>,
    /// Thread that owns single-user mode, set when `single_user_active` is `true`.
    pub(crate) single_user_owner: Arc<Mutex<Option<ThreadId>>>,
}

impl<K, V> TransactionManager<K, V>
where
    K: Serialize + DeserializeOwned + Ord + Clone,
    V: Serialize + DeserializeOwned,
{
    pub(crate) fn create(path: &Path) -> IsamResult<Self> {
        let storage = IsamStorage::create(path)?;
        Ok(Self::from_storage(storage))
    }

    pub(crate) fn open(path: &Path) -> IsamResult<Self> {
        let storage = IsamStorage::open(path)?;
        Ok(Self::from_storage(storage))
    }

    pub(crate) fn from_storage(storage: IsamStorage<K, V>) -> Self {
        Self {
            storage: Arc::new(Mutex::new(storage)),
            single_user_active: Arc::new(AtomicBool::new(false)),
            single_user_owner: Arc::new(Mutex::new(None)),
        }
    }

    /// Acquire the storage lock, checking single-user mode first.
    ///
    /// Returns `Err(IsamError::SingleUserMode)` if single-user mode is active
    /// and the calling thread is not the owner.
    pub(crate) fn lock_storage(&self) -> IsamResult<std::sync::MutexGuard<'_, IsamStorage<K, V>>> {
        if self.single_user_active.load(Ordering::Acquire) {
            let owner = self
                .single_user_owner
                .lock()
                .map_err(|_| IsamError::LockPoisoned)?;
            if *owner != Some(std::thread::current().id()) {
                return Err(IsamError::SingleUserMode);
            }
        }
        self.storage.lock().map_err(|_| IsamError::LockPoisoned)
    }

    pub(crate) fn begin(&self) -> IsamResult<Transaction<'_, K, V>> {
        let guard = self.lock_storage()?;
        Ok(Transaction::new(guard))
    }
}

impl<K, V> Clone for TransactionManager<K, V> {
    fn clone(&self) -> Self {
        Self {
            storage: Arc::clone(&self.storage),
            single_user_active: Arc::clone(&self.single_user_active),
            single_user_owner: Arc::clone(&self.single_user_owner),
        }
    }
}

// ── SingleUserGuard ───────────────────────────────────────────────────────── //

/// RAII guard that holds single-user mode for the duration of its lifetime.
///
/// Created internally by [`Isam::as_single_user`]; not part of the public API.
/// Single-user mode is released when this guard is dropped, even on panic.
pub(crate) struct SingleUserGuard<K, V> {
    manager: TransactionManager<K, V>,
}

impl<K, V> Drop for SingleUserGuard<K, V> {
    fn drop(&mut self) {
        if let Ok(mut owner) = self.manager.single_user_owner.lock() {
            *owner = None;
        }
        self.manager
            .single_user_active
            .store(false, Ordering::Release);
    }
}

impl<K, V> TransactionManager<K, V> {
    /// Enter single-user mode and return a guard that exits it on drop.
    ///
    /// Sets the single-user flag immediately (so other threads start failing at
    /// once), then spins on `try_lock` until any in-flight transaction finishes
    /// or `timeout` expires.
    ///
    /// Returns:
    /// - `Err(IsamError::SingleUserMode)` if another thread already holds single-user mode.
    /// - `Err(IsamError::Timeout)` if an in-flight transaction did not finish within `timeout`.
    pub(crate) fn enter_single_user_mode(&self, timeout: Duration) -> IsamResult<SingleUserGuard<K, V>> {
        // Atomically claim single-user mode; fail fast if already active.
        self.single_user_active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| IsamError::SingleUserMode)?;

        {
            let mut owner = self
                .single_user_owner
                .lock()
                .map_err(|_| IsamError::LockPoisoned)?;
            *owner = Some(std::thread::current().id());
        }

        // Spin until no transaction holds the storage lock, or timeout expires.
        let deadline = Instant::now() + timeout;
        loop {
            if self.storage.try_lock().is_ok() {
                // Successfully acquired and immediately released — no active transaction.
                break;
            }
            if Instant::now() >= deadline {
                // Undo: clear state so other threads are unblocked.
                if let Ok(mut owner) = self.single_user_owner.lock() {
                    *owner = None;
                }
                self.single_user_active.store(false, Ordering::Release);
                return Err(IsamError::Timeout);
            }
            std::thread::sleep(Duration::from_millis(1));
        }

        Ok(SingleUserGuard {
            manager: self.clone(),
        })
    }
}
