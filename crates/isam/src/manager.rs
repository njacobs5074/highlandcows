/// `TransactionManager<K, V>` — owns shared storage and creates transactions.
///
/// This type is `pub(crate)` — callers interact with it only through `Isam`.
/// It is `Clone` (via `Arc::clone`) so that `Isam` can be cloned freely.
use std::ops::{Deref, DerefMut};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::ThreadId;
use std::time::{Duration, Instant};

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::error::{IsamError, IsamResult};
use crate::storage::IsamStorage;
use crate::transaction::Transaction;

// ── Per-thread tag ───────────────────────────────────────────────────────── //

// `ThreadId` has no stable way to convert to an integer (`as_u64` is still
// gated behind the unstable `thread_id_value` feature), so `txn_owner` can't
// be a lock-free `AtomicU64` storing a `ThreadId` directly. Instead, assign
// each thread its own small `u64` tag the first time it checks, from a single
// global counter. Tags are unique for the life of the process (the counter
// never resets or recycles), so comparing tags is equivalent to comparing
// `ThreadId`s for this purpose. `0` is reserved to mean "no owner".
static NEXT_THREAD_TAG: AtomicU64 = AtomicU64::new(1);

thread_local! {
    static THREAD_TAG: u64 = NEXT_THREAD_TAG.fetch_add(1, Ordering::Relaxed);
}

fn current_thread_tag() -> u64 {
    THREAD_TAG.with(|tag| *tag)
}

pub(crate) struct TransactionManager<K, V> {
    pub(crate) storage: Arc<Mutex<IsamStorage<K, V>>>,
    /// `true` while single-user mode is active.
    pub(crate) single_user_active: Arc<AtomicBool>,
    /// Thread that owns single-user mode, set when `single_user_active` is `true`.
    pub(crate) single_user_owner: Arc<Mutex<Option<ThreadId>>>,
    /// Tag (see `current_thread_tag`) of the thread that currently holds
    /// `storage`'s lock via a [`StorageGuard`], or `0` if none. Checked on
    /// every `lock_storage()` call, so it's a lock-free atomic rather than a
    /// `Mutex` like `single_user_owner` — that check is gated behind the rare
    /// `single_user_active` flag, but this one can't be gated behind anything
    /// similarly rare, since any thread may hold the lock at any time. Used
    /// only to detect same-thread reentrancy and fail fast with
    /// `WouldDeadlock`; it plays no role in actual mutual exclusion — the
    /// `Mutex` on `storage` still does that.
    pub(crate) txn_owner: Arc<AtomicU64>,
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
            txn_owner: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Acquire the storage lock, checking single-user mode and same-thread
    /// reentrancy first.
    ///
    /// Returns `Err(IsamError::SingleUserMode)` if single-user mode is active
    /// and the calling thread is not the owner, or `Err(IsamError::WouldDeadlock)`
    /// if the calling thread already holds the lock — in which case blocking on
    /// `storage.lock()` would hang forever rather than eventually succeeding.
    ///
    /// Every caller gets the returned [`StorageGuard`], not a bare `MutexGuard`:
    /// it records this thread as the owner for as long as it's held, so that
    /// reentrancy is caught no matter *how* the lock is being held when the
    /// reentrant call happens — via an open `Transaction`, or via one of the
    /// admin methods (`compact`, `migrate_values`, `migrate_keys`,
    /// `migrate_index`) that invoke a caller-supplied closure while holding
    /// this same guard.
    pub(crate) fn lock_storage(&self) -> IsamResult<StorageGuard<'_, K, V>> {
        if self.single_user_active.load(Ordering::Acquire) {
            let owner = self
                .single_user_owner
                .lock()
                .map_err(|_| IsamError::LockPoisoned)?;
            if *owner != Some(std::thread::current().id()) {
                return Err(IsamError::SingleUserMode);
            }
        }

        if self.txn_owner.load(Ordering::Acquire) == current_thread_tag() {
            return Err(IsamError::WouldDeadlock);
        }

        let guard = self.storage.lock().map_err(|_| IsamError::LockPoisoned)?;
        self.txn_owner.store(current_thread_tag(), Ordering::Release);
        Ok(StorageGuard {
            guard,
            owner: Arc::clone(&self.txn_owner),
        })
    }

    pub(crate) fn begin(&self) -> IsamResult<Transaction<'_, K, V>> {
        let guard = self.lock_storage()?;
        Ok(Transaction::new(guard))
    }
}

// ── StorageGuard ─────────────────────────────────────────────────────────── //

/// RAII wrapper around the real storage lock, returned by [`TransactionManager::lock_storage`].
///
/// Derefs transparently to `IsamStorage<K, V>`, exactly like the `MutexGuard`
/// it wraps. The only extra behavior is clearing `txn_owner` on drop, which it
/// does *before* the inner `MutexGuard` is released (custom `Drop::drop` runs
/// before a struct's fields are automatically dropped, and `guard` is declared
/// after `owner` is cleared), so there is no window where another thread could
/// already be the new owner when this runs.
pub(crate) struct StorageGuard<'a, K, V> {
    guard: MutexGuard<'a, IsamStorage<K, V>>,
    owner: Arc<AtomicU64>,
}

impl<'a, K, V> Deref for StorageGuard<'a, K, V> {
    type Target = IsamStorage<K, V>;
    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl<'a, K, V> DerefMut for StorageGuard<'a, K, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}

impl<'a, K, V> Drop for StorageGuard<'a, K, V> {
    fn drop(&mut self) {
        self.owner.store(0, Ordering::Release);
    }
}

impl<K, V> Clone for TransactionManager<K, V> {
    fn clone(&self) -> Self {
        Self {
            storage: Arc::clone(&self.storage),
            single_user_active: Arc::clone(&self.single_user_active),
            single_user_owner: Arc::clone(&self.single_user_owner),
            txn_owner: Arc::clone(&self.txn_owner),
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
    /// Returns, in priority order:
    /// - `Err(IsamError::SingleUserMode)` if another thread already holds single-user
    ///   mode. Checked first: it's the more fundamental obstacle of the two — it can't
    ///   be resolved by anything the calling thread does, whereas `WouldDeadlock` can
    ///   (by releasing its own transaction).
    /// - `Err(IsamError::WouldDeadlock)` if, having claimed single-user mode, the
    ///   calling thread turns out to already hold an open `Transaction` itself —
    ///   waiting would never succeed, since nothing else can release a lock this
    ///   thread itself holds.
    /// - `Err(IsamError::Timeout)` if an in-flight transaction (on another thread) did
    ///   not finish within `timeout`.
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

        // Having claimed single-user mode, fail fast rather than spin to a
        // timeout if the calling thread already holds the lock itself — no
        // amount of waiting will free it.
        if self.txn_owner.load(Ordering::Acquire) == current_thread_tag() {
            self.release_single_user_claim();
            return Err(IsamError::WouldDeadlock);
        }

        // Spin until no transaction holds the storage lock, or timeout expires.
        let deadline = Instant::now() + timeout;
        loop {
            if self.storage.try_lock().is_ok() {
                // Successfully acquired and immediately released — no active transaction.
                break;
            }
            if Instant::now() >= deadline {
                self.release_single_user_claim();
                return Err(IsamError::Timeout);
            }
            std::thread::sleep(Duration::from_millis(1));
        }

        Ok(SingleUserGuard {
            manager: self.clone(),
        })
    }

    /// Undo a single-user mode claim made earlier in `enter_single_user_mode`,
    /// for a path that fails after claiming it but before returning a
    /// `SingleUserGuard` to run it down on drop.
    fn release_single_user_claim(&self) {
        if let Ok(mut owner) = self.single_user_owner.lock() {
            *owner = None;
        }
        self.single_user_active.store(false, Ordering::Release);
    }
}
