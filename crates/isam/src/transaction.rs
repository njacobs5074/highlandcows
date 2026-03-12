/// `Transaction<'a, K, V>` — a bounded execution context for ISAM operations.
///
/// Holds the `MutexGuard` for its entire lifetime (serializable isolation) and
/// an undo log so that a rollback (explicit or via `Drop`) can restore the
/// index to the state it had before the transaction began.
use std::sync::MutexGuard;

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::error::IsamResult;
use crate::storage::IsamStorage;
use crate::store::RecordRef;

// ── Undo log entry ───────────────────────────────────────────────────────── //

pub(crate) enum UndoEntry<K> {
    /// Undo an insert by deleting the key from the index.
    Insert { key: K },
    /// Undo an update by restoring the old RecordRef.
    Update { key: K, old_rec: RecordRef },
    /// Undo a delete by re-inserting the key with its old RecordRef.
    Delete { key: K, old_rec: RecordRef },
}

// ── Transaction ──────────────────────────────────────────────────────────── //

pub struct Transaction<'a, K, V>
where
    K: Serialize + DeserializeOwned + Ord + Clone,
    V: Serialize + DeserializeOwned,
{
    guard: MutexGuard<'a, IsamStorage<K, V>>,
    undo_log: Vec<UndoEntry<K>>,
    committed: bool,
}

impl<'a, K, V> Transaction<'a, K, V>
where
    K: Serialize + DeserializeOwned + Ord + Clone,
    V: Serialize + DeserializeOwned,
{
    pub(crate) fn new(guard: MutexGuard<'a, IsamStorage<K, V>>) -> Self {
        Self {
            guard,
            undo_log: Vec::new(),
            committed: false,
        }
    }

    // ── pub(crate) interface consumed by Isam ──────────────────────────── //

    pub(crate) fn storage_mut(&mut self) -> &mut IsamStorage<K, V> {
        &mut self.guard
    }

    pub(crate) fn log_insert(&mut self, key: K) {
        self.undo_log.push(UndoEntry::Insert { key });
    }

    pub(crate) fn log_update(&mut self, key: K, old_rec: RecordRef) {
        self.undo_log.push(UndoEntry::Update { key, old_rec });
    }

    pub(crate) fn log_delete(&mut self, key: K, old_rec: RecordRef) {
        self.undo_log.push(UndoEntry::Delete { key, old_rec });
    }

    // ── Public interface consumed by callers ───────────────────────────── //

    /// Flush store + index to disk and release the lock.
    pub fn commit(mut self) -> IsamResult<()> {
        self.guard.fsync()?;
        self.committed = true;
        Ok(())
        // MutexGuard drops here, releasing the lock
    }

    /// Apply undo log in reverse and release the lock.
    pub fn rollback(mut self) -> IsamResult<()> {
        self.do_rollback()?;
        self.committed = true;
        Ok(())
    }

    // ── Private helpers ────────────────────────────────────────────────── //

    fn do_rollback(&mut self) -> IsamResult<()> {
        // Apply in reverse order
        while let Some(entry) = self.undo_log.pop() {
            match entry {
                UndoEntry::Insert { key } => {
                    let _ = self.guard.index.delete(&key);
                }
                UndoEntry::Update { key, old_rec } => {
                    let _ = self.guard.index.update(&key, old_rec);
                }
                UndoEntry::Delete { key, old_rec } => {
                    let _ = self.guard.index.insert(&key, old_rec);
                }
            }
        }
        Ok(())
    }
}

impl<'a, K, V> Drop for Transaction<'a, K, V>
where
    K: Serialize + DeserializeOwned + Ord + Clone,
    V: Serialize + DeserializeOwned,
{
    fn drop(&mut self) {
        if !self.committed {
            let _ = self.do_rollback();
        }
    }
}
