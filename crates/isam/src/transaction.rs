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

pub(crate) enum UndoEntry<K, V> {
    /// Undo an insert by deleting the key from the primary index and secondary indices.
    Insert { key: K, value: V },
    /// Undo an update by restoring the old RecordRef and reversing secondary index changes.
    Update { key: K, old_rec: RecordRef, old_value: V, new_value: V },
    /// Undo a delete by re-inserting the key with its old RecordRef and secondary indices.
    Delete { key: K, old_rec: RecordRef, value: V },
}

// ── Transaction ──────────────────────────────────────────────────────────── //

pub struct Transaction<'a, K, V>
where
    K: Serialize + DeserializeOwned + Ord + Clone,
    V: Serialize + DeserializeOwned,
{
    guard: MutexGuard<'a, IsamStorage<K, V>>,
    undo_log: Vec<UndoEntry<K, V>>,
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

    pub(crate) fn log_insert(&mut self, key: K, value: V) {
        self.undo_log.push(UndoEntry::Insert { key, value });
    }

    pub(crate) fn log_update(&mut self, key: K, old_rec: RecordRef, old_value: V, new_value: V) {
        self.undo_log.push(UndoEntry::Update { key, old_rec, old_value, new_value });
    }

    pub(crate) fn log_delete(&mut self, key: K, old_rec: RecordRef, value: V) {
        self.undo_log.push(UndoEntry::Delete { key, old_rec, value });
    }

    // ── Public interface consumed by callers ───────────────────────────── //

    /// Flush store and index to disk (`fsync`) and release the database lock.
    ///
    /// After `commit` returns the changes are durable and the lock is released,
    /// allowing other threads to begin their own transactions.
    ///
    /// # Example
    /// ```
    /// # use tempfile::TempDir;
    /// # use highlandcows_isam::Isam;
    /// # let dir = TempDir::new().unwrap();
    /// # let path = dir.path().join("db");
    /// # let db: Isam<u32, String> = Isam::create(&path).unwrap();
    /// let mut txn = db.begin_transaction().unwrap();
    /// db.insert(&mut txn, 1u32, &"hello".to_string()).unwrap();
    /// txn.commit().unwrap();
    ///
    /// // Data is now durable — visible after reopen.
    /// let db2: Isam<u32, String> = Isam::open(&path).unwrap();
    /// let mut txn2 = db2.begin_transaction().unwrap();
    /// assert_eq!(db2.get(&mut txn2, &1u32).unwrap(), Some("hello".to_string()));
    /// txn2.commit().unwrap();
    /// ```
    pub fn commit(mut self) -> IsamResult<()> {
        self.guard.fsync()?;
        self.committed = true;
        Ok(())
        // MutexGuard drops here, releasing the lock
    }

    /// Roll back all changes made in this transaction and release the lock.
    ///
    /// The undo log is applied in reverse order, restoring the index to the
    /// state it had before the transaction began.  Dropping a `Transaction`
    /// without calling `commit` has the same effect automatically.
    ///
    /// # Example
    /// ```
    /// # use tempfile::TempDir;
    /// # use highlandcows_isam::Isam;
    /// # let dir = TempDir::new().unwrap();
    /// # let path = dir.path().join("db");
    /// # let db: Isam<u32, String> = Isam::create(&path).unwrap();
    /// let mut txn = db.begin_transaction().unwrap();
    /// db.insert(&mut txn, 1u32, &"oops".to_string()).unwrap();
    /// txn.rollback().unwrap();
    ///
    /// // Insert was rolled back — key is absent.
    /// let mut txn2 = db.begin_transaction().unwrap();
    /// assert_eq!(db.get(&mut txn2, &1u32).unwrap(), None);
    /// txn2.commit().unwrap();
    /// ```
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
                UndoEntry::Insert { key, value } => {
                    for si in &mut self.guard.secondary_indices {
                        let _ = si.undo_insert(&key, &value);
                    }
                    let _ = self.guard.index.delete(&key);
                }
                UndoEntry::Update { key, old_rec, old_value, new_value } => {
                    for si in &mut self.guard.secondary_indices {
                        let _ = si.undo_update(&key, &old_value, &new_value);
                    }
                    let _ = self.guard.index.update(&key, old_rec);
                }
                UndoEntry::Delete { key, old_rec, value } => {
                    for si in &mut self.guard.secondary_indices {
                        let _ = si.undo_delete(&key, &value);
                    }
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
