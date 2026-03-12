/// `Isam<K, V>` — the public orchestration interface for an ISAM database.
///
/// `Isam` is a thin facade over `TransactionManager`.  All CRUD operations
/// take a `&mut Transaction` obtained from `begin_transaction()`.
///
/// ## Generic parameters
///
/// - `K` — key type; serializable, deserializable, ordered, cheap to clone.
/// - `V` — value type; serializable and deserializable.
use std::ops::{Bound, RangeBounds};
use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::error::{IsamError, IsamResult};
use crate::manager::TransactionManager;
use crate::storage::IsamStorage;
use crate::store::RecordRef;
use crate::transaction::Transaction;

// ── Path helpers (pub(crate) so storage.rs can use them) ─────────────────── //

pub(crate) fn idb_path(base: &Path) -> PathBuf {
    base.with_extension("idb")
}

pub(crate) fn idx_path(base: &Path) -> PathBuf {
    base.with_extension("idx")
}

/// Convert a borrowed `Bound<&K>` into an owned `Bound<K>` by cloning.
fn clone_bound<K: Clone>(b: Bound<&K>) -> Bound<K> {
    match b {
        Bound::Included(k) => Bound::Included(k.clone()),
        Bound::Excluded(k) => Bound::Excluded(k.clone()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

// ── Isam ─────────────────────────────────────────────────────────────────── //

/// The public ISAM database handle.
///
/// `Isam` is `Clone` — every clone is another handle to the same underlying
/// storage.  Thread safety is provided by `TransactionManager`.
pub struct Isam<K, V> {
    manager: TransactionManager<K, V>,
}

impl<K, V> Clone for Isam<K, V> {
    fn clone(&self) -> Self {
        Self {
            manager: self.manager.clone(),
        }
    }
}

impl<K, V> Isam<K, V>
where
    K: Serialize + DeserializeOwned + Ord + Clone,
    V: Serialize + DeserializeOwned,
{
    // ── Lifecycle ────────────────────────────────────────────────────────── //

    pub fn create(path: impl AsRef<Path>) -> IsamResult<Self> {
        Ok(Self {
            manager: TransactionManager::create(path.as_ref())?,
        })
    }

    pub fn open(path: impl AsRef<Path>) -> IsamResult<Self> {
        Ok(Self {
            manager: TransactionManager::open(path.as_ref())?,
        })
    }

    /// Begin a new transaction.  The returned `Transaction` holds the database
    /// lock until it is committed, rolled back, or dropped.
    pub fn begin_transaction(&self) -> IsamResult<Transaction<'_, K, V>> {
        self.manager.begin()
    }

    // ── CRUD ─────────────────────────────────────────────────────────────── //

    pub fn insert(&self, txn: &mut Transaction<'_, K, V>, key: K, value: &V) -> IsamResult<()> {
        let storage = txn.storage_mut();
        let rec = storage.store.append(&key, value)?;
        storage.index.insert(&key, rec)?;
        txn.log_insert(key);
        Ok(())
    }

    pub fn get(&self, txn: &mut Transaction<'_, K, V>, key: &K) -> IsamResult<Option<V>> {
        let storage = txn.storage_mut();
        match storage.index.search(key)? {
            None => Ok(None),
            Some(rec) => Ok(Some(storage.store.read_value(rec)?)),
        }
    }

    pub fn update(&self, txn: &mut Transaction<'_, K, V>, key: K, value: &V) -> IsamResult<()> {
        let storage = txn.storage_mut();
        let old_rec = storage.index.search(&key)?.ok_or(IsamError::KeyNotFound)?;
        let new_rec = storage.store.append(&key, value)?;
        storage.index.update(&key, new_rec)?;
        txn.log_update(key, old_rec);
        Ok(())
    }

    pub fn delete(&self, txn: &mut Transaction<'_, K, V>, key: &K) -> IsamResult<()> {
        let storage = txn.storage_mut();
        let old_rec = storage.index.search(key)?.ok_or(IsamError::KeyNotFound)?;
        storage.index.delete(key)?;
        storage.store.append_tombstone(key)?;
        txn.log_delete(key.clone(), old_rec);
        Ok(())
    }

    /// Return the smallest key in the database, or `None` if empty.
    pub fn min_key(&self, txn: &mut Transaction<'_, K, V>) -> IsamResult<Option<K>> {
        txn.storage_mut().index.min_key()
    }

    /// Return the largest key in the database, or `None` if empty.
    pub fn max_key(&self, txn: &mut Transaction<'_, K, V>) -> IsamResult<Option<K>> {
        txn.storage_mut().index.max_key()
    }

    // ── Iterators ────────────────────────────────────────────────────────── //

    pub fn iter<'txn>(
        &self,
        txn: &'txn mut Transaction<'_, K, V>,
    ) -> IsamResult<IsamIter<'txn, K, V>> {
        let storage = txn.storage_mut();
        let first_id = storage.index.first_leaf_id()?;
        let (entries, next_id) = if first_id != 0 {
            storage.index.read_leaf(first_id)?
        } else {
            (vec![], 0)
        };
        Ok(IsamIter {
            storage: txn.storage_mut(),
            buffer: entries,
            buf_pos: 0,
            next_leaf_id: next_id,
        })
    }

    pub fn range<'txn, R>(
        &self,
        txn: &'txn mut Transaction<'_, K, V>,
        range: R,
    ) -> IsamResult<RangeIter<'txn, K, V>>
    where
        R: RangeBounds<K>,
    {
        let start_bound = clone_bound(range.start_bound());
        let end_bound = clone_bound(range.end_bound());

        let storage = txn.storage_mut();

        let start_leaf_id = match &start_bound {
            Bound::Included(k) | Bound::Excluded(k) => storage.index.find_leaf_for_key(k)?,
            Bound::Unbounded => storage.index.first_leaf_id()?,
        };

        let (entries, next_leaf_id) = if start_leaf_id != 0 {
            storage.index.read_leaf(start_leaf_id)?
        } else {
            (vec![], 0)
        };

        let buf_pos = match &start_bound {
            Bound::Included(k) => entries.partition_point(|(ek, _)| ek < k),
            Bound::Excluded(k) => entries.partition_point(|(ek, _)| ek <= k),
            Bound::Unbounded => 0,
        };

        Ok(RangeIter {
            storage: txn.storage_mut(),
            buffer: entries,
            buf_pos,
            next_leaf_id,
            end_bound,
        })
    }

    // ── Schema versioning ────────────────────────────────────────────────── //

    /// Return the key schema version stored in the index metadata.
    ///
    /// # Deadlock warning
    /// Acquires the database lock internally.  Must not be called while a
    /// [`Transaction`] is live on the same thread.
    pub fn key_schema_version(&self) -> IsamResult<u32> {
        let guard = self.manager.storage.lock().map_err(|_| IsamError::LockPoisoned)?;
        Ok(guard.index.key_schema_version())
    }

    /// Return the value schema version stored in the index metadata.
    ///
    /// # Deadlock warning
    /// Acquires the database lock internally.  Must not be called while a
    /// [`Transaction`] is live on the same thread.
    pub fn val_schema_version(&self) -> IsamResult<u32> {
        let guard = self.manager.storage.lock().map_err(|_| IsamError::LockPoisoned)?;
        Ok(guard.index.val_schema_version())
    }

    // ── Structural operations ─────────────────────────────────────────────── //

    /// Compact the database, removing tombstones and stale values.
    ///
    /// # Deadlock warning
    /// Acquires the database lock internally.  Must not be called while a
    /// [`Transaction`] is live on the same thread.  These operations are
    /// intended for offline administration — commit or roll back all open
    /// transactions before calling them.
    pub fn compact(&self) -> IsamResult<()> {
        let mut storage = self
            .manager
            .storage
            .lock()
            .map_err(|_| IsamError::LockPoisoned)?;

        let mut records: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let first_id = storage.index.first_leaf_id()?;
        let mut current_id = first_id;
        while current_id != 0 {
            let (entries, next_id) = storage.index.read_leaf(current_id)?;
            for (_, rec) in &entries {
                let (_status, key_bytes, val_bytes) = storage.store.read_record_raw(rec.offset)?;
                records.push((key_bytes, val_bytes));
            }
            current_id = next_id;
        }

        let tmp_idb = storage.base_path.with_extension("idb.tmp");
        let tmp_idx = storage.base_path.with_extension("idx.tmp");

        let mut new_store = crate::store::DataStore::create(&tmp_idb)?;
        let mut new_index: crate::index::BTree<K> = crate::index::BTree::create(&tmp_idx)?;

        for (key_bytes, val_bytes) in &records {
            let rec = new_store.write_raw_record(crate::store::STATUS_ALIVE, key_bytes, val_bytes)?;
            let key: K = bincode::deserialize(key_bytes)?;
            new_index.insert(&key, rec)?;
        }

        new_store.flush()?;
        new_index.flush()?;
        drop(new_store);
        drop(new_index);

        let base = storage.base_path.clone();
        std::fs::rename(&tmp_idb, idb_path(&base))?;
        std::fs::rename(&tmp_idx, idx_path(&base))?;

        storage.store = crate::store::DataStore::open(&idb_path(&base))?;
        storage.index = crate::index::BTree::open(&idx_path(&base))?;

        Ok(())
    }

    /// Rewrite every value through `f`, bump the val schema version, and
    /// return a ready-to-use `Isam<K, V2>`.  Consumes `self`.
    ///
    /// # Deadlock warning
    /// Acquires the database lock internally.  Must not be called while a
    /// [`Transaction`] is live on the same thread.  These operations are
    /// intended for offline administration — commit or roll back all open
    /// transactions before calling them.
    pub fn migrate_values<V2, F>(self, new_val_version: u32, mut f: F) -> IsamResult<Isam<K, V2>>
    where
        V2: Serialize + DeserializeOwned,
        F: FnMut(V) -> IsamResult<V2>,
    {
        let mut storage = self
            .manager
            .storage
            .lock()
            .map_err(|_| IsamError::LockPoisoned)?;

        let base_path = storage.base_path.clone();
        let key_schema_v = storage.index.key_schema_version();

        let mut records: Vec<(Vec<u8>, V)> = Vec::new();
        let first_id = storage.index.first_leaf_id()?;
        let mut current_id = first_id;
        while current_id != 0 {
            let (entries, next_id) = storage.index.read_leaf(current_id)?;
            for (_, rec) in &entries {
                let (_status, key_bytes, val_bytes) = storage.store.read_record_raw(rec.offset)?;
                let v: V = bincode::deserialize(&val_bytes)?;
                records.push((key_bytes, v));
            }
            current_id = next_id;
        }

        let mut transformed: Vec<(Vec<u8>, V2)> = Vec::with_capacity(records.len());
        for (key_bytes, v) in records {
            transformed.push((key_bytes, f(v)?));
        }

        let tmp_idb = base_path.with_extension("idb.tmp");
        let tmp_idx = base_path.with_extension("idx.tmp");

        let mut new_store = crate::store::DataStore::create(&tmp_idb)?;
        let mut new_index: crate::index::BTree<K> = crate::index::BTree::create(&tmp_idx)?;
        new_index.set_schema_versions(key_schema_v, new_val_version)?;

        for (key_bytes, v2) in &transformed {
            let val_bytes = bincode::serialize(v2)?;
            let rec = new_store.write_raw_record(crate::store::STATUS_ALIVE, key_bytes, &val_bytes)?;
            let key: K = bincode::deserialize(key_bytes)?;
            new_index.insert(&key, rec)?;
        }

        new_store.flush()?;
        new_index.flush()?;
        drop(new_store);
        drop(new_index);
        drop(storage);

        std::fs::rename(&tmp_idb, idb_path(&base_path))?;
        std::fs::rename(&tmp_idx, idx_path(&base_path))?;

        Isam::<K, V2>::open(&base_path)
    }

    /// Rewrite every key through `f`, bump the key schema version, re-sort by
    /// `K2::Ord`, rebuild the index, and return a ready-to-use `Isam<K2, V>`.
    /// Consumes `self`.
    ///
    /// # Deadlock warning
    /// Acquires the database lock internally.  Must not be called while a
    /// [`Transaction`] is live on the same thread.  These operations are
    /// intended for offline administration — commit or roll back all open
    /// transactions before calling them.
    pub fn migrate_keys<K2, F>(self, new_key_version: u32, mut f: F) -> IsamResult<Isam<K2, V>>
    where
        K2: Serialize + DeserializeOwned + Ord + Clone,
        F: FnMut(K) -> IsamResult<K2>,
    {
        let mut storage = self
            .manager
            .storage
            .lock()
            .map_err(|_| IsamError::LockPoisoned)?;

        let base_path = storage.base_path.clone();
        let val_schema_v = storage.index.val_schema_version();

        let mut records: Vec<(K2, Vec<u8>)> = Vec::new();
        let first_id = storage.index.first_leaf_id()?;
        let mut current_id = first_id;
        while current_id != 0 {
            let (entries, next_id) = storage.index.read_leaf(current_id)?;
            for (k, rec) in &entries {
                let (_status, _key_bytes, val_bytes) = storage.store.read_record_raw(rec.offset)?;
                let k2 = f(k.clone())?;
                records.push((k2, val_bytes));
            }
            current_id = next_id;
        }

        records.sort_by(|(a, _), (b, _)| a.cmp(b));

        let tmp_idb = base_path.with_extension("idb.tmp");
        let tmp_idx = base_path.with_extension("idx.tmp");

        let mut new_store = crate::store::DataStore::create(&tmp_idb)?;
        let mut new_index: crate::index::BTree<K2> = crate::index::BTree::create(&tmp_idx)?;
        new_index.set_schema_versions(new_key_version, val_schema_v)?;

        for (k2, val_bytes) in &records {
            let key_bytes = bincode::serialize(k2)?;
            let rec = new_store.write_raw_record(crate::store::STATUS_ALIVE, &key_bytes, val_bytes)?;
            new_index.insert(k2, rec)?;
        }

        new_store.flush()?;
        new_index.flush()?;
        drop(new_store);
        drop(new_index);
        drop(storage);

        std::fs::rename(&tmp_idb, idb_path(&base_path))?;
        std::fs::rename(&tmp_idx, idx_path(&base_path))?;

        Isam::<K2, V>::open(&base_path)
    }
}

// ── IsamIter ──────────────────────────────────────────────────────────────── //

pub struct IsamIter<'txn, K, V> {
    storage: &'txn mut IsamStorage<K, V>,
    buffer: Vec<(K, RecordRef)>,
    buf_pos: usize,
    next_leaf_id: u32,
}

impl<'txn, K, V> Iterator for IsamIter<'txn, K, V>
where
    K: Serialize + DeserializeOwned + Ord + Clone,
    V: Serialize + DeserializeOwned,
{
    type Item = IsamResult<(K, V)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.buf_pos < self.buffer.len() {
                let (key, rec) = self.buffer[self.buf_pos].clone();
                self.buf_pos += 1;
                return Some(self.storage.store.read_value(rec).map(|value| (key, value)));
            }

            if self.next_leaf_id == 0 {
                return None;
            }

            match self.storage.index.read_leaf(self.next_leaf_id) {
                Ok((entries, next_id)) => {
                    self.buffer = entries;
                    self.buf_pos = 0;
                    self.next_leaf_id = next_id;
                }
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

// ── RangeIter ────────────────────────────────────────────────────────────── //

pub struct RangeIter<'txn, K, V> {
    storage: &'txn mut IsamStorage<K, V>,
    buffer: Vec<(K, RecordRef)>,
    buf_pos: usize,
    next_leaf_id: u32,
    end_bound: Bound<K>,
}

impl<'txn, K, V> Iterator for RangeIter<'txn, K, V>
where
    K: Serialize + DeserializeOwned + Ord + Clone,
    V: Serialize + DeserializeOwned,
{
    type Item = IsamResult<(K, V)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.buf_pos < self.buffer.len() {
                let (key, rec) = self.buffer[self.buf_pos].clone();
                self.buf_pos += 1;

                let within = match &self.end_bound {
                    Bound::Included(end) => &key <= end,
                    Bound::Excluded(end) => &key < end,
                    Bound::Unbounded => true,
                };
                if !within {
                    return None;
                }

                return Some(self.storage.store.read_value(rec).map(|value| (key, value)));
            }

            if self.next_leaf_id == 0 {
                return None;
            }

            match self.storage.index.read_leaf(self.next_leaf_id) {
                Ok((entries, next_id)) => {
                    self.buffer = entries;
                    self.buf_pos = 0;
                    self.next_leaf_id = next_id;
                }
                Err(e) => return Some(Err(e)),
            }
        }
    }
}
