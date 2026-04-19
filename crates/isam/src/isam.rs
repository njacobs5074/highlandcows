/// `Isam<K, V>` — the public orchestration interface for an ISAM database.
///
/// `Isam` is a thin facade over `TransactionManager`.  All CRUD operations
/// take a `&mut Transaction` obtained from `begin_transaction()`.
///
/// ## Generic parameters
///
/// - `K` — key type; serializable, deserializable, ordered, cheap to clone.
/// - `V` — value type; serializable and deserializable.
use std::collections::HashSet;
use std::marker::PhantomData;
use std::ops::{Bound, RangeBounds};
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::error::{IsamError, IsamResult};
use crate::manager::TransactionManager;
use crate::secondary_index::{AnySecondaryIndex, DeriveKey, SecondaryIndexImpl};
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

// ── Constants ────────────────────────────────────────────────────────────── //

/// Default timeout for [`Isam::as_single_user`].
///
/// 30 seconds — long enough for typical in-flight transactions to finish,
/// short enough to surface hung transactions rather than waiting forever.
pub const DEFAULT_SINGLE_USER_TIMEOUT: Duration = Duration::from_secs(30);

// ── Isam ─────────────────────────────────────────────────────────────────── //

/// The public ISAM database handle.
///
/// `Isam` is `Clone` — every clone is another handle to the same underlying
/// storage.  Thread safety is provided by `TransactionManager`.
///
/// ## Creating and opening databases
///
/// For databases without secondary indices, use [`Isam::create`] and [`Isam::open`].
/// To attach secondary indices at construction time, use [`Isam::builder`].
///
/// ## Running transactions
///
/// For simple single-operation writes or reads, use the [`write`](Self::write) and
/// [`read`](Self::read) helpers — they handle begin/commit/rollback automatically:
///
/// ```
/// # use tempfile::TempDir;
/// # use highlandcows_isam::Isam;
/// # let dir = TempDir::new().unwrap();
/// # let path = dir.path().join("db");
/// # let db: Isam<u32, String> = Isam::create(&path).unwrap();
/// db.write(|txn| db.insert(txn, 1u32, &"hello".to_string())).unwrap();
/// let val = db.read(|txn| db.get(txn, &1u32)).unwrap();
/// assert_eq!(val, Some("hello".to_string()));
/// ```
///
/// For multi-operation transactions, use [`begin_transaction`](Self::begin_transaction) directly.
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
    K: Serialize + DeserializeOwned + Ord + Clone + 'static,
    V: Serialize + DeserializeOwned + Clone + 'static,
{
    // ── Lifecycle ────────────────────────────────────────────────────────── //

    /// Return a builder for creating or opening a database with secondary indices.
    ///
    /// For databases without secondary indices, [`create`](Self::create) and
    /// [`open`](Self::open) are simpler alternatives.
    ///
    /// # Example
    /// ```
    /// # use tempfile::TempDir;
    /// use serde::{Serialize, Deserialize};
    /// use highlandcows_isam::{Isam, DeriveKey};
    ///
    /// #[derive(Serialize, Deserialize, Clone)]
    /// struct User { name: String, city: String }
    ///
    /// struct CityIndex;
    /// impl DeriveKey<User> for CityIndex {
    ///     type Key = String;
    ///     fn derive(u: &User) -> String { u.city.clone() }
    /// }
    ///
    /// # let dir = TempDir::new().unwrap();
    /// # let path = dir.path().join("db");
    /// let db = Isam::<u64, User>::builder()
    ///     .with_index("city", CityIndex)
    ///     .create(&path)
    ///     .unwrap();
    ///
    /// let city_idx = db.index::<CityIndex>("city");
    /// ```
    pub fn builder() -> IsamBuilder<K, V> {
        IsamBuilder::default()
    }

    /// Create a new, empty database at `path` with no secondary indices.
    ///
    /// Two files are created: `<path>.idb` (data) and `<path>.idx` (index).
    /// Any existing files at those paths are truncated.
    ///
    /// To register secondary indices, use [`builder`](Self::builder) instead.
    ///
    /// # Example
    /// ```
    /// # use tempfile::TempDir;
    /// # use highlandcows_isam::Isam;
    /// # let dir = TempDir::new().unwrap();
    /// # let path = dir.path().join("db");
    /// let db: Isam<u32, String> = Isam::create(&path).unwrap();
    /// ```
    pub fn create(path: impl AsRef<Path>) -> IsamResult<Self> {
        Ok(Self {
            manager: TransactionManager::create(path.as_ref())?,
        })
    }

    /// Open an existing database at `path` with no secondary indices.
    ///
    /// To re-register secondary indices on open, use [`builder`](Self::builder) instead.
    ///
    /// # Example
    /// ```
    /// # use tempfile::TempDir;
    /// # use highlandcows_isam::Isam;
    /// # let dir = TempDir::new().unwrap();
    /// # let path = dir.path().join("db");
    /// # Isam::<u32, String>::create(&path).unwrap();
    /// let db: Isam<u32, String> = Isam::open(&path).unwrap();
    /// ```
    pub fn open(path: impl AsRef<Path>) -> IsamResult<Self> {
        Ok(Self {
            manager: TransactionManager::open(path.as_ref())?,
        })
    }

    // ── Single-user mode ─────────────────────────────────────────────────── //

    /// Execute a closure in single-user mode.
    ///
    /// Sets the single-user flag immediately, then waits up to `timeout` for any
    /// in-flight transaction on another thread to finish.  Once exclusive access
    /// is confirmed, `f` is called.  Any other thread that attempts any database
    /// operation while `f` is running receives [`IsamError::SingleUserMode`]
    /// immediately (no blocking).  The calling thread can continue to use `self`
    /// normally inside `f`.
    ///
    /// Single-user mode is intended for administrative operations — compaction,
    /// schema migration, and similar tasks — where you need to ensure no other
    /// thread modifies the database concurrently.  It is an in-process
    /// mechanism only; multi-process exclusion is not supported.
    ///
    /// The return value of `f` is forwarded to the caller.  Single-user mode is
    /// released when `f` returns, including if `f` returns an error or panics.
    ///
    /// # Errors
    ///
    /// - [`IsamError::SingleUserMode`] — single-user mode is already active
    ///   (e.g. called recursively, or another thread holds it).
    /// - [`IsamError::Timeout`] — an in-flight transaction did not finish within
    ///   `timeout`.  This also occurs if the calling thread itself holds an open
    ///   [`Transaction`]: the transaction holds the storage lock, so the spin
    ///   will never succeed and the call will time out.  Commit or roll back all
    ///   transactions on the calling thread before calling `as_single_user`.
    ///
    /// # Example
    /// ```
    /// # use tempfile::TempDir;
    /// # use highlandcows_isam::{Isam, DEFAULT_SINGLE_USER_TIMEOUT};
    /// # let dir = TempDir::new().unwrap();
    /// # let path = dir.path().join("db");
    /// # let db: Isam<u32, String> = Isam::create(&path).unwrap();
    /// # let mut txn = db.begin_transaction().unwrap();
    /// # for i in 0u32..5 { db.insert(&mut txn, i, &i.to_string()).unwrap(); }
    /// # for i in 0u32..3 { db.delete(&mut txn, &i).unwrap(); }
    /// # txn.commit().unwrap();
    /// // Run compact exclusively — no other thread can touch the database.
    /// db.as_single_user(DEFAULT_SINGLE_USER_TIMEOUT, || db.compact()).unwrap();
    /// ```
    pub fn as_single_user<F, T>(&self, timeout: Duration, f: F) -> IsamResult<T>
    where
        F: FnOnce() -> IsamResult<T>,
    {
        let _guard = self.manager.enter_single_user_mode(timeout)?;
        f()
    }

    /// Return a [`SecondaryIndexHandle`] for the named index.
    ///
    /// The index must have been registered via
    /// [`IsamBuilder::with_index`] when the database was created or opened.
    /// No I/O is performed — the handle is just a typed wrapper around the
    /// index name.
    ///
    /// # Example
    /// ```
    /// # use tempfile::TempDir;
    /// use serde::{Serialize, Deserialize};
    /// use highlandcows_isam::{Isam, DeriveKey};
    ///
    /// #[derive(Serialize, Deserialize, Clone)]
    /// struct User { name: String, city: String }
    ///
    /// struct CityIndex;
    /// impl DeriveKey<User> for CityIndex {
    ///     type Key = String;
    ///     fn derive(u: &User) -> String { u.city.clone() }
    /// }
    ///
    /// # let dir = TempDir::new().unwrap();
    /// # let path = dir.path().join("db");
    /// let db = Isam::<u64, User>::builder()
    ///     .with_index("city", CityIndex)
    ///     .create(&path)
    ///     .unwrap();
    ///
    /// let city_idx = db.index::<CityIndex>("city");
    ///
    /// db.write(|txn| db.insert(txn, 1, &User { name: "Alice".into(), city: "London".into() })).unwrap();
    ///
    /// let results = db.read(|txn| city_idx.lookup(txn, &"London".to_string())).unwrap();
    /// assert_eq!(results.len(), 1);
    /// ```
    pub fn index<E: DeriveKey<V>>(&self, name: &str) -> SecondaryIndexHandle<K, V, E::Key> {
        SecondaryIndexHandle {
            name: name.to_owned(),
            _phantom: PhantomData,
        }
    }

    /// Return information about all secondary indices registered on this database.
    ///
    /// # Deadlock warning
    /// Acquires the database lock internally.  Must not be called while a
    /// [`Transaction`] is live on the same thread.
    ///
    /// # Example
    /// ```
    /// # use tempfile::TempDir;
    /// use serde::{Serialize, Deserialize};
    /// use highlandcows_isam::{Isam, DeriveKey};
    ///
    /// #[derive(Serialize, Deserialize, Clone)]
    /// struct User { name: String, city: String }
    ///
    /// struct CityIndex;
    /// impl DeriveKey<User> for CityIndex {
    ///     type Key = String;
    ///     fn derive(u: &User) -> String { u.city.clone() }
    /// }
    ///
    /// # let dir = TempDir::new().unwrap();
    /// # let path = dir.path().join("db");
    /// let db = Isam::<u64, User>::builder()
    ///     .with_index("city", CityIndex)
    ///     .create(&path)
    ///     .unwrap();
    ///
    /// let indices = db.secondary_indices().unwrap();
    /// assert_eq!(indices.len(), 1);
    /// assert_eq!(indices[0].name, "city");
    /// ```
    pub fn secondary_indices(&self) -> IsamResult<Vec<IndexInfo>> {
        let guard = self.manager.lock_storage()?;
        Ok(guard
            .secondary_indices
            .iter()
            .map(|si| IndexInfo {
                name: si.name().to_owned(),
                extractor_type: si.extractor_type_name(),
                schema_version: si.stored_schema_version(),
            })
            .collect())
    }

    /// Begin a new transaction.
    ///
    /// The returned [`Transaction`] holds an exclusive lock on the database
    /// until it is committed, rolled back, or dropped.  Dropping without
    /// committing automatically rolls back all changes made in the transaction.
    ///
    /// For simple single-operation use, prefer the [`write`](Self::write) and
    /// [`read`](Self::read) helpers, which handle begin/commit/rollback
    /// automatically.
    ///
    /// # Example
    /// ```
    /// # use tempfile::TempDir;
    /// # use highlandcows_isam::Isam;
    /// # let dir = TempDir::new().unwrap();
    /// # let path = dir.path().join("db");
    /// # let db: Isam<u32, String> = Isam::create(&path).unwrap();
    /// let mut txn = db.begin_transaction().unwrap();
    /// // ... perform operations ...
    /// txn.commit().unwrap();
    /// ```
    pub fn begin_transaction(&self) -> IsamResult<Transaction<'_, K, V>> {
        self.manager.begin()
    }

    /// Execute a write closure inside a transaction.
    ///
    /// Begins a transaction, passes it to `f`, then commits on `Ok` or rolls
    /// back on `Err`.  The return value of `f` is forwarded to the caller.
    ///
    /// Use this for inserts, updates, and deletes where you don't need to
    /// manage the transaction lifetime manually.
    ///
    /// # Example
    /// ```
    /// # use tempfile::TempDir;
    /// # use highlandcows_isam::Isam;
    /// # let dir = TempDir::new().unwrap();
    /// # let path = dir.path().join("db");
    /// # let db: Isam<u32, String> = Isam::create(&path).unwrap();
    /// db.write(|txn| db.insert(txn, 1u32, &"hello".to_string())).unwrap();
    /// ```
    pub fn write<F, T>(&self, f: F) -> IsamResult<T>
    where
        F: FnOnce(&mut Transaction<'_, K, V>) -> IsamResult<T>,
    {
        let mut txn = self.begin_transaction()?;
        match f(&mut txn) {
            Ok(val) => { txn.commit()?; Ok(val) }
            Err(e)  => { let _ = txn.rollback(); Err(e) }
        }
    }

    /// Execute a read closure inside a transaction.
    ///
    /// Begins a transaction, passes it to `f`, then rolls back unconditionally
    /// (since reads make no changes).  The return value of `f` is forwarded to
    /// the caller.
    ///
    /// # Example
    /// ```
    /// # use tempfile::TempDir;
    /// # use highlandcows_isam::Isam;
    /// # let dir = TempDir::new().unwrap();
    /// # let path = dir.path().join("db");
    /// # let db: Isam<u32, String> = Isam::create(&path).unwrap();
    /// # db.write(|txn| db.insert(txn, 1u32, &"hello".to_string())).unwrap();
    /// let val = db.read(|txn| db.get(txn, &1u32)).unwrap();
    /// assert_eq!(val, Some("hello".to_string()));
    /// ```
    pub fn read<F, T>(&self, f: F) -> IsamResult<T>
    where
        F: FnOnce(&mut Transaction<'_, K, V>) -> IsamResult<T>,
    {
        let mut txn = self.begin_transaction()?;
        let result = f(&mut txn);
        let _ = txn.rollback();
        result
    }

    // ── CRUD ─────────────────────────────────────────────────────────────── //

    /// Insert a new key-value pair.
    ///
    /// Returns [`IsamError::DuplicateKey`] if the key already exists.
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
    /// ```
    pub fn insert(&self, txn: &mut Transaction<'_, K, V>, key: K, value: &V) -> IsamResult<()> {
        {
            let storage = txn.storage_mut();
            let rec = storage.store.append(&key, value)?;
            storage.index.insert(&key, rec)?;
            for si in &mut storage.secondary_indices {
                si.on_insert(&key, value)?;
            }
        }
        txn.log_insert(key, value.clone());
        Ok(())
    }

    /// Look up a key and return its value, or `None` if the key does not exist.
    ///
    /// # Example
    /// ```
    /// # use tempfile::TempDir;
    /// # use highlandcows_isam::Isam;
    /// # let dir = TempDir::new().unwrap();
    /// # let path = dir.path().join("db");
    /// # let db: Isam<u32, String> = Isam::create(&path).unwrap();
    /// # let mut txn = db.begin_transaction().unwrap();
    /// # db.insert(&mut txn, 1u32, &"hello".to_string()).unwrap();
    /// # txn.commit().unwrap();
    /// let mut txn = db.begin_transaction().unwrap();
    /// assert_eq!(db.get(&mut txn, &1u32).unwrap(), Some("hello".to_string()));
    /// assert_eq!(db.get(&mut txn, &99u32).unwrap(), None);
    /// txn.commit().unwrap();
    /// ```
    pub fn get(&self, txn: &mut Transaction<'_, K, V>, key: &K) -> IsamResult<Option<V>> {
        let storage = txn.storage_mut();
        match storage.index.search(key)? {
            None => Ok(None),
            Some(rec) => Ok(Some(storage.store.read_value(rec)?)),
        }
    }

    /// Replace the value for an existing key.
    ///
    /// Returns [`IsamError::KeyNotFound`] if the key does not exist.
    ///
    /// # Example
    /// ```
    /// # use tempfile::TempDir;
    /// # use highlandcows_isam::Isam;
    /// # let dir = TempDir::new().unwrap();
    /// # let path = dir.path().join("db");
    /// # let db: Isam<u32, String> = Isam::create(&path).unwrap();
    /// # let mut txn = db.begin_transaction().unwrap();
    /// # db.insert(&mut txn, 1u32, &"old".to_string()).unwrap();
    /// # txn.commit().unwrap();
    /// let mut txn = db.begin_transaction().unwrap();
    /// db.update(&mut txn, 1u32, &"new".to_string()).unwrap();
    /// assert_eq!(db.get(&mut txn, &1u32).unwrap(), Some("new".to_string()));
    /// txn.commit().unwrap();
    /// ```
    pub fn update(&self, txn: &mut Transaction<'_, K, V>, key: K, value: &V) -> IsamResult<()> {
        let (old_rec, old_value) = {
            let storage = txn.storage_mut();
            let old_rec = storage.index.search(&key)?.ok_or(IsamError::KeyNotFound)?;
            let old_value: V = storage.store.read_value(old_rec)?;
            (old_rec, old_value)
        };
        {
            let storage = txn.storage_mut();
            let new_rec = storage.store.append(&key, value)?;
            storage.index.update(&key, new_rec)?;
            for si in &mut storage.secondary_indices {
                si.on_update(&key, &old_value, value)?;
            }
        }
        txn.log_update(key, old_rec, old_value, value.clone());
        Ok(())
    }

    /// Remove a key and its associated value.
    ///
    /// Returns [`IsamError::KeyNotFound`] if the key does not exist.
    ///
    /// # Example
    /// ```
    /// # use tempfile::TempDir;
    /// # use highlandcows_isam::Isam;
    /// # let dir = TempDir::new().unwrap();
    /// # let path = dir.path().join("db");
    /// # let db: Isam<u32, String> = Isam::create(&path).unwrap();
    /// # let mut txn = db.begin_transaction().unwrap();
    /// # db.insert(&mut txn, 1u32, &"hello".to_string()).unwrap();
    /// # txn.commit().unwrap();
    /// let mut txn = db.begin_transaction().unwrap();
    /// db.delete(&mut txn, &1u32).unwrap();
    /// assert_eq!(db.get(&mut txn, &1u32).unwrap(), None);
    /// txn.commit().unwrap();
    /// ```
    pub fn delete(&self, txn: &mut Transaction<'_, K, V>, key: &K) -> IsamResult<()> {
        let (old_rec, old_value) = {
            let storage = txn.storage_mut();
            let old_rec = storage.index.search(key)?.ok_or(IsamError::KeyNotFound)?;
            let old_value: V = storage.store.read_value(old_rec)?;
            (old_rec, old_value)
        };
        {
            let storage = txn.storage_mut();
            storage.index.delete(key)?;
            storage.store.append_tombstone(key)?;
            for si in &mut storage.secondary_indices {
                si.on_delete(key, &old_value)?;
            }
        }
        txn.log_delete(key.clone(), old_rec, old_value);
        Ok(())
    }

    /// Return the smallest key in the database, or `None` if empty.
    ///
    /// # Example
    /// ```
    /// # use tempfile::TempDir;
    /// # use highlandcows_isam::Isam;
    /// # let dir = TempDir::new().unwrap();
    /// # let path = dir.path().join("db");
    /// # let db: Isam<u32, u32> = Isam::create(&path).unwrap();
    /// # let mut txn = db.begin_transaction().unwrap();
    /// # for k in [3u32, 1, 2] { db.insert(&mut txn, k, &k).unwrap(); }
    /// # txn.commit().unwrap();
    /// let mut txn = db.begin_transaction().unwrap();
    /// assert_eq!(db.min_key(&mut txn).unwrap(), Some(1u32));
    /// txn.commit().unwrap();
    /// ```
    pub fn min_key(&self, txn: &mut Transaction<'_, K, V>) -> IsamResult<Option<K>> {
        txn.storage_mut().index.min_key()
    }

    /// Return the largest key in the database, or `None` if empty.
    ///
    /// # Example
    /// ```
    /// # use tempfile::TempDir;
    /// # use highlandcows_isam::Isam;
    /// # let dir = TempDir::new().unwrap();
    /// # let path = dir.path().join("db");
    /// # let db: Isam<u32, u32> = Isam::create(&path).unwrap();
    /// # let mut txn = db.begin_transaction().unwrap();
    /// # for k in [3u32, 1, 2] { db.insert(&mut txn, k, &k).unwrap(); }
    /// # txn.commit().unwrap();
    /// let mut txn = db.begin_transaction().unwrap();
    /// assert_eq!(db.max_key(&mut txn).unwrap(), Some(3u32));
    /// txn.commit().unwrap();
    /// ```
    pub fn max_key(&self, txn: &mut Transaction<'_, K, V>) -> IsamResult<Option<K>> {
        txn.storage_mut().index.max_key()
    }

    // ── Iterators ────────────────────────────────────────────────────────── //

    /// Return a key-ordered iterator over all records.
    ///
    /// The iterator borrows `txn` for its lifetime, so no other operations
    /// can be performed on the database until the iterator is dropped.
    ///
    /// # Example
    /// ```
    /// # use tempfile::TempDir;
    /// # use highlandcows_isam::Isam;
    /// # let dir = TempDir::new().unwrap();
    /// # let path = dir.path().join("db");
    /// # let db: Isam<u32, u32> = Isam::create(&path).unwrap();
    /// # let mut txn = db.begin_transaction().unwrap();
    /// # for k in [3u32, 1, 2] { db.insert(&mut txn, k, &k).unwrap(); }
    /// # txn.commit().unwrap();
    /// let mut txn = db.begin_transaction().unwrap();
    /// let keys: Vec<u32> = db.iter(&mut txn).unwrap()
    ///     .map(|r| r.unwrap().0)
    ///     .collect();
    /// assert_eq!(keys, vec![1, 2, 3]);
    /// txn.commit().unwrap();
    /// ```
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

    /// Return a key-ordered iterator over records whose keys fall within `range`.
    ///
    /// Accepts any of Rust's built-in range expressions: `a..b`, `a..=b`,
    /// `a..`, `..b`, `..=b`, `..`.
    ///
    /// The iterator borrows `txn` for its lifetime, so no other operations
    /// can be performed on the database until the iterator is dropped.
    ///
    /// # Example
    /// ```
    /// # use tempfile::TempDir;
    /// # use highlandcows_isam::Isam;
    /// # let dir = TempDir::new().unwrap();
    /// # let path = dir.path().join("db");
    /// # let db: Isam<u32, u32> = Isam::create(&path).unwrap();
    /// # let mut txn = db.begin_transaction().unwrap();
    /// # for k in 1u32..=10 { db.insert(&mut txn, k, &k).unwrap(); }
    /// # txn.commit().unwrap();
    /// let mut txn = db.begin_transaction().unwrap();
    /// let keys: Vec<u32> = db.range(&mut txn, 3u32..=7).unwrap()
    ///     .map(|r| r.unwrap().0)
    ///     .collect();
    /// assert_eq!(keys, vec![3, 4, 5, 6, 7]);
    /// txn.commit().unwrap();
    /// ```
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
    /// Schema versions are set by [`migrate_keys`](Self::migrate_keys) and
    /// default to `0` for newly created databases.
    ///
    /// # Deadlock warning
    /// Acquires the database lock internally.  Must not be called while a
    /// [`Transaction`] is live on the same thread.
    ///
    /// # Example
    /// ```
    /// # use tempfile::TempDir;
    /// # use highlandcows_isam::Isam;
    /// # let dir = TempDir::new().unwrap();
    /// # let path = dir.path().join("db");
    /// # let db: Isam<u32, String> = Isam::create(&path).unwrap();
    /// assert_eq!(db.key_schema_version().unwrap(), 0);
    /// ```
    pub fn key_schema_version(&self) -> IsamResult<u32> {
        let guard = self.manager.lock_storage()?;
        Ok(guard.index.key_schema_version())
    }

    /// Return the value schema version stored in the index metadata.
    ///
    /// Schema versions are set by [`migrate_values`](Self::migrate_values) and
    /// default to `0` for newly created databases.
    ///
    /// # Deadlock warning
    /// Acquires the database lock internally.  Must not be called while a
    /// [`Transaction`] is live on the same thread.
    ///
    /// # Example
    /// ```
    /// # use tempfile::TempDir;
    /// # use highlandcows_isam::Isam;
    /// # let dir = TempDir::new().unwrap();
    /// # let path = dir.path().join("db");
    /// # let db: Isam<u32, String> = Isam::create(&path).unwrap();
    /// assert_eq!(db.val_schema_version().unwrap(), 0);
    /// ```
    pub fn val_schema_version(&self) -> IsamResult<u32> {
        let guard = self.manager.lock_storage()?;
        Ok(guard.index.val_schema_version())
    }

    // ── Secondary index migration ─────────────────────────────────────────── //

    /// Migrate a secondary index to a new schema version.
    ///
    /// This is the secondary index counterpart to [`migrate_values`](Self::migrate_values)
    /// and [`migrate_keys`](Self::migrate_keys).  Use it when the [`DeriveKey`]
    /// derivation logic for a named secondary index has changed and the on-disk
    /// index needs to be rebuilt to match.
    ///
    /// The named secondary index is cleared and repopulated by scanning all
    /// primary records.  For each record, `f` is applied to the stored value
    /// before the registered [`DeriveKey`] extractor derives the secondary key,
    /// letting you adapt the effective input to the updated derivation logic.
    /// Pass the identity closure (`|v| Ok(v)`) for a plain rebuild with no
    /// value transformation.
    ///
    /// After the rebuild, `new_version` is written into the `.sidx` metadata
    /// so that [`Isam::secondary_indices`] reflects the current migration state
    /// via [`IndexInfo::schema_version`].
    ///
    /// **Primary records are not modified.**  Only the named secondary index
    /// is affected; other secondary indices are left untouched.
    ///
    /// # Deadlock warning
    /// Acquires the database lock internally.  Must not be called while a
    /// [`Transaction`] is live on the same thread.
    ///
    /// # Example
    /// ```
    /// # use tempfile::TempDir;
    /// use serde::{Serialize, Deserialize};
    /// use highlandcows_isam::{Isam, DeriveKey};
    ///
    /// #[derive(Serialize, Deserialize, Clone)]
    /// struct User { name: String, city: String }
    ///
    /// struct CityIndex;
    /// impl DeriveKey<User> for CityIndex {
    ///     type Key = String;
    ///     // derive now normalizes to lowercase
    ///     fn derive(u: &User) -> String { u.city.to_lowercase() }
    /// }
    ///
    /// # let dir = TempDir::new().unwrap();
    /// # let path = dir.path().join("users");
    /// let db = Isam::<u64, User>::builder()
    ///     .with_index("city", CityIndex)
    ///     .create(&path)
    ///     .unwrap();
    ///
    /// db.write(|txn| db.insert(txn, 1, &User { name: "Alice".into(), city: "London".into() }))
    ///     .unwrap();
    ///
    /// // Rebuild the "city" index, normalizing city names to lowercase
    /// // so the index matches the updated DeriveKey logic.
    /// db.migrate_index("city", 1, |mut u: User| {
    ///     u.city = u.city.to_lowercase();
    ///     Ok(u)
    /// }).unwrap();
    ///
    /// let info = db.secondary_indices().unwrap();
    /// assert_eq!(info[0].schema_version, 1);
    /// ```
    pub fn migrate_index<F>(&self, name: &str, new_version: u32, mut f: F) -> IsamResult<()>
    where
        F: FnMut(V) -> IsamResult<V>,
    {
        let mut storage = self.manager.lock_storage()?;

        // Scan all primary records first, before mutating the index.
        let mut records: Vec<(K, V)> = Vec::new();
        let first_id = storage.index.first_leaf_id()?;
        let mut current_id = first_id;
        while current_id != 0 {
            let (entries, next_id) = storage.index.read_leaf(current_id)?;
            for (key, rec) in &entries {
                let value: V = storage.store.read_value(*rec)?;
                records.push((key.clone(), value));
            }
            current_id = next_id;
        }

        // Find the target index and reset it.
        let si = storage
            .secondary_indices
            .iter_mut()
            .find(|si| si.name() == name)
            .ok_or_else(|| IsamError::IndexNotFound(name.to_owned()))?;
        si.reset()?;

        // Repopulate the index using f(value) as input to DeriveKey::derive.
        for (key, value) in records {
            let effective = f(value)?;
            let si = storage
                .secondary_indices
                .iter_mut()
                .find(|si| si.name() == name)
                .unwrap();
            si.on_insert(&key, &effective)?;
        }

        // Persist the new schema version.
        let si = storage
            .secondary_indices
            .iter_mut()
            .find(|si| si.name() == name)
            .unwrap();
        si.persist_schema_version(new_version)?;
        si.fsync()?;

        Ok(())
    }

    // ── Structural operations ─────────────────────────────────────────────── //

    /// Compact the database, removing tombstones and stale values.
    ///
    /// Rewrites the data and index files atomically via temp-file rename,
    /// then re-opens them in place.
    ///
    /// # Deadlock warning
    /// Acquires the database lock internally.  Must not be called while a
    /// [`Transaction`] is live on the same thread.  These operations are
    /// intended for offline administration — commit or roll back all open
    /// transactions before calling them.
    ///
    /// # Example
    /// ```
    /// # use tempfile::TempDir;
    /// # use highlandcows_isam::Isam;
    /// # let dir = TempDir::new().unwrap();
    /// # let path = dir.path().join("db");
    /// # let db: Isam<u32, String> = Isam::create(&path).unwrap();
    /// # let mut txn = db.begin_transaction().unwrap();
    /// # for i in 0u32..5 { db.insert(&mut txn, i, &i.to_string()).unwrap(); }
    /// # for i in 0u32..3 { db.delete(&mut txn, &i).unwrap(); }
    /// # txn.commit().unwrap();
    /// // All transactions committed — safe to compact.
    /// db.compact().unwrap();
    /// ```
    pub fn compact(&self) -> IsamResult<()> {
        let mut storage = self.manager.lock_storage()?;

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
    /// Records are rewritten to new temp files and atomically renamed into
    /// place.  The key schema version is preserved.
    ///
    /// # Deadlock warning
    /// Acquires the database lock internally.  Must not be called while a
    /// [`Transaction`] is live on the same thread.  These operations are
    /// intended for offline administration — commit or roll back all open
    /// transactions before calling them.
    ///
    /// # Example
    /// ```
    /// # use tempfile::TempDir;
    /// # use highlandcows_isam::Isam;
    /// # let dir = TempDir::new().unwrap();
    /// # let path = dir.path().join("db");
    /// let db: Isam<u32, String> = Isam::create(&path).unwrap();
    /// # let mut txn = db.begin_transaction().unwrap();
    /// # db.insert(&mut txn, 1u32, &"42".to_string()).unwrap();
    /// # txn.commit().unwrap();
    /// // Migrate String values → u64, setting val schema version to 1.
    /// let db2: Isam<u32, u64> = db
    ///     .migrate_values(1, |s: String| Ok(s.parse::<u64>().unwrap()))
    ///     .unwrap();
    /// assert_eq!(db2.val_schema_version().unwrap(), 1);
    /// ```
    pub fn migrate_values<V2, F>(self, new_val_version: u32, mut f: F) -> IsamResult<Isam<K, V2>>
    where
        V2: Serialize + DeserializeOwned + Clone + 'static,
        F: FnMut(V) -> IsamResult<V2>,
    {
        let mut storage = self.manager.lock_storage()?;

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
    /// Records are rewritten to new temp files and atomically renamed into
    /// place.  The value schema version is preserved.
    ///
    /// # Deadlock warning
    /// Acquires the database lock internally.  Must not be called while a
    /// [`Transaction`] is live on the same thread.  These operations are
    /// intended for offline administration — commit or roll back all open
    /// transactions before calling them.
    ///
    /// # Example
    /// ```
    /// # use tempfile::TempDir;
    /// # use highlandcows_isam::Isam;
    /// # let dir = TempDir::new().unwrap();
    /// # let path = dir.path().join("db");
    /// let db: Isam<u32, String> = Isam::create(&path).unwrap();
    /// # let mut txn = db.begin_transaction().unwrap();
    /// # db.insert(&mut txn, 1u32, &"one".to_string()).unwrap();
    /// # txn.commit().unwrap();
    /// // Migrate u32 keys → String, setting key schema version to 1.
    /// let db2: Isam<String, String> = db
    ///     .migrate_keys(1, |k: u32| Ok(format!("{k}")))
    ///     .unwrap();
    /// assert_eq!(db2.key_schema_version().unwrap(), 1);
    /// ```
    pub fn migrate_keys<K2, F>(self, new_key_version: u32, mut f: F) -> IsamResult<Isam<K2, V>>
    where
        K2: Serialize + DeserializeOwned + Ord + Clone + 'static,
        F: FnMut(K) -> IsamResult<K2>,
    {
        let mut storage = self.manager.lock_storage()?;

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

/// Key-order iterator over all alive records.
///
/// Created by [`Isam::iter`].  Borrows the [`Transaction`] for its lifetime,
/// preventing other operations until the iterator is dropped.
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

/// Key-order iterator over records whose key falls within a given range.
///
/// Created by [`Isam::range`].  Borrows the [`Transaction`] for its lifetime,
/// preventing other operations until the iterator is dropped.
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

// ── SecondaryIndexHandle ──────────────────────────────────────────────────── //

/// An opaque handle to a secondary index, used for point lookups.
///
/// Obtained from [`Isam::index`].
pub struct SecondaryIndexHandle<K, V, SK> {
    name: String,
    _phantom: PhantomData<fn() -> (K, V, SK)>,
}

impl<K, V, SK> SecondaryIndexHandle<K, V, SK>
where
    K: Serialize + DeserializeOwned + Ord + Clone,
    V: Serialize + DeserializeOwned,
    SK: Serialize + DeserializeOwned + Ord + Clone,
{
    /// Return all `(primary_key, value)` pairs whose secondary key equals `sk`.
    ///
    /// Results are returned in insertion order (not key order).  For a
    /// non-existent secondary key the result is an empty `Vec`.
    ///
    /// # Example
    /// ```
    /// # use tempfile::TempDir;
    /// use serde::{Serialize, Deserialize};
    /// use highlandcows_isam::{Isam, DeriveKey};
    ///
    /// #[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
    /// struct User { name: String, city: String }
    ///
    /// struct CityIndex;
    /// impl DeriveKey<User> for CityIndex {
    ///     type Key = String;
    ///     fn derive(u: &User) -> String { u.city.clone() }
    /// }
    ///
    /// # let dir = TempDir::new().unwrap();
    /// # let path = dir.path().join("db");
    /// let db = Isam::<u64, User>::builder()
    ///     .with_index("city", CityIndex)
    ///     .create(&path)
    ///     .unwrap();
    /// let city_idx = db.index::<CityIndex>("city");
    ///
    /// let mut txn = db.begin_transaction().unwrap();
    /// db.insert(&mut txn, 1, &User { name: "Alice".into(), city: "London".into() }).unwrap();
    /// db.insert(&mut txn, 2, &User { name: "Bob".into(),   city: "London".into() }).unwrap();
    /// db.insert(&mut txn, 3, &User { name: "Carol".into(), city: "Paris".into()  }).unwrap();
    /// txn.commit().unwrap();
    ///
    /// let mut txn = db.begin_transaction().unwrap();
    /// let mut londoners = city_idx.lookup(&mut txn, &"London".to_string()).unwrap();
    /// londoners.sort_by_key(|(pk, _)| *pk);
    /// assert_eq!(londoners[0].0, 1);
    /// assert_eq!(londoners[1].0, 2);
    /// assert_eq!(city_idx.lookup(&mut txn, &"Berlin".to_string()).unwrap(), vec![]);
    /// txn.commit().unwrap();
    /// ```
    pub fn lookup(
        &self,
        txn: &mut Transaction<'_, K, V>,
        sk: &SK,
    ) -> IsamResult<Vec<(K, V)>> {
        let sk_bytes = bincode::serialize(sk)?;

        // Step 1: look up primary keys in the secondary index.
        let pks: Vec<K> = {
            let storage = txn.storage_mut();
            match storage.secondary_indices.iter_mut().find(|si| si.name() == self.name) {
                None => Vec::new(),
                Some(si) => si.lookup_primary_keys(&sk_bytes)?,
            }
        };

        // Step 2: fetch each primary record.
        let storage = txn.storage_mut();
        let mut results = Vec::with_capacity(pks.len());
        for pk in pks {
            if let Some(rec) = storage.index.search(&pk)? {
                let value = storage.store.read_value(rec)?;
                results.push((pk, value));
            }
        }
        Ok(results)
    }
}

// ── IndexInfo ─────────────────────────────────────────────────────────────── //

/// Metadata about a registered secondary index.
///
/// Returned by [`Isam::secondary_indices`].
#[derive(Debug, Clone)]
pub struct IndexInfo {
    /// The name the index was registered under.
    pub name: String,
    /// Fully-qualified type name of the [`DeriveKey`] extractor implementation.
    ///
    /// Provided by [`std::any::type_name`] — suitable for display and logging,
    /// but not for persistent storage (the value may change across compiler
    /// versions or if the type is renamed or moved).
    pub extractor_type: &'static str,
    /// The index schema version stored in the `.sidx` metadata.
    ///
    /// Set to `0` for newly created indices and updated by
    /// [`Isam::migrate_index`].  Use this to confirm that a migration has
    /// been applied, or to detect indices that predate schema versioning.
    pub schema_version: u32,
}

// ── IsamBuilder ───────────────────────────────────────────────────────────── //

/// Builder for creating or opening an [`Isam`] database with secondary indices.
///
/// Obtain a builder via [`Isam::builder`].  Call [`with_index`](Self::with_index)
/// for each secondary index, then [`create`](Self::create) or [`open`](Self::open).
pub struct IsamBuilder<K, V> {
    factories: Vec<(String, Box<dyn FnOnce(&Path) -> IsamResult<Box<dyn AnySecondaryIndex<K, V>>>>)>,
    rebuild: HashSet<String>,
    _phantom: PhantomData<(K, V)>,
}

impl<K, V> Default for IsamBuilder<K, V> {
    fn default() -> Self {
        Self {
            factories: Vec::new(),
            rebuild: HashSet::new(),
            _phantom: PhantomData,
        }
    }
}

impl<K, V> IsamBuilder<K, V>
where
    K: Serialize + DeserializeOwned + Ord + Clone + Send + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + 'static,
{
    /// Register a secondary index to be opened or created alongside the database.
    ///
    /// `name` must be unique within a database. The extractor value is used only
    /// to infer the `DeriveKey` implementation — it is not stored.
    ///
    /// After construction, obtain a typed handle for querying via [`Isam::index`].
    pub fn with_index<E>(mut self, name: &str, _extractor: E) -> Self
    where
        E: DeriveKey<V>,
    {
        let owned = name.to_owned();
        let owned2 = owned.clone();
        self.factories.push((owned, Box::new(move |base: &Path| {
            let si = SecondaryIndexImpl::<K, V, E>::create_or_open(&owned2, base)?;
            Ok(Box::new(si) as Box<dyn AnySecondaryIndex<K, V>>)
        })));
        self
    }

    /// Mark a secondary index to be fully rebuilt from primary data during [`open`](Self::open).
    ///
    /// The existing `.sidb`/`.sidx` files for `name` are deleted at open time
    /// and repopulated by scanning all primary records.  The index must also be
    /// registered via [`with_index`](Self::with_index).
    ///
    /// # When to use
    ///
    /// Call this whenever the [`DeriveKey`] extractor logic has changed and the
    /// on-disk index is therefore stale.  Without a rebuild, queries against a
    /// stale index will silently return incorrect results.
    ///
    /// # Example
    /// ```
    /// # use tempfile::TempDir;
    /// use serde::{Serialize, Deserialize};
    /// use highlandcows_isam::{Isam, DeriveKey};
    ///
    /// #[derive(Serialize, Deserialize, Clone)]
    /// struct User { name: String, city: String }
    ///
    /// struct CityIndex;
    /// impl DeriveKey<User> for CityIndex {
    ///     type Key = String;
    ///     fn derive(u: &User) -> String { u.city.clone() }
    /// }
    ///
    /// # let dir = TempDir::new().unwrap();
    /// # let path = dir.path().join("db");
    /// # Isam::<u64, User>::builder().with_index("city", CityIndex).create(&path).unwrap();
    /// // Reopen and force a full rebuild of the "city" index.
    /// let db = Isam::<u64, User>::builder()
    ///     .with_index("city", CityIndex)
    ///     .rebuild_index("city")
    ///     .open(&path)
    ///     .unwrap();
    /// ```
    pub fn rebuild_index(mut self, name: &str) -> Self {
        self.rebuild.insert(name.to_owned());
        self
    }

    /// Create a new, empty database at `path` with the registered indices.
    ///
    /// # Example
    /// ```
    /// # use tempfile::TempDir;
    /// use serde::{Serialize, Deserialize};
    /// use highlandcows_isam::{Isam, DeriveKey};
    ///
    /// #[derive(Serialize, Deserialize, Clone)]
    /// struct User { name: String, city: String }
    ///
    /// struct CityIndex;
    /// impl DeriveKey<User> for CityIndex {
    ///     type Key = String;
    ///     fn derive(u: &User) -> String { u.city.clone() }
    /// }
    ///
    /// # let dir = TempDir::new().unwrap();
    /// # let path = dir.path().join("db");
    /// let db = Isam::<u64, User>::builder()
    ///     .with_index("city", CityIndex)
    ///     .create(&path)
    ///     .unwrap();
    /// ```
    pub fn create(self, path: impl AsRef<Path>) -> IsamResult<Isam<K, V>> {
        let path = path.as_ref();
        let mut storage = IsamStorage::create(path)?;
        for (_name, factory) in self.factories {
            storage.secondary_indices.push(factory(path)?);
        }
        Ok(Isam {
            manager: TransactionManager::from_storage(storage),
        })
    }

    /// Open an existing database at `path` with the registered indices.
    ///
    /// Secondary indices must be registered again on every open — the index
    /// name links the handle to the files on disk, but the extractor type is
    /// not persisted.
    ///
    /// # Example
    /// ```
    /// # use tempfile::TempDir;
    /// use serde::{Serialize, Deserialize};
    /// use highlandcows_isam::{Isam, DeriveKey};
    ///
    /// #[derive(Serialize, Deserialize, Clone)]
    /// struct User { name: String, city: String }
    ///
    /// struct CityIndex;
    /// impl DeriveKey<User> for CityIndex {
    ///     type Key = String;
    ///     fn derive(u: &User) -> String { u.city.clone() }
    /// }
    ///
    /// # let dir = TempDir::new().unwrap();
    /// # let path = dir.path().join("db");
    /// # Isam::<u64, User>::builder().with_index("city", CityIndex).create(&path).unwrap();
    /// let db = Isam::<u64, User>::builder()
    ///     .with_index("city", CityIndex)
    ///     .open(&path)
    ///     .unwrap();
    /// let city_idx = db.index::<CityIndex>("city");
    /// ```
    pub fn open(self, path: impl AsRef<Path>) -> IsamResult<Isam<K, V>> {
        use crate::secondary_index::{sidb_path, sidx_path};

        let path = path.as_ref();
        let mut storage = IsamStorage::open(path)?;

        // Delete stale files for any indices marked for rebuild so the
        // factories below recreate them fresh.
        for name in &self.rebuild {
            let sidb = sidb_path(path, name);
            let sidx = sidx_path(path, name);
            if sidb.exists() { std::fs::remove_file(&sidb)?; }
            if sidx.exists() { std::fs::remove_file(&sidx)?; }
        }

        for (_name, factory) in self.factories {
            storage.secondary_indices.push(factory(path)?);
        }

        // Populate rebuilt indices by scanning all primary records.
        if !self.rebuild.is_empty() {
            let first_id = storage.index.first_leaf_id()?;
            let mut current_id = first_id;
            while current_id != 0 {
                let (entries, next_id) = storage.index.read_leaf(current_id)?;
                for (key, rec) in &entries {
                    let value: V = storage.store.read_value(*rec)?;
                    for si in &mut storage.secondary_indices {
                        if self.rebuild.contains(si.name()) {
                            si.on_insert(key, &value)?;
                        }
                    }
                }
                current_id = next_id;
            }
            for si in &mut storage.secondary_indices {
                if self.rebuild.contains(si.name()) {
                    si.fsync()?;
                }
            }
        }

        Ok(Isam {
            manager: TransactionManager::from_storage(storage),
        })
    }
}
