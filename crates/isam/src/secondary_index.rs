/// Secondary index support for `Isam<K, V>`.
///
/// A secondary index maps a derived key (`SK`) to the set of primary keys
/// (`K`) whose values produce that secondary key.  One secondary key can map
/// to many primary keys (non-unique index).
///
/// # Files on disk
///
/// Each named secondary index uses two files alongside the primary store:
///
/// | File | Contents |
/// |------|----------|
/// | `<base>_<name>.sidb` | Append-only store of serialised `Vec<K>` buckets |
/// | `<base>_<name>.sidx` | B-tree (`SK → RecordRef`) pointing into `.sidb` |
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::error::IsamResult;
use crate::index::BTree;
use crate::store::DataStore;

// ── DeriveKey ─────────────────────────────────────────────────────────────── //

/// Describes how to derive a secondary index key from a record value.
///
/// Implement this trait on a marker struct, one per secondary index.
/// For composite indices in the future, set `Key` to a tuple type —
/// no change to this trait is required.
///
/// # Example
/// ```
/// use serde::{Serialize, Deserialize};
/// use highlandcows_isam::DeriveKey;
///
/// #[derive(Serialize, Deserialize, Clone)]
/// struct User { name: String, city: String }
///
/// struct CityIndex;
/// impl DeriveKey<User> for CityIndex {
///     type Key = String;
///     fn derive(value: &User) -> String { value.city.clone() }
/// }
/// ```
pub trait DeriveKey<V>: Send + Sync + 'static {
    /// The type of the derived secondary key.
    type Key: Serialize + DeserializeOwned + Ord + Clone + Send;

    /// Derive the secondary key from a value.
    fn derive(value: &V) -> Self::Key;
}

// ── AnySecondaryIndex ─────────────────────────────────────────────────────── //

/// Type-erased secondary index interface stored inside `IsamStorage`.
///
/// All methods receive the primary key and deserialized value so the concrete
/// implementation can extract `SK` without the storage layer knowing about it.
pub(crate) trait AnySecondaryIndex<K, V>: Send {
    // ── Forward operations (called during CRUD) ───────────────────────── //

    fn on_insert(&mut self, key: &K, value: &V) -> IsamResult<()>;
    fn on_update(&mut self, key: &K, old_value: &V, new_value: &V) -> IsamResult<()>;
    fn on_delete(&mut self, key: &K, value: &V) -> IsamResult<()>;

    // ── Inverse operations (called during rollback) ───────────────────── //

    fn undo_insert(&mut self, key: &K, value: &V) -> IsamResult<()>;
    fn undo_update(&mut self, key: &K, old_value: &V, new_value: &V) -> IsamResult<()>;
    fn undo_delete(&mut self, key: &K, value: &V) -> IsamResult<()>;

    /// Return all primary keys whose secondary key serialises to `sk_bytes`.
    fn lookup_primary_keys(&mut self, sk_bytes: &[u8]) -> IsamResult<Vec<K>>;

    fn fsync(&mut self) -> IsamResult<()>;
    fn name(&self) -> &str;

    /// Return the fully-qualified type name of the `DeriveKey` extractor.
    ///
    /// Uses [`std::any::type_name`] — suitable for display, not for persistent
    /// storage (the value can change between compiler versions or refactors).
    fn extractor_type_name(&self) -> &'static str;
}

// ── SecondaryIndexImpl ────────────────────────────────────────────────────── //

/// Concrete secondary index backed by a `DataStore` + `BTree<SK>` pair.
///
/// The data store holds serialised `Vec<K>` buckets (one per distinct SK value).
/// The B-tree maps each SK to the `RecordRef` of its current bucket in the store.
pub(crate) struct SecondaryIndexImpl<K, V, E>
where
    E: DeriveKey<V>,
{
    name: String,
    store: DataStore,
    btree: BTree<E::Key>,
    _phantom: PhantomData<(K, V)>,
}

impl<K, V, E> SecondaryIndexImpl<K, V, E>
where
    K: Serialize + DeserializeOwned + Ord + Clone + Send,
    V: Send,
    E: DeriveKey<V>,
{
    /// Create new secondary index files for `name` alongside `base`.
    pub(crate) fn create(name: &str, base: &Path) -> IsamResult<Self> {
        Ok(Self {
            name: name.to_owned(),
            store: DataStore::create(&sidb_path(base, name))?,
            btree: BTree::create(&sidx_path(base, name))?,
            _phantom: PhantomData,
        })
    }

    /// Open existing secondary index files for `name` alongside `base`.
    pub(crate) fn open(name: &str, base: &Path) -> IsamResult<Self> {
        Ok(Self {
            name: name.to_owned(),
            store: DataStore::open(&sidb_path(base, name))?,
            btree: BTree::open(&sidx_path(base, name))?,
            _phantom: PhantomData,
        })
    }

    /// Open existing files if present, otherwise create new ones.
    pub(crate) fn create_or_open(name: &str, base: &Path) -> IsamResult<Self> {
        if sidb_path(base, name).exists() {
            Self::open(name, base)
        } else {
            Self::create(name, base)
        }
    }

    // ── Private helpers ───────────────────────────────────────────────── //

    /// Read the current primary-key bucket for `sk`, or an empty vec.
    fn read_pks(&mut self, sk: &E::Key) -> IsamResult<Vec<K>> {
        match self.btree.search(sk)? {
            None => Ok(Vec::new()),
            Some(rec) => self.store.read_value(rec),
        }
    }

    /// Write (append + update index) a primary-key bucket for `sk`.
    fn write_pks(&mut self, sk: &E::Key, pks: &[K]) -> IsamResult<()> {
        let exists = self.btree.search(sk)?.is_some();
        let rec = self.store.append(sk, &pks)?;
        if exists {
            self.btree.update(sk, rec)?;
        } else {
            self.btree.insert(sk, rec)?;
        }
        Ok(())
    }

    /// Add `pk` to the bucket for `sk` (no-op if already present).
    fn add_pk(&mut self, sk: &E::Key, pk: &K) -> IsamResult<()> {
        let mut pks = self.read_pks(sk)?;
        if !pks.contains(pk) {
            pks.push(pk.clone());
            self.write_pks(sk, &pks)?;
        }
        Ok(())
    }

    /// Remove `pk` from the bucket for `sk`.  Deletes the bucket when empty.
    fn remove_pk(&mut self, sk: &E::Key, pk: &K) -> IsamResult<()> {
        let mut pks = self.read_pks(sk)?;
        pks.retain(|k| k != pk);
        if pks.is_empty() {
            // Ignore KeyNotFound — bucket may already be absent.
            let _ = self.btree.delete(sk);
        } else {
            self.write_pks(sk, &pks)?;
        }
        Ok(())
    }
}

impl<K, V, E> AnySecondaryIndex<K, V> for SecondaryIndexImpl<K, V, E>
where
    K: Serialize + DeserializeOwned + Ord + Clone + Send,
    V: Send,
    E: DeriveKey<V>,
{
    fn on_insert(&mut self, key: &K, value: &V) -> IsamResult<()> {
        let sk = E::derive(value);
        self.add_pk(&sk, key)
    }

    fn on_update(&mut self, key: &K, old_value: &V, new_value: &V) -> IsamResult<()> {
        let old_sk = E::derive(old_value);
        let new_sk = E::derive(new_value);
        if old_sk != new_sk {
            self.remove_pk(&old_sk, key)?;
            self.add_pk(&new_sk, key)?;
        }
        Ok(())
    }

    fn on_delete(&mut self, key: &K, value: &V) -> IsamResult<()> {
        let sk = E::derive(value);
        self.remove_pk(&sk, key)
    }

    // Undo operations are exact inverses of the forward operations.

    fn undo_insert(&mut self, key: &K, value: &V) -> IsamResult<()> {
        self.on_delete(key, value)
    }

    fn undo_update(&mut self, key: &K, old_value: &V, new_value: &V) -> IsamResult<()> {
        // Swap old/new to reverse the direction.
        self.on_update(key, new_value, old_value)
    }

    fn undo_delete(&mut self, key: &K, value: &V) -> IsamResult<()> {
        self.on_insert(key, value)
    }

    fn lookup_primary_keys(&mut self, sk_bytes: &[u8]) -> IsamResult<Vec<K>> {
        let sk: E::Key = bincode::deserialize(sk_bytes)?;
        self.read_pks(&sk)
    }

    fn fsync(&mut self) -> IsamResult<()> {
        self.store.fsync()?;
        self.btree.fsync()
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn extractor_type_name(&self) -> &'static str {
        std::any::type_name::<E>()
    }
}

// ── Path helpers ──────────────────────────────────────────────────────────── //

pub(crate) fn sidb_path(base: &Path, name: &str) -> PathBuf {
    let parent = base.parent().unwrap_or(Path::new(""));
    let stem = base.file_stem().unwrap_or_default().to_string_lossy();
    parent.join(format!("{stem}_{name}.sidb"))
}

pub(crate) fn sidx_path(base: &Path, name: &str) -> PathBuf {
    let parent = base.parent().unwrap_or(Path::new(""));
    let stem = base.file_stem().unwrap_or_default().to_string_lossy();
    parent.join(format!("{stem}_{name}.sidx"))
}
