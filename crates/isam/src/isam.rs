/// `IsamFile<K, V>` — the top-level public handle for an ISAM database.
///
/// Each logical database is backed by two files:
/// - `<name>.idb`  — append-only data records
/// - `<name>.idx`  — on-disk B-tree index
///
/// ## Generic parameters
///
/// - `K` — the key type; must be serializable, deserializable, ordered, and
///   cheap to clone.  These bounds are stated once on the `impl` block rather
///   than on every method.
/// - `V` — the value type; must be serializable and deserializable.
///   `V` is not stored as a field inside the struct, so we use
///   `PhantomData<V>` to tell the Rust type checker that `IsamFile`
///   logically "contains" values of type `V`.
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::error::{IsamError, IsamResult};
use crate::index::BTree;
use crate::store::DataStore;

/// The public ISAM database handle.
///
/// Stores `base_path` so that `compact()` can create sibling temp files
/// and atomically rename them into place.
pub struct Isam<K, V> {
    store: DataStore,
    index: BTree<K>,
    base_path: PathBuf,
    _phantom: PhantomData<V>,
}

impl<K, V> Isam<K, V>
where
    K: Serialize + DeserializeOwned + Ord + Clone,
    V: Serialize + DeserializeOwned,
{
    pub fn create(path: impl AsRef<Path>) -> IsamResult<Self> {
        let base = path.as_ref().to_path_buf();
        Ok(Self {
            store: DataStore::create(&idb_path(&base))?,
            index: BTree::create(&idx_path(&base))?,
            base_path: base,
            _phantom: PhantomData,
        })
    }

    pub fn open(path: impl AsRef<Path>) -> IsamResult<Self> {
        let base = path.as_ref().to_path_buf();
        Ok(Self {
            store: DataStore::open(&idb_path(&base))?,
            index: BTree::open(&idx_path(&base))?,
            base_path: base,
            _phantom: PhantomData,
        })
    }

    pub fn insert(&mut self, key: K, value: &V) -> IsamResult<()> {
        let rec = self.store.append(&key, value)?;
        self.index.insert(&key, rec)?;
        Ok(())
    }

    pub fn get(&mut self, key: &K) -> IsamResult<Option<V>> {
        match self.index.search(key)? {
            None => Ok(None),
            Some(rec) => Ok(Some(self.store.read_value(rec)?)),
        }
    }

    pub fn update(&mut self, key: K, value: &V) -> IsamResult<()> {
        if self.index.search(&key)?.is_none() {
            return Err(IsamError::KeyNotFound);
        }
        let rec = self.store.append(&key, value)?;
        self.index.update(&key, rec)?;
        Ok(())
    }

    pub fn delete(&mut self, key: &K) -> IsamResult<()> {
        self.index.delete(key)?;
        self.store.append_tombstone(key)?;
        Ok(())
    }

    pub fn iter(&mut self) -> IsamResult<IsamIter<'_, K, V>> {
        let first_id = self.index.first_leaf_id()?;
        let (entries, next_id) = if first_id != 0 {
            self.index.read_leaf(first_id)?
        } else {
            (vec![], 0)
        };
        Ok(IsamIter {
            isam: self,
            buffer: entries,
            buf_pos: 0,
            next_leaf_id: next_id,
        })
    }

    pub fn compact(&mut self) -> IsamResult<()> {
        // Collect alive records in key order from the leaf chain.
        let mut records: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let first_id = self.index.first_leaf_id()?;
        let mut current_id = first_id;
        while current_id != 0 {
            let (entries, next_id) = self.index.read_leaf(current_id)?;
            for (_, rec) in &entries {
                let (_status, key_bytes, val_bytes) = self.store.read_record_raw(rec.offset)?;
                records.push((key_bytes, val_bytes));
            }
            current_id = next_id;
        }

        // Write to temp files.
        let tmp_idb = self.base_path.with_extension("idb.tmp");
        let tmp_idx = self.base_path.with_extension("idx.tmp");

        let mut new_store = DataStore::create(&tmp_idb)?;
        let mut new_index: BTree<K> = BTree::create(&tmp_idx)?;

        for (key_bytes, val_bytes) in &records {
            let rec = new_store.write_raw_record(crate::store::STATUS_ALIVE, key_bytes, val_bytes)?;
            let key: K = bincode::deserialize(key_bytes)?;
            new_index.insert(&key, rec)?;
        }

        new_store.flush()?;
        new_index.flush()?;

        // Drop file handles before renaming.
        drop(new_store);
        drop(new_index);

        // Atomically replace old files.
        std::fs::rename(&tmp_idb, idb_path(&self.base_path))?;
        std::fs::rename(&tmp_idx, idx_path(&self.base_path))?;

        // Re-open the fresh files.
        self.store = DataStore::open(&idb_path(&self.base_path))?;
        self.index = BTree::open(&idx_path(&self.base_path))?;

        Ok(())
    }
}

// ───────────────────────────────────────────────────────────────────────── //
//  Iterator
// ───────────────────────────────────────────────────────────────────────── //

/// Key-order iterator over all alive records.
///
/// The lifetime `'a` ties the iterator to the `Isam` it was created from.
/// While this iterator exists, `isam` is mutably borrowed, so you can't
/// call `insert`/`delete` at the same time — the borrow checker enforces
/// this at compile time.
pub struct IsamIter<'a, K, V> {
    isam: &'a mut Isam<K, V>,
    buffer: Vec<(K, crate::store::RecordRef)>,
    buf_pos: usize,
    next_leaf_id: u32,
}

/// `impl Iterator` means we implement the standard `Iterator` trait, giving
/// the caller access to `for` loops, `.collect()`, `.map()`, etc. for free.
impl<'a, K, V> Iterator for IsamIter<'a, K, V>
where
    K: Serialize + DeserializeOwned + Ord + Clone,
    V: Serialize + DeserializeOwned,
{
    /// Each `next()` call yields either `Some(Ok((K, V)))` or `Some(Err(_))`
    /// (for I/O errors), and `None` when exhausted.
    type Item = IsamResult<(K, V)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Serve from the in-memory buffer first.
            if self.buf_pos < self.buffer.len() {
                let (key, rec) = self.buffer[self.buf_pos].clone();
                self.buf_pos += 1;
                return Some(
                    self.isam
                        .store
                        .read_value(rec)
                        .map(|value| (key, value)),
                );
            }

            // Buffer exhausted — load the next leaf page.
            if self.next_leaf_id == 0 {
                return None; // end of the leaf chain
            }

            match self.isam.index.read_leaf(self.next_leaf_id) {
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

// ───────────────────────────────────────────────────────────────────────── //
//  Path helpers
// ───────────────────────────────────────────────────────────────────────── //

fn idb_path(base: &Path) -> PathBuf {
    base.with_extension("idb")
}

fn idx_path(base: &Path) -> PathBuf {
    base.with_extension("idx")
}
