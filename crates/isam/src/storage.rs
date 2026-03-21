/// `IsamStorage<K, V>` — the raw I/O state for an ISAM database.
///
/// This is the inner state shared behind an `Arc<Mutex<>>`.  It is never
/// exposed publicly; callers interact with it only through `Transaction`.
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::error::IsamResult;
use crate::index::BTree;
use crate::isam::{idb_path, idx_path};
use crate::secondary_index::AnySecondaryIndex;
use crate::store::DataStore;

pub(crate) struct IsamStorage<K, V> {
    pub(crate) store: DataStore,
    pub(crate) index: BTree<K>,
    pub(crate) base_path: PathBuf,
    pub(crate) secondary_indices: Vec<Box<dyn AnySecondaryIndex<K, V>>>,
    pub(crate) _phantom: PhantomData<V>,
}

impl<K, V> IsamStorage<K, V>
where
    K: Serialize + DeserializeOwned + Ord + Clone,
    V: Serialize + DeserializeOwned,
{
    pub(crate) fn create(path: &Path) -> IsamResult<Self> {
        let base = path.to_path_buf();
        Ok(Self {
            store: DataStore::create(&idb_path(&base))?,
            index: BTree::create(&idx_path(&base))?,
            base_path: base,
            secondary_indices: Vec::new(),
            _phantom: PhantomData,
        })
    }

    pub(crate) fn open(path: &Path) -> IsamResult<Self> {
        let base = path.to_path_buf();
        Ok(Self {
            store: DataStore::open(&idb_path(&base))?,
            index: BTree::open(&idx_path(&base))?,
            base_path: base,
            secondary_indices: Vec::new(),
            _phantom: PhantomData,
        })
    }

    /// Flush store, index, and all secondary indices to disk (called at commit).
    pub(crate) fn fsync(&mut self) -> IsamResult<()> {
        self.store.fsync()?;
        self.index.fsync()?;
        for si in &mut self.secondary_indices {
            si.fsync()?;
        }
        Ok(())
    }
}
