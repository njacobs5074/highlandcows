/// `TransactionManager<K, V>` — owns shared storage and creates transactions.
///
/// This type is `pub(crate)` — callers interact with it only through `Isam`.
/// It is `Clone` (via `Arc::clone`) so that `Isam` can be cloned freely.
use std::path::Path;
use std::sync::{Arc, Mutex};

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::error::{IsamError, IsamResult};
use crate::storage::IsamStorage;
use crate::transaction::Transaction;

pub(crate) struct TransactionManager<K, V> {
    pub(crate) storage: Arc<Mutex<IsamStorage<K, V>>>,
}

impl<K, V> TransactionManager<K, V>
where
    K: Serialize + DeserializeOwned + Ord + Clone,
    V: Serialize + DeserializeOwned,
{
    pub(crate) fn create(path: &Path) -> IsamResult<Self> {
        let storage = IsamStorage::create(path)?;
        Ok(Self {
            storage: Arc::new(Mutex::new(storage)),
        })
    }

    pub(crate) fn open(path: &Path) -> IsamResult<Self> {
        let storage = IsamStorage::open(path)?;
        Ok(Self {
            storage: Arc::new(Mutex::new(storage)),
        })
    }

    pub(crate) fn begin(&self) -> IsamResult<Transaction<'_, K, V>> {
        let guard = self.storage.lock().map_err(|_| IsamError::LockPoisoned)?;
        Ok(Transaction::new(guard))
    }
}

impl<K, V> Clone for TransactionManager<K, V> {
    fn clone(&self) -> Self {
        Self {
            storage: Arc::clone(&self.storage),
        }
    }
}
