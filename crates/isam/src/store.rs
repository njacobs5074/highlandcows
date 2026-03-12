/// DataStore — manages the append-only `.idb` data file.
///
/// ## Record format (per record, sequentially appended)
/// ```text
/// [status:   u8       ]   0 = alive, 1 = tombstone
/// [key_len:  u32 LE   ]
/// [val_len:  u32 LE   ]
/// [key:      key_len bytes]   bincode-encoded key
/// [val:      val_len bytes]   bincode-encoded value (0 bytes for tombstones)
/// ```
///
/// Because records are only ever appended (never overwritten), the file
/// grows monotonically.  Stale or deleted records are reclaimed by
/// `IsamFile::compact()`.
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::error::IsamResult;

pub const STATUS_ALIVE: u8 = 0;
pub const STATUS_TOMBSTONE: u8 = 1;

/// Returned by `DataStore::append` so the caller can record the record's
/// location in the B-tree index.
#[derive(Debug, Clone, Copy)]
pub struct RecordRef {
    /// Byte offset of the *start* of this record in the `.idb` file.
    pub offset: u64,
    /// Total byte length of the encoded record (header + key + value).
    pub len: u32,
}

pub struct DataStore {
    file: File,
}

impl DataStore {
    /// Create a new, empty `.idb` file, truncating any existing one.
    pub fn create(path: &Path) -> IsamResult<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        Ok(Self { file })
    }

    /// Open an existing `.idb` file for reading and appending.
    pub fn open(path: &Path) -> IsamResult<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        Ok(Self { file })
    }

    /// Append a live record to the file.
    ///
    /// Returns a `RecordRef` containing the byte offset and length so the
    /// B-tree index can locate this record later.
    pub fn append<K, V>(&mut self, key: &K, value: &V) -> IsamResult<RecordRef>
    where
        K: Serialize,
        V: Serialize,
    {
        // bincode::serialize converts a value to a Vec<u8>.
        // The `?` operator propagates any error upward automatically.
        let key_bytes = bincode::serialize(key)?;
        let val_bytes = bincode::serialize(value)?;

        // Seek to the end so we always append.
        // `seek` returns the new absolute position — that's our offset.
        let offset = self.file.seek(SeekFrom::End(0))?;

        let key_len = key_bytes.len() as u32;
        let val_len = val_bytes.len() as u32;

        // Write the 9-byte header: status + key_len + val_len.
        // `to_le_bytes()` converts an integer to little-endian byte array.
        self.file.write_all(&[STATUS_ALIVE])?;
        self.file.write_all(&key_len.to_le_bytes())?;
        self.file.write_all(&val_len.to_le_bytes())?;
        self.file.write_all(&key_bytes)?;
        self.file.write_all(&val_bytes)?;

        // Total record size = 1 (status) + 4 (key_len) + 4 (val_len) + key + val
        let len = 1 + 4 + 4 + key_len + val_len;
        Ok(RecordRef { offset, len })
    }

    /// Append a tombstone record for `key`.
    ///
    /// The value portion is zero bytes; the B-tree entry will be removed
    /// separately, so tombstones in the data file are only needed for
    /// compaction safety.
    pub fn append_tombstone<K>(&mut self, key: &K) -> IsamResult<()>
    where
        K: Serialize,
    {
        let key_bytes = bincode::serialize(key)?;
        self.file.seek(SeekFrom::End(0))?;

        let key_len = key_bytes.len() as u32;
        let val_len: u32 = 0;

        self.file.write_all(&[STATUS_TOMBSTONE])?;
        self.file.write_all(&key_len.to_le_bytes())?;
        self.file.write_all(&val_len.to_le_bytes())?;
        self.file.write_all(&key_bytes)?;
        Ok(())
    }

    /// Read and deserialize the *value* portion of the record at `rec`.
    ///
    /// `&mut self` because `Seek` requires mutability on the file handle.
    pub fn read_value<V>(&mut self, rec: RecordRef) -> IsamResult<V>
    where
        V: DeserializeOwned,
    {
        // Jump to the record start.
        self.file.seek(SeekFrom::Start(rec.offset))?;

        // Read header bytes.
        let mut header = [0u8; 9];
        self.file.read_exact(&mut header)?;

        let _status = header[0];
        let key_len = u32::from_le_bytes(header[1..5].try_into().unwrap()) as usize;
        let val_len = u32::from_le_bytes(header[5..9].try_into().unwrap()) as usize;

        // Skip over the key bytes.
        self.file.seek(SeekFrom::Current(key_len as i64))?;

        // Read the value bytes and deserialize.
        let mut val_buf = vec![0u8; val_len];
        self.file.read_exact(&mut val_buf)?;
        let value: V = bincode::deserialize(&val_buf)?;
        Ok(value)
    }

    /// Read the raw bytes of a record (for use during compaction).
    ///
    /// Returns `(status, key_bytes, val_bytes)`.
    pub fn read_record_raw(&mut self, offset: u64) -> IsamResult<(u8, Vec<u8>, Vec<u8>)> {
        self.file.seek(SeekFrom::Start(offset))?;

        let mut header = [0u8; 9];
        self.file.read_exact(&mut header)?;

        let status = header[0];
        let key_len = u32::from_le_bytes(header[1..5].try_into().unwrap()) as usize;
        let val_len = u32::from_le_bytes(header[5..9].try_into().unwrap()) as usize;

        let mut key_buf = vec![0u8; key_len];
        self.file.read_exact(&mut key_buf)?;

        let mut val_buf = vec![0u8; val_len];
        self.file.read_exact(&mut val_buf)?;

        Ok((status, key_buf, val_buf))
    }

    /// Write a raw pre-encoded record directly (used during compaction to
    /// copy records without re-serializing).
    pub fn write_raw_record(
        &mut self,
        status: u8,
        key_bytes: &[u8],
        val_bytes: &[u8],
    ) -> IsamResult<RecordRef> {
        let offset = self.file.seek(SeekFrom::End(0))?;

        let key_len = key_bytes.len() as u32;
        let val_len = val_bytes.len() as u32;

        self.file.write_all(&[status])?;
        self.file.write_all(&key_len.to_le_bytes())?;
        self.file.write_all(&val_len.to_le_bytes())?;
        self.file.write_all(key_bytes)?;
        self.file.write_all(val_bytes)?;

        let len = 1 + 4 + 4 + key_len + val_len;
        Ok(RecordRef { offset, len })
    }

    /// Flush OS buffers to disk.
    pub fn flush(&mut self) -> IsamResult<()> {
        self.file.flush()?;
        Ok(())
    }

    /// Flush OS buffers and call `fsync` to ensure durability.
    pub fn fsync(&mut self) -> IsamResult<()> {
        self.file.flush()?;
        self.file.sync_all()?;
        Ok(())
    }
}
