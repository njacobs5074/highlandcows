/// Pager — raw page read/write on the `.idx` B-tree index file.
///
/// ## Page 0 — metadata header (fixed layout)
/// ```text
/// [magic:        8 bytes ]   b"ISAMIDX\0"
/// [page_size:    u32 LE  ]   always 4096
/// [root_page_id: u32 LE  ]   initially 1; changes as tree grows
/// [page_count:   u32 LE  ]   total allocated pages (including page 0)
/// ```
///
/// ## Data pages (pages 1..page_count-1)
///
/// Each page is exactly `PAGE_SIZE` bytes.  The content is interpreted
/// by the B-tree layer; the pager only handles raw byte I/O.
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::error::{IsamError, IsamResult};

pub const PAGE_SIZE: usize = 4096;
pub const MAGIC: &[u8; 8] = b"ISAMIDX\0";

/// The decoded metadata from page 0.
#[derive(Debug, Clone)]
pub struct IndexMeta {
    pub root_page_id: u32,
    pub page_count: u32,
    pub key_schema_version: u32,
    pub val_schema_version: u32,
}

pub struct Pager {
    file: File,
    pub meta: IndexMeta,
}

impl Pager {
    /// Create a brand-new index file. Writes page 0 (metadata) and
    /// allocates page 1 as an empty leaf (the initial root).
    pub fn create(path: &Path) -> IsamResult<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;

        // Write page 0 — metadata.
        let meta_bytes = Self::encode_meta(1, 2, 0, 0); // root=1, 2 pages total, versions 0,0
        file.write_all(&meta_bytes)?;

        // Write page 1 — empty leaf root.
        let leaf = Self::empty_leaf_page();
        file.write_all(&leaf)?;

        file.flush()?;

        Ok(Self {
            file,
            meta: IndexMeta {
                root_page_id: 1,
                page_count: 2,
                key_schema_version: 0,
                val_schema_version: 0,
            },
        })
    }

    /// Open an existing index file and read its metadata header.
    pub fn open(path: &Path) -> IsamResult<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let mut pager = Self {
            file,
            meta: IndexMeta {
                root_page_id: 0,
                page_count: 0,
                key_schema_version: 0,
                val_schema_version: 0,
            },
        };
        pager.read_meta()?;
        Ok(pager)
    }

    // ------------------------------------------------------------------ //
    //  Page I/O
    // ------------------------------------------------------------------ //

    /// Read page `id` into a fresh `PAGE_SIZE`-byte buffer.
    pub fn read_page(&mut self, id: u32) -> IsamResult<Vec<u8>> {
        let offset = id as u64 * PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset))?;
        let mut buf = vec![0u8; PAGE_SIZE];
        self.file.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// Write `data` (must be exactly `PAGE_SIZE` bytes) to page `id`.
    pub fn write_page(&mut self, id: u32, data: &[u8]) -> IsamResult<()> {
        assert_eq!(data.len(), PAGE_SIZE, "page must be exactly PAGE_SIZE bytes");
        let offset = id as u64 * PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(data)?;
        Ok(())
    }

    /// Allocate a new blank page at the end of the file and return its id.
    pub fn alloc_page(&mut self) -> IsamResult<u32> {
        let new_id = self.meta.page_count;
        // Extend the file by one page of zeros.
        let offset = new_id as u64 * PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(&[0u8; PAGE_SIZE])?;
        self.meta.page_count += 1;
        self.flush_meta()?;
        Ok(new_id)
    }

    // ------------------------------------------------------------------ //
    //  Metadata helpers
    // ------------------------------------------------------------------ //

    /// Re-read page 0 and populate `self.meta`.
    fn read_meta(&mut self) -> IsamResult<()> {
        self.file.seek(SeekFrom::Start(0))?;
        let mut buf = [0u8; PAGE_SIZE];
        self.file.read_exact(&mut buf)?;

        if &buf[0..8] != MAGIC {
            return Err(IsamError::CorruptIndex(
                "bad magic number in index header".into(),
            ));
        }
        let page_size = u32::from_le_bytes(buf[8..12].try_into().unwrap());
        if page_size as usize != PAGE_SIZE {
            return Err(IsamError::CorruptIndex(format!(
                "page_size mismatch: file has {page_size}, library expects {PAGE_SIZE}"
            )));
        }
        self.meta.root_page_id = u32::from_le_bytes(buf[12..16].try_into().unwrap());
        self.meta.page_count = u32::from_le_bytes(buf[16..20].try_into().unwrap());
        self.meta.key_schema_version = u32::from_le_bytes(buf[20..24].try_into().unwrap());
        self.meta.val_schema_version = u32::from_le_bytes(buf[24..28].try_into().unwrap());
        Ok(())
    }

    /// Write the current `self.meta` back to page 0.
    pub fn flush_meta(&mut self) -> IsamResult<()> {
        let bytes = Self::encode_meta(
            self.meta.root_page_id,
            self.meta.page_count,
            self.meta.key_schema_version,
            self.meta.val_schema_version,
        );
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&bytes)?;
        Ok(())
    }

    /// Flush OS write buffers.
    pub fn flush(&mut self) -> IsamResult<()> {
        self.file.flush()?;
        Ok(())
    }

    /// Flush and fsync for durability.
    pub fn fsync(&mut self) -> IsamResult<()> {
        self.file.flush()?;
        self.file.sync_all()?;
        Ok(())
    }

    // ------------------------------------------------------------------ //
    //  Static encoding helpers
    // ------------------------------------------------------------------ //

    fn encode_meta(root_page_id: u32, page_count: u32, key_schema_version: u32, val_schema_version: u32) -> Vec<u8> {
        let mut buf = vec![0u8; PAGE_SIZE];
        buf[0..8].copy_from_slice(MAGIC);
        let page_size = PAGE_SIZE as u32;
        buf[8..12].copy_from_slice(&page_size.to_le_bytes());
        buf[12..16].copy_from_slice(&root_page_id.to_le_bytes());
        buf[16..20].copy_from_slice(&page_count.to_le_bytes());
        buf[20..24].copy_from_slice(&key_schema_version.to_le_bytes());
        buf[24..28].copy_from_slice(&val_schema_version.to_le_bytes());
        buf
    }

    /// Returns an empty leaf page (all zeros except the page_type byte).
    pub fn empty_leaf_page() -> Vec<u8> {
        let mut buf = vec![0u8; PAGE_SIZE];
        buf[0] = PAGE_TYPE_LEAF;
        // num_entries (u16) at offset 1 = 0
        // next_leaf_id (u32) at offset 3 = 0  (0 = end of list)
        buf
    }

    /// Returns an empty internal page.
    pub fn empty_internal_page() -> Vec<u8> {
        let mut buf = vec![0u8; PAGE_SIZE];
        buf[0] = PAGE_TYPE_INTERNAL;
        buf
    }
}

// Page type constants shared with the B-tree layer.
pub const PAGE_TYPE_LEAF: u8 = 0;
pub const PAGE_TYPE_INTERNAL: u8 = 1;
