/// B-tree index over the `.idx` file.
///
/// ## Page wire format
///
/// **Leaf page** (page_type = 0):
/// ```text
/// [page_type:    u8      ] = 0
/// [num_entries:  u16 LE  ]
/// [next_leaf_id: u32 LE  ]   0 = end of linked list
/// for each entry:
///   [key_len:    u16 LE  ]
///   [key:        bytes   ]   bincode-encoded key
///   [data_offset:u64 LE  ]   byte offset in .idb
///   [data_len:   u32 LE  ]   byte length of record in .idb
/// ```
///
/// **Internal page** (page_type = 1):
/// ```text
/// [page_type:  u8      ] = 1
/// [num_keys:   u16 LE  ]
/// [child_0:    u32 LE  ]
/// for each key i:
///   [key_len:  u16 LE  ]
///   [key:      bytes   ]
///   [child_(i+1): u32 LE]
/// ```
pub mod pager;

use std::path::Path;

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::error::{IsamError, IsamResult};
use crate::store::RecordRef;
use pager::{
    Pager, PAGE_SIZE, PAGE_TYPE_INTERNAL, PAGE_TYPE_LEAF,
};

// ───────────────────────────────────────────────────────────────────────── //
//  In-memory representations of decoded pages
// ───────────────────────────────────────────────────────────────────────── //

#[derive(Debug, Clone)]
struct LeafEntry {
    key_bytes: Vec<u8>,
    data_offset: u64,
    data_len: u32,
}

#[derive(Debug, Clone)]
struct LeafPage {
    next_leaf_id: u32,
    entries: Vec<LeafEntry>,
}

#[derive(Debug, Clone)]
struct InternalPage {
    /// children[i] is the child to the left of keys[i].
    /// children has one more element than keys.
    children: Vec<u32>,
    key_bytes: Vec<Vec<u8>>,
}

// ───────────────────────────────────────────────────────────────────────── //
//  Encode / decode helpers
// ───────────────────────────────────────────────────────────────────────── //

fn encode_leaf(page: &LeafPage) -> Vec<u8> {
    let mut buf = vec![0u8; PAGE_SIZE];
    let mut pos = 0;

    buf[pos] = PAGE_TYPE_LEAF;
    pos += 1;

    let n = page.entries.len() as u16;
    buf[pos..pos + 2].copy_from_slice(&n.to_le_bytes());
    pos += 2;

    buf[pos..pos + 4].copy_from_slice(&page.next_leaf_id.to_le_bytes());
    pos += 4;

    for e in &page.entries {
        let klen = e.key_bytes.len() as u16;
        buf[pos..pos + 2].copy_from_slice(&klen.to_le_bytes());
        pos += 2;
        buf[pos..pos + e.key_bytes.len()].copy_from_slice(&e.key_bytes);
        pos += e.key_bytes.len();
        buf[pos..pos + 8].copy_from_slice(&e.data_offset.to_le_bytes());
        pos += 8;
        buf[pos..pos + 4].copy_from_slice(&e.data_len.to_le_bytes());
        pos += 4;
    }

    buf
}

fn decode_leaf(data: &[u8]) -> IsamResult<LeafPage> {
    if data[0] != PAGE_TYPE_LEAF {
        return Err(IsamError::CorruptIndex("expected leaf page".into()));
    }
    let num_entries = u16::from_le_bytes(data[1..3].try_into().unwrap()) as usize;
    let next_leaf_id = u32::from_le_bytes(data[3..7].try_into().unwrap());

    let mut pos = 7;
    let mut entries = Vec::with_capacity(num_entries);

    for _ in 0..num_entries {
        let klen = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2;
        let key_bytes = data[pos..pos + klen].to_vec();
        pos += klen;
        let data_offset = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let data_len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
        pos += 4;
        entries.push(LeafEntry {
            key_bytes,
            data_offset,
            data_len,
        });
    }

    Ok(LeafPage {
        next_leaf_id,
        entries,
    })
}

fn encode_internal(page: &InternalPage) -> Vec<u8> {
    let mut buf = vec![0u8; PAGE_SIZE];
    let mut pos = 0;

    buf[pos] = PAGE_TYPE_INTERNAL;
    pos += 1;

    let n = page.key_bytes.len() as u16;
    buf[pos..pos + 2].copy_from_slice(&n.to_le_bytes());
    pos += 2;

    // Write child_0 first.
    buf[pos..pos + 4].copy_from_slice(&page.children[0].to_le_bytes());
    pos += 4;

    for (i, kb) in page.key_bytes.iter().enumerate() {
        let klen = kb.len() as u16;
        buf[pos..pos + 2].copy_from_slice(&klen.to_le_bytes());
        pos += 2;
        buf[pos..pos + kb.len()].copy_from_slice(kb);
        pos += kb.len();
        buf[pos..pos + 4].copy_from_slice(&page.children[i + 1].to_le_bytes());
        pos += 4;
    }

    buf
}

fn decode_internal(data: &[u8]) -> IsamResult<InternalPage> {
    if data[0] != PAGE_TYPE_INTERNAL {
        return Err(IsamError::CorruptIndex("expected internal page".into()));
    }
    let num_keys = u16::from_le_bytes(data[1..3].try_into().unwrap()) as usize;
    let mut pos = 3;

    let child_0 = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
    pos += 4;

    let mut children = vec![child_0];
    let mut key_bytes = Vec::with_capacity(num_keys);

    for _ in 0..num_keys {
        let klen = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2;
        let kb = data[pos..pos + klen].to_vec();
        pos += klen;
        let child = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
        pos += 4;
        key_bytes.push(kb);
        children.push(child);
    }

    Ok(InternalPage { children, key_bytes })
}

// ───────────────────────────────────────────────────────────────────────── //
//  Capacity helpers
// ───────────────────────────────────────────────────────────────────────── //

/// Compute how many bytes a leaf entry occupies on the page.
fn leaf_entry_size(key_len: usize) -> usize {
    2 + key_len + 8 + 4 // key_len_field + key + data_offset + data_len
}

/// Compute how many bytes an internal key+right-child pair occupies.
fn internal_key_size(key_len: usize) -> usize {
    2 + key_len + 4 // key_len_field + key + child_id
}

/// Fixed overhead bytes for an empty leaf page.
const LEAF_HEADER: usize = 1 + 2 + 4; // type + num_entries + next_leaf_id
/// Fixed overhead bytes for an empty internal page.
const INTERNAL_HEADER: usize = 1 + 2 + 4; // type + num_keys + child_0

// ───────────────────────────────────────────────────────────────────────── //
//  BTree
// ───────────────────────────────────────────────────────────────────────── //

/// On-disk B-tree index.
///
/// `K` is the key type; it must be serializable, deserializable, ordered,
/// and cheap to clone.  These bounds appear once on the `impl` block so we
/// don't have to repeat them on every method.
pub struct BTree<K> {
    pager: Pager,
    // `PhantomData` tells the compiler that `BTree` logically "owns" values
    // of type `K`, even though the struct field only stores bytes on disk.
    // Without this, Rust would complain that `K` is unused.
    _phantom: std::marker::PhantomData<K>,
}

impl<K> BTree<K>
where
    K: Serialize + DeserializeOwned + Ord + Clone,
{
    pub fn create(path: &Path) -> IsamResult<Self> {
        Ok(Self {
            pager: Pager::create(path)?,
            _phantom: std::marker::PhantomData,
        })
    }

    pub fn open(path: &Path) -> IsamResult<Self> {
        Ok(Self {
            pager: Pager::open(path)?,
            _phantom: std::marker::PhantomData,
        })
    }

    // ------------------------------------------------------------------ //
    //  Public API
    // ------------------------------------------------------------------ //

    /// Look up `key` and return its `RecordRef` if found.
    pub fn search(&mut self, key: &K) -> IsamResult<Option<RecordRef>> {
        let key_bytes = bincode::serialize(key)?;
        let root_id = self.pager.meta.root_page_id;
        self.search_page(root_id, &key_bytes)
    }

    /// Insert `key → rec` into the tree.  Returns `DuplicateKey` if the
    /// key already exists.
    pub fn insert(&mut self, key: &K, rec: RecordRef) -> IsamResult<()> {
        let key_bytes = bincode::serialize(key)?;
        // search_down returns the path from root to the target leaf.
        let root_id = self.pager.meta.root_page_id;
        let path = self.find_leaf_path(root_id, &key_bytes)?;
        let leaf_id = *path.last().unwrap();

        let mut leaf = decode_leaf(&self.pager.read_page(leaf_id)?)?;

        // Check for duplicate.
        if leaf.entries.iter().any(|e| e.key_bytes == key_bytes) {
            return Err(IsamError::DuplicateKey);
        }

        // Insert in sorted position using K::Ord, not raw byte order.
        let pos = leaf
            .entries
            .partition_point(|e| self.cmp_key_bytes(&e.key_bytes, &key_bytes) == std::cmp::Ordering::Less);
        leaf.entries.insert(
            pos,
            LeafEntry {
                key_bytes: key_bytes.clone(),
                data_offset: rec.offset,
                data_len: rec.len,
            },
        );

        // Check if the page fits.
        if self.leaf_fits(&leaf) {
            self.pager.write_page(leaf_id, &encode_leaf(&leaf))?;
        } else {
            // Need to split — do a full insert+split pass from the root.
            self.insert_with_splits(&key_bytes, rec, path)?;
        }

        self.pager.flush()
    }

    /// Delete `key` from the tree.  Returns `KeyNotFound` if absent.
    pub fn delete(&mut self, key: &K) -> IsamResult<()> {
        let key_bytes = bincode::serialize(key)?;
        let root_id = self.pager.meta.root_page_id;
        let path = self.find_leaf_path(root_id, &key_bytes)?;
        let leaf_id = *path.last().unwrap();

        let mut leaf = decode_leaf(&self.pager.read_page(leaf_id)?)?;

        let pos = leaf
            .entries
            .iter()
            .position(|e| e.key_bytes == key_bytes)
            .ok_or(IsamError::KeyNotFound)?;
        leaf.entries.remove(pos);

        self.pager.write_page(leaf_id, &encode_leaf(&leaf))?;

        // Under-fill handling: borrow from sibling or merge.
        // We walk back up the path attempting to rebalance.
        if path.len() >= 2 {
            let parent_id = path[path.len() - 2];
            self.rebalance_after_delete(&path, leaf_id, parent_id)?;
        }

        self.pager.flush()
    }

    /// Update the `RecordRef` stored for `key`.
    pub fn update(&mut self, key: &K, rec: RecordRef) -> IsamResult<()> {
        let key_bytes = bincode::serialize(key)?;
        let root_id = self.pager.meta.root_page_id;
        let path = self.find_leaf_path(root_id, &key_bytes)?;
        let leaf_id = *path.last().unwrap();

        let mut leaf = decode_leaf(&self.pager.read_page(leaf_id)?)?;

        let entry = leaf
            .entries
            .iter_mut()
            .find(|e| e.key_bytes == key_bytes)
            .ok_or(IsamError::KeyNotFound)?;
        entry.data_offset = rec.offset;
        entry.data_len = rec.len;

        self.pager.write_page(leaf_id, &encode_leaf(&leaf))?;
        self.pager.flush()
    }

    /// Return the leaf page id where `key` would be found (or inserted).
    ///
    /// Unlike `search`, this never returns `None` — it always finds the
    /// appropriate leaf even if the key is not present.  Used by range scans
    /// to position the iterator at the correct starting leaf.
    pub fn find_leaf_for_key(&mut self, key: &K) -> IsamResult<u32> {
        let key_bytes = bincode::serialize(key)?;
        let root_id = self.pager.meta.root_page_id;
        let path = self.find_leaf_path(root_id, &key_bytes)?;
        Ok(*path.last().unwrap())
    }

    /// Return the id of the first (leftmost) leaf page for sequential scan.
    pub fn first_leaf_id(&mut self) -> IsamResult<u32> {
        let root_id = self.pager.meta.root_page_id;
        self.leftmost_leaf(root_id)
    }

    /// Return the smallest key in the tree, or `None` if the tree is empty.
    pub fn min_key(&mut self) -> IsamResult<Option<K>> {
        let root_id = self.pager.meta.root_page_id;
        let leaf_id = self.leftmost_leaf(root_id)?;
        let leaf = decode_leaf(&self.pager.read_page(leaf_id)?)?;
        match leaf.entries.first() {
            None => Ok(None),
            Some(e) => Ok(Some(bincode::deserialize(&e.key_bytes)?)),
        }
    }

    /// Return the largest key in the tree, or `None` if the tree is empty.
    pub fn max_key(&mut self) -> IsamResult<Option<K>> {
        let root_id = self.pager.meta.root_page_id;
        let leaf_id = self.rightmost_leaf(root_id)?;
        let leaf = decode_leaf(&self.pager.read_page(leaf_id)?)?;
        match leaf.entries.last() {
            None => Ok(None),
            Some(e) => Ok(Some(bincode::deserialize(&e.key_bytes)?)),
        }
    }

    /// Read leaf page `id` and return its entries and `next_leaf_id`.
    pub fn read_leaf(&mut self, id: u32) -> IsamResult<(Vec<(K, RecordRef)>, u32)> {
        let data = self.pager.read_page(id)?;
        let leaf = decode_leaf(&data)?;
        let mut out = Vec::with_capacity(leaf.entries.len());
        for e in leaf.entries {
            let key: K = bincode::deserialize(&e.key_bytes)?;
            out.push((
                key,
                RecordRef {
                    offset: e.data_offset,
                    len: e.data_len,
                },
            ));
        }
        Ok((out, leaf.next_leaf_id))
    }

    pub fn key_schema_version(&self) -> u32 {
        self.pager.meta.key_schema_version
    }

    pub fn val_schema_version(&self) -> u32 {
        self.pager.meta.val_schema_version
    }

    pub fn set_schema_versions(&mut self, key_v: u32, val_v: u32) -> IsamResult<()> {
        self.pager.meta.key_schema_version = key_v;
        self.pager.meta.val_schema_version = val_v;
        self.pager.flush_meta()
    }

    pub fn flush(&mut self) -> IsamResult<()> {
        self.pager.flush()
    }

    // ------------------------------------------------------------------ //
    //  Private helpers — search
    // ------------------------------------------------------------------ //

    fn search_page(&mut self, page_id: u32, key_bytes: &[u8]) -> IsamResult<Option<RecordRef>> {
        let data = self.pager.read_page(page_id)?;
        match data[0] {
            PAGE_TYPE_LEAF => {
                let leaf = decode_leaf(&data)?;
                for e in &leaf.entries {
                    if e.key_bytes == key_bytes {
                        return Ok(Some(RecordRef {
                            offset: e.data_offset,
                            len: e.data_len,
                        }));
                    }
                }
                Ok(None)
            }
            PAGE_TYPE_INTERNAL => {
                let internal = decode_internal(&data)?;
                let child_id = self.find_child(&internal, key_bytes);
                self.search_page(child_id, key_bytes)
            }
            t => Err(IsamError::CorruptIndex(format!("unknown page type {t}"))),
        }
    }

    /// Compare two serialized keys using K's `Ord` (not raw byte order).
    ///
    /// Bincode uses little-endian integers, so raw byte comparison diverges
    /// from numeric order for values ≥ 256 in multi-byte types.  By
    /// deserializing and using `K::cmp` we guarantee the B-tree is ordered
    /// the same way the caller's `Ord` impl defines.
    fn cmp_key_bytes(&self, a: &[u8], b: &[u8]) -> std::cmp::Ordering {
        let ka: K = bincode::deserialize(a).expect("corrupt key bytes in index");
        let kb: K = bincode::deserialize(b).expect("corrupt key bytes in index");
        ka.cmp(&kb)
    }

    /// Binary-search the internal page's keys to pick the right child.
    fn find_child(&self, page: &InternalPage, key_bytes: &[u8]) -> u32 {
        // children[i] holds all keys < keys[i].
        // We want the largest i such that keys[i-1] <= key_bytes,
        // or child[0] if key_bytes < keys[0].
        // Use K::Ord (via cmp_key_bytes) so the routing matches the insertion
        // order — raw byte order diverges from K::Ord for multi-byte integers.
        let pos = page
            .key_bytes
            .partition_point(|kb| !matches!(self.cmp_key_bytes(kb, key_bytes), std::cmp::Ordering::Greater));
        page.children[pos]
    }

    /// Walk from `page_id` down to a leaf, collecting page ids along the way.
    fn find_leaf_path(&mut self, page_id: u32, key_bytes: &[u8]) -> IsamResult<Vec<u32>> {
        let mut path = vec![page_id];
        let mut current = page_id;
        loop {
            let data = self.pager.read_page(current)?;
            match data[0] {
                PAGE_TYPE_LEAF => break,
                PAGE_TYPE_INTERNAL => {
                    let internal = decode_internal(&data)?;
                    let child = self.find_child(&internal, key_bytes);
                    path.push(child);
                    current = child;
                }
                t => return Err(IsamError::CorruptIndex(format!("unknown page type {t}"))),
            }
        }
        Ok(path)
    }

    fn leftmost_leaf(&mut self, page_id: u32) -> IsamResult<u32> {
        let data = self.pager.read_page(page_id)?;
        match data[0] {
            PAGE_TYPE_LEAF => Ok(page_id),
            PAGE_TYPE_INTERNAL => {
                let internal = decode_internal(&data)?;
                self.leftmost_leaf(internal.children[0])
            }
            t => Err(IsamError::CorruptIndex(format!("unknown page type {t}"))),
        }
    }

    fn rightmost_leaf(&mut self, page_id: u32) -> IsamResult<u32> {
        let data = self.pager.read_page(page_id)?;
        match data[0] {
            PAGE_TYPE_LEAF => Ok(page_id),
            PAGE_TYPE_INTERNAL => {
                let internal = decode_internal(&data)?;
                self.rightmost_leaf(*internal.children.last().unwrap())
            }
            t => Err(IsamError::CorruptIndex(format!("unknown page type {t}"))),
        }
    }

    // ------------------------------------------------------------------ //
    //  Private helpers — capacity
    // ------------------------------------------------------------------ //

    fn leaf_byte_usage(leaf: &LeafPage) -> usize {
        LEAF_HEADER
            + leaf
                .entries
                .iter()
                .map(|e| leaf_entry_size(e.key_bytes.len()))
                .sum::<usize>()
    }

    fn leaf_fits(&self, leaf: &LeafPage) -> bool {
        Self::leaf_byte_usage(leaf) <= PAGE_SIZE
    }

    fn internal_byte_usage(page: &InternalPage) -> usize {
        INTERNAL_HEADER
            + page
                .key_bytes
                .iter()
                .map(|kb| internal_key_size(kb.len()))
                .sum::<usize>()
    }

    fn internal_fits(page: &InternalPage) -> bool {
        Self::internal_byte_usage(page) <= PAGE_SIZE
    }

    // ------------------------------------------------------------------ //
    //  Private helpers — insert with splits
    // ------------------------------------------------------------------ //

    /// Full insert-and-split pass. Called when the simple insert overflows.
    fn insert_with_splits(
        &mut self,
        key_bytes: &[u8],
        rec: RecordRef,
        path: Vec<u32>,
    ) -> IsamResult<()> {
        // Re-insert into the leaf (undo the speculative insert above by
        // re-reading fresh state from disk and doing it properly).
        let leaf_id = *path.last().unwrap();
        let mut leaf = decode_leaf(&self.pager.read_page(leaf_id)?)?;

        // The entry may already be there from the speculative path — remove it
        // if so, then re-insert cleanly.
        leaf.entries.retain(|e| e.key_bytes != key_bytes);
        let pos = leaf
            .entries
            .partition_point(|e| self.cmp_key_bytes(&e.key_bytes, key_bytes) == std::cmp::Ordering::Less);
        leaf.entries.insert(
            pos,
            LeafEntry {
                key_bytes: key_bytes.to_vec(),
                data_offset: rec.offset,
                data_len: rec.len,
            },
        );

        // Split the leaf in half.
        let mid = leaf.entries.len() / 2;
        let right_entries = leaf.entries.split_off(mid);
        let promote_key = right_entries[0].key_bytes.clone();

        // Allocate a new right-leaf page.
        let right_id = self.pager.alloc_page()?;

        // Fix the linked list: new_right.next = old_leaf.next
        let right_leaf = LeafPage {
            next_leaf_id: leaf.next_leaf_id,
            entries: right_entries,
        };
        leaf.next_leaf_id = right_id;

        self.pager.write_page(leaf_id, &encode_leaf(&leaf))?;
        self.pager.write_page(right_id, &encode_leaf(&right_leaf))?;

        // Propagate the promoted key upward.
        self.insert_into_parent(&path, path.len() - 1, promote_key, right_id)?;

        Ok(())
    }

    /// Recursively insert a promoted key+right_child into the parent.
    fn insert_into_parent(
        &mut self,
        path: &[u32],
        child_path_idx: usize, // index of the child that was split
        promote_key: Vec<u8>,
        right_child_id: u32,
    ) -> IsamResult<()> {
        if child_path_idx == 0 {
            // The root was split — create a new root.
            let left_child_id = path[0];
            let new_root = InternalPage {
                children: vec![left_child_id, right_child_id],
                key_bytes: vec![promote_key],
            };
            let new_root_id = self.pager.alloc_page()?;
            self.pager
                .write_page(new_root_id, &encode_internal(&new_root))?;
            self.pager.meta.root_page_id = new_root_id;
            self.pager.flush_meta()?;
            return Ok(());
        }

        let parent_id = path[child_path_idx - 1];
        let mut parent = decode_internal(&self.pager.read_page(parent_id)?)?;

        // Find where in parent's children the left child sits.
        let left_child_id = path[child_path_idx];
        let child_pos = parent
            .children
            .iter()
            .position(|&c| c == left_child_id)
            .ok_or_else(|| IsamError::CorruptIndex("child not found in parent".into()))?;

        parent.key_bytes.insert(child_pos, promote_key);
        parent.children.insert(child_pos + 1, right_child_id);

        if Self::internal_fits(&parent) {
            self.pager
                .write_page(parent_id, &encode_internal(&parent))?;
        } else {
            // Split the internal page.
            let mid = parent.key_bytes.len() / 2;
            let promote_up = parent.key_bytes[mid].clone();

            let right_keys = parent.key_bytes.split_off(mid + 1);
            parent.key_bytes.truncate(mid); // remove the promoted key itself

            let right_children = parent.children.split_off(mid + 1);

            let right_internal = InternalPage {
                children: right_children,
                key_bytes: right_keys,
            };
            let right_id = self.pager.alloc_page()?;
            self.pager
                .write_page(parent_id, &encode_internal(&parent))?;
            self.pager
                .write_page(right_id, &encode_internal(&right_internal))?;

            self.insert_into_parent(path, child_path_idx - 1, promote_up, right_id)?;
        }

        Ok(())
    }

    // ------------------------------------------------------------------ //
    //  Private helpers — delete rebalancing
    // ------------------------------------------------------------------ //

    fn rebalance_after_delete(
        &mut self,
        _path: &[u32],
        leaf_id: u32,
        parent_id: u32,
    ) -> IsamResult<()> {
        let leaf_data = self.pager.read_page(leaf_id)?;
        let leaf = decode_leaf(&leaf_data)?;

        // Minimum occupancy: a leaf should hold at least some entries.
        // We use a simple heuristic: if the leaf is less than 1/4 full and
        // there is a sibling we can steal from or merge with, do so.
        // For simplicity we only merge (no borrow), which is correct but
        // may cause tree height to drop faster than strictly necessary.
        let min_bytes = PAGE_SIZE / 4;
        if Self::leaf_byte_usage(&leaf) >= min_bytes {
            return Ok(()); // still sufficiently full
        }

        let parent_data = self.pager.read_page(parent_id)?;
        let mut parent = decode_internal(&parent_data)?;

        // Find the index of leaf_id in parent.children.
        let idx = match parent.children.iter().position(|&c| c == leaf_id) {
            Some(i) => i,
            None => return Ok(()), // shouldn't happen, bail safely
        };

        // Try to merge with the right sibling first, then left.
        if idx + 1 < parent.children.len() {
            let right_id = parent.children[idx + 1];
            let right_data = self.pager.read_page(right_id)?;
            let mut right = decode_leaf(&right_data)?;

            // Merge: absorb right into leaf.
            let mut merged = leaf.clone();
            merged.entries.append(&mut right.entries);
            merged.next_leaf_id = right.next_leaf_id;

            if Self::leaf_byte_usage(&merged) <= PAGE_SIZE {
                self.pager.write_page(leaf_id, &encode_leaf(&merged))?;
                // Remove the separator key and right child from parent.
                parent.key_bytes.remove(idx);
                parent.children.remove(idx + 1);
                self.pager
                    .write_page(parent_id, &encode_internal(&parent))?;

                // If parent is now the root and empty, shrink tree height.
                if parent_id == self.pager.meta.root_page_id && parent.children.len() == 1 {
                    self.pager.meta.root_page_id = parent.children[0];
                    self.pager.flush_meta()?;
                }
                return Ok(());
            }
        }

        if idx > 0 {
            let left_id = parent.children[idx - 1];
            let left_data = self.pager.read_page(left_id)?;
            let left = decode_leaf(&left_data)?;

            let mut merged = left.clone();
            merged.entries.append(&mut leaf.entries.clone());
            merged.next_leaf_id = leaf.next_leaf_id;

            if Self::leaf_byte_usage(&merged) <= PAGE_SIZE {
                self.pager.write_page(left_id, &encode_leaf(&merged))?;
                parent.key_bytes.remove(idx - 1);
                parent.children.remove(idx);
                self.pager
                    .write_page(parent_id, &encode_internal(&parent))?;

                if parent_id == self.pager.meta.root_page_id && parent.children.len() == 1 {
                    self.pager.meta.root_page_id = parent.children[0];
                    self.pager.flush_meta()?;
                }
            }
        }

        Ok(())
    }
}
