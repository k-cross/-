use std::alloc::{Layout, alloc, dealloc, handle_alloc_error};
use std::collections::HashMap;
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::ptr::NonNull;
use std::slice;
use std::time::Instant;

use crate::blob::BlobId;
use crate::plat::{DIRECT_ALIGN, open_direct};

#[derive(Debug)]
pub struct Bytes {
    ptr: NonNull<u8>,
    layout: Layout,
}

// SAFETY: Bytes uniquely owns its allocation and hands out references only through &self/&mut self.
unsafe impl Send for Bytes {}

impl Bytes {
    #[must_use]
    pub fn new(len: usize) -> Self {
        let len = len.next_multiple_of(DIRECT_ALIGN).max(DIRECT_ALIGN);
        let layout = Layout::from_size_align(len, DIRECT_ALIGN).expect("valid layout");
        // SAFETY: layout size is non-zero (>= DIRECT_ALIGN).
        let raw = unsafe { alloc(layout) };
        let Some(ptr) = NonNull::new(raw) else {
            handle_alloc_error(layout)
        };
        Self { ptr, layout }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.layout.size()
    }

    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: ptr is valid for layout.size() bytes and lives as long as self.
        unsafe { slice::from_raw_parts(self.ptr.as_ptr(), self.layout.size()) }
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: ptr is valid for layout.size() bytes and self is uniquely borrowed.
        unsafe { slice::from_raw_parts_mut(self.ptr.as_ptr(), self.layout.size()) }
    }

    pub fn fault_in(&mut self, seed: u8) {
        let s = self.as_mut_slice();
        let mut i = 0;
        while i < s.len() {
            s[i] = seed;
            i += DIRECT_ALIGN;
        }
    }
}

impl Drop for Bytes {
    fn drop(&mut self) {
        // SAFETY: ptr came from alloc with this exact layout and is dropped once.
        unsafe { dealloc(self.ptr.as_ptr(), self.layout) }
    }
}

#[derive(Debug)]
struct Spill {
    file: File,
    slots: HashMap<BlobId, (u64, usize)>,
    free: HashMap<usize, Vec<u64>>,
    end: u64,
    cap: u64,
}

impl Spill {
    fn alloc(&mut self, len: usize) -> Option<u64> {
        if let Some(off) = self.free.get_mut(&len).and_then(Vec::pop) {
            return Some(off);
        }
        if self.end + len as u64 > self.cap {
            return None;
        }
        let off = self.end;
        self.end += len as u64;
        Some(off)
    }
}

#[derive(Clone, Copy, Default, Debug)]
pub struct StoreStats {
    pub dram_bytes: u64,
    pub spill_bytes: u64,
    pub writes: u64,
    pub reads: u64,
    pub write_ns: u64,
    pub read_ns: u64,
    pub fault_ns: u64,
}

#[derive(Debug)]
pub struct Store {
    dram: HashMap<BlobId, Bytes>,
    spill: Spill,
    pub stats: StoreStats,
}

impl Store {
    pub fn open(path: &Path, cap: u64) -> std::io::Result<Self> {
        let file = open_direct(path)?;
        Ok(Self {
            dram: HashMap::new(),
            spill: Spill {
                file,
                slots: HashMap::new(),
                free: HashMap::new(),
                end: 0,
                cap,
            },
            stats: StoreStats::default(),
        })
    }

    pub fn materialize(&mut self, id: BlobId, len: usize) -> u64 {
        if self.dram.contains_key(&id) {
            return 0;
        }
        let t = Instant::now();
        let mut b = Bytes::new(len);
        b.fault_in(1);
        let ns = t.elapsed().as_nanos() as u64;
        self.stats.fault_ns += ns;
        self.stats.dram_bytes += b.len() as u64;
        self.dram.insert(id, b);
        ns
    }

    pub fn demote(&mut self, id: BlobId) -> u64 {
        let Some(b) = self.dram.remove(&id) else {
            return 0;
        };
        self.stats.dram_bytes -= b.len() as u64;
        let len = b.len();
        let Some(off) = self.spill.alloc(len) else {
            return 0;
        };
        let t = Instant::now();
        let ok = self.spill.file.write_all_at(b.as_slice(), off).is_ok();
        let ns = t.elapsed().as_nanos() as u64;
        if ok {
            self.spill.slots.insert(id, (off, len));
            self.stats.spill_bytes += len as u64;
            self.stats.writes += 1;
            self.stats.write_ns += ns;
        }
        ns
    }

    pub fn promote(&mut self, id: BlobId) -> u64 {
        let Some((off, len)) = self.spill.slots.remove(&id) else {
            return 0;
        };
        let mut b = Bytes::new(len);
        let t = Instant::now();
        let ok = self.spill.file.read_exact_at(b.as_mut_slice(), off).is_ok();
        let ns = t.elapsed().as_nanos() as u64;
        self.spill.free.entry(len).or_default().push(off);
        self.stats.spill_bytes -= len as u64;
        self.stats.reads += 1;
        self.stats.read_ns += ns;
        if ok {
            self.stats.dram_bytes += b.len() as u64;
            self.dram.insert(id, b);
        }
        ns
    }

    pub fn drop_cold(&mut self, id: BlobId) {
        if let Some((off, len)) = self.spill.slots.remove(&id) {
            self.spill.free.entry(len).or_default().push(off);
            self.stats.spill_bytes -= len as u64;
        }
    }
}
