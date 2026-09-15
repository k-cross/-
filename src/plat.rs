use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::io::AsRawFd;
use std::path::Path;

pub const DIRECT_ALIGN: usize = 4096;

/// # Errors
/// Returns the OS error if the spill file cannot be opened.
#[cfg(target_os = "linux")]
pub fn open_direct(path: &Path) -> io::Result<File> {
    use std::os::unix::fs::OpenOptionsExt;
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .custom_flags(libc::O_DIRECT)
        .open(path)
        .or_else(|_| {
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(path)
        })
}

/// # Errors
/// Returns the OS error if the spill file cannot be opened, or if `F_NOCACHE` is refused.
#[cfg(not(target_os = "linux"))]
pub fn open_direct(path: &Path) -> io::Result<File> {
    let f = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)?;
    // Without F_NOCACHE the unified buffer cache absorbs the I/O and we measure DRAM, not the device.
    #[cfg(target_os = "macos")]
    {
        // SAFETY: fd is owned by `f` and valid for the duration of this call.
        let rc = unsafe { libc::fcntl(f.as_raw_fd(), libc::F_NOCACHE, 1) };
        if rc == -1 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(f)
}

#[cfg(target_os = "macos")]
#[must_use]
pub fn sysctl_u64(name: &str) -> Option<u64> {
    let c = std::ffi::CString::new(name).ok()?;
    let mut out: u64 = 0;
    let mut len = std::mem::size_of::<u64>();
    // SAFETY: `out` is a live u64 and `len` describes it exactly; sysctlbyname writes at most
    // `len` bytes and updates it to what it wrote.
    let rc = unsafe {
        libc::sysctlbyname(
            c.as_ptr(),
            std::ptr::from_mut(&mut out).cast(),
            &raw mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    if rc != 0 {
        return None;
    }
    // Some keys are 32-bit (cpu counts) and some 64-bit (memsize); interpret by the width
    // the kernel reports back rather than assuming.
    match len {
        4 => Some(u64::from(u32::try_from(out & 0xFFFF_FFFF).ok()?)),
        8 => Some(out),
        _ => None,
    }
}

#[cfg(not(target_os = "macos"))]
#[must_use]
pub fn sysctl_u64(_name: &str) -> Option<u64> {
    None
}

/// Scheduling class, which on Apple Silicon is what actually steers a thread to the
/// performance or efficiency cluster -- affinity hints are advisory and largely ignored.
#[derive(Clone, Copy, Debug)]
pub enum Cluster {
    Performance,
    Efficiency,
}

#[cfg(target_os = "macos")]
unsafe extern "C" {
    fn pthread_set_qos_class_self_np(qos_class: u32, relative_priority: i32) -> i32;
}

#[cfg(target_os = "macos")]
#[must_use]
pub fn pin_cluster(c: Cluster) -> bool {
    const QOS_USER_INTERACTIVE: u32 = 0x21;
    const QOS_BACKGROUND: u32 = 0x09;
    let q = match c {
        Cluster::Performance => QOS_USER_INTERACTIVE,
        Cluster::Efficiency => QOS_BACKGROUND,
    };
    // SAFETY: sets the QoS class of the calling thread; no memory is shared with the call.
    unsafe { pthread_set_qos_class_self_np(q, 0) == 0 }
}

#[cfg(not(target_os = "macos"))]
#[must_use]
pub fn pin_cluster(_c: Cluster) -> bool {
    false
}

/// NUMA nodes visible to the host, with the kernel's own distance matrix where it exposes
/// one. Returns a single node when the platform has no NUMA concept.
#[must_use]
pub fn numa_nodes() -> Vec<(u8, Vec<u8>)> {
    #[cfg(target_os = "linux")]
    {
        let Ok(dir) = std::fs::read_dir("/sys/devices/system/node") else {
            return vec![(0, vec![10])];
        };
        let mut out: Vec<(u8, Vec<u8>)> = Vec::new();
        for e in dir.flatten() {
            let name = e.file_name();
            let Some(name) = name.to_str() else { continue };
            let Some(idx) = name.strip_prefix("node").and_then(|n| n.parse::<u8>().ok()) else {
                continue;
            };
            let dist = std::fs::read_to_string(e.path().join("distance"))
                .ok()
                .map(|s| {
                    s.split_whitespace()
                        .filter_map(|d| d.parse().ok())
                        .collect()
                })
                .unwrap_or_default();
            out.push((idx, dist));
        }
        out.sort_by_key(|(i, _)| *i);
        if out.is_empty() {
            vec![(0, vec![10])]
        } else {
            out
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        vec![(0, vec![10])]
    }
}
