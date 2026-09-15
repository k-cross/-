use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::io::AsRawFd;
use std::path::Path;

pub const DIRECT_ALIGN: usize = 4096;

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
