use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::time::Instant;

use crate::boundary::timer_overhead;

pub const PAYLOAD_BYTES: usize = 128;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Commit {
    Append,
    Flush,
    FullFlush,
}

impl Commit {
    pub const ALL: [Self; 3] = [Self::Append, Self::Flush, Self::FullFlush];

    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Append => "append",
            Self::Flush => "append and fsync",
            Self::FullFlush => "append and full flush",
        }
    }

    #[must_use]
    pub fn iterations(self) -> usize {
        match self {
            Self::Append => 2_000,
            Self::Flush => 1_000,
            Self::FullFlush => 200,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Timing {
    pub median_ns: u64,
    pub p99_ns: u64,
}

#[derive(Clone, Debug)]
pub struct Appends {
    pub timings: [Timing; 3],
    pub timer_ns: f64,
    pub dir: PathBuf,
}

impl Appends {
    #[must_use]
    pub fn of(&self, commit: Commit) -> Timing {
        self.timings[commit as usize]
    }
}

fn percentile(sorted: &[u64], q: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    sorted[((sorted.len() as f64 * q) as usize).min(sorted.len() - 1)]
}

fn sync(file: &File, commit: Commit) -> io::Result<()> {
    match commit {
        Commit::Append => Ok(()),
        Commit::Flush => {
            // SAFETY: the descriptor is open for the whole call and fsync reads nothing from memory.
            let rc = unsafe { libc::fsync(file.as_raw_fd()) };
            if rc == 0 {
                Ok(())
            } else {
                Err(io::Error::last_os_error())
            }
        }
        Commit::FullFlush => file.sync_all(),
    }
}

pub fn append_latencies(path: &Path, commit: Commit, iterations: usize) -> io::Result<Vec<u64>> {
    let payload = [0x5au8; PAYLOAD_BYTES];
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    let mut out = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let started = Instant::now();
        file.write_all(&payload)?;
        sync(&file, commit)?;
        out.push(started.elapsed().as_nanos() as u64);
    }
    Ok(out)
}

fn timing_of(mut latencies: Vec<u64>, timer_ns: f64) -> Timing {
    let net = |ns: u64| (ns as f64 - timer_ns).max(0.0) as u64;
    latencies.sort_unstable();
    Timing {
        median_ns: net(percentile(&latencies, 0.50)),
        p99_ns: net(percentile(&latencies, 0.99)),
    }
}

fn sweep(path: &Path, reps: usize, timer_ns: f64, best: &mut [Timing; 3]) -> io::Result<()> {
    for _ in 0..reps.max(1) {
        for commit in Commit::ALL {
            let latencies = append_latencies(path, commit, commit.iterations())?;
            fs::remove_file(path)?;
            let t = timing_of(latencies, timer_ns);
            let slot = &mut best[commit as usize];
            slot.median_ns = slot.median_ns.min(t.median_ns);
            slot.p99_ns = slot.p99_ns.min(t.p99_ns);
        }
    }
    Ok(())
}

pub fn measure(dir: &Path, reps: usize) -> io::Result<Appends> {
    let timer_ns = timer_overhead();
    let path = dir.join(format!("polyphonic-durable-{}.log", std::process::id()));
    let mut best = [Timing {
        median_ns: u64::MAX,
        p99_ns: u64::MAX,
    }; 3];
    let outcome = sweep(&path, reps, timer_ns, &mut best);
    let _ = fs::remove_file(&path);
    outcome?;
    Ok(Appends {
        timings: best,
        timer_ns,
        dir: dir.to_path_buf(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("polyphonic-test-{}-{name}", std::process::id()));
        fs::create_dir_all(&dir).expect("a scratch directory");
        dir
    }

    #[test]
    fn every_commit_appends_its_payload_once_per_iteration() {
        let dir = scratch("append");
        for commit in Commit::ALL {
            let path = dir.join(format!("{commit:?}.log"));
            let latencies = append_latencies(&path, commit, 7).expect("appends");
            assert_eq!(latencies.len(), 7);
            assert_eq!(
                fs::metadata(&path).expect("the file").len(),
                (7 * PAYLOAD_BYTES) as u64
            );
        }
        fs::remove_dir_all(&dir).expect("cleanup");
    }

    #[test]
    fn measuring_leaves_no_file_behind_and_reports_ordered_percentiles() {
        let dir = scratch("measure");
        let appends = measure(&dir, 1).expect("a measurement");
        assert_eq!(fs::read_dir(&dir).expect("the directory").count(), 0);
        for commit in Commit::ALL {
            let t = appends.of(commit);
            assert!(
                t.median_ns <= t.p99_ns || t.p99_ns == 0,
                "{commit:?}: {t:?}"
            );
        }
        fs::remove_dir_all(&dir).expect("cleanup");
    }

    #[test]
    fn a_missing_directory_is_an_error_not_a_panic() {
        let missing = std::env::temp_dir().join("polyphonic-test-does-not-exist/inner");
        assert!(measure(&missing, 1).is_err());
    }

    #[test]
    fn percentiles_read_the_sorted_latencies() {
        let sorted: Vec<u64> = (1..=100).collect();
        assert_eq!(percentile(&sorted, 0.50), 51);
        assert_eq!(percentile(&sorted, 0.99), 100);
        assert_eq!(percentile(&[], 0.5), 0);
    }
}
