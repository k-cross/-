//! What it costs to cross a boundary, measured rather than assumed.
//!
//! Every orchestrator decision is an *action across a boundary*: a scheduler asks an
//! extender, a runtime asks a CSI plugin, a proxy asks a policy engine. Kubernetes pays a
//! gRPC round trip for each. A ledger that answers in-process pays a function call. The
//! interesting quantity is not either number but the ladder between them -- which rungs are
//! irreducible (a ring transition is physics) and which are self-inflicted (HTTP/2 framing
//! around a 40-byte question).
//!
//! Nothing here is modelled. `Ladder::measure` runs on the host and the constants it
//! produces are what that host actually does.

use std::cell::UnsafeCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Instant;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Boundary {
    /// No boundary: a direct call. The zero-cost-extension target.
    Native,
    /// Shared memory with a spin-polled sequence protocol. No kernel involvement, at the
    /// cost of a burned core. What an ABI or WASM extension can achieve at best.
    Ring,
    /// Irreducible user->kernel->user transition, no payload.
    Syscall,
    /// Syscall plus a kernel-buffer copy, same thread. No scheduler involvement.
    Pipe,
    /// Syscall, copy, and a wakeup of a thread that was blocked.
    UnixSocket,
    /// Adds the loopback network stack.
    TcpLoopback,
    /// Adds HTTP/2 framing and protobuf encode/decode.
    Grpc,
}

impl Boundary {
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Native => "native call",
            Self::Ring => "shared ring (spin)",
            Self::Syscall => "syscall floor",
            Self::Pipe => "pipe (same thread)",
            Self::UnixSocket => "unix socket RTT",
            Self::TcpLoopback => "TCP loopback RTT",
            Self::Grpc => "gRPC unary RTT",
        }
    }
}

/// An affine cost model: a fixed charge to cross plus a per-byte charge to marshal.
#[derive(Clone, Copy, Debug, Default)]
pub struct Cost {
    pub fixed_ns: f64,
    pub ns_per_byte: f64,
}

impl Cost {
    #[must_use]
    pub fn ns(&self, bytes: u64) -> u64 {
        (self.fixed_ns + self.ns_per_byte * bytes as f64).max(0.0) as u64
    }

    /// Least-squares fit over (bytes, ns) samples. Two or more distinct sizes are needed to
    /// separate the fixed cost from the per-byte cost; with fewer, the per-byte term is
    /// reported as zero rather than guessed.
    #[must_use]
    pub fn fit(points: &[(usize, u64)]) -> Self {
        let n = points.len() as f64;
        if n == 0.0 {
            return Self::default();
        }
        let mx = points.iter().map(|p| p.0 as f64).sum::<f64>() / n;
        let my = points.iter().map(|p| p.1 as f64).sum::<f64>() / n;
        let var = points
            .iter()
            .map(|p| (p.0 as f64 - mx).powi(2))
            .sum::<f64>();
        if var <= 0.0 {
            return Self {
                fixed_ns: my,
                ns_per_byte: 0.0,
            };
        }
        let cov = points
            .iter()
            .map(|p| (p.0 as f64 - mx) * (p.1 as f64 - my))
            .sum::<f64>();
        let slope = cov / var;
        Self {
            fixed_ns: my - slope * mx,
            ns_per_byte: slope,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Rung {
    pub boundary: Boundary,
    /// p50 nanoseconds per operation at each payload size.
    pub by_size: Vec<(usize, u64)>,
    pub cost: Cost,
    /// Tail at the largest payload measured, where the tail is worst.
    pub p99_ns: u64,
    /// Worst repetition divided by best, at the middle payload size. On a host that migrates
    /// threads between core clusters this runs near 2x, and the ladder's *ordering* is the
    /// robust result rather than any single constant.
    pub spread: f64,
}

#[derive(Clone, Debug, Default)]
pub struct Ladder {
    pub rungs: Vec<Rung>,
    /// Cost of `Instant::now()` itself. Any rung within a few multiples of this is measured
    /// in batch, not per-operation, and has no meaningful tail.
    pub timer_ns: f64,
}

impl Ladder {
    #[must_use]
    pub fn get(&self, b: Boundary) -> Option<Cost> {
        self.rungs.iter().find(|r| r.boundary == b).map(|r| r.cost)
    }

    #[must_use]
    pub fn ns(&self, b: Boundary, bytes: u64) -> u64 {
        self.get(b).map_or(0, |c| c.ns(bytes))
    }
}

fn percentile(v: &mut [u64], q: f64) -> u64 {
    if v.is_empty() {
        return 0;
    }
    v.sort_unstable();
    v[(((v.len() as f64) * q) as usize).min(v.len() - 1)]
}

/// Cost of the measurement instrument, so cheap rungs can be judged against it.
#[must_use]
pub fn timer_overhead() -> f64 {
    const N: usize = 200_000;
    let t = Instant::now();
    for _ in 0..N {
        std::hint::black_box(Instant::now());
    }
    t.elapsed().as_nanos() as f64 / N as f64
}

/// Remove the clock's own cost from a per-operation measurement.
fn net(ns: u64, timer_ns: f64) -> u64 {
    (ns as f64 - timer_ns).max(0.0) as u64
}

/// Time `f` in batches and return per-operation nanoseconds. For anything near the timer's
/// own cost, per-operation timing measures the clock, not the work.
fn batched(iters: usize, batch: usize, mut f: impl FnMut()) -> u64 {
    for _ in 0..batch {
        f();
    }
    let rounds = iters / batch;
    let mut per = Vec::with_capacity(rounds);
    for _ in 0..rounds {
        let t = Instant::now();
        for _ in 0..batch {
            f();
        }
        per.push(t.elapsed().as_nanos() as u64 / batch as u64);
    }
    percentile(&mut per, 0.50)
}

/// Control-plane message sizes. A scheduling decision is tens to thousands of bytes; the
/// span is wide enough to separate the fixed crossing cost from the marshalling slope.
pub const SIZES: [usize; 3] = [64, 1024, 8192];
const ITERS: usize = 20_000;

/// Run the ladder `reps` times and keep the best observation of each rung. A microbenchmark
/// can only be disturbed upward, so the minimum is the estimate and the spread is the noise.
#[must_use]
pub fn measure(reps: usize) -> Ladder {
    let mut best: Option<Ladder> = None;
    let mut worst: Vec<Vec<(usize, u64)>> = Vec::new();
    for _ in 0..reps.max(1) {
        let l = measure_once();
        let Some(b) = &mut best else {
            worst = l.rungs.iter().map(|r| r.by_size.clone()).collect();
            best = Some(l);
            continue;
        };
        for (i, r) in l.rungs.iter().enumerate() {
            let Some(prev) = b.rungs.get_mut(i) else {
                continue;
            };
            for (j, &(n, ns)) in r.by_size.iter().enumerate() {
                if let Some(slot) = prev.by_size.get_mut(j) {
                    slot.1 = slot.1.min(ns);
                }
                if let Some(w) = worst.get_mut(i).and_then(|v| v.get_mut(j)) {
                    w.1 = w.1.max(ns);
                    debug_assert_eq!(w.0, n);
                }
            }
            prev.p99_ns = prev.p99_ns.min(r.p99_ns);
        }
    }
    let mut l = best.unwrap_or_default();
    let mid = SIZES.len() / 2;
    for (i, r) in l.rungs.iter_mut().enumerate() {
        r.cost = Cost::fit(&r.by_size);
        let lo = r.by_size.get(mid).map_or(0, |s| s.1);
        let hi = worst.get(i).and_then(|v| v.get(mid)).map_or(0, |s| s.1);
        r.spread = if lo == 0 { 1.0 } else { hi as f64 / lo as f64 };
    }
    l
}

fn measure_once() -> Ladder {
    let _ = crate::plat::pin_cluster(crate::plat::Cluster::Performance);
    let timer_ns = timer_overhead();
    // Round trips are timed per operation; the cheap rungs are timed in batches. Charging
    // the clock to one group and not the other would put a 35 ns thumb on the scale.
    let mut rungs = vec![
        native(),
        ring(timer_ns),
        syscall(),
        pipe(),
        unix_socket(timer_ns),
        tcp_loopback(timer_ns),
    ];
    for r in &mut rungs {
        r.cost = Cost::fit(&r.by_size);
    }
    #[cfg(feature = "grpc")]
    rungs.push(grpc::measure(timer_ns));
    Ladder { rungs, timer_ns }
}

fn rung(boundary: Boundary, by_size: Vec<(usize, u64)>, p99_ns: u64) -> Rung {
    Rung {
        boundary,
        by_size,
        cost: Cost::default(),
        p99_ns,
        spread: 1.0,
    }
}

/// An extension invoked through a vtable, payload passed by pointer. Nothing is copied, so
/// the per-byte term should fit to ~zero -- that is the whole claim of an in-process ABI.
fn native() -> Rung {
    let f: &dyn Fn(&[u8]) -> u64 = &|b: &[u8]| u64::from(b[0]) + u64::from(b[b.len() - 1]);
    let by_size = SIZES
        .iter()
        .map(|&n| {
            let buf = vec![7u8; n];
            (
                n,
                batched(ITERS, 1000, || {
                    std::hint::black_box(f(std::hint::black_box(&buf)));
                }),
            )
        })
        .collect();
    rung(Boundary::Native, by_size, 0)
}

fn syscall() -> Rung {
    let by_size = SIZES
        .iter()
        .map(|&n| {
            (
                n,
                batched(ITERS, 1000, || {
                    // close(-1) fails with EBADF without doing any work, so what is left is
                    // the ring transition itself.
                    // SAFETY: -1 is never a valid descriptor; the call has no effect.
                    std::hint::black_box(unsafe { libc::close(-1) });
                }),
            )
        })
        .collect();
    rung(Boundary::Syscall, by_size, 0)
}

fn pipe() -> Rung {
    let mut fds = [0i32; 2];
    // SAFETY: `fds` is a valid two-element array, which is what pipe(2) writes into.
    if unsafe { libc::pipe(fds.as_mut_ptr()) } != 0 {
        return rung(Boundary::Pipe, Vec::new(), 0);
    }
    let (r, w) = (fds[0], fds[1]);
    let by_size = SIZES
        .iter()
        .map(|&n| {
            let src = vec![7u8; n];
            let mut dst = vec![0u8; n];
            let ns = batched(ITERS, 200, || {
                // SAFETY: both buffers are `n` bytes and the descriptors stay open for the
                // lifetime of this closure.
                unsafe {
                    libc::write(w, src.as_ptr().cast(), n);
                    libc::read(r, dst.as_mut_ptr().cast(), n);
                }
            });
            (n, ns)
        })
        .collect();
    // SAFETY: both descriptors were opened above and are not used again.
    unsafe {
        libc::close(r);
        libc::close(w);
    }
    rung(Boundary::Pipe, by_size, 0)
}

fn unix_socket(timer_ns: f64) -> Rung {
    use std::io::{Read, Write};
    use std::os::unix::net::UnixStream;
    let mut by_size = Vec::new();
    let mut p99 = 0;
    for &n in &SIZES {
        let Ok((mut a, mut b)) = UnixStream::pair() else {
            continue;
        };
        let server = std::thread::spawn(move || {
            let _ = crate::plat::pin_cluster(crate::plat::Cluster::Performance);
            let mut buf = vec![0u8; n];
            while b.read_exact(&mut buf).is_ok() {
                if b.write_all(&[1u8; 8]).is_err() {
                    return;
                }
            }
        });
        let (p50, tail) = echo_rtt(n, &mut a, timer_ns);
        drop(a);
        server.join().ok();
        by_size.push((n, p50));
        p99 = tail;
    }
    rung(Boundary::UnixSocket, by_size, p99)
}

fn tcp_loopback(timer_ns: f64) -> Rung {
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    let mut by_size = Vec::new();
    let mut p99 = 0;
    for &n in &SIZES {
        let Ok(listener) = TcpListener::bind("127.0.0.1:0") else {
            continue;
        };
        let Ok(addr) = listener.local_addr() else {
            continue;
        };
        let server = std::thread::spawn(move || {
            let _ = crate::plat::pin_cluster(crate::plat::Cluster::Performance);
            let Ok((mut s, _)) = listener.accept() else {
                return;
            };
            s.set_nodelay(true).ok();
            let mut buf = vec![0u8; n];
            while s.read_exact(&mut buf).is_ok() {
                if s.write_all(&[1u8; 8]).is_err() {
                    return;
                }
            }
        });
        let Ok(mut c) = TcpStream::connect(addr) else {
            continue;
        };
        c.set_nodelay(true).ok();
        let (p50, tail) = echo_rtt(n, &mut c, timer_ns);
        drop(c);
        server.join().ok();
        by_size.push((n, p50));
        p99 = tail;
    }
    rung(Boundary::TcpLoopback, by_size, p99)
}

/// Round trips are far above the timer's resolution, so these are timed per operation and
/// carry a meaningful tail.
fn echo_rtt<S: std::io::Read + std::io::Write>(
    n: usize,
    sock: &mut S,
    timer_ns: f64,
) -> (u64, u64) {
    let msg = vec![7u8; n];
    let mut reply = [0u8; 8];
    let iters = ITERS / 4;
    for _ in 0..200 {
        if sock.write_all(&msg).is_err() || sock.read_exact(&mut reply).is_err() {
            return (0, 0);
        }
    }
    let mut out = Vec::with_capacity(iters);
    for _ in 0..iters {
        let t = Instant::now();
        if sock.write_all(&msg).is_err() || sock.read_exact(&mut reply).is_err() {
            break;
        }
        out.push(t.elapsed().as_nanos() as u64);
    }
    (
        net(percentile(&mut out, 0.50), timer_ns),
        net(percentile(&mut out, 0.99), timer_ns),
    )
}

#[repr(align(128))]
struct Pad<T>(T);

struct Shared {
    req: Pad<AtomicU32>,
    rep: Pad<AtomicU32>,
    buf: UnsafeCell<Box<[u8]>>,
}

// SAFETY: `buf` is handed between the two sides by the sequence protocol below. The writer
// finishes its stores before a Release on `req`; the reader observes them through the
// matching Acquire and touches nothing after publishing `rep`. Only one side owns the buffer
// at a time, so there is no concurrent access to synchronise.
unsafe impl Sync for Shared {}

const RING_STOP: u32 = u32::MAX;

/// Shared memory with a spin-polled sequence protocol: no syscall, no scheduler, one core
/// burned on each side. This is the floor a zero-cost extension is aiming at, and the cost
/// of reaching it is a core that cannot do anything else.
fn ring(timer_ns: f64) -> Rung {
    let mut by_size = Vec::new();
    let mut p99 = 0;
    for &n in &SIZES {
        let s = Arc::new(Shared {
            req: Pad(AtomicU32::new(0)),
            rep: Pad(AtomicU32::new(0)),
            buf: UnsafeCell::new(vec![0u8; n].into_boxed_slice()),
        });
        let server = std::thread::spawn({
            let s = Arc::clone(&s);
            move || {
                let _ = crate::plat::pin_cluster(crate::plat::Cluster::Performance);
                let mut seen = 0u32;
                loop {
                    let seq = s.req.0.load(Ordering::Acquire);
                    if seq == RING_STOP {
                        return;
                    }
                    if seq == seen {
                        std::hint::spin_loop();
                        continue;
                    }
                    seen = seq;
                    // SAFETY: the Acquire above pairs with the client's Release, so the
                    // client's writes are visible and it will not touch the buffer again
                    // until it observes our reply.
                    let b = unsafe { &*s.buf.get() };
                    std::hint::black_box(u64::from(b[0]) + u64::from(b[n - 1]));
                    s.rep.0.store(seq, Ordering::Release);
                }
            }
        });

        let src = vec![7u8; n];
        let mut seq = 0u32;
        let mut send = || {
            seq += 1;
            // SAFETY: the client owns the buffer until it publishes `req`; the server is
            // still spinning on the previous sequence number and reads nothing.
            unsafe { (*s.buf.get()).copy_from_slice(&src) };
            s.req.0.store(seq, Ordering::Release);
            while s.rep.0.load(Ordering::Acquire) != seq {
                std::hint::spin_loop();
            }
        };
        for _ in 0..1000 {
            send();
        }
        let iters = ITERS / 4;
        let mut out = Vec::with_capacity(iters);
        for _ in 0..iters {
            let t = Instant::now();
            send();
            out.push(t.elapsed().as_nanos() as u64);
        }
        s.req.0.store(RING_STOP, Ordering::Release);
        server.join().ok();
        by_size.push((n, net(percentile(&mut out, 0.50), timer_ns)));
        p99 = net(percentile(&mut out, 0.99), timer_ns);
    }
    rung(Boundary::Ring, by_size, p99)
}

#[cfg(feature = "grpc")]
mod grpc {
    use super::{Boundary, Cost, ITERS, Rung, SIZES, net, percentile, rung};
    use std::time::Instant;

    #[allow(
        clippy::pedantic,
        clippy::result_large_err,
        reason = "tonic-generated code"
    )]
    mod pb {
        tonic::include_proto!("admission");
    }
    use pb::echo_client::EchoClient;
    use pb::echo_server::{Echo, EchoServer};
    use pb::{EchoReply, EchoRequest};

    #[derive(Debug)]
    struct Svc;

    #[tonic::async_trait]
    impl Echo for Svc {
        async fn call(
            &self,
            req: tonic::Request<EchoRequest>,
        ) -> Result<tonic::Response<EchoReply>, tonic::Status> {
            Ok(tonic::Response::new(EchoReply {
                n: req.into_inner().payload.len() as u64,
            }))
        }
    }

    /// The same bytes as every other rung, so the extra cost visible here is HTTP/2 framing
    /// and protobuf encode/decode, not a different question being asked.
    pub fn measure(timer_ns: f64) -> Rung {
        let Ok(rt) = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
        else {
            return rung(Boundary::Grpc, Vec::new(), 0);
        };
        rt.block_on(async move {
            let Ok(probe) = tokio::net::TcpListener::bind("127.0.0.1:0").await else {
                return rung(Boundary::Grpc, Vec::new(), 0);
            };
            let Ok(addr) = probe.local_addr() else {
                return rung(Boundary::Grpc, Vec::new(), 0);
            };
            drop(probe);
            tokio::spawn(async move {
                tonic::transport::Server::builder()
                    .add_service(EchoServer::new(Svc))
                    .serve(addr)
                    .await
            });
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            let Ok(mut client) = EchoClient::connect(format!("http://{addr}")).await else {
                return rung(Boundary::Grpc, Vec::new(), 0);
            };

            let mut by_size = Vec::new();
            let mut p99 = 0;
            for &n in &SIZES {
                let payload = vec![7u8; n];
                for _ in 0..200 {
                    let _ = client
                        .call(EchoRequest {
                            payload: payload.clone(),
                        })
                        .await;
                }
                let iters = ITERS / 8;
                let mut out = Vec::with_capacity(iters);
                for _ in 0..iters {
                    let req = EchoRequest {
                        payload: payload.clone(),
                    };
                    let t = Instant::now();
                    let r = client.call(req).await;
                    out.push(t.elapsed().as_nanos() as u64);
                    std::hint::black_box(r.is_ok());
                }
                by_size.push((n, net(percentile(&mut out, 0.50), timer_ns)));
                p99 = net(percentile(&mut out, 0.99), timer_ns);
            }
            let mut r = rung(Boundary::Grpc, by_size, p99);
            r.cost = Cost::fit(&r.by_size);
            r
        })
    }
}
