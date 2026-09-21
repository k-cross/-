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
    /// A warm WASM instance, called through a typed function. Sandboxed, but no kernel
    /// involvement -- the isolation `Ring` was standing in for before this rung existed.
    Wasm,
    /// Shared memory with a spin-polled sequence protocol. No kernel involvement, at the
    /// cost of a burned core. What an ABI extension can achieve at best.
    Ring,
    /// Irreducible user->kernel->user transition, no payload.
    Syscall,
    /// Syscall plus a kernel-buffer copy, same thread. No scheduler involvement.
    Pipe,
    /// Syscall, copy, and a wakeup of a thread that was blocked.
    UnixSocket,
    /// Adds the loopback network stack.
    TcpLoopback,
    /// A bidirectional-stream callout shaped like Envoy's `ext_proc`, on a stream already
    /// open. Adds HTTP/2 DATA framing and protobuf encode/decode without per-call stream
    /// setup.
    ExtProc,
    /// Adds HTTP/2 framing and protobuf encode/decode, and opens a stream per call.
    Grpc,
}

impl Boundary {
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Native => "native call",
            Self::Wasm => "wasm (warm instance)",
            Self::Ring => "shared ring (spin)",
            Self::Syscall => "syscall floor",
            Self::Pipe => "pipe (same thread)",
            Self::UnixSocket => "unix socket RTT",
            Self::TcpLoopback => "TCP loopback RTT",
            Self::ExtProc => "ext_proc callout",
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
        // Noise can fit a negative slope on a rung whose cost barely depends on size. Copying
        // bytes never makes a crossing cheaper, and letting it through would price a large
        // payload below zero once the fit is added to a link cost.
        let slope = (cov / var).max(0.0);
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
    /// Single-figure costs that are not a `by_size` rung because they price an event that
    /// happens once per unit of isolation rather than once per byte: a fresh WASM instance,
    /// or a freshly opened `ext_proc` stream. Best-of-reps like every rung above.
    pub extra: Vec<(&'static str, u64)>,
}

impl Ladder {
    /// A rung whose measurement failed is present but empty, and `Cost::fit` on no samples
    /// is a free crossing. Reporting that as `None` rather than `Some(0 ns)` is what keeps
    /// a failed rung printing as "-" instead of as the cheapest boundary on the ladder.
    #[must_use]
    pub fn get(&self, b: Boundary) -> Option<Cost> {
        self.rungs
            .iter()
            .find(|r| r.boundary == b && !r.by_size.is_empty())
            .map(|r| r.cost)
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
            // A repetition whose rung failed carries no samples. Folding it in would fold
            // in the absence of a measurement as if it were a fast one.
            if r.by_size.is_empty() {
                continue;
            }
            if prev.by_size.is_empty() {
                prev.by_size.clone_from(&r.by_size);
                prev.p99_ns = r.p99_ns;
                if let Some(w) = worst.get_mut(i) {
                    w.clone_from(&r.by_size);
                }
                continue;
            }
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
        for (label, ns) in &l.extra {
            match b.extra.iter_mut().find(|(l2, _)| l2 == label) {
                Some(slot) => slot.1 = slot.1.min(*ns),
                None => b.extra.push((label, *ns)),
            }
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
    let mut rungs = vec![native()];
    #[allow(unused_mut, reason = "pushed to only when wasm or grpc is enabled")]
    let mut extra: Vec<(&'static str, u64)> = Vec::new();

    #[cfg(feature = "wasm")]
    {
        let (r, percall) = wasm::measure(timer_ns);
        rungs.push(r);
        if let Some(ns) = percall {
            extra.push(("wasm: fresh instance + one call", ns));
        }
    }

    rungs.push(ring());
    rungs.push(syscall());
    rungs.push(pipe());
    rungs.push(unix_socket(timer_ns));
    rungs.push(tcp_loopback(timer_ns));
    for r in &mut rungs {
        r.cost = Cost::fit(&r.by_size);
    }

    #[cfg(feature = "grpc")]
    {
        let (r, stream_open) = extproc::measure(timer_ns);
        rungs.push(r);
        if let Some(ns) = stream_open {
            extra.push(("ext_proc: stream open + first callout", ns));
        }
        rungs.push(grpc::measure(timer_ns));
    }

    Ladder {
        rungs,
        timer_ns,
        extra,
    }
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
///
/// Batch-timed, not per-operation: at 52 ns against a ~35 ns timer the old per-operation
/// measurement ran at a 1.5:1 signal-to-instrument ratio, which is why this rung used to
/// carry the ladder's worst spread (4.2x). Batching amortises the clock the same way
/// `native()` and `syscall()` already do, at the cost of the per-operation tail: like those
/// two, this rung reports no p99.
fn ring() -> Rung {
    let mut by_size = Vec::new();
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
        let send = || {
            seq += 1;
            // SAFETY: the client owns the buffer until it publishes `req`; the server is
            // still spinning on the previous sequence number and reads nothing.
            unsafe { (*s.buf.get()).copy_from_slice(&src) };
            s.req.0.store(seq, Ordering::Release);
            while s.rep.0.load(Ordering::Acquire) != seq {
                std::hint::spin_loop();
            }
        };
        let ns = batched(ITERS, 1000, send);
        s.req.0.store(RING_STOP, Ordering::Release);
        server.join().ok();
        by_size.push((n, ns));
    }
    rung(Boundary::Ring, by_size, 0)
}

#[cfg(feature = "wasm")]
#[allow(
    clippy::cast_possible_wrap,
    reason = "payload lengths are bounded by SIZES (max 8192), well under i32::MAX"
)]
mod wasm {
    use super::{Boundary, ITERS, Rung, SIZES, net, percentile, rung};
    use std::time::Instant;
    use wasmtime::{Engine, Instance, Linker, Module, Store, TypedFunc};

    /// Same question as `native()`: sum the first and last byte. No imports, one page of
    /// memory -- enough for the 8 KiB maximum payload.
    const GUEST_WAT: &str = r#"
        (module
          (memory (export "mem") 1)
          (func (export "score") (param $ptr i32) (param $len i32) (result i64)
            (i64.add
              (i64.load8_u (local.get $ptr))
              (i64.load8_u (i32.add (local.get $ptr) (i32.sub (local.get $len) (i32.const 1)))))))
    "#;

    /// A warm sandbox, called through a typed function. The copy into linear memory is
    /// included deliberately: a guest cannot be handed a host pointer, so marshalling into
    /// its address space *is* the boundary being measured.
    ///
    /// Returns the ladder rung alongside a second figure that is not part of it: the cost
    /// of a fresh instance rather than a shared one, which is what per-call isolation (as
    /// opposed to a per-tenant sandbox reused across calls) would actually cost.
    pub fn measure(timer_ns: f64) -> (Rung, Option<u64>) {
        let empty = || (rung(Boundary::Wasm, Vec::new(), 0), None);
        let engine = Engine::default();
        let Ok(module) = Module::new(&engine, GUEST_WAT) else {
            return empty();
        };
        let mut store = Store::new(&engine, ());
        let Ok(instance) = Instance::new(&mut store, &module, &[]) else {
            return empty();
        };
        let Some(memory) = instance.get_memory(&mut store, "mem") else {
            return empty();
        };
        let Ok(score_fn): Result<TypedFunc<(i32, i32), i64>, _> =
            instance.get_typed_func(&mut store, "score")
        else {
            return empty();
        };

        // Correctness gate: a benchmark that times a call which did not happen is worse
        // than no benchmark.
        let probe = [3u8, 5, 9, 7];
        memory.write(&mut store, 0, &probe).expect("write probe");
        let got = score_fn
            .call(&mut store, (0, probe.len() as i32))
            .expect("call guest");
        let want = i64::from(probe[0]) + i64::from(probe[probe.len() - 1]);
        assert_eq!(
            got, want,
            "wasm guest result diverges from host computation"
        );

        let by_size = SIZES
            .iter()
            .map(|&n| {
                let buf = vec![7u8; n];
                let ns = super::batched(ITERS, 1000, || {
                    memory.write(&mut store, 0, &buf).expect("write payload");
                    std::hint::black_box(
                        score_fn
                            .call(&mut store, (0, n as i32))
                            .expect("call guest"),
                    );
                });
                (n, ns)
            })
            .collect();

        let linker: Linker<()> = Linker::new(&engine);
        let percall = linker
            .instantiate_pre(&module)
            .ok()
            .and_then(|pre| percall_instantiate(&engine, &pre, timer_ns));

        (rung(Boundary::Wasm, by_size, 0), percall)
    }

    /// Price of per-call isolation: a fresh store and a fresh instance for every call,
    /// rather than one warm instance reused across a session. Timed per operation --
    /// instantiation is expected to be microseconds, far above the timer's own cost.
    fn percall_instantiate(
        engine: &Engine,
        pre: &wasmtime::InstancePre<()>,
        timer_ns: f64,
    ) -> Option<u64> {
        let n = SIZES[0];
        let buf = vec![7u8; n];
        let run = || -> Option<()> {
            let mut store = Store::new(engine, ());
            let instance = pre.instantiate(&mut store).ok()?;
            let memory = instance.get_memory(&mut store, "mem")?;
            let score_fn: TypedFunc<(i32, i32), i64> =
                instance.get_typed_func(&mut store, "score").ok()?;
            memory.write(&mut store, 0, &buf).ok()?;
            std::hint::black_box(score_fn.call(&mut store, (0, n as i32)).ok()?);
            Some(())
        };
        for _ in 0..50 {
            run();
        }
        let iters = 2_000;
        let mut out = Vec::with_capacity(iters);
        for _ in 0..iters {
            let t = Instant::now();
            let ok = run().is_some();
            let ns = t.elapsed().as_nanos() as u64;
            // A failed instantiation is not a cheap one; timing the failure path would
            // publish it as the price of isolation.
            if ok {
                out.push(ns);
            }
        }
        if out.len() < iters / 2 {
            return None;
        }
        Some(net(percentile(&mut out, 0.50), timer_ns))
    }
}

#[cfg(feature = "grpc")]
mod rpc {
    use std::net::SocketAddr;
    use std::time::Duration;

    /// Bind, read the assigned port, release it for the server to claim. The window
    /// between the release and the server's own bind is a race, but it is the only way to
    /// learn an ephemeral port before the server owns it.
    pub async fn ephemeral_addr() -> Option<SocketAddr> {
        let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.ok()?;
        let addr = probe.local_addr().ok()?;
        drop(probe);
        Some(addr)
    }

    /// Poll until the server accepts, rather than sleeping a guessed interval: at
    /// `--repeat 10` a fixed 250 ms wait per server is seconds of pure sleep.
    pub async fn wait_ready(addr: SocketAddr) -> bool {
        for _ in 0..400 {
            if tokio::net::TcpStream::connect(addr).await.is_ok() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        false
    }
}

/// A bidirectional-stream callout shaped like Envoy's `ext_proc`: a header map in, a
/// header mutation out, three levels of nesting each way with a `oneof` on both sides.
/// Envoy's real descriptor carries more optional fields, so this is a lower bound on the
/// real callout -- the right direction, since the claim it tests is that the callout is
/// too expensive for an argmin.
#[cfg(feature = "grpc")]
mod extproc {
    use super::{Boundary, Cost, ITERS, Rung, SIZES, net, percentile, rung};
    use std::time::Instant;
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;

    #[allow(
        clippy::pedantic,
        clippy::result_large_err,
        reason = "tonic-generated code"
    )]
    mod pb {
        tonic::include_proto!("extproc");
    }
    use pb::external_processor_client::ExternalProcessorClient;
    use pb::external_processor_server::{ExternalProcessor, ExternalProcessorServer};
    use pb::{
        CommonResponse, HeaderMap, HeaderMutation, HeaderValue, HeaderValueOption, HeadersResponse,
        HttpHeaders, ProcessingRequest, ProcessingResponse, processing_request,
        processing_response,
    };

    #[derive(Debug)]
    struct Svc;

    #[tonic::async_trait]
    impl ExternalProcessor for Svc {
        type ProcessStream = tonic::codegen::BoxStream<ProcessingResponse>;

        async fn process(
            &self,
            request: tonic::Request<tonic::Streaming<ProcessingRequest>>,
        ) -> Result<tonic::Response<Self::ProcessStream>, tonic::Status> {
            let mut inbound = request.into_inner();
            let (tx, rx) = mpsc::channel(4);
            tokio::spawn(async move {
                while let Ok(Some(_req)) = inbound.message().await {
                    let reply = ProcessingResponse {
                        response: Some(processing_response::Response::RequestHeaders(
                            HeadersResponse {
                                response: Some(CommonResponse {
                                    header_mutation: Some(HeaderMutation {
                                        set_headers: vec![HeaderValueOption {
                                            header: Some(HeaderValue {
                                                key: "x-gateway-destination-endpoint".into(),
                                                value: "10.0.0.1:8000".into(),
                                            }),
                                        }],
                                    }),
                                }),
                            },
                        )),
                    };
                    if tx.send(Ok(reply)).await.is_err() {
                        break;
                    }
                }
            });
            Ok(tonic::Response::new(Box::pin(ReceiverStream::new(rx))))
        }
    }

    /// A realistic gateway header map at a fixed field count, with one filler value padded
    /// so the encoded message reaches `total_bytes`. Holding the count fixed keeps
    /// per-field decode cost constant across sizes, so the fitted slope reflects bytes
    /// rather than field count.
    ///
    /// Returns the size it actually reached, which is not always the size asked for: the
    /// unpadded map already encodes to ~370 bytes, so the ladder's 64 B rung is below this
    /// rung's floor and padding cannot go downwards. Reporting the achieved size keeps
    /// `Cost::fit` honest -- fitting a 370-byte sample at x=64 would push the slope into
    /// the intercept and misprice every extrapolation drawn from it.
    fn build_request(total_bytes: usize) -> (ProcessingRequest, usize) {
        let base = [
            (":authority", "svc.default.svc.cluster.local"),
            (":method", "POST"),
            (":path", "/v1/completions"),
            ("content-type", "application/json"),
            ("content-length", "1024"),
            ("user-agent", "vllm-client/1.0"),
            ("authorization", "Bearer redacted-token-value"),
            ("x-request-id", "9f4d9f1e-2b7a-4c3a-9c2e-9b6a2b1e7f3a"),
            (
                "traceparent",
                "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
            ),
        ];
        let mut headers: Vec<HeaderValue> = base
            .iter()
            .map(|(k, v)| HeaderValue {
                key: (*k).to_string(),
                value: (*v).to_string(),
            })
            .collect();
        headers.push(HeaderValue {
            key: "x-filler".to_string(),
            value: String::new(),
        });

        let mut req = ProcessingRequest {
            request: Some(processing_request::Request::RequestHeaders(HttpHeaders {
                headers: Some(HeaderMap { headers }),
            })),
        };
        while prost::Message::encoded_len(&req) < total_bytes {
            let Some(processing_request::Request::RequestHeaders(hh)) = req.request.as_mut() else {
                break;
            };
            let Some(hm) = hh.headers.as_mut() else {
                break;
            };
            let Some(last) = hm.headers.last_mut() else {
                break;
            };
            last.value.push('x');
        }
        let actual = prost::Message::encoded_len(&req);
        (req, actual)
    }

    fn mutation(resp: &ProcessingResponse) -> Option<&HeaderMutation> {
        match &resp.response {
            Some(processing_response::Response::RequestHeaders(hr)) => {
                hr.response.as_ref()?.header_mutation.as_ref()
            }
            None => None,
        }
    }

    /// Same bytes and the same question as `grpc::measure`: what the extra shape of an
    /// `ext_proc` callout costs above a unary echo. Two numbers, because there are two
    /// deployment shapes: a callout on a stream already open (a long-lived processor), and
    /// stream-open-plus-first-callout (Envoy's default, a stream per HTTP request).
    pub fn measure(timer_ns: f64) -> (Rung, Option<u64>) {
        let empty = || (rung(Boundary::ExtProc, Vec::new(), 0), None);
        let Ok(rt) = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
        else {
            return empty();
        };
        rt.block_on(async move {
            let Some(addr) = super::rpc::ephemeral_addr().await else {
                return empty();
            };
            tokio::spawn(async move {
                tonic::transport::Server::builder()
                    .add_service(ExternalProcessorServer::new(Svc))
                    .serve(addr)
                    .await
            });
            if !super::rpc::wait_ready(addr).await {
                return empty();
            }
            let Ok(mut client) = ExternalProcessorClient::connect(format!("http://{addr}")).await
            else {
                return empty();
            };

            // Correctness gate: assert the reply on the wire actually carries the header
            // mutation, so a full round trip is what gets timed below.
            {
                let (tx, rx) = mpsc::channel::<ProcessingRequest>(4);
                let Ok(resp) = client.process(ReceiverStream::new(rx)).await else {
                    return empty();
                };
                let mut resp = resp.into_inner();
                if tx.send(build_request(SIZES[0]).0).await.is_err() {
                    return empty();
                }
                let Ok(Some(reply)) = resp.message().await else {
                    return empty();
                };
                let has_mutation = mutation(&reply).is_some_and(|m| {
                    m.set_headers.iter().any(|h| {
                        h.header
                            .as_ref()
                            .is_some_and(|hv| hv.key == "x-gateway-destination-endpoint")
                    })
                });
                assert!(has_mutation, "ext_proc reply missing header mutation");
            }

            let mut by_size = Vec::new();
            let mut p99 = 0;
            for &n in &SIZES {
                // A size that fails is left out rather than recorded as zero: `measure`
                // keeps the minimum across repetitions, so a zero would publish this
                // boundary as free and never be beaten.
                if let Some((bytes, p50, tail)) = established_stream(&mut client, n, timer_ns).await
                {
                    by_size.push((bytes, p50));
                    p99 = tail;
                }
            }
            let mut r = rung(Boundary::ExtProc, by_size, p99);
            r.cost = Cost::fit(&r.by_size);

            let stream_open = stream_open_plus_first(&mut client, SIZES[0], timer_ns).await;
            (r, stream_open)
        })
    }

    /// Per callout on a stream that stays open, modelling a long-lived processor
    /// connection: DATA frames and codec only, no per-call stream setup.
    async fn established_stream(
        client: &mut ExternalProcessorClient<tonic::transport::Channel>,
        n: usize,
        timer_ns: f64,
    ) -> Option<(usize, u64, u64)> {
        let (tx, rx) = mpsc::channel::<ProcessingRequest>(4);
        let resp = client.process(ReceiverStream::new(rx)).await.ok()?;
        let mut resp = resp.into_inner();
        let (req, bytes) = build_request(n);

        for _ in 0..200 {
            tx.send(req.clone()).await.ok()?;
            if !matches!(resp.message().await, Ok(Some(_))) {
                return None;
            }
        }
        let iters = ITERS / 8;
        let mut out = Vec::with_capacity(iters);
        for _ in 0..iters {
            // Cloned before the clock starts, as `grpc::measure` builds its request
            // before starting the clock: charging one rung for a deep copy the rung it
            // is compared against does not pay puts the difference straight into
            // `ns_per_byte`.
            let msg = req.clone();
            let t = Instant::now();
            if tx.send(msg).await.is_err() {
                break;
            }
            // `Ok(None)` is a closed stream, not a round trip. Counting it would record
            // the cost of `send` alone and collapse the median toward zero.
            if !matches!(resp.message().await, Ok(Some(_))) {
                break;
            }
            out.push(t.elapsed().as_nanos() as u64);
        }
        if out.len() < iters / 2 {
            return None;
        }
        Some((
            bytes,
            net(percentile(&mut out, 0.50), timer_ns),
            net(percentile(&mut out, 0.99), timer_ns),
        ))
    }

    /// Envoy's default shape: a new HTTP/2 stream per HTTP request. Times opening the
    /// callout stream and receiving its first reply, as a single figure rather than a
    /// `by_size` row -- the setup cost this prices does not scale with payload the way a
    /// marshalling slope does.
    async fn stream_open_plus_first(
        client: &mut ExternalProcessorClient<tonic::transport::Channel>,
        n: usize,
        timer_ns: f64,
    ) -> Option<u64> {
        let (req, _) = build_request(n);
        for _ in 0..20 {
            let (tx, rx) = mpsc::channel::<ProcessingRequest>(4);
            let Ok(resp) = client.process(ReceiverStream::new(rx)).await else {
                continue;
            };
            let _ = tx.send(req.clone()).await;
            let _ = resp.into_inner().message().await;
        }

        let iters = 500;
        let mut out = Vec::with_capacity(iters);
        for _ in 0..iters {
            let (tx, rx) = mpsc::channel::<ProcessingRequest>(4);
            let msg = req.clone();
            let t = Instant::now();
            let call = client.process(ReceiverStream::new(rx));
            let send = tx.send(msg);
            let (call, _send) = tokio::join!(call, send);
            let Ok(resp) = call else { continue };
            let Ok(Some(_first)) = resp.into_inner().message().await else {
                continue;
            };
            out.push(t.elapsed().as_nanos() as u64);
        }
        if out.len() < iters / 2 {
            return None;
        }
        Some(net(percentile(&mut out, 0.50), timer_ns))
    }
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
            let Some(addr) = super::rpc::ephemeral_addr().await else {
                return rung(Boundary::Grpc, Vec::new(), 0);
            };
            tokio::spawn(async move {
                tonic::transport::Server::builder()
                    .add_service(EchoServer::new(Svc))
                    .serve(addr)
                    .await
            });
            if !super::rpc::wait_ready(addr).await {
                return rung(Boundary::Grpc, Vec::new(), 0);
            }
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
