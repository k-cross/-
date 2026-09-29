use std::cell::UnsafeCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Instant;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Boundary {
    Native,

    Wasm,

    Ring,

    Syscall,

    Pipe,

    UnixSocket,

    TcpLoopback,

    ExtProc,

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

    pub by_size: Vec<(usize, u64)>,
    pub cost: Cost,

    pub p99_ns: u64,

    pub spread: f64,
}

#[derive(Clone, Debug, Default)]
pub struct Ladder {
    pub rungs: Vec<Rung>,

    pub timer_ns: f64,

    pub extra: Vec<(&'static str, u64)>,
}

impl Ladder {
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

#[must_use]
pub fn timer_overhead() -> f64 {
    const N: usize = 200_000;
    let t = Instant::now();
    for _ in 0..N {
        std::hint::black_box(Instant::now());
    }
    t.elapsed().as_nanos() as f64 / N as f64
}

fn net(ns: u64, timer_ns: f64) -> u64 {
    (ns as f64 - timer_ns).max(0.0) as u64
}

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

pub const SIZES: [usize; 3] = [64, 1024, 8192];

pub const EXTPROC_STREAM_OPEN: &str = "ext_proc: stream open + first callout";
const ITERS: usize = 20_000;

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

    let mut rungs = vec![native()];
    #[allow(unused_mut)]
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
            extra.push((EXTPROC_STREAM_OPEN, ns));
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
#[allow(clippy::cast_possible_wrap)]
mod wasm {
    use super::{Boundary, ITERS, Rung, SIZES, net, percentile, rung};
    use std::time::Instant;
    use wasmtime::{Engine, Instance, Linker, Module, Store, TypedFunc};

    const GUEST_WAT: &str = r#"
        (module
          (memory (export "mem") 1)
          (func (export "score") (param $ptr i32) (param $len i32) (result i64)
            (i64.add
              (i64.load8_u (local.get $ptr))
              (i64.load8_u (i32.add (local.get $ptr) (i32.sub (local.get $len) (i32.const 1)))))))
    "#;

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

    pub async fn ephemeral_addr() -> Option<SocketAddr> {
        let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.ok()?;
        let addr = probe.local_addr().ok()?;
        drop(probe);
        Some(addr)
    }

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

#[cfg(feature = "grpc")]
mod extproc {
    use super::{Boundary, Cost, ITERS, Rung, SIZES, net, percentile, rung};
    use std::time::Instant;
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;

    #[allow(clippy::pedantic, clippy::result_large_err)]
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
            let msg = req.clone();
            let t = Instant::now();
            if tx.send(msg).await.is_err() {
                break;
            }

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

    #[allow(clippy::pedantic, clippy::result_large_err)]
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
