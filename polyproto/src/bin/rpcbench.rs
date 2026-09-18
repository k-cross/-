use std::collections::HashSet;
use std::time::Instant;

use polyphonic::blob::BlobKind;
use polyphonic::cache::{Hierarchy, NodeMemory, Policy, Quota, accelerated};
use polyphonic::flow::FlowHint;

#[allow(
    clippy::pedantic,
    clippy::result_large_err,
    reason = "tonic-generated code"
)]
pub mod pb {
    tonic::include_proto!("admission");
}

use pb::admission_client::AdmissionClient;
use pb::admission_server::{Admission, AdmissionServer};
use pb::{Blob, CanSatisfyReply, CanSatisfyRequest};

const HBM: u64 = 4 << 30;
const DRAM: u64 = 8 << 30;
const NVME: u64 = 64 << 30;
const WARMUP_OPS: u64 = 6000;
const ITERS: usize = 3000;

/// What the ledger would tell a remote asker: the hot set, and what each pool could still give
/// up. The two pools are separate budgets, so a remote answer has to carry both or it answers
/// a different question from the in-process one.
struct Downstream {
    resident: HashSet<[u8; 32]>,
    reclaimable_hbm: u64,
    reclaimable_ddr: u64,
}

#[tonic::async_trait]
impl Admission for Downstream {
    async fn can_satisfy(
        &self,
        req: tonic::Request<CanSatisfyRequest>,
    ) -> Result<tonic::Response<CanSatisfyReply>, tonic::Status> {
        let r = req.into_inner();
        let (mut missing_hbm, mut missing_ddr) = (0u64, 0u64);
        for b in &r.downstream {
            // A malformed id must not silently hash to zeros and report as non-resident:
            // that answers a different question than the one asked.
            let key: [u8; 32] = b.id.as_slice().try_into().map_err(|_| {
                tonic::Status::invalid_argument(format!(
                    "blob id must be 32 bytes, got {}",
                    b.id.len()
                ))
            })?;
            if self.resident.contains(&key) {
                continue;
            }
            if b.accelerated {
                missing_hbm += b.bytes;
            } else {
                missing_ddr += b.bytes;
            }
        }
        Ok(tonic::Response::new(CanSatisfyReply {
            ok: missing_hbm <= self.reclaimable_hbm && missing_ddr <= self.reclaimable_ddr,
            reclaimable: self.reclaimable_ddr,
            reclaimable_hbm: self.reclaimable_hbm,
        }))
    }
}

fn percentile(v: &[u64], q: f64) -> u64 {
    if v.is_empty() {
        return 0;
    }
    v[(((v.len() as f64) * q) as usize).min(v.len() - 1)]
}

fn report(name: &str, mut ns: Vec<u64>) -> u64 {
    ns.sort_unstable();
    let p50 = percentile(&ns, 0.50);
    println!(
        "{name:<34} {:>12.3} {:>12.3} {:>12.3}",
        p50 as f64 / 1000.0,
        percentile(&ns, 0.99) as f64 / 1000.0,
        percentile(&ns, 0.999) as f64 / 1000.0,
    );
    p50
}

/// Build a genuinely populated ledger, and collect the flow hints the gate would query on.
fn populate() -> (Hierarchy, Vec<FlowHint>, Vec<u64>) {
    let bands = [0u8, 1, 2, 1];
    let mut h = Hierarchy::new(
        NodeMemory {
            hbm: HBM,
            ddr: DRAM,
            nvme: NVME,
            hbm_quota: Quota::from_split(HBM, [0.25, 0.0, 0.50, 0.0], bands, false),
            ddr_quota: Quota::from_split(DRAM, [0.10, 0.15, 0.15, 0.35], bands, false).offloaded(),
            can_decode: true,
        },
        Policy::Gdsf,
    );
    let mut hints = Vec::new();
    let mut access_ns = Vec::new();
    for req in polyphonic::work::Workload::new(1, WARMUP_OPS, 1.0) {
        let t = Instant::now();
        let c = h.access(&req.chain);
        access_ns.push(t.elapsed().as_nanos() as u64);
        std::hint::black_box(c.pending);
        if let Some(hint) = req.hint {
            hints.push(hint);
        }
    }
    (h, hints, access_ns)
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (h, hints, access_ns) = populate();
    let resident_blobs = h.hot_ids().count();
    let Some(sample) = hints.first() else {
        return Err("warmup produced no flow hints; nothing to benchmark".into());
    };
    let payload: Vec<Blob> = sample
        .downstream
        .iter()
        .map(|(id, m)| Blob {
            id: id.as_bytes().to_vec(),
            bytes: m.bytes,
            accelerated: accelerated(m.kind),
        })
        .collect();
    let wire_bytes: usize = payload.iter().map(|b| b.id.len() + 9).sum();

    println!(
        "ledger: {resident_blobs} resident blobs, {:.1} GiB used, {} flow hints",
        BlobKind::ALL
            .iter()
            .map(|k| h.resident_bytes(*k))
            .sum::<u64>() as f64
            / (1u64 << 30) as f64,
        hints.len()
    );
    println!(
        "query: {} downstream blobs, ~{wire_bytes} B payload\n",
        payload.len()
    );
    println!(
        "{:<34} {:>12} {:>12} {:>12}",
        "admission query", "p50 (us)", "p99 (us)", "p999 (us)"
    );

    // 1. in-process: the ledger answers its own question
    let mut direct = Vec::with_capacity(ITERS);
    for i in 0..ITERS {
        let hint = &hints[i % hints.len()];
        let t = Instant::now();
        let ok = h.can_satisfy(hint);
        direct.push(t.elapsed().as_nanos() as u64);
        std::hint::black_box(ok);
    }
    let p50_direct = report("in-process (direct call)", direct);

    // 2. real gRPC over TCP loopback: the sidecar/extender shape
    let resident: HashSet<[u8; 32]> = h.hot_ids().map(|id| *id.as_bytes()).collect();
    let svc = Downstream {
        resident,
        reclaimable_hbm: h.hbm.reclaimable(),
        reclaimable_ddr: h.ddr.reclaimable(),
    };
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = probe.local_addr()?;
    drop(probe);
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(AdmissionServer::new(svc))
            .serve(addr)
            .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let mut client = AdmissionClient::connect(format!("http://{addr}")).await?;
    for _ in 0..200 {
        let _ = client
            .can_satisfy(CanSatisfyRequest {
                task: 0,
                downstream: payload.clone(),
            })
            .await?;
    }
    let mut rpc = Vec::with_capacity(ITERS);
    for i in 0..ITERS {
        let req = CanSatisfyRequest {
            task: i as u64,
            downstream: payload.clone(),
        };
        let t = Instant::now();
        let reply = client.can_satisfy(req).await?;
        rpc.push(t.elapsed().as_nanos() as u64);
        std::hint::black_box(reply.into_inner().ok);
    }
    let p50_rpc = report("gRPC unary (TCP loopback)", rpc);

    // 3. raw loopback echo with the same payload: the transport floor under gRPC
    let floor = raw_tcp_floor(wire_bytes).await?;
    let p50_floor = report("raw TCP echo (same payload)", floor);

    println!(
        "\ngRPC is {:.0}x the in-process call; {:.0}% of it is HTTP/2 + protobuf above raw sockets",
        p50_rpc as f64 / p50_direct.max(1) as f64,
        100.0 * (p50_rpc.saturating_sub(p50_floor)) as f64 / p50_rpc.max(1) as f64,
    );

    decision_rates(&h, &access_ns, p50_rpc);
    Ok(())
}

/// Does the boundary matter? Only relative to the work each decision governs. `access` is
/// measured directly; the gRPC column is an extrapolation of what it would cost if each
/// in-process decision instead crossed a sidecar boundary.
fn decision_rates(h: &Hierarchy, access_ns: &[u64], p50_rpc: u64) {
    let mut sorted = access_ns.to_vec();
    sorted.sort_unstable();
    let p50_access = percentile(&sorted, 0.50);
    let evictions: u64 = h.hbm.evicted.iter().chain(&h.ddr.evicted).sum();
    let per_req = evictions as f64 / access_ns.len() as f64 + 1.0;

    println!(
        "\nchain access, measured in-process: {:.2} us p50 (includes {:.1} evictions/request)",
        p50_access as f64 / 1000.0,
        evictions as f64 / access_ns.len() as f64
    );
    let ceil_in = 1e9 / p50_access.max(1) as f64;
    let ceil_rpc = 1e9 / (per_req * p50_rpc as f64);
    println!("\nsingle decision thread");
    println!("  in-process {ceil_in:>10.0} req/s   (measured)");
    println!(
        "  over gRPC  {ceil_rpc:>10.0} req/s   (extrapolated: {per_req:.1} decisions x {:.1} us)",
        p50_rpc as f64 / 1000.0
    );
    println!("  ratio      {:>10.0}x", ceil_in / ceil_rpc);
}

async fn raw_tcp_floor(payload_bytes: usize) -> Result<Vec<u64>, Box<dyn std::error::Error>> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.expect("accept");
        sock.set_nodelay(true).ok();
        // Reply once per logical message, not once per read(): a payload split across two
        // reads would otherwise pre-buffer a reply and make the next RTT look near-zero.
        let mut buf = vec![0u8; payload_bytes];
        loop {
            if sock.read_exact(&mut buf).await.is_err() {
                return;
            }
            if sock.write_all(&[1u8; 9]).await.is_err() {
                return;
            }
        }
    });
    let mut sock = tokio::net::TcpStream::connect(addr).await?;
    sock.set_nodelay(true)?;
    let msg = vec![7u8; payload_bytes];
    let mut reply = [0u8; 9];
    let mut out = Vec::with_capacity(ITERS);
    for _ in 0..200 {
        sock.write_all(&msg).await?;
        sock.read_exact(&mut reply).await?;
    }
    for _ in 0..ITERS {
        let t = Instant::now();
        sock.write_all(&msg).await?;
        sock.read_exact(&mut reply).await?;
        out.push(t.elapsed().as_nanos() as u64);
    }
    Ok(out)
}
