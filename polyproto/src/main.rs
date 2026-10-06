mod belief_cmd;
mod durability_cmd;
mod enforce_cmd;
mod fleet_cmd;
mod influence_cmd;
mod programs_cmd;
mod regions_cmd;

use clap::{Parser, Subcommand};
use polyphonic::admit::Reserve;
use polyphonic::arms::{Budget, Correction, EngineArm, Report, Trial, mean_ms, run, run_on, trace};
use polyphonic::blob::BlobKind;
use polyphonic::cache::{Ahead, AnnounceMix, EngineKv, Half, NodeMemory, Policy, Quota};
use polyphonic::flow::FlowMode;
use polyphonic::own::{Authority, Question, authority};
use polyphonic::tier::Tier;

#[derive(Parser, Debug)]
#[command(name = "polyphonic", about = "state-residency scheduler experiments")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
#[allow(clippy::large_enum_variant)]
enum Cmd {
    /// Unified vs siloed residency ledger under a phase-shifting workload
    Residency {
        /// Accelerator HBM holding KV and weights, e.g. 4GiB. 0 models unified memory, where
        /// every class shares the DRAM pool
        #[arg(long, default_value = "4GiB", value_parser = parse_bytes)]
        hbm: u64,
        /// Host DDR capacity, e.g. 8GiB
        #[arg(long, default_value = "8GiB", value_parser = parse_bytes)]
        dram: u64,
        /// `NVMe` tier capacity
        #[arg(long, default_value = "64GiB", value_parser = parse_bytes)]
        nvme: u64,
        /// Requests to issue
        #[arg(long, default_value_t = 60_000)]
        ops: u64,
        #[arg(long, default_value_t = 1)]
        seed: u64,
        /// Static-partition sweep granularity for the siloed arms
        #[arg(long, default_value_t = 0.125)]
        step: f64,
        /// Phase-shift amplitude: 0 = flat mix, 1 = full swing
        #[arg(long, default_value_t = 1.0)]
        volatility: f64,
        /// Priority bands as inference,faas,weights,service (0 = highest)
        #[arg(long, default_value = "0,1,2,1", value_parser = parse_bands)]
        bands: String,
        /// Add a furthest-next-use eviction baseline (phase-2.md §1.7, §4.5): a diagnostic,
        /// not an arm competing with hard-partition/soft-floor. It needs the whole trace ahead
        /// of time, so it exists to separate eviction quality from budget policy
        #[arg(long)]
        clairvoyant: bool,
        #[command(flatten)]
        p3: Correct,
    },

    /// Do cross-workload flows pay: blind vs. anticipatory value vs. downstream-aware admission
    Flows {
        /// Accelerator HBM holding KV and weights, e.g. 4GiB. 0 models unified memory, where
        /// every class shares the DRAM pool
        #[arg(long, default_value = "4GiB", value_parser = parse_bytes)]
        hbm: u64,
        #[arg(long, default_value = "8GiB", value_parser = parse_bytes)]
        dram: u64,
        #[arg(long, default_value = "64GiB", value_parser = parse_bytes)]
        nvme: u64,
        #[arg(long, default_value_t = 15_000)]
        ops: u64,
        #[arg(long, default_value_t = 1)]
        seed: u64,
        #[arg(long, default_value_t = 0.125)]
        step: f64,
        #[arg(long, default_value = "0,1,2,1", value_parser = parse_bands)]
        bands: String,
        /// Add a furthest-next-use eviction baseline (phase-2.md §1.7, §4.5), blind to flows
        /// so it is not confounded with the prewarm effect
        #[arg(long)]
        clairvoyant: bool,
        #[command(flatten)]
        p3: Correct,
        #[command(flatten)]
        ahead: AheadArgs,
    },

    /// State-blind vs state-aware placement over a synthetic multi-domain machine
    Placement {
        /// Memory domains (sockets). Link costs between them are MODELLED, not measured.
        #[arg(long, default_value_t = 4)]
        sockets: usize,
        #[arg(long, default_value_t = 3)]
        units_per_socket: usize,
        /// Total accelerator HBM across all domains, holding KV and weights. 0 models unified
        /// memory; compare against it with the HBM added to --dram
        #[arg(long, default_value = "16GiB", value_parser = parse_bytes)]
        hbm: u64,
        /// Total host DDR across all domains
        #[arg(long, default_value = "32GiB", value_parser = parse_bytes)]
        dram: u64,
        #[arg(long, default_value = "64GiB", value_parser = parse_bytes)]
        nvme: u64,
        #[arg(long, default_value_t = 15_000)]
        ops: u64,
        #[arg(long, default_value_t = 1)]
        seed: u64,
        #[arg(long, default_value = "0,1,2,1", value_parser = parse_bands)]
        bands: String,
        /// Arrival rate in requests/sec, driving the engine model. 0 charges a flat
        /// per-token decode cost instead
        #[arg(long, default_value_t = 350.0)]
        rate: f64,
        /// Drain one domain at this fraction through the trace (0 = never). Its state
        /// migrates to the survivors, so the bytes remain but every hash to it is stale.
        #[arg(long, default_value_t = 0.0)]
        drain_at: f64,
        /// Drain the retired domain's spill tier too (phase-3.md §1.12's fix, its own bit)
        #[arg(long)]
        drain_spill: bool,
        #[command(flatten)]
        p3: Correct,
    },

    /// Residency-aware placement across a cluster, with the control plane's own cost charged
    Distributed {
        #[arg(long, default_value_t = 4)]
        nodes: usize,
        #[arg(long, default_value_t = 3)]
        units_per_node: usize,
        /// Total accelerator HBM across all nodes, holding KV and weights. 0 models unified
        /// memory; compare against it with the HBM added to --dram
        #[arg(long, default_value = "16GiB", value_parser = parse_bytes)]
        hbm: u64,
        /// Total host DDR across all nodes
        #[arg(long, default_value = "32GiB", value_parser = parse_bytes)]
        dram: u64,
        #[arg(long, default_value = "64GiB", value_parser = parse_bytes)]
        nvme: u64,
        #[arg(long, default_value_t = 15_000)]
        ops: u64,
        #[arg(long, default_value_t = 1)]
        seed: u64,
        #[arg(long, default_value = "0,1,2,1", value_parser = parse_bands)]
        bands: String,
        /// Node distances to sweep
        #[arg(long, default_value = "rack,zone,region")]
        distances: String,
        /// Transport the control plane crosses on: native|wasm|ring|syscall|pipe|unix|tcp|extproc|grpc
        #[arg(long, default_value = "grpc")]
        crossing: String,
        /// Requests between gossip refreshes for the stale-view arm
        #[arg(long, default_value_t = 200)]
        gossip_period: u64,
        /// Arrival rate in requests/sec. Drives the engine model: decode cost depends on
        /// batch occupancy, which depends on load. 0 charges a flat per-token cost instead
        #[arg(long, default_value_t = 250.0)]
        rate: f64,
        /// Fraction of agent turns that fan out to sub-agents (each making function tool calls);
        /// 0 leaves fan-outs out of the stream
        #[arg(long, default_value_t = 0.10)]
        fanout: f64,
        /// Boundary-ladder repetitions
        #[arg(long, default_value_t = 3)]
        repeat: usize,
        /// Print the regret decomposition, feasibility regret, and coupled % on both axes
        /// (owned-and-observed.md §3.4, phase-2.md). Prices every candidate a second time
        /// against truth, so it costs real wall time and is off by default
        #[arg(long)]
        regret: bool,
        /// Override the `FaaS` -> inference handoff payload (bytes). phase-2.md §4.7's knob for
        /// re-running residency-ledger.md's flow-payload sweep under --regret
        #[arg(long, value_parser = parse_bytes)]
        flow_payload: Option<u64>,
        #[command(flatten)]
        p3: Correct,
        #[command(flatten)]
        bits: ClusterBits,
        #[command(flatten)]
        belief: BeliefArgs,
        #[command(flatten)]
        influence: InfluenceArgs,
        #[command(flatten)]
        fleet: FleetArgs,
        #[command(flatten)]
        enforce: EnforceArgs,
    },

    /// One model host and one agent-framework host, swept from same-socket to cross-region.
    /// The agent host has no accelerator: it can never decode, only run tool calls and hold
    /// its own bookkeeping. Every reasoning step pays the round trip to the model host; tool
    /// calls default to the agent host and ship only when that is actually cheaper.
    CodeReview {
        /// Model host's accelerator memory, holding KV and weight shards
        #[arg(long, default_value = "24GiB", value_parser = parse_bytes)]
        hbm: u64,
        /// Model host's DDR: the offload target under HBM
        #[arg(long, default_value = "16GiB", value_parser = parse_bytes)]
        model_ddr: u64,
        /// Agent host's DDR: tool-call (`FaaS`) cells and orchestration bookkeeping. This
        /// node has no HBM and no decode engine
        #[arg(long, default_value = "32GiB", value_parser = parse_bytes)]
        agent_ddr: u64,
        #[arg(long, default_value = "64GiB", value_parser = parse_bytes)]
        nvme: u64,
        #[arg(long, default_value_t = 3)]
        units_per_node: usize,
        #[arg(long, default_value_t = 15_000)]
        ops: u64,
        #[arg(long, default_value_t = 1)]
        seed: u64,
        #[arg(long, default_value = "0,1,2,1", value_parser = parse_bands)]
        bands: String,
        /// Distances to sweep, local to cross-region
        #[arg(long, default_value = "socket,rack,zone,region")]
        distances: String,
        /// Transport the control plane crosses on: native|wasm|ring|syscall|pipe|unix|tcp|extproc|grpc
        #[arg(long, default_value = "grpc")]
        crossing: String,
        #[arg(long, default_value_t = 200)]
        gossip_period: u64,
        /// Arrival rate in requests/sec. One engine serves the whole cluster here, so the
        /// saturation knee sits near half the symmetric-cluster rate -- about 130/s at these
        /// defaults. 110 keeps the sweep below it, where placement still decides something
        #[arg(long, default_value_t = 110.0)]
        rate: f64,
        /// Fraction of agent turns that call a tool (read a file, grep, run tests) -- a code
        /// review agent reaches for one far more often than a chat agent does
        #[arg(long, default_value_t = 0.70)]
        tool_fraction: f64,
        /// Tool-call payload: file-sized, not a small function argument
        #[arg(long, default_value = "512KiB", value_parser = parse_bytes)]
        tool_payload: u64,
        /// Give every class a fixed, non-borrowable slice, the way separate orchestrators
        /// owning separate budgets would. Off means one ledger arbitrates all of them
        #[arg(long)]
        hard_pools: bool,
        #[arg(long, default_value_t = 3)]
        repeat: usize,
        /// Print the regret decomposition, feasibility regret, and coupled % on both axes
        /// (phase-2.md). Prices every candidate a second time against truth, so it costs real
        /// wall time and is off by default
        #[arg(long)]
        regret: bool,
        #[command(flatten)]
        p3: Correct,
        #[command(flatten)]
        bits: ClusterBits,
    },

    /// The data path as an arm: integrated vs. sidecar, charging Phase 0's measured seam
    /// costs per request. Same trace and placement policy across arms, so the only
    /// difference is who pays the routing hook and the dispatch hop, and what it costs.
    /// Publishes the tax, the crossover against service time, and the fleet size at which
    /// an unsharded scheduler saturates. See docs/phase-8.md.
    DataPath {
        #[arg(long, default_value_t = 4)]
        nodes: usize,
        #[arg(long, default_value_t = 3)]
        units_per_node: usize,
        #[arg(long, default_value = "16GiB", value_parser = parse_bytes)]
        hbm: u64,
        #[arg(long, default_value = "32GiB", value_parser = parse_bytes)]
        dram: u64,
        #[arg(long, default_value = "64GiB", value_parser = parse_bytes)]
        nvme: u64,
        #[arg(long, default_value_t = 15_000)]
        ops: u64,
        #[arg(long, default_value_t = 1)]
        seed: u64,
        #[arg(long, default_value = "0,1,2,1", value_parser = parse_bands)]
        bands: String,
        /// Node distance the class table and `d` are measured at. The tax and the fleet
        /// ceiling come from the ladder alone and do not depend on it.
        #[arg(long, default_value = "rack")]
        distance: String,
        #[arg(long, default_value_t = 250.0)]
        rate: f64,
        #[arg(long, default_value_t = 0.10)]
        fanout: f64,
        /// Boundary-ladder repetitions
        #[arg(long, default_value_t = 5)]
        repeat: usize,
        /// Override the tax the crossover divides by, in microseconds: a per-*request* figure,
        /// the `realized/req` column, not one crossing. Recomputes the crossover for a host
        /// this prototype has never run on -- phase-8.md §1.2, §4.5
        #[arg(long)]
        tax_us: Option<f64>,
    },

    /// Discover the host's compute/memory graph and measure its link asymmetry
    Topology {
        /// Streaming buffer per probe; must exceed the largest cache to measure memory
        #[arg(long, default_value = "256MiB", value_parser = parse_bytes)]
        bytes: u64,
        #[arg(long, default_value_t = 5)]
        iters: u32,
    },

    /// Measure this machine's real tier costs: page-fault, spill write, spill read
    Calibrate {
        /// Backing file for the spill tier
        #[arg(long, default_value = "target/polyphonic-spill.bin")]
        path: String,
        #[arg(long, default_value_t = 8)]
        iters: u32,
    },

    /// Measure what it costs to cross a boundary on this host: native call, shared ring,
    /// syscall, pipe, unix socket, TCP loopback
    Boundary {
        /// Repetitions; the best observation of each rung is kept and the spread reported
        #[arg(long, default_value_t = 5)]
        repeat: usize,
    },

    /// Does the unified advantage scale with volatility, and vanish at zero?
    Volatility {
        /// Accelerator HBM holding KV and weights, e.g. 4GiB. 0 models unified memory, where
        /// every class shares the DRAM pool
        #[arg(long, default_value = "4GiB", value_parser = parse_bytes)]
        hbm: u64,
        #[arg(long, default_value = "8GiB", value_parser = parse_bytes)]
        dram: u64,
        #[arg(long, default_value = "64GiB", value_parser = parse_bytes)]
        nvme: u64,
        #[arg(long, default_value_t = 30_000)]
        ops: u64,
        #[arg(long, default_value_t = 1)]
        seed: u64,
        #[arg(long, default_value_t = 0.125)]
        step: f64,
        /// Add a furthest-next-use eviction baseline at each volatility level
        /// (phase-2.md §1.7, §4.5)
        #[arg(long)]
        clairvoyant: bool,
        #[command(flatten)]
        p3: Correct,
    },

    /// Print the ownership predicate `own.rs` computes (owned-and-observed.md §1's table,
    /// executable) and the dynamic census: how many engine-authority allocations a trace
    /// under this config actually makes. See docs/phase-1.md.
    Ownership {
        #[arg(long, default_value = "4GiB", value_parser = parse_bytes)]
        hbm: u64,
        #[arg(long, default_value = "8GiB", value_parser = parse_bytes)]
        dram: u64,
        #[arg(long, default_value = "64GiB", value_parser = parse_bytes)]
        nvme: u64,
        #[arg(long, default_value_t = 15_000)]
        ops: u64,
        #[arg(long, default_value_t = 1)]
        seed: u64,
        #[arg(long, default_value = "0,1,2,1", value_parser = parse_bands)]
        bands: String,
        #[command(flatten)]
        p3: Correct,
    },

    /// What routing quality costs when residency is a lossy belief: phase-4.md §4.10's sweeps.
    /// Charges no control crossing, so every number is reproducible from the seed
    Belief {
        #[arg(long, default_value_t = 4)]
        nodes: usize,
        #[arg(long, default_value_t = 3)]
        units_per_node: usize,
        #[arg(long, default_value = "16GiB", value_parser = parse_bytes)]
        hbm: u64,
        #[arg(long, default_value = "32GiB", value_parser = parse_bytes)]
        dram: u64,
        #[arg(long, default_value = "64GiB", value_parser = parse_bytes)]
        nvme: u64,
        #[arg(long, default_value_t = 15_000)]
        ops: u64,
        #[arg(long, default_value_t = 1)]
        seed: u64,
        #[arg(long, default_value_t = 250.0)]
        rate: f64,
        #[arg(long, default_value_t = 0.10)]
        fanout: f64,
        /// Sections to run, comma-separated: gate, lag, loss, silence, calibration, gossip,
        /// observables, slo
        #[arg(
            long,
            default_value = "gate,lag,loss,silence,calibration,gossip,observables,slo"
        )]
        sections: String,
    },

    /// What the router can do to memory it does not allocate: phase-5.md §4.10's sweeps.
    /// Charges no control crossing, so every number is reproducible from the seed
    Influence {
        #[arg(long, default_value_t = 4)]
        nodes: usize,
        #[arg(long, default_value_t = 3)]
        units_per_node: usize,
        #[arg(long, default_value = "16GiB", value_parser = parse_bytes)]
        hbm: u64,
        #[arg(long, default_value = "32GiB", value_parser = parse_bytes)]
        dram: u64,
        #[arg(long, default_value = "64GiB", value_parser = parse_bytes)]
        nvme: u64,
        #[arg(long, default_value_t = 15_000)]
        ops: u64,
        #[arg(long, default_value_t = 1)]
        seed: u64,
        #[arg(long, default_value_t = 250.0)]
        rate: f64,
        #[arg(long, default_value_t = 0.10)]
        fanout: f64,
        /// Seeds per directive cell
        #[arg(long, default_value_t = 3, value_parser = clap::value_parser!(u64).range(1..))]
        seeds: u64,
        /// Sections to run, comma-separated: gate, causes, flows, prefill, declared, ceilings,
        /// marks, retain, reuse
        #[arg(
            long,
            default_value = "gate,causes,flows,prefill,declared,ceilings,marks,retain,reuse"
        )]
        sections: String,
    },

    /// The engine's corrections and the fleet's decisions: phase-6.md §4.14's sweeps.
    /// Charges no control crossing, so every number is reproducible from the seed
    Fleet {
        #[arg(long, default_value_t = 4)]
        nodes: usize,
        #[arg(long, default_value_t = 3)]
        units_per_node: usize,
        #[arg(long, default_value = "16GiB", value_parser = parse_bytes)]
        hbm: u64,
        #[arg(long, default_value = "32GiB", value_parser = parse_bytes)]
        dram: u64,
        #[arg(long, default_value = "64GiB", value_parser = parse_bytes)]
        nvme: u64,
        #[arg(long, default_value_t = 15_000)]
        ops: u64,
        #[arg(long, default_value_t = 1)]
        seed: u64,
        #[arg(long, default_value_t = 250.0)]
        rate: f64,
        #[arg(long, default_value_t = 0.10)]
        fanout: f64,
        /// Seeds per cell
        #[arg(long, default_value_t = 3, value_parser = clap::value_parser!(u64).range(1..))]
        seeds: u64,
        /// Sections to run, comma-separated, printed in phase-6.md §4.14's order: gate,
        /// batches, routing, clock, placed, offload, sizes, prefill, keyed, pairing, tenants, duty
        #[arg(
            long,
            default_value = "gate,batches,routing,clock,placed,offload,sizes,prefill,keyed,pairing,tenants,duty"
        )]
        sections: String,
    },

    /// What enforcement does when the engine cannot place a sequence: phase-9.md §4.12's sweeps.
    /// Charges no control crossing, so every number is reproducible from the seed
    Enforce {
        #[arg(long, default_value_t = 4)]
        nodes: usize,
        #[arg(long, default_value_t = 3)]
        units_per_node: usize,
        #[arg(long, default_value = "16GiB", value_parser = parse_bytes)]
        hbm: u64,
        #[arg(long, default_value = "32GiB", value_parser = parse_bytes)]
        dram: u64,
        #[arg(long, default_value = "64GiB", value_parser = parse_bytes)]
        nvme: u64,
        #[arg(long, default_value_t = 15_000)]
        ops: u64,
        #[arg(long, default_value_t = 1)]
        seed: u64,
        #[arg(long, default_value_t = 250.0)]
        rate: f64,
        #[arg(long, default_value_t = 0.10)]
        fanout: f64,
        /// Seeds per cell
        #[arg(long, default_value_t = 3, value_parser = clap::value_parser!(u64).range(1..))]
        seeds: u64,
        /// Fraction of sessions that declare a throughput objective
        #[arg(long, default_value_t = 0.3)]
        throughput: f64,
        /// Sections to run, comma-separated, printed in phase-9.md §4.12's order: gate, engine,
        /// queue, order, cancel, claims, restart, llmd, batch, departures, buffer
        #[arg(
            long,
            default_value = "gate,engine,queue,order,cancel,claims,restart,llmd,batch,departures,buffer"
        )]
        sections: String,
    },

    /// Regional schedulers against the global argmin, with clients in regions: phase-11.md §4.14.
    /// Charges no control crossing, so every number is reproducible from the seed
    Regions {
        /// Regions
        #[arg(long, default_value_t = 3)]
        regions: usize,
        /// Nodes running in a region
        #[arg(long, default_value_t = 4)]
        per_region: usize,
        /// Node slots a region, at least --per-region; the budget section adds two
        #[arg(long, default_value_t = 0)]
        slots: usize,
        #[arg(long, default_value_t = 3)]
        units_per_node: usize,
        /// Accelerator HBM a node
        #[arg(long, default_value = "4GiB", value_parser = parse_bytes)]
        hbm: u64,
        #[arg(long, default_value = "8GiB", value_parser = parse_bytes)]
        dram: u64,
        #[arg(long, default_value = "16GiB", value_parser = parse_bytes)]
        nvme: u64,
        /// Arrival rate a region, requests a second
        #[arg(long, default_value_t = 250.0)]
        rate: f64,
        /// Seconds of arrivals
        #[arg(long, default_value_t = 60.0)]
        seconds: f64,
        #[arg(long, default_value_t = 1)]
        seed: u64,
        /// Seeds per cell
        #[arg(long, default_value_t = 3, value_parser = clap::value_parser!(u64).range(1..))]
        seeds: u64,
        #[arg(long, default_value_t = 0.10)]
        fanout: f64,
        /// One-way latencies between regions: uniform (30 ms), near, far, or azure (three regions)
        #[arg(long, default_value = "uniform")]
        rtt: String,
        /// Class-mix volatility; 0 holds the mix flat so the regions' shares are the only time structure
        #[arg(long, default_value_t = 0.0)]
        volatility: f64,
        /// Model the KV a decode writes, held with its prompt until the decode ends
        #[arg(long)]
        decode_kv: bool,
        /// Sections to run, comma-separated, printed in this order: gate, even, burst, day, table, budget, models, tenants, shards, arithmetic
        #[arg(
            long,
            default_value = "gate,even,burst,day,table,budget,models,tenants,shards,arithmetic"
        )]
        sections: String,
    },

    /// What each durability tier writes and what a crash costs: phase-10.md §4.14's sweeps.
    /// Charges no control crossing, so every number bar the host's durable-append timings is
    /// reproducible from the seed
    Durability {
        #[arg(long, default_value_t = 4)]
        nodes: usize,
        #[arg(long, default_value_t = 3)]
        units_per_node: usize,
        #[arg(long, default_value = "16GiB", value_parser = parse_bytes)]
        hbm: u64,
        #[arg(long, default_value = "32GiB", value_parser = parse_bytes)]
        dram: u64,
        #[arg(long, default_value = "64GiB", value_parser = parse_bytes)]
        nvme: u64,
        #[arg(long, default_value_t = 15_000)]
        ops: u64,
        #[arg(long, default_value_t = 1)]
        seed: u64,
        #[arg(long, default_value_t = 250.0)]
        rate: f64,
        #[arg(long, default_value_t = 0.10)]
        fanout: f64,
        /// Seeds per cell
        #[arg(long, default_value_t = 3, value_parser = clap::value_parser!(u64).range(1..))]
        seeds: u64,
        /// Fraction of sessions that declare a throughput objective
        #[arg(long, default_value_t = 0.3)]
        throughput: f64,
        /// Partitions of the published grant the enforced arm is counted at, comma-separated
        #[arg(long, default_value = "0.75,1.0")]
        scales: String,
        /// Fleet size the count is multiplied to in the fleet section
        #[arg(long, default_value_t = 10_000)]
        fleet_nodes: u64,
        /// Seconds between a node's lease renewals; Kubernetes renews every 10
        #[arg(long, default_value_t = 10.0)]
        lease_renew: f64,
        /// Runs of the durable-append rung; the lowest median and p99 are kept
        #[arg(long, default_value_t = 5, value_parser = clap::value_parser!(u64).range(1..))]
        reps: u64,
        /// Directory the durable-append rung writes to; a tmpfs measures no flush
        #[arg(long, default_value_os_t = std::env::temp_dir())]
        dir: std::path::PathBuf,
        /// Sections to run, comma-separated, printed in this order: gate, count, logged, restart,
        /// routing, estimators, engine, node, crossover, durable, rung, fleet
        #[arg(
            long,
            default_value = "gate,count,logged,restart,routing,estimators,engine,node,crossover,durable,rung,fleet"
        )]
        sections: String,
    },

    /// Agent programs submitted step by step: phase-7.md §4.18's sweeps. Charges no control
    /// crossing, so every number is reproducible from the seed
    Programs {
        /// Seeds per cell
        #[arg(long, default_value_t = 3, value_parser = clap::value_parser!(u64).range(1..))]
        seeds: u64,
        #[arg(long, default_value_t = 1)]
        seed: u64,
        /// Programs per run
        #[arg(long, default_value_t = 900)]
        programs: usize,
        /// Programs arriving a second
        #[arg(long, default_value_t = 12.0)]
        rate: f64,
        /// Tool and idle durations are divided by this: the simulator's call is about a fifth of
        /// a production one
        #[arg(long, default_value_t = 5.0)]
        compress: f64,
        /// Requests of the published trace in the sections that replay it
        #[arg(long, default_value_t = 15_000)]
        ops: u64,
        /// Sections to run, comma-separated, printed in phase-7.md §4.18's order: gate, causal,
        /// hints, speculation, leases, retention, lifecycle, logged, retrieval, roles, classify,
        /// table
        #[arg(
            long,
            default_value = "gate,causal,hints,speculation,leases,retention,lifecycle,logged,retrieval,roles,classify,table"
        )]
        sections: String,
    },

    /// The price of the engine boundary: phase-3.md §4.11's sweeps, per class and at p99.
    /// Charges no control crossing, so every number is reproducible from the seed
    Price {
        #[arg(long, default_value_t = 4)]
        nodes: usize,
        #[arg(long, default_value_t = 3)]
        units_per_node: usize,
        /// Total accelerator HBM across the cluster
        #[arg(long, default_value = "16GiB", value_parser = parse_bytes)]
        hbm: u64,
        /// Total host DDR across the cluster
        #[arg(long, default_value = "32GiB", value_parser = parse_bytes)]
        dram: u64,
        #[arg(long, default_value = "64GiB", value_parser = parse_bytes)]
        nvme: u64,
        #[arg(long, default_value_t = 15_000)]
        ops: u64,
        #[arg(long, default_value_t = 1)]
        seed: u64,
        #[arg(long, default_value = "0,1,2,1", value_parser = parse_bands)]
        bands: String,
        #[arg(long, default_value = "rack")]
        distance: String,
        #[arg(long, default_value_t = 250.0)]
        rate: f64,
        #[arg(long, default_value_t = 0.10)]
        fanout: f64,
        /// Single-node HBM for the eviction-rule sweep (P7), as `residency` runs it
        #[arg(long, default_value = "4GiB", value_parser = parse_bytes)]
        node_hbm: u64,
        #[arg(long, default_value = "8GiB", value_parser = parse_bytes)]
        node_dram: u64,
        /// Requests for the single-node sweep
        #[arg(long, default_value_t = 20_000)]
        node_ops: u64,
    },
}

#[derive(clap::Args, Debug, Clone, Copy)]
struct Correct {
    /// Engine allocates KV in a partition this process sizes; prints beside the ledger's run
    #[arg(long)]
    engine_cache: bool,
    /// Model the KV a decode writes, held with its prompt until the decode ends
    #[arg(long)]
    decode_kv: bool,
    /// Tokens per KV block under --decode-kv; 35 keeps today's mean chain growth
    #[arg(long, default_value_t = polyphonic::work::TOKENS_PER_KV_BLOCK)]
    tokens_per_block: u64,
    /// What the router reserves per request against the partition; quantile, tiered and gate are
    /// router-queue claims, which only `distributed` runs
    #[arg(long, value_enum, default_value_t = AdmitArg::None)]
    admit: AdmitArg,
    /// `max_tokens` as a multiple of the workload's longest output
    #[arg(long, default_value_t = polyphonic::work::MAX_TOKEN_SLACK)]
    max_token_slack: f64,
    /// Partition relative to the default phase-3.md §1.11 derives
    #[arg(long, default_value_t = 1.0)]
    kv_scale: f64,
    /// Partition per node, overriding --kv-scale
    #[arg(long, value_parser = parse_bytes)]
    kv_partition: Option<u64>,
}

#[derive(clap::ValueEnum, Clone, Copy, Debug)]
enum RecoveryArg {
    Replay,
    Periodic,
    None,
}

#[derive(clap::ValueEnum, Clone, Copy, Debug)]
enum LoadArg {
    Path,
    Stream,
    StreamPlusDispatch,
}

#[derive(clap::ValueEnum, Clone, Copy, Debug)]
enum ScoringArg {
    FaceValue,
    Expected,
    Quantile,
    Slo,
}

#[derive(clap::Args, Debug, Clone, Copy)]
struct BeliefArgs {
    /// Router reads engine KV through a lossy event stream instead of the truth (needs
    /// --engine-cache and --rate; phase-4.md)
    #[arg(long)]
    belief: bool,
    /// Fraction of engine step batches dropped in transit
    #[arg(long, default_value_t = 0.0)]
    loss: f64,
    /// How a dropped batch is recovered
    #[arg(long, value_enum, default_value_t = RecoveryArg::Replay)]
    recovery: RecoveryArg,
    /// Seconds each node's stream goes silent, one staggered episode per node
    #[arg(long, default_value_t = 0.0)]
    silence: f64,
    /// How the score prices a believed block
    #[arg(long, value_enum, default_value_t = ScoringArg::FaceValue)]
    scoring: ScoringArg,
    /// Survival a block needs to count as present under --scoring quantile
    #[arg(long, default_value_t = 0.9)]
    quantile: f64,
    /// Where the score reads engine occupancy: the router's own in-flight count, or the stream
    #[arg(long, value_enum, default_value_t = LoadArg::Path)]
    load: LoadArg,
    /// The score sees the observed mean output length, not the exact one
    #[arg(long)]
    observables: bool,
    /// Fraction of sessions that declare a throughput objective instead of an interactive one
    #[arg(long, default_value_t = 0.0)]
    throughput: f64,
    /// An instantaneous, lossless channel: the gate, where the belief must equal the truth
    #[arg(long)]
    exact: bool,
}

impl BeliefArgs {
    const OFF: Self = Self {
        belief: false,
        loss: 0.0,
        recovery: RecoveryArg::Replay,
        silence: 0.0,
        scoring: ScoringArg::FaceValue,
        quantile: 0.9,
        load: LoadArg::Path,
        observables: false,
        throughput: 0.0,
        exact: false,
    };

    fn scoring(self) -> polyphonic::belief::Scoring {
        use polyphonic::belief::Scoring;
        match self.scoring {
            ScoringArg::FaceValue => Scoring::FaceValue,
            ScoringArg::Expected => Scoring::Expected,
            ScoringArg::Quantile => Scoring::Quantile(self.quantile),
            ScoringArg::Slo => Scoring::Slo,
        }
    }

    fn conditions(
        self,
        nodes: usize,
        lag_ns: u64,
        span_ns: u64,
        seed: u64,
    ) -> polyphonic::belief::Conditions {
        use polyphonic::belief::{Conditions, Episode, LoadSource, Recovery};
        if self.exact {
            return Conditions::exact();
        }
        let duration = (self.silence * 1e9) as u64;
        let episodes = if duration == 0 {
            Vec::new()
        } else {
            (0..nodes)
                .map(|node| {
                    let from_ns = span_ns * (node as u64 + 1) / (nodes as u64 + 1);
                    Episode {
                        node,
                        from_ns,
                        until_ns: from_ns + duration,
                    }
                })
                .collect()
        };
        Conditions {
            cadence: true,
            lag_ns,
            loss: self.loss,
            recovery: match self.recovery {
                RecoveryArg::Replay => Recovery::Replay,
                RecoveryArg::Periodic => Recovery::Periodic,
                RecoveryArg::None => Recovery::None,
            },
            period_ns: 1_000_000_000,
            episodes,
            seed,
            load: match self.load {
                LoadArg::Path => LoadSource::Path,
                LoadArg::Stream => LoadSource::Stream,
                LoadArg::StreamPlusDispatch => LoadSource::StreamPlusDispatch,
            },
        }
    }
}

#[derive(clap::ValueEnum, Clone, Copy, Debug)]
enum EmitArg {
    Declared,
    Retain,
    EvictFirst,
    Oracle,
}

#[derive(clap::ValueEnum, Clone, Copy, Debug)]
enum MarksArg {
    Acked,
    Trusted,
}

#[derive(clap::ValueEnum, Clone, Copy, Debug)]
enum TargetArg {
    Argmin,
    Deepest,
}

#[derive(clap::ValueEnum, Clone, Copy, Debug)]
enum HintGradeArg {
    Declared,
    Template,
    Learned,
}

#[derive(clap::ValueEnum, Clone, Copy, Debug)]
enum HalfArg {
    Both,
    Retain,
    Prewarm,
    Off,
}

impl HalfArg {
    fn half(self) -> Half {
        match self {
            Self::Both => Half::Both,
            Self::Retain => Half::Retain,
            Self::Prewarm => Half::Prewarm,
            Self::Off => Half::Off,
        }
    }
}

#[derive(clap::Args, Debug, Clone, Copy)]
#[allow(clippy::struct_excessive_bools)]
struct InfluenceArgs {
    /// Attach retention directives to dispatched requests (needs --engine-cache and --rate)
    #[arg(long)]
    directives: bool,
    /// Who decides the marks; oracle reads the trace and is a ceiling, never an arm
    #[arg(long, value_enum, default_value_t = EmitArg::Declared)]
    emit: EmitArg,
    /// The oracle's horizon in seconds
    #[arg(long, default_value_t = 5.0)]
    horizon: f64,
    /// The engine drops every directive
    #[arg(long)]
    ignores: bool,
    /// Whether the router believes only marks the stream acknowledges, or its own
    #[arg(long, value_enum, default_value_t = MarksArg::Acked)]
    marks: MarksArg,
    /// Prefill a declared downstream's prompt when its hint arrives
    #[arg(long)]
    prefill_ahead: bool,
    /// Where the prefill goes: the downstream's scored argmin, or the deepest believed prefix
    #[arg(long, value_enum, default_value_t = TargetArg::Argmin)]
    prefill_target: TargetArg,
    /// Engines evict furthest-next-use: a ceiling, never an arm
    #[arg(long)]
    clairvoyant_kv: bool,
    /// Track reuse by origin; only `influence` reports it
    #[arg(long)]
    reuse: bool,
    /// What a prefill-ahead is told: the declared downstream, its static template only, or a
    /// template learned per function
    #[arg(long, value_enum, default_value_t = HintGradeArg::Declared)]
    hint_grade: HintGradeArg,
    /// A learned template is sent only where the function's observed share of calls that flow is
    /// at least this
    #[arg(long, default_value_t = 0.0)]
    learn_gate: f64,
}

impl InfluenceArgs {
    const OFF: Self = Self {
        directives: false,
        emit: EmitArg::Declared,
        horizon: 5.0,
        ignores: false,
        marks: MarksArg::Acked,
        prefill_ahead: false,
        prefill_target: TargetArg::Argmin,
        clairvoyant_kv: false,
        reuse: false,
        hint_grade: HintGradeArg::Declared,
        learn_gate: 0.0,
    };

    fn directives(self) -> Option<polyphonic::machine::Directives> {
        use polyphonic::belief::Marks;
        use polyphonic::machine::{Directives, Emit};
        self.directives.then_some(Directives {
            emit: match self.emit {
                EmitArg::Declared => Emit::Declared {
                    retain: true,
                    evict_first: true,
                },
                EmitArg::Retain => Emit::Declared {
                    retain: true,
                    evict_first: false,
                },
                EmitArg::EvictFirst => Emit::Declared {
                    retain: false,
                    evict_first: true,
                },
                EmitArg::Oracle => Emit::Oracle {
                    horizon_ns: (self.horizon * 1e9) as u64,
                },
            },
            ignores: self.ignores,
            marks: match self.marks {
                MarksArg::Acked => Marks::Acked,
                MarksArg::Trusted => Marks::Trusted,
            },
        })
    }

    fn oracle(self) -> bool {
        self.directives && matches!(self.emit, EmitArg::Oracle)
    }

    fn needs_trace(self) -> bool {
        self.oracle() || self.clairvoyant_kv
    }
}

#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
enum ModelBatchesArg {
    Off,
    Blind,
    Priced,
}

#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
enum PrefillArg {
    Off,
    Blind,
    Priced,
}

#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
enum PlannerArg {
    None,
    Once,
    Follow,
    Eager,
    Oracle,
}

type TraceKey = String;

#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
enum PairingArg {
    Off,
    List,
    Independent,
    Joint,
}

#[derive(clap::Args, Debug, Clone, Copy)]
#[allow(clippy::struct_excessive_bools)]
struct FleetArgs {
    /// Each model on a node decodes in a batch of its own and the batches take turns; blind leaves
    /// the score unaware of it (needs --rate)
    #[arg(long, value_enum, default_value_t = ModelBatchesArg::Off)]
    model_batches: ModelBatchesArg,
    /// Prefill work takes engine time: it stretches the decodes admitted while it is inside the
    /// window; blind leaves the score unaware of it (needs --rate)
    #[arg(long, value_enum, default_value_t = PrefillArg::Off)]
    prefill_time: PrefillArg,
    /// The trailing window prefill load is measured over, in seconds
    #[arg(long, default_value_t = 1.0)]
    prefill_window: f64,
    /// Prefill each decode step carries for free, in milliseconds
    #[arg(long, default_value_t = 0.0)]
    prefill_free: f64,
    /// A fan-out agent on another model than its parent's keys its copy of the parent's context
    /// by its own model
    #[arg(long)]
    model_keyed: bool,
    /// Every request names the first model: the published engine's exact case
    #[arg(long)]
    one_model: bool,
    /// Weights leave the ledger: a replica is one model on one node, requests are routed to the
    /// replicas that serve their model, and the KV partition is what the weights leave (needs
    /// --engine-cache, --model-keyed, split memory and a positive --rate)
    #[arg(long)]
    fleet: bool,
    /// Replicas per model under --fleet, as four comma-separated counts; default an even split
    #[arg(long, value_parser = parse_counts)]
    replicas: Option<[u8; 4]>,
    /// Rotate the initial placement by this many nodes: the same replicas on other nodes, which a
    /// hash router sees as a different draw
    #[arg(long, default_value_t = 0)]
    rotate: usize,
    /// Each model's weights under --fleet in GiB, four comma-separated; only the step's base scales
    /// with them, and the partition is what they leave
    #[arg(long, value_parser = parse_sizes)]
    sizes: Option<[f64; 4]>,
    /// Hold the KV partition under --fleet at this size instead of what the weights leave
    #[arg(long, value_parser = parse_bytes)]
    fleet_partition: Option<u64>,
    /// The busiest model's share of session and flow demand, then the rest, as four comma-separated
    /// shares; the hot model rotates one place each phase and the class mix is held flat
    #[arg(long, value_parser = parse_shares)]
    model_mix: Option<[f64; 4]>,
    /// Extra requests per request with a long prompt nothing holds and a short output, arriving at
    /// the instant of the request before them
    #[arg(long, default_value_t = 0.0)]
    fresh: f64,
    /// Who moves replicas under --fleet: nobody, one move after the first interval, a move once
    /// the loss suffered pays for it, a move whenever the best allocation changes, or the
    /// generator's own phases as a ceiling that reads the trace
    #[arg(long, value_enum, default_value_t = PlannerArg::None)]
    planner: PlannerArg,
    /// The planner's interval, in seconds
    #[arg(long, default_value_t = 5.0)]
    interval: f64,
    /// Seconds the oracle's moves are delayed by
    #[arg(long, default_value_t = 0.0)]
    late: f64,
    /// The first n replicas under --fleet only prefill, each a first-come queue, and the rest only
    /// decode
    #[arg(long, default_value_t = 0)]
    prefillers: usize,
    /// How a request's prefill is paired with a prefiller: a list takes every prefill over
    /// --pair-over in turn, independent picks the prefiller with the shortest queue for those over
    /// it, joint weighs the pair against prefilling at the decoder (needs --prefillers and
    /// --prefill-time priced)
    #[arg(long, value_enum, default_value_t = PairingArg::Off)]
    pairing: PairingArg,
    /// A prefiller may fetch the prefix of a prompt from the decoder that holds it instead of
    /// starting over
    #[arg(long)]
    prefill_fetch: bool,
    /// The prefill work, in milliseconds, at or under which a list or independent pairing leaves a
    /// request at its decoder
    #[arg(long, default_value_t = 0.0)]
    pair_over: f64,
    /// One tenant sends fresh 64-block prompts between 40% and 60% of the run, this many extra
    /// requests per request
    #[arg(long, default_value_t = 0.0)]
    neighbour: f64,
    /// The share of the run the neighbour bursts for, centred on its middle
    #[arg(long, default_value_t = 0.2)]
    neighbour_duty: f64,
    /// A prefix per model under every tenant's own
    #[arg(long)]
    shared_prefix: bool,
    /// Record which tenant first touched each KV block, cross-tenant reads, hits by origin and
    /// tenant, and evictions by owner
    #[arg(long)]
    tenants: bool,
    /// Replicas only the neighbour may use, the rest serving everyone else
    #[arg(long, default_value_t = 0)]
    tenant_set: usize,
    /// The router admits each tenant this many engine-seconds of prefill work a second and
    /// refuses the excess (needs --prefill-time priced)
    #[arg(long, default_value_t = 0.0)]
    tenant_prefill: f64,
    /// The router admits each tenant this many sequences in flight and refuses the rest
    #[arg(long, default_value_t = 0)]
    tenant_slots: usize,
    /// A ceiling, not an arm: the engine keeps this many bytes of each tenant's KV from other
    /// tenants' evictions
    #[arg(long, value_parser = parse_bytes)]
    tenant_floor: Option<u64>,
    /// How long a model takes to start once its bytes are on the node, in seconds
    #[arg(long, default_value_t = 8.0)]
    start: f64,
    /// The connector's host-DDR offload grant per node, overriding the ledger-sized one
    #[arg(long, value_parser = parse_bytes)]
    kv_offload: Option<u64>,
}

impl FleetArgs {
    const OFF: Self = Self {
        model_batches: ModelBatchesArg::Off,
        prefill_time: PrefillArg::Off,
        prefill_window: 1.0,
        prefill_free: 0.0,
        model_keyed: false,
        one_model: false,
        fleet: false,
        replicas: None,
        rotate: 0,
        sizes: None,
        fleet_partition: None,
        model_mix: None,
        fresh: 0.0,
        planner: PlannerArg::None,
        interval: 5.0,
        late: 0.0,
        prefillers: 0,
        pairing: PairingArg::Off,
        pair_over: 0.0,
        prefill_fetch: false,
        neighbour: 0.0,
        neighbour_duty: 0.2,
        shared_prefix: false,
        tenants: false,
        tenant_set: 0,
        tenant_prefill: 0.0,
        tenant_slots: 0,
        tenant_floor: None,
        start: 8.0,
        kv_offload: None,
    };

    fn batching(self) -> (polyphonic::engine::Batching, bool) {
        use polyphonic::engine::Batching;
        match self.model_batches {
            ModelBatchesArg::Off => (Batching::Shared, false),
            ModelBatchesArg::Blind => (Batching::PerModel, false),
            ModelBatchesArg::Priced => (Batching::PerModel, true),
        }
    }

    fn prefill_load(self) -> Option<polyphonic::engine::PrefillLoad> {
        (self.prefill_time != PrefillArg::Off).then_some(polyphonic::engine::PrefillLoad {
            window_ns: (self.prefill_window * 1e9) as u64,
            free_ns: (self.prefill_free * 1e6) as u64,
        })
    }

    fn needs_engine(self) -> bool {
        self.model_batches != ModelBatchesArg::Off
            || self.prefill_time != PrefillArg::Off
            || self.fleet
    }

    fn mix_rows(self) -> Option<polyphonic::work::ModelMix> {
        self.model_mix.map(polyphonic::work::rotating_mix)
    }

    fn workload(self, w: polyphonic::work::Workload) -> polyphonic::work::Workload {
        let w = w
            .with_model_keyed(self.model_keyed)
            .with_one_model(self.one_model)
            .with_fresh(self.fresh)
            .with_neighbour(self.neighbour)
            .with_neighbour_duty(self.neighbour_duty);
        let w = match self.mix_rows() {
            Some(rows) => w.with_model_mix(rows),
            None => w,
        };
        w.with_shared_prefix(self.shared_prefix)
    }

    fn neighbour_window(self) -> (f64, f64) {
        polyphonic::work::neighbour_window(self.neighbour_duty)
    }

    fn tenant_refusal(self, nodes: usize, engine_cache: bool) -> Option<&'static str> {
        if self.tenant_prefill > 0.0 && self.prefill_time != PrefillArg::Priced {
            return Some("--tenant-prefill needs --prefill-time priced");
        }
        if self.neighbour_duty <= 0.0 || self.neighbour_duty > 1.0 {
            return Some("--neighbour-duty is a share of the run, above 0 and at most 1");
        }
        if self.tenant_set >= nodes {
            return Some("--tenant-set must leave a replica for everyone else");
        }
        if self.tenant_floor.is_some() && !engine_cache {
            return Some("--tenant-floor needs --engine-cache");
        }
        None
    }

    fn trace_only(self) -> Self {
        Self {
            model_keyed: self.model_keyed,
            one_model: self.one_model,
            model_mix: self.model_mix,
            fresh: self.fresh,
            neighbour: self.neighbour,
            neighbour_duty: self.neighbour_duty,
            shared_prefix: self.shared_prefix,
            ..Self::OFF
        }
    }

    fn trace_key(self) -> TraceKey {
        format!("{:?}", self.trace_only())
    }

    fn volatility(self) -> f64 {
        if self.model_mix.is_some() { 0.0 } else { 1.0 }
    }

    fn placement(self, nodes: usize) -> Vec<Option<u8>> {
        let counts = self.replicas.unwrap_or_else(|| {
            if self.one_model {
                [u8::try_from(nodes).unwrap_or(u8::MAX), 0, 0, 0]
            } else {
                std::array::from_fn(|m| {
                    u8::try_from(nodes / 4 + usize::from(m < nodes % 4)).unwrap_or(u8::MAX)
                })
            }
        });
        let mut placement = polyphonic::fleet::layout(&counts.map(usize::from), nodes);
        if nodes > 0 {
            placement.rotate_left(self.rotate % nodes);
        }
        placement
    }

    fn fleet_of(
        self,
        nodes: usize,
        initial: Option<&[Option<u8>]>,
    ) -> Option<polyphonic::fleet::Fleet> {
        self.fleet.then(|| {
            let mut fleet = polyphonic::fleet::Fleet::new(
                self.catalogue(),
                &initial.map_or_else(|| self.placement(nodes), <[Option<u8>]>::to_vec),
            );
            if self.pairing != PairingArg::Off {
                let held: Vec<usize> = (0..nodes)
                    .filter(|&d| fleet.model_on(d).is_some())
                    .collect();
                for (i, &d) in held.iter().enumerate() {
                    let role = if i < self.prefillers {
                        polyphonic::fleet::Role::Prefill
                    } else {
                        polyphonic::fleet::Role::Decode
                    };
                    fleet.set_role(d, role);
                }
            }
            fleet
        })
    }

    fn pairing_rule(self) -> polyphonic::machine::Pairing {
        use polyphonic::machine::Pairing;
        match self.pairing {
            PairingArg::Off => Pairing::Off,
            PairingArg::List => Pairing::List,
            PairingArg::Independent => Pairing::Independent,
            PairingArg::Joint => Pairing::Joint,
        }
    }

    fn catalogue(self) -> polyphonic::fleet::Catalogue {
        let published = polyphonic::fleet::Catalogue::published((self.start * 1e9) as u64);
        match self.sizes {
            Some(gib) => {
                published.with_sizes(gib.map(|g| (g * polyphonic::fleet::GIB as f64) as u64))
            }
            None => published,
        }
    }

    fn planning(self, mach: &mut polyphonic::machine::Machine) {
        use polyphonic::fleet::PlannerKind;
        let interval_ns = (self.interval * 1e9) as u64;
        match self.planner {
            PlannerArg::Once => mach.set_planner(PlannerKind::Once, interval_ns),
            PlannerArg::Follow => mach.set_planner(PlannerKind::Follow, interval_ns),
            PlannerArg::Eager => mach.set_planner(PlannerKind::Eager, interval_ns),
            PlannerArg::None | PlannerArg::Oracle => {}
        }
    }

    fn apply(self, mach: &mut polyphonic::machine::Machine) {
        let (batching, priced) = self.batching();
        mach.set_model_batches(batching, priced);
        mach.set_prefill_time(self.prefill_load(), self.prefill_time == PrefillArg::Priced);
        mach.set_tenant_set(self.tenant_set);
        mach.set_tenant_quota(self.tenant_prefill * 1e9, self.tenant_slots);
        mach.set_tenant_floor(self.tenant_floor.unwrap_or(0));
        mach.set_pairing(
            self.pairing_rule(),
            (self.pair_over * 1e6) as u64,
            self.prefill_fetch,
        );
    }
}

#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
enum EngineWaitArg {
    Off,
    Fifo,
    FirstFit,
    Priority,
}

#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
enum ObserveArg {
    Dispatch,
    Completion,
}

#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
enum QueueArg {
    Off,
    Fifo,
    Slo,
    Plas,
}

#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
enum CancelArg {
    Off,
    Continue,
    Drop,
}

#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
enum VictimArg {
    Recent,
    Remaining,
    Attained,
}

#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
enum CancelAtArg {
    Both,
    Router,
}

#[derive(clap::Args, Debug, Clone, Copy)]
#[allow(clippy::struct_excessive_bools)]
struct EnforceArgs {
    /// Engine arm: a sequence the partition cannot hold waits at its node until its blocks fit,
    /// in arrival order, first fit, or by declared class, instead of running with no memory
    /// (needs --engine-cache, --decode-kv, a positive --rate, no --pairing and no --regret)
    #[arg(long, value_enum, default_value_t = EngineWaitArg::Off)]
    engine_wait: EngineWaitArg,
    /// Instrument: for each sequence today's engine runs with no memory, read the wait it would
    /// have had from the engine's own release schedule; changes nothing
    #[arg(long)]
    probe_engine: bool,
    /// Router arm: a request no node's check admits waits at the router, served in arrival order,
    /// declared class first, or by the attained service of its program, and is placed among the
    /// nodes that admit it when it leaves (needs what --engine-wait needs)
    #[arg(long, value_enum, default_value_t = QueueArg::Off)]
    queue: QueueArg,
    /// The quantile of a class's observed output length that --admit quantile and --admit tiered
    /// reserve a request against
    #[arg(long, default_value_t = 0.9)]
    claim_quantile: f64,
    /// Under --admit quantile, reserve against the pooled distribution instead of the request's
    /// own class's
    #[arg(long)]
    pooled: bool,
    /// Under --admit gate, a node admits a request while its pinned KV is under this share of its
    /// partition
    #[arg(long, default_value_t = 0.9)]
    gate: f64,
    /// Router and engine arm: abort a lower-class sequence in flight to admit a higher-class
    /// request, and resubmit it as a continuation of what it decoded or drop it and send it again
    /// whole (needs --queue, --engine-cache, --decode-kv, a positive --rate, no --belief, no
    /// --regret and no --pairing)
    #[arg(long, value_enum, default_value_t = CancelArg::Off)]
    cancel: CancelArg,
    /// Which sequence a cancel takes first: the most recently dispatched, the one with the most
    /// decode left, or, as a comparison arm, the one whose program has attained the most service
    #[arg(long, value_enum, default_value_t = VictimArg::Recent)]
    victim: VictimArg,
    /// Where a cancel may be triggered: at the router when the head of its queue fits nowhere, and
    /// also at an engine where a higher-class request waits for its blocks
    #[arg(long, value_enum, default_value_t = CancelAtArg::Both)]
    cancel_at: CancelAtArg,
    /// The share of decodes whose client leaves part-way through, chosen from a stream of its own;
    /// the router aborts the sequence at the next arrival unless --leak (needs what --cancel
    /// needs, bar --queue)
    #[arg(long, default_value_t = 0.0)]
    disconnect: f64,
    /// A departed client's sequence runs to its end instead of being aborted
    #[arg(long)]
    leak: bool,
    /// Instrument: the tokens every decode in flight has emitted, per node, sampled at each
    /// arrival; changes nothing
    #[arg(long)]
    stream_buffer: bool,
    /// Instrument: count the KV events each engine emits, by type and tier, beside the owned
    /// changes the machine always counts; changes nothing
    #[arg(long)]
    count_writes: bool,
    /// Instrument: register every decode as a flight, a fan-out's agents as one gang, so that a
    /// fault can reach it; changes nothing
    #[arg(long)]
    track_flights: bool,
    /// When the router records an output length: as a decode is dispatched, or when it ends
    #[arg(long, value_enum, default_value_t = ObserveArg::Dispatch)]
    observe: ObserveArg,
    /// Node agents' arm: a node refuses a dispatch its own ledger of claims does not admit, and
    /// the request returns to the router's queue, so a router whose ledger is blind cannot
    /// over-admit a node (acts only after a scheduler restart that leaves the ledger blind)
    #[arg(long)]
    node_check: bool,
    /// Seconds between snapshots of the router's estimators, written to the logged tier; 0 takes
    /// none
    #[arg(long, default_value_t = 0.0)]
    snapshot_estimators: f64,
    /// Copy a durable cell off its node when it is marked, so that losing the node loses none of
    /// them; charged its bytes
    #[arg(long)]
    copy_durable: bool,
    /// After each request, with this chance from a stream of its own, a throughput request with an
    /// unshared 8-block prompt and 200-600 output tokens
    #[arg(long, default_value_t = 0.0)]
    batch: f64,
}

impl EnforceArgs {
    const OFF: Self = Self {
        engine_wait: EngineWaitArg::Off,
        probe_engine: false,
        queue: QueueArg::Off,
        claim_quantile: 0.9,
        pooled: false,
        gate: 0.9,
        cancel: CancelArg::Off,
        victim: VictimArg::Recent,
        cancel_at: CancelAtArg::Both,
        disconnect: 0.0,
        leak: false,
        stream_buffer: false,
        count_writes: false,
        track_flights: false,
        observe: ObserveArg::Dispatch,
        node_check: false,
        snapshot_estimators: 0.0,
        copy_durable: false,
        batch: 0.0,
    };

    fn refusal(
        self,
        p3: Correct,
        rate: f64,
        regret: bool,
        fleet: FleetArgs,
        belief: BeliefArgs,
    ) -> Option<&'static str> {
        if !(0.0..=1.0).contains(&self.disconnect) || !(0.0..=1.0).contains(&self.batch) {
            return Some("--disconnect and --batch are shares between 0 and 1");
        }
        if self.leak && self.disconnect <= 0.0 {
            return Some("--leak needs --disconnect");
        }
        if !(self.claim_quantile > 0.0 && self.claim_quantile <= 1.0) {
            return Some("--claim-quantile is a share above 0 and at most 1");
        }
        if !(self.gate > 0.0 && self.gate <= 1.0) {
            return Some("--gate is a share of the partition, above 0 and at most 1");
        }
        if self.pooled && p3.admit != AdmitArg::Quantile {
            return Some("--pooled needs --admit quantile");
        }
        if self.cancel != CancelArg::Off
            && self.victim == VictimArg::Attained
            && self.queue != QueueArg::Plas
        {
            return Some("--victim attained needs --queue plas, which accrues attained service");
        }
        if self.is_off(p3.admit) {
            return None;
        }
        if !(p3.engine_cache && p3.decode_kv && rate > 0.0) {
            return Some(
                "--engine-wait, --probe-engine, --queue and --admit quantile, tiered or gate need \
                 --engine-cache, --decode-kv and a positive --rate",
            );
        }
        if regret || fleet.pairing != PairingArg::Off {
            return Some(
                "--engine-wait, --probe-engine, --queue and --admit quantile, tiered or gate need \
                 no --regret and no --pairing",
            );
        }
        if self.cancel != CancelArg::Off && (self.queue == QueueArg::Off || belief.belief) {
            return Some("--cancel needs --queue to resubmit into and no --belief");
        }
        if (self.disconnect > 0.0 || self.stream_buffer) && belief.belief {
            return Some("--disconnect and --stream-buffer need no --belief");
        }
        None
    }

    fn is_off(self, admit: AdmitArg) -> bool {
        self.engine_wait == EngineWaitArg::Off
            && !self.probe_engine
            && self.queue == QueueArg::Off
            && self.cancel == CancelArg::Off
            && self.disconnect <= 0.0
            && !self.stream_buffer
            && !admit.is_claim()
    }

    fn apply(self, mach: &mut polyphonic::machine::Machine, admit: AdmitArg) {
        use polyphonic::machine::{
            CancelMode, Claim, Departures, EngineWait, Queue, Triggers, Victim,
        };
        mach.set_claim(match admit {
            AdmitArg::Quantile => Claim::Quantile {
                q: self.claim_quantile,
                pooled: self.pooled,
            },
            AdmitArg::Tiered => Claim::Tiered {
                q: self.claim_quantile,
            },
            AdmitArg::Gate => Claim::Gate(self.gate),
            AdmitArg::Bound | AdmitArg::Perfect | AdmitArg::None => Claim::Static,
        });
        mach.set_queue(match self.queue {
            QueueArg::Off => Queue::Off,
            QueueArg::Fifo => Queue::Fifo,
            QueueArg::Slo => Queue::Slo,
            QueueArg::Plas => Queue::Plas,
        });
        mach.set_cancel(
            match self.cancel {
                CancelArg::Off => CancelMode::Off,
                CancelArg::Continue => CancelMode::Continue,
                CancelArg::Drop => CancelMode::Drop,
            },
            match self.victim {
                VictimArg::Recent => Victim::Recent,
                VictimArg::Remaining => Victim::Remaining,
                VictimArg::Attained => Victim::Attained,
            },
        );
        mach.set_cancel_triggers(match self.cancel_at {
            CancelAtArg::Both => Triggers::Both,
            CancelAtArg::Router => Triggers::Router,
        });
        mach.set_departures((self.disconnect > 0.0).then_some(Departures {
            share: self.disconnect,
            leak: self.leak,
        }));
        mach.set_track_stream(self.stream_buffer);
        mach.set_count_events(self.count_writes);
        mach.set_armed(self.track_flights);
        mach.set_node_check(self.node_check);
        mach.set_copy_durable(self.copy_durable);
        mach.set_snapshot_every(
            (self.snapshot_estimators > 0.0).then_some((self.snapshot_estimators * 1e9) as u64),
        );
        mach.set_observe(match self.observe {
            ObserveArg::Dispatch => polyphonic::fault::Observe::Dispatch,
            ObserveArg::Completion => polyphonic::fault::Observe::Completion,
        });
        mach.set_probe_engine(self.probe_engine);
        mach.set_engine_wait(match self.engine_wait {
            EngineWaitArg::Off => EngineWait::Off,
            EngineWaitArg::Fifo => EngineWait::Fifo,
            EngineWaitArg::FirstFit => EngineWait::FirstFit,
            EngineWaitArg::Priority => EngineWait::Priority,
        });
    }
}

#[derive(clap::Args, Debug, Clone, Copy)]
struct AheadArgs {
    /// Engine arm: prefill a declared downstream's missing blocks when its hint arrives
    #[arg(long)]
    prefill_ahead: bool,
    /// Engine arm: hold a declared downstream's resident blocks until it arrives
    #[arg(long)]
    hold: bool,
    /// Which half of announce reaches KV on the ledger
    #[arg(long, value_enum, default_value_t = HalfArg::Both)]
    announce_kv: HalfArg,
    /// Which half of announce reaches host state
    #[arg(long, value_enum, default_value_t = HalfArg::Both)]
    announce_host: HalfArg,
    /// Withdraw a bump when its hint's lead has passed
    #[arg(long)]
    retain: bool,
    /// Chance that a function request also issues a hint for a flow that never comes
    #[arg(long, default_value_t = 0.0)]
    false_hints: f64,
}

impl AheadArgs {
    fn ledger(self, fix: Correction) -> Correction {
        Correction {
            announce: AnnounceMix {
                kv: self.announce_kv.half(),
                host: self.announce_host.half(),
            },
            deadline: self.retain,
            false_hints: self.false_hints,
            ..fix
        }
    }

    fn engine(self, fix: Correction) -> Correction {
        Correction {
            ahead: Ahead {
                prefill: self.prefill_ahead,
                hold: self.hold,
            },
            ..self.ledger(fix)
        }
    }
}

#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
enum AdmitArg {
    Bound,
    Perfect,
    None,
    Quantile,
    Tiered,
    Gate,
}

impl AdmitArg {
    fn is_claim(self) -> bool {
        matches!(self, Self::Quantile | Self::Tiered | Self::Gate)
    }
}

#[derive(clap::Args, Debug, Clone, Copy)]
struct ClusterBits {
    /// Price a cross-node `NVMe` pool of this size, LRU, in the acquire argmin (phase-3.md §4.10)
    #[arg(long, value_parser = parse_bytes)]
    shared_l2: Option<u64>,
    /// Zero the displacement term: phase-3.md §4.8's control arm
    #[arg(long)]
    no_displacement: bool,
}

impl Correct {
    fn reserve(self) -> Reserve {
        match self.admit {
            AdmitArg::Bound => Reserve::Bound,
            AdmitArg::Perfect => Reserve::Perfect,
            AdmitArg::None | AdmitArg::Quantile | AdmitArg::Tiered | AdmitArg::Gate => {
                Reserve::Prompt
            }
        }
    }

    fn base(self) -> Correction {
        Correction {
            engine: None,
            decode_kv: self.decode_kv.then_some(self.tokens_per_block),
            reserve: self.reserve(),
            max_token_slack: self.max_token_slack,
            ..Correction::default()
        }
    }

    fn engine(self, clairvoyant: bool) -> Correction {
        Correction {
            engine: Some(EngineArm {
                scale: self.kv_scale,
                partition: self.kv_partition,
                clairvoyant,
            }),
            ..self.base()
        }
    }

    fn workload(self, w: polyphonic::work::Workload) -> polyphonic::work::Workload {
        let w = w.with_max_token_slack(self.max_token_slack);
        if self.decode_kv {
            w.with_decode_kv(self.tokens_per_block)
        } else {
            w
        }
    }

    fn setup(self, mach: &mut polyphonic::machine::Machine, bits: ClusterBits) {
        mach.set_admission(self.reserve(), self.tokens_per_block);
        mach.set_hold_decodes(self.decode_kv);
        mach.set_shared_l2(bits.shared_l2);
        mach.set_displacement(!bits.no_displacement);
    }

    fn grant(self, mem: &NodeMemory, off: [u64; 3]) -> Option<EngineKv> {
        mem.can_decode.then(|| {
            polyphonic::arms::grant_for(
                mem,
                off,
                EngineArm {
                    scale: self.kv_scale,
                    partition: self.kv_partition,
                    clairvoyant: false,
                },
            )
        })
    }

    fn engine_memory(self, mem: NodeMemory, off: [u64; 3]) -> NodeMemory {
        NodeMemory {
            kv: self.grant(&mem, off),
            ..mem
        }
    }

    fn describe(self) -> String {
        let decode = if self.decode_kv {
            format!(
                "decode output modelled at {} tokens/block",
                self.tokens_per_block
            )
        } else {
            "decode output not modelled".to_string()
        };
        format!(
            "router reserves {}, max_tokens {:.0}x, {decode}",
            self.admission(),
            self.max_token_slack
        )
    }

    fn admission(self) -> &'static str {
        match self.admit {
            AdmitArg::Quantile => "a quantile claim of observed lengths",
            AdmitArg::Tiered => "the tiered claim",
            AdmitArg::Gate => "under a utilisation gate",
            AdmitArg::Bound | AdmitArg::Perfect | AdmitArg::None => self.reserve().label(),
        }
    }
}

fn grant_label(g: Option<EngineKv>) -> String {
    g.map_or_else(
        || "no engine".to_string(),
        |g| {
            format!(
                "partition {:.2} GiB, offload {:.2} GiB, spill {:.2} GiB",
                gib(g.partition),
                gib(g.offload),
                gib(g.spill)
            )
        },
    )
}

fn parse_four<T>(s: &str, what: &str) -> Result<[T; 4], String>
where
    T: std::str::FromStr + Copy + Default,
    T::Err: std::fmt::Display,
{
    let parts: Vec<&str> = s.split(',').collect();
    if parts.len() != 4 {
        return Err(format!(
            "expected 4 comma-separated {what}, got {}",
            parts.len()
        ));
    }
    let mut out = [T::default(); 4];
    for (slot, p) in out.iter_mut().zip(parts) {
        *slot = p.trim().parse::<T>().map_err(|e| e.to_string())?;
    }
    Ok(out)
}

fn parse_shares(s: &str) -> Result<[f64; 4], String> {
    let out: [f64; 4] = parse_four(s, "shares")?;
    let sum: f64 = out.iter().sum();
    if (sum - 1.0).abs() > 1e-6 {
        return Err(format!("shares sum to {sum}, not 1"));
    }
    Ok(out)
}

fn parse_sizes(s: &str) -> Result<[f64; 4], String> {
    let out: [f64; 4] = parse_four(s, "sizes")?;
    if out.iter().any(|&g| g.is_nan() || g <= 0.0) {
        return Err("every size must be positive".to_string());
    }
    Ok(out)
}

fn parse_counts(s: &str) -> Result<[u8; 4], String> {
    parse_four(s, "counts")
}

fn parse_bands(s: &str) -> Result<String, String> {
    let n = s.split(',').count();
    if n != BlobKind::N {
        return Err(format!(
            "expected {} comma-separated bands, got {n}",
            BlobKind::N
        ));
    }
    for p in s.split(',') {
        p.trim().parse::<u8>().map_err(|e| e.to_string())?;
    }
    Ok(s.to_string())
}

fn bands_of(s: &str) -> [u8; BlobKind::N] {
    let mut out = [0u8; BlobKind::N];
    for (i, p) in s.split(',').enumerate() {
        out[i] = p.trim().parse().unwrap_or(0);
    }
    out
}

fn parse_bytes(s: &str) -> Result<u64, String> {
    let t = s.trim();
    let (num, mult) = if let Some(p) = t.strip_suffix("GiB") {
        (p, 1u64 << 30)
    } else if let Some(p) = t.strip_suffix("MiB") {
        (p, 1u64 << 20)
    } else if let Some(p) = t.strip_suffix("KiB") {
        (p, 1u64 << 10)
    } else {
        (t, 1)
    };
    num.trim()
        .parse::<f64>()
        .map(|v| (v * mult as f64) as u64)
        .map_err(|e| e.to_string())
}

fn prefer(a: &Report, b: &Report, bands: [u8; BlobKind::N]) -> bool {
    let (ga, gb) = (a.goodput(), b.goodput());
    if (ga - gb).abs() > 0.02 {
        return ga > gb;
    }
    let max_band = bands.iter().copied().max().unwrap_or(0);
    for band in 0..=max_band {
        let stall = |r: &Report| -> f64 {
            let (mut ns, mut ops) = (0u64, 0u64);
            for (k, &kb) in bands.iter().enumerate() {
                if kb == band {
                    ns += r.kind_ns[k];
                    ops += r.kind_ops[k];
                }
            }
            mean_ms(ns, ops)
        };
        let good = |r: &Report| -> f64 {
            let (mut s, mut n) = (0u64, 0u64);
            for (k, &kb) in bands.iter().enumerate() {
                if kb == band {
                    s += r.served[k];
                    n += r.served[k] + r.refused[k] + r.refused_by_router[k];
                }
            }
            if n == 0 { 1.0 } else { s as f64 / n as f64 }
        };
        let (ga, gb) = (good(a), good(b));
        if (ga - gb).abs() > 0.02 {
            return ga > gb;
        }
        let (sa, sb) = (stall(a), stall(b));
        if sa < sb * 0.98 {
            return true;
        }
        if sb < sa * 0.98 {
            return false;
        }
    }
    false
}

fn best_split(t: Trial, hard: bool, step: f64) -> (Report, [f64; BlobKind::N]) {
    let mut best: Option<(Report, [f64; BlobKind::N])> = None;
    let stream = trace(t);
    let n = (1.0 / step).round() as u64;
    for a in 1..n {
        for b in 1..n - a {
            for c in 1..n - a - b {
                for d in 1..n - a - b - c {
                    let split = [a, b, c, d].map(|x| x as f64 / n as f64);
                    let r = run_on("", t, Budget::Split { split, hard }, &stream);
                    let better = best.as_ref().is_none_or(|(x, _)| prefer(&r, x, t.bands));
                    if better {
                        best = Some((r, split));
                    }
                }
            }
        }
    }
    best.expect("sweep produced no candidate splits")
}

const PHASE_NAME: [&str; 4] = ["agent-heavy", "faas-burst", "service-steady", "mixed"];
const CLASS_NAME: [&str; BlobKind::N] = ["inference-kv", "faas", "weights", "service"];

fn memory_label(hbm: u64, dram: u64) -> String {
    if hbm == 0 {
        format!("unified memory: dram={:.1}GiB", gib(dram))
    } else {
        format!("hbm={:.1}GiB ddr={:.1}GiB", gib(hbm), gib(dram))
    }
}

fn node_memory(hbm: u64, ddr: u64, nvme: u64, bands: [u8; BlobKind::N], hard: bool) -> NodeMemory {
    if hbm == 0 {
        let q = Quota::from_split(ddr, [0.10, 0.12, 0.50, 0.26], bands, hard);
        return NodeMemory {
            hbm: 0,
            ddr,
            nvme,
            hbm_quota: Quota::open(0, bands),
            ddr_quota: q,
            can_decode: true,
            kv: None,
        };
    }
    NodeMemory {
        hbm,
        ddr,
        nvme,
        hbm_quota: Quota::from_split(hbm, [0.25, 0.0, 0.50, 0.0], bands, hard),
        ddr_quota: Quota::from_split(ddr, [0.10, 0.15, 0.15, 0.35], bands, hard).offloaded(),
        can_decode: true,
        kv: None,
    }
}

fn ms(ns: u64) -> f64 {
    ns as f64 / 1e6
}

fn gib(b: u64) -> f64 {
    b as f64 / (1u64 << 30) as f64
}

fn calibrate(path: &str, iters: u32) {
    use polyphonic::blob::BlobId;
    use polyphonic::store::Store;

    let sizes: [(&str, usize); 4] = [
        ("kv-block    512KiB", 512 * 1024),
        ("prefill-batch 2MiB", 2 * 1024 * 1024),
        ("snapshot     32MiB", 32 * 1024 * 1024),
        ("weight-shard 512MiB", 512 * 1024 * 1024),
    ];
    let mut store = Store::open(std::path::Path::new(path), 8 << 30).expect("open spill file");
    println!("spill file: {path}  (direct I/O; page cache bypassed)\n");
    println!(
        "{:<22} {:>12} {:>10} {:>12} {:>10} {:>12} {:>10}",
        "object", "fault (us)", "GB/s", "write (us)", "GB/s", "read (us)", "GB/s"
    );
    for (name, len) in sizes {
        let (mut f, mut w, mut r) = (0u64, 0u64, 0u64);
        for i in 0..iters {
            let id = BlobId::leaf(format!("cal:{name}:{i}").as_bytes());
            f += store.materialize(id, len);
            w += store.demote(id);
            r += store.promote(id);
            store.demote(id);
            store.drop_cold(id);
        }
        let n = u64::from(iters);
        let gbs = |ns: u64| (len as f64 * n as f64) / (ns.max(1) as f64);
        println!(
            "{:<22} {:>12.1} {:>10.2} {:>12.1} {:>10.2} {:>12.1} {:>10.2}",
            name,
            f as f64 / n as f64 / 1000.0,
            gbs(f),
            w as f64 / n as f64 / 1000.0,
            gbs(w),
            r as f64 / n as f64 / 1000.0,
            gbs(r)
        );
    }
    let _ = std::fs::remove_file(path);
}

fn arm_row(r: &Report) {
    let served: u64 = r.served.iter().sum();
    println!(
        "{:<32} {:>11.3} {:>9.3} {:>8.1}% {:>9.0}%   {:.2}/{:.2}/{:.2}/{:.2}   {:.1}/{:.1}/{:.1}/{:.1}",
        r.label,
        mean_ms(r.total_ns, served),
        ms(r.p99_ns),
        100.0 * r.goodput(),
        100.0 * r.transfer_ns as f64 / r.total_ns.max(1) as f64,
        r.hit[0],
        r.hit[1],
        r.hit[2],
        r.hit[3],
        gib(r.resident[0]),
        gib(r.resident[1]),
        gib(r.resident[2]),
        gib(r.resident[3]),
    );
}

#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_lines)]
fn placement(
    sockets: usize,
    units_per_socket: usize,
    hbm: u64,
    dram: u64,
    nvme: u64,
    ops: u64,
    seed: u64,
    bands: [u8; BlobKind::N],
    rate: f64,
    drain_at: f64,
    p3: Correct,
    drain_spill: bool,
) {
    use polyphonic::machine::{Machine, Placement};
    use polyphonic::topo::Topology;

    let per_socket = dram / sockets as u64;
    let topo = Topology::synthetic(sockets, units_per_socket, per_socket);
    let memory = node_memory(
        hbm / sockets as u64,
        per_socket,
        nvme / sockets as u64,
        bands,
        false,
    );
    println!(
        "synthetic machine: {sockets} domains x {:.1} GiB, {units_per_socket} units each\n\
         cross-domain link constants are MODELLED (coherent, ~2x per-byte, 120 ns hop)",
        gib(per_socket)
    );
    if drain_at > 0.0 {
        println!(
            "domain 0 drained at {:.0}% through the trace; its state migrates\n",
            drain_at * 100.0
        );
    } else {
        println!();
    }

    let modes = [
        (Placement::Blind, false),
        (Placement::Sticky, false),
        (Placement::Aware, false),
        (Placement::Aware, true),
        (Placement::Scored, false),
        (Placement::Scored, true),
    ];

    let mut tables = vec![(false, drain_spill)];
    if drain_spill {
        tables.insert(0, (false, false));
    }
    if p3.engine_cache {
        tables.push((true, false));
        if drain_spill {
            tables.push((true, true));
        }
    }
    let mut means = Vec::with_capacity(modes.len());
    for (n, &(engine, spill)) in tables.iter().enumerate() {
        if tables.len() > 1 {
            let drained = if drain_at <= 0.0 {
                ""
            } else if spill {
                "; on drain its spill tier is migrated too"
            } else {
                "; on drain its spill tier is left behind"
            };
            println!(
                "{}{}{drained}",
                if n > 0 { "\n" } else { "" },
                if engine {
                    "engine allocates KV (phase-3.md), which dies with a drained node"
                } else {
                    "ledger allocates KV, which migrates with a drained node"
                },
            );
        }
        println!(
            "{:<13} {:>12} {:>14} {:>12} {:>10} {:>12} {:>14}",
            "placement",
            "stall/req",
            "interconnect",
            "local hops",
            "cold",
            "bytes moved",
            "domain spread"
        );
        for (i, &(mode, transfer)) in modes.iter().enumerate() {
            let mem = if engine {
                p3.engine_memory(memory, means[i])
            } else {
                memory
            };
            let mut m = Machine::new(topo.clone(), |_| mem, Policy::Gdsf, mode);
            m.set_state_transfer(transfer);
            m.set_arrival_rate(rate);
            m.set_drain_spill(spill);
            p3.setup(
                &mut m,
                ClusterBits {
                    shared_l2: None,
                    no_displacement: false,
                },
            );
            let mut total = 0u64;
            let mut served = 0u64;
            let drain_op = if drain_at > 0.0 {
                (ops as f64 * drain_at) as u64
            } else {
                u64::MAX
            };
            let workload = p3.workload(polyphonic::work::Workload::new(seed, ops, 1.0));
            for (i, req) in workload.enumerate() {
                if i as u64 == drain_op {
                    m.drain(0);
                }
                let c = m.serve_request(&req);
                if c.pending {
                    continue;
                }
                total += c.total_ns();
                served += 1;
            }
            if n == 0 {
                means.push(m.kv_mean());
            }
            let label = match (mode, transfer) {
                (Placement::Blind, _) => "blind",
                (Placement::Sticky, _) => "sticky",
                (Placement::Aware, false) => "aware",
                (Placement::Aware, true) => "aware+fetch",
                (Placement::Scored, false) => "scored",
                (Placement::Scored, true) => "scored+fetch",
            };
            println!(
                "{label:<13} {:>11.3}ms {:>13.1}s {:>10.2}s {:>11.1}% {:>9.1}% {:>13.2}",
                mean_ms(total, served),
                total.saturating_sub(m.interconnect_ns + m.handoff_ns) as f64 / 1e9,
                m.handoff_ns as f64 / 1e9,
                100.0 * m.split_tasks as f64 / (m.split_tasks + m.joined_tasks).max(1) as f64,
                100.0 * m.cold as f64 / served.max(1) as f64,
                m.domain_spread(),
            );
            if engine {
                println!(
                    "{:<13} {}; migrated {:.2} GiB; router refused {}, engine preempted {}",
                    "",
                    grant_label(mem.kv),
                    gib(m.migrated_bytes),
                    m.refused_by_router.iter().sum::<u64>(),
                    m.preempted.iter().sum::<u64>(),
                );
                debug_assert_eq!(m.kv_orphans(), 0);
            } else if tables.len() > 1 && drain_at > 0.0 {
                println!("{:<13} migrated {:.2} GiB", "", gib(m.migrated_bytes));
            }
        }
    }
}

fn chase_median(bytes: usize, cluster: polyphonic::plat::Cluster, reps: u32) -> f64 {
    let mut v: Vec<f64> = (0..reps.max(1))
        .map(|_| chase_latency(bytes, cluster))
        .collect();
    v.sort_by(f64::total_cmp);
    v[v.len() / 2]
}

fn chase_latency(bytes: usize, cluster: polyphonic::plat::Cluster) -> f64 {
    std::thread::spawn(move || {
        if !polyphonic::plat::pin_cluster(cluster) {
            return f64::NAN;
        }
        let stride = 128 / 8;
        let slots = bytes / 8;
        let steps = slots / stride;
        if steps < 2 {
            return f64::NAN;
        }

        let mut order: Vec<usize> = (0..steps).map(|i| i * stride).collect();
        let mut rng = polyphonic::rng::Rng::new(0x5EED);
        for i in (1..steps).rev() {
            let j = rng.below(i as u64 + 1) as usize;
            order.swap(i, j);
        }
        let mut buf = vec![0usize; slots];
        for w in 0..steps {
            buf[order[w]] = order[(w + 1) % steps];
        }
        let mut p = order[0];
        let warm = steps * 2;
        for _ in 0..warm {
            p = buf[p];
        }
        std::hint::black_box(p);
        let laps = (64 << 20) / bytes.max(1);
        let n = steps * laps.max(4);
        let t = std::time::Instant::now();
        for _ in 0..n {
            p = buf[p];
        }
        let ns = t.elapsed().as_nanos() as f64;
        std::hint::black_box(p);
        ns / n as f64
    })
    .join()
    .unwrap_or(f64::NAN)
}

fn topology(bytes: u64, iters: u32) {
    use polyphonic::plat::Cluster;
    use polyphonic::topo::Topology;

    let t = Topology::discover();
    println!("discovered host graph\n");
    println!(
        "{:<10} {:>6} {:>14} {:>10}",
        "domain", "id", "kind", "capacity"
    );
    for d in &t.domains {
        println!(
            "{:<10} {:>6} {:>14?} {:>9.1}G",
            "memory",
            d.id,
            d.kind,
            gib(d.capacity)
        );
    }
    let mut perf = 0;
    let mut eff = 0;
    for u in &t.units {
        match u.kind {
            polyphonic::topo::UnitKind::Efficiency => eff += 1,
            _ => perf += 1,
        }
    }
    println!(
        "\n{perf} performance units, {eff} efficiency units, {} memory domain(s)",
        t.domains.len()
    );

    println!("\nlink matrix (ns per byte, [c] = coherent)");
    print!("{:<12}", "unit\\domain");
    for d in &t.domains {
        print!("{:>14}", format!("dom{}", d.id));
    }
    println!();
    for (i, u) in t.units.iter().enumerate() {
        if i > 0 && t.units[i - 1].cluster == u.cluster {
            continue;
        }
        print!("{:<12}", format!("cluster{}", u.cluster));
        for (j, _) in t.domains.iter().enumerate() {
            let l = t.link(i, j);
            print!(
                "{:>14}",
                format!(
                    "{:.4}{}",
                    l.ns_per_byte,
                    if l.coherent { "[c]" } else { "" }
                )
            );
        }
        println!();
    }

    if eff == 0 {
        println!("\nsingle compute cluster: no asymmetry to measure on this host");
        return;
    }
    let _ = bytes;
    println!("\nmeasured dependent-load latency by working set (ns/access)");
    println!(
        "{:<14} {:>12} {:>12} {:>10}",
        "working set", "cluster A", "cluster B", "ratio"
    );
    let probes = [1usize, 4, 16, 64, 256];
    let (mut slower, mut faster) = (0usize, 0usize);
    for ws_mib in probes {
        let ws = ws_mib << 20;
        let a = chase_median(ws, Cluster::Performance, iters);
        let b = chase_median(ws, Cluster::Efficiency, iters);
        let r = b / a;
        if r > 1.15 {
            slower += 1;
        } else if r < 0.87 {
            faster += 1;
        }
        println!(
            "{:<14} {a:>12.2} {b:>12.2} {r:>9.2}x",
            format!("{ws_mib} MiB")
        );
    }

    let separated = slower >= probes.len() - 1 || faster >= probes.len() - 1;

    println!(
        "\nThe latency curve is a real measurement of this host's one memory domain, and it is\n\
         what a per-domain link cost should be derived from: ~10 ns in L2, ~30 ns at 32 MiB,\n\
         ~120 ns out to DRAM."
    );
    if separated {
        println!("Cluster placement separated the two probes; the ratio above is meaningful.");
    } else {
        println!(
            "\nCluster placement did NOT separate: QoS is advisory on this host and macOS runs\n\
             background threads on performance cores when idle, so both probes measure the same\n\
             cluster. Treat the ratio column as noise, not as an interconnect measurement --\n\
             this machine has one memory domain and cannot exhibit the asymmetry the cost model\n\
             is built for. The link constants in `Topology::synthetic` are MODELLED and need\n\
             re-deriving on multi-socket or multi-GPU hardware before any result depends on them."
        );
    }
}

#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_lines)]
fn flows_report(
    hbm: u64,
    dram: u64,
    nvme: u64,
    ops: u64,
    seed: u64,
    step: f64,
    bands: [u8; BlobKind::N],
    clairvoyant: bool,
    p3: Correct,
    ahead: AheadArgs,
) {
    println!(
        "{} ops={ops} seed={seed} bands={bands:?}\n",
        memory_label(hbm, dram)
    );
    let cfg = Trial {
        bands,
        flows: FlowMode::Blind,
        hbm,
        dram,
        nvme,
        policy: Policy::Gdsf,
        seed,
        ops,
        vol: 1.0,
        fix: ahead.ledger(p3.base()),
    };
    let (_, split) = best_split(cfg, false, step);
    let budget = Budget::Split { split, hard: false };
    println!(
        "soft floors [{:.2}/{:.2}/{:.2}/{:.2}], identical quota and trace in every row\n\
         task e2e is critical-path only; prewarm work is materialisation moved off it\n",
        split[0], split[1], split[2], split[3]
    );

    println!(
        "{:<10} {:>13} {:>13} {:>14} {:>11} {:>10} {:>10}",
        "flows", "task e2e (ms)", "stall total", "prewarm work", "net work", "inference", "goodput"
    );
    let row = |label: &str, r: &Report| {
        let stall_s = r.total_ns as f64 / 1e9;
        let prewarm_s = r.prewarm_ns as f64 / 1e9;
        println!(
            "{label:<10} {:>13.2} {stall_s:>12.2}s {prewarm_s:>13.2}s {:>10.2}s {:>10.2} {:>9.1}%",
            r.flow_e2e_ms(),
            stall_s + prewarm_s,
            mean_ms(r.kind_ns[0], r.kind_ops[0]),
            100.0 * r.goodput(),
        );
    };
    let modes = [FlowMode::Blind, FlowMode::Announce, FlowMode::Gate];
    let label = |mode: FlowMode| match mode {
        FlowMode::Blind => "blind",
        FlowMode::Announce => "announce",
        FlowMode::Gate => "gate",
    };
    let mut off = Vec::with_capacity(modes.len());
    for mode in modes {
        let r = run("", Trial { flows: mode, ..cfg }, budget);
        row(label(mode), &r);
        off.push(r);
    }

    if clairvoyant {
        let trial = Trial {
            flows: FlowMode::Blind,
            policy: Policy::Clairvoyant,
            ..cfg
        };
        row("clairvoy.", &run("", trial, budget));
    }
    if !p3.engine_cache {
        return;
    }

    let host_only = run(
        "",
        Trial {
            flows: FlowMode::Announce,
            fix: Correction {
                announce: AnnounceMix {
                    kv: Half::Off,
                    host: cfg.fix.announce.host,
                },
                ..cfg.fix
            },
            ..cfg
        },
        budget,
    );
    row("host-only", &host_only);
    println!(
        "\nengine allocates KV (phase-3.md), same soft floors sizing the partition; {}",
        p3.describe()
    );
    let mut on = Vec::with_capacity(modes.len());
    for mode in modes {
        let r = run(
            "",
            Trial {
                flows: mode,
                fix: ahead.engine(p3.engine(false)),
                ..cfg
            },
            budget,
        );
        row(label(mode), &r);
        on.push(r);
    }
    let margin = |blind: &Report, with: &Report| {
        100.0 * (blind.flow_e2e_ms() - with.flow_e2e_ms()) / blind.flow_e2e_ms().max(1e-9)
    };
    let full = margin(&off[0], &off[1]);
    let snapshot = margin(&off[0], &host_only);
    println!(
        "\nannounce's task-latency margin, P6: {full:.1}% with the ledger allocating, of which \
         {snapshot:.1}% survives without KV prewarm -- KV carried {:.0}% of it; {:.1}% with the \
         engine allocating",
        100.0 * (full - snapshot) / full.abs().max(1e-9),
        margin(&on[0], &on[1]),
    );
    println!(
        "grant: {}; router refused {}, engine preempted {} (announce arm)",
        grant_label(on[1].grant),
        on[1].refused_by_router.iter().sum::<u64>(),
        on[1].preempted.iter().sum::<u64>(),
    );
    if ahead.prefill_ahead || ahead.hold {
        println!(
            "announce arm: prefilled {} blocks, held {}; {:.2} s of prewarm and prefill work",
            on[1].prefilled_blocks,
            on[1].held_blocks,
            on[1].prewarm_ns as f64 / 1e9,
        );
    }
}

#[allow(clippy::too_many_arguments)]
fn volatility_sweep(
    hbm: u64,
    dram: u64,
    nvme: u64,
    ops: u64,
    seed: u64,
    step: f64,
    clairvoyant: bool,
    p3: Correct,
) {
    let bands = [0u8, 1, 2, 1];
    println!("{} ops={ops} seed={seed}\n", memory_label(hbm, dram));
    print!(
        "{:>10} {:>18} {:>16} {:>12}",
        "volatility", "hard-partition (ms)", "soft-floor (ms)", "advantage"
    );
    if clairvoyant {
        print!(
            " {:>16} {:>16} {:>12}",
            "no-floor (ms)", "clairvoyant (ms)", "vs no-floor"
        );
    }
    if p3.engine_cache {
        print!(
            " {:>16} {:>16} {:>14}",
            "engine hard (ms)", "engine soft (ms)", "engine adv."
        );
    }
    println!();
    for i in 0..=5 {
        let v = f64::from(i) / 5.0;
        let t = Trial {
            bands,
            flows: FlowMode::Blind,
            hbm,
            dram,
            nvme,
            policy: Policy::Gdsf,
            seed,
            ops,
            vol: v,
            fix: p3.base(),
        };
        let (hard, _) = best_split(t, true, step);
        let (soft, _) = best_split(t, false, step);
        let hm = mean_ms(hard.total_ns, hard.served.iter().sum());
        let sm = mean_ms(soft.total_ns, soft.served.iter().sum());
        print!(
            "{v:>10.1} {hm:>18.3} {sm:>16.3} {:>11.1}%",
            100.0 * (hm - sm) / hm
        );

        if clairvoyant {
            let t_clair = Trial {
                policy: Policy::Clairvoyant,
                ..t
            };
            let open = run("", t, Budget::Open);
            let clair = run("", t_clair, Budget::Open);
            let om = mean_ms(open.total_ns, open.served.iter().sum());
            let cm = mean_ms(clair.total_ns, clair.served.iter().sum());
            print!(
                " {om:>16.3} {cm:>16.3} {:>11.1}%",
                100.0 * (cm - om) / om.max(f64::MIN_POSITIVE)
            );
        }
        if p3.engine_cache {
            let t_on = Trial {
                fix: p3.engine(false),
                ..t
            };
            let (hard_on, _) = best_split(t_on, true, step);
            let (soft_on, _) = best_split(t_on, false, step);
            let (hn, sn) = (per_req(&hard_on), per_req(&soft_on));
            print!(
                " {hn:>16.3} {sn:>16.3} {:>13.1}%",
                100.0 * (hn - sn) / hn.max(f64::MIN_POSITIVE)
            );
        }
        println!();
    }
}

#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_lines)]
fn ownership_report(
    hbm: u64,
    dram: u64,
    nvme: u64,
    ops: u64,
    seed: u64,
    bands: [u8; BlobKind::N],
    p3: Correct,
) {
    println!(
        "own.rs's authority table -- owned-and-observed.md \u{a7}1, executable (phase-1.md \u{a7}4.2)\n"
    );
    println!(
        "{:<13} {:<5} {:<13} {:<13}",
        "kind", "tier", "capacity", "allocation"
    );
    for kind in BlobKind::ALL {
        for tier in [Tier::Hbm, Tier::Ddr, Tier::Nvme] {
            let cap = authority(kind, tier, Question::Capacity);
            let alloc = authority(kind, tier, Question::Allocation);
            println!(
                "{:<13} {:<5} {:<13} {:<13}",
                format!("{kind:?}"),
                format!("{tier:?}"),
                format!("{cap:?}"),
                format!("{alloc:?}"),
            );
        }
    }
    debug_assert!(
        BlobKind::ALL
            .iter()
            .all(|&k| authority(k, Tier::Hbm, Question::Capacity) == Authority::Orchestrator),
        "P1 (phase-1.md \u{a7}2): capacity authority is uniformly Orchestrator"
    );

    println!(
        "\nstatic census (phase-1.md \u{a7}4.4): the count of authority-split entry points -- \
         bodies Phase 3 replaces -- not of call sites into them, which the lint cannot see. \
         Run:\n\n  cargo build --release --features census 2>&1 | grep -c 'use of deprecated'\n"
    );

    println!(
        "dynamic census: how many of those decisions a {ops}-op trace (seed={seed}) actually \
         made, {}, Budget::Open, flows=announce\n",
        memory_label(hbm, dram)
    );
    println!(
        "one column per engine-authority operation. the four census-marked Quota reads have \
         no column -- they are reads on the eviction path, not decisions. drain fires only \
         when a domain retires, which a single-node trace never does"
    );
    let t = Trial {
        bands,
        flows: FlowMode::Announce,
        hbm,
        dram,
        nvme,
        policy: Policy::Gdsf,
        seed,
        ops,
        vol: 1.0,
        fix: p3.base(),
    };
    let r = run("", t, Budget::Open);
    let ops = r.engine_ops;
    println!(
        "{:<13} {:>8} {:>7} {:>10} {:>8} {:>11} {:>10} {:>8} {:>6} {:>9}",
        "class",
        "admit",
        "touch",
        "anticipate",
        "demote",
        "forget_cold",
        "superseded",
        "spill",
        "drain",
        "total"
    );
    census_rows("", &ops, &BlobKind::ALL);
    if !p3.engine_cache {
        return;
    }
    let on = run(
        "",
        Trial {
            fix: p3.engine(false),
            ..t
        },
        Budget::Open,
    );
    let with = on.engine_ops;
    println!(
        "\nwith the engine allocating KV (phase-3.md \u{a7}1.10, the phase's progress bar), {}",
        grant_label(on.grant)
    );
    census_rows(
        "engine ",
        &with,
        &[BlobKind::KvBlock, BlobKind::WeightShard],
    );
    let row = |o: &polyphonic::cache::EngineOps, k: usize| {
        [
            o.admit[k],
            o.touch[k],
            o.anticipate[k],
            o.demote[k],
            o.forget_cold[k],
            o.superseded[k],
            o.spill[k],
            o.drain[k],
        ]
    };
    let (kv, w) = (BlobKind::KvBlock.idx(), BlobKind::WeightShard.idx());
    println!(
        "KvBlock row zero on every counter: {}. WeightShard row unchanged op for op: {} -- \
         it can only be under a policy with no per-pool state shared across classes; GDSF's \
         inflation is per pool, so a KV eviction reorders the weights beside it (the test \
         census_weightshard_row_is_unchanged_across_the_bit_under_hard_lru_partitions checks \
         the case where it must hold)",
        row(&with, kv).iter().all(|&n| n == 0),
        row(&with, w) == row(&ops, w),
    );
}

fn census_rows(prefix: &str, ops: &polyphonic::cache::EngineOps, kinds: &[BlobKind]) {
    for &kind in kinds {
        let k = kind.idx();
        println!(
            "{:<13} {:>8} {:>7} {:>10} {:>8} {:>11} {:>10} {:>8} {:>6} {:>9}",
            format!("{prefix}{}", CLASS_NAME[k]),
            ops.admit[k],
            ops.touch[k],
            ops.anticipate[k],
            ops.demote[k],
            ops.forget_cold[k],
            ops.superseded[k],
            ops.spill[k],
            ops.drain[k],
            ops.total(kind),
        );
    }
}

fn residency_arms(t: Trial, step: f64) -> (Report, Report, Report) {
    let (mut hard, hs) = best_split(t, true, step);
    hard.label = format!(
        "hard-partition [{:.2}/{:.2}/{:.2}/{:.2}]",
        hs[0], hs[1], hs[2], hs[3]
    );
    let (mut soft, ss) = best_split(t, false, step);
    soft.label = format!(
        "soft-floor     [{:.2}/{:.2}/{:.2}/{:.2}]",
        ss[0], ss[1], ss[2], ss[3]
    );
    let mut open = run("", t, Budget::Open);
    open.label = "no-floor       [open]".to_string();
    (hard, soft, open)
}

fn residency_tables(rows: &[&Report]) {
    println!(
        "{:<32} {:>11} {:>9} {:>9} {:>10} {:>20} {:>21}",
        "arm", "stall/req", "p99 (ms)", "goodput", "from tier", "hit kv/sn/wt/svc", "resident GiB"
    );
    for r in rows {
        arm_row(r);
    }

    println!("\nadmission integrity");
    for r in rows {
        println!(
            "{:<32} over-capacity={:<7} refused={:?} pinned-skips={}",
            r.label, r.over_capacity, r.refused, r.pinned_skips
        );
    }

    println!("\nper-class: mean stall per served request (ms) / goodput");
    println!(
        "{:<32} {:>16} {:>16} {:>16} {:>16}",
        "arm", CLASS_NAME[0], CLASS_NAME[1], CLASS_NAME[2], CLASS_NAME[3]
    );
    for r in rows {
        print!("{:<32}", r.label);
        for k in 0..BlobKind::N {
            let cell = format!(
                "{:.1} / {:.0}%",
                mean_ms(r.kind_ns[k], r.kind_ops[k]),
                100.0 * r.class_goodput(k)
            );
            print!("{cell:>16}");
        }
        println!();
    }

    println!("\nshare of total stall by class (policy can only move what dominates)");
    for r in rows {
        print!("{:<32}", r.label);
        for k in 0..BlobKind::N {
            print!(
                "{:>15.1}%",
                100.0 * r.kind_ns[k] as f64 / r.total_ns.max(1) as f64
            );
        }
        println!();
    }

    println!("\nstall (s) by phase");
    println!(
        "{:<32} {:>16} {:>16} {:>16} {:>16}",
        "arm", PHASE_NAME[0], PHASE_NAME[1], PHASE_NAME[2], PHASE_NAME[3]
    );
    for r in rows {
        print!("{:<32}", r.label);
        for p in r.phase_ns {
            print!("{:>16.2}", p as f64 / 1e9);
        }
        println!();
    }
}

fn engine_taxonomy(rows: &[&Report]) {
    println!("\nengine grant and refusal taxonomy (goodput is not comparable across the bit)");
    for r in rows {
        println!(
            "{:<32} {}; router refused {:?}, engine preempted {:?}, ledger refused {:?}",
            r.label,
            grant_label(r.grant),
            r.refused_by_router,
            r.preempted,
            r.refused,
        );
    }
}

fn per_req(r: &Report) -> f64 {
    mean_ms(r.total_ns, r.served.iter().sum())
}

#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_lines)]
fn residency_report(
    hbm: u64,
    dram: u64,
    nvme: u64,
    ops: u64,
    seed: u64,
    step: f64,
    volatility: f64,
    bands: [u8; BlobKind::N],
    clairvoyant: bool,
    p3: Correct,
) {
    println!(
        "{} nvme={:.1}GiB ops={ops} seed={seed} volatility={volatility}\n",
        memory_label(hbm, dram),
        gib(nvme)
    );
    println!("priority bands (operator-configured): {bands:?}\n");

    let t = Trial {
        bands,
        flows: FlowMode::Blind,
        hbm,
        dram,
        nvme,
        policy: Policy::Gdsf,
        seed,
        ops,
        vol: volatility,
        fix: p3.base(),
    };
    let (hard, soft, open) = residency_arms(t, step);

    let clair = clairvoyant.then(|| {
        let t_clair = Trial {
            policy: Policy::Clairvoyant,
            ..t
        };
        let mut c = run("", t_clair, Budget::Open);
        c.label = "clairvoyant    [open]".to_string();
        c
    });
    let mut rows: Vec<&Report> = vec![&hard, &soft, &open];
    if let Some(c) = &clair {
        rows.push(c);
    }
    residency_tables(&rows);

    let hm = per_req(&hard);
    let sm = per_req(&soft);
    println!(
        "\nsoft-floor vs hard-partition: {:+.1}% stall/req at {:+.1}pp goodput",
        100.0 * (hm - sm) / hm,
        100.0 * (soft.goodput() - hard.goodput())
    );
    if let Some(c) = &clair {
        let cm = per_req(c);
        let om = per_req(&open);
        println!(
            "\nclairvoyant vs no-floor (both open, so eviction quality alone): \
             {:+.1}% stall/req at {:+.1}pp hit rate (kv) -- a signed difference against a \
             heuristic baseline, not a regret (phase-2.md §1.7)",
            100.0 * (cm - om) / om.max(f64::MIN_POSITIVE),
            100.0 * (c.hit[0] - open.hit[0])
        );
        println!(
            "clairvoyant vs soft-floor: {:+.1}% stall/req -- budget policy and eviction \
             quality together, not attributable to either alone",
            100.0 * (cm - sm) / sm.max(f64::MIN_POSITIVE),
        );
    }
    if !p3.engine_cache {
        return;
    }

    println!(
        "\n== engine allocates KV (phase-3.md): every arm re-tuned with the bit on; {} ==\n",
        p3.describe()
    );
    let t_on = Trial {
        fix: p3.engine(false),
        ..t
    };
    let (hard_on, soft_on, open_on) = residency_arms(t_on, step);
    let clair_on = clairvoyant.then(|| {
        let mut c = run(
            "",
            Trial {
                fix: p3.engine(true),
                ..t
            },
            Budget::Open,
        );
        c.label = "clairvoyant    [open]".to_string();
        c
    });
    let mut on: Vec<&Report> = vec![&hard_on, &soft_on, &open_on];
    if let Some(c) = &clair_on {
        on.push(c);
    }
    residency_tables(&on);
    engine_taxonomy(&on);

    let (hn, sn) = (per_req(&hard_on), per_req(&soft_on));
    println!(
        "\nsoft-floor vs hard-partition, P1: {:+.1}% stall/req with the ledger allocating, \
         {:+.1}% with the engine allocating; weight hit {:.2} -> {:.2} (soft) and {:.2} -> \
         {:.2} (hard)",
        100.0 * (hm - sm) / hm,
        100.0 * (hn - sn) / hn.max(f64::MIN_POSITIVE),
        soft.hit[2],
        soft_on.hit[2],
        hard.hit[2],
        hard_on.hit[2],
    );
    for (off, with) in [(&hard, &hard_on), (&soft, &soft_on), (&open, &open_on)] {
        println!(
            "the bit on {}: {:+.1}% stall/req, {:+.2}pp kv hit, {:+.2}pp weight hit",
            off.label.split_whitespace().next().unwrap_or(""),
            100.0 * (per_req(with) - per_req(off)) / per_req(off).max(f64::MIN_POSITIVE),
            100.0 * (with.hit[0] - off.hit[0]),
            100.0 * (with.hit[2] - off.hit[2]),
        );
    }
    if let Some(c) = &clair_on {
        let (cm, om) = (per_req(c), per_req(&open_on));
        println!(
            "\nclairvoyant engine cache vs LRU engine cache (phase-3.md §1.9, both open): \
             {:+.1}% stall/req at {:+.1}pp kv hit -- what a better block manager would be \
             worth, a signed difference and not a bound",
            100.0 * (cm - om) / om.max(f64::MIN_POSITIVE),
            100.0 * (c.hit[0] - open_on.hit[0]),
        );
    }
}

impl Cmd {
    fn correct_without_enforcement(&self) -> Option<Correct> {
        match self {
            Self::Residency { p3, .. }
            | Self::Flows { p3, .. }
            | Self::Placement { p3, .. }
            | Self::CodeReview { p3, .. }
            | Self::Volatility { p3, .. }
            | Self::Ownership { p3, .. } => Some(*p3),
            _ => None,
        }
    }
}

#[allow(clippy::too_many_lines)]
fn main() {
    let cmd = Cli::parse().cmd;
    if cmd
        .correct_without_enforcement()
        .is_some_and(|p3| p3.admit.is_claim())
    {
        println!(
            "--admit quantile, tiered and gate are claims the router queue makes, which only \
             `distributed` runs"
        );
        return;
    }
    match cmd {
        Cmd::Residency {
            hbm,
            dram,
            nvme,
            ops,
            seed,
            step,
            volatility,
            bands,
            clairvoyant,
            p3,
        } => {
            residency_report(
                hbm,
                dram,
                nvme,
                ops,
                seed,
                step,
                volatility,
                bands_of(&bands),
                clairvoyant,
                p3,
            );
        }
        Cmd::Calibrate { path, iters } => calibrate(&path, iters),
        Cmd::Boundary { repeat } => boundary(repeat),
        Cmd::Distributed {
            nodes,
            units_per_node,
            hbm,
            dram,
            nvme,
            ops,
            seed,
            bands,
            distances,
            crossing,
            gossip_period,
            rate,
            fanout,
            repeat,
            regret,
            flow_payload,
            p3,
            bits,
            belief,
            influence,
            fleet,
            enforce,
        } => distributed(
            nodes,
            units_per_node,
            hbm,
            dram,
            nvme,
            ops,
            seed,
            bands_of(&bands),
            &distances,
            &crossing,
            gossip_period,
            rate,
            fanout,
            repeat,
            regret,
            flow_payload,
            p3,
            bits,
            belief,
            influence,
            fleet,
            enforce,
        ),
        Cmd::CodeReview {
            hbm,
            model_ddr,
            agent_ddr,
            nvme,
            units_per_node,
            ops,
            seed,
            bands,
            distances,
            crossing,
            gossip_period,
            rate,
            tool_fraction,
            tool_payload,
            hard_pools,
            repeat,
            regret,
            p3,
            bits,
        } => code_review(
            hbm,
            model_ddr,
            agent_ddr,
            nvme,
            units_per_node,
            ops,
            seed,
            bands_of(&bands),
            &distances,
            &crossing,
            gossip_period,
            rate,
            tool_fraction,
            tool_payload,
            hard_pools,
            repeat,
            regret,
            p3,
            bits,
        ),
        Cmd::DataPath {
            nodes,
            units_per_node,
            hbm,
            dram,
            nvme,
            ops,
            seed,
            bands,
            distance,
            rate,
            fanout,
            repeat,
            tax_us,
        } => data_path(
            nodes,
            units_per_node,
            hbm,
            dram,
            nvme,
            ops,
            seed,
            bands_of(&bands),
            &distance,
            rate,
            fanout,
            repeat,
            tax_us,
        ),
        Cmd::Topology { bytes, iters } => topology(bytes, iters),
        Cmd::Placement {
            sockets,
            units_per_socket,
            hbm,
            dram,
            nvme,
            ops,
            seed,
            bands,
            rate,
            drain_at,
            drain_spill,
            p3,
        } => {
            placement(
                sockets,
                units_per_socket,
                hbm,
                dram,
                nvme,
                ops,
                seed,
                bands_of(&bands),
                rate,
                drain_at,
                p3,
                drain_spill,
            );
        }
        Cmd::Flows {
            hbm,
            dram,
            nvme,
            ops,
            seed,
            step,
            bands,
            clairvoyant,
            p3,
            ahead,
        } => {
            flows_report(
                hbm,
                dram,
                nvme,
                ops,
                seed,
                step,
                bands_of(&bands),
                clairvoyant,
                p3,
                ahead,
            );
        }
        Cmd::Volatility {
            hbm,
            dram,
            nvme,
            ops,
            seed,
            step,
            clairvoyant,
            p3,
        } => {
            volatility_sweep(hbm, dram, nvme, ops, seed, step, clairvoyant, p3);
        }
        Cmd::Ownership {
            hbm,
            dram,
            nvme,
            ops,
            seed,
            bands,
            p3,
        } => {
            ownership_report(hbm, dram, nvme, ops, seed, bands_of(&bands), p3);
        }
        Cmd::Belief {
            nodes,
            units_per_node,
            hbm,
            dram,
            nvme,
            ops,
            seed,
            rate,
            fanout,
            sections,
        } => belief_cmd::run(&belief_cmd::Env {
            nodes,
            units_per_node,
            hbm,
            dram,
            nvme,
            ops,
            seed,
            rate,
            fanout,
            sections,
        }),
        Cmd::Fleet {
            nodes,
            units_per_node,
            hbm,
            dram,
            nvme,
            ops,
            seed,
            rate,
            fanout,
            seeds,
            sections,
        } => fleet_cmd::run(&fleet_cmd::Env {
            nodes,
            units_per_node,
            hbm,
            dram,
            nvme,
            ops,
            seed,
            rate,
            fanout,
            seeds,
            sections,
        }),
        Cmd::Programs {
            seeds,
            seed,
            programs,
            rate,
            compress,
            ops,
            sections,
        } => programs_cmd::run_all(&programs_cmd::Env {
            seeds,
            seed,
            programs,
            rate,
            compress,
            ops,
            sections,
        }),
        Cmd::Regions {
            regions,
            per_region,
            slots,
            units_per_node,
            hbm,
            dram,
            nvme,
            rate,
            seconds,
            seed,
            seeds,
            fanout,
            rtt,
            volatility,
            decode_kv,
            sections,
        } => regions_cmd::run_all(&regions_cmd::Env {
            regions,
            per_region,
            slots,
            units_per_node,
            hbm,
            dram,
            nvme,
            rate,
            seconds,
            seed,
            seeds,
            fanout,
            rtt,
            volatility,
            decode_kv,
            sections,
        }),
        Cmd::Durability {
            nodes,
            units_per_node,
            hbm,
            dram,
            nvme,
            ops,
            seed,
            rate,
            fanout,
            seeds,
            throughput,
            scales,
            fleet_nodes,
            lease_renew,
            reps,
            dir,
            sections,
        } => durability_cmd::run_all(&durability_cmd::Env {
            nodes,
            units_per_node,
            hbm,
            dram,
            nvme,
            ops,
            seed,
            rate,
            fanout,
            seeds,
            throughput,
            scales: scales
                .split(',')
                .map(|s| s.trim().parse().expect("a scale is a number"))
                .collect(),
            fleet_nodes,
            lease_renew,
            reps: reps as usize,
            dir,
            sections,
        }),
        Cmd::Enforce {
            nodes,
            units_per_node,
            hbm,
            dram,
            nvme,
            ops,
            seed,
            rate,
            fanout,
            seeds,
            throughput,
            sections,
        } => enforce_cmd::run(&enforce_cmd::Env {
            nodes,
            units_per_node,
            hbm,
            dram,
            nvme,
            ops,
            seed,
            rate,
            fanout,
            seeds,
            throughput,
            sections,
        }),
        Cmd::Influence {
            nodes,
            units_per_node,
            hbm,
            dram,
            nvme,
            ops,
            seed,
            rate,
            fanout,
            seeds,
            sections,
        } => influence_cmd::run(&influence_cmd::Env {
            nodes,
            units_per_node,
            hbm,
            dram,
            nvme,
            ops,
            seed,
            rate,
            fanout,
            seeds,
            sections,
        }),
        Cmd::Price {
            nodes,
            units_per_node,
            hbm,
            dram,
            nvme,
            ops,
            seed,
            bands,
            distance,
            rate,
            fanout,
            node_hbm,
            node_dram,
            node_ops,
        } => price(&PriceArgs {
            nodes,
            units_per_node,
            hbm,
            dram,
            nvme,
            ops,
            seed,
            bands: bands_of(&bands),
            distance,
            rate,
            fanout,
            node_hbm,
            node_dram,
            node_ops,
        }),
    }
}

fn boundary(repeat: usize) {
    use polyphonic::boundary::measure;

    let l = measure(repeat);
    println!(
        "boundary ladder (p50 ns per operation, measured on this host)\ntimer overhead {:.1} ns/call -- rungs near it are batch-timed and have no tail\n",
        l.timer_ns
    );
    print!("{:<22}", "boundary");
    for n in polyphonic::boundary::SIZES {
        print!("{:>12}", format!("{n} B"));
    }
    println!(
        "{:>12}{:>14}{:>12}{:>10}",
        "fixed ns", "ns/byte", "p99 ns", "spread"
    );

    for r in &l.rungs {
        print!("{:<22}", r.boundary.label());
        for n in polyphonic::boundary::SIZES {
            match r.by_size.iter().find(|s| s.0 == n) {
                Some((_, ns)) => print!("{ns:>12}"),
                None => print!("{:>12}", "-"),
            }
        }
        println!(
            "{:>12.0}{:>14.3}{:>12}{:>9.1}x",
            r.cost.fixed_ns,
            r.cost.ns_per_byte,
            if r.p99_ns == 0 {
                "-".to_string()
            } else {
                r.p99_ns.to_string()
            },
            r.spread
        );
    }

    for (label, ns) in &l.extra {
        println!("{label:<45}{ns:>10} ns  (single figure, not a by_size rung)");
    }

    step_deltas(&l);
    hook_cost_table(&l);
}

#[allow(clippy::cast_possible_wrap)]
fn step_deltas(l: &polyphonic::boundary::Ladder) {
    use polyphonic::boundary::Boundary;

    let mid = polyphonic::boundary::SIZES[1];
    let at = |b: Boundary| -> Option<u64> {
        l.rungs
            .iter()
            .find(|r| r.boundary == b)
            .and_then(|r| r.by_size.iter().find(|s| s.0 == mid))
            .map(|s| s.1)
    };

    println!("\nwhat each step adds, at {mid} B");
    let steps = [
        (
            Boundary::Native,
            Boundary::Wasm,
            "sandbox entry + linear-memory copy",
        ),
        (
            Boundary::Native,
            Boundary::Ring,
            "cross-core cache line + spin detect",
        ),
        (Boundary::Ring, Boundary::Syscall, "ring transition"),
        (
            Boundary::Syscall,
            Boundary::Pipe,
            "kernel buffer copy + second syscall",
        ),
        (
            Boundary::Pipe,
            Boundary::UnixSocket,
            "waking a blocked thread",
        ),
        (
            Boundary::UnixSocket,
            Boundary::TcpLoopback,
            "loopback network stack",
        ),
        (
            Boundary::TcpLoopback,
            Boundary::ExtProc,
            "HTTP/2 framing on an open stream",
        ),
        (Boundary::ExtProc, Boundary::Grpc, "per-call stream setup"),
        (
            Boundary::TcpLoopback,
            Boundary::Grpc,
            "HTTP/2 framing + protobuf",
        ),
    ];
    for (lo, hi, what) in steps {
        let (Some(a), Some(b)) = (at(lo), at(hi)) else {
            continue;
        };

        let delta = b as i64 - a as i64;
        println!(
            "  {what:<38}{:>10.2} us   {:>6.1}x",
            delta as f64 / 1000.0,
            b as f64 / a.max(1) as f64
        );
    }

    let (Some(total), Some(ring), Some(unix), Some(pipe), Some(tcp)) = (
        at(Boundary::Grpc),
        at(Boundary::Ring),
        at(Boundary::UnixSocket),
        at(Boundary::Pipe),
        at(Boundary::TcpLoopback),
    ) else {
        return;
    };
    {
        let wake = unix.saturating_sub(pipe);
        let frame = total.saturating_sub(tcp);
        println!(
            "\nof a {:.1} us gRPC round trip: {:.0}% is HTTP/2 + protobuf, {:.0}% is one thread wakeup,\nand {:.2} us is what the same exchange costs through shared memory",
            total as f64 / 1000.0,
            100.0 * frame as f64 / total as f64,
            100.0 * wake as f64 / total as f64,
            ring as f64 / 1000.0
        );
    }
}

fn hook_cost_table(l: &polyphonic::boundary::Ladder) {
    use polyphonic::boundary::{Boundary, SIZES};

    let payload = SIZES[0] as u64;

    let rows: [(Boundary, &str); 5] = [
        (Boundary::Native, "none"),
        (Boundary::Wasm, "sandbox"),
        (Boundary::Ring, "threads"),
        (Boundary::ExtProc, "process"),
        (Boundary::Grpc, "process"),
    ];

    println!("\npolicy hook inside an argmin (one hook per candidate, {payload} B)\n");
    println!(
        "{:<24}{:<10}{:>12}{:>12}{:>12}{:>18}",
        "", "isolation", "4 nodes", "32 nodes", "128 nodes", "decisions/s @ 32"
    );
    for (b, isolation) in rows {
        let Some(fixed) = l.get(b).map(|c| c.ns(payload)) else {
            println!("{:<24}{:<10}{:>12}", b.label(), isolation, "-");
            continue;
        };
        let at_nodes = |n: u64| format!("{:.2} us", (n * fixed) as f64 / 1000.0);
        let rate = if fixed == 0 {
            "unbounded".to_string()
        } else {
            format!("{:.0}", 1e9 / (32.0 * fixed as f64))
        };
        println!(
            "{:<24}{:<10}{:>12}{:>12}{:>12}{rate:>18}",
            b.label(),
            isolation,
            at_nodes(4),
            at_nodes(32),
            at_nodes(128),
        );
    }

    if let Some(floor) = l
        .rungs
        .iter()
        .find(|r| r.boundary == Boundary::ExtProc)
        .and_then(|r| r.by_size.first())
        .map(|&(bytes, _)| bytes)
        && floor > payload as usize
    {
        println!(
            "\next_proc has no {payload} B sample: a realistic gateway header map encodes to\n{floor} B, so its row above is the fit extrapolated down to {payload} B."
        );
    }
}

#[derive(Default, Clone)]
struct ClassTally {
    stall: [u64; BlobKind::N],
    service: [u64; BlobKind::N],
    decide: [u64; BlobKind::N],
    ops: [u64; BlobKind::N],
    warm: [u64; BlobKind::N],

    warm_ns: [u64; BlobKind::N],

    regime: [u64; polyphonic::oracle::REGIME_COUNT],
    samples: [Vec<u64>; BlobKind::N],
    chat: Vec<u64>,
    stage: Vec<u64>,
    produced: [u64; 2],
    slo_service: [Vec<u64>; 2],
    slo_stall: [Vec<u64>; 2],
    phase: [(u64, u64); polyphonic::work::PHASES],
    tenant_samples: Vec<(u64, u32, u64)>,
    base: u64,
    window: (f64, f64),
    departed: u64,
    details: Vec<Detail>,
}

#[derive(Clone, Copy, Debug)]
struct Detail {
    position: u64,
    slo: usize,
    decodes: bool,
    class: usize,
    served: bool,
    service_ns: u64,
    stall_ns: u64,
    region: u8,
    served_in: Option<usize>,
    facing: bool,
}

type ClassRow<'a> = (&'a str, ClassTally);
type ClassRows<'a> = [ClassRow<'a>];

fn class_table(rows: &ClassRows<'_>) {
    println!("\n  per class: mean service time (ms) / share spent deciding / warm rate");
    print!("  {:<18}", "arm");
    for name in CLASS_NAME {
        print!("{name:>26}");
    }
    println!();
    for (label, t) in rows {
        print!("  {label:<18}");
        for k in BlobKind::ALL {
            let i = k.idx();
            print!(
                "{:>14.3} {:>5.2}% {:>4.0}%",
                mean_ms(t.service[i], t.ops[i]),
                100.0 * t.decide[i] as f64 / t.service[i].max(1) as f64,
                100.0 * t.warm[i] as f64 / t.ops[i].max(1) as f64,
            );
        }
        println!();
    }
    println!();
    regime_table(rows);
}

fn regime_table(rows: &ClassRows<'_>) {
    use polyphonic::oracle::Regime;
    println!("  acquisition regime (share of served requests):");
    print!("  {:<18}", "arm");
    for r in Regime::ALL {
        print!("{:>12}", r.label());
    }
    println!();
    for (label, t) in rows {
        let served: u64 = t.regime.iter().sum();
        print!("  {label:<18}");
        for r in Regime::ALL {
            print!(
                "{:>11.1}%",
                100.0 * t.regime[r.idx()] as f64 / served.max(1) as f64
            );
        }
        println!();
    }
    println!();
}

use polyphonic::machine::{Control, DataPath, Placement};

struct Arm {
    label: &'static str,
    placement: Placement,
    flow: bool,
    control: Control,

    transfer: bool,
}

fn arm(
    label: &'static str,
    placement: Placement,
    flow: bool,
    control: Control,
    transfer: bool,
) -> Arm {
    Arm {
        label,
        placement,
        flow,
        control,
        transfer,
    }
}

fn distributed_arms(gossip_period: u64) -> Vec<Arm> {
    use Control::{Gossip, Query, Unified};
    use Placement::{Aware, Scored, Sticky};
    vec![
        arm("hash only", Sticky, false, Unified, false),
        arm("residency only", Aware, false, Unified, false),
        arm("flow only", Sticky, true, Unified, false),
        arm("both, unified", Aware, true, Unified, false),
        arm("both, rpc query", Aware, true, Query, false),
        arm(
            "both, gossiped",
            Aware,
            true,
            Gossip {
                period: gossip_period,
            },
            false,
        ),
        arm("residency + fetch", Aware, false, Unified, true),
        arm("scored, no flows", Scored, false, Unified, false),
        arm("scored", Scored, true, Unified, false),
        arm("scored + fetch", Scored, true, Unified, true),
        arm(
            "scored + fetch, gossiped",
            Scored,
            true,
            Gossip {
                period: gossip_period,
            },
            true,
        ),
    ]
}

fn state_terms(mach: &polyphonic::machine::Machine, served: u64) {
    let pct = |n: u64| 100.0 * n as f64 / served.max(1) as f64;

    let acts = (mach.fetches + mach.rebuilds).max(1) as f64;
    print!(
        "{:<22} acquired: {:.1}% fetched ({:.1} GiB), {:.1}% rebuilt, {:.1}% stale",
        "",
        100.0 * mach.fetches as f64 / acts,
        gib(mach.fetched_bytes),
        100.0 * mach.rebuilds as f64 / acts,
        pct(mach.stale_fetches),
    );
    if mach.mean_batch() > 0.0 {
        print!(
            "; batch {:.1}, queue {:.2} ms/decode, {:.1}% of decodes arrived saturated",
            mach.mean_batch(),
            mean_ms(mach.queue_ns(), mach.decodes()),
            100.0 * mach.saturated() as f64 / mach.decodes().max(1) as f64,
        );
    }
    println!();
    let fanouts = mach.fanouts_admitted + mach.fanouts_refused;
    if fanouts > 0 {
        println!(
            "{:<22} fan-outs {}/{} ran, {:.1} ms each; agents co-located {:.1}%; \
             {} tool calls, {:.1}% beside their agent, {:.2} ms each",
            "",
            mach.fanouts_admitted,
            fanouts,
            mean_ms(mach.fanout_service_ns, mach.fanouts_admitted),
            100.0 * mach.agents_colocated as f64 / mach.agents_run.max(1) as f64,
            mach.tool_calls,
            100.0 * mach.tool_coplaced as f64 / mach.tool_calls.max(1) as f64,
            mean_ms(mach.tool_ns, mach.tool_calls),
        );
    }
}

fn term_spread(mach: &polyphonic::machine::Machine) {
    use polyphonic::machine::{TERM_COUNT, TERM_LABELS};
    let n = mach.scored_decisions.max(1) as f64;
    print!("{:<22} term spread (mean, ms):", "");
    let shown = if mach.prices_prefill() {
        TERM_COUNT
    } else {
        TERM_COUNT - 1
    };
    for (label, total) in TERM_LABELS.iter().zip(mach.term_spread).take(shown) {
        print!(" {label} {:.2}", total / n / 1e6);
    }
    println!();
}

fn score_terms(mach: &polyphonic::machine::Machine, served: u64) {
    let pct = |n: u64| 100.0 * n as f64 / served.max(1) as f64;
    println!(
        "{:<22} moved by displacement {:.1}%, by flow {:.1}%, by load {:.1}%, by congestion \
         {:.1}%, held at affinity {:.1}%; {:.1}% of {} flows co-placed; engine spread {:.2}",
        "",
        pct(mach.moved_by_displacement),
        pct(mach.moved_by_flow),
        pct(mach.moved_by_load),
        pct(mach.moved_by_congestion),
        pct(mach.held_by_affinity),
        100.0 * mach.flow_coplaced as f64 / mach.flow_requests.max(1) as f64,
        mach.flow_requests,
        mach.engine_spread(),
    );
}

fn regret_report(mach: &polyphonic::machine::Machine) {
    if mach.spans.is_empty() {
        println!(
            "{:<22} regret: no spans (pass --regret, and at least one non-gang request must \
             be served)",
            ""
        );
        return;
    }
    let n = mach.spans.len() as f64;
    let mut total = 0i64;
    let mut execution = 0i64;
    let mut heuristic = 0i64;
    let mut belief = 0i64;
    let mut model = 0i64;
    let mut total_disp = 0i64;
    let mut by_class = [(0i64, 0u64); BlobKind::N];
    for s in &mach.spans {
        let r = s.regret;
        total += r.total;
        execution += r.execution;
        heuristic += r.heuristic;
        belief += r.belief;
        model += r.model;
        total_disp += r.total_with_displacement;
        let (t, c) = &mut by_class[s.class.idx()];
        *t += r.total;
        *c += 1;
    }
    println!(
        "{:<22} regret (mean ns/decision, {} spans): total {:>9.0} = execution {:>8.0} + \
         heuristic {:>8.0} + belief {:>8.0} + model {:>8.0}; total w/ displacement {:>9.0}",
        "",
        mach.spans.len(),
        total as f64 / n,
        execution as f64 / n,
        heuristic as f64 / n,
        belief as f64 / n,
        model as f64 / n,
        total_disp as f64 / n,
    );
    print!("{:<22} regret by class (mean ns/decision):", "");
    for k in BlobKind::ALL {
        let (t, c) = by_class[k.idx()];
        print!(
            " {}: {:.0} (n={c})",
            CLASS_NAME[k.idx()],
            t as f64 / (c.max(1) as f64)
        );
    }
    println!();
    println!(
        "{:<22} feasibility regret: {} refusals where another candidate, priced against \
         truth, could have served",
        "", mach.feasibility_regret,
    );
    let (mc, mcd) = mach.memory_coupled();
    println!(
        "{:<22} coupled %: memory (host DDR) {:.1}% of {} evictions, locality {:.1}% of {} \
         scored decisions",
        "",
        100.0 * mc as f64 / mcd.max(1) as f64,
        mcd,
        100.0 * mach.locality_coupled as f64 / mach.locality_coupled_decisions.max(1) as f64,
        mach.locality_coupled_decisions,
    );
}

fn cluster_header(
    nodes: usize,
    units_per_node: usize,
    memory: &NodeMemory,
    crossing: &str,
    cost: polyphonic::boundary::Cost,
) {
    println!(
        "cluster: {nodes} nodes, {} per node, {units_per_node} units each\n\
         control crossing: {crossing} = {:.1} us + {:.3} ns/byte (MEASURED on this host)\n\
         node link latency, bandwidth and PCIe are MODELLED\n",
        memory_label(memory.hbm, memory.ddr),
        cost.fixed_ns / 1000.0,
        cost.ns_per_byte,
    );
}

fn crossing_of(
    l: &polyphonic::boundary::Ladder,
    name: &str,
) -> Option<(String, polyphonic::boundary::Cost)> {
    use polyphonic::boundary::Boundary;
    let b = match name {
        "native" => Boundary::Native,
        "wasm" => Boundary::Wasm,
        "ring" => Boundary::Ring,
        "syscall" => Boundary::Syscall,
        "pipe" => Boundary::Pipe,
        "unix" => Boundary::UnixSocket,
        "tcp" => Boundary::TcpLoopback,
        "extproc" => Boundary::ExtProc,
        _ => Boundary::Grpc,
    };
    if let Some(c) = l.get(b) {
        return Some((name.to_string(), c));
    }
    let fallback = Boundary::TcpLoopback;
    l.get(fallback).map(|c| {
        (
            format!("{name} NOT BUILT -- charging {}", fallback.label()),
            c,
        )
    })
}

struct ArmRun {
    mach: polyphonic::machine::Machine,
    t: ClassTally,
    total: u64,
    served: u64,
    offered: u64,
}

#[derive(Clone, Copy)]
struct Scenario {
    cost: polyphonic::boundary::Cost,
    rate: f64,
    fanout: f64,
    seed: u64,
    ops: u64,
    flow_payload: Option<u64>,
    regret: bool,
    p3: Correct,
    bits: ClusterBits,
    belief: BeliefArgs,
    influence: InfluenceArgs,
    fleet: FleetArgs,
    enforce: EnforceArgs,
    lag_ns: u64,
    fault: Option<FaultPlan>,
}

fn oracle_of(sc: &Scenario, nodes: usize) -> Option<polyphonic::fleet::OraclePlan> {
    (sc.fleet.fleet && sc.fleet.planner == PlannerArg::Oracle).then(|| {
        let trace: Vec<polyphonic::work::Request> = sc
            .p3
            .workload(
                sc.fleet.workload(
                    polyphonic::work::Workload::with_fanout(
                        sc.seed,
                        sc.ops,
                        sc.fleet.volatility(),
                        sc.fanout,
                    )
                    .with_throughput(sc.belief.throughput)
                    .with_batch(sc.enforce.batch),
                ),
            )
            .collect();
        polyphonic::fleet::oracle_plan(
            &polyphonic::fleet::Costs::of(&sc.fleet.catalogue(), (sc.fleet.interval * 1e9) as u64),
            &trace,
            nodes,
            (1e9 / sc.rate.max(f64::MIN_POSITIVE)) as u64,
            (sc.fleet.late * 1e9) as u64,
        )
    })
}

#[allow(clippy::too_many_lines)]
fn distributed_run(
    a: &Arm,
    topo: &polyphonic::topo::Topology,
    memory: NodeMemory,
    sc: &Scenario,
) -> ArmRun {
    use polyphonic::machine::Machine;
    let inf = sc.influence;
    let memory = NodeMemory {
        kv: memory.kv.map(|kv| EngineKv {
            clairvoyant: kv.clairvoyant || inf.clairvoyant_kv,
            offload: match sc.fleet.kv_offload {
                Some(bytes) if memory.hbm > 0 => bytes.min(memory.ddr),
                _ => kv.offload,
            },
            ..kv
        }),
        ..memory
    };
    let nodes = topo.domains.len();
    let oracle = oracle_of(sc, nodes);
    let fleet = sc
        .fleet
        .fleet_of(nodes, oracle.as_ref().map(|p| p.initial.as_slice()));
    let node_memory = |d: usize| {
        let Some(model) = fleet.as_ref().and_then(|f| f.model_on(d)) else {
            return memory;
        };
        let derived = fleet
            .as_ref()
            .map_or(0, |f| f.partition_bytes(model, memory.hbm));
        NodeMemory {
            kv: memory.kv.map(|kv| EngineKv {
                partition: sc.fleet.fleet_partition.map_or(derived, |p| p.min(derived)),
                ..kv
            }),
            ..memory
        }
    };
    let mut mach = Machine::new(topo.clone(), node_memory, Policy::Gdsf, a.placement);
    if let Some(fleet) = fleet {
        mach.set_fleet(fleet, sc.fleet.fleet_partition);
        sc.fleet.planning(&mut mach);
        for (at, placement) in oracle.iter().flat_map(|p| p.shifts.clone()) {
            mach.schedule_placement(at, placement);
        }
    }
    mach.set_flow_aware(a.flow);
    mach.set_control(a.control, sc.cost);
    mach.set_state_transfer(a.transfer);
    mach.set_arrival_rate(sc.rate);
    mach.set_fanout_atomic(true);
    mach.set_regret(sc.regret);
    sc.p3.setup(&mut mach, sc.bits);
    mach.set_observables(sc.belief.observables);
    if sc.belief.belief && memory.kv.is_some() {
        let span_ns = (sc.ops as f64 / sc.rate.max(f64::MIN_POSITIVE) * 1e9) as u64;
        let nodes = topo.domains.len();
        mach.set_belief(sc.belief.conditions(nodes, sc.lag_ns, span_ns, sc.seed));
        mach.set_scoring(sc.belief.scoring());
        mach.set_instrument(true);
    }
    sc.fleet.apply(&mut mach);
    sc.enforce.apply(&mut mach, sc.p3.admit);
    mach.set_directives(inf.directives());
    mach.set_prefill_ahead(inf.prefill_ahead);
    mach.set_hint_grade(
        match inf.hint_grade {
            HintGradeArg::Declared => polyphonic::machine::HintGrade::Declared,
            HintGradeArg::Template => polyphonic::machine::HintGrade::Template,
            HintGradeArg::Learned => polyphonic::machine::HintGrade::Learned,
        },
        inf.learn_gate,
    );
    mach.set_prefill_target(match inf.prefill_target {
        TargetArg::Argmin => polyphonic::machine::Target::Argmin,
        TargetArg::Deepest => polyphonic::machine::Target::Deepest,
    });
    let workload = sc.fleet.workload(
        polyphonic::work::Workload::with_fanout(sc.seed, sc.ops, sc.fleet.volatility(), sc.fanout)
            .with_throughput(sc.belief.throughput)
            .with_batch(sc.enforce.batch),
    );
    let workload = match sc.flow_payload {
        Some(bytes) => workload.with_flow_payload(bytes),
        None => workload,
    };
    let workload = if inf.reuse || sc.fleet.tenants {
        let origins = polyphonic::work::Origins::default();
        mach.set_origins(origins.clone());
        if sc.fleet.tenants {
            mach.track_tenants();
        }
        workload.with_origins(origins)
    } else {
        workload
    };
    let workload = sc.p3.workload(workload);
    let (mut t, total, served, offered) = if inf.needs_trace() {
        let trace: Vec<polyphonic::work::Request> = workload.collect();
        let foresight = polyphonic::foresight::Foresight::of(&trace);
        if inf.clairvoyant_kv {
            mach.set_clairvoyant(&foresight);
        }
        if inf.oracle() {
            mach.set_foresight(Some(foresight));
        }
        drive_with(&mut mach, sc.rate, &trace, sc.fault)
    } else {
        drive_with(&mut mach, sc.rate, workload, sc.fault)
    };
    t.window = sc.fleet.neighbour_window();
    ArmRun {
        mach,
        t,
        total,
        served,
        offered,
    }
}

fn distributed_header() {
    println!(
        "{:<22} {:>13} {:>12} {:>9} {:>10} {:>11} {:>9} {:>9} {:>9} {:>11}",
        "arm",
        "service/req",
        "stall/req",
        "served",
        "fan-outs",
        "deciding",
        "of stall",
        "split",
        "handoff",
        "spread"
    );
}

fn distributed_row(a: &Arm, r: &ArmRun, regret: bool) {
    let (mach, label) = (&r.mach, a.label);
    let stall = mean_ms(r.total, r.served);
    let service = mean_ms(r.t.service.iter().sum(), r.served);
    println!(
        "{label:<22} {service:>11.3}ms {stall:>10.3}ms {:>8.1}% {:>9.1}% {:>9.3}ms \
         {:>8.2}% {:>8.1}% {:>8.2}s {:>11.2}",
        100.0 * r.served as f64 / r.offered.max(1) as f64,
        100.0 * mach.fanouts_admitted as f64
            / (mach.fanouts_admitted + mach.fanouts_refused).max(1) as f64,
        mean_ms(mach.decide_ns, r.served),
        100.0 * mach.decide_ns as f64 / r.total.max(1) as f64,
        100.0 * mach.split_tasks as f64 / (mach.split_tasks + mach.joined_tasks).max(1) as f64,
        mach.handoff_ns as f64 / 1e9,
        mach.domain_spread(),
    );
    state_terms(mach, r.served);
    if a.placement == Placement::Scored {
        score_terms(mach, r.served);
        term_spread(mach);
    }
    if regret {
        regret_report(mach);
    }
}

fn correction_terms(mach: &polyphonic::machine::Machine, r: &ArmRun, bits: ClusterBits) {
    let pct = |n: u64| 100.0 * n as f64 / r.offered.max(1) as f64;
    let refused: u64 = mach.refused_by_router.iter().sum();
    let preempted: u64 = mach.preempted.iter().sum();
    print!(
        "{:<22} router refused {:.2}% of requests, engine preempted {:.2}%, {} orphaned blocks",
        "",
        pct(refused),
        pct(preempted),
        mach.kv_orphans(),
    );
    if bits.shared_l2.is_some() {
        let k = BlobKind::KvBlock.idx();
        let w = BlobKind::WeightShard.idx();
        print!(
            "; shared L2 read by {:.2}% of served requests for KV, {:.2}% for weights",
            100.0 * mach.shared_requests[k] as f64 / r.served.max(1) as f64,
            100.0 * mach.shared_requests[w] as f64 / r.served.max(1) as f64,
        );
    }
    let waits = &mach.engine_waits;
    if waits.queued > 0 || waits.unfittable > 0 {
        let mean_ms = |i: usize| waits.waited_ns[i] as f64 / waits.waited[i].max(1) as f64 / 1e6;
        print!(
            "; engine queued {:.2}% of requests, mean wait {:.1} ms interactive and {:.1} ms \
             throughput, deepest queue {}, {} too large to wait",
            pct(waits.queued),
            mean_ms(0),
            mean_ms(1),
            waits.max_depth,
            waits.unfittable,
        );
    }
    println!();
}

#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_lines)]
fn distributed(
    nodes: usize,
    units_per_node: usize,
    hbm: u64,
    dram: u64,
    nvme: u64,
    ops: u64,
    seed: u64,
    bands: [u8; BlobKind::N],
    distances: &str,
    crossing: &str,
    gossip_period: u64,
    rate: f64,
    fanout: f64,
    repeat: usize,
    regret: bool,
    flow_payload: Option<u64>,
    p3: Correct,
    bits: ClusterBits,
    belief: BeliefArgs,
    influence: InfluenceArgs,
    fleet: FleetArgs,
    enforce: EnforceArgs,
) {
    use polyphonic::topo::{Distance, Topology};

    if let Some(why) = enforce.refusal(p3, rate, regret, fleet, belief) {
        println!("{why}");
        return;
    }

    if fleet.needs_engine() && rate <= 0.0 {
        println!("--model-batches, --prefill-time and --fleet need a positive --rate");
        return;
    }
    if (fleet.planner != PlannerArg::None || fleet.sizes.is_some()) && !fleet.fleet {
        println!("--planner and --sizes need --fleet");
        return;
    }
    if fleet.sizes.is_some_and(|gib| {
        gib.iter()
            .any(|&g| (g * polyphonic::fleet::GIB as f64) as u64 >= hbm / nodes as u64)
    }) {
        println!("--sizes leaves no partition: every model must be smaller than a node's HBM");
        return;
    }
    if fleet.fleet && !(p3.engine_cache && fleet.model_keyed && hbm > 0 && !belief.belief) {
        println!("--fleet needs --engine-cache, --model-keyed, split memory and no --belief");
        return;
    }
    let plans_roles = matches!(
        fleet.planner,
        PlannerArg::Once | PlannerArg::Follow | PlannerArg::Eager
    );
    if (fleet.pairing != PairingArg::Off || fleet.prefillers > 0)
        && !(fleet.fleet
            && fleet.pairing != PairingArg::Off
            && (fleet.prefillers > 0 || plans_roles)
            && fleet.prefill_time == PrefillArg::Priced)
    {
        println!(
            "--pairing needs --fleet, --prefill-time priced and --prefillers or a --planner that \
             moves replicas"
        );
        return;
    }
    if let Some(why) = fleet.tenant_refusal(nodes, p3.engine_cache) {
        println!("{why}");
        return;
    }
    if fleet.prefillers > 0 && fleet.planner != PlannerArg::None {
        println!("--prefillers fixes the roles and takes no --planner");
        return;
    }
    if fleet.fleet_of(nodes, None).is_some_and(|f| {
        f.counts()
            .iter()
            .zip(f.decode_counts())
            .any(|(&all, dec)| all > 0 && dec == 0)
    }) {
        println!("--prefillers leaves a model with no replica that decodes");
        return;
    }
    if fleet.planner != PlannerArg::None && (fleet.interval * 1e9) as u64 == 0 {
        println!("--planner needs a positive --interval");
        return;
    }
    if fleet.prefill_load().is_some_and(|l| l.window_ns == 0) {
        println!("--prefill-time needs a positive --prefill-window");
        return;
    }
    if fleet
        .replicas
        .is_some_and(|r| r.iter().map(|&n| usize::from(n)).sum::<usize>() > nodes)
    {
        println!("--replicas places more replicas than there are --nodes");
        return;
    }
    if belief.belief && fleet.model_batches == ModelBatchesArg::Priced {
        println!("--model-batches priced needs no --belief: a reported load names no model");
        return;
    }
    if belief.belief && !(p3.engine_cache && rate > 0.0) {
        println!("--belief needs --engine-cache and a positive --rate");
        return;
    }
    if (influence.directives || influence.clairvoyant_kv) && !(p3.engine_cache && rate > 0.0) {
        println!("--directives and --clairvoyant-kv need --engine-cache and a positive --rate");
        return;
    }
    let ladder = polyphonic::boundary::measure(repeat);
    let Some((crossing, cost)) = crossing_of(&ladder, crossing) else {
        println!("no boundary rung available");
        return;
    };
    let crossing = crossing.as_str();
    let per_node = dram / nodes as u64;
    let memory = node_memory(
        hbm / nodes as u64,
        per_node,
        nvme / nodes as u64,
        bands,
        false,
    );

    cluster_header(nodes, units_per_node, &memory, crossing, cost);
    let sc = Scenario {
        cost,
        rate,
        fanout,
        seed,
        ops,
        flow_payload,
        regret,
        p3,
        bits,
        belief,
        influence,
        fleet,
        enforce,
        lag_ns: 0,
        fault: None,
    };

    let mut warm_seen = [(0u64, 0u64); BlobKind::N];
    let arms = distributed_arms(gossip_period);

    for name in distances.split(',') {
        let Ok(dist) = name.trim().parse::<Distance>() else {
            println!("skipping unknown distance {name}");
            continue;
        };
        let topo = Topology::cluster(nodes, units_per_node, per_node, dist, cost);
        let sc = Scenario {
            lag_ns: dist.one_way_ns(),
            ..sc
        };
        println!(
            "== {} : {:.0} us hop, {:.2} ns/byte ==",
            dist.label(),
            dist.one_way_ns() as f64 / 1000.0,
            dist.ns_per_byte()
        );
        distributed_header();
        let mut per_class: Vec<ClassRow<'_>> = Vec::new();
        let mut means = Vec::with_capacity(arms.len());
        for a in &arms {
            let r = distributed_run(a, &topo, memory, &sc);
            distributed_row(a, &r, regret);
            if p3.engine_cache {
                correction_terms(&r.mach, &r, bits);
            }
            means.push(r.mach.kv_mean());
            for (seen, (ns, n)) in warm_seen.iter_mut().zip(r.t.warm_ns.iter().zip(&r.t.warm)) {
                seen.0 += ns;
                seen.1 += n;
            }
            per_class.push((a.label, r.t));
        }

        class_table(&per_class);
        fanout_admission(&topo, memory, &sc);
        if !p3.engine_cache {
            continue;
        }
        let g = p3.grant(&memory, [0; 3]);
        let partition = if fleet.fleet {
            "what each node's model leaves".to_string()
        } else {
            format!("{:.2} GiB", gib(g.map_or(0, |g| g.partition)))
        };
        println!(
            "\n-- engine allocates KV (phase-3.md): partition {partition}, offload {:.2} GiB per \
             node, spill from each arm's own run above; {} --",
            gib(g.map_or(0, |g| g.offload)),
            p3.describe(),
        );
        distributed_header();
        let mut per_class_on: Vec<ClassRow<'_>> = Vec::new();
        for (a, off) in arms.iter().zip(&means) {
            let r = distributed_run(a, &topo, p3.engine_memory(memory, *off), &sc);
            distributed_row(a, &r, regret);
            correction_terms(&r.mach, &r, bits);
            per_class_on.push((a.label, r.t));
        }
        class_table(&per_class_on);
        engine_fanout_admission(&topo, memory, &sc);
    }
    crossover(&ladder, cost, &warm_seen);
}

#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_lines)]
fn code_review(
    hbm: u64,
    model_ddr: u64,
    agent_ddr: u64,
    nvme: u64,
    units_per_node: usize,
    ops: u64,
    seed: u64,
    bands: [u8; BlobKind::N],
    distances: &str,
    crossing: &str,
    gossip_period: u64,
    rate: f64,
    tool_fraction: f64,
    tool_payload: u64,
    hard_pools: bool,
    repeat: usize,
    regret: bool,
    p3: Correct,
    bits: ClusterBits,
) {
    use polyphonic::machine::Machine;
    use polyphonic::topo::{Distance, Topology};

    const MODEL: usize = 0;
    const AGENT: usize = 1;
    const NODES: usize = 2;

    let ladder = polyphonic::boundary::measure(repeat);
    let Some((crossing, cost)) = crossing_of(&ladder, crossing) else {
        println!("no boundary rung available");
        return;
    };

    let model_mem = node_memory(hbm, model_ddr, nvme / 2, bands, hard_pools);

    let agent_mem = NodeMemory {
        hbm: 0,
        ddr: agent_ddr,
        nvme: nvme / 2,
        hbm_quota: Quota::open(0, bands),
        ddr_quota: Quota::from_split(agent_ddr, [0.0, 0.35, 0.0, 0.35], bands, hard_pools),
        can_decode: false,
        kv: None,
    };

    println!(
        "model host:  hbm={:.1}GiB ddr={:.1}GiB, decode engine\n\
         agent host:  ddr={:.1}GiB, no accelerator -- cannot decode, runs tool calls\n\
         control crossing: {crossing} = {:.1} us + {:.3} ns/byte (MEASURED on this host)\n\
         node link, PCIe and every workload constant are MODELLED\n\
         memory: {}\n\
         tool calls: {:.0}% of turns, {:.0} KiB each way, anchored to the agent host\n",
        gib(hbm),
        gib(model_ddr),
        gib(agent_ddr),
        cost.fixed_ns / 1000.0,
        cost.ns_per_byte,
        if hard_pools {
            "hard partitions -- each class owns a fixed slice, no borrowing"
        } else {
            "one ledger, soft floors"
        },
        100.0 * tool_fraction,
        tool_payload as f64 / 1024.0,
    );

    let arms = distributed_arms(gossip_period);
    let mut warm_seen = [(0u64, 0u64); BlobKind::N];

    for name in distances.split(',') {
        let Ok(dist) = name.trim().parse::<Distance>() else {
            println!("skipping unknown distance {name}");
            continue;
        };

        let topo = Topology::cluster(NODES, units_per_node, model_ddr, dist, cost);
        println!(
            "== {} : {:.0} us hop, {:.2} ns/byte ==",
            dist.label(),
            dist.one_way_ns() as f64 / 1000.0,
            dist.ns_per_byte()
        );
        let header = || {
            println!(
                "{:<22} {:>13} {:>12} {:>9} {:>11} {:>10} {:>10} {:>11}",
                "arm",
                "service/req",
                "stall/req",
                "served",
                "round trip",
                "tools home",
                "handoff",
                "on model"
            );
        };
        header();
        let run_arm = |a: &Arm, model: NodeMemory| -> ArmRun {
            let memory_at = move |d: usize| if d == MODEL { model } else { agent_mem };
            let mut mach = Machine::new(topo.clone(), memory_at, Policy::Gdsf, a.placement);
            mach.set_flow_aware(a.flow);
            mach.set_control(a.control, cost);
            mach.set_state_transfer(a.transfer);
            mach.set_fanout_atomic(true);
            mach.set_tool_anchor(Some(AGENT));
            mach.set_regret(regret);
            p3.setup(&mut mach, bits);

            mach.set_origin(Some((AGENT, tool_payload)));
            let (t, total, served, offered) = drive(
                &mut mach,
                rate,
                p3.workload(
                    polyphonic::work::Workload::new(seed, ops, 1.0)
                        .with_tool_profile(tool_fraction, tool_payload),
                ),
            );
            ArmRun {
                mach,
                t,
                total,
                served,
                offered,
            }
        };
        let row = |a: &Arm, r: &ArmRun| {
            let mach = &r.mach;
            let label = a.label;
            let stall = mean_ms(r.total, r.served);
            let service = mean_ms(r.t.service.iter().sum(), r.served);

            let stages = (mach.split_tasks + mach.joined_tasks).max(1);
            println!(
                "{label:<22} {service:>11.3}ms {stall:>10.3}ms {:>8.1}% {:>9.3}ms {:>9.1}% \
                 {:>9.2}s {:>10.1}%",
                100.0 * r.served as f64 / r.offered.max(1) as f64,
                mean_ms(mach.origin_ns, mach.origin_hops),
                100.0 * mach.joined_tasks as f64 / stages as f64,
                mach.handoff_ns as f64 / 1e9,
                100.0 * mach.decodes_on(MODEL) as f64 / mach.decodes().max(1) as f64,
            );
            state_terms(mach, r.served);
            if a.placement == Placement::Scored {
                score_terms(mach, r.served);
            }
            if regret {
                regret_report(mach);
            }
        };
        let mut per_class: Vec<ClassRow<'_>> = Vec::new();
        let mut means = Vec::with_capacity(arms.len());
        for a in &arms {
            let r = run_arm(a, model_mem);
            row(a, &r);
            if p3.engine_cache {
                correction_terms(&r.mach, &r, bits);
            }
            means.push(r.mach.kv_mean());
            for (seen, (ns, n)) in warm_seen.iter_mut().zip(r.t.warm_ns.iter().zip(&r.t.warm)) {
                seen.0 += ns;
                seen.1 += n;
            }
            per_class.push((a.label, r.t));
        }
        class_table(&per_class);
        if !p3.engine_cache {
            continue;
        }
        println!(
            "\n-- engine allocates KV on the model host (phase-3.md); {} --",
            p3.describe()
        );
        header();
        let mut per_class_on: Vec<ClassRow<'_>> = Vec::new();
        for (a, off) in arms.iter().zip(&means) {
            let r = run_arm(a, p3.engine_memory(model_mem, *off));
            row(a, &r);
            correction_terms(&r.mach, &r, bits);
            per_class_on.push((a.label, r.t));
        }
        class_table(&per_class_on);
    }
    crossover(&ladder, cost, &warm_seen);
}

struct Shape {
    region: u8,
    served_in: Option<usize>,
    facing: bool,
    deferred_ns: u64,
    class: usize,
    base_at: u64,
    tenant: u32,
    phase: usize,
    decodes: bool,
    slo: polyphonic::work::Slo,
    produced: u64,
    stage: bool,
}

impl Shape {
    fn of(req: &polyphonic::work::Request, base_at: u64) -> Self {
        Self {
            region: req.region,
            served_in: None,
            facing: req.client_facing(),
            deferred_ns: 0,
            class: req.kind_idx(),
            base_at,
            tenant: req.tenant.unwrap_or(u32::MAX),
            phase: req.phase,
            decodes: req.tokens > 0,
            slo: req.slo,
            produced: req.produces.iter().map(|(_, m)| m.bytes).sum(),
            stage: req.completes.is_some(),
        }
    }
}

fn tally(
    t: &mut ClassTally,
    total: &mut u64,
    served: &mut u64,
    shape: &Shape,
    c: &polyphonic::cache::Cost,
) {
    let service = c.service_ns() + shape.deferred_ns;
    let stall = c.total_ns() + shape.deferred_ns;
    t.details.push(Detail {
        position: shape.base_at,
        slo: shape.slo.idx(),
        decodes: shape.decodes,
        class: shape.class,
        served: !c.pending,
        service_ns: service,
        stall_ns: stall,
        region: shape.region,
        served_in: shape.served_in,
        facing: shape.facing,
    });
    if c.pending {
        return;
    }
    let k = shape.class;
    t.tenant_samples
        .push((shape.base_at, shape.tenant, service));
    *total += stall;
    t.stall[k] += stall;
    t.service[k] += service;
    t.decide[k] += c.decide_ns;
    t.ops[k] += 1;
    t.samples[k].push(service);
    t.phase[shape.phase].0 += service;
    t.phase[shape.phase].1 += 1;
    if k == BlobKind::KvBlock.idx() && shape.decodes {
        t.slo_service[shape.slo.idx()].push(service);
        t.slo_stall[shape.slo.idx()].push(stall);
    }
    if k == BlobKind::KvBlock.idx() {
        if shape.stage {
            t.stage.push(service);
            t.produced[1] += shape.produced;
        } else {
            t.chat.push(service);
            t.produced[0] += shape.produced;
        }
    }

    if c.transfer_ns == 0 && c.recompute_ns == 0 {
        t.warm[k] += 1;
        t.warm_ns[k] += service;
    }
    t.regime[polyphonic::oracle::classify(c).idx()] += 1;
    *served += 1;
}

fn settle(
    mach: &mut polyphonic::machine::Machine,
    open: &mut std::collections::HashMap<usize, Shape>,
    t: &mut ClassTally,
    total: &mut u64,
    served: &mut u64,
) {
    for (id, cost) in mach.drain_closed() {
        let shape = open.remove(&id).expect("a closed request was open");
        tally(t, total, served, &shape, &cost);
    }
    for id in mach.drain_departed() {
        open.remove(&id).expect("a departed request was open");
        t.departed += 1;
    }
}

#[derive(Clone, Copy, Debug)]
struct FaultPlan {
    at_request: usize,
    fault: polyphonic::fault::Fault,
    retry: polyphonic::fault::Retry,
}

fn drive<R: std::borrow::Borrow<polyphonic::work::Request>>(
    mach: &mut polyphonic::machine::Machine,
    rate: f64,
    workload: impl IntoIterator<Item = R>,
) -> (ClassTally, u64, u64, u64) {
    drive_with(mach, rate, workload, None)
}

fn submit_tallying(
    mach: &mut polyphonic::machine::Machine,
    req: &polyphonic::work::Request,
    at_ns: Option<u64>,
    shape: Shape,
    acc: &mut Drive,
) {
    use polyphonic::machine::Submitted;
    let outcome = match at_ns {
        Some(at) => mach.submit_at(at, req),
        None => mach.submit(req),
    };
    match outcome {
        Submitted::Closed(c) => {
            let shape = Shape {
                served_in: mach.served_region().filter(|_| !c.pending),
                ..shape
            };
            tally(&mut acc.t, &mut acc.total, &mut acc.served, &shape, &c);
        }
        Submitted::Open(id) => {
            acc.open.insert(id, shape);
        }
    }
    settle(
        mach,
        &mut acc.open,
        &mut acc.t,
        &mut acc.total,
        &mut acc.served,
    );
}

#[derive(Default)]
struct Drive {
    t: ClassTally,
    total: u64,
    served: u64,
    open: std::collections::HashMap<usize, Shape>,
}

fn drive_with<R: std::borrow::Borrow<polyphonic::work::Request>>(
    mach: &mut polyphonic::machine::Machine,
    rate: f64,
    workload: impl IntoIterator<Item = R>,
    plan: Option<FaultPlan>,
) -> (ClassTally, u64, u64, u64) {
    mach.set_arrival_rate(rate);
    let interval_ns = if rate > 0.0 { (1e9 / rate) as u64 } else { 0 };
    let mut acc = Drive::default();
    let mut offered = 0u64;
    let mut outage_end = 0u64;
    let mut waiting: Vec<(u64, polyphonic::work::Request, Shape)> = Vec::new();
    for (position, req) in workload.into_iter().enumerate() {
        let req = req.borrow();
        mach.set_position(position as u64);
        offered += 1;
        let shape = Shape::of(req, acc.t.base);
        acc.t.base += u64::from(!req.concurrent);
        let mut at_ns = None;
        if let Some(plan) = plan {
            if position == plan.at_request {
                mach.inject(plan.fault);
                outage_end = mach.now_ns() + plan.fault.outage_ns();
            }
            let nominal = mach.now_ns() + if req.concurrent { 0 } else { interval_ns };
            if !req.concurrent && nominal < outage_end {
                mach.advance_to(nominal);
            }
            release_waiting(mach, &mut waiting, nominal, &mut acc);
            if nominal < outage_end {
                let mut attempts = 0u64;
                let at = polyphonic::fault::retry_at(plan.retry, nominal, outage_end, &mut || {
                    attempts += 1;
                    polyphonic::rng::Rng::hashed_unit(
                        (position as u64) << 8 ^ RETRY_JITTER_KEY ^ attempts,
                    )
                });
                waiting.push((
                    at,
                    req.clone(),
                    Shape {
                        deferred_ns: at - nominal,
                        ..shape
                    },
                ));
                settle(
                    mach,
                    &mut acc.open,
                    &mut acc.t,
                    &mut acc.total,
                    &mut acc.served,
                );
                continue;
            }
            at_ns = Some(nominal);
        }
        submit_tallying(mach, req, at_ns, shape, &mut acc);
    }
    release_waiting(mach, &mut waiting, u64::MAX, &mut acc);
    mach.finish();
    settle(
        mach,
        &mut acc.open,
        &mut acc.t,
        &mut acc.total,
        &mut acc.served,
    );
    assert!(
        acc.open.is_empty(),
        "every request is closed once the trace drains"
    );
    (acc.t, acc.total, acc.served, offered)
}

const RETRY_JITTER_KEY: u64 = 0x7e57_0a11;

fn release_waiting(
    mach: &mut polyphonic::machine::Machine,
    waiting: &mut Vec<(u64, polyphonic::work::Request, Shape)>,
    until_ns: u64,
    acc: &mut Drive,
) {
    waiting.sort_by_key(|(at, _, _)| *at);
    let due = waiting.partition_point(|(at, _, _)| *at <= until_ns);
    for (at, req, shape) in waiting.drain(..due) {
        submit_tallying(mach, &req, Some(at), shape, acc);
    }
}

fn fanout_admission(topo: &polyphonic::topo::Topology, memory: NodeMemory, sc: &Scenario) {
    if sc.fanout <= 0.0 {
        return;
    }
    println!("\n  fan-out admission (scored + fetch, unified control)");
    fanout_header();
    for atomic in [false, true] {
        let r = fanout_run(topo, memory, sc, atomic);
        fanout_row(atomic, &r);
    }
}

fn fanout_header() {
    println!(
        "  {:<16} {:>11} {:>13} {:>12} {:>12} {:>16} {:>9}",
        "admission", "fan-outs", "wasted work", "fan-out", "stall/req", "inference stall", "served"
    );
}

fn fanout_run(
    topo: &polyphonic::topo::Topology,
    memory: NodeMemory,
    sc: &Scenario,
    atomic: bool,
) -> ArmRun {
    use polyphonic::machine::{Machine, Placement};
    let mut mach = Machine::new(topo.clone(), |_| memory, Policy::Gdsf, Placement::Scored);
    mach.set_flow_aware(true);
    mach.set_state_transfer(true);
    mach.set_fanout_atomic(atomic);
    sc.p3.setup(&mut mach, sc.bits);
    let (t, total, served, offered) = drive(
        &mut mach,
        sc.rate,
        sc.p3.workload(polyphonic::work::Workload::with_fanout(
            sc.seed, sc.ops, 1.0, sc.fanout,
        )),
    );
    ArmRun {
        mach,
        t,
        total,
        served,
        offered,
    }
}

fn fanout_row(atomic: bool, r: &ArmRun) {
    let mach = &r.mach;
    println!(
        "  {:<16} {:>6}/{:<4} {:>11.2}s {:>10.1}ms {:>10.3}ms {:>14.3}ms {:>8.1}%",
        if atomic {
            "all-or-nothing"
        } else {
            "per agent"
        },
        mach.fanouts_admitted,
        mach.fanouts_admitted + mach.fanouts_refused,
        mach.fanout_wasted_ns as f64 / 1e9,
        mean_ms(mach.fanout_service_ns, mach.fanouts_admitted),
        mean_ms(r.total, r.served),
        mean_ms(r.t.stall[0], r.t.ops[0]),
        100.0 * r.served as f64 / r.offered.max(1) as f64,
    );
}

fn engine_fanout_admission(topo: &polyphonic::topo::Topology, memory: NodeMemory, sc: &Scenario) {
    if sc.fanout <= 0.0 {
        return;
    }
    if !sc.enforce.is_off(sc.p3.admit) {
        println!(
            "\n  fan-out admission, engine allocates KV: not run, since it drives fan-outs \
             without the engine wait, router queue, claims and cancel this run sets"
        );
        return;
    }
    println!(
        "\n  fan-out admission, engine allocates KV (router reserves {})",
        sc.p3.reserve().label()
    );
    fanout_header();
    let mut completed = [(0u64, 0u64); 2];
    for (i, atomic) in [false, true].into_iter().enumerate() {
        let off = fanout_run(topo, memory, sc, atomic);
        let r = fanout_run(
            topo,
            sc.p3.engine_memory(memory, off.mach.kv_mean()),
            sc,
            atomic,
        );
        fanout_row(atomic, &r);
        completed[i] = (off.mach.fanouts_admitted, r.mach.fanouts_admitted);
    }
    let gain = |base: u64, with: u64| 100.0 * (with as f64 - base as f64) / base.max(1) as f64;
    println!(
        "  all-or-nothing over per-agent: {:+.1}% fan-outs completed on the ledger, {:+.1}% \
         with the engine allocating",
        gain(completed[0].0, completed[1].0),
        gain(completed[0].1, completed[1].1),
    );
}

struct PriceArgs {
    nodes: usize,
    units_per_node: usize,
    hbm: u64,
    dram: u64,
    nvme: u64,
    ops: u64,
    seed: u64,
    bands: [u8; BlobKind::N],
    distance: String,
    rate: f64,
    fanout: f64,
    node_hbm: u64,
    node_dram: u64,
    node_ops: u64,
}

fn quantile(v: &[u64], q: f64) -> u64 {
    polyphonic::instruments::percentile(&mut v.to_vec(), q).unwrap_or(0)
}

fn mean_of(v: &[u64]) -> f64 {
    mean_ms(v.iter().sum(), v.len() as u64)
}

fn correct(decode_kv: bool, admit: AdmitArg) -> Correct {
    Correct {
        engine_cache: true,
        decode_kv,
        tokens_per_block: polyphonic::work::TOKENS_PER_KV_BLOCK,
        admit,
        max_token_slack: polyphonic::work::MAX_TOKEN_SLACK,
        kv_scale: 1.0,
        kv_partition: None,
    }
}

fn price_run(
    a: &PriceArgs,
    topo: &polyphonic::topo::Topology,
    memory: NodeMemory,
    p3: Correct,
    regret: bool,
) -> ArmRun {
    use polyphonic::machine::Machine;
    let mut mach = Machine::new(topo.clone(), |_| memory, Policy::Gdsf, Placement::Scored);
    mach.set_flow_aware(true);
    mach.set_control(Control::Unified, polyphonic::boundary::Cost::default());
    mach.set_state_transfer(true);
    mach.set_fanout_atomic(true);
    mach.set_regret(regret);
    p3.setup(
        &mut mach,
        ClusterBits {
            shared_l2: None,
            no_displacement: false,
        },
    );
    let workload = polyphonic::work::Workload::with_fanout(a.seed, a.ops, 1.0, a.fanout);
    let (t, total, served, offered) = drive(&mut mach, a.rate, p3.workload(workload));
    ArmRun {
        mach,
        t,
        total,
        served,
        offered,
    }
}

fn price_pair(
    a: &PriceArgs,
    topo: &polyphonic::topo::Topology,
    memory: NodeMemory,
    p3: Correct,
    regret: bool,
) -> (ArmRun, ArmRun) {
    let ledger = Correct {
        engine_cache: false,
        ..p3
    };
    let off = price_run(a, topo, memory, ledger, regret);
    let on = price_run(
        a,
        topo,
        p3.engine_memory(memory, off.mach.kv_mean()),
        p3,
        regret,
    );
    (off, on)
}

fn price_header() {
    println!(
        "  {:<24} {:>10} {:>9} {:>17} {:>17} {:>9} {:>9} {:>8} {:>9} {:>8} {:>8}",
        "",
        "service",
        "p99",
        "chat mean/p99",
        "stage mean/p99",
        "faas p99",
        "svc p99",
        "served",
        "fan-outs",
        "refused",
        "preempt"
    );
}

fn price_row(label: &str, r: &ArmRun) {
    let all: Vec<u64> = r.t.samples.iter().flatten().copied().collect();
    let refused: u64 = r.mach.refused_by_router.iter().sum();
    let preempted: u64 = r.mach.preempted.iter().sum();
    let pct = |n: u64| 100.0 * n as f64 / r.offered.max(1) as f64;
    let gangs = r.mach.fanouts_admitted + r.mach.fanouts_refused;
    println!(
        "  {label:<24} {:>8.1}ms {:>7.0}ms {:>7.1}/{:>7.0}ms {:>7.1}/{:>7.0}ms {:>7.1}ms \
         {:>7.0}ms {:>7.1}% {:>8.1}% {:>7.2}% {:>7.2}%",
        mean_of(&all),
        ms(quantile(&all, 0.99)),
        mean_of(&r.t.chat),
        ms(quantile(&r.t.chat, 0.99)),
        mean_of(&r.t.stage),
        ms(quantile(&r.t.stage, 0.99)),
        ms(quantile(&r.t.samples[BlobKind::Snapshot.idx()], 0.99)),
        ms(quantile(&r.t.samples[BlobKind::ServiceHeap.idx()], 0.99)),
        pct(r.served),
        100.0 * r.mach.fanouts_admitted as f64 / gangs.max(1) as f64,
        pct(refused),
        pct(preempted),
    );
}

fn signed(off: f64, on: f64) -> f64 {
    100.0 * (on - off) / off.abs().max(f64::MIN_POSITIVE)
}

#[allow(clippy::too_many_lines)]
fn price(a: &PriceArgs) {
    use polyphonic::topo::{Distance, Topology};
    let Ok(dist) = a.distance.trim().parse::<Distance>() else {
        println!("unknown distance {}", a.distance);
        return;
    };
    let n = a.nodes as u64;
    let memory = node_memory(a.hbm / n, a.dram / n, a.nvme / n, a.bands, false);
    let topo = Topology::cluster(
        a.nodes,
        a.units_per_node,
        a.dram / n,
        dist,
        polyphonic::boundary::Cost::default(),
    );
    println!(
        "the price of the engine boundary (phase-3.md \u{a7}4.11)\n\
         cluster: {} nodes, {} per node, {}, {} req/s, {:.0}% fan-out, ops={} seed={}\n\
         arm: scored + fetch, flow-aware, unified control, no control crossing charged\n\
         default grant per node: {}\n\
         chat = a session's own turn; stage = inference another task waits on\n",
        a.nodes,
        memory_label(memory.hbm, memory.ddr),
        dist.label(),
        a.rate,
        100.0 * a.fanout,
        a.ops,
        a.seed,
        {
            let g = correct(false, AdmitArg::None).grant(&memory, [0; 3]);
            format!(
                "partition {:.2} GiB, offload {:.2} GiB, spill sized per run",
                gib(g.map_or(0, |g| g.partition)),
                gib(g.map_or(0, |g| g.offload)),
            )
        },
    );

    println!("1. the bit: who allocates KV, holding everything else fixed");
    price_header();
    for decode_kv in [false, true] {
        let p3 = correct(decode_kv, AdmitArg::None);
        let (off, on) = price_pair(a, &topo, memory, p3, false);
        let tag = if decode_kv { ", decode kv" } else { "" };
        price_row(&format!("ledger{tag}"), &off);
        price_row(&format!("engine{tag}"), &on);
        let (all_off, all_on): (Vec<u64>, Vec<u64>) = (
            off.t.samples.iter().flatten().copied().collect(),
            on.t.samples.iter().flatten().copied().collect(),
        );
        println!(
            "  {:<24} {:+.1}% mean, {:+.1}% p99; chat p99 {:+.1}%, stage p99 {:+.1}%; \
             served {:+.1}pp -- a price is only a price where served agrees",
            "  price",
            signed(mean_of(&all_off), mean_of(&all_on)),
            signed(
                quantile(&all_off, 0.99) as f64,
                quantile(&all_on, 0.99) as f64
            ),
            signed(
                quantile(&off.t.chat, 0.99) as f64,
                quantile(&on.t.chat, 0.99) as f64
            ),
            signed(
                quantile(&off.t.stage, 0.99) as f64,
                quantile(&on.t.stage, 0.99) as f64
            ),
            100.0 * (on.served as f64 - off.served as f64) / off.offered.max(1) as f64,
        );
    }

    println!(
        "\n2. admission bracket (P4): what the router reserves, decode output modelled and held"
    );
    price_header();
    let ledger = price_run(
        a,
        &topo,
        memory,
        Correct {
            engine_cache: false,
            ..correct(true, AdmitArg::None)
        },
        false,
    );
    price_row("ledger (reference)", &ledger);
    let mean = ledger.mach.kv_mean();
    for scale in [1.0, 0.5, 0.25] {
        let mut bracket = Vec::new();
        for admit in [AdmitArg::Bound, AdmitArg::Perfect, AdmitArg::None] {
            let p3 = Correct {
                kv_scale: scale,
                ..correct(true, admit)
            };
            let r = price_run(a, &topo, p3.engine_memory(memory, mean), p3, false);
            price_row(&format!("{scale}x, {}", p3.reserve().label()), &r);
            bracket.push(r);
        }
        let (perfect, none) = (&bracket[1], &bracket[2]);
        let out = none.t.produced;
        println!(
            "  {scale}x none against perfect: chat p99 {:+.1}%, stage p99 {:+.1}%, faas p99 \
             {:+.1}%; unreserved output {:.0}% chat, {:.0}% stage (fan-out agents not counted)",
            signed(
                quantile(&perfect.t.chat, 0.99) as f64,
                quantile(&none.t.chat, 0.99) as f64
            ),
            signed(
                quantile(&perfect.t.stage, 0.99) as f64,
                quantile(&none.t.stage, 0.99) as f64
            ),
            signed(
                quantile(&perfect.t.samples[BlobKind::Snapshot.idx()], 0.99) as f64,
                quantile(&none.t.samples[BlobKind::Snapshot.idx()], 0.99) as f64
            ),
            100.0 * out[0] as f64 / (out[0] + out[1]).max(1) as f64,
            100.0 * out[1] as f64 / (out[0] + out[1]).max(1) as f64,
        );
    }

    println!("\n3. max_tokens slack under `bound`");
    price_header();
    for slack in [1.0, 2.0, 4.0, 8.0] {
        let p3 = Correct {
            max_token_slack: slack,
            ..correct(true, AdmitArg::Bound)
        };
        let r = price_run(a, &topo, p3.engine_memory(memory, mean), p3, false);
        price_row(&format!("bound, {slack:.0}x"), &r);
    }

    println!(
        "\n4. tokens per KV block (\u{a7}1.3): measured blocks per decode against the \
         arithmetic, and the bit at each"
    );
    price_header();
    for (per_block, expect) in [(8u64, 15.88), (16, 8.18), (32, 4.33), (35, 4.015)] {
        let p3 = Correct {
            tokens_per_block: per_block,
            ..correct(true, AdmitArg::None)
        };
        let single = run(
            "",
            Trial {
                bands: a.bands,
                flows: FlowMode::Blind,
                hbm: a.node_hbm,
                dram: a.node_dram,
                nvme: 64 << 30,
                policy: Policy::Gdsf,
                seed: a.seed,
                ops: a.node_ops,
                vol: 1.0,
                fix: p3.base(),
            },
            Budget::Open,
        );
        let (off, on) = price_pair(a, &topo, memory, p3, false);
        println!(
            "  {per_block} tokens/block: {:.3} blocks per decode, {expect} expected",
            single.decode_blocks as f64 / single.decodes.max(1) as f64
        );
        price_row(&format!("ledger, {per_block}"), &off);
        price_row(&format!("engine, {per_block}"), &on);
    }

    println!(
        "\n5. partition size (P7): the bit at each size on the cluster, and on one node beside \
         what a clairvoyant block manager would buy at that size"
    );
    price_header();
    let base = Trial {
        bands: a.bands,
        flows: FlowMode::Blind,
        hbm: a.node_hbm,
        dram: a.node_dram,
        nvme: 64 << 30,
        policy: Policy::Gdsf,
        seed: a.seed,
        ops: a.node_ops,
        vol: 1.0,
        fix: Correction::default(),
    };
    let stream = trace(base);
    let (node_off, split) = best_split(base, false, 0.125);
    let budget = Budget::Split { split, hard: false };
    let mut node_rows = Vec::new();
    let base_p3 = correct(false, AdmitArg::None);
    let cluster_off = price_run(
        a,
        &topo,
        memory,
        Correct {
            engine_cache: false,
            ..base_p3
        },
        false,
    );
    price_row("ledger", &cluster_off);
    for scale in [0.5, 0.75, 1.0, 1.5, 2.0] {
        let p3 = Correct {
            kv_scale: scale,
            ..base_p3
        };
        let on = price_run(
            a,
            &topo,
            p3.engine_memory(memory, cluster_off.mach.kv_mean()),
            p3,
            false,
        );
        price_row(
            &format!(
                "engine, {scale}x = {:.2} GiB",
                gib(p3.grant(&memory, [0; 3]).map_or(0, |g| g.partition))
            ),
            &on,
        );
        let arm = |clairvoyant| Trial {
            fix: Correction {
                engine: Some(EngineArm {
                    scale,
                    partition: None,
                    clairvoyant,
                }),
                ..Correction::default()
            },
            ..base
        };
        let lru = run_on("", arm(false), budget, &stream);
        let clair = run_on("", arm(true), budget, &stream);
        node_rows.push((scale, lru, clair));
    }
    println!(
        "\n  one node, {}, {} ops, residency's soft floors [{:.2}/{:.2}/{:.2}/{:.2}] held fixed; \
         ledger {:.3} ms stall/req at {:.1}% goodput",
        memory_label(a.node_hbm, a.node_dram),
        a.node_ops,
        split[0],
        split[1],
        split[2],
        split[3],
        per_req(&node_off),
        100.0 * node_off.goodput(),
    );
    println!(
        "  {:<8} {:>12} {:>13} {:>9} {:>14} {:>14} {:>22}",
        "scale",
        "partition",
        "bit (stall)",
        "goodput",
        "kv hit",
        "weight hit",
        "clairvoyant vs LRU"
    );
    for (scale, lru, clair) in &node_rows {
        println!(
            "  {:<8} {:>9.2} GiB {:>12.1}% {:>8.1}% {:>7.2} -> {:.2} {:>7.2} -> {:.2} {:>14.1}% \
             ({:+.1}pp kv)",
            format!("{scale}x"),
            gib(lru.grant.map_or(0, |g| g.partition)),
            signed(per_req(&node_off), per_req(lru)),
            100.0 * lru.goodput(),
            node_off.hit[0],
            lru.hit[0],
            node_off.hit[2],
            lru.hit[2],
            signed(per_req(lru), per_req(clair)),
            100.0 * (clair.hit[0] - lru.hit[0]),
        );
    }

    println!(
        "\n6. fan-out admission (P5): all-or-nothing against per agent, across the capacities \
         residency-ledger.md swept, on the ledger's per-block test and the router's partition one"
    );
    println!(
        "  {:<15} {:>22} {:>26} {:>26} {:>26}",
        "HBM + DDR", "ledger", "engine, none", "engine + decode, perfect", "engine + decode, bound"
    );
    for (hbm, dram) in [(4u64, 8u64), (5, 10), (6, 12), (8, 16)] {
        let mem = node_memory(
            (hbm << 30) / n,
            (dram << 30) / n,
            a.nvme / n,
            a.bands,
            false,
        );
        let sc = |p3: Correct| Scenario {
            cost: polyphonic::boundary::Cost::default(),
            rate: a.rate,
            fanout: a.fanout,
            seed: a.seed,
            ops: a.ops,
            flow_payload: None,
            regret: false,
            p3,
            bits: ClusterBits {
                shared_l2: None,
                no_displacement: false,
            },
            belief: BeliefArgs::OFF,
            influence: InfluenceArgs::OFF,
            fleet: FleetArgs::OFF,
            enforce: EnforceArgs::OFF,
            lag_ns: 0,
            fault: None,
        };
        let cell = |p3: Correct| -> String {
            let mut out = Vec::with_capacity(2);
            for atomic in [false, true] {
                let off = fanout_run(
                    &topo,
                    mem,
                    &sc(Correct {
                        engine_cache: false,
                        ..p3
                    }),
                    atomic,
                );
                let r = if p3.engine_cache {
                    fanout_run(
                        &topo,
                        p3.engine_memory(mem, off.mach.kv_mean()),
                        &sc(p3),
                        atomic,
                    )
                } else {
                    off
                };
                out.push((r.mach.fanouts_admitted, r.mach.fanout_wasted_ns));
            }
            format!(
                "{} / {} ({:.0}s / {:.0}s)",
                out[0].0,
                out[1].0,
                out[0].1 as f64 / 1e9,
                out[1].1 as f64 / 1e9
            )
        };
        println!(
            "  {:<15} {:>22} {:>26} {:>26} {:>26}",
            format!("{hbm} + {dram} GiB"),
            cell(Correct {
                engine_cache: false,
                ..correct(false, AdmitArg::None)
            }),
            cell(correct(false, AdmitArg::None)),
            cell(correct(true, AdmitArg::Perfect)),
            cell(correct(true, AdmitArg::Bound)),
        );
    }
    println!(
        "  cells: fan-outs completed per agent / all-or-nothing, of those offered (wasted work)"
    );

    println!(
        "\n7. arbitration and regret (P2, P3): host-DDR coupling and the four gaps on both \
         sides of the bit"
    );
    for dram in [a.dram, a.dram / 2] {
        let mem = node_memory(a.hbm / n, dram / n, a.nvme / n, a.bands, false);
        let (off, on) = price_pair(a, &topo, mem, correct(false, AdmitArg::None), true);
        println!("  {}", memory_label(mem.hbm, mem.ddr));
        for (label, r) in [("ledger", &off), ("engine", &on)] {
            let (c, d) = r.mach.memory_coupled();
            let spans = r.mach.spans.len().max(1) as f64;
            let sum = |f: fn(&polyphonic::oracle::Regret) -> i64| -> f64 {
                r.mach.spans.iter().map(|s| f(&s.regret)).sum::<i64>() as f64 / spans
            };
            let nonzero = r
                .mach
                .spans
                .iter()
                .filter(|s| s.regret.execution != 0)
                .count();
            println!(
                "    {label:<8} memory coupled {:>5.1}% of {:>6} DDR evictions; execution \
                 {:>9.0} ns/decision ({:.1}% of spans), belief {:.0}, model {:.0}, total {:.0}",
                100.0 * c as f64 / d.max(1) as f64,
                d,
                sum(|g| g.execution),
                100.0 * nonzero as f64 / spans,
                sum(|g| g.belief),
                sum(|g| g.model),
                sum(|g| g.total),
            );
        }
    }
}

fn crossover(
    ladder: &polyphonic::boundary::Ladder,
    grpc: polyphonic::boundary::Cost,
    warm: &[(u64, u64); BlobKind::N],
) {
    use polyphonic::boundary::Boundary;
    const RPCS: f64 = 4.0;
    let Some(ring) = ladder.get(Boundary::Ring) else {
        return;
    };
    let q = 1024;
    let (g, r) = (RPCS * grpc.ns(q) as f64, RPCS * ring.ns(q) as f64);
    println!(
        "control-plane tax as a share of one request, at {RPCS:.0} decisions/request\n\
         (gRPC {:.1} us and shared ring {:.2} us per decision, both measured)\n",
        grpc.ns(q) as f64 / 1000.0,
        ring.ns(q) as f64 / 1000.0
    );
    println!(
        "{:<40} {:>12} {:>12}",
        "work being scheduled", "over gRPC", "over a ring"
    );
    let row = |name: &str, ns: f64| {
        println!(
            "{name:<40} {:>11.1}% {:>11.2}%",
            100.0 * g / (g + ns),
            100.0 * r / (r + ns)
        );
    };
    for (k, name) in CLASS_NAME.iter().enumerate() {
        let (ns, n) = warm[k];
        if n == 0 {
            continue;
        }
        let mean = ns as f64 / n as f64;
        row(
            &format!("warm {name} ({n} seen, {:.0} us)", mean / 1000.0),
            mean,
        );
    }
    println!();
    for (name, ns) in [
        ("hypothetical: 10 us of work", 10_000.0),
        ("hypothetical: 1 ms of work", 1_000_000.0),
        ("hypothetical: 100 ms of work", 100_000_000.0),
    ] {
        row(name, ns);
    }
}

struct PathArm {
    label: &'static str,
    path: DataPath,
    hook: polyphonic::boundary::Cost,
}

fn path_arms(l: &polyphonic::boundary::Ladder) -> Option<Vec<PathArm>> {
    use polyphonic::boundary::{Boundary, Cost, EXTPROC_STREAM_OPEN};
    let reuse = l.get(Boundary::ExtProc)?;
    let mut arms = vec![
        PathArm {
            label: "integrated",
            path: DataPath::Integrated,
            hook: Cost::default(),
        },
        PathArm {
            label: "sidecar (stream reuse)",
            path: DataPath::Sidecar,
            hook: reuse,
        },
    ];
    if let Some(&(_, fresh_ns)) = l.extra.iter().find(|(k, _)| *k == EXTPROC_STREAM_OPEN) {
        arms.push(PathArm {
            label: "sidecar (stream/request)",
            path: DataPath::Sidecar,
            hook: Cost {
                fixed_ns: fresh_ns as f64,
                ns_per_byte: 0.0,
            },
        });
    } else {
        println!("no {EXTPROC_STREAM_OPEN} sample -- dropping the stream-per-request arm");
    }
    arms.push(PathArm {
        label: "sidecar, pluggable policy",
        path: DataPath::SidecarPluggable,
        hook: reuse,
    });
    Some(arms)
}

struct PathRun {
    served: u64,
    total_ns: u64,
    decisions: u64,
    candidates_seen: u64,
    dispatches: u64,
}

impl PathRun {
    fn d(&self) -> f64 {
        self.decisions as f64 / self.served.max(1) as f64
    }

    fn mean_candidates(&self) -> f64 {
        self.candidates_seen as f64 / self.decisions.max(1) as f64
    }

    fn dispatch_mult(&self) -> f64 {
        self.dispatches as f64 / self.served.max(1) as f64
    }

    fn mean_ns(&self) -> f64 {
        self.total_ns as f64 / self.served.max(1) as f64
    }
}

#[allow(clippy::too_many_arguments)]
fn data_path(
    nodes: usize,
    units_per_node: usize,
    hbm: u64,
    dram: u64,
    nvme: u64,
    ops: u64,
    seed: u64,
    bands: [u8; BlobKind::N],
    distance: &str,
    rate: f64,
    fanout: f64,
    repeat: usize,
    tax_us: Option<f64>,
) {
    use polyphonic::boundary::Boundary;
    use polyphonic::machine::Machine;
    use polyphonic::topo::{Distance, Topology};

    let Ok(dist) = distance.trim().parse::<Distance>() else {
        println!("unknown distance {distance}");
        return;
    };
    if nodes == 0 || units_per_node == 0 {
        println!("--nodes and --units-per-node must be at least 1");
        return;
    }
    if !cfg!(feature = "grpc") {
        println!("ext_proc rungs not built -- run with --features grpc");
        return;
    }

    let ladder = polyphonic::boundary::measure(repeat);
    let Some(arms) = path_arms(&ladder) else {
        println!("ext_proc rung did not measure on this host");
        return;
    };
    let Some(integrated_dispatch) = ladder.get(Boundary::UnixSocket) else {
        println!("no unix-socket rung available");
        return;
    };
    let Some(sidecar_dispatch) = ladder.get(Boundary::TcpLoopback) else {
        println!("no tcp-loopback rung available");
        return;
    };
    let Some((topo_label, topo_crossing)) = crossing_of(&ladder, "grpc") else {
        println!("no boundary rung available");
        return;
    };

    let per_node = dram / nodes as u64;
    let memory = node_memory(
        hbm / nodes as u64,
        per_node,
        nvme / nodes as u64,
        bands,
        false,
    );
    let topo = Topology::cluster(nodes, units_per_node, per_node, dist, topo_crossing);

    println!(
        "cluster: {nodes} nodes, {} per node, {units_per_node} units each -- {} : {:.0} us hop\n\
         node-to-node transport: {topo_label} = {:.1} us + {:.3} ns/byte (MEASURED, orthogonal to the data path below)\n\
         node link latency, bandwidth and PCIe are MODELLED\n\
         placement: scored, control: unified -- fixed across every arm, so the data path is the only thing that varies\n",
        memory_label(memory.hbm, memory.ddr),
        dist.label(),
        dist.one_way_ns() as f64 / 1000.0,
        topo_crossing.fixed_ns / 1000.0,
        topo_crossing.ns_per_byte,
    );

    println!(
        "{:<28}{:>13}{:>12}{:>8}{:>9}",
        "arm", "service/req", "stall/req", "served", "d"
    );

    let trace: Vec<_> = polyphonic::work::Workload::with_fanout(seed, ops, 1.0, fanout).collect();
    let mut class_rows: Vec<ClassRow<'static>> = Vec::new();
    let mut runs: Vec<PathRun> = Vec::new();
    for a in &arms {
        let mut mach = Machine::new(topo.clone(), |_| memory, Policy::Gdsf, Placement::Scored);
        mach.set_flow_aware(true);
        mach.set_control(Control::Unified, topo_crossing);
        let dispatch = if a.path == DataPath::Integrated {
            integrated_dispatch
        } else {
            sidecar_dispatch
        };
        mach.set_data_path(a.path, a.hook, dispatch);
        mach.set_state_transfer(true);
        mach.set_fanout_atomic(true);
        let (t, total, served, _offered) = drive(&mut mach, rate, &trace);
        let run = PathRun {
            served,
            total_ns: total,
            decisions: mach.decisions,
            candidates_seen: mach.candidates_seen,
            dispatches: mach.dispatches,
        };
        println!(
            "{:<28}{:>11.3}ms{:>10.3}ms{:>8}{:>9.2}",
            a.label,
            mean_ms(t.service.iter().sum(), served),
            mean_ms(total, served),
            served,
            run.d(),
        );
        class_rows.push((a.label, t));
        runs.push(run);
    }
    class_table(&class_rows);

    let taxes = path_taxes(&arms, &runs, integrated_dispatch, sidecar_dispatch);
    tax_table(&taxes);
    crossover_table(&taxes, tax_us, runs[0].d());
    fleet_ceiling_table(&ladder, rate, nodes, runs[0].d());
}

struct PathTax {
    label: &'static str,
    hook_label: String,
    d: f64,
    disp: f64,
    bound_ns: f64,
    realized_ns: f64,
}

fn path_taxes(
    arms: &[PathArm],
    runs: &[PathRun],
    integrated_dispatch: polyphonic::boundary::Cost,
    sidecar_dispatch: polyphonic::boundary::Cost,
) -> Vec<PathTax> {
    use polyphonic::machine::{DISPATCH_BYTES, HOOK_BYTES};

    let base_mean = runs[0].mean_ns();
    let dispatch_delta =
        sidecar_dispatch.ns(DISPATCH_BYTES) as f64 - integrated_dispatch.ns(DISPATCH_BYTES) as f64;
    arms.iter()
        .zip(runs)
        .skip(1)
        .map(|(a, r)| {
            let hook_ns = a.hook.ns(HOOK_BYTES) as f64;
            let per_decision = if a.path == DataPath::SidecarPluggable {
                r.mean_candidates()
            } else {
                1.0
            };
            let hook_label = if a.path == DataPath::SidecarPluggable {
                format!("{per_decision:.1} x {:.2} us", hook_ns / 1000.0)
            } else {
                format!("{:.2} us", hook_ns / 1000.0)
            };
            PathTax {
                label: a.label,
                hook_label,
                d: r.d(),
                disp: r.dispatch_mult(),
                bound_ns: per_decision * hook_ns * r.d() + dispatch_delta * r.dispatch_mult(),
                realized_ns: r.mean_ns() - base_mean,
            }
        })
        .collect()
}

fn tax_table(taxes: &[PathTax]) {
    println!(
        "\ncontrol-plane tax, from the measured ladder (parse term dropped -- phase-8.md §1.2)\n"
    );
    println!(
        "{:<28}{:<18}{:>7}{:>7}{:>16}{:>16}",
        "arm", "hook", "d", "disp", "upper bound/req", "realized/req"
    );
    for t in taxes {
        println!(
            "{:<28}{:<18}{:>7.2}{:>7.2}{:>13.2} us{:>13.2} us",
            t.label,
            t.hook_label,
            t.d,
            t.disp,
            t.bound_ns / 1000.0,
            t.realized_ns / 1000.0,
        );
    }
}

fn crossover_table(taxes: &[PathTax], tax_us: Option<f64>, d: f64) {
    println!(
        "\ncrossover against the integrated path (S* = T x (1/f - 1), at d = {d:.2} decisions/request)\n"
    );
    println!(
        "{:<28}{:>16}{:>18}",
        "arm", "5% of a request", "1% of a request"
    );
    let mut rows: Vec<(&str, f64)> = taxes.iter().map(|t| (t.label, t.realized_ns)).collect();
    if let Some(us) = tax_us {
        rows.push(("override (--tax-us)", us * 1000.0));
    }
    for (label, t) in &rows {
        println!(
            "{label:<28}{:>13.2} ms{:>15.2} ms",
            19.0 * t / 1e6,
            99.0 * t / 1e6,
        );
    }
    if tax_us.is_some() {
        println!(
            "\n--tax-us is read as a per-request tax, not a per-crossing one: scale a seam cost\nmeasured on another host by this workload's own d and dispatch multiplier before passing it."
        );
    }

    println!("\noverhead share at a log grid of service times\n");
    print!("{:<28}", "arm");
    let grid = [10e3, 100e3, 1e6, 10e6, 100e6, 1e9];
    for s in grid {
        print!("{:>12}", format_ns_label(s));
    }
    println!();
    for (label, t) in &rows {
        print!("{label:<28}");
        for s in grid {
            print!("{:>11.2}%", 100.0 * t / (t + s));
        }
        println!();
    }
}

fn format_ns_label(ns: f64) -> String {
    if ns < 1e6 {
        format!("{:.0} us", ns / 1e3)
    } else if ns < 1e9 {
        format!("{:.0} ms", ns / 1e6)
    } else {
        format!("{:.0} s", ns / 1e9)
    }
}

fn fleet_ceiling_table(l: &polyphonic::boundary::Ladder, rate: f64, nodes: usize, d: f64) {
    use polyphonic::boundary::{Boundary, SIZES};

    let payload = SIZES[0] as u64;
    let lambda = rate / nodes.max(1) as f64;
    println!(
        "\nfleet size at which one unsharded scheduler saturates\n\
         (lambda = {lambda:.1} req/s/node, d = {d:.2} measured, every active node scored)\n"
    );
    println!(
        "{:<24}{:>14}{:>10}",
        "hook boundary", "ns/crossing", "N_max"
    );
    for b in [
        Boundary::Native,
        Boundary::Wasm,
        Boundary::Ring,
        Boundary::ExtProc,
        Boundary::Grpc,
    ] {
        let Some(c) = l.get(b).map(|cost| cost.ns(payload)) else {
            println!("{:<24}{:>14}", b.label(), "-");
            continue;
        };
        let n_max = if c == 0 {
            "unbounded".to_string()
        } else {
            format!("{:.0}", (1e9 / (lambda * d * c as f64)).sqrt())
        };
        println!("{:<24}{c:>14}{n_max:>10}", b.label());
    }
    println!(
        "\nassumes one scheduler thread and every active node scored; sharding the scheduler\n\
         or pruning candidates before the hook divides the ceiling by the shard count or the\n\
         prune ratio instead (phase-8.md §1.4)."
    );
    if let Some(floor) = l
        .rungs
        .iter()
        .find(|r| r.boundary == Boundary::ExtProc)
        .and_then(|r| r.by_size.first())
        .map(|&(bytes, _)| bytes)
        && floor > payload as usize
    {
        println!(
            "ext_proc's row is its fit extrapolated down to {payload} B from a {floor} B floor."
        );
    }
}
