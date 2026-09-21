# Phase 0 -- Price the seams

Implementation plan for Phase 0 of [`owned-and-observed.md`](owned-and-observed.md): add the two
rungs the boundary ladder is missing, and publish the per-decision cost of a policy hook at each
isolation level.

**Status: implemented and measured.** `Boundary::Wasm` and `Boundary::ExtProc` are in
[`boundary.rs`](../src/boundary.rs) behind the `wasm` and `grpc` features, `Ring` is re-timed, and
`polyphonic boundary` prints the §3.4 hook-cost table. §1's three predictions are annotated below
with what `cargo run --release --features grpc,wasm -- boundary --repeat 10` found on this host; the
numbers are published in [`residency-ledger.md`](residency-ledger.md)'s *Boundary costs* section and
folded into [`owned-and-observed.md`](owned-and-observed.md) §2.2, §2.3, §2.6 and §2.7. Section 6's
exclusions (the `data_path` arm, a real Envoy filter, polyproto's own WASM ABI) remain out of scope
and undone.

**Two corrections from review, before the numbers below are read.** The first measurement of the
`ext_proc` rung labelled a 367-byte message as the ladder's 64 B sample -- a realistic header map
cannot be padded *downwards* -- and timed a deep copy of the request inside the clock that the gRPC
rung it is compared against does not pay. Both are fixed: the rung now reports the size it actually
achieved (no 64 B cell, and the report says so) and clones before starting the clock. The published
slope changed as a result, and the "`ext_proc` marshals more steeply than gRPC" reading it produced
did not survive. Separately, §5's risk 3 is now actually honoured: `wasmtime` is pinned to **34**,
the newest major whose own `rust-version` is 1.85, and the crate declares `rust-version = "1.85"` so
the floor is enforced by cargo rather than asserted in prose.

Phase 0 touches `boundary.rs`, `main.rs`'s report, and two new proto/guest artifacts. It changes no
scheduler, no ledger and no arm, so it can be finished before any correction in the memory chain
lands.

**Why it is worth doing first.** Every data-path claim in §2 rests on two numbers the ladder does
not measure. `ext_proc` is quoted at **49 us** -- but that is today's `Grpc` rung, a *unary* echo,
and an `ext_proc` callout is a **bidirectional stream carrying a header map**. WASM is quoted at
**52-232 ns** -- but that is the `Ring` rung standing in for a sandbox the repository has never
run. §2.2, §2.3 and §2.6 are all denominated in those two borrowed constants.

---

## 1. Predictions, stated first

§7's rule: a result may rest on constants, but the conclusions that flip have to be named before
the measurement, not after. Three predictions, each attached to the line it would rewrite.

**P1 -- WASM lands at or below `Ring`, not beside it.** A warm `wasmtime` instance called through a
typed function is a guarded indirect call and a trampoline: tens of nanoseconds, plus a `memcpy`
into linear memory. `Ring` pays a cross-core cache line and a spin detect, which WASM does not.
Predicted 20-80 ns fixed, 0.02-0.05 ns/byte.

- *If right:* §2.6's isolation table is right by accident and for the wrong reason, and the row
  should say `Wasm` rather than borrow `Ring`.
- *If wrong* (WASM in the hundreds of ns to low us): the sandboxed tier leaves the argmin and the
  expressiveness argument narrows to first-party `Native` extensions only.

**Measured: confirmed, by more margin than predicted.** Wasm fixed cost is 13 ns against Ring's
re-timed 67 ns -- below the *low* end of the 20-80 ns predicted range, not just below Ring.
`ns/byte` came in at 0.011, also under the predicted 0.02-0.05. §2.6's table now has its own `Wasm`
row rather than a borrowed `Ring` one.

**P2 -- `ext_proc` on an established stream is materially cheaper than gRPC unary, and this is the
risk to §2.2.** A unary call opens a stream per request: HEADERS, HPACK, per-call futures. A
callout riding an open bidi stream pays DATA frames and codec only. Predicted **40-70% of unary**,
so roughly 20-35 us.

- *If right:* §2.2's "4 x 49 us = 198 us per placement" becomes ~100-140 us, §2.3's 65 us saving
  shrinks, and §2.7's predicted crossover (~1.3 ms / ~6.5 ms) moves **down** by about the same
  factor. §2.7 already says what to do in that case, and Phase 0 rather than Phase 8 is where it
  would be found.
- *If wrong* -- and it may be, because Envoy's default `ext_proc` config opens **a stream per HTTP
  request**, which puts the setup back on the critical path -- then 49 us was approximately right
  and §2 is stronger than it claimed. Measure both shapes (§3.2) rather than assuming either; the
  per-request-stream question is a config detail to confirm against the Envoy release targeted, in
  the same spirit as §1's note on vLLM's `kv_events`.

**Measured: both branches hold, one per shape.** On a stream already open, the callout's fixed cost
is 36 us against gRPC unary's 48 us -- 74% of unary, materially cheaper but outside the top of the
40-70% predicted band. §2.2's "4 x 49 us = 198 us" becomes 142 us and §2.3's saving is recomputed
rather than shrinking cleanly. But the *other* shape -- a stream opened per request, which is
Envoy's documented default -- measures at 63 us, **above** gRPC unary, confirming the "if wrong"
branch for exactly the deployment most sidecars actually run. Both numbers are published; neither
is quietly preferred.

**P3 -- per-call isolation moves WASM out of the argmin.** §2.6 places a sandboxed hook "per
request, per decision". That holds for a *shared* instance. If untrusted means one call's state
must not reach the next, the cost is a fresh instance or a memory reset per call. Predicted 10-100x
the warm call -- microseconds, not nanoseconds.

- *If right:* §2.6's table needs two WASM rows, not one, and "can this extension be trusted" has a
  different answer depending on whether trust is per-tenant or per-call.

**Measured: confirmed, and by more than the predicted ratio.** A fresh instance plus one call costs
~9.5 us against a 13-25 ns warm call -- **400-750x**, not the predicted 10-100x. Per-call isolation
is real, costs more than expected, and leaves the argmin entirely; §2.6 now carries both WASM rows.

---

## 2. What the rungs must and must not do

The ladder's discipline is that every rung asks **the same question** so the difference is the
boundary and not the work. `native()` computes `b[0] + b[b.len()-1]` and returns a `u64`. Both new
rungs do exactly that and nothing else.

Three rules the implementation inherits:

1. **Same payload sizes.** `SIZES = [64, 1024, 8192]`, so `Cost::fit` can separate the fixed
   crossing cost from the marshalling slope, and so the rung is comparable to every other row.
2. **Same instrument.** Timer overhead subtracted via `net()`; anything predicted under ~500 ns is
   batch-timed with `batched()` rather than timed per operation, because per-operation timing near
   the clock measures the clock. WASM is batch-timed; `ext_proc` is timed per operation and carries
   a real tail.
3. **Best-of-`reps` with spread reported.** `measure(reps)` already does this; the new rungs need
   only return a `Rung` with `by_size` populated and let the existing machinery fit, minimise and
   compute spread.

---

## 3. Work items

### 3.1 `Boundary::Wasm` -- a real sandbox, warm instance

**Dependency.** `wasmtime`, behind a new `wasm` feature, following the `grpc` feature's pattern
exactly: the enum variant is unconditional, the measurement is `#[cfg(feature = "wasm")]`, and an
absent rung degrades the report rather than failing it. Off by default -- it pulls Cranelift and
costs build time that a `polyphonic residency` run should not pay for.

**Guest.** A `.wat` module embedded as a `const &str` in `boundary.rs` and compiled at runtime by
`Module::new`, which accepts WebAssembly text. No `wasm32-unknown-unknown` target to install, no
checked-in binary, and the guest is readable next to the rung it serves:

```wat
(module
  (memory (export "mem") 1)
  (func (export "score") (param $ptr i32) (param $len i32) (result i64)
    (i64.add
      (i64.load8_u (local.get $ptr))
      (i64.load8_u (i32.add (local.get $ptr) (i32.sub (local.get $len) (i32.const 1)))))))
```

One 64 KiB page covers the 8 KiB maximum payload.

**Measurement.** Compile once; one `Store`, one `Instance`, one `TypedFunc<(i32, i32), i64>`. Per
size: `Memory::write` the payload into linear memory at offset 0, then `call`. The copy is included
deliberately -- a guest cannot be handed a host pointer, so marshalling into linear memory *is* the
boundary, and `Cost::fit` reports it as the slope while the fixed term stays the number §2.2's
argmin argument needs.

**Correctness gate.** Assert once that the guest's result equals the host's `b[0] + b[len-1]` for
the same buffer. A benchmark that times a call which did not happen is worse than no benchmark.

**Second number, not a rung: per-call instantiation.** Build an `InstancePre` and time
`instantiate` + one call, reported as a single figure rather than a `by_size` row. This is P3's
test and the price of per-call isolation.

### 3.2 `Boundary::ExtProc` -- the callout shape, not a unary echo

**Proto.** A new `proto/extproc.proto`, shape-equivalent to Envoy's `ExternalProcessor`: a
bidirectional stream, a `oneof` on both sides, a header map in, a header mutation out.

```proto
service ExternalProcessor {
  rpc Process(stream ProcessingRequest) returns (stream ProcessingResponse);
}
```

with `ProcessingRequest -> HttpHeaders -> HeaderMap -> repeated HeaderValue` inbound and
`ProcessingResponse -> HeadersResponse -> CommonResponse -> HeaderMutation -> repeated
HeaderValueOption` outbound: three levels of nesting each way, a `oneof` on each.

**Honest caveat, to be published with the number.** Envoy's real descriptor is deeper and carries
more optional fields, so this measures a **lower bound** on `ext_proc` cost. A lower bound is the
right direction for the argument being made -- §2.2 claims the callout is too expensive for an
argmin, and a floor that is still too expensive settles it.

**Payload shape.** A realistic gateway header map -- `:authority`, `:method`, `:path`,
`content-type`, `content-length`, `user-agent`, `authorization`, `x-request-id`, `traceparent` and
a few custom -- at a **fixed header count** across all three sizes, with one filler value padded so
the encoded message hits each `SIZES` target. Holding the count fixed keeps per-field decode cost
constant, so the fitted slope reflects bytes rather than fields.

*As built, this only works for two of the three sizes.* The unpadded map encodes to **367 bytes**,
so the 64 B target is below this rung's floor and padding cannot subtract. The rung reports the size
it actually achieved, which leaves the ladder's 64 B cell empty for `ext_proc` and makes its 64 B
hook cost an extrapolation -- both stated wherever the number appears. The alternative, shrinking
the header values until the map fits in 64 bytes, would have bought comparability by giving up the
"realistic gateway header map" this section asks for.

**Reply.** One `HeaderValueOption` setting `x-gateway-destination-endpoint`, which is what llm-d's
Endpoint Picker actually returns. Small and fixed, as in production.

**Two numbers, because there are two deployment shapes.**

| measured | what it models |
|---|---|
| per callout on an already-open stream | a long-lived processor stream |
| stream open + first callout | Envoy's default, a stream per HTTP request |

P2 turns on which of these llm-d is paying. Measuring both removes the need to guess, and the gap
between them is itself the answer to "does stream reuse rescue the callout".

Lives under the existing `grpc` feature -- it needs tonic, prost and tokio, all already gated
there.

### 3.3 Re-time the `Ring` rung (a correction, not a new rung)

`ring()` is timed per operation and reports 52 ns at 64 B against a ~35 ns timer. That is a 1.5:1
signal-to-instrument ratio, and it is why `Ring` carries the ladder's worst spread at **4.2x**.
Batch-timing it, as `native()` and `syscall()` already are, should tighten it.

This changes a published constant that §2.2 quotes ("ring 52-232 ns"), so §7's rule applies: report
the old and new side by side and say which conclusions move. Cheap, in scope for "price the seams",
and it improves the denominator of the zero-cost-extension claim rather than the numerator.

### 3.4 The deliverable: per-decision hook cost

The ladder is the input; the deliverable is the table §2.2 currently computes by hand. Add to
`boundary()` in `main.rs`, using the 64 B fixed cost, since an argmin's per-candidate payload is
small:

```
policy hook inside an argmin (one hook per candidate, 64 B)

                        isolation      4 nodes   32 nodes  128 nodes   decisions/s @ 32
native call             none            ...        ...       ...            ...
wasm (warm instance)    sandbox         ...        ...       ...            ...
shared ring (spin)      process         ...        ...       ...            ...
ext_proc callout        process         ...        ...       ...            ...
gRPC unary              process         ...        ...       ...            ...
```

`N x fixed_ns` per placement, and `1e9 / (N x fixed_ns)` as the single-thread decision-rate
ceiling. That ceiling is the form of the claim that survives: it is a property of the measured
ladder alone and needs no workload, no `exec_ns`, and no simulator.

### 3.5 Report plumbing

- Extend the `steps` list in `boundary()` with the two new adjacencies:
  `native -> wasm` ("sandbox entry + linear-memory copy") and `tcp_loopback -> ext_proc`
  ("HTTP/2 framing on an open stream"), with `ext_proc -> grpc` isolating **per-call stream setup**
  -- the step that decides P2.
- Change the step deltas from `saturating_sub` to a **signed** difference. If `ext_proc` lands
  above `grpc`, an inversion is a finding and clamping it to `0.00 us` would hide it.
- `crossing_of()` in `main.rs` gains `"wasm"` and `"extproc"` names so an arm can be charged at
  either boundary later. Phase 8 will want this; adding it now costs one match arm.
- Add `--isolation` (or reuse `--repeat`) nothing new on the CLI: `polyphonic boundary --repeat 5`
  stays the single entry point, with the new rungs appearing when their features are built.

### 3.6 Publish

| target | change |
|---|---|
| `residency-ledger.md`, *Boundary costs* | two new rows, the re-timed `Ring` row, and the hook-cost table |
| `owned-and-observed.md` §2.6 isolation table | replace the borrowed `Ring` figure with the measured `Wasm` one; add the per-call-isolation row if P3 holds |
| §2.2 per-decision row, §2.3 accounting, §2.7 predicted crossover | update the quoted constants if `ext_proc` differs from unary, and say by how much the crossover moves |
| §2.6 / §8 | nothing -- the library-versus-written argument is independent of these numbers |

---

## 4. Verification

- `cargo fmt --check` and `cargo clippy --all-targets --all-features` clean. `pedantic` is on at
  `warn` and `unsafe_op_in_unsafe_fn` / `unused_must_use` are `deny`; neither new rung needs
  `unsafe`, unlike `pipe()` and `ring()`.
- `cargo build` with no features still succeeds and the report degrades gracefully, printing `-`
  for absent rungs exactly as it does for `Grpc` today.
- `cargo run --features grpc,wasm -- boundary --repeat 5` produces every row.
- The WASM guest's result is asserted equal to the host computation.
- The `ext_proc` reply is asserted to carry the mutation header, so a full round trip is what was
  timed.
- Spread is reported per rung, as now. On this host (Apple silicon, `pin_cluster` is a QoS hint
  only) the **ordering is the result** and no constant survives being quoted to two digits.

---

## 5. Risks

1. **`ext_proc` comes in far below 49 us.** The likeliest outcome, and it weakens §2.2's headline
   arithmetic. It is also the point of measuring: §2.7 already commits to saying so plainly rather
   than reaching for a workload mix that rescues the claim.
2. **The minimal proto understates.** Flagged inline as a lower bound rather than corrected by
   vendoring Envoy's descriptor tree, which would drag in the xDS proto graph for a benchmark.
3. **`wasmtime` build weight and MSRV.** Feature-gated and off by default. The floor here is
   edition 2024 / 1.85 with no `rust-toolchain.toml`; if the current `wasmtime` major wants newer,
   pin an older major rather than raise the floor.
4. **Re-timing `Ring` changes a number already in print.** Intentional, and handled by publishing
   both.
5. **Host noise.** Unchanged from today: best-of-`reps`, spread published, ordering is the claim.

---

## 6. Out of scope

- The `data_path: { Sidecar, Integrated }` arm and the crossover sweep. That is Phase 8, and it
  depends on this and nothing else.
- Everything in the memory chain -- `cache.rs`, `machine.rs`, ownership types, telemetry.
- A real Envoy filter, a real proxy integration, or the choice of proxy core. §2.6 settles that
  argument on structure, not on these numbers.
- Designing polyproto's own WASM ABI. This measures what a sandbox boundary costs; what crosses it
  is a later question.
