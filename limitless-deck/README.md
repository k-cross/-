# Limitless Deck 💥
### *On the Road to Lock Freedom*

> **A high-octane, interactive presentation engine built in Rust with [Bevy](https://bevyengine.org/) (`0.19`), heavily inspired by the stylized punk rebellion, anti-establishment typography, and kinetic visual anarchy of *Persona 5*.**

---

## 📚 Content Sources & Technical References

The entire technical narrative, code snippets, hardware profiling data, and mathematical formulas in this deck are based on the two-part engineering post series by **Ken Cross**:

1. **Part 1:** [**Paving the Way to Lock Freedom (Part 1)**](https://k-cross.github.io/limits1)
   - First commit naive ring buffer with atomics and synchronous spin loops.
   - Atomic underflow on the `size` variable (`fetch_sub` wrapping to `usize::MAX = 18,446,744,073,709,551,615`).
   - State ambiguity without size: distinguishing empty (`head == tail`) from full (`(tail + 1) % cap == head`).
   - Why `AtomicBool` is insufficient for memory initialization (lapping disaster across turns).
   - Generational turn tracking: embedded metadata in `Slot<T>` vs. global bitmaps.
   - Dmitry Vyukov's bounded MPMC turn-stamp algorithm and ABA immunity.
   - Model checking with [Loom](https://github.com/tokio-rs/loom): uncovering spin lock branch explosion and concurrent `UnsafeCell` race conditions.
   - Critical section vulnerabilities: why lockless algorithms block on thread death/preemption.
   - Automated peer-thread recovery and why heuristic healing reintroduces ABA hazards.
   - Measuring hardware cache contention using macOS Instruments and Apple Silicon performance counters.
   - L1 load miss rate ($50.74\%$), CAS contention failure rate ($67.17\%$), and IPC ($0.0306$).
   - Disassembly analysis: identifying false sharing when `read_idx` and `write_idx` occupy the same 128-byte L1 cache line on Apple Silicon.
   - Hardware cache padding using `#[repr(align(128))]` / `crossbeam_utils::CachePadded`.

2. **Part 2:** [**Paving the Way to Lock Freedom (Part 2)**](https://k-cross.github.io/limits2)
   - Review of core architectural changes: size removal, turn-stamps, and bit-walk generation tracking.
   - Branchless index computation: eliminating branch mispredictions via arithmetic bitmasking (`((idx + 1) & at_capacity.wrapping_sub(1)) | (at_capacity * ((idx & mcb) ^ mcb))`).
   - CPU instruction pipeline hazards: visualization of pipeline flushes, gaps, and refills on mispredicted branches.
   - Production observability with User-Level Statically Defined Tracing (USDT) and DTrace probes.
   - Quantizing unbounded tail latency: $64\times$ read latency multipliers ($16.4\ \mu\text{s}$) and $256\times$ write spikes ($131\ \mu\text{s}$).
   - Exponential backoff mitigation using `crossbeam_utils::Backoff` to reduce interconnect bus storms and match Crossbeam benchmarks.
   - Multi-core scaling ceilings: interconnect MESI invalidation bus saturation across cores.
   - Architectural simplification: transitioning to Single-Producer Single-Consumer (SPSC) with thread pinning and local index caching.

---

## 🎭 Overview

**Limitless Deck** is a presentation deck about lock-free concurrency and multithreaded systems architecture, disguised as a Phantom Thief palace infiltration. It rejects sterile, corporate slideshow templates in favor of visceral motion, torn magazine cutout typography, procedural ink splatters, and dramatic visual contrast—while keeping technical code blocks crisp, rectilinear, scrollable, and immediately readable.

---

## 🎨 Aesthetic & Design Rules

### 1. Strict Punk Palette & High-Voltage Accents
The entire application adheres to a disciplined high-contrast palette:
- **Crimson Reds** (`#E60012`, `#8B0008`, `#FF1A2B`): High-voltage punk accents—razor screen slashes, letter drop-shadows, wax seals, ink sprays, chevrons, and alarm states.
- **Stark Whites** (`#FFFFFF`, `#ECECF0`): Aggressive contrast headlines, domino mask, and bright cutout scraps.
- **Pitch Blacks & Off-Blacks** (`#0D0D11`, `#16161D`): Deep canvas bases, heavy block scraps, and terminal backgrounds.
- **Silvers & Greys** (`#70707D`, `#353542`, `#8E8E9B`): Structural dividers, subtle annotations, and code comments.
- **Persona Menu & Strikers Accents** (`P5_GOLD`, `P5_CYAN`, `P5_MAGENTA`): Amber-gold for the climax menu card (mirroring Iwai's airsoft `SELL` card) and prismatic neon cyan/magenta shards for dynamic title badge slices.

### 2. Graphic Manga UI & Typographic Hierarchy (`FontAssets`)
All typography is powered by bundled TrueType fonts embedded via `include_bytes!` in `src/slideshow/fonts.rs`:
- **Display / Title Banners:** *Impact* (`font_assets.display`) for massive, compressed comic action titles and calendar date stamps.
- **Geometric Sans-Serif:** *Arial Bold* (`font_assets.sans`, also assigned as engine `AssetId::default()`) for crisp route subtitles and tactical card descriptions.
- **Punchy Heavy Accents:** *Arial Black* (`font_assets.sans_heavy`) for loud classified tags, button prompts, and security alerts.
- **Ransom Editorial Serif:** *Georgia Bold* (`font_assets.serif`) for the Phantom Thieves calling card body manifesto.
- **Typewriter Monospace:** *Courier New Bold* (`font_assets.monospace`) for code block tokens, line gutters, formulas, and technical sign-offs.
- **Vector Symbols (`font_assets.symbols`):** Comprehensive symbol glyph coverage eliminating `.notdef` tofu blocks for stars (`★`), dagger pointers (`▶`), bullets (`•`), security alarm blocks (`▮`, `▯`), lightning (`⚡`), and diamonds (`◆`).

### 3. Integrated Visual Diagrams & Math Engine
Beyond standard text callouts, technical slides embed custom Persona 5-styled UI widgets built from native Bevy flexbox and text primitives:
- **Thread Execution Flow Diagrams (`spawn_flow_diagram`):** Multi-step horizontal execution pipelines showing thread stages with arrows (`→`) and highlighted failure points.
- **Mathematical Formula Panels (`spawn_math_formula`):** Dedicated formula display containers highlighting variables, operations, and numerical evaluations with color-coded syntax.
- **Hardware Performance Bar Charts (`spawn_bar_chart`):** Horizontal proportional bar charts showing cache miss rates, contention percentages, and multi-core scaling.
- **Ring Buffer State Diagrams (`spawn_ring_buffer_diagram`):** Visual representation of array slots with generation stamps, values, and dynamic `RD` / `WR` cursor pointers.
- **Latency Distribution Histograms (`spawn_histogram`):** Power-of-2 quantize histograms showing median clusters and extreme tail latency spikes.

---

## 🕹️ Controls

| Key / Input | Action |
| :--- | :--- |
| `Space` / `→` / `Enter` / `PageDown` | Advance to Next Slide (with blade slash & character lunge) |
| `←` / `Backspace` / `PageUp` | Retreat to Previous Slide |
| `Mouse Wheel` (Scroll) | Scroll code blocks & debugger logs smoothly |
| `J` / `↓` (Arrow Down) | Scroll code block down |
| `K` / `↑` (Arrow Up) | Scroll code block up |
| `F` / `F11` | Toggle Borderless Fullscreen |

---

## 📑 Slide Deck Breakdown (23 Slides)

| # | Slide Title | Technical Topic & Source Content | Visual / Math Component |
| :---: | :--- | :--- | :--- |
| **01** | **On the Road to Lock Freedom** | Palace Infiltration calling card manifesto & descending zine agenda. | Extruded 3D title banners, stars, & calling card seal |
| **02** | **Synchronous with Atomics** | Naive ring buffer commit (`b0f8f05d`), atomic cursors, livelock deadlock. | Thread flow diagram (Write vs Read thread pipeline) |
| **03** | **The Size Variable Race** | Atomic counter underflow in `read()` and LLDB breakpoint analysis. | Math formula: $\text{size} = 0 \to 18{,}446{,}744{,}073{,}709{,}551{,}615$ |
| **04** | **State Ambiguity** | Removing `size` and resolving the `head == tail` boundary condition. | Math formulas: Empty ($\text{head} == \text{tail}$), Full ($(\text{tail}+1)\%\text{cap} == \text{head}$) |
| **05** | **AtomicBool Is Not Enough** | Memory initialization and data corruption when producers lap consumers. | Math formula: 2-state lapping sequence ($\text{false} \to \text{true} \to \text{false} \to \text{true}$) |
| **06** | **Who Checks What, and When** | Graphic execution lanes showing race windows between check and commit. | Full-width multi-thread step timeline with highlighted race gaps |
| **07** | **The Sleeping Reader** | Visualizing ABA when a reader deschedules and wakeups up after a full lap. | 4-slot ring buffer state scenes before and after buffer wrap |
| **08** | **Manual Memory Tracking** | Generational sequence stamps in `Slot<T>` and fixing test stack overflows. | Locality formula: $\text{bitmap\_words} = \frac{\text{capacity}}{64}$ vs embedded $\text{Slot}\langle T \rangle$ |
| **09** | **The ABA Problem Resolution** | Dmitry Vyukov's MPMC turn-stamp algorithm and monotonic progression. | 4-slot ring buffer memory diagram + Vyukov invariant formula |
| **10** | **Testing with Loom** | Model checking reveals spin lock branch explosion and concurrent `UnsafeCell` race. | Metrics panel: Iteration $68{,}005$ branch limit panic + race alert |
| **11** | **Crash in Critical Section** | Lockless slot progression vulnerabilities: thread death in critical section. | Critical section flow diagram: `CAS` $\to$ `Write` $\to$ `Commit Stamp` |
| **12** | **Automating Recovery** | Why peer-thread healing heuristics cannot distinguish preemption from death. | Lap distance invariant formula: $|\lfloor\text{rd}/\text{cap}\rfloor - \lfloor\text{st}/\text{cap}\rfloor| \ge 2$ |
| **13** | **Measuring Cache Contention** | Hardware counter metrics on Apple Silicon using macOS Instruments. | Bar chart: L1 Miss Rate ($50.74\%$), CAS Contention ($67.17\%$), IPC ($0.03$) |
| **14** | **Disassembly & Cache Lines** | Decompilation analysis: false sharing across 128-byte cache lines on Apple Silicon. | 128-byte L1 cache line diagram + `#[repr(align(128))]` fix |
| **15** | **The Lap Bit** | Bit-walk generation tracking: shifting and flipping the furthest-reaching bit. | Dual binary bit-row comparison table with XOR flip demonstrations |
| **16** | **Branchless Computation** | Pure arithmetic wrapping via `wrapping_sub(1)` bitmasking without branches. | Math formulas: `at_capacity.wrapping_sub(1)` mask + XOR flip |
| **17** | **When the CPU Guesses Wrong** | CPU pipeline stall visualization: fetch/decode stalls and bubble flushes. | 5-stage CPU pipeline grid across misprediction, flush, and refill |
| **18** | **Measuring Tail Latency** | Production tracing using User-Level Statically Defined Tracing (USDT). | DTrace USDT probe overhead formula ($0\text{ ns}$ when disabled) |
| **19** | **Unbounded Tail Latency** | DTrace quantize histograms revealing extreme latency outlier multipliers. | Power-of-2 histogram: $64\times$ read spike ($16.4\ \mu\text{s}$), $256\times$ write spike ($131\ \mu\text{s}$) |
| **20** | **Backoff Mitigation** | Exponential backoff (`crossbeam_utils::Backoff`) relieving interconnect storms. | Backoff progression flow: `CAS` $\to$ `spin_loop` $\to$ `yield_now` $\to$ `Snooze` |
| **21** | **Four Cores, One Index** | Multi-core contention topology: 4 readers and 4 writers hammering single lines. | Dual multi-core cluster topology mapping cores to index lines |
| **22** | **Core Scaling Limitations** | Multi-threaded scaling ceiling caused by MESI invalidation bus saturation. | Bar chart: Throughput vs Core Count (scaling collapse past 8 cores) |
| **23** | **SPSC & Architectural Paths** | Single-Producer Single-Consumer queues with core pinning & zero CAS ops. | Synchronization architecture panel: 0 CAS ops, Acquire/Release ordering |

---

## 📂 Project Architecture

```
limitless-deck/
├── Cargo.toml                  # Bevy 0.19.1 dependency & fast-compile profiles
├── README.md                   # Project documentation & content sources
├── assets/                     # Bundled TrueType & OpenType font assets
│   └── fonts/                  # Impact, Arial Bold, Arial Black, Georgia, Courier, Symbols
└── src/
    ├── main.rs                 # App entry point, 1280x720 window, 2D camera setup
    ├── theme.rs                # Centralized design system (colors, geometry, cutout, ink, typography)
    ├── slides/                 # Slide implementations
    │   ├── mod.rs              # SlidesPlugin registration & OnEnter state routing
    │   ├── intro.rs            # Slide 1: 3D extruded comic title banners, agenda staircase, calling card
    │   ├── auto_slides.rs      # Code blocks, diagrams, math panels, bar charts, callout cards
    │   └── figure_slides.rs    # Full-width graphic diagram slides (lanes, scenes, bit-walk, pipeline, topology)
    └── slideshow/              # Presentation engine systems & reusable widgets
        ├── mod.rs              # SlideState (23 variants), SlideController, slide navigation
        ├── animation.rs        # SlamEntrance, PunkJitter, BobbingCursor, ScreenSlashBlade
        ├── background.rs       # Hazard stripes, speed lines, tumbling fractured chains
        ├── character.rs        # Phantom Thief silhouette, breathing idle & slash lunges
        ├── code_view.rs        # Monospace tokenized syntax highlighter & scrollable terminal frame
        ├── diagrams.rs         # Flow diagrams, math formula panels, bar charts, ring buffer diagrams, histograms
        ├── figures.rs          # Graphic lane, bit-row, pipeline, and core topology widgets
        ├── fonts.rs            # FontAssets resource & font registration system
        ├── hud.rs              # Calendar date, palace security gauge, slide counters
        └── splatter.rs         # Procedural ink blotches, UI splatters & corner-stamped card ink droplets
```

---

## 🚀 Getting Started

### Prerequisites
- [Rust](https://www.rust-lang.org/) (2024 edition compatible, 1.85+)
- Operating System: macOS (Metal), Linux (Vulkan/X11/Wayland), or Windows (DirectX 12/Vulkan)

### Running the Presentation
```bash
# Clone the repository (if not already local)
cd limitless-deck

# Launch the presentation
cargo run
```

### Building for Release
```bash
cargo build --release
./target/release/limitless-deck
```

> **Compilation Note:** `Cargo.toml` is pre-configured with `debug = 0` and `opt-level = 1` for dev builds to avoid multi-gigabyte DWARF symbol bloat and ensure fast 1–2 second incremental compilation.
