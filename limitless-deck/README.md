# Limitless Deck 💥
### *On the Road to Lock Freedom*

> **A high-octane, interactive presentation engine built in Rust with [Bevy](https://bevyengine.org/) (`0.19`), heavily inspired by the stylized punk rebellion, anti-establishment typography, and kinetic visual anarchy of *Persona 5*.**

---

## 🎭 Overview

**Limitless Deck** is a presentation deck about lock-free concurrency and multithreaded systems architecture, disguised as a Phantom Thief palace infiltration. It rejects sterile, corporate slideshow templates in favor of visceral motion, torn magazine cutout typography, procedural ink splatters, and dramatic visual contrast—while keeping technical code blocks crisp, rectilinear, and immediately readable.

---

## 🎨 Aesthetic & Design Rules

### 1. Strict Punk Palette
The entire application strictly adheres to a four-tone palette:
- **Crimson Reds** (`#E60012`, `#FF1A2B`): Reserved strictly as a high-voltage punk accent—razor screen slashes, letter drop-shadows, wax seals, ink sprays, chevrons, and alarm states.
- **Stark Whites** (`#FFFFFF`, `#ECECF0`): Aggressive contrast headlines, domino mask, and bright cutout scraps.
- **Pitch Blacks & Off-Blacks** (`#0D0D11`, `#16161D`): Deep canvas bases, heavy block scraps, and terminal backgrounds.
- **Silvers & Greys** (`#70707D`, `#353542`, `#8E8E9B`): Structural dividers, subtle annotations, and code comments.

### 2. Per-Letter Magazine Cutout Typography
Headlines are not uniform corporate text blocks. The primary title **"ON THE ROAD TO LOCK FREEDOM"** is constructed letter-by-letter as an underground punk zine ransom note:
- Every letter sits on an individually clipped scrap with conflicting font sizes (`38px` to `62px`), contrasting backgrounds (black-on-white, white-on-black), asymmetric borders, and distinct tilt angles (`-12.6°` to `+8.0°`).
- Torn masking tape scraps separate words and anchor calling cards.
- Each letter scrap drops onto the screen independently along unique diagonal trajectories.

### 3. Non-Rectangular & Organic Geometry
- **Procedural Ink Blotches & Splatters:** Multi-lobed organic ink pools and high-speed directional spray droplets break straight lines and hard angles.
- **Explosive Comic Action Starbursts:** Jagged, multi-point comic action bubbles (`💥 HOLD UP!`) and wax seals (`💥 TAKE YOUR LOCKS 💥`).
- **Captivity Motifs:** Diagonal black-and-white zebra hazard bands (`/ / / / /`) and tumbling 2D shattered chain links with razor crimson fracture lines.

### 4. Rectilinear Monospace Code Presentation
While the surrounding environment thrives on anarchy, code comparisons remain 100% rectilinear, clean, and legible inside high-contrast P5 terminal frames to preserve technical clarity.

---

## ⚡ Kinetic Motion Engine

- **Violent 15-Frame Slam-In (`SlamEntrance`):** UI elements do not fade in statically. They violently slap onto the canvas from off-screen using snappy spring overshoot physics (`ease_out_back`) with staggered cascading delays.
- **Living Erratic Micro-Jitter (`PunkJitter`):** Headings, tags, and action stamps possess subtle breathing oscillations and organic twitches to feel alive on screen.
- **Razor Diagonal Screen Slashes (`ScreenSlashBlade`):** Advancing or retreating slides cuts the entire screen with a high-velocity crimson and black diagonal blade wipe.
- **Dynamic 2D Character Performance:** An animated Phantom Thief silhouette with fluttering coat tails, crimson inner trim, glowing mask eye glints, and a dagger. Breathes at idle and lunges forward into a dynamic slash on slide navigation.
- **P5 HUD:** Top-left calendar date (`09 / 23 AFTER SCHOOL`), pulsing Palace Security Level gauge (`[ ▮▮▮▮▮▮▮▮▮▯ 99% ] [! TRESPASSER !]`), slide counter, and spinning star compass (`TAKE YOUR TIME`).

---

## 🕹️ Controls

| Key | Action |
| :--- | :--- |
| `Space` / `→` / `Enter` / `PageDown` | Advance to Next Slide (with blade slash & character lunge) |
| `←` / `Backspace` / `PageUp` | Retreat to Previous Slide |
| `F` / `F11` | Toggle Borderless Fullscreen |

---

## 📂 Project Architecture

```
limitless-deck/
├── Cargo.toml                  # Bevy 0.19.1 dependency & fast-compile profiles
├── README.md                   # Project documentation
└── src/
    ├── main.rs                 # App entry point, window configuration, camera setup
    ├── theme.rs                # Color palette constants & rotational angles
    ├── slides/                 # Slide implementations
    │   ├── mod.rs              # SlidesPlugin registration & cleanup routing
    │   ├── intro.rs            # Slide 1: Magazine cutout title, agenda, calling card
    │   └── mutex_bottleneck.rs # Slide 2: Mutex vs Atomic code comparison & tactical callouts
    └── slideshow/              # Engine systems & visual presentation components
        ├── mod.rs              # SlideState, SlideController, and keyboard routing
        ├── animation.rs        # SlamEntrance, PunkJitter, BobbingCursor, ScreenSlashBlade
        ├── background.rs       # Hazard stripes, speed lines, tumbling fractured chains
        ├── character.rs        # Phantom Thief silhouette, breathing idle & slash lunges
        ├── code_view.rs        # Monospace tokenized syntax renderer & terminal frame
        ├── hud.rs              # Calendar date, palace security gauge, slide counters
        └── splatter.rs         # Procedural organic ink blotches & directional spray droplets
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

---

## 📑 Slide Deck Content

1. **Slide 1: Intro — *"On the Road to Lock Freedom"***
   - Jagged comic starburst (`💥 HOLD UP!`)
   - 23 individually clipped magazine scraps forming the anarchic headline
   - Anchoring black ink blotch and high-voltage crimson spray
   - Infiltration agenda with bobbing dagger menu pointers (`▶`)
   - Phantom Calling Card manifesto addressed to *"SIR MUTEX OF THE KERNEL"* with a multi-point starburst wax seal (`💥 TAKE YOUR LOCKS 💥`)

2. **Slide 2: Exhibit A — *"The Lock-Free Paradigm"***
   - Monospace terminal comparison: Blocking `Mutex<u64>` (thread parking & context switching) vs Hardware `AtomicU64::fetch_add` (single-instruction lockless execution)
   - Tactical callout cards highlighting the performance penalty of kernel preemption and contention fallbacks

---

## 🛠️ Adding New Slides

To add a new slide to the presentation:

1. Add a new variant to `SlideState` in `src/slideshow/mod.rs` and update `TOTAL_COUNT`:
   ```rust
   #[derive(States, Default, Debug, Clone, Copy, PartialEq, Eq, Hash)]
   pub enum SlideState {
       #[default]
       Intro,
       MutexBottleneck,
       MyNewSlide, // <-- Add here
   }
   ```
2. Create `src/slides/my_new_slide.rs` with a spawn function attaching `DespawnOnExit(SlideState::MyNewSlide)` to the root slide entity.
3. Register the system in `src/slides/mod.rs`:
   ```rust
   app.add_systems(OnEnter(SlideState::MyNewSlide), spawn_my_new_slide);
   ```
