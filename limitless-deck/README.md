# Limitless Deck 💥
### *On the Road to Lock Freedom*

> **A high-octane, interactive presentation engine built in Rust with [Bevy](https://bevyengine.org/) (`0.19`), heavily inspired by the stylized punk rebellion, anti-establishment typography, and kinetic visual anarchy of *Persona 5*.**

---

## 🎭 Overview

**Limitless Deck** is a presentation deck about lock-free concurrency and multithreaded systems architecture, disguised as a Phantom Thief palace infiltration. It rejects sterile, corporate slideshow templates in favor of visceral motion, torn magazine cutout typography, procedural ink splatters, and dramatic visual contrast—while keeping technical code blocks crisp, rectilinear, and immediately readable.

---

## 🎨 Aesthetic & Design Rules

### 1. Strict Punk Palette & High-Voltage Accents
The entire application adheres to a disciplined high-contrast palette:
- **Crimson Reds** (`#E60012`, `#8B0008`, `#FF1A2B`): High-voltage punk accents—razor screen slashes, letter drop-shadows, wax seals, ink sprays, chevrons, and alarm states.
- **Stark Whites** (`#FFFFFF`, `#ECECF0`): Aggressive contrast headlines, domino mask, and bright cutout scraps.
- **Pitch Blacks & Off-Blacks** (`#0D0D11`, `#16161D`): Deep canvas bases, heavy block scraps, and terminal backgrounds.
- **Silvers & Greys** (`#70707D`, `#353542`, `#8E8E9B`): Structural dividers, subtle annotations, and code comments.
- **Persona Menu & Strikers Accents** (`P5_GOLD`, `P5_CYAN`, `P5_MAGENTA`): Amber-gold for the climax menu card (mirroring Iwai's airsoft `SELL` card) and prismatic neon cyan/magenta shards for dynamic title badge slices.

### 2. Graphic Manga UI & Typographic Hierarchy (Persona 5 & Strikers Style)
UI commands and headlines are not flat corporate text blocks or disconnected ransom letters. They embody the authentic graphic design language of *Persona 5*:
- **Unified 3D Extruded Comic Banners:** The primary title **"ON THE R★AD TO LOCK-F★EEDOM >>"** is constructed as unified, chunky graphic badges with thick comic outlines, deep 3D drop-extrusion layers, embedded stars (`★`), and sliced neon cyan/magenta energy wedges (`P5_CYAN`, `P5_MAGENTA`) inspired by *Persona 5 Strikers*.
- **Authentic Typographic Hierarchy (`FontAssets`):** All typography is powered by bundled TrueType fonts embedded via `include_bytes!` in `src/slideshow/fonts.rs`:
  - **Display / Title Banners:** *Impact* (`font_assets.display`) for massive, compressed comic action titles and calendar date stamps.
  - **Geometric Sans-Serif:** *Arial Bold* (`font_assets.sans`, also assigned as engine `AssetId::default()`) for crisp route subtitles and tactical card descriptions.
  - **Punchy Heavy Accents:** *Arial Black* (`font_assets.sans_heavy`) for loud classified tags, button prompts, and security alerts.
  - **Ransom Editorial Serif:** *Georgia Bold* (`font_assets.serif`) for the Phantom Thieves calling card body manifesto.
  - **Typewriter Monospace:** *Courier New Bold* (`font_assets.monospace`) for code block tokens, line gutters, and technical sign-offs.
  - **Vector Symbols (`font_assets.symbols`):** Comprehensive symbol glyph coverage eliminating `.notdef` tofu blocks for stars (`★`), dagger pointers (`▶`), bullets (`•`), security alarm blocks (`▮`, `▯`), lightning (`⚡`), and diamonds (`◆`).
- **Descending Card Staircase (Agenda Menu):** Inspired directly by the *Untouchable Airsoft Shop* menu (Munehisa Iwai), the infiltration agenda cascades diagonally at ~20° (`margin-left: i * 26px`), with rounded thick-bordered cards (`BorderRadius::all(Val::Px(8.0))`).
- **Corner-Stamped Ink Splatters:** In true P5 fashion, organic fluid ink splatters are **stamped directly onto the corners of each card** (`spawn_corner_ink_splatter`), bleeding across the card boundary and onto the canvas.
- **High-Voltage Palette Highlights:** The active route pops in stark white and crimson (`P5_RED`), while the climax agenda card (`WAIT-FREE DATA STRUCTURES`) pops in **amber-gold** (`P5_GOLD`) with a black corner splatter, mirroring the iconic `SELL` card.
- **Embedded Graphic Iconography:** Typography integrates sharp vector stars (`★`), comic action bursts (`HOLD UP!`), dagger cursors (`▶`), and sub-label tags (`[CURRENT ROUTE]`, `// PALACE 01 //`).

### 3. Non-Rectangular & Organic Geometry
- **Procedural Ink Blotches & Splatters:** 
  - *Corner-Stamped Card Splatters (`spawn_corner_ink_splatter`):* Pinned to the corners of UI cards (`TopLeft`, `TopRight`, `BottomLeft`, `BottomRight`) with multi-lobed antialiased circular pools and radiating droplets.
  - *UI-Layer Fluid Splatters (`spawn_ui_ink_blotch`):* Fluid circular puddles bleeding from behind the title banners and comic tags.
  - *Background World Splatters (`spawn_ink_blotch`):* Layered crimson (`#E60012`) and bright red spray droplets cutting across dark background shards and hazard bands.
- **Explosive Comic Action Starbursts:** Jagged, multi-point comic action bubbles (`HOLD UP!`) and wax seals (`★ TAKE YOUR LOCKS ★`).
- **Captivity Motifs:** Diagonal black-and-white zebra hazard bands (`/ / / / /`) and tumbling 2D shattered chain links with razor crimson fracture lines.

### 4. Rectilinear Monospace Code Presentation
While the surrounding environment thrives on anarchy, code comparisons remain 100% rectilinear, clean, and legible using dedicated monospace typography inside high-contrast P5 terminal frames to preserve technical clarity.

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
├── assets/                     # Bundled TrueType & OpenType font assets
│   └── fonts/                  # Impact, Arial Bold, Arial Black, Georgia, Courier, Symbols
└── src/
    ├── main.rs                 # App entry point, window configuration, camera setup
    ├── theme.rs                # Centralized design system (colors, geometry, cutout, ink, captivity, motion, typography)
    ├── slides/                 # Slide implementations
    │   ├── mod.rs              # SlidesPlugin registration & cleanup routing
    │   ├── intro.rs            # Slide 1: 3D extruded comic title banners, 5-card descending agenda staircase, calling card
    │   └── mutex_bottleneck.rs # Slide 2: Mutex vs Atomic code comparison & tactical callouts
    └── slideshow/              # Engine systems & visual presentation components
        ├── mod.rs              # SlideState, SlideController, and keyboard routing
        ├── animation.rs        # SlamEntrance, PunkJitter, BobbingCursor, ScreenSlashBlade
        ├── background.rs       # Hazard stripes, speed lines, tumbling fractured chains
        ├── character.rs        # Phantom Thief silhouette, breathing idle & slash lunges
        ├── code_view.rs        # Monospace tokenized syntax renderer & terminal frame
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

---

## 📑 Slide Deck Content

1. **Slide 1: Intro — *"On the Road to Lock Freedom"***
   - Jagged comic starburst (`HOLD UP!`) with asymmetric red/black border accents.
   - Dual unified 3D extruded comic title banners (`ON THE R★AD` and `[TO] LOCK-F★EEDOM >>`) with thick comic outlines, embedded black stars, and sliced neon cyan/magenta energy shards.
   - Procedural fluid crimson ink puddles anchoring the title banners.
   - 5-card descending agenda staircase inspired by the Untouchable Airsoft Shop menu, with corner-stamped ink splatters on each card, bobbing dagger pointers (`▶`), and an amber-gold climax card (`WAIT-FREE DATA STRUCTURES`).
   - Phantom Calling Card manifesto addressed to *"SIR MUTEX OF THE KERNEL"* featuring a multi-point starburst wax seal (`TAKE YOUR LOCKS`).

2. **Slide 2: Exhibit A — *"The Lock-Free Paradigm"***
   - Monospace terminal comparison: Blocking `Mutex<u64>` (thread parking & context switching) vs Hardware `AtomicU64::fetch_add` (single-instruction lockless execution).
   - Tactical callout cards highlighting the performance penalty of kernel preemption and contention fallbacks.

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
